"""
ETL: load the CBIP/BCFI repertoire dump (PostgreSQL `pg_dump --inserts`) into
the same SQLite database as the SAM data, under `cbip_*` table names.

The dump contains:
- DDL using PostgreSQL types (varchar(N), numeric(p,s), bool, char(N), text)
- One INSERT per row, with multi-line strings inside CREATE/INSERT bodies.

We translate types to SQLite-compatible ones, prefix every table name with
`cbip_`, skip session-level statements (SET SEARCH_PATH …), and execute the
result statement-by-statement. Single-quote escapes (`''`) and embedded
newlines inside string literals are handled by a small character-level
state machine, so multi-line INSERTs (typical for `hyr.intro`) parse safely.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path
from typing import Iterator

CBIP_TABLES = ("gal", "ggr_link", "hyr", "innm", "ir", "mp", "mpp", "sam")

_VARCHAR_RE = re.compile(r"\bvarchar\s*\(\s*\d+\s*\)", re.IGNORECASE)
_NUMERIC_RE = re.compile(r"\bnumeric\s*\(\s*\d+\s*,\s*\d+\s*\)", re.IGNORECASE)
_CHAR_RE    = re.compile(r"\bchar\s*\(\s*\d+\s*\)", re.IGNORECASE)
_BOOL_RE    = re.compile(r"\bbool\b", re.IGNORECASE)
_CREATE_RE  = re.compile(r"\bCREATE TABLE\s+([A-Za-z_]\w*)\b", re.IGNORECASE)
_INSERT_RE  = re.compile(r"\bINSERT INTO\s+([A-Za-z_]\w*)\b", re.IGNORECASE)


def iter_statements(text: str) -> Iterator[str]:
    """Yield SQL statements from a dump, respecting single-quoted strings."""
    in_string = False
    start = 0
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and text[i + 1] == "'":
                    i += 2
                    continue
                in_string = False
        else:
            if ch == "'":
                in_string = True
            elif ch == ";":
                stmt = text[start : i + 1].strip()
                if stmt:
                    yield stmt
                start = i + 1
        i += 1
    tail = text[start:].strip()
    if tail:
        yield tail


def translate(stmt: str) -> str | None:
    s = stmt.strip()
    # Skip PG-only session settings.
    if re.match(r"\s*SET\s", s, re.IGNORECASE):
        return None

    s = _CREATE_RE.sub(lambda m: f"CREATE TABLE cbip_{m.group(1)}", s, count=1)
    s = _INSERT_RE.sub(lambda m: f"INSERT INTO cbip_{m.group(1)}", s, count=1)

    # Type rewrites only inside CREATE TABLE — safer than touching INSERTs
    # whose VALUES could in principle contain those words inside strings.
    if s.lstrip().upper().startswith("CREATE TABLE"):
        s = _VARCHAR_RE.sub("TEXT", s)
        s = _NUMERIC_RE.sub("REAL", s)
        s = _CHAR_RE.sub("TEXT", s)
        s = _BOOL_RE.sub("TEXT", s)
    return s


def drop_existing(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for tbl in CBIP_TABLES:
        cur.execute(f"DROP TABLE IF EXISTS cbip_{tbl}")
    cur.execute("DROP TABLE IF EXISTS cbip_mp_fts")
    conn.commit()


def create_indexes_and_fts(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_cbip_mpp_cnk     ON cbip_mpp(inncnk);
        CREATE INDEX IF NOT EXISTS idx_cbip_mpp_mpcv    ON cbip_mpp(mpcv);
        CREATE INDEX IF NOT EXISTS idx_cbip_mp_hyrcv    ON cbip_mp(hyrcv);
        CREATE INDEX IF NOT EXISTS idx_cbip_sam_mppcv   ON cbip_sam(mppcv);
        CREATE INDEX IF NOT EXISTS idx_cbip_sam_stofcv  ON cbip_sam(stofcv);

        CREATE VIRTUAL TABLE IF NOT EXISTS cbip_mp_fts USING fts5(
            mpcv UNINDEXED, mpnm,
            tokenize='unicode61 remove_diacritics 2'
        );
        """
    )
    cur.execute("DELETE FROM cbip_mp_fts")
    cur.execute("INSERT INTO cbip_mp_fts(mpcv, mpnm) "
                "SELECT mpcv, COALESCE(mpnm,'') FROM cbip_mp")
    conn.commit()


def run(sql_path: Path, db_path: Path, encoding: str = "utf-8") -> int:
    if not sql_path.exists():
        print(f"! SQL dump not found: {sql_path}", file=sys.stderr)
        return 1
    if not db_path.exists():
        print(f"! SQLite DB not found: {db_path}\n"
              f"  Build it first: python -m sam_mcp.etl --db {db_path}",
              file=sys.stderr)
        return 1

    print(f"[CBIP] reading {sql_path.name} ({sql_path.stat().st_size/1e6:.1f} MB)")
    text = sql_path.read_text(encoding=encoding)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA journal_mode = WAL")

    drop_existing(conn)

    cur = conn.cursor()
    n_create = n_insert = n_skipped = 0
    errors = 0

    cur.execute("BEGIN")
    try:
        for stmt in iter_statements(text):
            sql = translate(stmt)
            if sql is None:
                n_skipped += 1
                continue
            try:
                cur.execute(sql)
            except sqlite3.Error as e:
                errors += 1
                if errors <= 5:
                    print(f"  ! {e} :: {sql[:120]}…", file=sys.stderr)
                continue
            up = sql.lstrip().upper()
            if up.startswith("CREATE"):
                n_create += 1
            elif up.startswith("INSERT"):
                n_insert += 1
                if n_insert % 10000 == 0:
                    print(f"  ... {n_insert} rows", file=sys.stderr)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    print(f"[CBIP] create={n_create} insert={n_insert} "
          f"skipped={n_skipped} errors={errors}")

    create_indexes_and_fts(conn)
    conn.execute("INSERT OR REPLACE INTO meta(key,value) "
                 "VALUES ('cbip_loaded_at', datetime('now'))")
    conn.execute("INSERT OR REPLACE INTO meta(key,value) "
                 "VALUES ('cbip_source', ?)", (sql_path.name,))
    conn.commit()
    conn.close()
    print(f"[done] CBIP loaded into {db_path}")
    return 0 if errors == 0 else 2


def main() -> int:
    p = argparse.ArgumentParser(description="Load CBIP dump into SAM SQLite DB")
    p.add_argument("--sql", type=Path, default=Path("d:/Git/SAM/exportFr.sql"))
    p.add_argument("--db",  type=Path, default=Path("d:/Git/SAM/db/sam.db"))
    p.add_argument("--encoding", default="utf-8")
    args = p.parse_args()
    return run(args.sql, args.db, args.encoding)


if __name__ == "__main__":
    raise SystemExit(main())
