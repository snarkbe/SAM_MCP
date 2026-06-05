"""
SAM MCP server.

Exposes read-only tools over the SQLite database built by `sam_mcp.etl`.

Two transports are supported:

    uv run sam-mcp                     # stdio (Claude Desktop / Claude Code)
    uv run sam-mcp --http              # streamable-http on 0.0.0.0:8000/mcp

The HTTP mode is intended for LAN access (other machines pointing their
MCP client at http://<host-lan-ip>:8000/mcp). Database stays read-only.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.responses import JSONResponse

DB_PATH = Path(os.environ.get("SAM_DB", "db/sam.db"))

mcp = FastMCP("sam")


@contextmanager
def db():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"SAM database not found at {DB_PATH}. "
            f"Run: python -m sam_mcp.etl --data <xml_dir> --db {DB_PATH}"
        )
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _fts_query(text: str) -> str:
    """Make user input safe for FTS5 MATCH: keep alphanumeric tokens, prefix each."""
    tokens = [
        "".join(ch for ch in tok if ch.isalnum() or ch in "-_")
        for tok in text.split()
    ]
    tokens = [t for t in tokens if t]
    if not tokens:
        return '""'
    return " ".join(f'"{t}"*' for t in tokens)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _amp_summary(conn: sqlite3.Connection, code: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT code, name_fr, name_nl, name_en, official_name, status,"
        " medicine_type, company FROM amp WHERE code = ?",
        (code,),
    ).fetchone()
    return _row_to_dict(row) if row else None


def _resolve_to_amp_codes(conn: sqlite3.Connection, ident: str) -> list[str]:
    """Accept either an AMP code (e.g. 'SAM660978-00') or a CNK (e.g. '3104965')."""
    ident = ident.strip()
    # Try CNK first (digits only, typical CNK is 7 digits)
    if ident.isdigit():
        rows = conn.execute(
            "SELECT DISTINCT amp_code FROM dmpp WHERE cnk = ?", (ident,)
        ).fetchall()
        if rows:
            return [r["amp_code"] for r in rows]
    # Else treat as AMP code
    row = conn.execute("SELECT code FROM amp WHERE code = ?", (ident,)).fetchone()
    if row:
        return [row["code"]]
    return []


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------

@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def search_medicine(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Belgian medicines (AMPs) by brand or prescription name.
    Matches French, Dutch and English names with diacritics ignored.
    Returns: list of {amp_code, name_fr, name_nl, status, medicine_type, company}.
    """
    q = _fts_query(query)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT a.code, a.name_fr, a.name_nl, a.status,
                   a.medicine_type, a.company
              FROM amp_fts f
              JOIN amp a ON a.code = f.amp_code
             WHERE amp_fts MATCH ?
             ORDER BY rank
             LIMIT ?
            """,
            (q, max(1, min(limit, 100))),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_medicine(identifier: str) -> dict[str, Any] | None:
    """
    Look up a medicine by CNK (e.g. '3104965') or AMP code (e.g. 'SAM660978-00').
    Returns the full record: identity, pharmaceutical form & route, all active
    ingredients with strength, and the available packs (CNKs).
    """
    with db() as conn:
        codes = _resolve_to_amp_codes(conn, identifier)
        if not codes:
            return None
        amp_code = codes[0]
        amp = _amp_summary(conn, amp_code)
        if amp is None:
            return None

        components = [_row_to_dict(r) for r in conn.execute(
            "SELECT seq, pharma_form_code, pharma_form_fr, pharma_form_nl,"
            " route_code, route_fr, route_nl"
            " FROM amp_component WHERE amp_code = ? ORDER BY seq",
            (amp_code,),
        ).fetchall()]

        ingredients = [_row_to_dict(r) for r in conn.execute(
            "SELECT component_seq, rank, type, substance_code,"
            " substance_name_fr, substance_name_nl,"
            " strength_operator, strength_quantity, strength_unit"
            " FROM amp_ingredient WHERE amp_code = ?"
            " ORDER BY component_seq, rank",
            (amp_code,),
        ).fetchall()]

        packs = [_row_to_dict(r) for r in conn.execute(
            """
            SELECT d.cnk, p.cti_extended, p.pack_display_fr, p.pack_display_nl,
                   p.status, p.delivery_modus, p.ex_factory_price
              FROM ampp p
              LEFT JOIN dmpp d ON d.cti_extended = p.cti_extended
             WHERE p.amp_code = ?
             ORDER BY p.cti_extended
            """,
            (amp_code,),
        ).fetchall()]

    return {
        "amp": amp,
        "components": components,
        "ingredients": ingredients,
        "packs": packs,
    }


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_ingredients(identifier: str) -> list[dict[str, Any]]:
    """
    Return active substances and strengths for a given medicine.
    Identifier can be a CNK or an AMP code. This is the answer to
    "which molecules does X contain?" and "what is the dose of X?".
    """
    with db() as conn:
        codes = _resolve_to_amp_codes(conn, identifier)
        if not codes:
            return []
        rows = conn.execute(
            """
            SELECT component_seq, rank, type, substance_code,
                   substance_name_fr, substance_name_nl,
                   strength_operator, strength_quantity, strength_unit
              FROM amp_ingredient
             WHERE amp_code = ? AND type = 'ACTIVE_SUBSTANCE'
             ORDER BY component_seq, rank
            """,
            (codes[0],),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def find_by_substance(substance: str, limit: int = 50) -> list[dict[str, Any]]:
    """
    Find all medicines (AMPs) that contain a given active substance.
    The substance can be partial: 'paracet', 'salbut', 'ibuprof' all work.
    """
    q = _fts_query(substance)
    with db() as conn:
        sub_codes = [r["substance_code"] for r in conn.execute(
            "SELECT substance_code FROM substance_fts WHERE substance_fts MATCH ?"
            " ORDER BY rank LIMIT 50",
            (q,),
        ).fetchall()]
        if not sub_codes:
            return []
        placeholders = ",".join(["?"] * len(sub_codes))
        rows = conn.execute(
            f"""
            SELECT DISTINCT a.code, a.name_fr, a.name_nl, a.status,
                   i.substance_code, i.substance_name_fr, i.substance_name_nl,
                   i.strength_operator, i.strength_quantity, i.strength_unit
              FROM amp_ingredient i
              JOIN amp a ON a.code = i.amp_code
             WHERE i.substance_code IN ({placeholders})
               AND i.type = 'ACTIVE_SUBSTANCE'
             ORDER BY a.name_fr
             LIMIT ?
            """,
            (*sub_codes, max(1, min(limit, 200))),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_atc(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Look up an ATC classification by code (exact or prefix) or description.
    """
    q = query.strip()
    with db() as conn:
        if q and all(c.isalnum() for c in q):
            rows = conn.execute(
                "SELECT code, description FROM atc"
                " WHERE code = ? OR code LIKE ? ORDER BY code LIMIT ?",
                (q.upper(), q.upper() + "%", max(1, min(limit, 100))),
            ).fetchall()
            if rows:
                return [_row_to_dict(r) for r in rows]
        rows = conn.execute(
            "SELECT code, description FROM atc"
            " WHERE description LIKE ? ORDER BY code LIMIT ?",
            (f"%{q}%", max(1, min(limit, 100))),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_reimbursement(cnk: str) -> list[dict[str, Any]] | None:
    """
    Return reimbursement data for a Belgian medicine by CNK.
    Includes base/reference prices, flat-rate flag, delivery environment and
    reimbursement criteria (category + description). Returns None if the CNK
    has no reimbursement record.
    """
    cnk = cnk.strip()
    with db() as conn:
        rows = conn.execute(
            "SELECT cnk, delivery_environment, valid_from, valid_to, legal_reference,"
            " temporary, is_reference, flat_rate_system,"
            " reimbursement_price, reference_price,"
            " pricing_unit_qty, pricing_unit_fr, pricing_unit_nl"
            " FROM reimbursement WHERE cnk = ?",
            (cnk,),
        ).fetchall()
        if not rows:
            return None
        results = []
        for row in rows:
            entry = _row_to_dict(row)
            entry["pricing_unit"] = {
                "quantity":  entry.pop("pricing_unit_qty"),
                "label_fr":  entry.pop("pricing_unit_fr"),
                "label_nl":  entry.pop("pricing_unit_nl"),
            }
            entry["criteria"] = [_row_to_dict(r) for r in conn.execute(
                "SELECT category, code, description_fr, description_nl"
                " FROM reimbursement_criterion"
                " WHERE cnk = ? AND delivery_environment = ? AND valid_from = ?",
                (cnk, row["delivery_environment"], row["valid_from"]),
            ).fetchall()]
            results.append(entry)
    return results


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def search_nonmedicinal(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Search non-medicinal products (dietary supplements, etc.) by name.
    Returns: list of {code, name_fr, name_nl, category, commercial_status,
    producer_fr, producer_nl}.
    """
    q = f"%{query.strip()}%"
    with db() as conn:
        rows = conn.execute(
            "SELECT code, name_fr, name_nl, category, commercial_status,"
            " producer_fr, producer_nl"
            " FROM nonmedicinal"
            " WHERE name_fr LIKE ? OR name_nl LIKE ?"
            " ORDER BY name_fr LIMIT ?",
            (q, q, max(1, min(limit, 100))),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def find_compounding(query: str, limit: int = 20) -> list[dict[str, Any]]:
    """
    Find compounding (magistral) ingredients by name or synonym.
    Returns: list of {code, synonyms: [{lang, rank, name}]}.
    """
    q = f"%{query.strip()}%"
    with db() as conn:
        codes = [r["code"] for r in conn.execute(
            "SELECT DISTINCT code FROM compounding_synonym WHERE name LIKE ? LIMIT ?",
            (q, max(1, min(limit, 100))),
        ).fetchall()]
        if not codes:
            return []
        results = []
        for code in codes:
            syns = [_row_to_dict(r) for r in conn.execute(
                "SELECT lang, rank, name FROM compounding_synonym"
                " WHERE code = ? ORDER BY lang, rank",
                (code,),
            ).fetchall()]
            results.append({"code": code, "synonyms": syns})
    return results


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_legal_text(text_key: str) -> dict[str, Any] | None:
    """
    Fetch a reimbursement law text by its key (e.g. '3051').
    Returns content in French and Dutch, text type, sequence number and the
    parent legal reference and basis for context.
    """
    with db() as conn:
        row = conn.execute(
            "SELECT basis_key, ref_key, text_key, parent_text_key,"
            " content_fr, content_nl, type, sequence_nr"
            " FROM legal_text WHERE text_key = ?",
            (text_key.strip(),),
        ).fetchone()
    return _row_to_dict(row) if row else None


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _has_cbip(conn: sqlite3.Connection) -> bool:
    return _has_table(conn, "cbip_mpp")


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_cbip_notes(cnk: str) -> dict[str, Any] | None:
    """
    Return CBIP/BCFI editorial commentary for a Belgian medicine, identified
    by CNK. Includes the therapeutic chapter (title, introduction, positioning),
    product-level notes/positioning, and the active substances as listed in
    the repertoire. Returns None if the CNK is not in the CBIP repertoire
    (the CBIP curates a subset of all SAM medicines).
    """
    cnk = cnk.strip()
    with db() as conn:
        if not _has_cbip(conn):
            return None
        head = conn.execute(
            """
            SELECT m.mpcv,
                   m.mpnm                         AS product_name,
                   m.note                         AS product_note,
                   m.pos                          AS product_positioning,
                   m.bt, m.orphan, m.narcotic, m.specrules,
                   p.mppcv,
                   p.mppnm                        AS pack_name,
                   p.galnm_                       AS galenic_form,
                   p.pupr                         AS public_price,
                   p.law, p.ssecr, p."index", p.rema, p.remw,
                   h.hyrcv,
                   h.hyr                          AS chapter_code,
                   h.ti                           AS chapter_title,
                   h.intro                        AS chapter_intro,
                   h.pos                          AS chapter_positioning
              FROM cbip_mpp p
              JOIN cbip_mp  m ON m.mpcv  = p.mpcv
         LEFT JOIN cbip_hyr h ON h.hyrcv = m.hyrcv
             WHERE p.mppcv = ?
            """,
            (cnk,),
        ).fetchone()
        if head is None:
            return None
        result = _row_to_dict(head)

        # Active substances as recorded by CBIP for this pack
        substances = [_row_to_dict(r) for r in conn.execute(
            """
            SELECT s.stofcv, s.stofnm_           AS substance_name,
                   s.inq                         AS quantity,
                   s.inu                         AS unit,
                   s."add"                       AS strength_operator,
                   s.inq2                        AS quantity_per,
                   s.inu2                        AS unit_per,
                   s.inrank                      AS rank
              FROM cbip_sam s
             WHERE s.mppcv = ?
             ORDER BY s.inrank
            """,
            (cnk,),
        ).fetchall()]
        result["substances"] = substances
        return result


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def db_info() -> dict[str, Any]:
    """Return SAM database build info and row counts (for debugging)."""
    with db() as conn:
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
        counts = {}
        for tbl in ("amp", "ampp", "dmpp", "amp_ingredient",
                    "substance", "atc", "pharma_form", "route"):
            counts[tbl] = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
        for tbl in ("vtm", "reimbursement", "reimbursement_criterion",
                    "nonmedicinal", "compounding_ingredient",
                    "legal_basis", "legal_reference", "legal_text"):
            if _has_table(conn, tbl):
                counts[tbl] = conn.execute(
                    f"SELECT COUNT(*) AS n FROM {tbl}"
                ).fetchone()["n"]
        if _has_cbip(conn):
            for tbl in ("cbip_mp", "cbip_mpp", "cbip_hyr",
                        "cbip_innm", "cbip_sam"):
                counts[tbl] = conn.execute(
                    f"SELECT COUNT(*) AS n FROM {tbl}"
                ).fetchone()["n"]
    return {"db_path": str(DB_PATH), "meta": meta, "counts": counts}


def _log_startup_counts() -> None:
    """Log row counts for the important tables at startup.

    Writes to stderr: in stdio mode stdout is the JSON-RPC channel, so any
    diagnostic output must stay off it.
    """
    tables = ("amp", "ampp", "dmpp", "amp_ingredient", "substance", "atc")
    cbip_tables = ("cbip_mp", "cbip_mpp", "cbip_sam")
    try:
        with db() as conn:
            meta = {
                r["key"]: r["value"]
                for r in conn.execute("SELECT key, value FROM meta")
            }
            counts = {
                tbl: conn.execute(
                    f"SELECT COUNT(*) AS n FROM {tbl}"
                ).fetchone()["n"]
                for tbl in tables
            }
            if _has_cbip(conn):
                for tbl in cbip_tables:
                    counts[tbl] = conn.execute(
                        f"SELECT COUNT(*) AS n FROM {tbl}"
                    ).fetchone()["n"]
    except sqlite3.Error as exc:
        print(f"[sam-mcp] WARNING: could not read DB counts from {DB_PATH}: {exc}",
              file=sys.stderr, flush=True)
        return

    built = meta.get("built_at") or "unknown"
    print(f"[sam-mcp] DB {DB_PATH} (built: {built})", file=sys.stderr, flush=True)
    summary = ", ".join(f"{tbl}={n}" for tbl, n in counts.items())
    print(f"[sam-mcp] row counts: {summary}", file=sys.stderr, flush=True)


async def _status_handler(request) -> JSONResponse:
    """HTTP GET /status — DB build metadata and per-table row counts."""
    try:
        with db() as conn:
            meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
            counts: dict[str, int] = {}
            for tbl in ("amp", "ampp", "dmpp", "amp_ingredient",
                        "substance", "atc", "pharma_form", "route"):
                counts[tbl] = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
            for tbl in ("vtm", "reimbursement", "reimbursement_criterion",
                        "nonmedicinal", "compounding_ingredient",
                        "legal_basis", "legal_reference", "legal_text"):
                if _has_table(conn, tbl):
                    counts[tbl] = conn.execute(
                        f"SELECT COUNT(*) AS n FROM {tbl}"
                    ).fetchone()["n"]
            if _has_cbip(conn):
                for tbl in ("cbip_mp", "cbip_mpp", "cbip_hyr", "cbip_innm", "cbip_sam"):
                    counts[tbl] = conn.execute(
                        f"SELECT COUNT(*) AS n FROM {tbl}"
                    ).fetchone()["n"]
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return JSONResponse({"meta": meta, "counts": counts})


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="SAM MCP server")
    p.add_argument("--http", action="store_true",
                   help="Serve over streamable-http instead of stdio")
    p.add_argument("--host",
                   default=os.environ.get("SAM_HOST", "0.0.0.0"),
                   help="Bind address for --http (default: 0.0.0.0, LAN-accessible)")
    p.add_argument("--port", type=int,
                   default=int(os.environ.get("SAM_PORT", "8000")),
                   help="Port for --http (default: 8000)")
    p.add_argument("--allowed-hosts", default=None,
                   help="Comma-separated allowed Host headers for reverse-proxy use "
                        "(e.g. 'sam.example.com,localhost'). Default: no restriction.")
    p.add_argument("--behind-proxy", action="store_true",
                   help="Trust X-Forwarded-* headers from a reverse proxy (e.g. NPM). "
                        "Implied when --allowed-hosts is set.")
    args = p.parse_args()

    if not DB_PATH.exists():
        print(f"[sam-mcp] FATAL: database not found at {DB_PATH}. "
              f"Run: python -m sam_mcp.etl --data <xml_dir> --db {DB_PATH}",
              file=sys.stderr, flush=True)
        raise SystemExit(1)

    _log_startup_counts()

    if args.http:
        import uvicorn
        from starlette.routing import Route

        # FastMCP auto-enables DNS-rebinding protection when it's constructed,
        # because it sees the default localhost host and locks the allow-list
        # to 127.0.0.1/localhost. Behind a reverse proxy the real Host header
        # (e.g. sam.reichert.be) is then rejected with HTTP 421. The proxy /
        # LAN is our trust boundary, so disable that check for HTTP serving;
        # --allowed-hosts below is the opt-in Host allow-list.
        mcp.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        )

        # Inject /status into the MCP app's own router so its lifespan
        # (task group init) is preserved — wrapping in a new Starlette app
        # would orphan the lifespan and cause a 500 on /mcp.
        app = mcp.streamable_http_app()
        app.router.routes.insert(0, Route("/status", _status_handler))

        print(f"[sam-mcp] HTTP listening on http://{args.host}:{args.port}/mcp",
              flush=True)
        if args.allowed_hosts or args.behind_proxy:
            from starlette.middleware.trustedhost import TrustedHostMiddleware
            if args.allowed_hosts:
                hosts = [h.strip() for h in args.allowed_hosts.split(",") if h.strip()]
                print(f"[sam-mcp] Allowed hosts: {hosts}", flush=True)
                app = TrustedHostMiddleware(app, allowed_hosts=hosts)
            uvicorn.run(app, host=args.host, port=args.port,
                        proxy_headers=True, forwarded_allow_ips="*")
        else:
            uvicorn.run(app, host=args.host, port=args.port)
    else:
        print("[sam-mcp] stdio mode — ready for JSON-RPC on stdin/stdout",
              file=sys.stderr, flush=True)
        mcp.run()


if __name__ == "__main__":
    main()
