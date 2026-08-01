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

import hmac
import html
import logging
import os
import sqlite3
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response

DB_PATH = Path(os.environ.get("SAM_DB", "db/sam.db"))
ASSETS = Path(__file__).resolve().parent  # favicon.svg / favicon.ico ship with the package

mcp = FastMCP("sam")

# --- log capture -----------------------------------------------------------
# Nothing in this process keeps log history: every diagnostic is a bare print()
# straight to stderr, which is what `docker logs` shows. To serve the same
# stream over HTTP (/log) we tee stdout/stderr into an in-memory ring buffer.

try:
    _LOG_CAPACITY = max(1, int(os.environ.get("SAM_LOG_LINES", "2000")))
except ValueError:
    _LOG_CAPACITY = 2000
_LOG_BUFFER: deque[tuple[float, str]] = deque(maxlen=_LOG_CAPACITY)  # (timestamp, line)
_LOG_LOCK = threading.Lock()


class _TeeStream:
    """Write-through wrapper that also records completed lines in _LOG_BUFFER."""

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self._partial = ""

    def write(self, s: str) -> int:
        # Write through first — capture must never break real output.
        n = self._wrapped.write(s)
        try:
            self._capture(s)
        except Exception:
            pass
        return n

    def _capture(self, s: str) -> None:
        # print() emits the text and the trailing newline as separate writes,
        # so hold back the tail until a newline actually arrives.
        self._partial += s
        if "\n" not in self._partial:
            return
        *lines, self._partial = self._partial.split("\n")
        now = time.time()
        with _LOG_LOCK:
            for line in lines:
                # Skip uvicorn's own access lines for /log and its favicon,
                # otherwise the page's auto-refresh would fill the buffer with
                # records of itself. Access format:
                # 1.2.3.4:5678 - "GET /log HTTP/1.1" 200 OK
                if '"GET /log' in line or '"GET /favicon' in line:
                    continue
                _LOG_BUFFER.append((now, line))

    def flush(self) -> None:
        self._wrapped.flush()

    def __getattr__(self, item):
        # encoding, fileno, isatty, writable, ... — uvicorn and
        # logging.StreamHandler expect a real file-like object.
        return getattr(self._wrapped, item)


def _install_log_capture() -> None:
    """Tee stdout/stderr into the ring buffer served by /log. Idempotent."""
    if not isinstance(sys.stdout, _TeeStream):
        sys.stdout = _TeeStream(sys.stdout)
    if not isinstance(sys.stderr, _TeeStream):
        sys.stderr = _TeeStream(sys.stderr)

    # FastMCP calls logging.basicConfig() from its constructor, which runs at
    # import time (`mcp = FastMCP("sam")` above) — long before this. That root
    # handler captured the pre-tee stderr, so without re-pointing it the mcp
    # SDK's own lines reach `docker logs` but never the buffer.
    for handler in logging.root.handlers:
        stream = getattr(handler, "stream", None)
        if not hasattr(handler, "setStream"):
            continue
        if stream is sys.stderr._wrapped:
            handler.setStream(sys.stderr)
        elif stream is sys.stdout._wrapped:
            handler.setStream(sys.stdout)


def _summarize_result(result: Any) -> str:
    # mcp.call_tool(..., convert_result=True) normalizes tool return values into
    # (content_blocks, structured_content) -- unwrap that to report item counts.
    if isinstance(result, tuple) and len(result) == 2:
        _content, structured = result
        if isinstance(structured, dict) and isinstance(structured.get("result"), list):
            return f"{len(structured['result'])} item(s)"
        return "1 object"
    if isinstance(result, list):
        return f"{len(result)} item(s)"
    if isinstance(result, dict):
        return "1 object"
    return type(result).__name__


async def _logged_call_tool(name: str, arguments: dict[str, Any]):
    """Log every tool call to stderr, then delegate to FastMCP's own dispatcher.

    FastMCP's built-in handler only logs "Processing request of type
    CallToolRequest" (no tool name/args), so this re-registers the low-level
    CallToolRequest handler -- the one choke point every call passes through
    for both stdio and --http transports. If a future mcp SDK upgrade changes
    FastMCP._setup_handlers(), re-check this still overrides the same hook.
    """
    start = time.perf_counter()
    try:
        result = await mcp.call_tool(name, arguments)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - start) * 1000
        print(f"[sam-mcp] tool={name} args={arguments} FAILED in {elapsed_ms:.0f}ms: {exc}",
              file=sys.stderr, flush=True)
        raise
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"[sam-mcp] tool={name} args={arguments} -> {_summarize_result(result)} in {elapsed_ms:.0f}ms",
          file=sys.stderr, flush=True)
    return result


mcp._mcp_server.call_tool(validate_input=False)(_logged_call_tool)


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
        " medicine_type, company, vmp_code FROM amp WHERE code = ?",
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


_SEARCH_ARMS: dict[str, str] = {
    "amp": """
        SELECT f.amp_code AS entity_key, a.name_fr, a.name_nl
          FROM amp_fts f JOIN amp a ON a.code = f.amp_code
         WHERE amp_fts MATCH ?
         ORDER BY rank LIMIT ?
    """,
    "substance": """
        SELECT substance_code AS entity_key, name_fr, name_nl
          FROM substance_fts
         WHERE substance_fts MATCH ?
         ORDER BY rank LIMIT ?
    """,
    "nonmedicinal": """
        SELECT code AS entity_key, name_fr, name_nl
          FROM nonmedicinal_fts
         WHERE nonmedicinal_fts MATCH ?
         ORDER BY rank LIMIT ?
    """,
    "atc": """
        SELECT code AS entity_key, description
          FROM atc_fts
         WHERE atc_fts MATCH ?
         ORDER BY rank LIMIT ?
    """,
    "impp": """
        SELECT cnk AS entity_key, name
          FROM impp_fts
         WHERE impp_fts MATCH ?
         ORDER BY rank LIMIT ?
    """,
    # cbip_mp_fts is product-level (mpcv), but get_cbip_notes and every other
    # CBIP-aware tool key off a pack-level CNK (cbip_mpp.mppcv). Resolve to
    # one representative pack CNK per product so the result is drillable.
    "cbip_mp": """
        SELECT MIN(p.mppcv) AS entity_key, m.mpnm AS name
          FROM cbip_mp_fts f
          JOIN cbip_mp  m ON m.mpcv = f.mpcv
          JOIN cbip_mpp p ON p.mpcv = m.mpcv
         WHERE cbip_mp_fts MATCH ?
         GROUP BY f.mpcv, m.mpnm
         ORDER BY MIN(f.rank) LIMIT ?
    """,
}

_SEARCH_COUNT_SQL: dict[str, str] = {
    "amp": "SELECT COUNT(*) FROM amp_fts WHERE amp_fts MATCH ?",
    "substance": "SELECT COUNT(*) FROM substance_fts WHERE substance_fts MATCH ?",
    "nonmedicinal": "SELECT COUNT(*) FROM nonmedicinal_fts WHERE nonmedicinal_fts MATCH ?",
    "atc": "SELECT COUNT(*) FROM atc_fts WHERE atc_fts MATCH ?",
    "impp": "SELECT COUNT(*) FROM impp_fts WHERE impp_fts MATCH ?",
    "cbip_mp": "SELECT COUNT(*) FROM cbip_mp_fts WHERE cbip_mp_fts MATCH ?",
}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def search_everything(query: str, types: list[str] | None = None,
                       limit: int = 10) -> dict[str, Any]:
    """
    Fuzzy discoverability search across medicines, substances, nonmedicinal
    products, ATC codes, imported medicines (IMPP) and CBIP products in one
    call. Matches French, Dutch and English text with diacritics ignored.

    Use this when you do not know which entity type you are looking for,
    when a name might be misspelled, when a brand name might be Dutch-only,
    or to check whether something exists in the database at all. It is a
    ROUTER, not a terminus: take the entity_key from a hit and call
    get_medicine / get_ingredients / find_by_substance / search_nonmedicinal /
    get_atc / find_imported / get_cbip_notes for authoritative details — do
    not answer directly from these fuzzy matches.

    Do NOT use this instead of search_medicine when the medicine name is
    already known: search_medicine's ranking is specific to AMPs, while this
    tool spreads results across unrelated entity types and will be noisier.

    Parameters
    ----------
    query : free text to search for
    types : subset of {'amp', 'substance', 'nonmedicinal', 'atc', 'impp',
            'cbip_mp'} to search. Defaults to all types available in this DB
            build ('cbip_mp' is skipped automatically if CBIP wasn't loaded).
    limit : max hits returned PER TYPE, not total (default 10, max 50).
            counts_by_type reports the true match count even when truncated,
            so e.g. 200 nonmedicinal matches are visible even if only 10 show.

    Returns {"counts_by_type": {type: total_matches}, "results": {type: [hits]}}.
    Only types that were actually searched appear in either dict. Hit shape
    varies by type:
      - amp / substance / nonmedicinal: {entity_key, name_fr, name_nl}
      - atc: {entity_key, description}
      - impp: {entity_key, name}
      - cbip_mp: {entity_key, name} — entity_key is one representative pack
        CNK for the product; call get_cbip_notes for the full pack list.
    """
    q = _fts_query(query)
    lim = max(1, min(limit, 50))
    requested = types if types else list(_SEARCH_ARMS)
    unknown = [t for t in requested if t not in _SEARCH_ARMS]
    if unknown:
        raise ValueError(f"Unknown type(s) {unknown}; valid types are {list(_SEARCH_ARMS)}")

    counts: dict[str, int] = {}
    results: dict[str, list[dict[str, Any]]] = {}
    with db() as conn:
        for t in requested:
            if t == "cbip_mp" and not _has_cbip(conn):
                continue
            count = conn.execute(_SEARCH_COUNT_SQL[t], (q,)).fetchone()[0]
            counts[t] = count
            results[t] = (
                [_row_to_dict(r) for r in conn.execute(_SEARCH_ARMS[t], (q, lim)).fetchall()]
                if count else []
            )

    return {"counts_by_type": counts, "results": results}


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
                   p.status, p.delivery_modus, p.legal_basis_fr, p.legal_basis_nl,
                   p.ex_factory_price
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

    Each entry carries `strength_missing: true` when strength_quantity is
    NULL -- this happens when the SAM source export itself has no <Strength>
    for that substance (e.g. AMP SAM663104-00 / Eliquis 1.5 mg, whose active
    ingredient is declared with no strength upstream). Never inferred from
    the medicine's name; treat it as genuinely unknown, not zero.
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
    results = [_row_to_dict(r) for r in rows]
    for entry in results:
        entry["strength_missing"] = entry["strength_quantity"] is None
    return results


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
def aggregate_substances(
    name: str = "",
    min_cnk: int = 1,
    max_cnk: int = 99999,
    atc: str = "",
    limit: int = 200,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    Aggregate all active substances by CNK count (one SQL pass — no per-substance loops).

    This is the answer to questions like:
      - "Which substances have exactly one CNK?"      → min_cnk=1, max_cnk=1
      - "Which substances have more than 10 CNKs?"   → min_cnk=11
      - "How many CNKs does paracetamol have?"       → name='paracetamol'
      - "All PPIs with their CNK count?"             → atc='A02BC'
      - "IPP à CNK unique?"                          → atc='A02BC', min_cnk=1, max_cnk=1

    Parameters
    ----------
    name     : optional partial match on French or Dutch substance name (case-insensitive)
    min_cnk  : minimum number of distinct CNKs (inclusive, default 1)
    max_cnk  : maximum number of distinct CNKs (inclusive, default unbounded)
    atc      : ATC code prefix to restrict to a therapeutic class (e.g. 'A02BC' for PPIs,
               'C09' for RAAS drugs). Requires the DB to have been built after the
               amp_atc table was introduced. Returns empty list with a warning key if
               the table is absent.
    limit    : max rows returned (default 200, max 2000)
    offset   : pagination offset

    Returns list of {substance_code, name_fr, name_nl, cnk_count, amp_count},
    sorted by cnk_count ASC then name_fr ASC.
    """
    min_cnk = max(1, min_cnk)
    max_cnk = max(min_cnk, max_cnk)
    limit   = max(1, min(limit, 2000))
    offset  = max(0, offset)
    atc     = atc.strip().upper()
    name    = name.strip()

    with db() as conn:
        if atc and not _has_table(conn, "amp_atc"):
            return [{"warning": "amp_atc table not found — rebuild the DB to enable ATC filtering"}]

        # Build WHERE / JOIN clauses dynamically based on active filters.
        joins  = ["JOIN dmpp d ON d.amp_code = ai.amp_code",
                  "LEFT JOIN substance s ON s.code = ai.substance_code"]
        wheres = ["ai.type = 'ACTIVE_SUBSTANCE'"]
        params: list = []

        if atc:
            joins.append("JOIN amp_atc aa ON aa.amp_code = ai.amp_code")
            wheres.append("aa.atc_code LIKE ?")
            params.append(atc + "%")

        if name:
            wheres.append(
                "(COALESCE(s.name_fr, ai.substance_name_fr) LIKE ?"
                " OR COALESCE(s.name_nl, ai.substance_name_nl) LIKE ?)"
            )
            pat = f"%{name}%"
            params.extend([pat, pat])

        params.extend([min_cnk, max_cnk, limit, offset])

        sql = f"""
            SELECT ai.substance_code,
                   COALESCE(s.name_fr, ai.substance_name_fr) AS name_fr,
                   COALESCE(s.name_nl, ai.substance_name_nl) AS name_nl,
                   COUNT(DISTINCT d.cnk)      AS cnk_count,
                   COUNT(DISTINCT ai.amp_code) AS amp_count
              FROM amp_ingredient ai
              {" ".join(joins)}
             WHERE {" AND ".join(wheres)}
             GROUP BY ai.substance_code
            HAVING cnk_count >= ? AND cnk_count <= ?
             ORDER BY cnk_count ASC, name_fr ASC
             LIMIT ? OFFSET ?
        """
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_substance_cnks(substance_code: str) -> list[dict[str, Any]]:
    """
    List every CNK that contains a given active substance, with pack and medicine details.

    Use this after aggregate_substances to drill into a specific substance.
    The substance_code comes from the 'substance_code' field in aggregate_substances results
    or from find_by_substance / get_ingredients.

    Returns list of {cnk, amp_code, amp_name_fr, amp_name_nl, amp_status,
    pack_display_fr, pack_display_nl, strength_quantity, strength_unit},
    sorted by amp_name_fr then cnk.
    """
    substance_code = substance_code.strip()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT d.cnk,
                   a.code             AS amp_code,
                   a.name_fr          AS amp_name_fr,
                   a.name_nl          AS amp_name_nl,
                   a.status           AS amp_status,
                   p.pack_display_fr,
                   p.pack_display_nl,
                   ai.strength_quantity,
                   ai.strength_unit
              FROM amp_ingredient ai
              JOIN amp  a ON a.code          = ai.amp_code
              JOIN dmpp d ON d.amp_code      = a.code
              LEFT JOIN ampp p ON p.cti_extended = d.cti_extended
             WHERE ai.substance_code = ?
               AND ai.type = 'ACTIVE_SUBSTANCE'
             ORDER BY a.name_fr, d.cnk
            """,
            (substance_code,),
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


def _reimbursement_for_cnks(
    conn: sqlite3.Connection, cnks: list[str], as_of: str, include_history: bool
) -> dict[str, list[dict[str, Any]]]:
    """Batched, temporally-filtered reimbursement lookup for 1..N CNKs.

    Issues exactly two queries (reimbursement rows, then their criteria)
    regardless of how many CNKs are requested -- never one query per CNK.
    Every returned tranche carries `is_current`, true when it is valid at
    `as_of` (`valid_from <= as_of` and `valid_to` is NULL or `>= as_of`).
    When `include_history` is False, non-current tranches are dropped.

    Returns {cnk: [tranche, ...]}; a CNK with no reimbursement record at all,
    or none matching after filtering, is simply absent from the dict.
    """
    placeholders = ",".join("?" * len(cnks))
    rows = conn.execute(
        f"SELECT cnk, delivery_environment, valid_from, valid_to, legal_reference,"
        f" temporary, is_reference, flat_rate_system,"
        f" reimbursement_price, reference_price,"
        f" pricing_unit_qty, pricing_unit_fr, pricing_unit_nl"
        f" FROM reimbursement WHERE cnk IN ({placeholders})"
        f" ORDER BY cnk, valid_from",
        cnks,
    ).fetchall()
    if not rows:
        return {}

    criteria_rows = conn.execute(
        f"SELECT cnk, delivery_environment, valid_from, category, code,"
        f" description_fr, description_nl"
        f" FROM reimbursement_criterion WHERE cnk IN ({placeholders})",
        cnks,
    ).fetchall()
    criteria_by_tranche: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for r in criteria_rows:
        key = (r["cnk"], r["delivery_environment"], r["valid_from"])
        criteria_by_tranche.setdefault(key, []).append({
            "category":       r["category"],
            "code":           r["code"],
            "description_fr": r["description_fr"],
            "description_nl": r["description_nl"],
        })

    by_cnk: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        is_current = row["valid_from"] <= as_of and (
            row["valid_to"] is None or row["valid_to"] >= as_of
        )
        if not include_history and not is_current:
            continue
        entry = _row_to_dict(row)
        entry["pricing_unit"] = {
            "quantity":  entry.pop("pricing_unit_qty"),
            "label_fr":  entry.pop("pricing_unit_fr"),
            "label_nl":  entry.pop("pricing_unit_nl"),
        }
        entry["is_current"] = is_current
        entry["criteria"] = criteria_by_tranche.get(
            (row["cnk"], row["delivery_environment"], row["valid_from"]), []
        )
        by_cnk.setdefault(row["cnk"], []).append(entry)
    return by_cnk


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_reimbursement(
    cnk: str, as_of: str | None = None, include_history: bool = False
) -> list[dict[str, Any]] | None:
    """
    Return reimbursement data for a Belgian medicine by CNK.
    Includes base/reference prices, flat-rate flag, delivery environment and
    reimbursement criteria (category + description).

    By default only the tranche(s) valid on `as_of` (ISO date, default today)
    are returned, each marked `is_current: true` -- older or not-yet-started
    tranches (e.g. one that lapsed years ago) are hidden, so a caller reading
    `reimbursement_price` cannot accidentally quote a dead price. Pass
    `include_history=True` to get every tranche ever recorded, current or
    not, each still carrying `is_current` relative to `as_of`.

    Returns None if the CNK has no reimbursement record at all, or none
    matching the current filter.

    Looking up several CNKs at once? get_pack_overview batches this together
    with get_cbip_notes in a single call instead of one get_reimbursement
    call per CNK.
    """
    cnk = cnk.strip()
    as_of_date = as_of.strip() if as_of else date.today().isoformat()
    with db() as conn:
        results = _reimbursement_for_cnks(conn, [cnk], as_of_date, include_history).get(cnk)
    return results if results else None


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
def find_imported(query: str, limit: int = 50) -> list[dict[str, Any]]:
    """
    Search imported medicinal products (IMPP) by product name, CNK, or active substance.

    IMPP are medicines brought in from abroad for compassionate use or when a
    standard Belgian product is unavailable.  Unlike regular AMPs they have a
    plain-text name (not multilang), a free-text strength/pack-size description,
    and a country-of-origin code.  They carry a Belgian CNK like any dispensed
    product.

    Returns list of {id, cnk, name, country, strength, pack_size,
    pharma_form_fr, pharma_form_nl, valid_from,
    substances: [{substance_code, name_fr, name_nl}],
    routes: [{route_code, route_fr, route_nl}]}.
    Returns empty list if the IMPP table is absent (requires DB rebuild).
    """
    raw = query.strip()
    pat = f"%{raw}%"
    with db() as conn:
        if not _has_table(conn, "impp"):
            return []
        rows = conn.execute(
            """
            SELECT DISTINCT i.id, i.cnk, i.name, i.country,
                   i.strength, i.pack_size,
                   i.pharma_form_code, i.pharma_form_fr, i.pharma_form_nl,
                   i.valid_from
              FROM impp i
              LEFT JOIN impp_substance s ON s.impp_id = i.id
             WHERE i.name LIKE ?
                OR i.cnk  = ?
                OR s.name_fr LIKE ?
                OR s.name_nl LIKE ?
             ORDER BY i.name
             LIMIT ?
            """,
            (pat, raw, pat, pat, max(1, min(limit, 200))),
        ).fetchall()
        results = []
        for row in rows:
            entry = _row_to_dict(row)
            entry["substances"] = [_row_to_dict(r) for r in conn.execute(
                "SELECT substance_code, name_fr, name_nl"
                " FROM impp_substance WHERE impp_id = ? ORDER BY substance_code",
                (row["id"],),
            ).fetchall()]
            entry["routes"] = [_row_to_dict(r) for r in conn.execute(
                "SELECT route_code, route_fr, route_nl"
                " FROM impp_route WHERE impp_id = ? ORDER BY route_code",
                (row["id"],),
            ).fetchall()]
            results.append(entry)
    return results


def _legal_text_root(text_key: str, rows_by_key: dict[str, sqlite3.Row]) -> str:
    """Walk parent_text_key up to the version root (parent_text_key IS NULL)."""
    seen: set[str] = set()
    while text_key not in seen:
        seen.add(text_key)
        row = rows_by_key.get(text_key)
        if row is None or row["parent_text_key"] is None:
            return text_key
        text_key = row["parent_text_key"]
    return text_key  # cycle guard; should not happen on well-formed data


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _legal_text_sort_key(version: dict[str, Any]) -> tuple[str, int]:
    return (version["valid_from"] or "", _safe_int(version["root_text_key"]))


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_legal_text(text_key: str, latest_only: bool = True) -> dict[str, Any] | None:
    """
    Fetch reimbursement law text, by either of two key formats:

    - a legal_text.text_key primary key (e.g. '40869') - returns that single
      text node: content in French and Dutch, text type, sequence number,
      validity period and the parent legal reference/basis for context.
    - a legal_reference path, i.e. the exact string found in
      get_reimbursement's `legal_reference` field (e.g. 'RD20180201-IV-2630000',
      shaped "{legal_basis.key}-{chapter}-{paragraph}") - resolves the
      chapter/paragraph and returns its text as `versions`: one entry per
      historical revision of that paragraph, each `{root_text_key,
      valid_from, valid_to, texts: [...]}` with `texts` in sequence order.
      A paragraph amended over the years accumulates several parallel text
      trees in legal_text (one root per revision, each root's own subtree
      renumbering sequence_nr from 1); a revision's texts are grouped by
      its root's (valid_from, valid_to) validity window rather than by the
      root text_key itself, because some paragraphs are exported as several
      flat, parentless sibling nodes sharing one window instead of a single
      tree -- keying on text_key would fragment one such revision into one
      spurious single-paragraph "version" per sibling. Grouping by window
      also keeps a 2020 revision from interleaving with the 2024 one that
      superseded it, since the two windows never overlap.
      `latest_only=True` (default) returns only the most recent version;
      set it to False to see the full amendment history. "Most recent" is
      determined by `valid_from` descending (every legal_text row in this
      DB build has it populated); ties -- or a future build where it is
      NULL -- fall back to root_text_key descending, since higher keys are
      assigned later.
      Note: some chapters carry no conditional text at all - e.g. Chapter I
      ('...-I-...') is the base reimbursement category with no special
      conditions, so each version's `texts` is legitimately an empty list.
      That is not a lookup failure.

    Returns None only when the key matches neither a legal_text.text_key nor
    a resolvable legal_basis in a 3-part path.
    """
    key = text_key.strip()
    parts = key.split("-")
    with db() as conn:
        if len(parts) == 3:
            basis_key, chapter, paragraph = parts
            basis_row = conn.execute(
                "SELECT key FROM legal_basis WHERE key = ?", (basis_key,)
            ).fetchone()
            if basis_row is not None:
                ref_row = conn.execute(
                    "SELECT title_fr, title_nl, type FROM legal_reference"
                    " WHERE basis_key = ? AND ref_key = ? AND parent_ref_key = ?",
                    (basis_key, paragraph, chapter),
                ).fetchone()
                rows = conn.execute(
                    "SELECT basis_key, ref_key, text_key, parent_text_key,"
                    " content_fr, content_nl, type, sequence_nr, valid_from, valid_to"
                    " FROM legal_text WHERE basis_key = ? AND ref_key = ?"
                    " ORDER BY sequence_nr",
                    (basis_key, paragraph),
                ).fetchall()

                # Group by each node's root VALIDITY WINDOW, not by the root
                # text_key itself: some paragraphs are exported as several
                # flat, parentless sibling nodes (no shared root text_key)
                # that all belong to one and the same revision. Keying on
                # text_key would fragment that single revision into one
                # spurious one-paragraph "version" per sibling -- confirmed
                # on RD20180201-IV-12410000 (Revlimid's chapter IV
                # condition), which has 22 parentless siblings all sharing
                # one window; grouping by root text_key split it into 22
                # versions of 1 text each, so latest_only=True returned only
                # the last paragraph and silently dropped the other 21.
                rows_by_key = {r["text_key"]: r for r in rows}
                windows_by_root: dict[str, tuple[str | None, str | None]] = {}
                grouped: dict[tuple[str | None, str | None], list[sqlite3.Row]] = {}
                roots_by_window: dict[tuple[str | None, str | None], set[str]] = {}
                for r in rows:
                    root = _legal_text_root(r["text_key"], rows_by_key)
                    root_row = rows_by_key.get(root)
                    window = windows_by_root.get(root)
                    if window is None:
                        window = (
                            (root_row["valid_from"] if root_row else None),
                            (root_row["valid_to"] if root_row else None),
                        )
                        windows_by_root[root] = window
                    grouped.setdefault(window, []).append(r)
                    roots_by_window.setdefault(window, set()).add(root)

                versions = []
                for (valid_from, valid_to), group_rows in grouped.items():
                    # Representative id for sorting/display when a window is
                    # shared by several flat sibling roots: the highest one,
                    # since higher keys are assigned later (see
                    # _legal_text_sort_key).
                    representative_root = max(roots_by_window[(valid_from, valid_to)], key=_safe_int)
                    versions.append({
                        "root_text_key": representative_root,
                        "valid_from": valid_from,
                        "valid_to":   valid_to,
                        "texts": sorted(
                            (_row_to_dict(r) for r in group_rows),
                            key=lambda t: _safe_int(t["sequence_nr"]),
                        ),
                    })
                versions.sort(key=_legal_text_sort_key, reverse=True)
                if latest_only and versions:
                    versions = versions[:1]

                return {
                    "basis_key": basis_key,
                    "chapter": chapter,
                    "paragraph": paragraph,
                    "reference_title_fr": ref_row["title_fr"] if ref_row else None,
                    "reference_title_nl": ref_row["title_nl"] if ref_row else None,
                    "versions": versions,
                }
        row = conn.execute(
            "SELECT basis_key, ref_key, text_key, parent_text_key,"
            " content_fr, content_nl, type, sequence_nr, valid_from, valid_to"
            " FROM legal_text WHERE text_key = ?",
            (key,),
        ).fetchone()
    return _row_to_dict(row) if row else None


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _has_cbip(conn: sqlite3.Connection) -> bool:
    return _has_table(conn, "cbip_mpp")


_CORE_TABLES = ("amp", "ampp", "dmpp", "amp_ingredient", "substance", "atc",
                "pharma_form", "route")
_OPTIONAL_TABLES = ("amp_atc", "vtm", "reimbursement", "reimbursement_criterion",
                    "nonmedicinal", "compounding_ingredient",
                    "legal_basis", "legal_reference", "legal_text",
                    "impp", "impp_substance", "impp_route")
_CBIP_TABLES = ("cbip_mp", "cbip_mpp", "cbip_hyr", "cbip_innm", "cbip_sam")


def _collect_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Row counts for every table this DB build has, core + optional + CBIP."""
    counts = {
        tbl: conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
        for tbl in _CORE_TABLES
    }
    for tbl in _OPTIONAL_TABLES:
        if _has_table(conn, tbl):
            counts[tbl] = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
    if _has_cbip(conn):
        for tbl in _CBIP_TABLES:
            counts[tbl] = conn.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
    return counts


_CBIP_PRICE_FIELDS = ("public_price", "index", "rema", "remw")


def _flag_cbip_price_placeholder(result: dict[str, Any]) -> None:
    """Detect CBIP's zero-price placeholder and null it out, in place.

    ~19% of cbip_mpp packs carry public_price=0 together with index/rema/remw
    all zero -- verified DB-wide (1670/8747 packs), not limited to parallel
    imports: the same all-zero pattern shows up on e.g. reimbursed
    lenalidomide generics, which certainly have a real public price. This is
    never a genuine EUR 0.00 in the Belgian public-price system; treat it as
    "CBIP does not track pricing for this pack" and flag it explicitly rather
    than let a raw 0.0 read as "free of charge". No-op when public_price is
    already None (e.g. product_level coverage, which nulls it separately).
    """
    result["cbip_price_placeholder"] = result.get("public_price") == 0
    if result["cbip_price_placeholder"]:
        for field in _CBIP_PRICE_FIELDS:
            result[field] = None


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_cbip_notes(cnk: str) -> dict[str, Any] | None:
    """
    Return CBIP/BCFI editorial commentary for a Belgian medicine, identified
    by CNK. Includes the therapeutic chapter (title, introduction, positioning),
    product-level notes/positioning, and the active substances as listed in
    the repertoire.

    The result always contains a ``coverage`` field:
    - ``"pack_level"``: the CNK has a direct entry in the CBIP pack table;
      all pack-specific fields (public_price, index, rema, remw, pack_name,
      galenic_form, law, ssecr) are populated.
    - ``"product_level"``: the CNK has no direct CBIP pack entry (e.g.
      re-coded 77xxxxx CNKs) but a sibling pack for the same SAM AMP was
      found; product-level editorial data is returned and pack-specific
      fields are null.

    The result also always contains ``cbip_price_placeholder``: true when
    CBIP's own pack record has public_price = 0 (about 19% of pack_level
    CBIP packs, e.g. hospital-restricted specialties and some parallel
    imports -- CBIP simply does not track a public price for these, it is
    never a genuine EUR 0.00). When true, public_price/index/rema/remw are
    all returned as null instead of a raw, misleading 0.0 -- treat this the
    same way as strength_missing: a genuine unknown, never zero.

    Returns None if neither a pack-level nor a product-level match exists
    (the CBIP curates a subset of all SAM medicines).

    Looking up several CNKs at once? get_pack_overview batches this together
    with get_reimbursement in a single call instead of one get_cbip_notes
    call per CNK.
    """
    cnk = cnk.strip()
    with db() as conn:
        if not _has_cbip(conn):
            return None

        # --- primary path: direct pack-level match ---
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

        if head is not None:
            result = _row_to_dict(head)
            result["coverage"] = "pack_level"
            substances_cnk = cnk
        else:
            # --- fallback: product-level match via SAM AMP siblings ---
            # Resolve CNK → amp_code via dmpp, then find a sibling CNK that
            # does have a cbip_mpp entry and use its mpcv to pull product data.
            head = conn.execute(
                """
                SELECT m.mpcv,
                       m.mpnm                     AS product_name,
                       m.note                     AS product_note,
                       m.pos                      AS product_positioning,
                       m.bt, m.orphan, m.narcotic, m.specrules,
                       h.hyrcv,
                       h.hyr                      AS chapter_code,
                       h.ti                       AS chapter_title,
                       h.intro                    AS chapter_intro,
                       h.pos                      AS chapter_positioning,
                       p.mppcv                    AS _canon_mppcv
                  FROM dmpp d_req
                  JOIN dmpp     d_sib ON d_sib.amp_code = d_req.amp_code
                  JOIN cbip_mpp p     ON p.mppcv        = d_sib.cnk
                  JOIN cbip_mp  m     ON m.mpcv         = p.mpcv
             LEFT JOIN cbip_hyr h     ON h.hyrcv        = m.hyrcv
                 WHERE d_req.cnk = ?
                 LIMIT 1
                """,
                (cnk,),
            ).fetchone()

            if head is None:
                return None

            result = _row_to_dict(head)
            substances_cnk = result.pop("_canon_mppcv")
            # Pack-specific fields have no meaning at product level
            for field in ("mppcv", "pack_name", "galenic_form", "public_price",
                          "law", "ssecr", "index", "rema", "remw"):
                result[field] = None
            result["coverage"] = "product_level"

        # Active substances — use canonical pack CNK (same product either way)
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
            (substances_cnk,),
        ).fetchall()]
        result["substances"] = substances
        _flag_cbip_price_placeholder(result)
        return result


def _pack_identities(conn: sqlite3.Connection, cnks: list[str]) -> dict[str, dict[str, Any]]:
    """Batched identity lookup: one query for all requested CNKs, not one per CNK."""
    placeholders = ",".join("?" * len(cnks))
    rows = conn.execute(
        f"""
        SELECT d.cnk, a.code AS amp_code, a.name_fr, a.name_nl, a.company, a.status,
               c.pharma_form_fr, c.pharma_form_nl, c.route_fr, c.route_nl,
               p.pack_display_fr, p.pack_display_nl, p.delivery_modus, p.ex_factory_price,
               (SELECT COUNT(*) FROM amp_component ac WHERE ac.amp_code = a.code)
                                                        AS component_count
          FROM dmpp d
          JOIN amp a ON a.code = d.amp_code
     LEFT JOIN ampp p ON p.cti_extended = d.cti_extended
     LEFT JOIN amp_component c ON c.amp_code = a.code AND c.seq = 1
         WHERE d.cnk IN ({placeholders})
        """,
        cnks,
    ).fetchall()
    identities = {}
    for row in rows:
        entry = _row_to_dict(row)
        entry["multi_component"] = entry["component_count"] > 1
        identities[row["cnk"]] = entry
    return identities


def _pack_substances(conn: sqlite3.Connection, amp_codes: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Batched active-substance lookup keyed by amp_code, one query for every AMP involved."""
    placeholders = ",".join("?" * len(amp_codes))
    rows = conn.execute(
        f"""
        SELECT amp_code, substance_code, substance_name_fr, substance_name_nl,
               strength_operator, strength_quantity, strength_unit
          FROM amp_ingredient
         WHERE amp_code IN ({placeholders}) AND type = 'ACTIVE_SUBSTANCE'
         ORDER BY amp_code, component_seq, rank
        """,
        amp_codes,
    ).fetchall()
    by_amp: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        entry = _row_to_dict(row)
        entry["strength_missing"] = entry["strength_quantity"] is None
        by_amp.setdefault(entry.pop("amp_code"), []).append(entry)
    return by_amp


_CBIP_OVERVIEW_HEAD_SQL = """
    SELECT p.mppcv,
           p.pupr AS public_price, p."index", p.rema, p.remw, p.law, p.ssecr,
           m.bt, m.orphan, m.narcotic, m.specrules,
           h.hyr AS chapter_code, h.ti AS chapter_title, h.pos AS chapter_positioning
      FROM cbip_mpp p
      JOIN cbip_mp  m ON m.mpcv  = p.mpcv
 LEFT JOIN cbip_hyr h ON h.hyrcv = m.hyrcv
     WHERE p.mppcv IN ({placeholders})
"""


def _cbip_overview_entry(row: sqlite3.Row, coverage: str) -> dict[str, Any]:
    entry = _row_to_dict(row)
    entry.pop("mppcv", None)
    entry["flags"] = {k: entry.pop(k) for k in ("bt", "orphan", "narcotic", "specrules")}
    entry["coverage"] = coverage
    if coverage == "product_level":
        # Pack-specific fields have no meaning when resolved via a sibling pack.
        for field in ("public_price", "index", "rema", "remw", "law", "ssecr"):
            entry[field] = None
    _flag_cbip_price_placeholder(entry)
    return entry


def _pack_cbip(conn: sqlite3.Connection, cnks: list[str]) -> dict[str, dict[str, Any]]:
    """Batched CBIP lookup mirroring get_cbip_notes' pack/product-level fallback.

    A fixed number of queries regardless of len(cnks): one direct pack-level
    fetch, and -- only if some CNKs miss it -- one sibling-resolution query
    plus one more head fetch for the resolved siblings. Never one query per CNK.
    """
    if not _has_cbip(conn):
        return {}
    placeholders = ",".join("?" * len(cnks))
    direct_rows = conn.execute(
        _CBIP_OVERVIEW_HEAD_SQL.format(placeholders=placeholders), cnks
    ).fetchall()
    result = {row["mppcv"]: _cbip_overview_entry(row, "pack_level") for row in direct_rows}

    missing = [cnk for cnk in cnks if cnk not in result]
    if missing:
        m_placeholders = ",".join("?" * len(missing))
        sibling_rows = conn.execute(
            f"""
            SELECT d_req.cnk AS requested_cnk, MIN(d_sib.cnk) AS canon_cnk
              FROM dmpp d_req
              JOIN dmpp     d_sib ON d_sib.amp_code = d_req.amp_code
              JOIN cbip_mpp p     ON p.mppcv        = d_sib.cnk
             WHERE d_req.cnk IN ({m_placeholders})
             GROUP BY d_req.cnk
            """,
            missing,
        ).fetchall()
        sibling_map = {r["requested_cnk"]: r["canon_cnk"] for r in sibling_rows}
        if sibling_map:
            canon_cnks = sorted(set(sibling_map.values()))
            c_placeholders = ",".join("?" * len(canon_cnks))
            canon_rows = conn.execute(
                _CBIP_OVERVIEW_HEAD_SQL.format(placeholders=c_placeholders), canon_cnks
            ).fetchall()
            canon_by_mppcv = {row["mppcv"]: row for row in canon_rows}
            for cnk, canon_cnk in sibling_map.items():
                row = canon_by_mppcv.get(canon_cnk)
                if row is not None:
                    result[cnk] = _cbip_overview_entry(row, "product_level")
    return result


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_pack_overview(
    cnks: list[str], as_of: str | None = None, include_history: bool = False
) -> dict[str, Any]:
    """
    Merge identity, active substances, reimbursement and CBIP data for 1-50
    CNKs in a single call.

    REPLACES the get_reimbursement + get_cbip_notes sequence once the CNKs
    are already known: cross-referencing reimbursement against CBIP notes
    for N packs used to cost 2N tool calls (one get_reimbursement and one
    get_cbip_notes per CNK, one CNK at a time). This tool does it in one
    round trip regardless of N, using a fixed, small number of batched
    `WHERE cnk IN (...)` SQL queries -- never one query per CNK. Keep using
    get_reimbursement / get_cbip_notes directly for single-CNK, targeted
    lookups, or when you need the full CBIP editorial text (chapter intro,
    product notes/positioning) that this overview deliberately omits.

    Parameters
    ----------
    cnks : 1 to 50 CNKs.
    as_of : ISO date controlling reimbursement currency, default today.
        See get_reimbursement for the exact semantics.
    include_history : if True, `reimbursement` includes every tranche ever
        recorded (each still marked `is_current` relative to `as_of`)
        instead of only the currently active one(s).

    Returns {"packs": {cnk: {...}}, "not_found": [cnk, ...]}.

    Every requested CNK gets an entry in `packs`, even one that does not
    exist at all (all four sections then null, with
    `..._absent_reason: "cnk_not_found"`) -- `not_found` additionally lists
    those CNKs for a quick existence check without inspecting every section.

    Each pack entry has four sections, each paired with a
    `<section>_absent_reason` key that is null when the section is present
    and otherwise one of "cnk_not_found", "no_reimbursement_record" (known
    CNK, nothing reimbursed as of `as_of`), or "not_in_cbip_selection"
    (known CNK, not curated by CBIP/BCFI -- or this DB build has no CBIP
    data at all). A bare `null` is ambiguous for a caller deciding whether
    it needs to double-check with the unitary tool; the reason makes that
    unnecessary.

    - identity: {amp_code, name_fr, name_nl, company, status, pharma_form_fr,
      pharma_form_nl, route_fr, route_nl, pack_display_fr, pack_display_nl,
      delivery_modus, ex_factory_price, component_count, multi_component}.
      pharma_form/route reflect AMP component #1 only -- call get_medicine
      for the full per-component breakdown. multi_component is true when
      component_count > 1 (e.g. a multiphasic contraceptive like Qlaira,
      whose pill strip has several distinct tablet types): when true, the
      `substances` list below mixes ingredients from every component with
      no per-component grouping, so the same substance_code can legitimately
      appear several times at different strengths -- that means different
      phases, not a contradiction. Check this flag before reading
      `substances` at face value; call get_medicine for the per-component
      strengths.
    - substances: list of active substances with strength (quantity/unit/
      operator) and a `strength_missing` flag, true when the SAM source
      export itself has no strength for that substance -- never inferred
      from the medicine's name (see get_ingredients). Flat across all AMP
      components -- see `multi_component` above.
    - reimbursement: list of currently-active tranche(s), same shape as
      get_reimbursement -- usually a single entry, but can hold more than
      one when several delivery environments are simultaneously active.
    - cbip: {public_price, index, rema, remw, law, ssecr,
      flags: {bt, orphan, narcotic, specrules}, chapter_code, chapter_title,
      chapter_positioning, coverage, cbip_price_placeholder}. coverage is
      "pack_level" or "product_level" (same fallback as get_cbip_notes;
      pack-specific fields are null at product level). cbip_price_placeholder
      is true when CBIP's own record has public_price = 0 (~19% of
      pack_level CBIP packs, e.g. hospital-restricted specialties -- never
      a genuine EUR 0.00); when true, public_price/index/rema/remw are null
      rather than a raw, misleading 0.0.
    """
    cnks = [c.strip() for c in cnks]
    if not cnks or len(cnks) > 50:
        raise ValueError("cnks must contain between 1 and 50 CNKs")

    as_of_date = as_of.strip() if as_of else date.today().isoformat()

    with db() as conn:
        identities = _pack_identities(conn, cnks)
        amp_codes = sorted({e["amp_code"] for e in identities.values()})
        substances = _pack_substances(conn, amp_codes) if amp_codes else {}
        reimbursements = _reimbursement_for_cnks(conn, cnks, as_of_date, include_history)
        cbip = _pack_cbip(conn, cnks)

    packs: dict[str, Any] = {}
    not_found: list[str] = []
    for cnk in cnks:
        identity = identities.get(cnk)
        if identity is None:
            not_found.append(cnk)
            packs[cnk] = {
                "identity": None, "identity_absent_reason": "cnk_not_found",
                "substances": None, "substances_absent_reason": "cnk_not_found",
                "reimbursement": None, "reimbursement_absent_reason": "cnk_not_found",
                "cbip": None, "cbip_absent_reason": "cnk_not_found",
            }
            continue

        pack_reimbursement = reimbursements.get(cnk)
        pack_cbip = cbip.get(cnk)
        packs[cnk] = {
            "identity": identity,
            "identity_absent_reason": None,
            "substances": substances.get(identity["amp_code"], []),
            "substances_absent_reason": None,
            "reimbursement": pack_reimbursement,
            "reimbursement_absent_reason": None if pack_reimbursement else "no_reimbursement_record",
            "cbip": pack_cbip,
            "cbip_absent_reason": None if pack_cbip else "not_in_cbip_selection",
        }

    return {"packs": packs, "not_found": not_found}


@mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False))
def get_database_stats() -> dict[str, Any]:
    """
    Report what's in the SAM database and how much of it there is.

    Call this first when you don't know whether something is searchable, or
    to gauge how big a result set a search might return — each table has a
    row count plus a short description of what it holds (e.g. how many
    medicines, active substances, or CBIP-annotated packs are available).
    Optional tables (ATC links, IMPP imports, CBIP notes) only appear if this
    DB build includes them.
    """
    with db() as conn:
        meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
        counts = _collect_counts(conn)
    tables = {
        tbl: {"count": n, "description": _TABLE_DESCRIPTIONS.get(tbl, "")}
        for tbl, n in counts.items()
    }
    return {"db_path": str(DB_PATH), "meta": meta, "tables": tables}


def _log_startup_counts() -> None:
    """Log row counts for the important tables at startup.

    Writes to stderr: in stdio mode stdout is the JSON-RPC channel, so any
    diagnostic output must stay off it.
    """
    core_tables  = ("amp", "ampp", "dmpp", "amp_ingredient", "substance", "atc")
    opt_tables   = ("amp_atc", "impp")
    cbip_tables  = ("cbip_mp", "cbip_mpp", "cbip_sam")
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
                for tbl in core_tables
            }
            for tbl in opt_tables:
                if _has_table(conn, tbl):
                    counts[tbl] = conn.execute(
                        f"SELECT COUNT(*) AS n FROM {tbl}"
                    ).fetchone()["n"]
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
    cbip = meta.get("cbip_export_date") or meta.get("cbip_source")
    suffix = f", CBIP edition: {cbip}" if cbip else ""
    print(f"[sam-mcp] DB {DB_PATH} (built: {built}{suffix})",
          file=sys.stderr, flush=True)
    summary = ", ".join(f"{tbl}={n}" for tbl, n in counts.items())
    print(f"[sam-mcp] row counts: {summary}", file=sys.stderr, flush=True)

_TABLE_DESCRIPTIONS = {
    "amp_atc": "AMP → ATC links extracted from the AMP file (Ampp/Atc, populated since v1.6)",
    "amp": "Actual Medicinal Product - marketed medicine (brand + strength + company)",
    "ampp": "Actual Medicinal Product Package - specific pack of an AMP",
    "dmpp": "Dispensed Medicinal Product Package - pack at dispensing/CNK level with price/reimbursement data",
    "amp_ingredient": "Link rows between an AMP and its active substances",
    "substance": "Active substances (molecule reference list)",
    "atc": "ATC classification codes (Anatomical Therapeutic Chemical)",
    "pharma_form": "Pharmaceutical forms (tablet, syrup, gel, etc.)",
    "route": "Routes of administration (oral, IV, cutaneous, etc.)",
    "vtm": "Virtual Therapeutic Moiety - abstract molecule concept",
    "reimbursement": "Reimbursement records (base/reference price, flat-rate flag, per pack)",
    "reimbursement_criterion": "Reimbursement criteria (category and conditions)",
    "nonmedicinal": "Non-medicinal products and supplements",
    "compounding_ingredient": "Ingredients for magistral/compounded preparations",
    "legal_basis": "Root legal bases (legal foundation)",
    "legal_reference": "Legal references (specific articles/paragraphs)",
    "legal_text": "Legal texts (French and Dutch)",
    "cbip_mp": "Medicinal Product as annotated by CBIP/BCFI",
    "cbip_mpp": "Medicinal Product Package as annotated by CBIP/BCFI",
    "cbip_hyr": "CBIP therapeutic hierarchy and chapters",
    "cbip_innm": "INN/generic names as listed by CBIP",
    "cbip_sam": "CBIP-to-SAM mapping table",
    "impp": "Imported Medicinal Product Package — medicines imported for compassionate use or shortage",
    "impp_substance": "Active substances per imported product",
    "impp_route": "Routes of administration per imported product",
}


async def _status_handler(request) -> JSONResponse:
    """HTTP GET /status — DB build metadata and per-table row counts."""
    try:
        with db() as conn:
            meta = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM meta")}
            counts = _collect_counts(conn)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    tables = {
        tbl: {"count": n, "description": _TABLE_DESCRIPTIONS.get(tbl, "")}
        for tbl, n in counts.items()
    }
    return JSONResponse({"meta": meta, "tables": tables})


_FAVICONS = {
    "/favicon.svg": "image/svg+xml",
    "/favicon.ico": "image/x-icon",
}


async def _favicon_handler(request):
    """HTTP GET /favicon.svg | /favicon.ico — the tab icon for /log."""
    name = request.url.path.rsplit("/", 1)[-1]
    media_type = _FAVICONS.get(f"/{name}")
    if media_type is None:
        return PlainTextResponse("not found\n", status_code=404)
    try:
        data = (ASSETS / name).read_bytes()
    except OSError:
        # A packaging slip shouldn't turn into a 500 on every page load.
        return PlainTextResponse("not found\n", status_code=404)
    # /log meta-refreshes every 10s; without this the browser would re-fetch
    # the icon on each reload and clutter the access log.
    return Response(data, media_type=media_type,
                    headers={"Cache-Control": "public, max-age=86400"})


_LOG_PAGE = """<!doctype html>
<html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="{refresh}">
<title>sam-mcp log</title>
<link rel="icon" href="favicon.svg" type="image/svg+xml">
<link rel="alternate icon" href="favicon.ico" sizes="32x32 16x16">
<style>
  :root {{ color-scheme: dark; }}
  body {{ margin: 0; background: #11131a; color: #d5d8e0;
         font: 13px/1.45 ui-monospace, "SFMono-Regular", Consolas, monospace; }}
  header {{ position: sticky; top: 0; background: #191c26; border-bottom: 1px solid #2b3040;
            padding: 8px 14px; color: #8b93a7; }}
  header b {{ color: #d5d8e0; font-weight: 600; }}
  header a {{ color: #6ea8fe; text-decoration: none; }}
  main {{ padding: 10px 14px 40px; }}
  div.l {{ white-space: pre-wrap; word-break: break-word; }}
  span.t {{ color: #5a6178; }}
  div.err {{ color: #ff8080; }}
  div.warn {{ color: #ffc46b; }}
  p.empty {{ color: #8b93a7; }}
</style></head>
<body>
<header><b>sam-mcp</b> — {count} line(s), buffer {capacity} · DB {db} ·
refreshing every {refresh}s · <a href="{text_url}">plain text</a></header>
<main>{body}</main>
<script>window.scrollTo(0, document.body.scrollHeight);</script>
</body></html>
"""


def _log_lines(request) -> list[tuple[float, str]]:
    with _LOG_LOCK:
        entries = list(_LOG_BUFFER)
    raw = request.query_params.get("lines")
    if raw:
        try:
            n = max(1, min(int(raw), _LOG_CAPACITY))
        except ValueError:
            return entries
        return entries[-n:]
    return entries


async def _log_handler(request):
    """HTTP GET /log — recent server output, i.e. what `docker logs` shows.

    HTML by default (auto-refreshing); plain text for `?format=text` or a
    non-browser Accept header. Optional `SAM_LOG_TOKEN` gates access.
    """
    token = os.environ.get("SAM_LOG_TOKEN")
    if token and not hmac.compare_digest(request.query_params.get("token", ""), token):
        return PlainTextResponse("forbidden\n", status_code=403)

    try:
        entries = _log_lines(request)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)

    # Browsers send an Accept containing text/html and get the page; curl and
    # friends send */* and get plain text. ?format= overrides either way.
    fmt = request.query_params.get("format")
    if fmt in ("text", "html"):
        wants_text = fmt == "text"
    else:
        wants_text = "text/html" not in request.headers.get("accept", "")

    stamped = [(time.strftime("%H:%M:%S", time.gmtime(ts)), line) for ts, line in entries]

    if wants_text:
        text = "".join(f"{stamp} {line}\n" for stamp, line in stamped)
        return PlainTextResponse(text or "(no output captured yet)\n")

    if stamped:
        rows = []
        for stamp, line in stamped:
            cls = "l"
            if "FAILED" in line or "FATAL" in line or "ERROR" in line:
                cls = "l err"
            elif "WARNING" in line:
                cls = "l warn"
            rows.append(f'<div class="{cls}"><span class="t">{stamp}</span> {html.escape(line)}</div>')
        body = "\n".join(rows)
    else:
        body = '<p class="empty">(no output captured yet)</p>'

    # Keep token/lines when linking to the text view; the meta-refresh reloads
    # the current URL, so those params survive on their own there.
    keep = [(k, v) for k, v in request.query_params.items() if k != "format"]
    text_url = "?" + urlencode([("format", "text"), *keep])

    return HTMLResponse(_LOG_PAGE.format(
        refresh=10,
        count=len(entries),
        capacity=_LOG_CAPACITY,
        db=html.escape(str(DB_PATH)),
        text_url=html.escape(text_url, quote=True),
        body=body,
    ))


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

    # HTTP mode only: in stdio mode stdout is the JSON-RPC channel, so there is
    # nothing worth buffering. Installed before anything else prints (and before
    # uvicorn.run, which resolves ext://sys.stdout|stderr at dictConfig time) so
    # startup diagnostics and access logs both land in the buffer.
    if args.http:
        _install_log_capture()

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
        app.router.routes.insert(0, Route("/log", _log_handler))
        for _path in _FAVICONS:
            app.router.routes.insert(0, Route(_path, _favicon_handler))

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
