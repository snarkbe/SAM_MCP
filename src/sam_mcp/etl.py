"""
ETL: stream Belgian SAM v2 XML exports into SQLite.

Reads (in order): REF (ATC + reference data), AMP (actual medicinal products
with their ingredients, packs and CNKs). Skips reimbursement/Chapter-IV/CMP
for the first cut — they can be added later without schema changes.

Strategy:
- lxml.iterparse with `tag=` filter so we only stop at <Amp>, <Ampp>, etc.
- After each top-level element is processed we call .clear() and prune
  preceding siblings, keeping memory flat regardless of file size.
- For every entity we extract its "currently valid" <Data> slice (today
  between from/to). If none is current, fall back to the latest <Data>.
- Inserts are batched inside one transaction per file.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

from lxml import etree

ROOT = Path(__file__).resolve().parent
SCHEMA = (ROOT / "schema.sql").read_text(encoding="utf-8")

NS_EXPORT  = "urn:be:fgov:ehealth:samws:v2:export"
NS_CORE    = "urn:be:fgov:ehealth:samws:v2:core"
NS_REFDATA = "urn:be:fgov:ehealth:samws:v2:refdata"


def _local(tag: str) -> str:
    """Strip XML namespace from a tag like '{ns}Name' -> 'Name'."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _children(elem, name: str):
    """Direct children matching local-name == name (any namespace)."""
    for c in elem:
        if _local(c.tag) == name:
            yield c


def _child(elem, name: str):
    return next(_children(elem, name), None)


def _text(elem, name: str) -> str | None:
    c = _child(elem, name)
    return c.text if c is not None else None


def _multilang(elem) -> dict[str, str | None]:
    """Read a <Name> / <PrescriptionNameFamhp> block with Fr/Nl/De/En children."""
    out = {"Fr": None, "Nl": None, "De": None, "En": None}
    if elem is None:
        return out
    for c in elem:
        ln = _local(c.tag)
        if ln in out:
            out[ln] = c.text
    return out


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def pick_current_data(parent, today: date):
    """
    Return the <Data> child that is valid today (from <= today <= to|inf).
    If none is current, fall back to the <Data> with the most recent `from`.
    """
    candidates = list(_children(parent, "Data"))
    if not candidates:
        return None
    current = None
    current_from = None
    latest = None
    latest_from = date.min
    for d in candidates:
        df = _parse_date(d.get("from"))
        dt = _parse_date(d.get("to"))
        if df and df > latest_from:
            latest_from = df
            latest = d
        if df and df <= today and (dt is None or dt >= today):
            if current_from is None or df > current_from:
                current = d
                current_from = df
    return current if current is not None else latest


# --------------------------------------------------------------------------
# REF
# --------------------------------------------------------------------------

def load_ref(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[REF] {path.name}")
    cur = conn.cursor()
    n_atc = n_sub = n_form = n_route = 0

    # REF root has no <Data> wrappers; the entries are direct children of root.
    # We still iterparse to keep memory usage flat.
    context = etree.iterparse(
        str(path), events=("end",), huge_tree=True
    )
    for _, elem in context:
        ln = _local(elem.tag)

        if ln == "AtcClassification":
            cur.execute(
                "INSERT OR REPLACE INTO atc(code, description) VALUES (?, ?)",
                (elem.get("code"), _text(elem, "Description")),
            )
            n_atc += 1
            elem.clear()

        elif ln == "Substance":
            data = pick_current_data(elem, today) or elem
            name = _multilang(_child(data, "Name"))
            cur.execute(
                "INSERT OR REPLACE INTO substance(code, name_fr, name_nl, name_en, type) "
                "VALUES (?,?,?,?,?)",
                (
                    elem.get("code"),
                    name["Fr"], name["Nl"], name["En"],
                    _text(data, "Type"),
                ),
            )
            n_sub += 1
            elem.clear()

        elif ln == "PharmaceuticalForm":
            data = pick_current_data(elem, today) or elem
            name = _multilang(_child(data, "Name"))
            cur.execute(
                "INSERT OR REPLACE INTO pharma_form(code, name_fr, name_nl, name_en) "
                "VALUES (?,?,?,?)",
                (elem.get("code"), name["Fr"], name["Nl"], name["En"]),
            )
            n_form += 1
            elem.clear()

        elif ln == "RouteOfAdministration":
            data = pick_current_data(elem, today) or elem
            name = _multilang(_child(data, "Name"))
            cur.execute(
                "INSERT OR REPLACE INTO route(code, name_fr, name_nl, name_en) "
                "VALUES (?,?,?,?)",
                (elem.get("code"), name["Fr"], name["Nl"], name["En"]),
            )
            n_route += 1
            elem.clear()

    conn.commit()

    # Populate substance FTS
    cur.execute("DELETE FROM substance_fts")
    cur.execute(
        "INSERT INTO substance_fts(substance_code, name_fr, name_nl, name_en) "
        "SELECT code, COALESCE(name_fr,''), COALESCE(name_nl,''), COALESCE(name_en,'') FROM substance"
    )
    conn.commit()
    print(f"[REF] atc={n_atc} substance={n_sub} form={n_form} route={n_route}")


# --------------------------------------------------------------------------
# AMP
# --------------------------------------------------------------------------

def _ampp_iter(amp_elem):
    """Yield direct <Ampp> children (any namespace)."""
    return _children(amp_elem, "Ampp")


def _component_iter(amp_elem):
    return _children(amp_elem, "AmpComponent")


def _ingredient_iter(component_elem):
    return _children(component_elem, "RealActualIngredient")


def _dmpp_iter(ampp_elem):
    return _children(ampp_elem, "Dmpp")


def _strength(elem) -> tuple[str | None, str | None, str | None]:
    """Read <Strength><Operator/><Quantity unit=.../></Strength>."""
    s = _child(elem, "Strength")
    if s is None:
        return (None, None, None)
    operator = _text(s, "Operator")
    qty_el = _child(s, "Quantity")
    quantity = qty_el.text if qty_el is not None else None
    unit = qty_el.get("unit") if qty_el is not None else None
    return operator, quantity, unit


def process_amp(conn: sqlite3.Connection, amp_elem, today: date,
                stats: dict) -> None:
    code = amp_elem.get("code")
    if not code:
        return

    cur = conn.cursor()

    # ----- AMP-level data -----
    amp_data = pick_current_data(amp_elem, today)
    if amp_data is None:
        return
    name = _multilang(_child(amp_data, "Name"))
    presc = _multilang(_child(amp_data, "PrescriptionNameFamhp"))
    company_el = _child(amp_data, "Company")
    company_name = None
    if company_el is not None:
        c_data = pick_current_data(company_el, today)
        if c_data is not None:
            company_name = _text(c_data, "Denomination")

    cur.execute(
        """INSERT OR REPLACE INTO amp(
            code, name_fr, name_nl, name_en, official_name, status,
            medicine_type, black_triangle, company,
            prescription_name_fr, prescription_name_nl,
            valid_from, valid_to)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            code,
            name["Fr"], name["Nl"], name["En"],
            _text(amp_data, "OfficialName"),
            _text(amp_data, "Status"),
            _text(amp_data, "MedicineType"),
            1 if (_text(amp_data, "BlackTriangle") or "").lower() == "true" else 0,
            company_name,
            presc["Fr"], presc["Nl"],
            amp_data.get("from"), amp_data.get("to"),
        ),
    )
    cur.execute(
        "INSERT INTO amp_fts(amp_code, name_fr, name_nl, name_en, official_name, "
        "prescription_name_fr, prescription_name_nl) VALUES (?,?,?,?,?,?,?)",
        (
            code,
            name["Fr"] or "", name["Nl"] or "", name["En"] or "",
            _text(amp_data, "OfficialName") or "",
            presc["Fr"] or "", presc["Nl"] or "",
        ),
    )
    stats["amp"] += 1

    # ----- components + ingredients -----
    for comp in _component_iter(amp_elem):
        seq_attr = comp.get("sequenceNr")
        try:
            seq = int(seq_attr) if seq_attr is not None else 0
        except ValueError:
            seq = 0
        comp_data = pick_current_data(comp, today)

        pf_code = pf_fr = pf_nl = None
        ro_code = ro_fr = ro_nl = None
        if comp_data is not None:
            pf = _child(comp_data, "PharmaceuticalForm")
            if pf is not None:
                pf_code = pf.get("code")
                pf_name = _multilang(_child(pf, "Name"))
                pf_fr, pf_nl = pf_name["Fr"], pf_name["Nl"]
            ro = _child(comp_data, "RouteOfAdministration")
            if ro is not None:
                ro_code = ro.get("code")
                ro_name = _multilang(_child(ro, "Name"))
                ro_fr, ro_nl = ro_name["Fr"], ro_name["Nl"]

        cur.execute(
            "INSERT OR REPLACE INTO amp_component("
            "amp_code, seq, pharma_form_code, pharma_form_fr, pharma_form_nl,"
            " route_code, route_fr, route_nl) VALUES (?,?,?,?,?,?,?,?)",
            (code, seq, pf_code, pf_fr, pf_nl, ro_code, ro_fr, ro_nl),
        )

        for ing in _ingredient_iter(comp):
            rank_attr = ing.get("rank")
            try:
                rank = int(rank_attr) if rank_attr is not None else 0
            except ValueError:
                rank = 0
            ing_data = pick_current_data(ing, today)
            if ing_data is None:
                continue
            sub = _child(ing_data, "Substance")
            sub_code = sub.get("code") if sub is not None else None
            sub_name = _multilang(_child(sub, "Name")) if sub is not None else _multilang(None)
            op, qty, unit = _strength(ing_data)

            cur.execute(
                "INSERT INTO amp_ingredient("
                "amp_code, component_seq, rank, type, substance_code,"
                " substance_name_fr, substance_name_nl, substance_name_en,"
                " strength_operator, strength_quantity, strength_unit)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    code, seq, rank, _text(ing_data, "Type"), sub_code,
                    sub_name["Fr"], sub_name["Nl"], sub_name["En"],
                    op, qty, unit,
                ),
            )
            stats["ing"] += 1

            # Backfill substance table from inline data if absent
            if sub_code:
                cur.execute(
                    "INSERT OR IGNORE INTO substance(code, name_fr, name_nl, name_en, type)"
                    " VALUES (?,?,?,?,?)",
                    (sub_code, sub_name["Fr"], sub_name["Nl"], sub_name["En"],
                     _text(ing_data, "Type")),
                )

    # ----- ampp + dmpp (CNK) -----
    for ampp in _ampp_iter(amp_elem):
        cti = ampp.get("ctiExtended")
        if not cti:
            continue
        ampp_data = pick_current_data(ampp, today)
        ex_price = None
        if ampp_data is not None:
            try:
                p = _text(ampp_data, "OfficialExFactoryPrice") \
                    or _text(ampp_data, "RealExFactoryPrice")
                ex_price = float(p) if p else None
            except ValueError:
                ex_price = None

            pack = _multilang(_child(ampp_data, "PackDisplayValue"))
            presc_p = _multilang(_child(ampp_data, "PrescriptionNameFamhp"))
            legal = _multilang(_child(ampp_data, "LegalBasis"))
            dm_el = _child(ampp_data, "DeliveryModus")
            dm_code = dm_el.get("code") if dm_el is not None else None

            cur.execute(
                """INSERT OR REPLACE INTO ampp(
                    cti_extended, amp_code, auth_nr,
                    pack_display_fr, pack_display_nl, status,
                    prescription_name_fr, prescription_name_nl,
                    delivery_modus, legal_basis_fr, ex_factory_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    cti, code, _text(ampp_data, "AuthorisationNr"),
                    pack["Fr"], pack["Nl"],
                    _text(ampp_data, "Status"),
                    presc_p["Fr"], presc_p["Nl"],
                    dm_code, legal["Fr"], ex_price,
                ),
            )
            stats["ampp"] += 1

        for dmpp in _dmpp_iter(ampp):
            if dmpp.get("codeType") != "CNK":
                continue
            cnk = dmpp.get("code")
            if not cnk:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO dmpp(cnk, cti_extended, amp_code,"
                " delivery_environment, product_id) VALUES (?,?,?,?,?)",
                (cnk, cti, code,
                 dmpp.get("deliveryEnvironment"),
                 dmpp.get("ProductId")),
            )
            stats["dmpp"] += 1


def load_amp(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[AMP] {path.name} (streaming)")
    stats = {"amp": 0, "ampp": 0, "dmpp": 0, "ing": 0}
    amp_tag = f"{{{NS_EXPORT}}}Amp"

    context = etree.iterparse(
        str(path), events=("end",), tag=amp_tag, huge_tree=True
    )
    for _, elem in context:
        # Defensive: only top-level Amp nodes (parent is the root). The
        # iterparse `tag=` filter already restricts us, but a nested element
        # of the same name would otherwise sneak through.
        try:
            process_amp(conn, elem, today, stats)
        except Exception as e:  # noqa: BLE001
            print(f"  ! error on AMP {elem.get('code')}: {e}", file=sys.stderr)

        if stats["amp"] % 1000 == 0:
            conn.commit()
            print(f"  ... {stats['amp']} AMPs", file=sys.stderr)

        # Free memory: clear this element AND all preceding siblings
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    conn.commit()
    print(f"[AMP] amp={stats['amp']} ampp={stats['ampp']} "
          f"dmpp={stats['dmpp']} ing={stats['ing']}")


# --------------------------------------------------------------------------
# entrypoint
# --------------------------------------------------------------------------

def find_file(data_dir: Path, prefix: str) -> Path | None:
    matches = sorted(data_dir.glob(f"{prefix}-*.xml"))
    return matches[-1] if matches else None


def main() -> int:
    p = argparse.ArgumentParser(description="Build SAM SQLite DB from XML exports")
    p.add_argument("--data", type=Path, default=Path("xml"),
                   help="Directory containing the SAM XML files")
    p.add_argument("--db", type=Path, default=Path("db/sam.db"),
                   help="Output SQLite database path")
    p.add_argument("--today", type=str, default=None,
                   help="Reference date (YYYY-MM-DD). Defaults to today.")
    p.add_argument("--skip-ref", action="store_true")
    p.add_argument("--skip-amp", action="store_true")
    p.add_argument("--with-cbip", action="store_true",
                   help="Also load the CBIP/BCFI dump after the SAM build")
    p.add_argument("--cbip-sql", type=Path, default=Path("exportFr.sql"),
                   help="Path to the CBIP pg_dump .sql (used with --with-cbip)")
    args = p.parse_args()

    today = date.fromisoformat(args.today) if args.today else date.today()

    args.db.parent.mkdir(parents=True, exist_ok=True)
    if args.db.exists():
        args.db.unlink()

    conn = sqlite3.connect(args.db)
    conn.executescript(SCHEMA)
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('built_at', datetime('now'))")
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('reference_date', ?)",
                 (today.isoformat(),))

    if not args.skip_ref:
        ref = find_file(args.data, "REF")
        if ref:
            load_ref(conn, ref, today)
        else:
            print("! no REF file found", file=sys.stderr)

    if not args.skip_amp:
        amp = find_file(args.data, "AMP")
        if amp:
            load_amp(conn, amp, today)
        else:
            print("! no AMP file found", file=sys.stderr)

    # Build AMP FTS contents (already populated row-by-row in load_amp;
    # nothing more needed here, but optimize the FTS index).
    print("[FTS] optimizing")
    conn.execute("INSERT INTO amp_fts(amp_fts) VALUES('optimize')")
    conn.execute("INSERT INTO substance_fts(substance_fts) VALUES('optimize')")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    print(f"[done] {args.db}")

    if args.with_cbip:
        from . import etl_cbip
        rc = etl_cbip.run(args.cbip_sql, args.db)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
