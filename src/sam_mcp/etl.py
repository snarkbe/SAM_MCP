"""
ETL: stream Belgian SAM v2 XML exports into SQLite.

Reads (in order): REF, AMP, VMP, RMB, NONMEDICINAL, CMP, RML.

Strategy:
- lxml.iterparse with `tag=` filter so we only stop at top-level elements.
- After each element is processed we call .clear() and prune preceding
  siblings, keeping memory flat regardless of file size.
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

XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


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
            valid_from, valid_to, vmp_code)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
            amp_elem.get("vmpCode"),
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
                    delivery_modus, legal_basis_fr, legal_basis_nl, ex_factory_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    cti, code, _text(ampp_data, "AuthorisationNr"),
                    pack["Fr"], pack["Nl"],
                    _text(ampp_data, "Status"),
                    presc_p["Fr"], presc_p["Nl"],
                    dm_code, legal["Fr"], legal["Nl"], ex_price,
                ),
            )
            stats["ampp"] += 1

            # AMP -> ATC link. The ATC classification lives on each Ampp's
            # current Data slice (<Atc code="..."/>), not in the VMP file.
            # Roll it up to the AMP so aggregate_substances can filter by
            # ATC prefix. INSERT OR IGNORE: packs of one AMP repeat the code.
            atc_el = _child(ampp_data, "Atc")
            atc_code = atc_el.get("code") if atc_el is not None else None
            if atc_code:
                cur.execute(
                    "INSERT OR IGNORE INTO amp_atc(amp_code, atc_code)"
                    " VALUES (?,?)",
                    (code, atc_code),
                )
                stats["atc"] += 1

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
    stats = {"amp": 0, "ampp": 0, "dmpp": 0, "ing": 0, "atc": 0}
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
          f"dmpp={stats['dmpp']} ing={stats['ing']} amp_atc={stats['atc']}")


# --------------------------------------------------------------------------
# VMP
# --------------------------------------------------------------------------

def load_vmp(conn: sqlite3.Connection, path: Path, today: date) -> None:
    """Parse the VMP file for Vtm (molecule definitions).

    Note: the AMP→ATC link is *not* in the VMP file — the current SAM v2
    export carries the ATC classification on each Ampp's Data slice in the
    AMP file, where load_amp populates the amp_atc table.
    """
    print(f"[VMP] {path.name}")
    n_vtm = 0
    cur = conn.cursor()

    # No tag filter: we walk both top-level Vtm and Vmp elements so each is
    # cleared to keep memory flat. Nested <Vtm code="..."/> references inside
    # <Vmp> are skipped by checking the parent tag — they carry no Data child.
    for _, elem in etree.iterparse(str(path), events=("end",), huge_tree=True):
        ln = _local(elem.tag)

        if ln == "Vtm":
            parent = elem.getparent()
            if parent is not None and _local(parent.tag) == "Vmp":
                continue  # nested reference — skip without clearing
            data = pick_current_data(elem, today)
            if data is not None:
                name = _multilang(_child(data, "Name"))
                cur.execute(
                    "INSERT OR REPLACE INTO vtm(code, name_fr, name_nl, valid_from, valid_to)"
                    " VALUES (?,?,?,?,?)",
                    (elem.get("code"), name["Fr"], name["Nl"],
                     data.get("from"), data.get("to")),
                )
                n_vtm += 1
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        elif ln == "Vmp":
            # ATC links used to be parsed here, but the current export does
            # not carry them in VMP (see load_amp). Clear to free memory.
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    conn.commit()
    print(f"[VMP] vtm={n_vtm}")


# --------------------------------------------------------------------------
# RMB
# --------------------------------------------------------------------------

def load_rmb(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[RMB] {path.name} (streaming)")
    n = n_crit = 0
    tag = f"{{{NS_EXPORT}}}ReimbursementContext"
    cur = conn.cursor()
    for _, elem in etree.iterparse(str(path), events=("end",), tag=tag, huge_tree=True):
        data = pick_current_data(elem, today)
        if data is None:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        cnk        = elem.get("code")
        deliv_env  = elem.get("deliveryEnvironment")
        legal_ref  = elem.get("legalReferencePath")
        valid_from = data.get("from")
        valid_to   = data.get("to")

        def _bool(name: str) -> int:
            return 1 if (_text(data, name) or "").lower() == "true" else 0

        def _price(name: str) -> float | None:
            v = _text(data, name)
            try:
                return float(v) if v else None
            except ValueError:
                return None

        pu = _child(data, "PricingUnit")
        pu_qty = pu_fr = pu_nl = None
        if pu is not None:
            try:
                pu_qty = float(_text(pu, "Quantity") or "")
            except ValueError:
                pu_qty = None
            label = _multilang(_child(pu, "Label"))
            pu_fr, pu_nl = label["Fr"], label["Nl"]

        cur.execute(
            "INSERT OR REPLACE INTO reimbursement("
            "cnk, delivery_environment, valid_from, valid_to, legal_reference,"
            " temporary, is_reference, flat_rate_system,"
            " reimbursement_price, reference_price,"
            " pricing_unit_qty, pricing_unit_fr, pricing_unit_nl)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (cnk, deliv_env, valid_from, valid_to, legal_ref,
             _bool("Temporary"), _bool("Reference"), _bool("FlatRateSystem"),
             _price("ReimbursementBasePrice"), _price("ReferenceBasePrice"),
             pu_qty, pu_fr, pu_nl),
        )
        n += 1

        for crit in _children(data, "ReimbursementCriterion"):
            desc = _multilang(_child(crit, "Description"))
            cur.execute(
                "INSERT OR REPLACE INTO reimbursement_criterion("
                "cnk, delivery_environment, valid_from, category, code,"
                " description_fr, description_nl)"
                " VALUES (?,?,?,?,?,?,?)",
                (cnk, deliv_env, valid_from,
                 crit.get("category"), crit.get("code"),
                 desc["Fr"], desc["Nl"]),
            )
            n_crit += 1

        if n % 1000 == 0:
            conn.commit()
            print(f"  ... {n} reimbursement records", file=sys.stderr)

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    conn.commit()
    print(f"[RMB] reimbursement={n} criteria={n_crit}")


# --------------------------------------------------------------------------
# NONMEDICINAL
# --------------------------------------------------------------------------

def load_nonmedicinal(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[NONMEDICINAL] {path.name}")
    n = 0
    cur = conn.cursor()
    tag = f"{{{NS_EXPORT}}}NonMedicinalProduct"
    for _, elem in etree.iterparse(str(path), events=("end",), tag=tag, huge_tree=True):
        data = pick_current_data(elem, today)
        if data is None:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue
        name        = _multilang(_child(data, "Name"))
        producer    = _multilang(_child(data, "Producer"))
        distributor = _multilang(_child(data, "Distributor"))
        cur.execute(
            "INSERT OR REPLACE INTO nonmedicinal("
            "code, product_id, name_fr, name_nl, category, commercial_status,"
            " producer_fr, producer_nl, distributor_fr, distributor_nl,"
            " valid_from, valid_to)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                elem.get("code"), elem.get("ProductId"),
                name["Fr"], name["Nl"],
                _text(data, "Category"), _text(data, "CommercialStatus"),
                producer["Fr"], producer["Nl"],
                distributor["Fr"], distributor["Nl"],
                data.get("from"), data.get("to"),
            ),
        )
        n += 1
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    conn.commit()
    print(f"[NONMEDICINAL] nonmedicinal={n}")


# --------------------------------------------------------------------------
# CMP
# --------------------------------------------------------------------------

def load_cmp(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[CMP] {path.name}")
    n = n_syn = 0
    cur = conn.cursor()
    tag = f"{{{NS_EXPORT}}}CompoundingIngredient"
    for _, elem in etree.iterparse(str(path), events=("end",), tag=tag, huge_tree=True):
        code = elem.get("code")
        if not code:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue
        data = pick_current_data(elem, today)
        cur.execute(
            "INSERT OR REPLACE INTO compounding_ingredient(code, product_id, valid_from)"
            " VALUES (?,?,?)",
            (code, elem.get("ProductId"),
             data.get("from") if data is not None else None),
        )
        n += 1
        if data is not None:
            for syn in _children(data, "Synonym"):
                lang = syn.get(XML_LANG)
                rank = syn.get("rank")
                if lang and rank and syn.text:
                    cur.execute(
                        "INSERT OR REPLACE INTO compounding_synonym(code, lang, rank, name)"
                        " VALUES (?,?,?,?)",
                        (code, lang, int(rank), syn.text),
                    )
                    n_syn += 1
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    conn.commit()
    print(f"[CMP] ingredients={n} synonyms={n_syn}")


# --------------------------------------------------------------------------
# RML
# --------------------------------------------------------------------------

def _walk_legal_texts(cur, basis_key: str, ref_key: str,
                      text_elem, parent_text_key, today: date) -> None:
    text_key = text_elem.get("key")
    if not text_key:
        return
    data    = pick_current_data(text_elem, today)
    content = _multilang(_child(data, "Content")) if data is not None else {"Fr": None, "Nl": None}
    seq     = _text(data, "SequenceNr") if data is not None else None
    cur.execute(
        "INSERT OR REPLACE INTO legal_text("
        "basis_key, ref_key, text_key, parent_text_key,"
        " content_fr, content_nl, type, sequence_nr, valid_from, valid_to)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (basis_key, ref_key, text_key, parent_text_key,
         content["Fr"], content["Nl"],
         _text(data, "Type") if data is not None else None,
         int(seq) if seq else None,
         data.get("from") if data is not None else None,
         data.get("to") if data is not None else None),
    )
    for child_text in _children(text_elem, "LegalText"):
        _walk_legal_texts(cur, basis_key, ref_key, child_text, text_key, today)


def _walk_legal_refs(cur, basis_key: str, ref_elem,
                     parent_ref_key, today: date) -> None:
    ref_key = ref_elem.get("key")
    if not ref_key:
        return
    data  = pick_current_data(ref_elem, today)
    title = _multilang(_child(data, "Title")) if data is not None else {"Fr": None, "Nl": None}
    cur.execute(
        "INSERT OR REPLACE INTO legal_reference("
        "basis_key, ref_key, parent_ref_key, title_fr, title_nl,"
        " type, first_published_on, valid_from, valid_to)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (basis_key, ref_key, parent_ref_key,
         title["Fr"], title["Nl"],
         _text(data, "Type") if data is not None else None,
         _text(data, "FirstPublishedOn") if data is not None else None,
         data.get("from") if data is not None else None,
         data.get("to") if data is not None else None),
    )
    for child_ref in _children(ref_elem, "LegalReference"):
        _walk_legal_refs(cur, basis_key, child_ref, ref_key, today)
    for text_elem in _children(ref_elem, "LegalText"):
        _walk_legal_texts(cur, basis_key, ref_key, text_elem, None, today)


def load_rml(conn: sqlite3.Connection, path: Path, today: date) -> None:
    print(f"[RML] {path.name} (streaming)")
    n_basis = n_ref = 0
    tag = f"{{{NS_EXPORT}}}LegalBasis"
    cur = conn.cursor()
    for _, elem in etree.iterparse(str(path), events=("end",), tag=tag, huge_tree=True):
        basis_key = elem.get("key")
        if not basis_key:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        data  = pick_current_data(elem, today)
        title = _multilang(_child(data, "Title")) if data is not None else {"Fr": None, "Nl": None}
        cur.execute(
            "INSERT OR REPLACE INTO legal_basis("
            "key, title_fr, title_nl, type, effective_on, valid_from, valid_to)"
            " VALUES (?,?,?,?,?,?,?)",
            (basis_key,
             title["Fr"], title["Nl"],
             _text(data, "Type") if data is not None else None,
             _text(data, "EffectiveOn") if data is not None else None,
             data.get("from") if data is not None else None,
             data.get("to") if data is not None else None),
        )
        n_basis += 1

        for ref in _children(elem, "LegalReference"):
            _walk_legal_refs(cur, basis_key, ref, None, today)
            n_ref += 1

        if n_basis % 500 == 0:
            conn.commit()
            print(f"  ... {n_basis} legal bases", file=sys.stderr)

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    conn.commit()
    print(f"[RML] legal_basis={n_basis} top_refs={n_ref}")


# --------------------------------------------------------------------------
# entrypoint
# --------------------------------------------------------------------------

def load_impp(conn: sqlite3.Connection, path: Path, today: date) -> None:
    """Parse the IMPP file (ImportedMedicinalProductPackage elements).

    IMPP = Imported Medicinal Products — medicines brought in from abroad for
    compassionate use or when a standard Belgian product is unavailable.  Each
    package has a Belgian CNK, a plain-text name (not multilang), a country
    code, free-text strength/pack-size, one pharmaceutical form, and one or
    more routes of administration.
    """
    print(f"[IMPP] {path.name}")
    n = 0
    cur = conn.cursor()
    tag = f"{{{NS_EXPORT}}}ImportedMedicinalProductPackage"

    for _, elem in etree.iterparse(str(path), events=("end",), tag=tag, huge_tree=True):
        impp_id = elem.get("Id")
        if not impp_id:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        data = pick_current_data(elem, today)
        if data is None:
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            continue

        cnk       = _text(data, "CNK")
        name      = _text(data, "Name")
        country   = _text(data, "Country")
        strength  = _text(data, "Strength")
        pack_size = _text(data, "PackSize")

        pf_el   = _child(data, "PharmaceuticalFormCode")
        pf_code = pf_el.get("code") if pf_el is not None else None
        pf_name = _multilang(_child(pf_el, "Name")) if pf_el is not None else _multilang(None)

        cur.execute(
            "INSERT OR REPLACE INTO impp"
            "(id, cnk, name, country, strength, pack_size,"
            " pharma_form_code, pharma_form_fr, pharma_form_nl, valid_from)"
            " VALUES (?,?,?,?,?,?,?,?,?,?)",
            (impp_id, cnk, name, country, strength, pack_size,
             pf_code, pf_name["Fr"], pf_name["Nl"],
             data.get("from")),
        )

        for sub_el in _children(data, "ActiveSubstance"):
            sub_code = sub_el.get("code")
            sub_name = _multilang(_child(sub_el, "Name"))
            cur.execute(
                "INSERT OR REPLACE INTO impp_substance"
                "(impp_id, substance_code, name_fr, name_nl) VALUES (?,?,?,?)",
                (impp_id, sub_code, sub_name["Fr"], sub_name["Nl"]),
            )

        for ro_el in _children(data, "RouteOfAdministrationCode"):
            ro_code = ro_el.get("code")
            ro_name = _multilang(_child(ro_el, "Name"))
            cur.execute(
                "INSERT OR REPLACE INTO impp_route"
                "(impp_id, route_code, route_fr, route_nl) VALUES (?,?,?,?)",
                (impp_id, ro_code, ro_name["Fr"], ro_name["Nl"]),
            )

        n += 1
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    conn.commit()
    print(f"[IMPP] impp={n}")


def find_file(data_dir: Path, prefix: str) -> Path | None:
    matches = sorted(data_dir.glob(f"{prefix}-*.xml"))
    return matches[-1] if matches else None


def write_stats(db_path: Path, out_path: Path) -> None:
    """Write per-table row counts of the built DB as `table,count` CSV lines.

    Enumerates every real table from sqlite_master, skipping SQLite internals
    and FTS5 shadow tables (their counts are not meaningful). The refresh
    script merges these counts with the SAM/CBIP versions and a timestamp into
    its tracking CSV, so we deliberately emit only table/count here.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%' "
                "ORDER BY name"
            )
        ]
        lines = [
            f"{t},{conn.execute(f'SELECT COUNT(*) FROM \"{t}\"').fetchone()[0]}"
            for t in tables
        ]
    finally:
        conn.close()
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[stats] {len(lines)} table counts -> {out_path}")


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
    p.add_argument("--stats-out", type=Path, default=None,
                   help="Write per-table row counts (CSV: table,count) here "
                        "after the build, for refresh trend tracking")
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

    for prefix, loader in [
        ("VMP",          load_vmp),
        ("RMB",          load_rmb),
        ("NONMEDICINAL", load_nonmedicinal),
        ("CMP",          load_cmp),
        ("RML",          load_rml),
        ("IMPP",         load_impp),
    ]:
        f = find_file(args.data, prefix)
        if f:
            loader(conn, f, today)

    # Build AMP FTS contents (already populated row-by-row in load_amp;
    # nothing more needed here, but optimize the FTS index).
    print("[FTS] optimizing")
    conn.execute("INSERT INTO amp_fts(amp_fts) VALUES('optimize')")
    conn.execute("INSERT INTO substance_fts(substance_fts) VALUES('optimize')")
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    print(f"[done] {args.db}")

    rc = 0
    if args.with_cbip:
        from . import etl_cbip
        rc = etl_cbip.run(args.cbip_sql, args.db)

    # Stats are written even on a CBIP row-level error (rc == 2): the DB is
    # built and self-consistent. The caller decides whether to keep the build.
    if args.stats_out:
        write_stats(args.db, args.stats_out)

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
