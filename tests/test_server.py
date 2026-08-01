"""
Unit tests for sam_mcp.server, run against a real, locally-built SAM database.

There is no fixture DB: building one requires the full ETL (~10-20 min) over
proprietary SAM/CBIP source exports that are not checked into this repo (see
CLAUDE.md). Instead these tests skip themselves when db/sam.db (or SAM_DB) is
absent, and otherwise exercise the tools against real CNKs that were used
during manual testing of Eliquis (apixaban) -- see get_pack_overview,
get_reimbursement and get_legal_text docstrings for the scenarios covered:

    3018181  - Eliquis 5 mg, 56 tabl.   - reimbursed, has an expired
               (2018-01-01..2018-03-31) tranche alongside the active one
    2843167  - Eliquis 2.5 mg, 60 tabl. - reimbursed, direct CBIP pack match
    3919222  - Eliquis 5 mg (Abacus), parallel import - NOT reimbursed
    2843183  - Eliquis 2.5 mg, 60x1 UD  - NOT reimbursed, CBIP product-level
               fallback only (no direct pack entry, resolved via sibling
               CNK 2843167)
    9999999  - does not exist

Uses only the standard library (unittest) -- no new test-runner dependency.
Run with:  uv run python -m unittest discover -s tests
"""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from sam_mcp import server  # noqa: E402

CNK_REIMBURSED_WITH_HISTORY = "3018181"
CNK_REIMBURSED_DIRECT_CBIP = "2843167"
CNK_NOT_REIMBURSED_PARALLEL_IMPORT = "3919222"
CNK_NOT_REIMBURSED_CBIP_FALLBACK = "2843183"
CNK_UNKNOWN = "9999999"
AMP_ELIQUIS_1_5MG = "SAM663104-00"  # strength gap in the SAM source itself
AMP_ELIQUIS_2MG = "SAM663105-00"
CNK_QLAIRA_MULTICOMPONENT = "2597003"  # 5 AMP components (multiphasic pill)
CNK_CBIP_ZERO_PRICE_A = "4272175"  # Eliquis 5mg parallel import, CBIP pupr=0
CNK_CBIP_ZERO_PRICE_B = "4201810"  # Eliquis 2.5mg parallel import, CBIP pupr=0


def _count_sql_statements(fn, *args, **kwargs):
    """Run fn(*args, **kwargs) and return (result, number_of_sql_statements_executed).

    Spies on every sqlite3 connection the call opens via sqlite3.Connection's
    trace callback, so the count reflects real executed statements rather
    than call sites in the source -- the only reliable way to catch an N+1
    regression (one query per CNK) instead of the required fixed, batched
    query count.
    """
    statements: list[str] = []
    real_connect = sqlite3.connect

    def spy_connect(*a, **kw):
        conn = real_connect(*a, **kw)
        conn.set_trace_callback(statements.append)
        return conn

    with mock.patch("sam_mcp.server.sqlite3.connect", side_effect=spy_connect):
        result = fn(*args, **kwargs)
    return result, len(statements)


@unittest.skipUnless(
    server.DB_PATH.exists(),
    f"SAM database not found at {server.DB_PATH} -- run the ETL first (see CLAUDE.md)",
)
class DbBackedTestCase(unittest.TestCase):
    pass


class TestGetPackOverview(DbBackedTestCase):
    def test_rejects_empty_or_oversized_cnk_lists(self):
        with self.assertRaises(ValueError):
            server.get_pack_overview([])
        with self.assertRaises(ValueError):
            server.get_pack_overview([str(n) for n in range(51)])

    def test_merges_all_sections_for_a_reimbursed_pack(self):
        result = server.get_pack_overview([CNK_REIMBURSED_WITH_HISTORY])
        pack = result["packs"][CNK_REIMBURSED_WITH_HISTORY]
        self.assertEqual(result["not_found"], [])

        self.assertIsNone(pack["identity_absent_reason"])
        self.assertEqual(pack["identity"]["amp_code"], "SAM442574-00")

        self.assertIsNone(pack["substances_absent_reason"])
        substance_codes = {s["substance_code"] for s in pack["substances"]}
        self.assertIn("6392", substance_codes)  # apixaban

        self.assertIsNone(pack["reimbursement_absent_reason"])
        self.assertTrue(all(t["is_current"] for t in pack["reimbursement"]))
        # The expired 2018 tranche must never surface through the overview.
        self.assertTrue(all(t["valid_from"] != "2018-01-01" for t in pack["reimbursement"]))

        self.assertIsNone(pack["cbip_absent_reason"])
        self.assertEqual(pack["cbip"]["coverage"], "pack_level")

    def test_non_reimbursed_pack_reports_explicit_reason(self):
        result = server.get_pack_overview([CNK_NOT_REIMBURSED_PARALLEL_IMPORT])
        pack = result["packs"][CNK_NOT_REIMBURSED_PARALLEL_IMPORT]
        self.assertIsNone(pack["reimbursement"])
        self.assertEqual(pack["reimbursement_absent_reason"], "no_reimbursement_record")
        # Still has CBIP data even though it isn't reimbursed.
        self.assertIsNone(pack["cbip_absent_reason"])

    def test_cbip_product_level_fallback_via_sibling_pack(self):
        result = server.get_pack_overview([CNK_NOT_REIMBURSED_CBIP_FALLBACK])
        pack = result["packs"][CNK_NOT_REIMBURSED_CBIP_FALLBACK]
        self.assertIsNone(pack["reimbursement"])
        self.assertEqual(pack["reimbursement_absent_reason"], "no_reimbursement_record")
        self.assertIsNone(pack["cbip_absent_reason"])
        self.assertEqual(pack["cbip"]["coverage"], "product_level")
        # Pack-specific fields have no meaning when resolved via a sibling.
        for field in ("public_price", "index", "rema", "remw", "law", "ssecr"):
            self.assertIsNone(pack["cbip"][field])

    def test_unknown_cnk_is_reported_both_per_pack_and_at_root(self):
        result = server.get_pack_overview([CNK_UNKNOWN])
        self.assertEqual(result["not_found"], [CNK_UNKNOWN])
        pack = result["packs"][CNK_UNKNOWN]
        for section in ("identity", "substances", "reimbursement", "cbip"):
            self.assertIsNone(pack[section])
            self.assertEqual(pack[f"{section}_absent_reason"], "cnk_not_found")

    def test_mixed_batch_resolves_each_cnk_independently(self):
        cnks = [
            CNK_REIMBURSED_WITH_HISTORY,
            CNK_REIMBURSED_DIRECT_CBIP,
            CNK_NOT_REIMBURSED_PARALLEL_IMPORT,
            CNK_NOT_REIMBURSED_CBIP_FALLBACK,
            CNK_UNKNOWN,
        ]
        result = server.get_pack_overview(cnks)
        self.assertEqual(set(result["packs"]), set(cnks))
        self.assertEqual(result["not_found"], [CNK_UNKNOWN])

    def test_query_count_does_not_scale_with_number_of_cnks(self):
        """The whole point of this tool: batched IN(...) queries, never one per CNK.

        Compared within each pair the *shape* of the request is identical (both
        pack-level direct CBIP hits, or both needing the product-level CBIP
        fallback) -- only the number of CNKs changes. A per-CNK loop would add
        statements per extra CNK; batched IN(...) queries do not.
        """
        _, n_one_direct = _count_sql_statements(server.get_pack_overview, [CNK_REIMBURSED_WITH_HISTORY])
        _, n_two_direct = _count_sql_statements(
            server.get_pack_overview, [CNK_REIMBURSED_WITH_HISTORY, CNK_REIMBURSED_DIRECT_CBIP]
        )
        self.assertEqual(n_one_direct, n_two_direct)

        _, n_one_fallback = _count_sql_statements(
            server.get_pack_overview, [CNK_NOT_REIMBURSED_CBIP_FALLBACK]
        )
        _, n_two_fallback = _count_sql_statements(
            server.get_pack_overview,
            [CNK_NOT_REIMBURSED_CBIP_FALLBACK, CNK_NOT_REIMBURSED_CBIP_FALLBACK],
        )
        self.assertEqual(n_one_fallback, n_two_fallback)


class TestGetReimbursement(DbBackedTestCase):
    def test_default_as_of_today_hides_expired_tranche(self):
        result = server.get_reimbursement(CNK_REIMBURSED_WITH_HISTORY)
        self.assertIsNotNone(result)
        self.assertTrue(all(t["is_current"] for t in result))
        self.assertTrue(all(t["valid_from"] != "2018-01-01" for t in result))

    def test_include_history_reveals_expired_tranche(self):
        result = server.get_reimbursement(CNK_REIMBURSED_WITH_HISTORY, include_history=True)
        self.assertIsNotNone(result)
        expired = [t for t in result if t["valid_from"] == "2018-01-01"]
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0]["valid_to"], "2018-03-31")
        self.assertFalse(expired[0]["is_current"])
        # And the currently active tranche(s) are still marked as such.
        self.assertTrue(any(t["is_current"] for t in result))

    def test_as_of_selects_the_matching_historical_tranche(self):
        result = server.get_reimbursement(CNK_REIMBURSED_WITH_HISTORY, as_of="2018-02-01")
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["valid_from"], "2018-01-01")
        self.assertTrue(result[0]["is_current"])

    def test_non_reimbursed_cnk_returns_none(self):
        self.assertIsNone(server.get_reimbursement(CNK_NOT_REIMBURSED_PARALLEL_IMPORT))
        self.assertIsNone(server.get_reimbursement(CNK_NOT_REIMBURSED_CBIP_FALLBACK))

    def test_unknown_cnk_returns_none(self):
        self.assertIsNone(server.get_reimbursement(CNK_UNKNOWN))


class TestGetLegalText(DbBackedTestCase):
    REF = "RD20180201-IV-10220000"

    def test_latest_only_returns_a_single_open_ended_version(self):
        result = server.get_legal_text(self.REF)
        self.assertEqual(len(result["versions"]), 1)
        version = result["versions"][0]
        self.assertIsNone(version["valid_to"])

    def test_full_history_groups_texts_by_version_without_interleaving(self):
        result = server.get_legal_text(self.REF, latest_only=False)
        versions = result["versions"]
        self.assertGreaterEqual(len(versions), 3)
        # Newest first.
        valid_froms = [v["valid_from"] for v in versions]
        self.assertEqual(valid_froms, sorted(valid_froms, reverse=True))
        # Every text in a version belongs to that version's own period --
        # this is precisely the bug: root periods used to get interleaved
        # because the old query sorted globally by sequence_nr.
        for version in versions:
            for text in version["texts"]:
                self.assertEqual(text["valid_from"], version["valid_from"])
                self.assertEqual(text["valid_to"], version["valid_to"])

        # The 2020-05-01 revision required a 12-month attestation; the
        # 2021-02-01 revision that superseded it shortened this to 4 months.
        # Both must be visible, undiluted, in their own version.
        by_valid_from = {v["valid_from"]: v for v in versions}
        old_version_text = " ".join(t["content_fr"] for t in by_valid_from["2020-05-01"]["texts"])
        new_version_text = " ".join(t["content_fr"] for t in by_valid_from["2021-02-01"]["texts"])
        self.assertIn("douze mois", old_version_text)
        self.assertIn("quatre mois", new_version_text)

    def test_unknown_key_returns_none(self):
        self.assertIsNone(server.get_legal_text("RD99999999-IV-99999999"))
        self.assertIsNone(server.get_legal_text("0"))


class TestGetLegalTextFlatSiblingGrouping(DbBackedTestCase):
    # Revlimid's chapter IV paragraph: 22 parentless sibling text_keys
    # sharing one validity window. Grouping by root text_key used to treat
    # each sibling as its own version, so latest_only=True (default) kept
    # only the last paragraph and silently dropped the other 21.
    REF = "RD20180201-IV-12410000"

    def test_latest_only_returns_every_paragraph_in_one_version(self):
        result = server.get_legal_text(self.REF)
        self.assertEqual(len(result["versions"]), 1)
        texts = result["versions"][0]["texts"]
        self.assertEqual(len(texts), 22)
        self.assertEqual([t["sequence_nr"] for t in texts], list(range(1, 23)))

    def test_full_history_is_the_same_single_version(self):
        result = server.get_legal_text(self.REF, latest_only=False)
        self.assertEqual(len(result["versions"]), 1)
        self.assertEqual(len(result["versions"][0]["texts"]), 22)


class TestStrengthMissingFlag(DbBackedTestCase):
    def test_eliquis_1_5mg_has_no_strength_in_the_sam_source(self):
        ingredients = server.get_ingredients(AMP_ELIQUIS_1_5MG)
        active = [i for i in ingredients if i["type"] == "ACTIVE_SUBSTANCE"]
        self.assertEqual(len(active), 1)
        self.assertTrue(active[0]["strength_missing"])
        self.assertIsNone(active[0]["strength_quantity"])

    def test_sibling_strengths_are_not_flagged(self):
        ingredients = server.get_ingredients(AMP_ELIQUIS_2MG)
        active = [i for i in ingredients if i["type"] == "ACTIVE_SUBSTANCE"]
        self.assertEqual(len(active), 1)
        self.assertFalse(active[0]["strength_missing"])
        self.assertEqual(active[0]["strength_quantity"], "2.0000")


class TestMultiComponentFlag(DbBackedTestCase):
    def test_qlaira_reports_five_components(self):
        result = server.get_pack_overview([CNK_QLAIRA_MULTICOMPONENT])
        identity = result["packs"][CNK_QLAIRA_MULTICOMPONENT]["identity"]
        self.assertEqual(identity["component_count"], 5)
        self.assertTrue(identity["multi_component"])

    def test_mono_component_pack_is_not_flagged(self):
        result = server.get_pack_overview([CNK_REIMBURSED_WITH_HISTORY])
        identity = result["packs"][CNK_REIMBURSED_WITH_HISTORY]["identity"]
        self.assertEqual(identity["component_count"], 1)
        self.assertFalse(identity["multi_component"])


class TestCbipPricePlaceholder(DbBackedTestCase):
    def test_zero_price_packs_are_flagged_and_nulled(self):
        for cnk in (CNK_CBIP_ZERO_PRICE_A, CNK_CBIP_ZERO_PRICE_B):
            with self.subTest(cnk=cnk):
                result = server.get_pack_overview([cnk])
                cbip = result["packs"][cnk]["cbip"]
                self.assertIsNotNone(cbip)
                self.assertTrue(cbip["cbip_price_placeholder"])
                for field in ("public_price", "index", "rema", "remw"):
                    self.assertIsNone(cbip[field])

    def test_get_cbip_notes_also_flags_zero_price(self):
        notes = server.get_cbip_notes(CNK_CBIP_ZERO_PRICE_A)
        self.assertTrue(notes["cbip_price_placeholder"])
        self.assertIsNone(notes["public_price"])

    def test_real_priced_pack_is_not_flagged(self):
        result = server.get_pack_overview([CNK_REIMBURSED_WITH_HISTORY])
        cbip = result["packs"][CNK_REIMBURSED_WITH_HISTORY]["cbip"]
        self.assertFalse(cbip["cbip_price_placeholder"])
        self.assertIsNotNone(cbip["public_price"])

    def test_product_level_fallback_is_not_flagged(self):
        # product_level already nulls public_price for an unrelated reason
        # (no pack-specific data at that CNK at all) -- the placeholder flag
        # must not also fire on top of that.
        result = server.get_pack_overview([CNK_NOT_REIMBURSED_CBIP_FALLBACK])
        cbip = result["packs"][CNK_NOT_REIMBURSED_CBIP_FALLBACK]["cbip"]
        self.assertEqual(cbip["coverage"], "product_level")
        self.assertFalse(cbip["cbip_price_placeholder"])


if __name__ == "__main__":
    unittest.main()
