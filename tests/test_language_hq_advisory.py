"""Unverified-language advisory in the DM dry-run preview.

The entry `language` attribute is seeded from the linked company's HQ
country. When that signal is undeterminable (no linked company, or the
company has no `hq_country_code`), the stored value is whatever the lane
defaults to — nobody verified it, and a flip-day misfire ships the wrong
language. The fail-closed guard (`language_mismatch_verdict`) deliberately
fails OPEN there, so this advisory is what surfaces the gap: per-row
warning plus a summary rollup, never a send gate.

The source-labelled variants (person override, HQ catch-all, broken
override) live in tests/test_person_language_override.py — this file pins
the HQ-unknown half.
"""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tests.test_integration import _attio_with_full_schema
from tests.test_run_dm_sequencing_trim import _entry, _fake_daily_run

_ENV = {
    "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake",
    "PHANTOMBUSTER_API_KEY": "fake", "PB_LI_SESSION_COOKIE": "fake-cookie",
    "PB_LI_USER_AGENT": "TestAgent/1.0", "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
}


def _run_sequencing(
    entries: list[dict],
    *,
    hq_country: str | None,
    company_id: str | None = "comp-1",
    person_language: object = None,
    get_message_mock=None,
    dry_run: bool = True,
    capsys,
):
    """Drive run_dm_sequencing over `entries` with all externals faked.

    `resolve_language` is NOT patched — the real resolver converts each
    entry's `language` code to a Language enum, so the guard sees realistic
    types. `expected_language_for_entry` resolves through a faked
    `_company_id_for_prospect` + `attio.company_hq_country_code`.

    Returns (result_dict, captured_out, escalate_mock).
    """
    from workflows.daily_check import run_dm_sequencing

    attio = _attio_with_full_schema()
    attio.company_hq_country_code.return_value = hq_country
    # Person-level language override. Default None = no override, which is
    # the normal answer on a narrow exception list. Set explicitly rather
    # than left as an auto-created Mock so the resolver sees a realistic
    # value, not a truthy sentinel.
    attio.person_language_override.return_value = person_language
    pb = MagicMock()
    pb.download_result_csv.return_value = ""

    cache = MagicMock()
    cache.get.return_value = (
        "Person Name", "Acme Corp", "https://www.linkedin.com/in/a0",
        "manufacturing", "Ops Director",
    )

    with patch.dict(os.environ, _ENV), \
            patch("workflows.daily_check._get_all_entries_with_raw",
                  return_value=([], list(entries))), \
            patch("workflows.daily_check.write_prospects_to_sheet",
                  return_value="https://sheet.example/x"), \
            patch("workflows.daily_check._pb_session_args", return_value={}), \
            patch("workflows.daily_check._company_id_for_prospect",
                  side_effect=lambda _attio, rid: f"{company_id}-{rid}" if company_id else None), \
            patch("workflows.daily_check.company_throttle_permits",
                  return_value=True), \
            patch(
                "workflows.daily_check.get_message",
                **({"new": get_message_mock} if get_message_mock is not None
                   else {"return_value": "Hola [firstName]"}),
            ), \
            patch("workflows.daily_check.personalize", return_value="Hola Jo"), \
            patch("workflows.daily_check.escalate") as escalate_mock, \
            patch("workflows.daily_check.emit_pb_silent_no_op"), \
            patch("workflows.daily_check.date") as mock_date:
        mock_date.today.return_value = date(2026, 5, 20)
        mock_date.fromisoformat = date.fromisoformat
        result = run_dm_sequencing(
            attio, pb, "sender-id",
            daily_run=_fake_daily_run(remaining=30),
            dry_run=dry_run, auto_confirm=True, cache=cache,
        )

    captured = capsys.readouterr()
    return result, captured.out + captured.err, escalate_mock


def _lane_entry(record_id: str = "a0", *, language: str = "es",
                scoring_lane: str = "enterprise_mode") -> dict:
    entry = _entry(record_id, "Accepted", "2026-05-18", dm_step=0)
    entry["language"] = language
    entry["scoring_lane"] = scoring_lane
    return entry


class TestLanguageHqUnknownAdvisory:
    """Advisory when the HQ-derived expectation is None: the stored language
    is a lane default nobody verified. Never gates the send."""

    def test_dry_run_hq_unknown_warns_and_counts(self, capsys):
        result, out, escalate_mock = _run_sequencing(
            [_lane_entry()], hq_country=None, capsys=capsys,
        )
        assert result["language_unverified"] == 1
        # Advisory only — the row still renders in the dry-run queue.
        assert result["dry_run"]["dm1"] == 1
        assert "lane-default" in out
        assert "no HQ country" in out
        escalate_mock.assert_not_called()

    def test_no_linked_company_also_advises(self, capsys):
        # The other undeterminable branch: nothing to derive an expectation
        # from at all.
        result, out, _ = _run_sequencing(
            [_lane_entry()], hq_country="MX", company_id=None, capsys=capsys,
        )
        assert result["language_unverified"] == 1
        assert "lane-default" in out

    def test_dry_run_hq_known_no_advisory(self, capsys):
        result, out, _ = _run_sequencing(
            [_lane_entry()], hq_country="MX", capsys=capsys,
        )
        assert result["language_unverified"] == 0
        assert "lane-default" not in out

    def test_us_mode_lane_never_advises(self, capsys):
        # us_mode's expectation is EN by construction, not HQ-derived — a
        # missing HQ country is irrelevant there.
        result, out, _ = _run_sequencing(
            [_lane_entry(language="en", scoring_lane="us_mode")],
            hq_country=None, capsys=capsys,
        )
        assert result["language_unverified"] == 0
        assert "lane-default" not in out

    def test_summary_rollup_reports_the_total(self, capsys):
        result, out, _ = _run_sequencing(
            [_lane_entry("a0"), _lane_entry("a1")],
            hq_country=None, capsys=capsys,
        )
        assert result["language_unverified"] == 2
        assert "2 queued DM(s) carry a language nothing corroborates" in out


class TestAdvisoryIsDryRunOnly:
    """A LANE_DEFAULT row is an inference nobody corroborated, not a broken
    signal, so it is review-time guidance only — the counter must not
    increment on a wet run, which has no review step to guide. (Broken
    overrides DO count wet; see test_person_language_override.py.)"""

    def test_wet_run_does_not_count(self, capsys):
        result, _out, _ = _run_sequencing(
            [_lane_entry()], hq_country=None, dry_run=False, capsys=capsys,
        )
        assert result["language_unverified"] == 0


@pytest.mark.parametrize("hq", [None, ""])
def test_blank_and_missing_hq_both_advise(hq, capsys):
    result, _out, _ = _run_sequencing(
        [_lane_entry()], hq_country=hq, capsys=capsys,
    )
    assert result["language_unverified"] == 1
