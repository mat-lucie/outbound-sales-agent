"""repair_bad_companies must never request more profiles than the phantom's
per-launch schema max (sibling of the 2026-06-11 Phase 0 incident: PB
validates numberOfProfilesPerLaunch against the phantom's argument schema and
rejects the ENTIRE launch with "numberOfProfilesPerLaunch => is more than
maximum"). Pre-fix, repair passed the full detect-CSV row count, so a 150+
backlog of bad company links would reject the whole repair launch.

The fix caps each run's scrape batch at REPAIR_MAX_PROFILES_PER_LAUNCH and
defers the tail; re-running detect-bad-companies drops repaired rows from the
new CSV, so repeated detect→repair cycles converge."""

from __future__ import annotations

import csv
from unittest.mock import MagicMock, patch

from workflows.backfill_companies import (
    REPAIR_MAX_PROFILES_PER_LAUNCH,
    repair_bad_companies,
)

# Verified 2026-06-11 via the PB API (scripts/fetch → argumentSchema): the
# Sales Navigator Profile Scraper (script 11108) declares
# numberOfProfilesPerLaunch maximum=150 — the lowest verified max among the
# org's profile scrapers (the legacy Profile Scraper agent is deleted, so its
# schema is unverifiable). The repair cap must stay at or under it.
_SN_SCRAPER_SCHEMA_MAX = 150

_PB_CSV = (
    "linkedinProfileUrl,companyName,companyUrl\n"
    "https://www.linkedin.com/in/p000/,Acme,https://acme.com\n"
)


def _write_detect_csv(tmp_path, n: int) -> str:
    path = tmp_path / "detect.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["linkedin_url", "attio_record_id", "name", "current_wrong_company"],
        )
        writer.writeheader()
        writer.writerows(
            {
                "linkedin_url": f"https://www.linkedin.com/in/p{i:03d}/",
                "attio_record_id": f"rec-{i}",
                "name": f"Person {i}",
                "current_wrong_company": "LinkedIn",
            }
            for i in range(n)
        )
    return str(path)


def _mock_attio() -> MagicMock:
    attio = MagicMock()
    attio.search_company_by_domain.return_value = None  # Step 0: already clean
    return attio


def _mock_pb(result_csv: str = _PB_CSV) -> MagicMock:
    pb = MagicMock()
    # Saved SN phantom argument — the repair launch fetches it and injects
    # the session into identities[0] (SN migration).
    pb.get_agent.return_value = {"argument": '{"identities": [{}]}'}
    pb.launch_agent.return_value = MagicMock(container_id="ct-repair")
    pb.wait_for_completion.return_value = MagicMock(log_output="done")
    pb.download_result_csv.return_value = result_csv
    return pb


def _run_repair(tmp_path, monkeypatch, n_rows: int, pb: MagicMock | None = None):
    """Run repair_bad_companies with PB/Sheets/import boundaries mocked.

    Returns (summary, pb, sheet_calls, import_mock).
    """
    detect_csv = _write_detect_csv(tmp_path, n_rows)
    # The success path writes the PB output to the relative ``exports/`` dir —
    # sandbox it so tests never touch the real one.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "exports").mkdir()
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-li-at")
    pb = pb or _mock_pb()
    sheet_calls: list[list[dict]] = []

    def _capture_sheet(rows, columns=None, include_header=True):
        assert include_header is False
        sheet_calls.append(rows)
        return "https://s"

    import_counters = {"processed": 1, "linked": 1, "failed": 0, "skipped": 0}
    with (
        patch("clients.google_sheets.write_prospects_to_sheet", side_effect=_capture_sheet),
        patch(
            "workflows.backfill_companies.backfill_import",
            return_value=dict(import_counters),
        ) as import_mock,
    ):
        summary = repair_bad_companies(_mock_attio(), pb, "sn-scraper-id", detect_csv)
    return summary, pb, sheet_calls, import_mock


def test_cap_stays_at_or_under_verified_schema_max():
    assert REPAIR_MAX_PROFILES_PER_LAUNCH <= _SN_SCRAPER_SCHEMA_MAX


def test_oversized_backlog_is_capped_and_tail_deferred(tmp_path, monkeypatch):
    n = REPAIR_MAX_PROFILES_PER_LAUNCH + 17
    summary, pb, sheet_calls, _ = _run_repair(tmp_path, monkeypatch, n)

    # The launch argument is the capped batch + the sheet header line PB
    # counts as a processable row (clients.google_sheets.profiles_per_launch);
    # cap + header must stay under the phantom's schema max or PB rejects the
    # whole launch.
    launch_args = pb.launch_agent.call_args.args[1]
    assert launch_args["numberOfProfilesPerLaunch"] == (
        REPAIR_MAX_PROFILES_PER_LAUNCH
    )

    # The sheet feeding the phantom holds only the capped head batch.
    assert len(sheet_calls) == 1
    assert len(sheet_calls[0]) == REPAIR_MAX_PROFILES_PER_LAUNCH

    # The tail is surfaced as deferred, not silently dropped.
    assert summary["deferred"] == 17


def test_within_cap_batch_passes_through_unchanged(tmp_path, monkeypatch):
    summary, pb, sheet_calls, _ = _run_repair(tmp_path, monkeypatch, 5)

    launch_args = pb.launch_agent.call_args.args[1]
    assert launch_args["numberOfProfilesPerLaunch"] == (5)
    assert len(sheet_calls[0]) == 5
    assert summary["deferred"] == 0


def test_batch_exactly_at_cap_is_not_deferred(tmp_path, monkeypatch):
    summary, pb, _, _ = _run_repair(tmp_path, monkeypatch, REPAIR_MAX_PROFILES_PER_LAUNCH)

    launch_args = pb.launch_agent.call_args.args[1]
    assert launch_args["numberOfProfilesPerLaunch"] == (
        REPAIR_MAX_PROFILES_PER_LAUNCH
    )
    assert summary["deferred"] == 0


def test_empty_detect_csv_returns_deferred_zero_shape(tmp_path):
    detect_csv = _write_detect_csv(tmp_path, 0)
    summary = repair_bad_companies(_mock_attio(), _mock_pb(), "sn-scraper-id", detect_csv)
    assert summary == {
        "processed": 0,
        "linked": 0,
        "failed": 0,
        "skipped": 0,
        "deferred": 0,
    }


def test_clean_empty_detect_csv_has_no_error_key(tmp_path):
    """The zero-counter SUCCESS shape (empty detect CSV) must not carry the
    failure marker — cli.py would otherwise report a clean run as FAILED."""
    detect_csv = _write_detect_csv(tmp_path, 0)
    summary = repair_bad_companies(_mock_attio(), _mock_pb(), "sn-scraper-id", detect_csv)
    assert "error" not in summary


def test_no_csv_failure_still_reports_deferred(tmp_path, monkeypatch):
    """A failed scrape on an oversized backlog must still surface the deferred
    count — the operator needs to know the backlog behind the failed head
    batch (mirror of the Phase 0 stale-scrape payload rationale)."""
    n = REPAIR_MAX_PROFILES_PER_LAUNCH + 3
    summary, _, _, import_mock = _run_repair(tmp_path, monkeypatch, n, pb=_mock_pb(result_csv=""))
    assert summary["deferred"] == 3
    assert summary["linked"] == 0
    import_mock.assert_not_called()
    # The "error" key distinguishes a failed scrape from a clean run over an
    # already-empty detect CSV (identical zero counters) — cli.py keys its
    # "Repair FAILED" banner + non-zero exit on it.
    assert summary["error"] == "no_csv"


def test_repair_launch_carries_sn_full_argument_contract(tmp_path, monkeypatch):
    """The repair launch must carry the SN phantom's full-argument contract:
    saved fields preserved, fresh session injected into identities[0] (NOT
    top-level sessionCookie, which the SN phantom silently ignores), and a
    fresh csvName to bust the processed-inputs dedup DB so a repair re-scrape
    re-visits profiles a prior daily-run scrape already processed."""
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")
    pb = _mock_pb()
    pb.get_agent.return_value = {
        "argument": '{"identities": [{}], "savedField": "keep-me"}'
    }
    summary, pb, sheet_calls, _ = _run_repair(tmp_path, monkeypatch, 4, pb=pb)

    args = pb.launch_agent.call_args.args
    assert args[0] == "sn-scraper-id"
    launch_args = args[1]
    assert launch_args["numberOfProfilesPerLaunch"] == (4)
    # SN full-argument contract.
    assert launch_args["savedField"] == "keep-me"
    assert launch_args["identities"][0]["sessionCookie"] == "fake-sn-li-at"
    assert launch_args["identities"][0]["userAgent"] == "fake-ua"
    assert "sessionCookie" not in launch_args
    # Fresh result-file name per launch, and the CSV download keyed to it.
    assert launch_args.get("csvName"), "repair launch must set a fresh csvName"
    assert pb.download_result_csv.call_args.kwargs.get("csv_name") == launch_args["csvName"]
