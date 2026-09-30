"""Phase 0 must never request more profiles than the phantom's per-launch
schema max (2026-06-11 incident: 197 stale profiles → PB rejected the whole
launch with "numberOfProfilesPerLaunch => is more than maximum" → BLIND run
→ no recheck_cache stamping → identical oversized launch every run after).

The fix caps each run's scrape batch at PHASE0_MAX_PROFILES_PER_LAUNCH and
defers the rest to subsequent runs — scraped rows get stamped fresh, so the
deferred set rotates in instead of re-queueing. Within the cap the batch is
filled NEWEST-invite first after reserving PHASE0_EXPIRING_RESERVE slots for
the oldest about-to-expire invites (Leak-A fix, PR-209)."""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from models.pipeline import PipelineStage
from workflows.daily_check import PHASE0_MAX_PROFILES_PER_LAUNCH

_CSV_HEADER = "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"

# Verified 2026-06-11 against the live phantom (PB scripts/fetch, script id
# 11108): the Sales Navigator Profile Scraper argument schema declares
# numberOfProfilesPerLaunch maximum=150. The cap must stay at or under it.
_SN_SCRAPER_SCHEMA_MAX = 150


def _conn_sent_batch(n: int, days_ago: int = 1, start: int = 0) -> list[dict]:
    sent = (date.today() - timedelta(days=days_ago)).isoformat()
    return [
        {
            "entry_id": f"entry-{start + i}",
            "record_id": f"rec-{start + i}",
            "stage": PipelineStage.CONNECTION_SENT.value,
            "last_contact_date": sent,
            "experiment_id": None,
            "linkedin_url": f"https://www.linkedin.com/in/p{start + i:03d}/",
        }
        for i in range(n)
    ]


def _csv_for(entries: list[dict], degree: str = "2nd") -> str:
    return _CSV_HEADER + "".join(
        f"{e['linkedin_url']},{degree},false,{e['linkedin_url']},Person {i}\n"
        for i, e in enumerate(entries)
    )


def _mock_pb(csv_text: str) -> MagicMock:
    pb = MagicMock()
    pb.get_agent.return_value = {
        "argument": json.dumps(
            {"numberOfProfilesPerLaunch": 10, "saveImg": False,
             "identities": [{"identityId": "i"}], "spreadsheetUrl": "old"}
        )
    }
    pb.launch_agent.return_value = MagicMock(container_id="ct-cap")
    pb.wait_for_completion.return_value = MagicMock(
        log_output="Scraped profiles. CSV saved."
    )
    pb.download_result_csv.return_value = csv_text
    return pb


def _run_phase0(entries, pb, monkeypatch, *, backend="sales_nav"):
    from workflows.daily_check import detect_accepted_connections

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", backend)
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "ck")
    monkeypatch.setenv("PB_LI_SESSION_COOKIE", "ck")
    monkeypatch.setenv("PB_LI_USER_AGENT", "ua")
    url_by_record = {e["record_id"]: e["linkedin_url"] for e in entries}
    mock_cache = MagicMock()
    mock_cache.get.side_effect = lambda rid: (
        "Person", None, url_by_record.get(rid), None, None
    )
    sheet_calls: list[list[dict]] = []

    def _capture_sheet(rows, columns=None, include_header=True):
        assert include_header is False
        sheet_calls.append(rows)
        return "https://s"

    with (
        patch("workflows.daily_check._get_all_entries_parsed", return_value=entries),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            side_effect=_capture_sheet,
        ),
        patch("workflows.daily_check.escalate") as mock_esc,
        patch(
            "workflows.daily_check._normalize_linkedin_url",
            side_effect=lambda u: u,
        ),
        patch(
            "workflows.daily_check._attio_advance_with_escalation",
            return_value=True,
        ),
    ):
        mock_rc.partition.return_value = ({}, [e["linkedin_url"] for e in entries])
        mock_rc.RECHECK_TTL_DAYS = 3
        result = detect_accepted_connections(
            MagicMock(), pb, profile_scraper_id="legacy",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-id",
        )
    return result, mock_rc, sheet_calls, mock_esc


def test_cap_constant_within_phantom_schema_max():
    assert 0 < PHASE0_MAX_PROFILES_PER_LAUNCH <= _SN_SCRAPER_SCHEMA_MAX


def test_oversized_stale_set_launches_capped_batch(monkeypatch, capsys):
    """197 stale profiles (the 2026-06-11 prod set) must launch ≤ cap, not 197."""
    entries = _conn_sent_batch(197)
    # CSV echoes every URL: only the submitted batch may be matched/stamped.
    pb = _mock_pb(_csv_for(entries))
    result, mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    args = pb.launch_agent.call_args[0][1]
    # Launch arg is batch + the sheet header line PB counts as a processable
    # row (clients.google_sheets.profiles_per_launch); the submitted sheet
    # batch itself stays at the cap.
    assert args["numberOfProfilesPerLaunch"] == (
        PHASE0_MAX_PROFILES_PER_LAUNCH
    )
    assert len(sheet_calls) == 1
    assert len(sheet_calls[0]) == PHASE0_MAX_PROFILES_PER_LAUNCH

    assert "error" not in result
    assert result["scraped"] == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert result["deferred"] == 197 - PHASE0_MAX_PROFILES_PER_LAUNCH

    # Deferred rows must NOT be cache-stamped — stamping would suppress
    # their re-check for RECHECK_TTL_DAYS without any fresh observation.
    stamped = mock_rc.record_many.call_args[0][0]
    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    assert set(stamped) == submitted

    out = capsys.readouterr().out
    assert f"Capping Phase 0 scrape batch at {PHASE0_MAX_PROFILES_PER_LAUNCH}" in out


def test_oldest_invites_win_the_capped_slots(monkeypatch):
    """Rows nearest the acceptance-window edge get checked before they age out."""
    newer = _conn_sent_batch(PHASE0_MAX_PROFILES_PER_LAUNCH, days_ago=2)
    oldest = _conn_sent_batch(5, days_ago=13, start=900)
    entries = newer + oldest  # oldest listed LAST: order alone won't save them
    pb = _mock_pb(_csv_for(entries))
    result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    for e in oldest:
        assert e["linkedin_url"] in submitted
    assert result["deferred"] == 5


def test_newest_invites_win_when_backlog(monkeypatch):
    """Leak-A regression: on a backlog day the NEWEST invites must be scraped,
    not deferred behind the older tail. Fresh acceptances cluster in the first
    days post-invite — pure oldest-first (pre-PR-209) starved them."""
    mid = _conn_sent_batch(PHASE0_MAX_PROFILES_PER_LAUNCH, days_ago=8)
    newest = _conn_sent_batch(10, days_ago=1, start=900)
    entries = mid + newest  # 60 > cap → 10 must be deferred
    pb = _mock_pb(_csv_for(entries))
    result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    # Every newest invite is checked; the deferred band is the middle-aged tail.
    for e in newest:
        assert e["linkedin_url"] in submitted
    assert result["deferred"] == len(entries) - PHASE0_MAX_PROFILES_PER_LAUNCH


def test_expiring_reserve_still_checked_under_fresh_pressure(monkeypatch):
    """The expiring reserve guarantees about-to-age-out invites still get a
    last-chance check even when a large fresh pool competes for the budget."""
    from workflows.daily_check import PHASE0_EXPIRING_RESERVE

    newest = _conn_sent_batch(60, days_ago=1)
    expiring = _conn_sent_batch(5, days_ago=13, start=900)
    entries = newest + expiring  # expiring listed LAST: only the reserve saves them
    pb = _mock_pb(_csv_for(entries))
    result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    for e in expiring:  # 5 expiring ≤ reserve → all protected
        assert e["linkedin_url"] in submitted
    assert len(expiring) <= PHASE0_EXPIRING_RESERVE
    assert result["deferred"] == len(entries) - PHASE0_MAX_PROFILES_PER_LAUNCH


def test_expiring_reserve_is_capped_not_unbounded(monkeypatch):
    """When the expiring set exceeds the reserve, EXACTLY PHASE0_EXPIRING_RESERVE
    slots go to expiring invites — the reserve is a cap, so the newest invites
    keep the majority of the budget (regression guard against the reserve
    growing to swallow the fresh fill)."""
    from workflows.daily_check import PHASE0_EXPIRING_RESERVE

    newest = _conn_sent_batch(40, days_ago=1)
    expiring = _conn_sent_batch(20, days_ago=13, start=900)  # 20 > 15 reserve
    entries = newest + expiring
    pb = _mock_pb(_csv_for(entries))
    result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    expiring_urls = {e["linkedin_url"] for e in expiring}
    # Reserve caps the expiring slice; the remaining 35 go newest-first.
    assert len(submitted & expiring_urls) == PHASE0_EXPIRING_RESERVE
    assert len(sheet_calls[0]) == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert result["deferred"] == len(entries) - PHASE0_MAX_PROFILES_PER_LAUNCH


def test_batch_at_or_under_cap_is_untrimmed(monkeypatch, capsys):
    entries = _conn_sent_batch(PHASE0_MAX_PROFILES_PER_LAUNCH)
    pb = _mock_pb(_csv_for(entries))
    result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(entries, pb, monkeypatch)

    args = pb.launch_agent.call_args[0][1]
    assert args["numberOfProfilesPerLaunch"] == (
        PHASE0_MAX_PROFILES_PER_LAUNCH
    )
    assert len(sheet_calls[0]) == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert result["deferred"] == 0
    assert "Capping Phase 0 scrape batch" not in capsys.readouterr().out


def test_regular_backend_launch_is_capped_too(monkeypatch):
    entries = _conn_sent_batch(197)
    pb = _mock_pb(_csv_for(entries))
    _result, _mock_rc, sheet_calls, _mock_esc = _run_phase0(
        entries, pb, monkeypatch, backend="regular"
    )
    # Fork deviation from upstream #194: the legacy (``regular``) Profile
    # Scraper launch in this fork does NOT pass numberOfProfilesPerLaunch
    # (only spreadsheetUrl + csvName + session args), so the cap is
    # expressed by the trimmed sheet batch rather than that launch arg.
    # The sales_nav branch (the production backend) still asserts the arg.
    assert len(sheet_calls) == 1
    assert len(sheet_calls[0]) == PHASE0_MAX_PROFILES_PER_LAUNCH


# ── Degraded runs must surface the deferred backlog ──────────────────────────
# BLIND/no-CSV/timeout runs stamp nothing into the recheck cache, so the
# SAME capped head batch is re-picked every run while the escalation keeps
# firing — without profiles_deferred in the payload, the tail queued behind
# a poison head batch is invisible to the operator (the recurrence mode of
# the 2026-06-11 incident).


def test_blind_run_escalation_carries_deferred_backlog(monkeypatch):
    entries = _conn_sent_batch(197)
    pb = _mock_pb(_CSV_HEADER)  # header-only CSV: zero fresh rows → BLIND
    result, mock_rc, _sheet, mock_esc = _run_phase0(entries, pb, monkeypatch)

    assert result["error"] == "stale_scrape"
    assert result["deferred"] == 197 - PHASE0_MAX_PROFILES_PER_LAUNCH
    mock_rc.record_many.assert_not_called()

    payload = mock_esc.call_args.kwargs["payload"]
    assert payload["profiles_submitted"] == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert payload["profiles_deferred"] == 197 - PHASE0_MAX_PROFILES_PER_LAUNCH


def test_timeout_degrade_escalation_carries_deferred_backlog(monkeypatch):
    from clients.pb_envelope import PBRunTimeout

    entries = _conn_sent_batch(197)
    pb = _mock_pb(_CSV_HEADER)
    pb.wait_for_completion.side_effect = PBRunTimeout(
        container_id="ct-cap", agent_id="sn-id", elapsed_seconds=750
    )
    result, _mock_rc, _sheet, mock_esc = _run_phase0(entries, pb, monkeypatch)

    assert result["error"] == "pb_timeout"
    assert result["deferred"] == 197 - PHASE0_MAX_PROFILES_PER_LAUNCH

    assert mock_esc.call_args.kwargs["type"] == "phase0_scrape_timeout"
    payload = mock_esc.call_args.kwargs["payload"]
    assert payload["profiles_pending"] == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert payload["profiles_deferred"] == 197 - PHASE0_MAX_PROFILES_PER_LAUNCH
