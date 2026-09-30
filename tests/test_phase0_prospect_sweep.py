"""Phase 0 PROSPECT sweep (Defect 2, 2026-06-24).

Acceptance detection historically only scanned CONNECTION_SENT rows, so a
LinkedIn 1st-degree connection sitting at PROSPECT (connected outside the
tracked invite flow, or they invited us) was invisible and never entered the
DM cadence. The sweep degree-checks PROSPECT rows too and flips the
genuinely-accepted ones to ACCEPTED — but with a hard safety gate:

  * Pattern B (zero engagement, externally connected) → flip to ACCEPTED.
  * Pattern A (1st-degree but already carries DM depth) → NEVER flip (would
    wipe cadence depth); escalate `prospect_first_degree_with_depth` instead
    and leave the row at PROSPECT for the repair tooling.

The total scrape batch stays capped at PHASE0_MAX_PROFILES_PER_LAUNCH with
CONNECTION_SENT priority (flat PB cost).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from models.pipeline import PipelineStage
from workflows.daily_check import (
    PHASE0_MAX_PROFILES_PER_LAUNCH,
    PHASE0_PROSPECT_MIN_BUDGET,
    _prospect_accept_disposition,
)

_CSV_HEADER = "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"


def _prospect(idx: int, **overrides) -> dict:
    """A PROSPECT-stage parsed entry with all the engagement attrs parse_entry
    exposes. Defaults to ZERO engagement (the Pattern-B flip case)."""
    attrs = {
        "entry_id": f"p-entry-{idx}",
        "record_id": f"p-rec-{idx}",
        "stage": PipelineStage.PROSPECT.value,
        "dm_step": None,
        "dm1_sent_at": None,
        "dm2_sent_at": None,
        "dm3_sent_at": None,
        "response_received_at": None,
        "experiment_id": None,
        "last_contact_date": None,
        # Sourced-gate marker: a pipeline-committed prospect always carries
        # prospect_committed_at. Present by default so the sweep collects the
        # fixture; pass prospect_committed_at=None to model a manual/organic
        # connection the sweep must NOT touch.
        "prospect_committed_at": "2026-06-01T00:00:00+00:00",
        "linkedin_url": f"https://www.linkedin.com/in/prospect{idx:03d}/",
    }
    attrs.update(overrides)
    return attrs


def _conn_sent(idx: int, days_ago: int = 1) -> dict:
    sent = (date.today() - timedelta(days=days_ago)).isoformat()
    return {
        "entry_id": f"c-entry-{idx}",
        "record_id": f"c-rec-{idx}",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "dm_step": None,
        "dm1_sent_at": None,
        "dm2_sent_at": None,
        "dm3_sent_at": None,
        "response_received_at": None,
        "experiment_id": None,
        "last_contact_date": sent,
        "linkedin_url": f"https://www.linkedin.com/in/conn{idx:03d}/",
    }


def _csv_for(entries: list[dict], degree_by_url: dict[str, str]) -> str:
    rows = "".join(
        f"{e['linkedin_url']},{degree_by_url.get(e['linkedin_url'], '2nd')},"
        f"false,{e['linkedin_url']},Person {i}\n"
        for i, e in enumerate(entries)
    )
    return _CSV_HEADER + rows


def _mock_pb(csv_text: str) -> MagicMock:
    pb = MagicMock()
    pb.get_agent.return_value = {
        "argument": json.dumps(
            {"numberOfProfilesPerLaunch": 10, "saveImg": False,
             "identities": [{"identityId": "i"}], "spreadsheetUrl": "old"}
        )
    }
    pb.launch_agent.return_value = MagicMock(container_id="ct-prospect")
    pb.wait_for_completion.return_value = MagicMock(
        log_output="Scraped profiles. CSV saved."
    )
    pb.download_result_csv.return_value = csv_text
    return pb


def _run_phase0(entries, pb, monkeypatch, *, backend="sales_nav", last_checked_by_url=None):
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

    advance_calls: list[dict] = []

    def _capture_advance(**kwargs):
        advance_calls.append(kwargs)
        return True

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
            side_effect=_capture_advance,
        ),
    ):
        mock_rc.partition.return_value = ({}, [e["linkedin_url"] for e in entries])
        mock_rc.RECHECK_TTL_DAYS = 3
        if last_checked_by_url is not None:
            mock_rc.last_checked.side_effect = lambda u: last_checked_by_url.get(u, "")
        else:
            mock_rc.last_checked.return_value = ""  # sortable; real impl reads the cache
        result = detect_accepted_connections(
            MagicMock(), pb, profile_scraper_id="legacy",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-id",
        )
    return result, mock_rc, sheet_calls, mock_esc, advance_calls


def _run_phase0_cached(entries, monkeypatch, *, cached_degree_by_url):
    """Drive Phase 0 with the recheck cache supplying degrees (no PB scrape)."""
    from workflows.daily_check import detect_accepted_connections

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-id")
    url_by_record = {e["record_id"]: e["linkedin_url"] for e in entries}
    mock_cache = MagicMock()
    mock_cache.get.side_effect = lambda rid: (
        "Person", None, url_by_record.get(rid), None, None
    )
    advance_calls: list[dict] = []

    def _capture_advance(**kwargs):
        advance_calls.append(kwargs)
        return True

    fresh = {url: {"degree": deg} for url, deg in cached_degree_by_url.items()}
    pb = MagicMock()

    with (
        patch("workflows.daily_check._get_all_entries_parsed", return_value=entries),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch("workflows.daily_check.escalate") as mock_esc,
        patch(
            "workflows.daily_check._normalize_linkedin_url",
            side_effect=lambda u: u,
        ),
        patch(
            "workflows.daily_check._attio_advance_with_escalation",
            side_effect=_capture_advance,
        ),
    ):
        # All entries are cache hits → no stale URLs → no PB launch.
        mock_rc.partition.return_value = (fresh, [])
        mock_rc.RECHECK_TTL_DAYS = 3
        mock_rc.last_checked.return_value = ""  # sortable; real impl reads the cache
        result = detect_accepted_connections(
            MagicMock(), pb, profile_scraper_id="legacy",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-id",
        )
    return result, pb, mock_esc, advance_calls


# ── Disposition helper (unit) ────────────────────────────────────────────────

def test_disposition_zero_engagement_flips():
    assert _prospect_accept_disposition(_prospect(0)) == "flip"


def test_disposition_dm_step_present_is_regression():
    assert _prospect_accept_disposition(_prospect(0, dm_step="dm1")) == "regression"


def test_disposition_any_sent_at_is_regression():
    assert _prospect_accept_disposition(
        _prospect(0, dm1_sent_at="2026-06-01")
    ) == "regression"
    assert _prospect_accept_disposition(
        _prospect(0, dm3_sent_at="2026-06-01")
    ) == "regression"


def test_disposition_response_received_is_regression():
    assert _prospect_accept_disposition(
        _prospect(0, response_received_at="2026-06-01")
    ) == "regression"


def test_disposition_dm0_slug_is_flip():
    # dm_step_int("dm0") == 0 — a non-DM state, not depth.
    assert _prospect_accept_disposition(_prospect(0, dm_step="dm0")) == "flip"


# ── Test 1: PROSPECT + 1st + zero engagement → flipped to ACCEPTED ───────────

def test_prospect_first_degree_zero_engagement_flips_to_accepted(monkeypatch):
    p = _prospect(0)
    pb = _mock_pb(_csv_for([p], {p["linkedin_url"]: "1st"}))
    result, _rc, _sheet, mock_esc, advance_calls = _run_phase0([p], pb, monkeypatch)

    assert result["prospects_checked"] == 1
    assert result["prospects_accepted"] == 1
    assert result["prospect_regressions_flagged"] == 0
    assert result["accepted"] == 1

    # The ACCEPTED advance fired with prior_stage="Prospect".
    assert len(advance_calls) == 1
    call = advance_calls[0]
    assert call["prior_stage"] == PipelineStage.PROSPECT.value
    assert call["entry_attributes"]["stage"] == PipelineStage.ACCEPTED.value
    assert call["step_label"] == "phase0_prospect_accepted"

    # No regression escalation opened.
    esc_types = [c.kwargs.get("type") for c in mock_esc.call_args_list]
    assert "prospect_first_degree_with_depth" not in esc_types


def test_prospect_first_degree_zero_engagement_flips_via_cache(monkeypatch):
    """Cache-hit path (no PB scrape) also flips a zero-engagement 1st-degree
    PROSPECT — cache flips are not capped and cover both stages."""
    p = _prospect(0)
    result, pb, mock_esc, advance_calls = _run_phase0_cached(
        [p], monkeypatch, cached_degree_by_url={p["linkedin_url"]: "1st"}
    )

    assert result["prospects_accepted"] == 1
    assert result["prospect_regressions_flagged"] == 0
    pb.launch_agent.assert_not_called()  # pure cache path, no scrape
    assert advance_calls[0]["prior_stage"] == PipelineStage.PROSPECT.value
    assert advance_calls[0]["entry_attributes"]["stage"] == PipelineStage.ACCEPTED.value


# ── Sourced-gate: a non-pipeline-sourced PROSPECT is never swept ─────────────

def test_unsourced_prospect_not_swept(monkeypatch):
    """A 1st-degree PROSPECT with NO prospect_committed_at (a manual/organic/
    imported connection we never sourced) must be excluded from the sweep —
    flipping it to ACCEPTED would drop it into the cold DM cadence."""
    p = _prospect(0, prospect_committed_at=None)
    pb = _mock_pb(_csv_for([p], {p["linkedin_url"]: "1st"}))
    result, _rc, _sheet, mock_esc, advance_calls = _run_phase0([p], pb, monkeypatch)

    assert result["prospects_checked"] == 0          # not a candidate
    assert result["prospects_accepted"] == 0
    assert advance_calls == []                        # never flipped
    pb.launch_agent.assert_not_called()               # never even scraped
    esc_types = [c.kwargs.get("type") for c in mock_esc.call_args_list]
    assert "prospect_first_degree_with_depth" not in esc_types


# ── Test 2: PROSPECT + 1st + depth → NOT flipped, escalated ──────────────────

def test_prospect_first_degree_with_depth_escalates_not_flips(monkeypatch):
    p = _prospect(0, dm_step="dm2", dm1_sent_at="2026-06-01", dm2_sent_at="2026-06-05")
    pb = _mock_pb(_csv_for([p], {p["linkedin_url"]: "1st"}))
    result, _rc, _sheet, mock_esc, advance_calls = _run_phase0([p], pb, monkeypatch)

    assert result["prospects_accepted"] == 0
    assert result["prospect_regressions_flagged"] == 1
    assert result["accepted"] == 0

    # No ACCEPTED advance — the row stays at PROSPECT.
    assert advance_calls == []

    # Escalation opened with the right type + payload.
    regression_calls = [
        c for c in mock_esc.call_args_list
        if c.kwargs.get("type") == "prospect_first_degree_with_depth"
    ]
    assert len(regression_calls) == 1
    payload = regression_calls[0].kwargs["payload"]
    assert payload["record_id"] == p["record_id"]
    assert payload["entry_id"] == p["entry_id"]
    assert payload["dm_step"] == "dm2"
    assert payload["dm1_sent_at_set"] is True
    assert payload["dm2_sent_at_set"] is True
    assert payload["dm3_sent_at_set"] is False
    assert payload["response_received_at_set"] is False


def test_prospect_depth_via_single_sent_at_escalates(monkeypatch):
    """Even dm_step==0 with a stray dm1_sent_at is a regression — depth is
    ANY of the timestamps, not just dm_step."""
    p = _prospect(0, dm_step="dm0", dm1_sent_at="2026-06-01")
    pb = _mock_pb(_csv_for([p], {p["linkedin_url"]: "1st"}))
    result, _rc, _sheet, mock_esc, advance_calls = _run_phase0([p], pb, monkeypatch)

    assert result["prospect_regressions_flagged"] == 1
    assert advance_calls == []


# ── Test 3: PROSPECT + 2nd/3rd → not flipped, not escalated ──────────────────

def test_prospect_second_degree_not_flipped_not_escalated(monkeypatch):
    p2 = _prospect(0)
    p3 = _prospect(1)
    pb = _mock_pb(_csv_for(
        [p2, p3], {p2["linkedin_url"]: "2nd", p3["linkedin_url"]: "3rd"}
    ))
    result, _rc, _sheet, mock_esc, advance_calls = _run_phase0([p2, p3], pb, monkeypatch)

    assert result["prospects_checked"] == 2
    assert result["prospects_accepted"] == 0
    assert result["prospect_regressions_flagged"] == 0
    assert advance_calls == []
    esc_types = [c.kwargs.get("type") for c in mock_esc.call_args_list]
    assert "prospect_first_degree_with_depth" not in esc_types


# ── Test 4: capacity sharing — CONNECTION_SENT priority, flat cap ────────────

def test_conn_sent_flood_reserves_prospect_floor(monkeypatch):
    """PR-208: when stale CONNECTION_SENT would fill the whole cap, the Defect-2
    PROSPECT sweep keeps a reserved floor (PHASE0_PROSPECT_MIN_BUDGET) instead of
    being starved to 0. Total batch stays ≤ cap."""
    conns = [_conn_sent(i) for i in range(PHASE0_MAX_PROFILES_PER_LAUNCH)]
    prospects = [_prospect(i) for i in range(20)]
    entries = conns + prospects
    degree_by_url = {e["linkedin_url"]: "2nd" for e in entries}
    pb = _mock_pb(_csv_for(entries, degree_by_url))
    result, _rc, sheet_calls, _esc, _adv = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    assert len(submitted) == PHASE0_MAX_PROFILES_PER_LAUNCH
    # PROSPECTs keep the reserved floor; CONNECTION_SENT takes the rest.
    prospect_urls = {p["linkedin_url"] for p in prospects}
    conn_urls = {c["linkedin_url"] for c in conns}
    assert len(submitted & prospect_urls) == PHASE0_PROSPECT_MIN_BUDGET
    assert len(submitted & conn_urls) == (
        PHASE0_MAX_PROFILES_PER_LAUNCH - PHASE0_PROSPECT_MIN_BUDGET
    )
    assert result["scraped"] == PHASE0_MAX_PROFILES_PER_LAUNCH
    assert result["prospects_accepted"] == 0


def test_light_conn_sent_lets_prospects_fill_remainder(monkeypatch):
    """When CONNECTION_SENT is light, PROSPECTs fill the remaining budget up to
    the shared cap — total scrape volume stays ≤ cap."""
    conns = [_conn_sent(i) for i in range(10)]
    prospects = [_prospect(i) for i in range(100)]
    entries = conns + prospects
    degree_by_url = {e["linkedin_url"]: "2nd" for e in entries}
    pb = _mock_pb(_csv_for(entries, degree_by_url))
    result, _rc, sheet_calls, _esc, _adv = _run_phase0(entries, pb, monkeypatch)

    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    # Total batch == cap (10 conn + 40 prospect).
    assert len(submitted) == PHASE0_MAX_PROFILES_PER_LAUNCH
    conn_urls = {c["linkedin_url"] for c in conns}
    prospect_urls = {p["linkedin_url"] for p in prospects}
    assert conn_urls <= submitted  # all 10 conn_sent present (priority)
    n_prospect_scraped = len(submitted & prospect_urls)
    assert n_prospect_scraped == PHASE0_MAX_PROFILES_PER_LAUNCH - 10


def test_prospect_tail_rotates_least_recently_checked_first(monkeypatch):
    """Under a binding prospect budget, the LEAST-recently-checked PROSPECTs are
    scraped first (round-robin) so a busy CONNECTION_SENT pipeline can't starve
    the same tail forever (MEDIUM-2). Never-checked ("") sorts ahead of dated.

    With the PR-208 reserved floor, a CONNECTION_SENT flood leaves the PROSPECT
    budget at exactly PHASE0_PROSPECT_MIN_BUDGET; here 5 extra stale PROSPECTs
    compete for those slots."""
    conns = [_conn_sent(i) for i in range(PHASE0_MAX_PROFILES_PER_LAUNCH)]
    prospects = [_prospect(i) for i in range(PHASE0_PROSPECT_MIN_BUDGET + 5)]
    entries = conns + prospects
    # The last 5 PROSPECTs were checked most recently → they LOSE the budgeted
    # slots; the first PHASE0_PROSPECT_MIN_BUDGET (never-checked, "") win.
    recent = prospects[PHASE0_PROSPECT_MIN_BUDGET:]
    last_checked = {
        p["linkedin_url"]: f"2026-06-2{i}" for i, p in enumerate(recent)
    }
    degree_by_url = {e["linkedin_url"]: "2nd" for e in entries}
    pb = _mock_pb(_csv_for(entries, degree_by_url))
    _result, _rc, sheet_calls, _esc, _adv = _run_phase0(
        entries, pb, monkeypatch, last_checked_by_url=last_checked
    )
    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    prospect_urls = {p["linkedin_url"] for p in prospects}
    scraped_prospects = submitted & prospect_urls
    assert len(scraped_prospects) == PHASE0_PROSPECT_MIN_BUDGET
    assert scraped_prospects == {
        p["linkedin_url"] for p in prospects[:PHASE0_PROSPECT_MIN_BUDGET]
    }
    assert scraped_prospects.isdisjoint({p["linkedin_url"] for p in recent})


def test_total_scrape_batch_never_exceeds_cap(monkeypatch):
    """Flat PB cost: even with both stages oversized, batch ≤ cap."""
    conns = [_conn_sent(i) for i in range(80)]
    prospects = [_prospect(i) for i in range(80)]
    entries = conns + prospects
    degree_by_url = {e["linkedin_url"]: "2nd" for e in entries}
    pb = _mock_pb(_csv_for(entries, degree_by_url))
    _result, _rc, sheet_calls, _esc, _adv = _run_phase0(entries, pb, monkeypatch)

    assert len(sheet_calls[0]) == PHASE0_MAX_PROFILES_PER_LAUNCH
    submitted = {row["profileUrl"] for row in sheet_calls[0]}
    # PR-208: CONNECTION_SENT keeps priority but PROSPECTs hold the reserved
    # floor — so the batch is (cap − reserve) conn + reserve prospect, still ≤ cap.
    conn_urls = {c["linkedin_url"] for c in conns}
    prospect_urls = {p["linkedin_url"] for p in prospects}
    assert len(submitted & prospect_urls) == PHASE0_PROSPECT_MIN_BUDGET
    assert len(submitted & conn_urls) == (
        PHASE0_MAX_PROFILES_PER_LAUNCH - PHASE0_PROSPECT_MIN_BUDGET
    )


# ── Mixed batch: conn_sent + prospect both flip in one pass ──────────────────

def test_mixed_batch_both_stages_flip(monkeypatch):
    c = _conn_sent(0)
    p = _prospect(0)
    pb = _mock_pb(_csv_for(
        [c, p], {c["linkedin_url"]: "1st", p["linkedin_url"]: "1st"}
    ))
    result, _rc, _sheet, _esc, advance_calls = _run_phase0([c, p], pb, monkeypatch)

    assert result["accepted"] == 2
    assert result["prospects_accepted"] == 1
    priors = sorted(call["prior_stage"] for call in advance_calls)
    assert priors == [PipelineStage.CONNECTION_SENT.value, PipelineStage.PROSPECT.value]
