"""Wave-1.6 FIX-2 regression: regular invite path preserves prior
experiment_id and raises on overwrite.

Background — done-qa-advertiser-daily-pass1-round2 BLOCKING-1:
`workflows/daily_check.py:run_connection_requests` invite-success branch
silently overwrote any prior PROSPECT-stamped `experiment_id` with the
experiment running NOW. Production rows from 2026-04-07 / 2026-04-20
were observed carrying exp-20260511-* despite being committed under an
earlier baseline — proof the bug had been firing for weeks.

The Pattern-A flip path at workflows/pre_invite_check.py:464 was already
guarded; only the regular invite-success path was missing the check.
Wave-1.6 closes the gap.
"""
from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tests.fakes import fake_daily_run


def _make_attio_entry_prospect(
    *,
    entry_id: str,
    record_id: str,
    prior_experiment_id: str | None,
    prior_frozen_at: str | None = "prospect",
) -> dict:
    """Build a PROSPECT-stage Attio list entry with a stamped
    experiment_id — the case Wave-1.6 FIX-2 protects.
    """
    old_date = (date.today() - timedelta(days=30)).isoformat()
    entry_values = {
        "stage": [{"status": {"title": "Prospect"}}],
        "quality_score": [75],
        "persona": ["operations_leaders"],
        "language": ["es"],
        "last_contact_date": [old_date],
        "dm_step": [0],
    }
    if prior_experiment_id is not None:
        entry_values["experiment_id"] = [{"value": prior_experiment_id}]
    if prior_frozen_at is not None:
        entry_values["experiment_id_frozen_at"] = [{"value": prior_frozen_at}]
    return {
        "entry_id": entry_id,
        "parent_record_id": record_id,
        "created_at": f"{old_date}T00:00:00.000000000Z",
        "entry_values": entry_values,
    }


def _drive_invite_success(
    *,
    prior_experiment_id: str | None,
    current_experiment_id: str | None,
    prior_frozen_at: str | None = "prospect",
) -> dict:
    """Drive run_connection_requests through to the invite-success Attio
    advance branch. Returns the entry_attributes dict captured at
    `_attio_advance_with_escalation` (so the test can assert on
    `experiment_id` + `experiment_id_frozen_at`).

    May raise `ExperimentIdImmutableError` — the test surrounds the call
    with `pytest.raises` for the overwrite cases.
    """
    from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
    from workflows.daily_check import run_connection_requests

    url = "https://www.linkedin.com/in/test-invite-immutable/"
    pb = MagicMock()
    launch = PBLaunch(
        container_id="c-inv",
        agent_id="agent-nb",
        launched_at=datetime(2026, 5, 25, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )
    completion = PBCompletion(
        container_id="c-inv",
        status="finished",
        log_output="Invitation sent to test-invite-immutable",
        raw_output={"status": "finished", "output": ""},
    )
    pb.launch_agent.return_value = launch
    pb.wait_for_completion.return_value = completion
    pb.download_result_csv.return_value = f"query,status\n{url},Message sent\n"

    attio = MagicMock()
    attio.is_person_company_corrupted.return_value = False
    attio.query_list_entries.return_value = [
        _make_attio_entry_prospect(
            entry_id="ent-imm",
            record_id="rec-imm",
            prior_experiment_id=prior_experiment_id,
            prior_frozen_at=prior_frozen_at,
        ),
    ]

    captured: dict = {}

    def fake_advance(**kwargs):
        captured.update(kwargs.get("entry_attributes", {}))
        return True

    def _passthrough_pic(rows, *_a, **_kw):
        return rows, []

    with patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    }), \
         patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
         patch("workflows.daily_check.can_send_connections", return_value=True), \
         patch("workflows.daily_check.get_remaining",
               return_value={"connections": 25, "messages": 30, "visits": 50}), \
         patch("workflows.daily_check.record_connections"), \
         patch("workflows.daily_check.record_visits"), \
         patch("workflows.daily_check.get_current_experiment_id",
               return_value=current_experiment_id), \
         patch("workflows.daily_check._attio_advance_with_escalation",
               side_effect=fake_advance), \
         patch("workflows.daily_check._pre_invite_degree_check",
               side_effect=_passthrough_pic), \
         patch("workflows.daily_check.write_prospects_to_sheet",
               return_value="https://docs.google.com/spreadsheets/d/fake"), \
         patch("workflows.daily_check._write_company_throttle_tally"), \
         patch("workflows.daily_check._company_id_for_prospect",
               return_value="comp-1"), \
         patch("workflows.daily_check.ensure_throttle_policy_decision_opened"):
        mock_cache_get.return_value = ("Test Person", "Acme", url, "", "Plant Manager")
        run_connection_requests(
            attio=attio,
            pb=pb,
            network_booster_id="agent-nb",
            auto_confirm=True,
            daily_run=fake_daily_run(),
        )

    return captured


def test_regular_invite_path_preserves_prior_experiment_id():
    """Wave-1.6 FIX-2 / Wave-1.6-ext FIX-2' core regression: prior
    experiment_id is preserved when current differs.

    Wave-1.6 (commit 5910149) raised ExperimentIdImmutableError. The
    adversarial lens-QA round-2 audit (SB-4) found the raise orphans
    PB-sent rows downstream in the same batch — re-invite risk
    tomorrow. Wave-1.6-ext converts to escalate-and-continue: prior
    cohort preserved, stage still advances to CONNECTION_SENT, and an
    `experiment_id_immutability_violation` queue row opens for
    operator review."""
    with patch(
        "workflows.daily_check.escalate"
    ) as escalate_mock:
        captured = _drive_invite_success(
            prior_experiment_id="exp-A-from-prospect-commit",
            current_experiment_id="exp-B-running-now",
        )

    # Stage still advances — no re-invite tomorrow.
    assert captured.get("stage") == "Connection Sent"
    # Prior cohort preserved (the immutability the original raise was
    # protecting).
    assert captured.get("experiment_id") == "exp-A-from-prospect-commit"
    assert captured.get("experiment_id_frozen_at") == "connection_sent"

    # Operator queue row was opened.
    immut_calls = [
        c
        for c in escalate_mock.call_args_list
        if c.kwargs.get("type") == "experiment_id_immutability_violation"
    ]
    assert len(immut_calls) == 1, (
        f"expected exactly 1 experiment_id_immutability_violation "
        f"escalation; got {[c.kwargs.get('type') for c in escalate_mock.call_args_list]}"
    )
    payload = immut_calls[0].kwargs["payload"]
    assert payload["record_id"] == "rec-imm"
    assert payload["prior_experiment_id"] == "exp-A-from-prospect-commit"
    assert payload["current_experiment_id"] == "exp-B-running-now"
    assert payload["effective_experiment_id"] == "exp-A-from-prospect-commit"


def test_regular_invite_path_no_raise_when_prior_matches_current():
    """When prior == current, the write is idempotent — no raise, and
    experiment_id is preserved (same value either way)."""
    captured = _drive_invite_success(
        prior_experiment_id="exp-same",
        current_experiment_id="exp-same",
    )
    assert captured.get("stage") == "Connection Sent"
    assert captured.get("experiment_id") == "exp-same"
    assert captured.get("experiment_id_frozen_at") == "connection_sent"


def test_regular_invite_path_uses_current_when_prior_is_none():
    """Pre-PR-21 rows have no prior experiment_id. The path falls back to
    current_experiment_id — that's the original PR-21 stamping behavior
    and Wave-1.6 FIX-2 preserves it."""
    captured = _drive_invite_success(
        prior_experiment_id=None,
        current_experiment_id="exp-active",
        prior_frozen_at=None,
    )
    assert captured.get("stage") == "Connection Sent"
    assert captured.get("experiment_id") == "exp-active"
    assert captured.get("experiment_id_frozen_at") == "connection_sent"


def test_regular_invite_path_no_raise_when_no_active_experiment():
    """When current_experiment_id is None (no active experiment),
    overwriting cannot happen by definition. Prior is preserved."""
    captured = _drive_invite_success(
        prior_experiment_id="exp-A-from-prospect-commit",
        current_experiment_id=None,
    )
    assert captured.get("stage") == "Connection Sent"
    # Prior preserved, frozen_at stamp fires because effective is non-None.
    assert captured.get("experiment_id") == "exp-A-from-prospect-commit"
    assert captured.get("experiment_id_frozen_at") == "connection_sent"


def test_regular_invite_path_no_raise_means_no_orphans_mid_batch():
    """Wave-1.6-ext FIX-2' (adversarial SB-4): the original FIX-2 raise
    would orphan PB-sent rows downstream of the offending row in the
    same batch. After FIX-2' the function must NOT raise on
    experiment_id mismatch — it escalates and continues so all PB-sent
    rows in the batch land their CONNECTION_SENT advance.

    We assert no exception escapes and the row's stage still advances."""
    # If the function still raised, this would propagate as
    # ExperimentIdImmutableError and the assert below would never run.
    with patch("workflows.daily_check.escalate"):
        captured = _drive_invite_success(
            prior_experiment_id="exp-A-old-cohort",
            current_experiment_id="exp-B-new-cohort",
        )

    assert captured.get("stage") == "Connection Sent", (
        "FIX-2' regression: the row's stage did not advance to "
        "CONNECTION_SENT despite no raise. This means PB sent the "
        "invite but Attio still shows PROSPECT — tomorrow's run will "
        "re-invite (§3.1 violation)."
    )


def test_regular_invite_path_escalate_raise_does_not_orphan_stage_advance():
    """Wave-1.6.2 FIX-A (adversarial EXT-SB-1 BLOCKING): if escalate()
    itself raises (transient Attio 5xx, network blip, payload schema
    drift during F1 deploy race, etc.), the loop's stage-advance step
    MUST still fire for the affected row. Without this guarantee, an
    escalate failure mid-batch orphans PB-sent rows that re-invite
    tomorrow — the exact §3.1 red-line FIX-2' was sold as fixing.

    Setup: experiment_id mismatch ⇒ escalate() invoked ⇒ patch it to
    raise httpx.HTTPStatusError ⇒ assert the stage still advances and
    the function does NOT re-raise.
    """
    import httpx

    response = MagicMock(spec=httpx.Response)
    response.status_code = 503
    response.text = '{"error": "service unavailable"}'

    with patch(
        "workflows.daily_check.escalate",
        side_effect=httpx.HTTPStatusError(
            "503 Service Unavailable", request=MagicMock(), response=response
        ),
    ):
        captured = _drive_invite_success(
            prior_experiment_id="exp-A-from-prospect-commit",
            current_experiment_id="exp-B-running-now",
        )

    # Stage MUST still advance even though escalate() raised — without
    # this, PB sent the invite but Attio still shows PROSPECT and
    # tomorrow's run re-invites (§3.1 violation, the very thing FIX-2'
    # was supposed to prevent).
    assert captured.get("stage") == "Connection Sent", (
        "FIX-A regression: escalate() raise terminated the loop and "
        "the row's stage was never advanced to CONNECTION_SENT. "
        "Tomorrow's run will re-invite. §3.1 violation."
    )
    # Prior cohort STILL preserved at the data layer (the immutability
    # semantic the original FIX-2' protects).
    assert captured.get("experiment_id") == "exp-A-from-prospect-commit"
    assert captured.get("experiment_id_frozen_at") == "connection_sent"


def test_regular_invite_path_escalate_raise_does_not_orphan_other_rows():
    """Wave-1.6.2 FIX-A (adversarial EXT-SB-1 BLOCKING): when escalate()
    raises for one row mid-batch, OTHER rows in the same batch must
    still get their stage advance. Without this, a single escalate
    failure orphans every PB-sent row after the offender — the
    fan-out version of the §3.1 red-line.

    Setup: 2-row batch. Row 1 has mismatched experiment_id and triggers
    escalate(). Row 2 has matching experiment_id. We patch escalate to
    raise; assert BOTH rows land their stage advance.
    """
    import httpx

    from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
    from workflows.daily_check import run_connection_requests

    url1 = "https://www.linkedin.com/in/orphan-test-1/"
    url2 = "https://www.linkedin.com/in/orphan-test-2/"

    pb = MagicMock()
    launch = PBLaunch(
        container_id="c-multi",
        agent_id="agent-nb",
        launched_at=datetime(2026, 5, 25, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )
    completion = PBCompletion(
        container_id="c-multi",
        status="finished",
        log_output="Invitation sent to orphan-test-1\nInvitation sent to orphan-test-2",
        raw_output={"status": "finished", "output": ""},
    )
    pb.launch_agent.return_value = launch
    pb.wait_for_completion.return_value = completion
    pb.download_result_csv.return_value = (
        f"query,status\n{url1},Message sent\n{url2},Message sent\n"
    )

    attio = MagicMock()
    attio.is_person_company_corrupted.return_value = False
    attio.query_list_entries.return_value = [
        _make_attio_entry_prospect(
            entry_id="ent-1",
            record_id="rec-1",
            prior_experiment_id="exp-A-mismatch",  # triggers escalate
            prior_frozen_at="prospect",
        ),
        _make_attio_entry_prospect(
            entry_id="ent-2",
            record_id="rec-2",
            prior_experiment_id="exp-current",  # matches → no escalate
            prior_frozen_at="prospect",
        ),
    ]

    advanced_entry_ids: list[str] = []

    def fake_advance(**kwargs):
        advanced_entry_ids.append(kwargs.get("entry_id"))
        return True

    def _passthrough_pic(rows, *_a, **_kw):
        return rows, []

    response = MagicMock(spec=httpx.Response)
    response.status_code = 503
    response.text = '{"error": "service unavailable"}'

    cache_lookup = {
        "rec-1": ("Person 1", "Acme", url1, "", "Plant Manager"),
        "rec-2": ("Person 2", "Acme", url2, "", "Plant Manager"),
    }

    with patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    }), \
         patch("workflows.daily_check.RecordCache.get",
               side_effect=lambda rid: cache_lookup.get(rid)), \
         patch("workflows.daily_check.can_send_connections", return_value=True), \
         patch("workflows.daily_check.get_remaining",
               return_value={"connections": 25, "messages": 30, "visits": 50}), \
         patch("workflows.daily_check.record_connections"), \
         patch("workflows.daily_check.record_visits"), \
         patch("workflows.daily_check.get_current_experiment_id",
               return_value="exp-current"), \
         patch("workflows.daily_check._attio_advance_with_escalation",
               side_effect=fake_advance), \
         patch("workflows.daily_check._pre_invite_degree_check",
               side_effect=_passthrough_pic), \
         patch("workflows.daily_check.write_prospects_to_sheet",
               return_value="https://docs.google.com/spreadsheets/d/fake"), \
         patch("workflows.daily_check._write_company_throttle_tally"), \
         patch("workflows.daily_check._company_id_for_prospect",
               side_effect=lambda _attio, rid: f"comp-{rid}"), \
         patch("workflows.daily_check.ensure_throttle_policy_decision_opened"), \
         patch("workflows.daily_check.escalate",
               side_effect=httpx.HTTPStatusError(
                   "503 Service Unavailable",
                   request=MagicMock(),
                   response=response,
               )):
        run_connection_requests(
            attio=attio,
            pb=pb,
            network_booster_id="agent-nb",
            auto_confirm=True,
            daily_run=fake_daily_run(),
        )

    # BOTH rows must have been advanced — escalate failure on row 1 must
    # not orphan row 2's stage flip.
    assert "ent-1" in advanced_entry_ids, (
        "FIX-A regression: row 1 (the one that triggered escalate) was "
        "not advanced after escalate raised — its stage stays at "
        "PROSPECT and PB-sent invite re-fires tomorrow."
    )
    assert "ent-2" in advanced_entry_ids, (
        "FIX-A regression: row 2 (downstream of the escalate-raising "
        "row) was orphaned — the for-loop terminated mid-batch. This "
        "is the exact §3.1 hard red-line FIX-2' was sold as fixing."
    )


@pytest.fixture(autouse=True)
def _fresh_state_boundary():
    from unittest.mock import patch

    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
