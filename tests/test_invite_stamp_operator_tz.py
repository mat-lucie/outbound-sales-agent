"""Regression: ``run_connection_requests`` stamps post-send dates with the
operator-local date it was given (``today`` / ``operator_today()``), NOT the
system UTC ``date.today()``.

An invite sent near operator-TZ midnight must record ``last_contact_date`` in
the same timezone frame as the eligibility / cadence gate (which uses
``today_op``). Otherwise the stamp can land a day ahead (UTC) and shift the
whole DM cadence + throttle window by a day. See the operator_today() convention
used for the Phase-0 timeout key in the same module.

The test drives the real function to the Pattern-A advance path and asserts the
date handed to ``_advance_already_processed_rows`` is the operator ``today``,
not UTC ``date.today()``.
"""
from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from tests.fakes import fake_daily_run

# Network Booster log line that triggers the input-already-processed (Pattern-A)
# advance branch for an invites-only batch.
_MARKER = (
    "✅ Got 1 line from csv.\n"
    "⚠️ We already processed every profile from this spreadsheet.\n"
)


def _entry(*, entry_id: str, record_id: str, stage_title: str, ref_day: date) -> dict:
    old_date = (ref_day - timedelta(days=30)).isoformat()
    return {
        "entry_id": entry_id,
        "parent_record_id": record_id,
        "created_at": f"{old_date}T00:00:00.000000000Z",
        "entry_values": {
            "stage": [{"status": {"title": stage_title}}],
            "quality_score": [75],
            "persona": ["operations_leaders"],
            "language": ["es"],
            "last_contact_date": [old_date],
            "dm_step": [0],
            "experiment_id": [{"value": "exp-1"}],
            "experiment_id_frozen_at": [{"value": "prospect"}],
        },
    }


def test_invite_advance_stamps_operator_today_not_utc():
    from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
    from workflows.daily_check import run_connection_requests

    # An operator-local "today" deliberately far from the real UTC date.today(),
    # so a date.today()-based stamp is unambiguously distinguishable from one
    # that respects the operator date. Offset keeps the test non-flaky (never
    # accidentally equals the wall-clock date).
    operator_day = date.today() - timedelta(days=400)
    assert operator_day != date.today()

    invite_url = "https://www.linkedin.com/in/invite-target/"

    pb = MagicMock()
    launch = PBLaunch(
        container_id="c-gate",
        agent_id="agent-nb",
        launched_at=datetime(2026, 5, 29, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )
    pb.launch_agent.return_value = launch
    pb.wait_for_completion.return_value = PBCompletion(
        container_id="c-gate",
        status="finished",
        log_output="Invitation sent to invite-target",
        raw_output={"status": "finished", "output": _MARKER},
    )
    pb.download_result_csv.return_value = ""

    entries = [
        _entry(entry_id="ent-inv", record_id="rec-inv",
               stage_title="Prospect", ref_day=operator_day)
    ]

    attio = MagicMock()
    attio.is_person_company_corrupted.return_value = False
    attio.query_list_entries.return_value = entries
    from tests.fakes import stub_guard_reread
    stub_guard_reread(attio, entries)

    def _cache_get(record_id):
        return ("Inv Person", "Acme", invite_url, "", "Plant Manager")

    def _passthrough_pic(rows, *_a, **_kw):
        return rows, []

    advance_spy = MagicMock(return_value=1)

    with patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    }), \
         patch("workflows.daily_check.RecordCache.get", side_effect=_cache_get), \
         patch("workflows.daily_check.can_send_connections", return_value=True), \
         patch("workflows.daily_check.record_connections"), \
         patch("workflows.daily_check.record_visits"), \
         patch("workflows.daily_check.get_remaining",
               return_value={"connections": 25, "messages": 30, "visits": 50}), \
         patch("workflows.daily_check.get_current_experiment_id", return_value="exp-1"), \
         patch("workflows.daily_check._pre_invite_degree_check", side_effect=_passthrough_pic), \
         patch("workflows.daily_check.write_prospects_to_sheet",
               return_value="https://docs.google.com/spreadsheets/d/fake"), \
         patch("workflows.daily_check._attio_advance_with_escalation", advance_spy), \
         patch("workflows.daily_check.emit_pb_silent_no_op"), \
         patch("workflows.daily_check.recheck_cache") as mock_rc:
        mock_rc.partition.side_effect = lambda urls: ({}, list(urls))
        mock_rc.RECHECK_TTL_DAYS = 3
        run_connection_requests(
            attio=attio, pb=pb, network_booster_id="agent-nb",
            auto_confirm=True, today=operator_day,
            daily_run=fake_daily_run(),
        )

    assert advance_spy.called, "invites-only confirmed invitation should advance"
    stamped_today = advance_spy.call_args.kwargs["today"]
    assert stamped_today == operator_day.isoformat(), (
        f"post-send stamp must use the operator-local date "
        f"({operator_day.isoformat()}), not UTC date.today() "
        f"({date.today().isoformat()}); got {stamped_today!r}"
    )
