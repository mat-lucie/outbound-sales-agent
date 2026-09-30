"""Integration coverage for the Layer-2 advance gate inside
``run_connection_requests`` (Pattern-A re-prospecting fix).

These drive the real function to the Network Booster advance gate and assert the
branch decision — the safety-critical glue the unit tests don't exercise:
  - already-processed batch                        -> advance, NO pb_silent_no_op
  - clean authenticated launch (unreliable NB CSV) -> optimistic per-row advance
  - auth failure (dead cookie, invites NOT sent)   -> NO advance, pb_silent_no_op

(The mixed-batch guard cases were deleted 2026-06-10 with the CONNECTION_SENT
re-check rows — the batch is always invite-only now, so the batch-level
already-processed marker is unambiguous.)
"""
from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from tests.fakes import fake_daily_run, stub_guard_reread


def _entry(*, entry_id: str, record_id: str, stage_title: str) -> dict:
    old_date = (date.today() - timedelta(days=30)).isoformat()
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


def _drive(*, log_output: str, csv_text: str):
    """Run run_connection_requests to the advance gate. Returns
    (advance_spy, silent_spy)."""
    from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
    from workflows.daily_check import run_connection_requests

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
        log_output=log_output,
        raw_output={"status": "finished", "output": log_output},
    )
    pb.download_result_csv.return_value = csv_text

    entries = [_entry(entry_id="ent-inv", record_id="rec-inv", stage_title="Prospect")]

    attio = MagicMock()
    attio.is_person_company_corrupted.return_value = False
    attio.query_list_entries.return_value = entries
    stub_guard_reread(attio, entries)

    def _cache_get(record_id):
        return ("Inv Person", "Acme", invite_url, "", "Plant Manager")

    # Degree check passthrough: the invite row is the confirmed 2nd/3rd-degree
    # to_send_data; never flips anything here.
    def _passthrough_pic(rows, *_a, **_kw):
        return rows, []

    advance_spy = MagicMock(return_value=1)   # _advance_already_processed_rows (Pattern-A path)
    silent_spy = MagicMock()                  # emit_pb_silent_no_op (gate-fail path)
    perrow_spy = MagicMock(return_value=True)  # _attio_advance_with_escalation (optimistic per-row advance)

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
         patch("workflows.daily_check.get_remaining", return_value={"connections": 25, "messages": 30, "visits": 50}), \
         patch("workflows.daily_check.get_current_experiment_id", return_value="exp-1"), \
         patch("workflows.daily_check._pre_invite_degree_check", side_effect=_passthrough_pic), \
         patch("workflows.daily_check.write_prospects_to_sheet",
               return_value="https://docs.google.com/spreadsheets/d/fake"), \
         patch("workflows.daily_check._advance_already_processed_rows", advance_spy), \
         patch("workflows.daily_check._attio_advance_with_escalation", perrow_spy), \
         patch("workflows.daily_check.emit_pb_silent_no_op", silent_spy):
        run_connection_requests(
            attio=attio, pb=pb, network_booster_id="agent-nb", auto_confirm=True,
            daily_run=fake_daily_run(),
        )

    return advance_spy, silent_spy, perrow_spy


_MARKER = (
    "✅ Got 1 line from csv.\n"
    "⚠️ We already processed every profile from this spreadsheet.\n"
)


def test_already_processed_batch_does_not_claim_unconfirmed_invites():
    advance_spy, silent_spy, perrow_spy = _drive(log_output=_MARKER, csv_text="")
    assert not advance_spy.called
    assert not perrow_spy.called
    assert silent_spy.called


def test_auth_failure_does_not_advance_opens_silent_no_op():
    # Dead-cookie launch: LinkedIn auth failed, so the invites did NOT go out.
    # This is the ONE invite case that must still take the dry-skip path —
    # never optimistically advance an un-sent batch to CONNECTION_SENT.
    csv_text = "query,error\nhttps://www.linkedin.com/in/invite-target/,\n"
    advance_spy, silent_spy, perrow_spy = _drive(
        log_output="🔄 Connecting to LinkedIn...\n❌ No valid credentials found",
        csv_text=csv_text,
    )
    assert not advance_spy.called
    assert not perrow_spy.called, "auth failure must NOT advance any invite row"
    assert silent_spy.called, "auth failure must open pb_silent_no_op"


def test_explicit_send_log_advances_confirmed_invite():
    csv_text = (
        "query,status\n"
        "https://www.linkedin.com/in/invite-target/,Can't send message\n"
    )
    advance_spy, silent_spy, perrow_spy = _drive(
        log_output="🔄 Adding Invite Target...\nInvitation sent to invite-target\n✅ CSV saved\nProcess finished successfully",
        csv_text=csv_text,
    )
    assert perrow_spy.called
    assert not silent_spy.called
