"""Network Booster's 10-add cap and per-person invite confirmation."""
from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from tests.fakes import fake_daily_run
from tests.test_integration import _make_attio_entry

_ENV = {
    "ATTIO_LIST_ID": "list-001",
    "ATTIO_API_KEY": "fake",
    "PHANTOMBUSTER_API_KEY": "fake",
    "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
    "PB_LI_SESSION_COOKIE": "fake-cookie",
    "PB_LI_USER_AGENT": "TestAgent/1.0",
    "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
}


def _allow_send_guard():
    from workflows.email_send_guard import GuardResult
    return patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True))


def _nb_pb(processed_urls: list[str], container_id: str = "c-nb-cap") -> MagicMock:
    """Typed PB mock: clean authenticated Auto Connect launch whose log
    carries the per-launch "URLs to process" attempt list (real 2026-06-10
    container shape)."""
    from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments

    log_lines = [
        "✅ Got 22 lines from csv.",
        "✅ Connected successfully as Test Operator",
        "[info_]ℹ️ URLs to process: [",
    ]
    for i, url in enumerate(processed_urls):
        comma = "," if i < len(processed_urls) - 1 else ""
        log_lines.append(f'[info_]    "{url}"{comma}')
    log_lines.append("[info_]]")
    for url in processed_urls:
        log_lines.append(f"[done_]✅ Invitation sent to {url.rstrip('/').split('/')[-1]}")
    log_lines.append("* Process finished successfully (exit code: 0)")

    launch = PBLaunch(
        container_id=container_id,
        agent_id="agent-nb",
        launched_at=datetime(2026, 6, 10, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )
    completion = PBCompletion(
        container_id=container_id,
        status="finished",
        log_output="\n".join(log_lines),
        raw_output={"status": "finished", "output": ""},
    )
    pb = MagicMock()
    pb.launch_agent.return_value = launch
    pb.wait_for_completion.return_value = completion
    pb.download_result_csv.return_value = "query,error\n"
    return pb


def _audit_events(audit_logger: MagicMock, event_type: str) -> list[dict]:
    return [
        c.kwargs
        for c in audit_logger.event.call_args_list
        if c.args and c.args[0] == event_type
    ]


class TestInviteLaunchArgsCarryExplicitCap:
    @patch.dict(os.environ, _ENV)
    def test_launch_args_carry_explicit_per_launch_count(self):
        """Every Network Booster launch must pass
        numberOfProfilesPerLaunch=len(invites): PB launch `arguments`
        replace the saved console config, so omitting the key falls back
        to the phantom default (10) — the invite-side mirror of the
        2026-06-10 DM trickle. The batch is invite-only (re-check rows
        removed 2026-06-10), so the count is exactly the invite count."""
        from workflows.daily_check import run_connection_requests

        invite_entries = [
            _make_attio_entry(
                entry_id=f"entry-cap-{i:03d}",
                record_id=f"rec-cap-{i:03d}",
                stage="Prospect",
                quality_score=75,
            )
            for i in range(2)
        ]
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = invite_entries
        attio._person_to_company = {
            "rec-cap-000": "company-cap-aaa",
            "rec-cap-001": "company-cap-bbb",
        }

        pb = _nb_pb(
            ["https://www.linkedin.com/in/person0",
             "https://www.linkedin.com/in/person1"]
        )
        dr = fake_daily_run()

        def _cache(record_id):
            idx = int(record_id.rsplit("-", 1)[1])
            return (
                f"Person {idx}", f"Co {idx}",
                f"https://linkedin.com/in/person{idx}", "", "",
            )

        with patch.dict(os.environ, _ENV), \
             patch("workflows.daily_check.RecordCache.get", side_effect=_cache), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             _allow_send_guard(), \
             patch("workflows.daily_check.record_connections"):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        pb.launch_agent.assert_called_once()
        args = pb.launch_agent.call_args.args[1]
        # Network Booster counts additions, not the sheet header.
        assert args["numberOfAddsPerLaunch"] == 2
        assert "numberOfProfilesPerLaunch" not in args


class TestInviteAuthoritativeAdvanceWithDiagnostic:
    @patch.dict(os.environ, _ENV)
    def test_list_absent_row_is_advanced_and_diagnosed(self, capsys):
        """Phase B: the launch log echoes only 1 of 2 requested invites, but a
        clean launch advances BOTH (the list-absent row was physically invited
        — log drift / dedup artifact). Both are charged; the list-absent row is
        surfaced via the `pb_invite_advanced_not_in_processed_list` diagnostic
        echo + audit event (NOT withheld)."""
        from workflows.daily_check import run_connection_requests

        entries = [
            _make_attio_entry(
                entry_id=f"entry-trunc-{i:03d}",
                record_id=f"rec-trunc-{i:03d}",
                stage="Prospect",
                quality_score=75,
            )
            for i in range(2)
        ]
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = entries
        attio._person_to_company = {
            "rec-trunc-000": "company-trunc-aaa",
            "rec-trunc-001": "company-trunc-bbb",
        }

        # The launch log's attempt list carries person0 only — person1 is
        # absent (log drift / phantom already-processed dedup), but Phase B
        # still advances it (the invite went out).
        pb = _nb_pb(["https://www.linkedin.com/in/person0"])
        dr = fake_daily_run()
        audit_logger = MagicMock()

        def _cache(record_id):
            idx = record_id.rsplit("-", 1)[1]
            return (
                f"Person {idx}", f"Co {idx}",
                f"https://linkedin.com/in/person{int(idx)}", "", "",
            )

        with patch.dict(os.environ, _ENV), \
             patch("workflows.daily_check.RecordCache.get", side_effect=_cache), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             _allow_send_guard(), \
             patch("workflows.daily_check.record_connections") as mock_record:
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
                audit_logger=audit_logger,
            )
            # Simulate Attio returning the durable hold on the next run.
            entries[1]["entry_values"]["invite_eligible_after"] = [
                {"value": "2099-12-31"}
            ]
            attio.query_list_entries.return_value = [entries[1]]
            run_connection_requests(
                attio=attio, pb=pb, network_booster_id="agent-nb-001",
                auto_confirm=True, daily_run=fake_daily_run(),
            )

        pb.launch_agent.assert_called_once()
        assert result["sent"] == 1
        assert result["pb_queued"] == 2
        assert any(
            c.kwargs["entry_id"] == "entry-trunc-001"
            and c.kwargs["entry_attributes"] == {"invite_eligible_after": "2099-12-31"}
            for c in attio.update_list_entry.call_args_list
        )

        # Only the explicitly confirmed row advances and consumes capacity.
        dr.reserve_send.assert_called_once_with("connections", 2)
        dr.confirm_lease.assert_called_once_with(
            "fake-lease-token", confirmed_count=1
        )
        mock_record.assert_called_once_with(1)

        # The missing row stays at Prospect and is surfaced for review.
        captured = capsys.readouterr()
        assert "confirmed 1/2 invitations" in captured.out
        events = _audit_events(audit_logger, "pb_invite_unconfirmed")
        assert len(events) == 1
        assert events[0]["requested"] == 2
        assert events[0]["confirmed"] == 1
        assert events[0]["urls"] == ["https://linkedin.com/in/person1"]
