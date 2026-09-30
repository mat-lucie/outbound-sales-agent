"""Invite daily-cap charging via the Attio daily_run row.

Split-brain fix (2026-06-09): the invite path charged only the legacy
local file (~/.outbound-agent/daily_limits.json) while the DM path charged
the Attio daily_run row — so every Attio row reported connections_sent=0
forever. These tests pin the new behaviour: the gate reads BOTH sources,
and charging goes through the PR-17 two-phase lease (reserve before PB
launch, confirm with the parsed outcome, release on failure), exactly
like the DM path.

(The visit-budget lease for CONNECTION_SENT "re-check" rows was removed
2026-06-10 along with the re-check rows themselves — the launch is now
invite-only, so only the connections lease remains.)
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import httpx
import pytest

from tests.fakes import fake_daily_run, stub_guard_reread
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


class TestInviteCapAttioRouting:

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_attio_remaining_zero_blocks_run(self):
        """When Attio daily_run shows remaining=0, the run must short-circuit.

        Even if the local file cap (can_send_connections) says OK, the Attio
        ledger is the authoritative cross-run cap — remaining=0 must block
        PB launch and return reason=daily_limit.
        """
        from workflows.daily_check import run_connection_requests

        entry = _make_attio_entry(
            entry_id="entry-attio-cap-001",
            record_id="rec-attio-cap-001",
            stage="Prospect",
            quality_score=75,
        )

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-attio-cap-001": "company-xyz"}

        pb = MagicMock()

        dr = fake_daily_run(remaining=0)

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining", return_value={"connections": 25, "messages": 30, "visits": 50}):
            mock_cache_get.return_value = ("Test Person", "Test Co", "https://linkedin.com/in/testperson", "", "")
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        assert result["reason"] == "daily_limit", (
            f"Expected reason=daily_limit when Attio remaining=0, got: {result}"
        )
        pb.launch_agent.assert_not_called()

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_target_trims_to_attio_remaining(self, capsys):
        """The invite target is min(local_remaining, attio_remaining, batch_size).

        With 2 eligible prospects and attio remaining=1 (but local remaining=25),
        the dry_run count must be 1 — trimmed to the Attio ledger cap.
        """
        from workflows.daily_check import run_connection_requests

        entries = [
            _make_attio_entry(
                entry_id=f"entry-trim-{i:03d}",
                record_id=f"rec-trim-{i:03d}",
                stage="Prospect",
                quality_score=75,
            )
            for i in range(2)
        ]

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = entries
        # Give each prospect a distinct company so within-run dedup doesn't
        # eliminate one before the Attio cap has a chance to trim.
        attio._person_to_company = {
            "rec-trim-000": "company-aaa",
            "rec-trim-001": "company-bbb",
        }

        pb = MagicMock()

        dr = fake_daily_run(remaining=1)

        def _cache_side_effect(record_id):
            idx = int(record_id.split("-")[-1])
            return (f"Person {idx}", f"Company {idx}", f"https://linkedin.com/in/person{idx}", "", "")

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining", return_value={"connections": 25, "messages": 30, "visits": 50}):
            mock_cache_get.side_effect = _cache_side_effect
            result = run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                dry_run=True,
                daily_run=dr,
            )

        assert result.get("dry_run") == 1, (
            f"Expected dry_run=1 (trimmed to Attio remaining=1), got: {result}"
        )

        captured = capsys.readouterr()
        assert "daily_run remaining: 1" in captured.out, (
            f"Expected trim echo to name Attio as the binding ledger, got:\n{captured.out}"
        )


class TestInviteLease:
    """Pin the two-phase reserve/confirm lease around the Network Booster launch.

    These mirror the DM-path PR-17 B-SD-006 contract: reserve before PB is
    touched, confirm with the parsed outcome BEFORE Attio stage advances,
    release in finally on any failure.
    """

    # ── shared env ────────────────────────────────────────────────────────────
    _ENV = {
        "ATTIO_LIST_ID": "list-001",
        "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake",
        "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie",
        "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    }

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _clean_pb(container_id: str = "c-lease-test") -> MagicMock:
        """Typed PB mock: clean authenticated launch (no auth failure marker).

        Network Booster CSV is unreliable — the advance gate uses optimistic
        logic, not 'Message sent' CSV status.  A log without the auth-fail
        marker ('No valid credentials found') is a clean launch.
        """
        from datetime import UTC, datetime

        from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments

        launch = PBLaunch(
            container_id=container_id,
            agent_id="agent-nb",
            launched_at=datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        completion = PBCompletion(
            container_id=container_id,
            status="finished",
            log_output="🔄 Adding Test Person...\nInvitation sent to testperson\n✅ CSV saved\nProcess finished successfully",
            raw_output={"status": "finished", "output": "Process finished successfully"},
        )
        pb = MagicMock()
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.return_value = completion
        pb.download_result_csv.return_value = (
            "query,status\nhttps://linkedin.com/in/testperson,Can't send message\n"
        )
        return pb

    @staticmethod
    def _gate_fail_pb(container_id: str = "c-gate-fail") -> MagicMock:
        """Typed PB mock: auth-failure launch (dead cookie, gate fails).

        The invite advance gate takes the dry-skip path; compute_invite_outcome
        returns Skipped/sent_count=0.
        """
        from datetime import UTC, datetime

        from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments

        launch = PBLaunch(
            container_id=container_id,
            agent_id="agent-nb",
            launched_at=datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        completion = PBCompletion(
            container_id=container_id,
            status="finished",
            log_output="🔄 Connecting to LinkedIn...\n❌ No valid credentials found",
            raw_output={"status": "finished", "output": ""},
        )
        pb = MagicMock()
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.return_value = completion
        pb.download_result_csv.return_value = (
            "query,error\nhttps://linkedin.com/in/testperson,\n"
        )
        return pb

    @staticmethod
    def _prospect_entry(idx: int = 0) -> dict:
        return _make_attio_entry(
            entry_id=f"entry-lease-{idx:03d}",
            record_id=f"rec-lease-{idx:03d}",
            stage="Prospect",
            quality_score=75,
        )

    # ── test 1: clean launch → reserve + confirm with sent_count ─────────────

    @patch.dict(os.environ, _ENV)
    def test_clean_launch_reserves_and_confirms_sent_count(self):
        """Clean launch: reserve_send("connections", 1) then confirm_lease with
        confirmed_count=1 (the outcome.sent_count from optimistic advance).
        release_lease must NOT be called (the lease was consumed).
        The legacy local mirror (record_connections) still fires once with 1.
        """
        from workflows.daily_check import run_connection_requests

        entry = self._prospect_entry(0)
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-lease-000": "company-lease-aaa"}
        stub_guard_reread(attio, [entry])

        pb = self._clean_pb()

        dr = fake_daily_run()
        # Reserve returns the stable token "fake-lease-token" from fake_daily_run.

        with patch.dict(os.environ, self._ENV), \
             patch("workflows.daily_check.RecordCache.get",
                   return_value=("Test Person", "Test Co",
                                 "https://linkedin.com/in/testperson", "", "")), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.record_connections") as mock_record:
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        # Lease charged via Attio daily_run
        dr.reserve_send.assert_called_once_with("connections", 1)
        dr.confirm_lease.assert_called_once_with("fake-lease-token", confirmed_count=1)
        dr.release_lease.assert_not_called()

        # Legacy local mirror still fires exactly once with 1
        mock_record.assert_called_once_with(1)

    # ── test 2: PB failure → release without confirm ─────────────────────────

    @patch.dict(os.environ, _ENV)
    def test_pb_failure_releases_lease_without_confirm(self):
        """When pb.wait_for_completion raises PBRunFailed the lease must be
        released (not confirmed), and the exception must propagate to the caller.
        """
        from datetime import UTC, datetime

        from clients.pb_envelope import PBLaunch, PBRunFailed, hash_arguments
        from workflows.daily_check import run_connection_requests

        entry = self._prospect_entry(1)
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-lease-001": "company-lease-bbb"}
        stub_guard_reread(attio, [entry])

        # Build a PB mock whose wait_for_completion raises PBRunFailed.
        launch = PBLaunch(
            container_id="c-pbfail",
            agent_id="agent-nb",
            launched_at=datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        pb = MagicMock()
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.side_effect = PBRunFailed(
            container_id="c-pbfail",
            agent_id="agent-nb",
            log_tail="PB reported status=error",
        )

        dr = fake_daily_run()

        with patch.dict(os.environ, self._ENV), \
             patch("workflows.daily_check.RecordCache.get",
                   return_value=("PB Fail Person", "Co",
                                 "https://linkedin.com/in/pbfailperson", "", "")), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.record_connections"), \
             pytest.raises(PBRunFailed):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        # On failure: release fires, confirm does not
        dr.release_lease.assert_called_once_with("fake-lease-token")
        dr.confirm_lease.assert_not_called()

    # ── test 3: gate-fail launch → confirm with confirmed_count=0 ────────────

    @patch.dict(os.environ, _ENV)
    def test_gate_fail_confirms_zero(self):
        """Auth-failure launch: compute_invite_outcome yields sent_count=0.
        confirm_lease must be called with confirmed_count=0.
        release_lease must NOT be called (the lease was consumed).
        """
        from workflows.daily_check import run_connection_requests

        entry = self._prospect_entry(2)
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-lease-002": "company-lease-ccc"}
        stub_guard_reread(attio, [entry])

        pb = self._gate_fail_pb()
        dr = fake_daily_run()

        with patch.dict(os.environ, self._ENV), \
             patch("workflows.daily_check.RecordCache.get",
                   return_value=("Gate Fail", "Co",
                                 "https://linkedin.com/in/gatefailperson", "", "")), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.record_connections"), \
             patch("workflows.daily_check.emit_pb_silent_no_op"):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        # Gate fails → sent_count=0 → confirm with confirmed_count=0
        dr.confirm_lease.assert_called_once_with("fake-lease-token", confirmed_count=0)
        dr.release_lease.assert_not_called()

    # ── test 6: transport error at confirm → loud echo, no mirror write ──────

    @patch.dict(os.environ, _ENV)
    def test_confirm_transport_error_echoes_sent_but_uncharged(self, capsys):
        """When confirm_lease raises httpx.RequestError (Attio transport error),
        the exception must propagate AND the operator must see a loud echo that
        PB already sent. record_connections must NOT be called — the mirror stays
        in lockstep with the failed Attio charge.
        """
        from workflows.daily_check import run_connection_requests

        entry = self._prospect_entry(20)
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-lease-020": "company-transport-err"}
        stub_guard_reread(attio, [entry])

        pb = self._clean_pb(container_id="c-transport-err")

        dr = fake_daily_run()
        # Simulate Attio being unreachable at confirm time
        dr.confirm_lease.side_effect = httpx.RequestError("attio down")

        with patch.dict(os.environ, self._ENV), \
             patch("workflows.daily_check.RecordCache.get",
                   return_value=("Transport Person", "Co",
                                 "https://linkedin.com/in/testperson", "", "")), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.record_connections") as mock_record_conn, \
             pytest.raises(httpx.RequestError):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        # The exception must propagate
        # The operator echo must name PB launch and the failed Attio charge
        captured = capsys.readouterr()
        assert "PB launch completed" in captured.err, (
            f"Expected 'PB launch completed' in stderr, got:\n{captured.err}"
        )
        assert "FAILED" in captured.err, (
            f"Expected 'FAILED' in stderr, got:\n{captured.err}"
        )

        # Mirror must NOT fire — ledgers stay in lockstep with the Attio charge
        mock_record_conn.assert_not_called()

    # ── verify test 1 still correct after mirror placement change ─────────────

    @patch.dict(os.environ, _ENV)
    def test_clean_launch_record_connections_fires_after_confirm(self):
        """Regression guard: after Fix 1, record_connections fires inside the
        try block immediately after confirm_lease, not after stage advances.
        The value passed must still be outcome.sent_count (= 1 for a clean
        optimistic-advance launch with 1 prospect).
        """
        from workflows.daily_check import run_connection_requests

        entry = self._prospect_entry(21)
        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [entry]
        attio._person_to_company = {"rec-lease-021": "company-order-guard"}
        stub_guard_reread(attio, [entry])

        pb = self._clean_pb(container_id="c-order-guard")
        dr = fake_daily_run()

        call_order: list[str] = []

        def _record_conn(n):
            call_order.append(f"record_connections({n})")

        def _confirm(*args, **kwargs):
            call_order.append("confirm_lease")

        dr.confirm_lease.side_effect = _confirm

        with patch.dict(os.environ, self._ENV), \
             patch("workflows.daily_check.RecordCache.get",
                   return_value=("Order Guard", "Co",
                                 "https://linkedin.com/in/testperson", "", "")), \
             patch("workflows.daily_check.can_send_connections", return_value=True), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check.record_connections", side_effect=_record_conn):
            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb-001",
                auto_confirm=True,
                daily_run=dr,
            )

        assert call_order[0] == "confirm_lease", (
            f"confirm_lease must fire before record_connections; got order: {call_order}"
        )
        assert call_order[1] == "record_connections(1)", (
            f"record_connections(1) must fire immediately after confirm; got order: {call_order}"
        )
