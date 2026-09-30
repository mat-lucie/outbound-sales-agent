"""Tests for clients/phantombuster.py F-PR-5 typed contract.

Mocks the underlying httpx client to drive PB API responses through
the typed contract. Covers:
- `launch_agent` returns PBLaunch with container_id parsed from
  response.
- `launch_agent` raises PBRunFailed when PB launch response lacks
  containerId.
- `wait_for_completion` raises PBRunFailed on `status="error"` (no
  more silent return).
- `wait_for_completion` raises PBRunTimeout when polling exhausts.
- `wait_for_completion` returns PBCompletion on `status="finished"`.
- `download_result_csv(launch)` keys to container_id via fetch-output
  with `containerId` param.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from clients.pb_envelope import PBLaunch, PBRunFailed, PBRunTimeout
from clients.phantombuster import PhantomBusterClient


@pytest.fixture(autouse=True)
def _stub_pb_api_key(monkeypatch):
    monkeypatch.setenv("PHANTOMBUSTER_API_KEY", "test_key_unused")


@pytest.fixture
def pb_client():
    return PhantomBusterClient(api_key="test_key")


def _response(status: int, body: dict) -> httpx.Response:
    req = httpx.Request("POST", "https://api.phantombuster.com/x")
    return httpx.Response(status, request=req, json=body)


class TestLaunchAgent:
    def test_returns_pblaunch_with_container_id(self, pb_client) -> None:
        with patch.object(pb_client, "_request") as mock_req:
            mock_req.return_value = {
                "containerId": "c_42",
                "requestId": "req_99",
            }
            launch = pb_client.launch_agent("ag_msg", {"sheet": "u"})
            assert isinstance(launch, PBLaunch)
            assert launch.container_id == "c_42"
            assert launch.agent_id == "ag_msg"
            assert launch.request_id == "req_99"
            assert launch.arguments_sha256  # non-empty

    def test_no_container_id_raises(self, pb_client) -> None:
        """PB launch that returns no containerId is a contract violation."""
        with patch.object(pb_client, "_request") as mock_req:
            mock_req.return_value = {"someOtherKey": "x"}
            with pytest.raises(PBRunFailed) as exc:
                pb_client.launch_agent("ag_msg", None)
            assert exc.value.agent_id == "ag_msg"
            assert "containerId" in exc.value.log_tail

    def test_snake_case_container_id_also_accepted(self, pb_client) -> None:
        """Be defensive about camelCase vs snake_case naming from PB."""
        with patch.object(pb_client, "_request") as mock_req:
            mock_req.return_value = {"container_id": "c_77"}
            launch = pb_client.launch_agent("ag_x", None)
            assert launch.container_id == "c_77"


class TestGetContainerOutput:
    def test_sends_agent_id_and_container_id(self, pb_client) -> None:
        """PB's /agents/fetch-output REQUIRES the agent `id`; a `containerId`
        param ALONE returns HTTP 400. Regression guard for the bug where
        get_container_output omitted `id`, so every wait_for_completion poll
        400'd → swallowed-as-transient → false PBRunTimeout(status=None),
        making healthy scrapes (PB exit 0) look like degraded/failed runs.
        (Confirmed live 2026-05-28: ?id=&containerId= → 200; ?containerId= → 400.)"""
        with patch.object(pb_client, "_request") as mock_req:
            mock_req.return_value = {"status": "finished", "containerId": "c_42"}
            pb_client.get_container_output("c_42", "ag_msg")
            mock_req.assert_called_once_with(
                "GET",
                "/agents/fetch-output",
                params={"id": "ag_msg", "containerId": "c_42"},
            )


class TestWaitForCompletion:
    def _launch(self) -> PBLaunch:
        from datetime import UTC, datetime

        from clients.pb_envelope import hash_arguments
        return PBLaunch(
            container_id="c_42",
            agent_id="ag_msg",
            launched_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )

    def test_status_finished_returns_completion(self, pb_client) -> None:
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "containerId": "c_42",
                "status": "finished",
                "isAgentRunning": False,
                "output": "log lines\nCSV saved at https://x.csv",
            }
            completion = pb_client.wait_for_completion(
                self._launch(), poll_interval=0, max_wait=10
            )
            assert completion.container_id == "c_42"
            assert completion.status == "finished"
            assert "CSV saved at" in completion.log_output

    def test_status_error_raises_pbrunfailed(self, pb_client) -> None:
        """No more silent error return."""
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "containerId": "c_42",
                "status": "error",
                "isAgentRunning": False,
                "output": "PB error: session cookie expired",
            }
            with pytest.raises(PBRunFailed) as exc:
                pb_client.wait_for_completion(
                    self._launch(), poll_interval=0, max_wait=10
                )
            assert exc.value.container_id == "c_42"
            assert "session cookie expired" in exc.value.log_tail

    def test_timeout_raises_pbruntimeout(self, pb_client) -> None:
        """Always-running responses → PBRunTimeout (typed, not bare TimeoutError)."""
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "containerId": "c_42",
                "status": "running",
                "isAgentRunning": True,
            }
            with patch("clients.phantombuster.time.sleep"), pytest.raises(PBRunTimeout) as exc:
                pb_client.wait_for_completion(
                    self._launch(), poll_interval=1, max_wait=3
                )
            assert exc.value.container_id == "c_42"

    def test_polls_container_keyed_endpoint(self, pb_client) -> None:
        """wait_for_completion must call get_container_output (NOT
        get_output) — PB's `containerId` param returns THIS run's
        output, not the latest."""
        launch = self._launch()
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "containerId": "c_42",
                "status": "finished",
                "isAgentRunning": False,
                "output": "ok",
            }
            pb_client.wait_for_completion(launch, poll_interval=0, max_wait=10)
            mock_out.assert_called_with("c_42", "ag_msg")

    def test_ignores_terminal_status_of_a_different_container(self, pb_client) -> None:
        """PB's fetch-output returns the agent's LATEST run; if that's a
        DIFFERENT container (ours not yet registered), a terminal status must
        NOT be accepted as ours. Only when the returned containerId matches
        do we return — preserving the F-PR-5 container-precision invariant."""
        launch = self._launch()  # container c_42
        with patch.object(pb_client, "get_container_output") as mock_out, \
             patch("clients.phantombuster.time.sleep"):
            mock_out.side_effect = [
                # Latest is a stale OTHER finished run → keep waiting.
                {"status": "finished", "isAgentRunning": False,
                 "output": "old run", "containerId": "c_OTHER"},
                # Now our container is the latest and finished → return it.
                {"status": "finished", "isAgentRunning": False,
                 "output": "ours\nCSV saved at https://x.csv", "containerId": "c_42"},
            ]
            completion = pb_client.wait_for_completion(launch, poll_interval=1, max_wait=30)
        assert completion.container_id == "c_42"
        assert "ours" in completion.log_output


class TestDownloadResultCsv:
    def _launch(self) -> PBLaunch:
        from datetime import UTC, datetime

        from clients.pb_envelope import hash_arguments
        return PBLaunch(
            container_id="c_42",
            agent_id="ag_msg",
            launched_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )

    def test_uses_container_keyed_log(self, pb_client) -> None:
        """download_result_csv calls get_container_output(launch.container_id)
        — not get_output(agent_id) — so the URL is keyed to THIS run."""
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "output": "CSV saved at https://s3.example/result.csv"
            }
            with patch("clients.phantombuster.httpx.get") as mock_get:
                mock_get.return_value = MagicMock(
                    text="query,status\na,Message sent\n",
                    raise_for_status=MagicMock(),
                )
                csv_text = pb_client.download_result_csv(self._launch())
                # The container_id is what was queried, NOT agent_id
                mock_out.assert_called_with("c_42", "ag_msg")
                assert csv_text == "query,status\na,Message sent\n"

    def test_no_url_returns_none(self, pb_client) -> None:
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {"output": ""}  # no "CSV saved at" line
            with patch.object(pb_client, "get_agent") as mock_agent:
                mock_agent.return_value = {}  # no s3Folder fallback
                csv_text = pb_client.download_result_csv(self._launch())
                assert csv_text is None


class TestNoResendInvariant:
    """The §3.1 chokepoint regression test.

    If `wait_for_completion` ever silently returns on `status="error"`
    again (pre-F-PR-5 behavior), callers will believe the run
    succeeded and advance stage. This test asserts the new contract:
    error MUST raise PBRunFailed, not return.
    """

    def test_error_status_must_raise_not_return(self, pb_client) -> None:
        from datetime import UTC, datetime

        from clients.pb_envelope import hash_arguments

        launch = PBLaunch(
            container_id="c_chokepoint",
            agent_id="ag_msg",
            launched_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        with patch.object(pb_client, "get_container_output") as mock_out:
            mock_out.return_value = {
                "containerId": "c_chokepoint",
                "status": "error",
                "isAgentRunning": False,
                "output": "anything",
            }
            with pytest.raises(PBRunFailed):
                pb_client.wait_for_completion(
                    launch, poll_interval=0, max_wait=10
                )
