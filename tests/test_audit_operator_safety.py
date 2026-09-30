"""Tests for the audit-operator-safety fixes.

Covers the 7 findings from docs/audits/2026-06-09-adversarial-qa-triage.md:
  T1.1 — check_regular_cookie_health uses AUTH_FAILURE_MARKERS
  T1.3 — pb_send_recovery container attribution guard
  T1.5b — .env.example documents PB_INBOX_SCRAPER_ID (static check)
  T1.5c — google_sheets.get_client raises on 0-byte google-oauth.json
  L6-3  — daily --dry-run never launches the Sales Nav health pre-flight
  L6-6  — health-check makes a real PB API read (list_agents)
  L4-3  — learn command acquires run lock; second invocation exits 75
"""
from __future__ import annotations

import fcntl
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ══════════════════════════════════════════════════════════════════════════════
# T1.1  Cookie health check uses canonical AUTH_FAILURE_MARKERS
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckRegularCookieHealthMarkers:
    """Each canonical auth-failure marker must flip the verdict to fail."""

    def _run_check_with_log(self, log_text: str) -> CheckResult:  # noqa: F821
        """Run check_regular_cookie_health with a stubbed PB output."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from scripts.validate_sales_nav_health import check_regular_cookie_health

        fake_output = {"output": log_text, "status": "finished", "containerId": "c1"}

        with (
            patch.dict(
                os.environ,
                {
                    "PB_INBOX_SCRAPER_ID": "inbox-123",
                    "PB_LI_SESSION_COOKIE": "cookie-abc",
                    "PHANTOMBUSTER_API_KEY": "key",
                },
            ),
            patch("scripts.validate_sales_nav_health.PhantomBusterClient") as MockPB,
        ):
            pb_instance = MagicMock()
            pb_instance.__enter__ = lambda s: pb_instance
            pb_instance.__exit__ = MagicMock(return_value=False)
            pb_instance.launch_agent.return_value = MagicMock(
                container_id="c1", agent_id="inbox-123"
            )
            pb_instance.wait_for_completion.return_value = MagicMock(status="finished")
            pb_instance.get_container_output.return_value = fake_output
            MockPB.return_value = pb_instance
            result = check_regular_cookie_health()
        return result

    def test_401_in_log_is_fail(self):
        r = self._run_check_with_log("some log with 401 status")
        assert r.status == "fail", f"Expected fail for 401 log; got {r.status}: {r.detail}"

    def test_no_valid_credentials_is_fail(self):
        r = self._run_check_with_log("Error: No valid credentials provided.")
        assert r.status == "fail", f"Expected fail for 'no valid credentials'; got {r.status}"

    def test_network_cookie_invalid_is_fail(self):
        r = self._run_check_with_log("network-cookie-invalid: session rejected")
        assert r.status == "fail", f"Expected fail for 'network-cookie-invalid'; got {r.status}"

    def test_cant_connect_to_linkedin_is_fail(self):
        r = self._run_check_with_log(
            "Can't connect to LinkedIn with this session cookie"
        )
        assert r.status == "fail", (
            f"Expected fail for cant-connect marker; got {r.status}"
        )

    def test_session_cookie_not_valid_is_fail(self):
        r = self._run_check_with_log("session cookie not valid — please update")
        assert r.status == "fail"

    def test_session_expired_is_fail(self):
        r = self._run_check_with_log("session expired at 2026-06-09")
        assert r.status == "fail"

    def test_clean_log_stays_ok(self):
        r = self._run_check_with_log(
            "Processing inbox... 1 thread scraped. Done."
        )
        assert r.status == "ok", f"Expected ok for clean log; got {r.status}: {r.detail}"

    def test_pb_status_error_is_fail(self):
        """T1.1 addendum: container status=error (non-zero run) must also fail."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from scripts.validate_sales_nav_health import check_regular_cookie_health

        fake_output = {
            "output": "Processing...",
            "status": "error",
            "containerId": "c1",
        }
        with (
            patch.dict(
                os.environ,
                {
                    "PB_INBOX_SCRAPER_ID": "inbox-123",
                    "PB_LI_SESSION_COOKIE": "cookie-abc",
                    "PHANTOMBUSTER_API_KEY": "key",
                },
            ),
            patch("scripts.validate_sales_nav_health.PhantomBusterClient") as MockPB,
        ):
            pb_instance = MagicMock()
            pb_instance.__enter__ = lambda s: pb_instance
            pb_instance.__exit__ = MagicMock(return_value=False)
            pb_instance.launch_agent.return_value = MagicMock(
                container_id="c1", agent_id="inbox-123"
            )
            pb_instance.wait_for_completion.return_value = MagicMock(status="finished")
            pb_instance.get_container_output.return_value = fake_output
            MockPB.return_value = pb_instance
            result = check_regular_cookie_health()
        assert result.status == "fail"

    def test_auth_failure_markers_exported_from_pb_envelope(self):
        """AUTH_FAILURE_MARKERS is importable from clients.pb_envelope."""
        from clients.pb_envelope import _AUTH_FAILURE_MARKERS, AUTH_FAILURE_MARKERS

        assert AUTH_FAILURE_MARKERS is _AUTH_FAILURE_MARKERS, (
            "Backward-compat alias _AUTH_FAILURE_MARKERS must point to AUTH_FAILURE_MARKERS"
        )
        assert "no valid credentials" in AUTH_FAILURE_MARKERS
        assert "network-cookie-invalid" in AUTH_FAILURE_MARKERS


# ══════════════════════════════════════════════════════════════════════════════
# T1.3  pb_send_recovery container attribution guard
# ══════════════════════════════════════════════════════════════════════════════

class TestPBSendRecoveryContainerGuard:
    """T1.3: When expected_container_id is provided and mismatches the output's
    containerId, recovery must skip entirely (skipped_wrong_run=-1) and make
    no Attio advances."""

    _CSV_URL = "https://phantombuster.s3.amazonaws.com/x/y/result.csv"

    def _make_sent_csv(self) -> str:
        return (
            "linkedinProfileUrl,fullName,timestamp,status,message,error\n"
            "https://linkedin.com/in/test-user,Test User,2026-06-09,Message sent,Hi,\n"
        )

    def test_mismatched_container_skips_recovery_and_no_attio_writes(self):
        """expected_container_id != observed containerId → skipped_wrong_run=-1,
        zero Attio updates."""
        from workflows.pb_send_recovery import recover_unrecorded_dm_sends

        pb_output = {
            "output": f"✅ CSV saved at {self._CSV_URL}\n",
            "status": "finished",
            "isAgentRunning": False,
            "containerId": "container-WRONG",  # different from expected
        }
        pb = MagicMock()
        pb.get_output.return_value = pb_output

        attio = MagicMock()

        with patch("workflows.pb_send_recovery._download_csv", return_value=self._make_sent_csv()):
            result = recover_unrecorded_dm_sends(
                attio, pb, "msg-sender-id", "list-id", "2026-06-09",
                dry_run=False,
                expected_container_id="container-EXPECTED",
            )

        assert result["skipped_wrong_run"] == -1, (
            f"Expected skipped_wrong_run=-1; got {result}"
        )
        assert result["recovered"] == 0
        attio.update_list_entry.assert_not_called()

    def test_matching_container_proceeds_normally(self, capsys):
        """expected_container_id == observed containerId → normal recovery path."""
        from clients.attio import _canonical_linkedin_url
        from workflows.pb_send_recovery import recover_unrecorded_dm_sends

        url = "https://linkedin.com/in/test-user"
        record_id = "rec-001"
        entry_values = {
            "stage": [{"status": {"title": "DM1 Sent"}}],
            "last_contact_date": [{"value": "2026-06-08"}],
        }
        entry = {
            "entry_id": "entry-001",
            "parent_record_id": record_id,
            "entry_values": entry_values,
        }
        person = {
            "id": {"record_id": record_id},
            "values": {"linkedin": [{"value": url}]},
        }

        pb_output = {
            "output": f"✅ CSV saved at {self._CSV_URL}\n",
            "status": "finished",
            "isAgentRunning": False,
            "containerId": "container-MATCH",
        }
        pb = MagicMock()
        pb.get_output.return_value = pb_output

        attio = MagicMock()
        attio.query_list_entries.return_value = [entry]
        attio.update_list_entry.return_value = {}
        attio.search_person_by_linkedin.side_effect = (
            lambda u: person if _canonical_linkedin_url(u) == _canonical_linkedin_url(url) else None
        )

        csv_text = (
            "linkedinProfileUrl,fullName,timestamp,status,message,error\n"
            f"{url},Test User,2026-06-09,Message sent,Hi,\n"
        )
        with patch('workflows.pb_send_recovery._download_csv', return_value=csv_text), patch('workflows.daily_check._attio_advance_with_escalation', return_value=True):
            # Patch the advance function (deferred import in pb_send_recovery)
            result = recover_unrecorded_dm_sends(
                attio, pb, "msg-sender-id", "list-id", "2026-06-09",
                dry_run=False,
                expected_container_id="container-MATCH",
            )

        assert result["skipped_wrong_run"] == 0
        assert result["recovered"] == 1

    def test_no_expected_container_warns_but_proceeds(self, capsys):
        """No expected_container_id → backward-compat warning emitted, no skip."""
        from workflows.pb_send_recovery import recover_unrecorded_dm_sends

        pb_output = {
            "output": "No sends.\n",
            "status": "finished",
            "isAgentRunning": False,
            "containerId": "container-some",
        }
        pb = MagicMock()
        pb.get_output.return_value = pb_output

        attio = MagicMock()
        attio.query_list_entries.return_value = []

        with patch("workflows.pb_send_recovery._download_csv", return_value=""):
            result = recover_unrecorded_dm_sends(
                attio, pb, "msg-sender-id", "list-id", "2026-06-09",
                expected_container_id=None,
            )

        # skipped_wrong_run stays 0 (no hard skip) — backward-compat path
        assert result["skipped_wrong_run"] == 0
        # No advances
        attio.update_list_entry.assert_not_called()

    def test_skipped_wrong_run_key_present_in_summary(self):
        """The skipped_wrong_run key is always present in the returned summary."""
        from workflows.pb_send_recovery import recover_unrecorded_dm_sends

        pb = MagicMock()
        pb.get_output.return_value = {"output": "", "status": "finished"}
        attio = MagicMock()
        attio.query_list_entries.return_value = []

        result = recover_unrecorded_dm_sends(
            attio, pb, "msg-id", "list-id", "2026-06-09"
        )
        assert "skipped_wrong_run" in result


# ══════════════════════════════════════════════════════════════════════════════
# T1.5b  .env.example documents PB_INBOX_SCRAPER_ID
# ══════════════════════════════════════════════════════════════════════════════

class TestEnvExampleDocumentsPBInboxScraperId:
    """T1.5b: .env.example must declare PB_INBOX_SCRAPER_ID with a comment
    explaining that it is required for Phase 0.5 reply detection."""

    def test_env_example_contains_pb_inbox_scraper_id(self):
        env_example = Path(__file__).parent.parent / ".env.example"
        assert env_example.exists(), ".env.example not found at repo root"
        content = env_example.read_text()
        assert "PB_INBOX_SCRAPER_ID" in content, (
            "PB_INBOX_SCRAPER_ID not found in .env.example"
        )

    def test_env_example_pb_inbox_scraper_id_has_comment(self):
        """The comment must mention reply detection / Phase 0.5."""
        env_example = Path(__file__).parent.parent / ".env.example"
        content = env_example.read_text()
        lines = content.splitlines()
        # Find the line(s) near PB_INBOX_SCRAPER_ID
        idx = next(
            (i for i, line in enumerate(lines) if "PB_INBOX_SCRAPER_ID" in line), None
        )
        assert idx is not None
        # Check the 5 lines before the declaration for a comment
        surrounding = "\n".join(lines[max(0, idx - 5) : idx + 2]).lower()
        assert "reply" in surrounding or "phase 0.5" in surrounding or "0.5" in surrounding, (
            "Expected comment near PB_INBOX_SCRAPER_ID to mention 'reply' or 'Phase 0.5'"
        )


# ══════════════════════════════════════════════════════════════════════════════
# T1.5c  Loud error on 0-byte google-oauth.json
# ══════════════════════════════════════════════════════════════════════════════

class TestGoogleOAuthZeroByteGuard:
    """T1.5c (amended 2026-06-09): get_client() must raise RuntimeError with
    a clear message when google-oauth.json is missing or 0 bytes AND an
    interactive flow would actually need it — i.e. when no usable
    authorized-user token exists. With a valid stored token, gspread.oauth()
    never reads the client secret, so the preflight must not fire (the
    unconditional version blocked the 2026-06-09 DM send on a client-secret
    file that had been harmlessly empty for a month)."""

    def _no_token(self, tmp_path, monkeypatch):
        """Point AUTHORIZED_USER at a non-existent file so the interactive
        flow (and therefore the client-secret preflight) applies."""
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER",
            str(tmp_path / "google-authorized-user.json"),
        )
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER_BACKUP",
            str(tmp_path / "google-authorized-user.json.bak"),
        )

    def test_raises_on_missing_credentials_file(self, tmp_path, monkeypatch):
        self._no_token(tmp_path, monkeypatch)
        monkeypatch.setattr(
            "clients.google_sheets.OAUTH_CREDENTIALS",
            str(tmp_path / "google-oauth.json"),  # does not exist
        )
        from clients.google_sheets import get_client

        with pytest.raises(RuntimeError, match="Missing OAuth credentials file"):
            get_client()

    def test_raises_on_zero_byte_credentials_file(self, tmp_path, monkeypatch):
        self._no_token(tmp_path, monkeypatch)
        cred_file = tmp_path / "google-oauth.json"
        cred_file.write_text("")  # 0-byte
        monkeypatch.setattr(
            "clients.google_sheets.OAUTH_CREDENTIALS",
            str(cred_file),
        )
        from clients.google_sheets import get_client

        with pytest.raises(RuntimeError, match="0 bytes"):
            get_client()

    def test_valid_token_skips_client_secret_preflight(self, tmp_path, monkeypatch):
        """Regression (2026-06-09 DM-send outage): a usable authorized-user
        token + a 0-byte client secret must NOT raise — gspread.oauth()
        won't read the client secret on this path."""
        token_file = tmp_path / "google-authorized-user.json"
        token_file.write_text('{"refresh_token": "rt", "type": "authorized_user"}')
        cred_file = tmp_path / "google-oauth.json"
        cred_file.write_text("")  # 0-byte — harmless with a valid token
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER", str(token_file),
        )
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER_BACKUP",
            str(tmp_path / "google-authorized-user.json.bak"),
        )
        monkeypatch.setattr(
            "clients.google_sheets.OAUTH_CREDENTIALS", str(cred_file),
        )
        from clients.google_sheets import get_client

        with patch("gspread.oauth") as mock_oauth:
            get_client()
        mock_oauth.assert_called_once()

    def test_no_error_on_valid_credentials_file(self, tmp_path, monkeypatch):
        """A non-empty credentials file must NOT raise in _check_oauth_credentials."""
        cred_file = tmp_path / "google-oauth.json"
        cred_file.write_text('{"type": "authorized_user"}')
        monkeypatch.setattr(
            "clients.google_sheets.OAUTH_CREDENTIALS",
            str(cred_file),
        )
        # Also stub the authorized-user path and gspread so get_client doesn't
        # actually try to call the real oauth flow.
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER",
            str(tmp_path / "google-authorized-user.json"),
        )
        monkeypatch.setattr(
            "clients.google_sheets.AUTHORIZED_USER_BACKUP",
            str(tmp_path / "google-authorized-user.json.bak"),
        )
        with patch("gspread.oauth") as mock_oauth:
            mock_oauth.return_value = MagicMock()
            from clients import google_sheets
            # Just check _check_oauth_credentials doesn't raise
            google_sheets._check_oauth_credentials()  # must not raise


# ══════════════════════════════════════════════════════════════════════════════
# L6-3  daily --dry-run skips mutating health pre-flight
# ══════════════════════════════════════════════════════════════════════════════

class TestDryRunSkipsSalesNavHealthPreFlight:
    """L6-3: Under --dry-run with PRE_INVITE_DEGREE_CHECK_BACKEND=sales_nav,
    the health pre-flight must be skipped: it launches PB jobs."""

    def _setup_stubs(self, monkeypatch, quick_check_return):
        """Patch everything except the health pre-flight invocation."""
        from click.testing import CliRunner

        # Track whether quick_check was called
        called = {"quick_check": 0}

        def _stub_quick_check(*a, **k):
            called["quick_check"] += 1
            return quick_check_return

        monkeypatch.setattr(
            "scripts.validate_sales_nav_health.quick_check", _stub_quick_check,
        )
        # Stub the import inside cli.py (lazy import)
        import scripts.validate_sales_nav_health as _mod
        monkeypatch.setattr(_mod, "quick_check", _stub_quick_check)

        monkeypatch.setattr("workflows.record_cache.preload_pipeline_persons", lambda *a, **k: 0)
        monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr("clients.attio.AttioClient.query_list_entries", lambda self, **_k: [])
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("workflows.run_lock.acquire_run_lock", lambda *_a, **_k: __import__("contextlib").nullcontext())
        monkeypatch.setattr("workflows.daily_check.run_connection_requests", lambda *a, **k: {"sent": 0})
        monkeypatch.setattr("workflows.daily_check.run_dm_sequencing", lambda *a, **k: {"dm1": 0, "dm2": 0, "dm3": 0})
        monkeypatch.setattr("workflows.pb_send_recovery.recover_unrecorded_dm_sends", lambda *a, **k: {})
        monkeypatch.setattr("workflows.starvation.evaluate_pipeline_starvation", lambda *a, **k: {"triggers_fired": []})
        monkeypatch.setattr("workflows.detect_responses.detect_responses", lambda *a, **k: {"detected": 0})

        return called, CliRunner()

    def test_dry_run_with_sales_nav_backend_skips_preflight(self, monkeypatch):
        """quick_check must not be called under --dry-run."""
        called, runner = self._setup_stubs(monkeypatch, quick_check_return=(0, "All OK"))

        from cli import cli
        result = runner.invoke(
            cli,
            ["daily", "--dry-run", "--yes"],
            env={
                **os.environ,
                "PRE_INVITE_DEGREE_CHECK_BACKEND": "sales_nav",
                "PB_PROFILE_SCRAPER_ID": "scraper",
                "PB_SALES_NAV_PROFILE_SCRAPER_ID": "sn-scraper",
                "PB_INBOX_SCRAPER_ID": "inbox",
                "ATTIO_LIST_ID": "lst",
            },
        )
        assert called["quick_check"] == 0, (
            f"Expected no quick_check under dry-run; output:\n{result.output}"
        )

    def test_dry_run_does_not_probe_cookie_failure(self, monkeypatch):
        """Dry-run does not probe cookie health or claim it passed."""
        called, runner = self._setup_stubs(
            monkeypatch, quick_check_return=(1, "SN cookie dead")
        )

        from cli import cli
        result = runner.invoke(
            cli,
            ["daily", "--dry-run", "--yes"],
            env={
                **os.environ,
                "PRE_INVITE_DEGREE_CHECK_BACKEND": "sales_nav",
                "PB_PROFILE_SCRAPER_ID": "scraper",
                "PB_SALES_NAV_PROFILE_SCRAPER_ID": "sn-scraper",
                "PB_INBOX_SCRAPER_ID": "inbox",
                "ATTIO_LIST_ID": "lst",
            },
        )
        assert called["quick_check"] == 0
        # No probe means no health result; inventory is still available.
        assert result.exit_code == 0, (
            f"Expected successful inventory; got {result.exit_code}"
        )

    def test_regular_backend_dry_run_does_not_run_preflight(self, monkeypatch):
        """Backend=regular should NOT call quick_check in any mode."""
        called, runner = self._setup_stubs(monkeypatch, quick_check_return=(0, "OK"))

        from cli import cli
        result = runner.invoke(
            cli,
            ["daily", "--dry-run", "--yes"],
            env={
                **os.environ,
                "PRE_INVITE_DEGREE_CHECK_BACKEND": "regular",
                "PB_PROFILE_SCRAPER_ID": "scraper",
                "PB_INBOX_SCRAPER_ID": "inbox",
                "ATTIO_LIST_ID": "lst",
            },
        )
        assert called["quick_check"] == 0, (
            f"quick_check should NOT fire for regular backend; output:\n{result.output}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# L6-6  health-check makes a real PB API read (list_agents)
# ══════════════════════════════════════════════════════════════════════════════

class TestHealthCheckMakesRealPBRead:
    """L6-6: The health-check command must call list_agents() (real API read)
    so a bad API key fails the check rather than passing silently."""

    def test_health_check_calls_list_agents(self, monkeypatch):
        """list_agents must be called; a failure there must surface as FAIL."""
        from click.testing import CliRunner

        from cli import cli

        called = {"list_agents": 0}

        def _stub_list_agents(self):
            called["list_agents"] += 1
            return []

        monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "clients.attio.AttioClient._request",
            lambda self, *a, **k: {},
        )
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.list_agents",
            _stub_list_agents,
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["health-check"])
        assert called["list_agents"] == 1, (
            f"Expected list_agents to be called; output:\n{result.output}"
        )

    def test_health_check_fails_when_list_agents_raises(self, monkeypatch):
        """If list_agents raises (bad API key), health-check must exit non-zero."""
        from click.testing import CliRunner

        from cli import cli

        monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "clients.attio.AttioClient._request",
            lambda self, *a, **k: {},
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__init__",
            lambda self, **_kwargs: None,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__enter__",
            lambda self: self,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__exit__",
            lambda self, *a: False,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.list_agents",
            lambda self: (_ for _ in ()).throw(RuntimeError("Unauthorized: bad API key")),
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["health-check"])
        assert result.exit_code != 0, (
            f"Expected non-zero exit when list_agents fails; exit={result.exit_code}"
        )
        assert "PhantomBuster: FAIL" in result.output

    def test_health_check_prints_cookie_liveness_note(self, monkeypatch):
        """health-check must print a note about cookie liveness NOT being tested."""
        from click.testing import CliRunner

        from cli import cli

        monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "clients.attio.AttioClient._request",
            lambda self, *a, **k: {},
        )
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.phantombuster.PhantomBusterClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.list_agents",
            lambda self: [],
        )

        runner = CliRunner()
        result = runner.invoke(cli, ["health-check"])
        assert "session-cookie" in result.output.lower() or "cookie" in result.output.lower(), (
            f"Expected cookie liveness note in output; got:\n{result.output}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# L4-3  learn command acquires run lock; second invocation exits 75
# ══════════════════════════════════════════════════════════════════════════════

class TestLearnRunLock:
    """L4-3: The `learn` command must acquire a run lock so concurrent
    invocations exit 75 (EX_TEMPFAIL) rather than racing."""

    def test_learn_exits_75_when_lock_held(self, lock_dir, monkeypatch):
        """Simulate a second concurrent invocation: flock is already held;
        `learn` must exit 75 without doing any work."""
        from click.testing import CliRunner

        from workflows.run_lock import EXIT_TEMPFAIL

        monkeypatch.setattr("workflows.run_lock.DEFAULT_LOCK_DIR", lock_dir)

        # Hold the sales-learn lock from an out-of-band fd.
        lock_path = lock_dir / "sales-learn.lock"
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            from cli import cli
            runner = CliRunner()
            result = runner.invoke(cli, ["learn", "--dry-run"])
            assert result.exit_code == EXIT_TEMPFAIL, (
                f"Expected exit {EXIT_TEMPFAIL}; got {result.exit_code}; "
                f"output:\n{result.output}"
            )
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def test_learn_acquires_and_releases_lock(self, lock_dir, monkeypatch):
        """When the lock is free, `learn` must acquire it, run, and release it."""
        from click.testing import CliRunner

        monkeypatch.setattr("workflows.run_lock.DEFAULT_LOCK_DIR", lock_dir)

        # Stub everything that would cause network calls.
        monkeypatch.setattr("clients.attio.AttioClient.__init__", lambda self, **_kwargs: None)
        monkeypatch.setattr("clients.attio.AttioClient.__enter__", lambda self: self)
        monkeypatch.setattr("clients.attio.AttioClient.__exit__", lambda self, *a: False)
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "workflows.learn.measure_cohorts", lambda *a, **k: []
        )
        monkeypatch.setattr(
            "workflows.learn.evaluate_experiments", lambda *a, **k: []
        )

        from cli import cli
        runner = CliRunner()
        result = runner.invoke(cli, ["learn", "--dry-run"])
        # Should succeed (exit 0) when no lock contention.
        assert result.exit_code == 0, (
            f"Expected exit 0; got {result.exit_code}; output:\n{result.output}"
        )
        # Lock file must be released after the command.
        lock_path = lock_dir / "sales-learn.lock"
        # We can re-acquire it → it's free.
        fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        except BlockingIOError:
            pytest.fail("Lock still held after learn command finished")
        finally:
            os.close(fd)


@pytest.fixture
def lock_dir(tmp_path):
    """Isolated lock directory to avoid ~/.outbound-agent pollution."""
    d = tmp_path / "locks"
    d.mkdir()
    return d
