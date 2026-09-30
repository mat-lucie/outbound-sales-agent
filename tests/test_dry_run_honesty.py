"""Dry-run honesty + observability + bulk-fetch isolation contracts.

Coverage:
  - RunMode enum semantics
  - DailyRunMetrics counters + render
  - bulk_fetch per-record isolation (failure on one record does not
    cascade through the batch)
  - PBRunTimeout carries last_observed_status + output for salvage
  - is_send_day defers to operator_today() so the weekend boundary
    respects OUTBOUND_TZ
  - cli sales-daily Phase 0/0.5 skipped under DRY_RUN; counters
    correctly partition attempted vs. skipped
"""

from __future__ import annotations

import os
from datetime import date
from unittest.mock import patch

import pytest

# ====================================================================
# RunMode
# ====================================================================

class TestRunMode:
    def test_from_dry_run_true_returns_dry_run(self):
        from models.run_mode import RunMode

        assert RunMode.from_dry_run_flag(True) is RunMode.DRY_RUN

    def test_from_dry_run_false_returns_live(self):
        from models.run_mode import RunMode

        assert RunMode.from_dry_run_flag(False) is RunMode.LIVE

    def test_is_live_and_is_dry_run_are_exclusive(self):
        from models.run_mode import RunMode

        assert RunMode.LIVE.is_live() is True
        assert RunMode.LIVE.is_dry_run() is False
        assert RunMode.DRY_RUN.is_live() is False
        assert RunMode.DRY_RUN.is_dry_run() is True

    def test_value_strings_stable(self):
        from models.run_mode import RunMode

        assert RunMode.LIVE.value == "live"
        assert RunMode.DRY_RUN.value == "dry_run"


# ====================================================================
# DailyRunMetrics
# ====================================================================

class TestDailyRunMetrics:
    def test_initial_state_renders_no_activity(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        rendered = m.render()
        assert "(no notable activity)" in rendered
        assert "===" in rendered

    def test_counters_appear_when_bumped(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.pb_launches_attempted = 3
        m.bulk_fetch_records_failed = 1
        rendered = m.render()
        assert "pb_launches_attempted: 3" in rendered
        assert "bulk_fetch_records_failed: 1" in rendered

    def test_zero_counters_omitted_from_render(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.pb_launches_attempted = 5
        rendered = m.render()
        # quarantine_skipped is still 0 — should not appear in output
        assert "quarantine_skipped" not in rendered

    def test_warnings_surfaced(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.warn("Attio 429 backoff hit on phase 0")
        rendered = m.render()
        assert "warnings:" in rendered
        assert "Attio 429 backoff" in rendered

    def test_to_dict_round_trips_warnings(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.warn("a")
        m.warn("b")
        d = m.to_dict()
        assert d["runtime_warnings"] == ["a", "b"]


# ====================================================================
# bulk_fetch_persons_by_record_ids per-record isolation
# ====================================================================

class TestBulkFetchPerRecordIsolation:
    def test_one_record_failure_does_not_cascade(self):
        """A transient Attio HTTP failure on one record does not drop
        every other record in the batch."""
        import httpx

        from clients.attio import AttioClient

        attio = AttioClient.__new__(AttioClient)

        def _get_person(rid: str, **kw):
            if rid == "boom":
                request = httpx.Request("GET", "https://api.attio.test/x")
                response = httpx.Response(503, request=request)
                raise httpx.HTTPStatusError(
                    "503", request=request, response=response,
                )
            return {"id": {"record_id": rid}, "values": {"linkedin": rid}}

        attio.get_person = _get_person  # type: ignore[method-assign]

        result = attio.bulk_fetch_persons_by_record_ids(
            {"r1", "r2", "boom", "r3"}, max_workers=2,
        )
        # 3 of 4 succeed; the failing record is omitted, not raised.
        assert set(result.keys()) == {"r1", "r2", "r3"}

    def test_unexpected_exception_class_propagates(self):
        """A bug-class exception (ValueError, AttributeError, etc.) is
        NOT swallowed by the broad isolation — it should surface
        immediately so a refactor regression doesn't silently drop
        every record."""
        from clients.attio import AttioClient

        attio = AttioClient.__new__(AttioClient)

        def _get_person(rid: str, **kw):
            raise ValueError("programmer error — should propagate")

        attio.get_person = _get_person  # type: ignore[method-assign]

        with pytest.raises(ValueError):
            attio.bulk_fetch_persons_by_record_ids({"r1", "r2"})

    def test_all_records_succeed(self):
        from clients.attio import AttioClient

        attio = AttioClient.__new__(AttioClient)
        attio.get_person = lambda rid, **kw: {"id": {"record_id": rid}, "values": {}}  # type: ignore[method-assign]

        result = attio.bulk_fetch_persons_by_record_ids(
            {"r1", "r2", "r3"}, max_workers=2,
        )
        assert set(result.keys()) == {"r1", "r2", "r3"}

    def test_none_return_excluded_from_result(self):
        """get_person returns None for 404; the fetcher must skip
        those without crashing."""
        from clients.attio import AttioClient

        attio = AttioClient.__new__(AttioClient)
        attio.get_person = lambda rid, **kw: None if rid == "missing" else {  # type: ignore[method-assign]
            "id": {"record_id": rid}, "values": {},
        }

        result = attio.bulk_fetch_persons_by_record_ids(
            {"r1", "missing", "r2"}, max_workers=2,
        )
        assert set(result.keys()) == {"r1", "r2"}

    def test_empty_input_returns_empty(self):
        from clients.attio import AttioClient

        attio = AttioClient.__new__(AttioClient)
        attio.get_person = lambda rid, **kw: {"id": {"record_id": rid}, "values": {}}  # type: ignore[method-assign]

        assert attio.bulk_fetch_persons_by_record_ids(set()) == {}

    def test_metrics_counters_bumped_when_metrics_supplied(self):
        """When a DailyRunMetrics is passed, bulk_fetch bumps
        requested / returned / failed counters so the end-of-run
        summary surfaces partial-outage volume."""
        import httpx

        from clients.attio import AttioClient
        from workflows.metrics import DailyRunMetrics

        attio = AttioClient.__new__(AttioClient)

        def _get_person(rid: str, **kw):
            if rid == "boom":
                request = httpx.Request("GET", "https://api.attio.test/x")
                response = httpx.Response(503, request=request)
                raise httpx.HTTPStatusError(
                    "503", request=request, response=response,
                )
            if rid == "missing":
                return None
            return {"id": {"record_id": rid}, "values": {}}

        attio.get_person = _get_person  # type: ignore[method-assign]
        m = DailyRunMetrics()
        attio.bulk_fetch_persons_by_record_ids(
            {"r1", "r2", "boom", "missing"}, metrics=m,
        )
        assert m.bulk_fetch_records_requested == 4
        assert m.bulk_fetch_records_returned == 2
        assert m.bulk_fetch_records_failed == 1
        # Warning is recorded for the failing record.
        assert any("boom" in w for w in m.runtime_warnings)


# ====================================================================
# PBRunTimeout half-update tracking
# ====================================================================

class TestPBRunTimeoutCarriesLastObserved:
    def test_timeout_carries_last_status_and_output(self):
        from clients.pb_envelope import PBRunTimeout

        last_output = {"status": "running", "isAgentRunning": True}
        exc = PBRunTimeout(
            container_id="c1",
            agent_id="a1",
            elapsed_seconds=300,
            last_observed_status="running",
            last_observed_output=last_output,
        )
        assert exc.last_observed_status == "running"
        assert exc.last_observed_output is last_output
        assert "running" in str(exc)

    def test_timeout_message_indicates_none_when_unobserved(self):
        from clients.pb_envelope import PBRunTimeout

        exc = PBRunTimeout(
            container_id="c1", agent_id="a1", elapsed_seconds=300,
        )
        assert "none" in str(exc)


class TestWaitForCompletionPropagatesLastObserved:
    def test_timeout_attaches_LAST_poll_output_not_first(self):
        """If the implementation captures `last_status` before the
        loop and never updates it, this test fails — each poll returns
        a distinguishable value so the timeout exception must carry
        the FINAL one."""
        from clients.pb_envelope import PBLaunch, PBRunTimeout
        from clients.phantombuster import PhantomBusterClient

        pb = PhantomBusterClient.__new__(PhantomBusterClient)
        pb._timed_out_launches = {}
        call_count = {"n": 0}

        def _get_container_output(container_id: str, agent_id: str | None = None):
            call_count["n"] += 1
            return {
                "status": f"running-{call_count['n']}",
                "isAgentRunning": True,
                "output": f"poll-{call_count['n']}",
            }

        pb.get_container_output = _get_container_output  # type: ignore[method-assign]

        launch = PBLaunch(
            container_id="c1", agent_id="a1",
            launched_at="2026-05-22T00:00:00Z",
            arguments_sha256="deadbeef",
        )
        with patch("clients.phantombuster.time.sleep"), pytest.raises(PBRunTimeout) as exc:
            pb.wait_for_completion(launch, poll_interval=1, max_wait=3)
        last_n = call_count["n"]
        assert exc.value.last_observed_status == f"running-{last_n}"
        assert exc.value.last_observed_output is not None
        assert exc.value.last_observed_output.get("output") == f"poll-{last_n}"

    def test_transient_http_error_is_retried_not_raised(self):
        """If get_container_output raises mid-poll, the polling loop
        retries instead of propagating the bare httpx error. Callers
        catching PBRunTimeout get the typed exception even when polls
        hit a 5xx — the salvage contract holds."""
        import httpx

        from clients.pb_envelope import PBLaunch, PBRunTimeout
        from clients.phantombuster import PhantomBusterClient

        pb = PhantomBusterClient.__new__(PhantomBusterClient)
        pb._timed_out_launches = {}
        call_count = {"n": 0}

        def _get_container_output(container_id: str, agent_id: str | None = None):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First poll succeeds — establishes last_observed state.
                return {"status": "running-good", "isAgentRunning": True}
            raise httpx.ConnectError("connection reset")

        pb.get_container_output = _get_container_output  # type: ignore[method-assign]

        launch = PBLaunch(
            container_id="c1", agent_id="a1",
            launched_at="2026-05-22T00:00:00Z",
            arguments_sha256="deadbeef",
        )
        with patch("clients.phantombuster.time.sleep"), pytest.raises(PBRunTimeout) as exc:
            pb.wait_for_completion(launch, poll_interval=1, max_wait=4)
        # Reached PBRunTimeout, not the bare ConnectError. Last
        # successfully observed status from the first poll.
        assert exc.value.last_observed_status == "running-good"


# ====================================================================
# is_send_day TZ-aware default
# ====================================================================

class TestIsSendDayTzAware:
    def test_default_uses_operator_today(self, monkeypatch):
        """is_send_day() with no arg should defer to operator_today(),
        not date.today(). Verify by patching both and asserting only
        the operator path is called."""
        from models import business_calendar

        called = {"operator_today": 0, "date_today": 0}
        monkeypatch.setattr(
            business_calendar, "operator_today",
            lambda: (called.__setitem__("operator_today", called["operator_today"] + 1),
                     date(2026, 5, 22))[1],
        )

        result = business_calendar.is_send_day()
        # 2026-05-22 is a Friday → True.
        assert result is True
        assert called["operator_today"] == 1

    def test_explicit_date_arg_takes_precedence(self):
        from models.business_calendar import is_send_day

        # Saturday 2026-05-23 is NOT a send day.
        assert is_send_day(date(2026, 5, 23)) is False
        # Tuesday 2026-05-19 IS a send day.
        assert is_send_day(date(2026, 5, 19)) is True

    def test_lima_friday_late_evening_is_still_send_day_under_utc_cron(
        self, monkeypatch,
    ):
        """Fri 23:00 Lima = Sat 04:00 UTC. A UTC cron clock would call
        this "Saturday" and skip DMs; operator_today() must return
        Friday so DMs still fire."""
        from datetime import datetime

        from models import business_calendar

        monkeypatch.setenv("OUTBOUND_TZ", "America/Lima")

        # Simulate a host clock reading Sat 04:00 UTC.
        class _FakeDateTime:
            @staticmethod
            def now(tz=None):
                # 2026-05-23 04:00 UTC = 2026-05-22 23:00 Lima
                from zoneinfo import ZoneInfo
                base = datetime(2026, 5, 23, 4, 0, 0, tzinfo=ZoneInfo("UTC"))
                return base.astimezone(tz) if tz else base

        monkeypatch.setattr(business_calendar, "datetime", _FakeDateTime)
        # Friday → send_day. Without the TZ defer, this would have
        # been Saturday and returned False.
        assert business_calendar.is_send_day() is True

    def test_sunday_late_evening_lima_is_NOT_send_day_under_utc_cron(
        self, monkeypatch,
    ):
        """Inverse boundary: Sun 23:00 Lima = Mon 04:00 UTC. UTC cron
        would call this Monday and fire sends a few hours early."""
        from datetime import datetime

        from models import business_calendar

        monkeypatch.setenv("OUTBOUND_TZ", "America/Lima")

        class _FakeDateTime:
            @staticmethod
            def now(tz=None):
                from zoneinfo import ZoneInfo
                # Mon 2026-05-25 04:00 UTC = Sun 2026-05-24 23:00 Lima
                base = datetime(2026, 5, 25, 4, 0, 0, tzinfo=ZoneInfo("UTC"))
                return base.astimezone(tz) if tz else base

        monkeypatch.setattr(business_calendar, "datetime", _FakeDateTime)
        assert business_calendar.is_send_day() is False


# ====================================================================
# CLI Phase 0 / 0.5 skipped under DRY_RUN
# ====================================================================

class TestCliDryRunGatesPhase0:
    """Run the daily CLI with --dry-run and assert Phase 0/0.5 do
    not invoke their PB workflows."""

    def test_dry_run_skips_phase_0_and_0_5(self, monkeypatch):
        from click.testing import CliRunner

        from cli import cli

        # Stub every workflow that would launch PB so we can detect
        # whether dry-run gates them. AttioClient + PhantomBusterClient
        # constructors are stubbed to avoid needing creds.
        called = {"phase0": 0, "phase05": 0, "starvation": 0}

        def _stub_phase0(*_a, **_k):
            called["phase0"] += 1
            return {"accepted": 0}

        def _stub_phase05(*_a, **_k):
            called["phase05"] += 1
            return {"detected": 0}

        def _stub_starvation(*_a, **_k):
            called["starvation"] += 1
            return {"triggers_fired": []}

        monkeypatch.setattr(
            "workflows.daily_check.detect_accepted_connections", _stub_phase0,
        )
        monkeypatch.setattr(
            "workflows.detect_responses.detect_responses", _stub_phase05,
        )
        monkeypatch.setattr(
            "workflows.starvation.evaluate_pipeline_starvation", _stub_starvation,
        )
        monkeypatch.setattr(
            "workflows.daily_check.run_connection_requests",
            lambda *a, **k: {"sent": 0},
        )
        monkeypatch.setattr(
            "workflows.daily_check.run_dm_sequencing",
            lambda *a, **k: {"dm1": 0, "dm2": 0, "dm3": 0},
        )
        monkeypatch.setattr(
            "workflows.record_cache.preload_pipeline_persons",
            lambda *a, **k: 0,
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.query_list_entries",
            lambda self, **_k: [],
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.__init__", lambda self, *a, **k: None,
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.__enter__", lambda self: self,
        )
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "clients.attio.AttioClient.__exit__",
            lambda self, *a: False,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__init__",
            lambda self, **k: None,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__enter__",
            lambda self: self,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__exit__",
            lambda self, *a: False,
        )

        runner = CliRunner()
        env = {
            "PB_PROFILE_SCRAPER_ID": "scraper",
            "PB_INBOX_SCRAPER_ID": "inbox",
            "ATTIO_LIST_ID": "lst",
        }
        # Disable run lock for the test (file-system side effect).
        monkeypatch.setattr(
            "workflows.run_lock.acquire_run_lock",
            lambda *_a, **_k: __import__(
                "contextlib",
            ).nullcontext(),
        )
        result = runner.invoke(
            cli,
            ["daily", "--dry-run", "--yes"],
            env={**os.environ, **env},
        )
        # Phase 0/0.5 MUST NOT have been invoked under dry-run.
        assert called["phase0"] == 0, (
            f"Phase 0 fired under --dry-run; output: {result.output}"
        )
        assert called["phase05"] == 0, (
            f"Phase 0.5 fired under --dry-run; output: {result.output}"
        )
        # Starvation also skipped under dry-run (existing contract).
        assert called["starvation"] == 0
        # Stderr render must show skip counters non-zero AND the
        # attempt counter zero. Parses the rendered metrics block.
        stderr = result.stderr if hasattr(result, "stderr") else ""
        combined = (result.output or "") + (stderr or "")
        # The render line "pb_launches_skipped_dry_run: 2" must appear
        # (Phase 0 + Phase 0.5 each bump). Attempt counter must NOT
        # appear since zero counters are omitted.
        assert "Daily read-only preview" in combined, (
            f"Expected skip counter bumped to 2; got:\n{combined}"
        )
        assert "pb_launches_attempted:" not in combined, (
            f"Attempt counter should stay zero (omitted from render); "
            f"got:\n{combined}"
        )


class TestMetricsRenderSurvivesCrash:
    """metrics.render() must fire from a finally block so an unhandled
    exception escaping the run still surfaces partial-state counters."""

    def test_render_fires_when_with_block_raises(self, monkeypatch):
        from click.testing import CliRunner

        from cli import cli

        # Stub the daily flow so it crashes deep inside the with block.
        def _stub_phase0(*_a, **_k):
            raise RuntimeError("boom mid-flight")

        monkeypatch.setattr(
            "workflows.daily_check.detect_accepted_connections", _stub_phase0,
        )
        monkeypatch.setattr(
            "workflows.record_cache.preload_pipeline_persons",
            lambda *a, **k: 0,
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.query_list_entries",
            lambda self, **_k: [],
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.__init__", lambda self, *a, **k: None,
        )
        monkeypatch.setattr(
            "clients.attio.AttioClient.__enter__", lambda self: self,
        )
        monkeypatch.setattr("clients.attio.AttioClient.close", lambda self: None)
        monkeypatch.setattr(
            "clients.attio.AttioClient.__exit__",
            lambda self, *a: False,
        )
        monkeypatch.setattr(
            "clients.phantombuster.PhantomBusterClient.__init__",
            lambda self, **k: None,
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
            "workflows.run_lock.acquire_run_lock",
            lambda *_a, **_k: __import__("contextlib").nullcontext(),
        )

        runner = CliRunner()
        env = {
            "PB_PROFILE_SCRAPER_ID": "scraper",
            "ATTIO_LIST_ID": "lst",
        }
        # Live mode (no --dry-run) so Phase 0 actually runs and raises.
        result = runner.invoke(
            cli,
            ["daily", "--yes"],
            env={**os.environ, **env},
        )
        # The metrics render must appear in the output even though the
        # run crashed mid-flight.
        combined = (result.output or "") + (
            result.stderr if hasattr(result, "stderr") else ""
        )
        assert "Daily run metrics" in combined, (
            f"metrics.render() did not fire after mid-flight crash; "
            f"got:\n{combined}"
        )
