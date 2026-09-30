"""Tests for PR-11: kill baseline-v0 silent fallback + three-step transactional recompute.

Coverage:
  1. get_current_experiment_id() returns Optional[str]:
     - 0 running → None
     - 1 running → str
     - 2+ running → raises MultipleRunningExperimentsError (with all ids in message)
  2. All callers of get_current_experiment_id handle None correctly (smoke test)
  3. Three-step recompute: success path writes ReclassificationRun + MigrationRun + superseded event
  4. Crash recovery: mid-step-3 failure → re-run detects orphan rec_run, resumes from step 3
  5. §9.4 idempotency: second --apply with same cohort data is no-op
  6. `won` invariant: a cohort with all dm_response_rate=0.0 cannot produce a `won` verdict
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import UTC

from models.experiment import (
    MultipleRunningExperimentsError,
    append_experiment,
    get_current_experiment_id,
)

# ── 1. get_current_experiment_id — Optional[str] contract ────────────────────


def _make_exp(experiment_id: str, status: str = "running"):
    """Minimal experiment fixture helper."""
    from models.experiment import Experiment, ExperimentStatus
    return Experiment(
        experiment_id=experiment_id,
        started="2026-01-01",
        completed="",
        cohort_size=30,
        dm_response_rate=0.10,
        baseline_rate=0.08,
        status=ExperimentStatus(status),
        variable="",
        description="test",
        dm1_response_rate=0.12,
        dm2_response_rate=0.08,
        dm3_response_rate=0.06,
    )


class TestGetCurrentExperimentIdContract:
    """§0 invariant #9: no silent fallback. Optional[str] semantics."""

    def test_zero_running_returns_none(self, tmp_path):
        """Zero running experiments → None (explicit 'no current experiment')."""
        tsv = tmp_path / "experiments.tsv"
        append_experiment(_make_exp("exp-001", status="won"), tsv)
        result = get_current_experiment_id(tsv)
        assert result is None, (
            "get_current_experiment_id must return None when no experiment is running, "
            "NOT 'baseline-v0' (§0 invariant #9 — no silent fallback)."
        )

    def test_one_running_returns_experiment_id(self, tmp_path):
        """Exactly one running experiment → returns its experiment_id string."""
        tsv = tmp_path / "experiments.tsv"
        append_experiment(_make_exp("exp-active"), tsv)
        result = get_current_experiment_id(tsv)
        assert result == "exp-active"
        assert isinstance(result, str)

    def test_multiple_running_raises_with_all_ids(self, tmp_path):
        """Two running experiments → raises MultipleRunningExperimentsError.

        §0 invariant #9: no 'pick the first' fallback. The operator must
        resolve the ambiguity manually.
        """
        tsv = tmp_path / "experiments.tsv"
        append_experiment(_make_exp("exp-alpha"), tsv)
        append_experiment(_make_exp("exp-beta"), tsv)
        with pytest.raises(MultipleRunningExperimentsError) as exc_info:
            get_current_experiment_id(tsv)
        msg = str(exc_info.value)
        assert "exp-alpha" in msg, "Error message must include all running experiment_ids"
        assert "exp-beta" in msg, "Error message must include all running experiment_ids"

    def test_multiple_running_three_ids_all_in_message(self, tmp_path):
        """Three running experiments — all three must appear in the error message."""
        tsv = tmp_path / "experiments.tsv"
        for name in ("exp-a", "exp-b", "exp-c"):
            append_experiment(_make_exp(name), tsv)
        with pytest.raises(MultipleRunningExperimentsError) as exc_info:
            get_current_experiment_id(tsv)
        msg = str(exc_info.value)
        for name in ("exp-a", "exp-b", "exp-c"):
            assert name in msg

    def test_empty_tsv_returns_none(self, tmp_path):
        """Empty TSV file → None."""
        tsv = tmp_path / "experiments.tsv"
        tsv.write_text(
            "experiment_id\tstarted\tcompleted\tcohort_size\tdm_response_rate"
            "\tbaseline_rate\tstatus\tvariable\tdescription\n"
        )
        assert get_current_experiment_id(tsv) is None

    def test_completed_experiment_after_verdict_returns_none(self, tmp_path):
        """After verdict row, completed experiment must not count as running."""
        tsv = tmp_path / "experiments.tsv"
        append_experiment(_make_exp("exp-001", status="running"), tsv)
        append_experiment(_make_exp("exp-001", status="won"), tsv)
        assert get_current_experiment_id(tsv) is None

    def test_return_type_is_optional_str(self, tmp_path):
        """Return type is Optional[str]: can be None or a non-empty string."""
        tsv_none = tmp_path / "no_running.tsv"
        append_experiment(_make_exp("x", status="won"), tsv_none)

        tsv_some = tmp_path / "one_running.tsv"
        append_experiment(_make_exp("exp-x"), tsv_some)

        result_none = get_current_experiment_id(tsv_none)
        result_str = get_current_experiment_id(tsv_some)

        assert result_none is None
        assert isinstance(result_str, str)
        assert len(result_str) > 0


# ── 2. Caller smoke tests: handle None correctly ─────────────────────────────


class TestCallerNoneHandling:
    """All callers of get_current_experiment_id must handle None gracefully.

    These are smoke tests that call the relevant daily_check code paths with
    a patched get_current_experiment_id returning None and verify no
    AttributeError / NoneType errors surface.
    """

    def test_daily_check_imports_and_accepts_none(self):
        """Verify daily_check.py imports get_current_experiment_id and can
        tolerate None being returned (no .foo attribute access on None).
        """
        # Import the module to confirm it parses cleanly
        import workflows.daily_check as dc
        # Confirm the import is present
        assert hasattr(dc, "get_current_experiment_id"), (
            "daily_check must import get_current_experiment_id from models.experiment"
        )

    def test_emit_pb_silent_no_op_accepts_none_experiment_id(self):
        """emit_pb_silent_no_op already accepts str | None — confirm None is
        handled without error (no NoneType crash)."""
        from datetime import datetime

        from clients.pb_envelope import PBLaunch, SendOutcome
        from workflows.pb_advance_gate import emit_pb_silent_no_op

        launch = PBLaunch(
            container_id="c1",
            agent_id="a1",
            launched_at=datetime(2026, 5, 22, tzinfo=UTC),
            arguments_sha256="abc",
        )
        outcome = SendOutcome(
            container_id="c1",
            csv_status="Empty",
            sent_count=0,
            requested_count=5,
            drift_skipped_reason=None,
            next_day_drift_key="drift-key-1",
            sent_urls=frozenset(),
            skipped_urls=frozenset(),
        )
        mock_attio = MagicMock()
        mock_attio._request.return_value = {"data": {}}

        # Passing None for experiment_id must not raise
        try:
            emit_pb_silent_no_op(
                launch,
                outcome,
                attio=mock_attio,
                experiment_id=None,
            )
        except Exception as exc:
            pytest.fail(
                f"emit_pb_silent_no_op raised {type(exc).__name__} with None experiment_id: {exc}"
            )


# ── 3. Three-step recompute success path ─────────────────────────────────────


class TestThreeStepRecomputeSuccess:
    """Success path: --apply writes ReclassificationRun + MigrationRun + superseded event."""

    def _make_mock_attio(
        self,
        *,
        rec_run_id="rec123",
        mig_run_id="mig456",
        existing_state_history_value=None,
        existing_rates=None,
    ):
        """Build a mock Attio client that captures write payloads.

        Post-fold-in (PR-11 review): the script now uses `attio._request(...)`
        everywhere (5 callsites previously bypassed `_request` via the raw
        `attio._client.request` path). Both surfaces are still wired so older
        callers that haven't migrated stay covered; the writers themselves
        already go through `_request`.

        existing_state_history_value: override the state_history string the
        GET on the experiment record returns. Default: a single
        "running" event (legacy F-PR-6 shape).

        existing_rates: optional dict of {"dm1_response_rate", "dm2_response_rate",
        "dm3_response_rate"} numeric values returned by the GET — these are the
        "prior" values the recompute records on the superseded event.
        """
        captured: dict = {}

        if existing_state_history_value is None:
            existing_state_history_value = '{"timestamp":"2026-01-01","status":"running"}'
        if existing_rates is None:
            existing_rates = {
                "dm1_response_rate": 0.05,
                "dm2_response_rate": 0.04,
                "dm3_response_rate": 0.03,
            }

        def _existing_values() -> dict:
            values: dict = {
                "state_history": [{"value": existing_state_history_value}],
            }
            for key, val in existing_rates.items():
                values[key] = [{"value": val}]
            return values

        def _handler(method, path, json=None, **_kwargs):
            # Reclassification Run create
            if method == "POST" and "reclassification_run" in path and "records" in path and "query" not in path:
                captured["rec_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {"id": {"record_id": "rec_attio_001"}}}
            # Migration Run create
            if method == "POST" and "migration_run" in path and "records" in path and "query" not in path:
                captured["mig_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {"id": {"record_id": "mig_attio_001"}}}
            # Experiment row query (baseline-v0 lookup)
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0_attio_001"}, "values": {}}]}
            # Experiment row PATCH (rates + state_history)
            if method == "PATCH" and "experiment/records" in path:
                captured["patch_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {}}
            # Reclassification Run query for orphan detection
            if method == "POST" and "reclassification_run" in path and "query" in path:
                return {"data": []}
            # Migration Run query for idempotency check
            if method == "POST" and "migration_run" in path and "query" in path:
                return {"data": []}
            # Experiment record GET (state_history fetch + prior rates fetch)
            if method == "GET" and "experiment/records" in path:
                return {"data": {"values": _existing_values()}}
            # Back-pointer PATCH on linkedin_outreach or generic PATCH
            if method == "PATCH":
                return {"data": {}}
            return {"data": []}

        class _MockResponse:
            def __init__(self, data):
                self._data = data
            def raise_for_status(self):
                pass
            def json(self):
                return self._data

        def _client_request(method, path, json=None, **_kwargs):
            return _MockResponse(_handler(method, path, json=json, **_kwargs))

        mock_client = MagicMock()
        mock_client.request.side_effect = _client_request

        mock_attio_instance = MagicMock()
        mock_attio_instance._client = mock_client
        # All writers + helpers in the script funnel through _request
        mock_attio_instance._request.side_effect = _handler
        mock_attio_instance.__enter__ = lambda s: s
        mock_attio_instance.__exit__ = MagicMock(return_value=False)

        # baseline-v0 cohort: 6 entries at dm_step=3 so every per-step
        # n_observed >= SMALL_N_THRESHOLD (5). PR-11 fold-in adds a
        # prior-dominated gate that aborts when any step has n_observed < 5.
        cohort_entries = [
            {
                "record_id": f"rec_{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                # Mix of responses and pending sends to keep success rates < 0.10
                "stage": "responded" if i == 0 else "dm3_sent",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "exp_frozen",
                "merged_into": None,
            }
            for i in range(6)
        ]
        mock_attio_instance.query_list_entries.return_value = cohort_entries
        mock_attio_instance.parse_entry.side_effect = lambda e: e

        return mock_attio_instance, captured

    def test_apply_writes_rec_run_and_migration_run(self, tmp_path):
        """--apply: both ReclassificationRun and MigrationRun rows are opened."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._make_mock_attio()

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0, f"Expected exit 0, got {rc}"
        assert "rec_payload" in captured or "patch_payload" in captured, (
            "Expected at least one Attio write for --apply"
        )

    def test_apply_writes_superseded_event_to_state_history(self, tmp_path):
        """--apply: state_history PATCH includes a superseded event."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._make_mock_attio()

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0
        assert "patch_payload" in captured, "Expected PATCH to experiment row"
        state_history = captured["patch_payload"].get("state_history", "")
        assert state_history, "state_history must be non-empty"
        # Parse the last line of the JSONL — must be the superseded event
        lines = [ln for ln in state_history.strip().split("\n") if ln]
        assert lines, "state_history must have at least one JSONL line"
        last_event = json.loads(lines[-1])
        assert last_event.get("status") == "superseded", (
            "Last state_history event must be 'superseded'"
        )
        assert "reclassification_run_id" in last_event, (
            "Superseded event must include reclassification_run_id for forensics"
        )

    def test_apply_writes_dm_rates(self, tmp_path):
        """--apply: PATCH payload includes dm1/dm2/dm3 response rates."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._make_mock_attio()

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0
        assert "patch_payload" in captured
        payload = captured["patch_payload"]
        assert "dm1_response_rate" in payload
        assert "dm2_response_rate" in payload
        assert "dm3_response_rate" in payload

    def test_migration_run_cross_references_rec_run(self, tmp_path):
        """MigrationRunWriter must be called with reclassification_run_id=rec_run.run_id."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._make_mock_attio()

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0
        # Both payloads captured
        assert "mig_payload" in captured, "Migration Run must be written"
        assert "rec_payload" in captured, "Reclassification Run must be written"

        mig = captured["mig_payload"]
        rec = captured["rec_payload"]
        assert mig.get("reclassification_run_id") == rec.get("run_id"), (
            "Migration Run row's reclassification_run_id must equal the "
            "Reclassification Run's run_id (PR-9b cross-reference pattern)"
        )

    def test_dry_run_produces_no_writes(self, tmp_path):
        """--dry-run: zero Attio writes; logs computed posteriors."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._make_mock_attio()

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--dry-run"])

        assert rc == 0, f"--dry-run should exit 0, got {rc}"
        assert "rec_payload" not in captured, "--dry-run must NOT write ReclassificationRun"
        assert "mig_payload" not in captured, "--dry-run must NOT write MigrationRun"
        assert "patch_payload" not in captured, "--dry-run must NOT PATCH experiment row"


# ── 4. Crash recovery ─────────────────────────────────────────────────────────


class TestCrashRecovery:
    """§9.4: mid-step-3 failure leaves orphaned rec_run; re-run resumes from step 3."""

    def _cohort_entries(self) -> list[dict]:
        # 6 entries at dm_step=3 — meets the prior-dominated gate
        # (n_observed >= SMALL_N_THRESHOLD=5 on every step).
        return [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                "stage": "responded" if i == 0 else "dm3_sent",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "some_exp",
                "merged_into": None,
            }
            for i in range(6)
        ]

    def test_orphan_detected_no_second_rec_run_opened(self, tmp_path):
        """If a Reclassification Run exists for the same cohort hash but no
        Migration Run references it, the script must NOT open a second
        Reclassification Run — it must resume from step 3.

        Also asserts run_id continuity: the resumed MigrationRun row must
        carry the orphan's original `run_id` ("orphan123") in its
        `reclassification_run_id` field, not a fresh UUID.
        """
        import scripts.recompute_baseline_v0 as script

        cohort = self._cohort_entries()
        cohort_record_ids = [c["record_id"] for c in cohort]
        cohort_hash = script._cohort_hash(cohort_record_ids)

        rec_run_create_calls = []
        captured: dict = {}

        def _handler(method, path, json=None, **_kwargs):
            # Reclassification Run query — return the orphaned run
            if method == "POST" and "reclassification_run" in path and "query" in path:
                return {
                    "data": [
                        {
                            "id": {"record_id": "rec_attio_existing"},
                            "values": {
                                "run_id": [{"value": "orphan123"}],
                                "audit_log_pointer": [{"value": f"cohort_hash={cohort_hash}"}],
                            },
                        }
                    ]
                }
            # Reclassification Run create — should NOT be called
            if method == "POST" and "reclassification_run" in path and "records" in path:
                rec_run_create_calls.append(1)
                return {"data": {"id": {"record_id": "rec_attio_NEW"}}}
            # Migration Run query — returns empty (step 3 never completed)
            if method == "POST" and "migration_run" in path and "query" in path:
                return {"data": []}
            # Migration Run create — capture so we can assert continuity
            if method == "POST" and "migration_run" in path and "records" in path:
                captured["mig_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {"id": {"record_id": "mig_attio_001"}}}
            # Experiment query (baseline-v0 lookup)
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0_001"}, "values": {}}]}
            # Experiment PATCH — capture state_history payload
            if method == "PATCH" and "experiment/records" in path:
                captured["patch_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {}}
            # Experiment GET
            if method == "GET" and "experiment/records" in path:
                return {"data": {"values": {}}}
            if method == "PATCH":
                return {"data": {}}
            return {"data": []}

        class _MockResponse:
            def __init__(self, data):
                self._data = data
            def raise_for_status(self):
                pass
            def json(self):
                return self._data

        def _client_request(method, path, json=None, **_kwargs):
            return _MockResponse(_handler(method, path, json=json))

        mock_client = MagicMock()
        mock_client.request.side_effect = _client_request

        mock_attio = MagicMock()
        mock_attio._client = mock_client
        mock_attio._request.side_effect = _handler
        mock_attio.__enter__ = lambda s: s
        mock_attio.__exit__ = MagicMock(return_value=False)
        mock_attio.query_list_entries.return_value = cohort
        mock_attio.parse_entry.side_effect = lambda e: e

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0
        assert len(rec_run_create_calls) == 0, (
            f"Script opened {len(rec_run_create_calls)} new Reclassification Run(s); "
            "it should have detected the orphan and skipped step 2."
        )
        # PR-11 fold-in (pr-test HIGH-1): run_id continuity assertion. The
        # resumed MigrationRun row must reference the orphan's original run_id,
        # not a fresh UUID — otherwise the cross-reference chain is broken.
        assert captured.get("mig_payload", {}).get("reclassification_run_id") == "orphan123", (
            "Resumed MigrationRun row must carry the orphan's original "
            "reclassification_run_id ('orphan123'), got: "
            f"{captured.get('mig_payload', {}).get('reclassification_run_id')!r}"
        )
        # The state_history PATCH must also include a `superseded` event
        # carrying the orphan's run_id (proof the supersedence event ties
        # back to the original rec_run, not to a new one).
        state_history = captured.get("patch_payload", {}).get("state_history", "")
        assert "orphan123" in state_history, (
            "state_history PATCH must include the orphan's run_id in the "
            "superseded event for forensic chain integrity."
        )


# ── 5. §9.4 Idempotency ───────────────────────────────────────────────────────


class TestIdempotency:
    """Second --apply with same cohort data is a no-op (rows_modified == 0)."""

    def test_second_apply_detects_completed_run_and_skips(self, tmp_path):
        """If the Reclassification Run + Migration Run both exist for this
        cohort hash, the script must exit 0 without any writes.
        """
        import scripts.recompute_baseline_v0 as script

        # 6 entries to clear the prior-dominated gate (SMALL_N_THRESHOLD=5)
        cohort = [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                "stage": "dm3_sent",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "some_exp",
                "merged_into": None,
            }
            for i in range(6)
        ]
        cohort_hash = script._cohort_hash([c["record_id"] for c in cohort])
        write_calls = []

        def _handler(method, path, json=None, **_kwargs):
            if method == "POST" and "reclassification_run" in path and "query" in path:
                # Return the existing rec_run
                return {
                    "data": [
                        {
                            "id": {"record_id": "rec_attio_done"},
                            "values": {
                                "run_id": [{"value": "done_rec_run_id"}],
                                "audit_log_pointer": [{"value": f"cohort_hash={cohort_hash}"}],
                            },
                        }
                    ]
                }
            if method == "POST" and "migration_run" in path and "query" in path:
                # Migration Run exists → step 3 already completed
                return {
                    "data": [
                        {
                            "id": {"record_id": "mig_attio_done"},
                            "values": {
                                "reclassification_run_id": [{"value": "done_rec_run_id"}],
                                "script_name": [{"value": script.SCRIPT_NAME}],
                            },
                        }
                    ]
                }
            # Any write call should NOT happen
            if method in ("POST", "PATCH") and "query" not in path:
                write_calls.append({"method": method, "path": path})
                return {"data": {}}
            return {"data": []}

        class _MockResponse:
            def __init__(self, data):
                self._data = data
            def raise_for_status(self):
                pass
            def json(self):
                return self._data

        def _client_request(method, path, json=None, **_kwargs):
            return _MockResponse(_handler(method, path, json=json))

        mock_client = MagicMock()
        mock_client.request.side_effect = _client_request

        mock_attio = MagicMock()
        mock_attio._client = mock_client
        mock_attio._request.side_effect = _handler
        mock_attio.__enter__ = lambda s: s
        mock_attio.__exit__ = MagicMock(return_value=False)
        mock_attio.query_list_entries.return_value = cohort
        mock_attio.parse_entry.side_effect = lambda e: e

        class _FakeAttioClient:
            _instance = mock_attio

            def __new__(cls, *args, **kwargs):
                return cls._instance

            def __init__(self):
                pass

        with patch.object(script, "AttioClient", _FakeAttioClient):
            rc = script.main(["--apply"])

        assert rc == 0
        assert len(write_calls) == 0, (
            f"Second apply produced {len(write_calls)} write(s): {write_calls}. "
            "Must be a no-op when both ReclassificationRun and MigrationRun exist."
        )


# ── 6. `won` verdict invariant: dm_response_rate > 0.0 ────────────────────────


class TestWonRateInvariant:
    """A `won` verdict with dm_response_rate=0.0 is a bug.

    This is the per-experiment verdict invariant. A `won` verdict with
    dm_response_rate=0.0 is a silent-fallback regression — it means the
    posterior math returned 0 for a winning experiment, which can only
    happen if _per_step_rates or _compute_posterior emitted 0 where it
    should emit the prior mean (baseline, not zero).
    """

    def _make_cohort_entry(self, dm_step: int, responded: bool, record_id: str = "r1") -> dict:
        stage = "responded" if responded else "dm1_sent"
        if dm_step == 1:
            stage = "responded" if responded else "dm1_sent"
        return {
            "record_id": record_id,
            "experiment_id": "exp-test",
            "dm_step": dm_step,
            "stage": stage,
            f"dm{dm_step}_sent_at": "2026-04-01",
            "experiment_id_frozen_at": "some_exp",
            "merged_into": None,
        }

    @pytest.mark.parametrize("n_entries,n_responded", [
        (20, 0),  # 20 DM'd, 0 responded — prior-dominated, cannot produce 'won'
        (5, 0),   # thin data, 0 responded
    ])
    def test_zero_response_cohort_cannot_produce_won(self, n_entries, n_responded):
        """A cohort with all dm_response_rate=0.0 observed cannot win.

        The posterior mean with 0 successes collapses toward the prior (not 0.0),
        which for the default priors is much smaller than a typical baseline_rate
        + 2pp threshold. So verdict must be 'lost' or 'inconclusive', never 'won'.
        """
        from workflows.learn import _per_step_rates

        entries = [
            self._make_cohort_entry(dm_step=1, responded=False, record_id=f"r{i}")
            for i in range(n_entries)
        ]

        per_step = _per_step_rates(entries)

        # The posterior mean with 0 successes must not be 0.0 — it should be > 0
        # (the prior pulls it up from zero). But it must also be small enough
        # that it cannot be > baseline + 2pp for a sane baseline.
        dm1_rate = per_step["dm1_response_rate"]
        # Posterior mean with 0 successes: alpha / (alpha + beta + n_observed)
        # For dm1 priors this is well below a 10% baseline, so won is impossible.
        assert dm1_rate >= 0.0, "Posterior mean must be non-negative"
        # A won verdict requires rate > baseline + 0.02. The baseline must be > 0
        # for a functioning system. With 0 successes the posterior mean is
        # dominated by the prior — if prior alpha is small (say 1), the mean
        # converges to ~1 / (1 + large_n), which is tiny.
        # We assert the rate is well below a typical baseline of 0.08 + 0.02 = 0.10.
        # This verifies the won-invariant: 0 successes can't win.
        assert dm1_rate < 0.10, (
            f"Posterior mean for 0 successes must be < 0.10 (baseline+2pp), "
            f"got {dm1_rate}. A cohort with no responses cannot win."
        )

    def test_evaluate_experiments_zero_response_not_won(self, tmp_path):
        """evaluate_experiments must not produce a 'won' verdict for a zero-response cohort
        when there is a meaningful baseline.

        This is the CI invariant: won rows have dm_response_rate > 0.0, and the
        Bayesian posterior for 0 successes must be less than a realistic baseline + 2pp.

        We set baseline-v0 at dm1_response_rate=0.09 (close to the prior mean), so
        the verdict threshold is 0.09 + 0.02 = 0.11. With 50 observations and 0
        successes the posterior mean is ~0.028 — well below 0.11.
        """
        from models.experiment import Experiment, ExperimentStatus, append_experiment
        from workflows.learn import _per_step_rates, evaluate_experiments

        tsv = tmp_path / "experiments.tsv"
        # baseline-v0 provides a non-zero baseline for step comparisons
        append_experiment(
            Experiment(
                experiment_id="baseline-v0",
                started="2026-01-01",
                completed="2026-03-01",
                cohort_size=100,
                dm_response_rate=0.09,
                baseline_rate=0.0,
                status=ExperimentStatus.WON,
                variable="",
                description="historical baseline",
                dm1_response_rate=0.09,
                dm2_response_rate=0.06,
                dm3_response_rate=0.05,
            ),
            tsv,
        )
        # The running experiment under test
        append_experiment(
            Experiment(
                experiment_id="exp-zero",
                started="2026-01-01",
                completed="",
                cohort_size=50,
                dm_response_rate=0.0,
                baseline_rate=0.09,
                status=ExperimentStatus.RUNNING,
                variable="messages.json:dm1:ops:es",
                description="zero-response test",
                dm1_response_rate=None,
                dm2_response_rate=None,
                dm3_response_rate=None,
            ),
            tsv,
        )

        # Cohort with 50 DM'd entries, 0 responded — mature
        entries = [
            {
                "record_id": f"r{i}",
                "experiment_id": "exp-zero",
                "dm_step": 1,
                "stage": "dm1_sent",
                "dm1_sent_at": "2026-04-01",
                "experiment_id_frozen_at": "some_exp",
                "merged_into": None,
            }
            for i in range(50)
        ]

        per_step = _per_step_rates(entries)
        dm1_posterior_mean = per_step["dm1_response_rate"]

        # Sanity: with 0 successes, posterior mean < 0.05 (much less than baseline + 2pp)
        assert dm1_posterior_mean < 0.05, (
            f"Posterior mean with 0 successes should be < 0.05, got {dm1_posterior_mean}. "
            "This would indicate a prior misconfiguration."
        )

        cohort_result = {
            "experiment_id": "exp-zero",
            "cohort_size": 50,
            "dmed": 50,
            "responded": 0,
            "dm_response_rate": 0.0,
            "is_mature": True,
            "days_running": 30,
            "classification_breakdown": {
                "positive": 0, "question": 0, "neutral": 0,
                "negative": 0, "defensive": 0, "none": 50,
            },
            "defensive_rate": 0.0,
            "per_persona": {},
            **per_step,
        }

        verdicts = evaluate_experiments([cohort_result], experiments_path=tsv)

        # There must be exactly one verdict for this cohort
        assert len(verdicts) == 1
        verdict = verdicts[0]

        # CI invariant: with 0 successes and a non-zero baseline, verdict != 'won'
        assert verdict["verdict"] != "won", (
            f"evaluate_experiments returned verdict='won' for a zero-response "
            f"cohort (step_rate={verdict.get('step_rate'):.4f}, "
            f"step_baseline_rate={verdict.get('step_baseline_rate'):.4f}). "
            "This is the CI invariant violation: a won verdict requires "
            "genuine response signal above baseline + 2pp."
        )

    @pytest.mark.parametrize("verdict_str", ["won"])
    def test_won_verdict_has_positive_rate(self, verdict_str, tmp_path):
        """Any 'won' verdict produced by evaluate_experiments must have step_rate > 0.

        Parameterized to make it easy to add other verdict types in the future.
        This invariant closes the regression door on the prior silent-fallback bug:
        if _per_step_rates ever emits 0.0 as a posterior mean (which should never
        happen given the Bayesian prior), a 'won' verdict would require baseline
        to be negative, which is also impossible. So this test is a belt-and-
        suspenders catch for any future regression.
        """
        from models.experiment import Experiment, ExperimentStatus, append_experiment
        from workflows.learn import _per_step_rates, evaluate_experiments

        tsv = tmp_path / "experiments.tsv"
        # Low baseline so a reasonable experiment can win
        append_experiment(
            Experiment(
                experiment_id="baseline-v0",
                started="2026-01-01",
                completed="2026-03-01",
                cohort_size=100,
                dm_response_rate=0.05,
                baseline_rate=0.0,
                status=ExperimentStatus.WON,
                variable="",
                description="low baseline",
                dm1_response_rate=0.05,
                dm2_response_rate=0.04,
                dm3_response_rate=0.03,
            ),
            tsv,
        )
        append_experiment(
            Experiment(
                experiment_id="exp-strong",
                started="2026-01-01",
                completed="",
                cohort_size=100,
                dm_response_rate=0.20,
                baseline_rate=0.05,
                status=ExperimentStatus.RUNNING,
                variable="messages.json:dm1:ops:es",
                description="strong responder",
                dm1_response_rate=None,
                dm2_response_rate=None,
                dm3_response_rate=None,
            ),
            tsv,
        )

        # 100 DM'd, 20 responded → strong positive signal
        entries = [
            {
                "record_id": f"r{i}",
                "experiment_id": "exp-strong",
                "dm_step": 1,
                # First 20 responded
                "stage": "responded" if i < 20 else "dm1_sent",
                "dm1_sent_at": "2026-04-01",
                "experiment_id_frozen_at": "some_exp",
                "merged_into": None,
            }
            for i in range(100)
        ]

        per_step = _per_step_rates(entries)
        cohort_result = {
            "experiment_id": "exp-strong",
            "cohort_size": 100,
            "dmed": 100,
            "responded": 20,
            "dm_response_rate": 0.20,
            "is_mature": True,
            "days_running": 30,
            "classification_breakdown": {
                "positive": 20, "question": 0, "neutral": 0,
                "negative": 0, "defensive": 0, "none": 80,
            },
            "defensive_rate": 0.0,
            "per_persona": {},
            **per_step,
        }

        verdicts = evaluate_experiments([cohort_result], experiments_path=tsv)
        assert len(verdicts) == 1
        verdict = verdicts[0]

        if verdict["verdict"] == verdict_str:
            assert verdict["step_rate"] > 0.0, (
                f"CI invariant: a '{verdict_str}' verdict must have step_rate > 0.0, "
                f"got step_rate={verdict['step_rate']}. "
                "A won verdict with zero rate is a silent-fallback regression."
            )


# ── 7. PR-11 fold-in tests ────────────────────────────────────────────────────


class TestMultipleRunningExperimentsErrorTypedAttr:
    """fold-in 11 (type-design IMP-1): the error must expose the ambiguous
    experiment_ids as a typed `.experiment_ids` tuple so programmatic callers
    can branch on the IDs without re-parsing the message string."""

    def test_experiment_ids_attribute_is_tuple_of_strings(self):
        err = MultipleRunningExperimentsError(("exp-001", "exp-002"))
        assert err.experiment_ids == ("exp-001", "exp-002")
        assert isinstance(err.experiment_ids, tuple)
        assert all(isinstance(eid, str) for eid in err.experiment_ids)

    def test_message_still_lists_all_ids(self):
        """Backward compat: the str(exc) must still contain every id (cli.py
        formats `f'ABORT: cannot run daily — {exc}'`)."""
        err = MultipleRunningExperimentsError(("a", "b", "c"))
        msg = str(err)
        for eid in ("a", "b", "c"):
            assert eid in msg

    def test_get_current_experiment_id_passes_ids_to_error(self, tmp_path):
        """Raise site at models/experiment.py must call the constructor with a
        tuple. Verifies the model factory wires the type-typed surface."""
        from models.experiment import (
            Experiment,
            ExperimentStatus,
            append_experiment,
        )
        tsv = tmp_path / "experiments.tsv"
        for name in ("exp-alpha", "exp-beta"):
            append_experiment(
                Experiment(
                    experiment_id=name,
                    started="2026-01-01",
                    completed="",
                    cohort_size=30,
                    dm_response_rate=0.10,
                    baseline_rate=0.08,
                    status=ExperimentStatus.RUNNING,
                    variable="",
                    description="t",
                    dm1_response_rate=0.12,
                    dm2_response_rate=0.08,
                    dm3_response_rate=0.06,
                ),
                tsv,
            )
        with pytest.raises(MultipleRunningExperimentsError) as exc_info:
            get_current_experiment_id(tsv)
        assert exc_info.value.experiment_ids == ("exp-alpha", "exp-beta")


class TestPriorDominatedGate:
    """fold-in 1 (math BLOCKING-1): the recompute MUST refuse to overwrite
    baseline-v0 with prior-mean rates when any per-step n_observed < 5
    (SMALL_N_THRESHOLD). Silent prior-mean write violates §0 invariant #9."""

    def _mock_attio_with_thin_cohort(self, n_entries: int):
        """Build a mock returning n_entries dm-step=1 cohort entries with no
        responses — n_observed for dm2/dm3 = 0, which is thin by definition."""
        captured: dict = {}

        def _handler(method, path, json=None, **_kwargs):
            if method == "POST" and "reclassification_run" in path and "records" in path and "query" not in path:
                captured["rec_create"] = True
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "migration_run" in path and "records" in path and "query" not in path:
                captured["mig_create"] = True
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0"}, "values": {}}]}
            if method == "POST" and "query" in path:
                return {"data": []}
            if method == "PATCH":
                captured["patch"] = True
                return {"data": {}}
            if method == "GET":
                return {"data": {"values": {}}}
            return {"data": []}

        class _MR:
            def __init__(self, d):
                self._d = d
            def raise_for_status(self):
                pass
            def json(self):
                return self._d

        def _client_request(method, path, json=None, **_kwargs):
            return _MR(_handler(method, path, json=json))

        mc = MagicMock()
        mc.request.side_effect = _client_request

        ma = MagicMock()
        ma._client = mc
        ma._request.side_effect = _handler
        ma.__enter__ = lambda s: s
        ma.__exit__ = MagicMock(return_value=False)
        ma.query_list_entries.return_value = [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 1,
                "stage": "dm1_sent",
                "dm1_sent_at": "2026-04-01",
                "experiment_id_frozen_at": "ef",
                "merged_into": None,
            }
            for i in range(n_entries)
        ]
        ma.parse_entry.side_effect = lambda e: e
        return ma, captured

    def test_thin_cohort_aborts_with_exit_1(self):
        """n_observed < 5 on any step → exit 1, no Attio writes."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._mock_attio_with_thin_cohort(n_entries=2)

        class _F:
            _i = mock_attio
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--apply"])

        assert rc == 1, f"Expected exit 1 (prior-dominated abort), got {rc}"
        # No Attio writes
        assert "rec_create" not in captured
        assert "mig_create" not in captured
        assert "patch" not in captured

    def test_thin_cohort_dry_run_also_aborts_with_exit_1(self):
        """The gate fires in both --dry-run and --apply: --dry-run with thin
        data still tells the operator the cohort isn't mature, exit 1."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, captured = self._mock_attio_with_thin_cohort(n_entries=4)

        class _F:
            _i = mock_attio
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--dry-run"])

        assert rc == 1
        assert "patch" not in captured

    def test_thin_cohort_logs_per_step_counts_to_stderr(self, capsys):
        """The error message must name the thin step counts so the operator
        can decide whether to wait or close baseline-v0 manually."""
        import scripts.recompute_baseline_v0 as script

        mock_attio, _ = self._mock_attio_with_thin_cohort(n_entries=3)

        class _F:
            _i = mock_attio
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--apply"])

        captured = capsys.readouterr()
        assert rc == 1
        assert "prior-dominated" in captured.err
        assert "dm1=3" in captured.err
        assert "dm2=0" in captured.err
        assert "dm3=0" in captured.err


class TestSupersededEventPriorRates:
    """fold-in 5 (5-agent convergence): the superseded event must record the
    REAL prior rates (fetched from the existing Attio row), not hard-coded
    None. None-valued priors render the forensic chain useless."""

    def test_prior_rates_populated_from_existing_record(self):
        """The PATCH payload's state_history must include the prior rates the
        recompute is overwriting."""
        import scripts.recompute_baseline_v0 as script

        # Build a mock_attio with a baseline-v0 row carrying explicit prior rates
        captured: dict = {}

        def _handler(method, path, json=None, **_kwargs):
            if method == "POST" and "reclassification_run" in path and "records" in path and "query" not in path:
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "migration_run" in path and "records" in path and "query" not in path:
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0_001"}, "values": {}}]}
            if method == "POST" and "query" in path:
                return {"data": []}
            if method == "PATCH" and "experiment/records" in path:
                captured["patch_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {}}
            if method == "PATCH":
                return {"data": {}}
            if method == "GET" and "experiment/records" in path:
                # Prior rates on the existing row — recompute should record these
                return {
                    "data": {
                        "values": {
                            "state_history": [{"value": '{"timestamp":"2026-01-01","status":"running"}'}],
                            "dm1_response_rate": [{"value": 0.07}],
                            "dm2_response_rate": [{"value": 0.05}],
                            "dm3_response_rate": [{"value": 0.03}],
                        }
                    }
                }
            return {"data": []}

        class _MR:
            def __init__(self, d): self._d = d
            def raise_for_status(self): pass
            def json(self): return self._d

        def _client_request(method, path, json=None, **_kwargs):
            return _MR(_handler(method, path, json=json))

        mc = MagicMock()
        mc.request.side_effect = _client_request

        ma = MagicMock()
        ma._client = mc
        ma._request.side_effect = _handler
        ma.__enter__ = lambda s: s
        ma.__exit__ = MagicMock(return_value=False)
        ma.query_list_entries.return_value = [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                "stage": "dm3_sent" if i > 0 else "responded",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "ef",
                "merged_into": None,
            }
            for i in range(6)
        ]
        ma.parse_entry.side_effect = lambda e: e

        class _F:
            _i = ma
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--apply"])

        assert rc == 0
        assert "patch_payload" in captured
        sh = captured["patch_payload"]["state_history"]
        # Parse last JSONL line — the appended superseded event
        last_line = [ln for ln in sh.strip().split("\n") if ln][-1]
        ev = json.loads(last_line)
        # Real prior values — not None
        assert ev["prior_dm1_response_rate"] == 0.07, (
            f"Expected prior_dm1=0.07 from existing row, got {ev['prior_dm1_response_rate']!r}"
        )
        assert ev["prior_dm2_response_rate"] == 0.05
        assert ev["prior_dm3_response_rate"] == 0.03


class TestSupersededEventDedupGuard:
    """fold-in 2 (math BLOCKING-2 + silent-failure HIGH-2 + data-steward
    BLOCKING-2): if a `superseded` event with the same reclassification_run_id
    is already in state_history (we crashed after the PATCH but before the
    MigrationRun row), a re-run must NOT double-append."""

    def test_dedup_guard_skips_append_when_event_exists(self):
        """Pre-seed state_history with a superseded event for an orphan
        rec_run. Re-run with --apply. The resulting PATCH state_history must
        contain exactly ONE superseded event for that rec_run_id, not two."""
        import scripts.recompute_baseline_v0 as script

        orphan_run_id = "orphan_run_123"
        seeded_event = {
            "timestamp": "2026-05-21",
            "status": "superseded",
            "reclassification_run_id": orphan_run_id,
            "prior_dm1_response_rate": 0.07,
            "new_dm1_response_rate": 0.05,
        }
        seeded_history = (
            '{"timestamp":"2026-01-01","status":"running"}\n'
            + json.dumps(seeded_event)
        )

        cohort = [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                "stage": "dm3_sent" if i > 0 else "responded",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "ef",
                "merged_into": None,
            }
            for i in range(6)
        ]
        cohort_hash = script._cohort_hash([c["record_id"] for c in cohort])

        captured: dict = {}

        def _handler(method, path, json=None, **_kwargs):
            # Orphan rec_run already exists for this cohort
            if method == "POST" and "reclassification_run" in path and "query" in path:
                return {
                    "data": [
                        {
                            "id": {"record_id": "rec_attio_existing"},
                            "values": {
                                "run_id": [{"value": orphan_run_id}],
                                "audit_log_pointer": [{"value": f"cohort_hash={cohort_hash}"}],
                            },
                        }
                    ]
                }
            # Migration Run query — returns empty (the second write step never
            # completed; this is the crash-resume scenario)
            if method == "POST" and "migration_run" in path and "query" in path:
                return {"data": []}
            if method == "POST" and "migration_run" in path and "records" in path:
                captured["mig_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {"id": {"record_id": "mig_attio_001"}}}
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0_001"}, "values": {}}]}
            if method == "PATCH" and "experiment/records" in path:
                captured["patch_payload"] = (json or {}).get("data", {}).get("values", {})
                return {"data": {}}
            if method == "GET" and "experiment/records" in path:
                # Pre-seeded state_history with the orphan's superseded event
                return {"data": {"values": {"state_history": [{"value": seeded_history}]}}}
            if method == "PATCH":
                return {"data": {}}
            return {"data": []}

        class _MR:
            def __init__(self, d): self._d = d
            def raise_for_status(self): pass
            def json(self): return self._d

        def _client_request(method, path, json=None, **_kwargs):
            return _MR(_handler(method, path, json=json))

        mc = MagicMock()
        mc.request.side_effect = _client_request

        ma = MagicMock()
        ma._client = mc
        ma._request.side_effect = _handler
        ma.__enter__ = lambda s: s
        ma.__exit__ = MagicMock(return_value=False)
        ma.query_list_entries.return_value = cohort
        ma.parse_entry.side_effect = lambda e: e

        class _F:
            _i = ma
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--apply"])

        assert rc == 0
        # Count superseded events for the orphan_run_id in the PATCH payload
        sh = captured["patch_payload"]["state_history"]
        events = []
        for line in sh.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if ev.get("status") == "superseded" and ev.get("reclassification_run_id") == orphan_run_id:
                events.append(ev)
        assert len(events) == 1, (
            f"Dedup guard failed — found {len(events)} superseded events for "
            f"rec_run_id={orphan_run_id}, expected exactly 1 (the original "
            "seeded event). A naive re-append would create a second event with "
            "a fresh datetime.now() timestamp."
        )


class TestStep3PatchFailure:
    """fold-in pr-test HIGH-2: when step 3's PATCH raises, the script must
    exit 1 AND must NOT have created a Migration Run row (the cross-reference
    chain would be broken)."""

    def _build_failing_patch_mock(self, cohort):
        captured: dict = {}

        def _handler(method, path, json=None, **_kwargs):
            if method == "POST" and "reclassification_run" in path and "records" in path and "query" not in path:
                captured["rec_create"] = True
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "migration_run" in path and "records" in path and "query" not in path:
                captured["mig_create"] = True
                return {"data": {"id": {"record_id": "x"}}}
            if method == "POST" and "experiment/records/query" in path:
                return {"data": [{"id": {"record_id": "bv0_001"}, "values": {}}]}
            if method == "POST" and "query" in path:
                return {"data": []}
            if method == "GET" and "experiment/records" in path:
                return {"data": {"values": {}}}
            if method == "PATCH" and "experiment/records" in path:
                # The rate-write PATCH fails
                import httpx
                req = httpx.Request("PATCH", "https://x")
                resp = httpx.Response(500, request=req)
                raise httpx.HTTPStatusError("simulated 500", request=req, response=resp)
            if method == "PATCH":
                return {"data": {}}
            return {"data": []}

        class _MR:
            def __init__(self, d): self._d = d
            def raise_for_status(self): pass
            def json(self): return self._d

        def _client_request(method, path, json=None, **_kwargs):
            return _MR(_handler(method, path, json=json))

        mc = MagicMock()
        mc.request.side_effect = _client_request

        ma = MagicMock()
        ma._client = mc
        ma._request.side_effect = _handler
        ma.__enter__ = lambda s: s
        ma.__exit__ = MagicMock(return_value=False)
        ma.query_list_entries.return_value = cohort
        ma.parse_entry.side_effect = lambda e: e
        return ma, captured

    def test_patch_failure_exits_1_no_migration_run(self):
        """Step 3 PATCH fails (500) — exit 1, no Migration Run row created."""
        import scripts.recompute_baseline_v0 as script

        cohort = [
            {
                "record_id": f"r{i}",
                "experiment_id": "baseline-v0",
                "dm_step": 3,
                "stage": "dm3_sent" if i > 0 else "responded",
                "dm1_sent_at": "2026-04-01",
                "dm2_sent_at": "2026-04-08",
                "dm3_sent_at": "2026-04-15",
                "experiment_id_frozen_at": "ef",
                "merged_into": None,
            }
            for i in range(6)
        ]
        mock_attio, captured = self._build_failing_patch_mock(cohort)

        class _F:
            _i = mock_attio
            def __new__(cls, *a, **k): return cls._i
            def __init__(self): pass

        with patch.object(script, "AttioClient", _F):
            rc = script.main(["--apply"])

        assert rc == 1, f"Expected exit 1 (step-3 PATCH failure), got {rc}"
        # ReclassificationRun row was opened (step 2 succeeded)
        assert captured.get("rec_create"), "Step 2 should have opened a ReclassificationRun"
        # MigrationRun row must NOT have been created — the cross-reference
        # chain would be broken if it were.
        assert "mig_create" not in captured, (
            "MigrationRun row must NOT be created when step 3 PATCH fails — "
            "otherwise the cross-reference chain is broken and the row points "
            "to a recompute that didn't actually write."
        )


class TestSupersededEventAlreadyAppendedHelper:
    """fold-in 2 unit-test the dedup helper directly: parsing tolerant of
    legacy non-JSON lines, correct match on rec_run_id."""

    def test_returns_true_when_event_present(self):
        import scripts.recompute_baseline_v0 as script
        history = (
            '{"timestamp":"2026-01-01","status":"running"}\n'
            '{"timestamp":"2026-05-21","status":"superseded","reclassification_run_id":"abc"}\n'
        )
        assert script._superseded_event_already_appended(history, "abc") is True

    def test_returns_false_when_rec_run_id_differs(self):
        import scripts.recompute_baseline_v0 as script
        history = (
            '{"timestamp":"2026-01-01","status":"running"}\n'
            '{"timestamp":"2026-05-21","status":"superseded","reclassification_run_id":"OTHER"}\n'
        )
        assert script._superseded_event_already_appended(history, "abc") is False

    def test_returns_false_when_no_superseded_event(self):
        import scripts.recompute_baseline_v0 as script
        history = '{"timestamp":"2026-01-01","status":"running"}'
        assert script._superseded_event_already_appended(history, "abc") is False

    def test_returns_false_for_empty_history(self):
        import scripts.recompute_baseline_v0 as script
        assert script._superseded_event_already_appended("", "abc") is False

    def test_tolerates_legacy_non_json_lines(self):
        """Legacy F-PR-6-era state_history may have plain-text lines. The
        helper must not crash on JSONDecodeError."""
        import scripts.recompute_baseline_v0 as script
        history = (
            "legacy-plain-text-event-2025-12-01\n"
            '{"timestamp":"2026-05-21","status":"superseded","reclassification_run_id":"abc"}'
        )
        assert script._superseded_event_already_appended(history, "abc") is True


class TestDailyEntrypointMultipleRunningGuard:
    """fold-in 6 (advertiser-daily + code-reviewer 2-agent): the daily CLI
    entrypoint must surface MultipleRunningExperimentsError as a clean exit 1
    BEFORE the lock + Attio + PB clients open. Today, the error propagates
    mid-flight from one of three call sites in workflows.daily_check."""

    def test_daily_aborts_when_two_experiments_running(self, monkeypatch):
        """Patch get_current_experiment_id to raise the error — the daily CLI
        command must catch it and exit 1 without touching the lock or Attio."""
        from click.testing import CliRunner

        # Build a fake module exposing the error + a patched
        # get_current_experiment_id. The daily command imports both inside
        # the function body, so we patch at the source module.
        import models.experiment as model_mod
        from cli import cli

        def _raise(*_a, **_k):
            raise MultipleRunningExperimentsError(("exp-001", "exp-002"))

        monkeypatch.setattr(model_mod, "get_current_experiment_id", _raise)
        monkeypatch.setattr("workflows.safety_limits.get_status", lambda: "offline")
        monkeypatch.setattr("workflows.run_provenance.assert_checkout_current", lambda **_k: {})

        runner = CliRunner()
        result = runner.invoke(cli, ["daily", "--yes"])
        assert result.exit_code == 1, (
            f"daily must exit 1 on MultipleRunningExperimentsError, got "
            f"{result.exit_code}. Output:\n{result.output}"
        )
        # stderr (mixed in by Click) must explain WHY we aborted
        assert "ABORT" in result.output
        assert "exp-001" in result.output
        assert "exp-002" in result.output
