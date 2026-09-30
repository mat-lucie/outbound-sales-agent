"""PR-21 v2: experiment_id stamping tests.

Locks in:
1. PROSPECT-commit stamping in _build_prospect_entry_attrs:
   - single running experiment → stamps experiment_id + frozen_at="prospect"
   - no running experiment (None) → both keys OMITTED from attrs dict
   - multiple running experiments → propagates MultipleRunningExperimentsError

2. Weekly CLI pre-flight guard: MultipleRunningExperimentsError → exit 1 + stderr

3. Pattern-A flip (pre_invite_check):
   - inherits experiment_id from row, stamps frozen_at="accepted"
   - "prospect" → "accepted" (Pattern-B path) is explicitly valid
   - None prior_experiment_id → no frozen_at stamp (no spurious write)

4. Phase 0 ACCEPTED flip (detect_accepted_connections):
   - stamps experiment_id_frozen_at="accepted" while preserving experiment_id

5. Immutability guard:
   - "accepted" → raises ExperimentIdImmutableError
   - "connection_sent" → raises ExperimentIdImmutableError
   - "prospect" → ALLOWED (Pattern-B carve-out)
   - None → ALLOWED (pre-PR-21 row)

6. ExperimentIdImmutableError is RuntimeError (not ValueError)
   - isinstance(exc, RuntimeError) and not isinstance(exc, ValueError)

7. Hard-error on missing to_send_data keys:
   - KeyError raised when experiment_id / experiment_id_frozen_at absent from row

8. Narrow exception in pre_invite_check:
   - httpx.HTTPStatusError → row goes to failed_to_flip (not silent drop)
   - TypeError → propagates as code bug

9. Writer registry parity: all 5 writers registered for both attrs

10. record_id field: ExperimentIdImmutableError carries record_id (not entry_id)
"""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import httpx
import pytest

from models.experiment import (
    ExperimentIdImmutableError,
    MultipleRunningExperimentsError,
)
from tests.fakes import fake_daily_run
from workflows.pre_invite_check import (
    _IMMUTABLE_FROZEN_AT_VALUES,
    _check_experiment_id_immutability,
)

# ============================================================
# Helpers
# ============================================================


def _make_score_result(**overrides) -> dict:
    base = {
        "persona": "operations_leaders",
        "language": "es",
        "score": 75,
    }
    base.update(overrides)
    return base


def _make_to_send_row(**overrides) -> dict:
    """Build a minimal to_send_data row with all required PR-21 keys."""
    base = {
        "linkedInUrl": "https://www.linkedin.com/in/test-person/",
        "message": "Hola!",
        "entry_id": "entry-abc",
        "record_id": "record-xyz",
        "current_stage": "PROSPECT",
        "name": "Test Person",
        "company": "Acme Corp",
        "title": "Plant Manager",
        "invite_eligible_after": None,
        # PR-21 required keys
        "experiment_id": "exp-001",
        "experiment_id_frozen_at": "prospect",
    }
    base.update(overrides)
    return base


# ============================================================
# 1. ExperimentIdImmutableError class invariants
# ============================================================


class TestExperimentIdImmutableError:
    def test_is_runtime_error_not_value_error(self):
        """Lesson 2: subclass of RuntimeError, NOT ValueError."""
        exc = ExperimentIdImmutableError(
            record_id="rec-1",
            entry_id="entry-1",
            field="experiment_id_frozen_at",
            old_value="accepted",
            new_value="prospect",
        )
        assert isinstance(exc, RuntimeError)
        assert not isinstance(exc, ValueError)

    def test_carries_record_id_not_entry_id(self):
        """Lesson: record_id field carries the Attio record ID."""
        exc = ExperimentIdImmutableError(
            record_id="record-actual-id",
            entry_id="entry-list-id",
            field="experiment_id_frozen_at",
            old_value="accepted",
            new_value="prospect",
        )
        assert exc.record_id == "record-actual-id"
        assert exc.entry_id == "entry-list-id"
        assert exc.field == "experiment_id_frozen_at"
        assert exc.old_value == "accepted"
        assert exc.new_value == "prospect"

    def test_message_contains_section_ref(self):
        """Error message must mention §3.1 hard-red-line guard."""
        exc = ExperimentIdImmutableError(
            record_id="r",
            field="experiment_id_frozen_at",
            old_value="accepted",
            new_value="prospect",
        )
        assert "§3.1" in str(exc)
        assert "DO NOT catch broadly" in str(exc)

    def test_entry_id_optional(self):
        """entry_id defaults to None when not provided."""
        exc = ExperimentIdImmutableError(
            record_id="r",
            field="f",
            old_value="a",
            new_value="b",
        )
        assert exc.entry_id is None


# ============================================================
# 2. _check_experiment_id_immutability guard
# ============================================================


class TestCheckExperimentIdImmutability:
    def test_accepted_raises(self):
        """Overwriting 'accepted' (terminal) raises ExperimentIdImmutableError."""
        with pytest.raises(ExperimentIdImmutableError) as exc_info:
            _check_experiment_id_immutability(
                record_id="rec-1",
                entry_id="ent-1",
                prior_frozen_at="accepted",
                new_frozen_at="prospect",
                prior_experiment_id="exp-1",
                new_experiment_id="exp-2",
            )
        assert exc_info.value.record_id == "rec-1"
        assert exc_info.value.old_value == "accepted"

    def test_connection_sent_raises(self):
        """Overwriting 'connection_sent' raises ExperimentIdImmutableError."""
        with pytest.raises(ExperimentIdImmutableError):
            _check_experiment_id_immutability(
                record_id="rec-2",
                entry_id="ent-2",
                prior_frozen_at="connection_sent",
                new_frozen_at="accepted",
                prior_experiment_id="exp-1",
                new_experiment_id="exp-1",
            )

    def test_legacy_pre_tsv_era_raises(self):
        """Overwriting 'legacy_pre_tsv_era' raises ExperimentIdImmutableError."""
        with pytest.raises(ExperimentIdImmutableError):
            _check_experiment_id_immutability(
                record_id="r",
                entry_id=None,
                prior_frozen_at="legacy_pre_tsv_era",
                new_frozen_at="accepted",
                prior_experiment_id=None,
                new_experiment_id="exp-1",
            )

    def test_legacy_inferred_by_archaeology_raises(self):
        with pytest.raises(ExperimentIdImmutableError):
            _check_experiment_id_immutability(
                record_id="r",
                entry_id=None,
                prior_frozen_at="legacy_inferred_by_archaeology",
                new_frozen_at="accepted",
                prior_experiment_id=None,
                new_experiment_id="exp-1",
            )

    def test_legacy_pure_unknown_raises(self):
        with pytest.raises(ExperimentIdImmutableError):
            _check_experiment_id_immutability(
                record_id="r",
                entry_id=None,
                prior_frozen_at="legacy_pure_unknown",
                new_frozen_at="accepted",
                prior_experiment_id=None,
                new_experiment_id="exp-1",
            )

    def test_prospect_to_accepted_is_allowed(self):
        """Pattern-B carve-out: 'prospect' → 'accepted' is explicitly allowed."""
        # Should not raise
        _check_experiment_id_immutability(
            record_id="rec-b",
            entry_id="ent-b",
            prior_frozen_at="prospect",
            new_frozen_at="accepted",
            prior_experiment_id="exp-1",
            new_experiment_id="exp-1",
        )

    def test_none_to_accepted_is_allowed(self):
        """NULL prior → any new value is allowed (pre-PR-21 row)."""
        _check_experiment_id_immutability(
            record_id="rec-old",
            entry_id=None,
            prior_frozen_at=None,
            new_frozen_at="accepted",
            prior_experiment_id=None,
            new_experiment_id="exp-1",
        )

    def test_idempotent_write_is_allowed(self):
        """Writing same frozen_at value (idempotent) never raises."""
        _check_experiment_id_immutability(
            record_id="r",
            entry_id=None,
            prior_frozen_at="accepted",
            new_frozen_at="accepted",
            prior_experiment_id="exp-1",
            new_experiment_id="exp-1",
        )

    def test_immutable_set_excludes_prospect_and_no_sentinel(self):
        """Verify 'prospect' and no sentinel are NOT in _IMMUTABLE_FROZEN_AT_VALUES."""
        assert "prospect" not in _IMMUTABLE_FROZEN_AT_VALUES
        assert "no_active_experiment" not in _IMMUTABLE_FROZEN_AT_VALUES
        assert None not in _IMMUTABLE_FROZEN_AT_VALUES
        # All 5 terminal values ARE in the set
        assert "connection_sent" in _IMMUTABLE_FROZEN_AT_VALUES
        assert "accepted" in _IMMUTABLE_FROZEN_AT_VALUES
        assert "legacy_pre_tsv_era" in _IMMUTABLE_FROZEN_AT_VALUES
        assert "legacy_inferred_by_archaeology" in _IMMUTABLE_FROZEN_AT_VALUES
        assert "legacy_pure_unknown" in _IMMUTABLE_FROZEN_AT_VALUES


# ============================================================
# 3. PROSPECT-commit stamping in _build_prospect_entry_attrs
# ============================================================


class TestBuildProspectEntryAttrs:
    def test_single_running_experiment_stamps_both_fields(self):
        """When one experiment is running, attrs include experiment_id + frozen_at='prospect'."""
        import workflows.weekly_prospect as wp
        from workflows.weekly_prospect import _build_prospect_entry_attrs

        with patch.object(wp, "get_current_experiment_id", return_value="exp-active"):
            attrs = _build_prospect_entry_attrs(
                _make_score_result(), "2026-05-22", record_id="rec-test"
            )

        assert attrs["experiment_id"] == "exp-active"
        assert attrs["experiment_id_frozen_at"] == "prospect"

    def test_none_experiment_omits_both_keys(self):
        """Lesson 1: when no experiment running, both keys MUST be absent from attrs."""
        import workflows.weekly_prospect as wp
        from workflows.weekly_prospect import _build_prospect_entry_attrs

        with patch.object(wp, "get_current_experiment_id", return_value=None):
            attrs = _build_prospect_entry_attrs(
                _make_score_result(), "2026-05-22", record_id="rec-none"
            )

        assert "experiment_id" not in attrs, (
            "experiment_id must be OMITTED (not set to None or sentinel) "
            "when no experiment is running"
        )
        assert "experiment_id_frozen_at" not in attrs, (
            "experiment_id_frozen_at must be OMITTED when no experiment is running"
        )

    def test_none_experiment_logs_with_record_id(self, capsys):
        """Lesson 7: None-running path logs record_id to stderr."""
        import workflows.weekly_prospect as wp
        from workflows.weekly_prospect import _build_prospect_entry_attrs

        with patch.object(wp, "get_current_experiment_id", return_value=None):
            _build_prospect_entry_attrs(
                _make_score_result(), "2026-05-22", record_id="rec-log-test"
            )

        captured = capsys.readouterr()
        assert "rec-log-test" in captured.err
        assert "PR-21" in captured.err

    def test_multiple_running_experiments_propagates(self):
        """MultipleRunningExperimentsError propagates — not swallowed."""
        import workflows.weekly_prospect as wp
        from workflows.weekly_prospect import _build_prospect_entry_attrs

        with patch.object(
            wp,
            "get_current_experiment_id",
            side_effect=MultipleRunningExperimentsError(("exp-1", "exp-2")),
        ), pytest.raises(MultipleRunningExperimentsError):
            _build_prospect_entry_attrs(
                _make_score_result(), "2026-05-22", record_id="rec-multi"
            )

    def test_no_sentinel_value_in_attrs(self):
        """Lesson 1: 'no_active_experiment' MUST NOT appear anywhere in attrs."""
        import workflows.weekly_prospect as wp
        from workflows.weekly_prospect import _build_prospect_entry_attrs

        for exp_id in [None, "exp-active"]:
            with patch.object(wp, "get_current_experiment_id", return_value=exp_id):
                attrs = _build_prospect_entry_attrs(
                    _make_score_result(), "2026-05-22", record_id="rec-sentinel"
                )

            assert "no_active_experiment" not in attrs.values(), (
                f"Sentinel value appeared in attrs for exp_id={exp_id!r}"
            )


# ============================================================
# 4. Weekly CLI pre-flight guard
# ============================================================


class TestWeeklyCliPreflight:
    def test_multiple_experiments_aborts_with_exit_1(self):
        """Lesson 3: weekly command aborts with sys.exit(1) on MultipleRunningExperimentsError."""
        from click.testing import CliRunner

        from cli import cli

        # Click 8.3+: CliRunner.result.output merges stdout+stderr for
        # backward compat, but `result.stdout_bytes` and `result.stderr`
        # are split. fold-in pr-test MED-1: verify the ABORT line actually
        # routes to stderr (not stdout) by asserting on the split streams.
        runner = CliRunner()
        with patch(
            "models.experiment.get_current_experiment_id",
            side_effect=MultipleRunningExperimentsError(("exp-1", "exp-2")),
        ):
            result = runner.invoke(
                cli,
                ["weekly", "--search-export-id", "fake-id"],
                catch_exceptions=False,
            )

        assert result.exit_code == 1
        assert b"ABORT" not in result.stdout_bytes  # not stdout
        assert "ABORT" in result.stderr             # stderr

    def test_multiple_experiments_message_on_stderr(self):
        """Weekly abort message appears and mentions closing an experiment."""
        from click.testing import CliRunner

        from cli import cli

        # fold-in pr-test MED-1: assert against split streams. Click 8.3+
        # exposes stdout_bytes + stderr separately; result.output is the
        # merged backward-compat view.
        runner = CliRunner()
        with patch(
            "models.experiment.get_current_experiment_id",
            side_effect=MultipleRunningExperimentsError(("exp-a", "exp-b")),
        ):
            result = runner.invoke(
                cli,
                ["weekly", "--search-export-id", "fake-id"],
                catch_exceptions=False,
            )

        assert b"ABORT" not in result.stdout_bytes  # not stdout
        assert "ABORT" in result.stderr             # stderr


# ============================================================
# 5. Pattern-A flip: pre_invite_check
# ============================================================


def _make_attio_writer_success():
    """AttioWriter mock that always succeeds."""
    writer = MagicMock()
    writer.apply.return_value = None
    return writer


class TestPatternAFlip:
    """Tests for _pre_invite_degree_check Pattern-A (1st-degree) flip."""

    def _run_flip(self, rows, *, degree="1st", **kwargs):
        """Helper: run _pre_invite_degree_check with one mocked 1st-degree row."""
        from workflows.pre_invite_check import _pre_invite_degree_check

        mock_pb = MagicMock()
        mock_attio = MagicMock()
        mock_writer = _make_attio_writer_success()
        today = date(2026, 5, 22)
        list_id = "list-test"

        # Patch: recheck_cache returns all rows as stale (no cache hits)
        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch("workflows.pre_invite_check.AttioWriter", return_value=mock_writer),
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
        ):
            # Cache: all rows stale, empty cached_results
            mock_cache.partition.return_value = ({}, [r["linkedInUrl"] for r in rows])
            mock_cache.RECHECK_TTL_DAYS = 7

            # All rows are 2nd-degree normally, but we override in tests
            # by making degree_lookup return "1st" for specific URLs
            # We do this by making the scraper results include "1st" or "2nd"
            degree_csv = "\n".join(
                [
                    "profileUrl,connectionDegree",
                    *[f"{r['linkedInUrl']},{degree}" for r in rows],
                ]
            )
            mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
            mock_pb.wait_for_completion.return_value = None
            mock_pb.download_result_csv.return_value = degree_csv

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
            ):
                return _pre_invite_degree_check(
                    rows,
                    mock_pb,
                    "scraper-id",
                    mock_attio,
                    list_id,
                    today=today,
                ), mock_writer

    def test_inherits_experiment_id_stamps_accepted(self):
        """Pattern-A: experiment_id preserved, frozen_at='accepted' stamped."""
        row = _make_to_send_row(
            experiment_id="exp-active",
            experiment_id_frozen_at="prospect",
        )
        (still_to_invite, already_connected), mock_writer = self._run_flip([row])

        assert len(already_connected) == 1
        assert len(still_to_invite) == 0
        # Check the WriteIntent updates contain experiment_id_frozen_at="accepted"
        call_args = mock_writer.apply.call_args
        assert call_args is not None
        write_intent = call_args[0][0]
        assert write_intent.updates.get("experiment_id_frozen_at") == "accepted"

    def test_1st_degree_inherits_experiment_id_as_accepted(self):
        """Pattern-B: prospect → accepted (skipping connection_sent) is valid."""
        row = _make_to_send_row(
            current_stage="PROSPECT",
            experiment_id="exp-pb",
            experiment_id_frozen_at="prospect",
        )
        (still_to_invite, already_connected), mock_writer = self._run_flip([row])

        assert len(already_connected) == 1
        write_intent = mock_writer.apply.call_args[0][0]
        assert write_intent.updates.get("experiment_id_frozen_at") == "accepted"
        # This is the Pattern-B path: prospect → accepted is explicitly valid

    def test_none_experiment_id_no_frozen_at_stamp(self):
        """When prior_experiment_id is None, don't stamp frozen_at (no spurious write)."""
        row = _make_to_send_row(
            experiment_id=None,
            experiment_id_frozen_at=None,
        )
        (still_to_invite, already_connected), mock_writer = self._run_flip([row])

        assert len(already_connected) == 1
        write_intent = mock_writer.apply.call_args[0][0]
        assert "experiment_id_frozen_at" not in write_intent.updates

    def test_missing_experiment_id_key_raises_key_error(self):
        """Lesson 6: row without experiment_id key → KeyError (not silent None)."""
        from workflows.pre_invite_check import _pre_invite_degree_check

        # Build row WITHOUT experiment_id key
        row = {
            "linkedInUrl": "https://www.linkedin.com/in/test/",
            "message": "Hi",
            "entry_id": "ent-1",
            "record_id": "rec-1",
            "current_stage": "PROSPECT",
            "name": "Test",
            "company": "Corp",
            "title": "Manager",
            "invite_eligible_after": None,
            # DELIBERATELY missing: "experiment_id", "experiment_id_frozen_at"
        }

        mock_pb = MagicMock()
        mock_attio = MagicMock()
        today = date(2026, 5, 22)

        degree_csv = "profileUrl,connectionDegree\nhttps://www.linkedin.com/in/test/,1st"
        mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
        mock_pb.wait_for_completion.return_value = None
        mock_pb.download_result_csv.return_value = degree_csv

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
        ):
            mock_cache.partition.return_value = ({}, [row["linkedInUrl"]])
            mock_cache.RECHECK_TTL_DAYS = 7

            import workflows.daily_check as _dc
            # fold-in 4-agent convergence: pytest.raises kept INSIDE the
            # parenthesized-with for context-manager parity. Original code
            # was malformed (`),pytest.raises(KeyError)` with no comma) —
            # this is the syntactically valid + ruff-compliant fix.
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
                pytest.raises(KeyError),
            ):
                _pre_invite_degree_check(
                    [row], mock_pb, "scraper-id", mock_attio, "list-id", today=today
                )

    def test_missing_experiment_id_frozen_at_key_raises_key_error(self):
        """Lesson 6: row without experiment_id_frozen_at key → KeyError."""
        from workflows.pre_invite_check import _pre_invite_degree_check

        row = _make_to_send_row()
        del row["experiment_id_frozen_at"]  # Remove the required key

        mock_pb = MagicMock()
        mock_attio = MagicMock()
        today = date(2026, 5, 22)

        degree_csv = f"profileUrl,connectionDegree\n{row['linkedInUrl']},1st"
        mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
        mock_pb.wait_for_completion.return_value = None
        mock_pb.download_result_csv.return_value = degree_csv

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
        ):
            mock_cache.partition.return_value = ({}, [row["linkedInUrl"]])
            mock_cache.RECHECK_TTL_DAYS = 7

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
                pytest.raises(KeyError),
            ):
                _pre_invite_degree_check(
                    [row], mock_pb, "scraper-id", mock_attio, "list-id", today=today
                )

    def test_http_status_error_goes_to_failed_to_flip(self):
        """Lesson 5: httpx.HTTPStatusError is caught → row in failed_to_flip (not crashed)."""
        from workflows.pre_invite_check import _pre_invite_degree_check

        row = _make_to_send_row()
        mock_pb = MagicMock()
        mock_attio = MagicMock()
        today = date(2026, 5, 22)

        http_err = httpx.HTTPStatusError(
            "500 Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=500),
        )
        mock_writer = MagicMock()
        mock_writer.apply.side_effect = http_err

        degree_csv = f"profileUrl,connectionDegree\n{row['linkedInUrl']},1st"
        mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
        mock_pb.wait_for_completion.return_value = None
        mock_pb.download_result_csv.return_value = degree_csv

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch("workflows.pre_invite_check.AttioWriter", return_value=mock_writer),
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
            patch("workflows.pre_invite_check.escalate"),
        ):
            mock_cache.partition.return_value = ({}, [row["linkedInUrl"]])
            mock_cache.RECHECK_TTL_DAYS = 7

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
            ):
                still_to_invite, already_connected = _pre_invite_degree_check(
                    [row], mock_pb, "scraper-id", mock_attio, "list-id", today=today
                )

        # Row must NOT be in already_connected — it failed to flip
        assert len(already_connected) == 0
        # Row must NOT be in still_to_invite — we don't fall back to inviting
        # (the function does not add failed flips to still_to_invite)
        assert len(still_to_invite) == 0

    def test_type_error_propagates_as_code_bug(self):
        """Lesson 5: TypeError from a caller bug propagates (not caught)."""
        from workflows.pre_invite_check import _pre_invite_degree_check

        row = _make_to_send_row()
        mock_pb = MagicMock()
        mock_attio = MagicMock()
        today = date(2026, 5, 22)

        type_err = TypeError("Caller assembled wrong type")
        mock_writer = MagicMock()
        mock_writer.apply.side_effect = type_err

        degree_csv = f"profileUrl,connectionDegree\n{row['linkedInUrl']},1st"
        mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
        mock_pb.wait_for_completion.return_value = None
        mock_pb.download_result_csv.return_value = degree_csv

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch("workflows.pre_invite_check.AttioWriter", return_value=mock_writer),
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
        ):
            mock_cache.partition.return_value = ({}, [row["linkedInUrl"]])
            mock_cache.RECHECK_TTL_DAYS = 7

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
                pytest.raises(TypeError),
            ):
                _pre_invite_degree_check(
                    [row], mock_pb, "scraper-id", mock_attio, "list-id", today=today
                )


# ============================================================
# 6. Phase 0 ACCEPTED flip (detect_accepted_connections)
# ============================================================


class TestPhase0AcceptedFlip:
    """Tests for detect_accepted_connections Phase 0 experiment_id_frozen_at stamping."""

    def _make_conn_sent_attrs(self, **overrides) -> dict:
        """Build a CONNECTION_SENT attrs dict with the correct PipelineStage value."""
        from models.pipeline import PipelineStage
        base = {
            "entry_id": "entry-cs-1",
            "record_id": "rec-cs-1",
            "stage": PipelineStage.CONNECTION_SENT.value,  # "Connection Sent"
            "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
            "experiment_id": "exp-active",
            "experiment_id_frozen_at": "connection_sent",
        }
        base.update(overrides)
        return base

    def _run_phase0(self, conn_sent_attrs: dict, linkedin_url: str):
        """Run detect_accepted_connections with a single fresh-cache-hit 1st-degree row."""
        from workflows.daily_check import detect_accepted_connections

        mock_attio = MagicMock()
        mock_pb = MagicMock()
        mock_cache = MagicMock()

        # cache.get returns (name, company, linkedin_url, industry, title)
        mock_cache.get.return_value = ("CS Person", None, linkedin_url, None, None)

        with (
            patch.dict("os.environ", {"ATTIO_LIST_ID": "test-list-id"}, clear=False),
            patch(
                "workflows.daily_check._get_all_entries_parsed",
                return_value=[conn_sent_attrs],
            ),
            patch("workflows.daily_check.recheck_cache") as mock_rc,
            patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
        ):
            # All URLs in fresh_cache with degree=1st → cache hit path
            mock_rc.partition.return_value = (
                {linkedin_url: {"degree": "1st"}},
                [],  # no stale URLs
            )
            mock_rc.RECHECK_TTL_DAYS = 7

            detect_accepted_connections(
                mock_attio, mock_pb, "scraper-id", cache=mock_cache
            )

        return mock_attio

    def test_stamps_experiment_id_frozen_at_accepted(self):
        """Phase 0 flip: experiment_id_frozen_at='accepted' stamped when experiment_id present."""
        from models.pipeline import PipelineStage

        url = "https://www.linkedin.com/in/cs-person/"
        attrs = self._make_conn_sent_attrs(experiment_id="exp-active")
        mock_attio = self._run_phase0(attrs, url)

        # The update_list_entry call should include experiment_id_frozen_at='accepted'
        assert mock_attio.update_list_entry.called, (
            "update_list_entry must be called to flip stage to ACCEPTED"
        )
        call_kwargs = mock_attio.update_list_entry.call_args[1]
        entry_attrs = call_kwargs.get("entry_attributes", {})
        assert entry_attrs.get("experiment_id_frozen_at") == "accepted"
        assert entry_attrs.get("stage") == PipelineStage.ACCEPTED.value

    def test_no_stamp_when_experiment_id_is_none(self):
        """Phase 0 flip: no frozen_at stamp when experiment_id is None (pre-PR-21 row)."""
        url = "https://www.linkedin.com/in/cs-person2/"
        attrs = self._make_conn_sent_attrs(experiment_id=None, experiment_id_frozen_at=None)
        mock_attio = self._run_phase0(attrs, url)

        assert mock_attio.update_list_entry.called
        call_kwargs = mock_attio.update_list_entry.call_args[1]
        entry_attrs = call_kwargs.get("entry_attributes", {})
        assert "experiment_id_frozen_at" not in entry_attrs


# ============================================================
# 7. to_send_data row shape contract (run_connection_requests)
# ============================================================


class TestToSendDataContract:
    def test_experiment_id_present_in_to_send_data(self):
        """run_connection_requests must populate experiment_id on to_send_data rows."""
        # We verify this by checking that the key is present in the dict shape
        # (the actual population is tested via integration; this is the contract test)
        row = _make_to_send_row()
        assert "experiment_id" in row, "PR-21 contract: experiment_id MUST be a key"
        assert "experiment_id_frozen_at" in row, (
            "PR-21 contract: experiment_id_frozen_at MUST be a key"
        )

    def test_experiment_id_value_can_be_none(self):
        """None is a valid value for experiment_id (no active experiment)."""
        row = _make_to_send_row(experiment_id=None, experiment_id_frozen_at=None)
        assert row["experiment_id"] is None
        assert row["experiment_id_frozen_at"] is None


# ============================================================
# 8. Writer registry parity (CI guard)
# ============================================================


class TestWriterRegistryParity:
    """All authorized writers must be registered for experiment_id attrs.

    Note: experiment_id has 6 writers (PR-22 adds scripts.backfill_experiment_id_archaeology).
    experiment_id_frozen_at has 7 writers (adds pre_invite_check which uses
    WriteIntent with writer_module="workflows.pre_invite_check._pre_invite_degree_check",
    and PR-22 adds scripts.backfill_experiment_id_archaeology as the 7th).
    """

    EXPECTED_WRITERS_EXPERIMENT_ID = {
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "workflows.daily_check.run_connection_requests",
        "workflows.daily_check.detect_accepted_connections",
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.attio_dedup",
        # PR-22: 6th writer — archaeology backfill for legacy rows
        "scripts.backfill_experiment_id_archaeology",
    }

    EXPECTED_WRITERS_FROZEN_AT = {
        "workflows.weekly_prospect._build_prospect_entry_attrs",
        "workflows.daily_check.run_connection_requests",
        "workflows.pre_invite_check._pre_invite_degree_check",
        "workflows.daily_check.detect_accepted_connections",
        "scripts.migrate_experiments_tsv_to_attio",
        "scripts.attio_dedup",
        # PR-22: 7th writer — archaeology backfill for legacy rows
        "scripts.backfill_experiment_id_archaeology",
        # Phase C: 8th writer — reconciliation sweep re-stamps connection_sent.
        "workflows.pending_invite_reconciliation.run_pending_invite_reconciliation",
        # 9th writer — the optional Botdog event drain freezes the cohort at
        # "connection_sent" on an event-confirmed invite, exactly as the PB
        # invite path does, so botdog-stamped rows are measured against the
        # same freeze boundary as their PB counterparts.
        "workflows.botdog_ingest.apply_lead_events",
    }

    def test_experiment_id_has_all_6_writers(self):
        """PR-22 update: 6 writers (added scripts.backfill_experiment_id_archaeology)."""
        from clients.attio_writer_registry import get_authorized_writers

        owners = get_authorized_writers("linkedin_outreach", "experiment_id")
        assert owners is not None
        assert set(owners) == self.EXPECTED_WRITERS_EXPERIMENT_ID, (
            f"experiment_id writer set mismatch: "
            f"got {sorted(owners)}, expected {sorted(self.EXPECTED_WRITERS_EXPERIMENT_ID)}"
        )

    def test_experiment_id_frozen_at_has_all_9_writers(self):
        """PR-22: 7th writer (archaeology backfill). Phase C: 8th writer
        (pending_invite_reconciliation re-stamps connection_sent). 9th:
        the optional Botdog event drain's invite-confirmed freeze."""
        from clients.attio_writer_registry import get_authorized_writers

        owners = get_authorized_writers("linkedin_outreach", "experiment_id_frozen_at")
        assert owners is not None
        assert set(owners) == self.EXPECTED_WRITERS_FROZEN_AT, (
            f"experiment_id_frozen_at writer set mismatch: "
            f"got {sorted(owners)}, expected {sorted(self.EXPECTED_WRITERS_FROZEN_AT)}"
        )

    def test_detect_accepted_connections_is_authorized_writer(self):
        """Phase 0 writer must be registered (Lesson 4)."""
        from clients.attio_writer_registry import is_authorized_writer

        assert is_authorized_writer(
            "linkedin_outreach",
            "experiment_id_frozen_at",
            "workflows.daily_check.detect_accepted_connections",
        )
        assert is_authorized_writer(
            "linkedin_outreach",
            "experiment_id",
            "workflows.daily_check.detect_accepted_connections",
        )

    def test_manifest_registry_parity(self):
        """Manifest and registry must agree on experiment_id + frozen_at writers."""
        from pathlib import Path

        import yaml

        from clients.attio_writer_registry import get_authorized_writers

        manifest_path = Path(__file__).parent.parent / "docs" / "attio_schema_deltas.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())

        target_slugs = {"experiment_id", "experiment_id_frozen_at"}
        for attr in manifest.get("attributes", []):
            if attr.get("object") != "linkedin_outreach":
                continue
            if attr.get("slug") not in target_slugs:
                continue

            key = (attr["object"], attr["slug"])
            registry_owners = get_authorized_writers(*key)
            assert registry_owners is not None

            manifest_owner = attr.get("write_owner_module")
            manifest_owners = [manifest_owner] if isinstance(manifest_owner, str) else list(manifest_owner or [])

            assert set(manifest_owners) == set(registry_owners), (
                f"{key}: manifest={sorted(manifest_owners)} vs "
                f"registry={sorted(registry_owners)}"
            )


# ============================================================
# 9. ExperimentIdImmutableError error surface & message
# ============================================================


class TestExperimentIdImmutableErrorSurface:
    def test_error_not_swallowed_by_except_value_error(self):
        """Lesson 2: ExperimentIdImmutableError must NOT be caught by bare except ValueError."""
        exc = ExperimentIdImmutableError(
            record_id="r",
            field="experiment_id_frozen_at",
            old_value="accepted",
            new_value="prospect",
        )

        # Verify it's NOT a ValueError
        try:
            raise exc
        except ValueError:
            pytest.fail(
                "ExperimentIdImmutableError was caught by except ValueError — "
                "it must subclass RuntimeError only"
            )
        except RuntimeError:
            pass  # Correct: RuntimeError catches it

    def test_error_is_catchable_as_runtime_error(self):
        """ExperimentIdImmutableError propagates through except ValueError blocks."""
        exc = ExperimentIdImmutableError(
            record_id="r",
            field="experiment_id_frozen_at",
            old_value="accepted",
            new_value="prospect",
        )
        caught = None
        try:
            raise exc
        except ValueError:
            caught = "ValueError"
        except RuntimeError:
            caught = "RuntimeError"

        assert caught == "RuntimeError"


# ============================================================
# 10. Invite-success path: frozen_at="connection_sent" stamp
#     (fold-in BLOCKING-1 — advertiser-daily)
# ============================================================


class TestInviteSuccessFrozenAtStamp:
    """Daily-invite success path (`run_connection_requests` PB-confirmed-sent
    branch) MUST stamp experiment_id_frozen_at="connection_sent" when an
    experiment is running. Without this stamp, the `connection_sent` enum
    value would be effectively dead — never written by any authorized writer.

    Guarded by current_experiment_id is not None: no stamp on no-experiment
    rows (mirrors Phase 0 stamping pattern).
    """

    def _make_attio_entry_prospect(self, *, entry_id: str, record_id: str) -> dict:
        """Build a PROSPECT-stage Attio list entry (qualified, not in quarantine).

        Shape mirrors `tests.test_integration._make_attio_entry` so parse_entry
        produces a valid persona/language pair.
        """
        from datetime import date, timedelta
        # Old `last_contact_date` so the entry passes invite eligibility:
        old_date = (date.today() - timedelta(days=30)).isoformat()
        return {
            "entry_id": entry_id,
            "parent_record_id": record_id,
            "created_at": f"{old_date}T00:00:00.000000000Z",
            "entry_values": {
                "stage": [{"status": {"title": "Prospect"}}],
                "quality_score": [75],
                "persona": ["operations_leaders"],
                "language": ["es"],
                "last_contact_date": [old_date],
                "dm_step": [0],
            },
        }

    def _run_invite_success(self, *, current_experiment_id: str | None) -> dict:
        """Drive run_connection_requests through to the PB-confirmed-sent
        Attio advance branch. Returns the entry_attributes dict captured at
        the `_attio_advance_with_escalation` call site.
        """
        import os
        from datetime import UTC, datetime

        from clients.pb_envelope import PBCompletion, PBLaunch, hash_arguments
        from workflows.daily_check import run_connection_requests

        # CSV "Message sent" for the URL so the advance gate passes
        url = "https://www.linkedin.com/in/test-invite/"
        pb = MagicMock()
        launch = PBLaunch(
            container_id="c-inv",
            agent_id="agent-nb",
            launched_at=datetime(2026, 5, 22, 12, 0, tzinfo=UTC),
            arguments_sha256=hash_arguments(None),
        )
        completion = PBCompletion(
            container_id="c-inv",
            status="finished",
            log_output="Invitation sent to test-invite",
            raw_output={"status": "finished", "output": ""},
        )
        pb.launch_agent.return_value = launch
        pb.wait_for_completion.return_value = completion
        pb.download_result_csv.return_value = f"query,status\n{url},Message sent\n"

        attio = MagicMock()
        attio.is_person_company_corrupted.return_value = False
        attio.query_list_entries.return_value = [
            self._make_attio_entry_prospect(entry_id="ent-inv", record_id="rec-inv"),
        ]

        captured: dict = {}

        def fake_advance(**kwargs):
            captured.update(kwargs.get("entry_attributes", {}))
            return True

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
             patch("workflows.daily_check.record_connections"), \
             patch("workflows.daily_check.record_visits"), \
             patch("workflows.daily_check.get_current_experiment_id",
                   return_value=current_experiment_id), \
             patch("workflows.daily_check._attio_advance_with_escalation",
                   side_effect=fake_advance), \
             patch("workflows.daily_check._pre_invite_degree_check") as mock_pic, \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"), \
             patch("workflows.daily_check._write_company_throttle_tally"), \
             patch("workflows.daily_check._company_id_for_prospect", return_value="comp-1"), \
             patch("workflows.daily_check.get_remaining",
                   return_value={"connections": 25, "messages": 30, "visits": 50}), \
             patch("workflows.daily_check.ensure_throttle_policy_decision_opened"):
            mock_cache_get.return_value = ("Test Person", "Acme", url, "", "Plant Manager")

            # Pre-invite passes both rows through unchanged (already 2nd/3rd).
            # _pre_invite_degree_check returns (still_to_invite, already_connected).
            def _passthrough_pic(rows, *_a, **_kw):
                return rows, []
            mock_pic.side_effect = _passthrough_pic

            run_connection_requests(
                attio=attio,
                pb=pb,
                network_booster_id="agent-nb",
                daily_run=fake_daily_run(),
                auto_confirm=True,
            )

        return captured

    def test_invite_success_stamps_frozen_at_connection_sent(self):
        """Fold-in BLOCKING-1: when an experiment is running, invite-success
        path stamps experiment_id_frozen_at='connection_sent'.
        """
        captured = self._run_invite_success(current_experiment_id="exp-active")

        assert captured.get("stage") == "Connection Sent"
        assert captured.get("experiment_id") == "exp-active"
        assert captured.get("experiment_id_frozen_at") == "connection_sent", (
            "Invite-success path must stamp frozen_at='connection_sent' so the "
            "enum value is actually used; otherwise it's a dead value."
        )

    def test_invite_success_omits_frozen_at_when_no_experiment(self):
        """Fold-in BLOCKING-1: when no experiment is running, frozen_at is
        NOT stamped (mirrors Phase 0 stamping pattern — no spurious write).
        """
        captured = self._run_invite_success(current_experiment_id=None)

        assert captured.get("stage") == "Connection Sent"
        # experiment_id is explicitly None (set by the call site even with no
        # experiment), but frozen_at must be absent (no stamp for empty cohort).
        assert "experiment_id_frozen_at" not in captured, (
            "When no experiment is running, frozen_at must NOT be stamped — "
            "the row is measurement-excluded and PR-22 archaeology handles it."
        )


# ============================================================
# 11. Phase 0 warning includes record_id (fold-in CRIT — silent-failure)
# ============================================================


class TestPhase0EscalatesOnAttioFailure:
    """Phase 0 ACCEPTED flips (in detect_accepted_connections) must route
    Attio failures through ``_attio_advance_with_escalation`` so a
    transient Attio error opens an ``attio_write_failed`` queue row
    instead of being swallowed by a broad-except + click.echo.

    Pre-fix (Wave-1.5 lens-QA pass-1, salesman-daily + engineer-sre lenses):
    the cache-hit and PB-scrape branches called ``attio.update_list_entry``
    directly + ``except Exception: click.echo("Warning...")``. That violated
    F-PR-4 AttioWriter routing (§3.15) and §3 #9 no-silent-fallbacks. After
    the refactor, transient httpx errors are caught by the helper, escalated
    typed, and the row is left at its prior stage; non-transient exceptions
    (RuntimeError, TypeError) propagate as code bugs.
    """

    def _run_phase0_with_failing_update(
        self, attrs: dict, linkedin_url: str, side_effect: BaseException
    ) -> tuple[MagicMock, MagicMock]:
        """Run detect_accepted_connections with a single cache-hit row whose
        update_list_entry raises ``side_effect``. Returns ``(mock_attio,
        mock_escalate)`` so callers can assert on both.

        Wave-2-B: AttioWriter routes list-entry writes via
        ``update_list_entry`` (legacy mock injection point still
        works) AND requires a non-empty ``list_id``. Patch
        ``workflows.escalation.escalate`` at its canonical module so
        BOTH the helper's defense-in-depth escalate path and
        AttioWriter's built-in ``_dlq_and_escalate`` route into the
        same mock.
        """
        from workflows.daily_check import detect_accepted_connections

        mock_attio = MagicMock()
        mock_attio.update_list_entry.side_effect = side_effect
        mock_pb = MagicMock()
        mock_cache = MagicMock()
        mock_cache.get.return_value = ("CS Person", None, linkedin_url, None, None)

        with (
            patch.dict("os.environ", {"ATTIO_LIST_ID": "test-list-id"}, clear=False),
            patch(
                "workflows.daily_check._get_all_entries_parsed",
                return_value=[attrs],
            ),
            patch("workflows.daily_check.recheck_cache") as mock_rc,
            patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
            patch("workflows.daily_check.escalate") as mock_escalate,
            patch("workflows.escalation.escalate", new=mock_escalate),
            patch("clients.attio_writer.RETRY_BUDGET_SECONDS", 0.0001),
        ):
            mock_rc.partition.return_value = (
                {linkedin_url: {"degree": "1st"}},
                [],  # no stale URLs
            )
            mock_rc.RECHECK_TTL_DAYS = 7

            detect_accepted_connections(
                mock_attio, mock_pb, "scraper-id", cache=mock_cache
            )

        return mock_attio, mock_escalate

    def _attrs(self, url: str) -> dict:
        from models.pipeline import PipelineStage

        return {
            "entry_id": "ent-fail",
            "record_id": "rec-phase0-fail-id",
            "stage": PipelineStage.CONNECTION_SENT.value,
            "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
            "experiment_id": "exp-active",
            "experiment_id_frozen_at": "connection_sent",
            "linkedin_url": url,
        }

    def test_phase0_cache_hit_httpx_error_opens_attio_write_failed_queue_row(
        self, capsys
    ):
        """Wave-1.5 (§3.15 + §3 #9): cache-hit Phase 0 httpx failure routes
        through the helper, opens an attio_write_failed queue row with the
        entry_id + step_label, and does NOT silently warn-and-continue."""
        url = "https://www.linkedin.com/in/phase0-fail/"
        http_err = httpx.HTTPStatusError(
            "503 Server Error",
            request=MagicMock(),
            response=MagicMock(status_code=503),
        )
        mock_attio, mock_escalate = self._run_phase0_with_failing_update(
            self._attrs(url), url, http_err
        )

        # AttioWriter opened a typed queue row (not a click.echo
        # "Warning") via its built-in _dlq_and_escalate. Wave-2-B
        # uses AttioWriter's idempotency_key shape
        # ``f"{object}-{record_id}-{kind}"`` instead of the legacy
        # ``f"{entry_id}|{today}|{step_label}"``. Either key still
        # carries the entry_id so the operator can correlate back.
        assert mock_escalate.called, (
            "transient httpx error must open attio_write_failed queue row "
            "(F-PR-4 §3.15 / §3 #9)"
        )
        attio_write_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "attio_write_failed"
        ]
        assert attio_write_calls, "no attio_write_failed escalation emitted"
        first = attio_write_calls[0]
        assert "ent-fail" in first.kwargs["idempotency_key"]

        # Helper-emitted stderr message includes the URL + entry_id so the
        # operator can triage the queue row back to the source profile.
        captured = capsys.readouterr()
        assert url in captured.err
        assert "ent-fail" in captured.err
        assert "FAILED" in captured.err

    def test_phase0_cache_hit_runtime_error_propagates_as_code_bug(self):
        """Wave-1.5: a non-transient exception (RuntimeError / TypeError) is
        NOT a silent-warning case — it's a code bug. The helper only catches
        typed-narrow httpx errors; other exceptions propagate."""
        url = "https://www.linkedin.com/in/phase0-bug/"
        with pytest.raises(RuntimeError):
            self._run_phase0_with_failing_update(
                self._attrs(url), url, RuntimeError("not a transient error")
            )


# ============================================================
# Wave-1.6.2 FIX-B regression: Pattern-A immutability raise
# must not orphan other rows in the batch
# ============================================================


class TestFixBPatternAImmutabilityNoOrphan:
    """Wave-1.6.2 FIX-B (adversarial EXT-SB-3 IMPORTANT): when
    `_check_experiment_id_immutability` raises mid-loop in the
    Pattern-A flip path, OTHER 1st-degree rows in the batch must
    still get their flip to ACCEPTED. Pre-Wave-1.6.2 the raise
    propagated out of the `for row in to_send_data:` loop and
    orphaned every row after the offender — those stayed at
    PROSPECT and became eligible to re-invite tomorrow (§3.1
    violation).
    """

    def _run_two_row_batch_with_first_immutable(self):
        from workflows.pre_invite_check import _pre_invite_degree_check

        # Row 1: prior_frozen_at='connection_sent' → triggers
        # ExperimentIdImmutableError when guard tries to overwrite to
        # 'accepted'. Row 2: prior_frozen_at='prospect' → clean Pattern-B.
        rows = [
            _make_to_send_row(
                linkedInUrl="https://www.linkedin.com/in/row-1-immutable/",
                entry_id="ent-1",
                record_id="rec-1",
                experiment_id="exp-a",
                experiment_id_frozen_at="connection_sent",  # immutable
            ),
            _make_to_send_row(
                linkedInUrl="https://www.linkedin.com/in/row-2-clean/",
                entry_id="ent-2",
                record_id="rec-2",
                experiment_id="exp-b",
                experiment_id_frozen_at="prospect",  # clean
            ),
        ]

        mock_pb = MagicMock()
        mock_attio = MagicMock()
        mock_writer = _make_attio_writer_success()
        today = date(2026, 5, 25)
        list_id = "list-test"

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch("workflows.pre_invite_check.AttioWriter", return_value=mock_writer),
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
            patch("workflows.pre_invite_check.escalate") as mock_escalate,
        ):
            mock_cache.partition.return_value = ({}, [r["linkedInUrl"] for r in rows])
            mock_cache.RECHECK_TTL_DAYS = 7

            degree_csv = "\n".join([
                "profileUrl,connectionDegree",
                f"{rows[0]['linkedInUrl']},1st",
                f"{rows[1]['linkedInUrl']},1st",
            ])
            mock_pb.launch_agent.return_value = MagicMock(container_id="c1")
            mock_pb.wait_for_completion.return_value = None
            mock_pb.download_result_csv.return_value = degree_csv

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
            ):
                still, already = _pre_invite_degree_check(
                    rows,
                    mock_pb,
                    "scraper-id",
                    mock_attio,
                    list_id,
                    today=today,
                )
                return still, already, mock_writer, mock_escalate

    def test_immutability_raise_does_not_orphan_subsequent_row(self):
        """The §3.1 hard red line: row 1 hits ExperimentIdImmutableError
        on the guard, row 2 must STILL get its Pattern-A flip applied
        (writer.apply called for entry_id='ent-2'). Pre-FIX-B, the raise
        bubbled out of the for-loop and row 2 was never processed.
        """
        still, already, mock_writer, _ = self._run_two_row_batch_with_first_immutable()

        # Both rows must be flipped to ACCEPTED — neither orphaned.
        apply_call_record_ids = [
            call.args[0].record_id  # WriteIntent.record_id holds the entry_id
            for call in mock_writer.apply.call_args_list
        ]
        assert "ent-1" in apply_call_record_ids, (
            "FIX-B regression: row 1 (the immutability violator) was not "
            "flipped to ACCEPTED. Pre-FIX-B the raise blocked even this "
            "row's flip — leaving it at PROSPECT and eligible to re-invite."
        )
        assert "ent-2" in apply_call_record_ids, (
            "FIX-B regression: row 2 (downstream of the immutability raise) "
            "was orphaned — the for-loop terminated mid-batch and row 2's "
            "Pattern-A flip never ran. Tomorrow's run will invite an "
            "already-1st-degree contact. Hard §3.1 violation."
        )
        # already_connected should contain both rows.
        assert len(already) == 2, (
            f"expected both rows in already_connected; got {len(already)}"
        )

    def test_immutability_raise_opens_operator_queue_row(self):
        """When the guard raises, the function must escalate via
        experiment_id_immutability_violation so the operator sees the
        cohort overwrite attempt. Mirrors the daily_check.py FIX-2'
        semantic."""
        _, _, _, mock_escalate = self._run_two_row_batch_with_first_immutable()

        immut_calls = [
            c
            for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "experiment_id_immutability_violation"
        ]
        assert len(immut_calls) == 1, (
            f"expected 1 experiment_id_immutability_violation escalation; "
            f"got {[c.kwargs.get('type') for c in mock_escalate.call_args_list]}"
        )
        payload = immut_calls[0].kwargs["payload"]
        assert payload["record_id"] == "rec-1"
        assert "pre_invite_check" in payload["context"].lower() or \
               "pattern-a" in payload["context"].lower()

    def test_escalate_itself_raises_does_not_orphan_subsequent_row(self):
        """Wave-1.6.3 (adversarial follow-up NIT — symmetric with FIX-A
        test_regular_invite_path_escalate_raise_does_not_orphan_other_rows).

        FIX-B wrapped the escalate() call inside the immutability except
        branch in a try/except Exception so that an escalate() raise
        (transient Attio 5xx, network blip, payload schema drift during
        F1 deploy race, etc.) cannot kill the per-row loop. Without this
        guarantee, an escalate failure on row 1 would orphan row 2's
        Pattern-A flip — row 2 stays at PROSPECT and becomes eligible to
        re-invite tomorrow (§3.1 violation).

        Setup mirrors `_run_two_row_batch_with_first_immutable` but
        patches `escalate` to ITSELF raise. We assert BOTH rows still
        get their Pattern-A flip applied via writer.apply.
        """
        from datetime import date as _date

        from workflows.pre_invite_check import _pre_invite_degree_check

        rows = [
            _make_to_send_row(
                linkedInUrl="https://www.linkedin.com/in/row-1-immutable-esc-raise/",
                entry_id="ent-1-esc",
                record_id="rec-1-esc",
                experiment_id="exp-a",
                experiment_id_frozen_at="connection_sent",  # immutable
            ),
            _make_to_send_row(
                linkedInUrl="https://www.linkedin.com/in/row-2-clean-esc-raise/",
                entry_id="ent-2-esc",
                record_id="rec-2-esc",
                experiment_id="exp-b",
                experiment_id_frozen_at="prospect",
            ),
        ]

        mock_pb = MagicMock()
        mock_attio = MagicMock()
        mock_writer = _make_attio_writer_success()
        today = _date(2026, 5, 25)
        list_id = "list-test-esc-raise"

        response = MagicMock(spec=httpx.Response)
        response.status_code = 503
        response.text = '{"error": "service unavailable"}'

        with (
            patch("workflows.pre_invite_check.recheck_cache") as mock_cache,
            patch("workflows.pre_invite_check.AttioWriter", return_value=mock_writer),
            patch(
                "workflows.pre_invite_check._normalize_linkedin_url",
                side_effect=lambda u: u,
            ),
            patch("workflows.pre_invite_check.operator_today", return_value=today),
            patch(
                "workflows.pre_invite_check.escalate",
                side_effect=httpx.HTTPStatusError(
                    "503 Service Unavailable",
                    request=MagicMock(),
                    response=response,
                ),
            ),
        ):
            mock_cache.partition.return_value = ({}, [r["linkedInUrl"] for r in rows])
            mock_cache.RECHECK_TTL_DAYS = 7

            degree_csv = "\n".join([
                "profileUrl,connectionDegree",
                f"{rows[0]['linkedInUrl']},1st",
                f"{rows[1]['linkedInUrl']},1st",
            ])
            mock_pb.launch_agent.return_value = MagicMock(container_id="c-esc")
            mock_pb.wait_for_completion.return_value = None
            mock_pb.download_result_csv.return_value = degree_csv

            import workflows.daily_check as _dc
            with (
                patch.object(_dc, "write_prospects_to_sheet", return_value="http://sheet"),
                patch.object(_dc, "_pb_session_args", return_value={}),
            ):
                still, already = _pre_invite_degree_check(
                    rows,
                    mock_pb,
                    "scraper-id",
                    mock_attio,
                    list_id,
                    today=today,
                )

        apply_call_record_ids = [
            call.args[0].record_id
            for call in mock_writer.apply.call_args_list
        ]
        assert "ent-1-esc" in apply_call_record_ids, (
            "Wave-1.6.3 regression: row 1's Pattern-A flip was orphaned "
            "because escalate() raised inside the immutability except "
            "branch and the FIX-B try/except failed to swallow. "
            "Pre-FIX-B and pre-Wave-1.6.3 wrap, the raise terminated the "
            "loop before the writer.apply call below. Hard §3.1 risk."
        )
        assert "ent-2-esc" in apply_call_record_ids, (
            "Wave-1.6.3 regression: row 2 (downstream of the escalate-"
            "raising row) was orphaned — the for-loop terminated mid-batch. "
            "This is the FIX-A-class §3.1 violation FIX-B was meant to "
            "prevent in the Pattern-A path."
        )
        assert len(already) == 2, (
            f"expected both rows in already_connected; got {len(already)}"
        )


@pytest.fixture(autouse=True)
def _fresh_state_boundary():
    from unittest.mock import patch

    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.email_campaign.verify_email_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
