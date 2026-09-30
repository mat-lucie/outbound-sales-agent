"""PR-13 (B-PD-002): per-company outreach throttle.

Covers the four pieces shipped together:

  1. `workflows.throttle.company_throttle_permits` — pure-read helper
     that returns True iff outreach to a company is permitted today.
  2. `workflows.throttle.ensure_throttle_policy_decision_opened` — the
     once-per-daily-run idempotent open of the
     `throttle_ttl_policy` configuration_decision row.
  3. `workflows.daily_check._check_company_throttle_or_skip` — the
     daily_check integration that opens `company_throttled` queue
     rows on skip.
  4. `workflows.daily_check._write_company_throttle_tally` — the
     post-confirmed-send write of the four Companies attrs.

§3.1 protection: the throttle is the second line of no-resend defense.
Tests assert that:
  - a throttled company blocks the daily slice from sending,
  - the write fires BEFORE the next sibling person is evaluated
    (Round-4 D32 multi-thread ABM safety),
  - permissive defaults (no company / null last_outreach_at) preserve
    forward progress without triggering false positives.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from workflows.throttle import (
    DEFAULT_THROTTLE_WINDOW_DAYS,
    company_throttle_permits,
    ensure_throttle_policy_decision_opened,
)

# ==================================================================
# Helpers
# ==================================================================


def _company_payload(last_outreach_at: str | None) -> dict:
    """Build a minimal Attio company record dict whose `values` carry
    `last_outreach_at`. Other fields are omitted — the helper only
    reads this attribute."""
    if last_outreach_at is None:
        values: dict = {}
    else:
        values = {"last_outreach_at": [{"value": last_outreach_at}]}
    return {"values": values}


# ==================================================================
# company_throttle_permits — happy/skip/blocked paths
# ==================================================================


class TestCompanyThrottlePermits:
    def test_none_company_id_is_permitted(self):
        """Permissive default: prospect with no linked company. The
        per-person guards (dm_step / last_contact_date) remain the
        primary §3.1 protection."""
        attio = MagicMock()
        assert company_throttle_permits(None, date(2026, 5, 22), attio=attio) is True
        # No Attio read should happen.
        attio.get_company.assert_not_called()

    def test_unknown_company_returns_permitted(self):
        """Company record_id doesn't resolve in Attio (deleted? mis-typed?)
        — defensively permissive, same rationale as None company_id."""
        attio = MagicMock()
        attio.get_company.return_value = None
        assert company_throttle_permits("rec_missing", date(2026, 5, 22), attio=attio) is True

    def test_null_last_outreach_at_is_permitted(self):
        """First contact ever to this company — never throttled."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload(None)
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is True

    def test_recent_outreach_blocks(self):
        """Last outreach 5 days ago, default window is 30d — blocked."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is False

    def test_exactly_at_window_permitted(self):
        """30 days elapsed exactly — permitted (window is `elapsed >=
        window_days`, not `>`). Off-by-one correctness."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-04-22T12:00:00Z")
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is True

    def test_one_day_before_window_blocks(self):
        """13 days elapsed — still inside the 14-day window, blocked."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-05-09T12:00:00Z")
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is False

    def test_custom_window_days(self):
        """A 14-day window changes the verdict for a 20-day-old contact."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-05-02T12:00:00Z")
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio, window_days=14) is True
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio, window_days=30) is False

    def test_default_window_is_14_days(self):
        """Operator lowered the §3.8 default 30→14 (2026-06-01) for throughput
        while still preventing same-week same-company collisions."""
        assert DEFAULT_THROTTLE_WINDOW_DAYS == 14

    def test_malformed_timestamp_treated_as_no_outreach(self):
        """A garbage value in Attio (operator manually entered junk?)
        should NOT throttle — defensively permissive matches the null
        case. The malformed parse path returns None, which is treated
        as 'no recorded prior outreach'."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("not-an-iso-date")
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is True

    def test_zulu_timezone_parsed(self):
        """Attio commonly emits ISO with trailing 'Z'; helper must
        canonicalize before fromisoformat."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        # 5 days ago → blocked.
        assert company_throttle_permits("rec_co", date(2026, 5, 22), attio=attio) is False

    def test_stale_company_reference_emits_audit_event(self):
        """PR-13 fold-in: when `attio.get_company()` returns None for a
        non-null company_id, the cache is stale (deleted Company record,
        manual mid-run delete, race with dedup). The helper stays
        permissive at runtime but emits a `stale_company_reference`
        audit event so operators can fix the cache. Distinguishing this
        case from `company_id=None` (intentional, no linked company) is
        the §0 #9 requirement."""
        attio = MagicMock()
        attio.get_company.return_value = None

        events: list[dict] = []

        class _FakeAuditLogger:
            def event(self, kind: str, **fields: object) -> None:
                events.append({"kind": kind, **fields})

        result = company_throttle_permits(
            "rec_deleted_co",
            date(2026, 5, 22),
            attio=attio,
            audit_logger=_FakeAuditLogger(),  # type: ignore[arg-type]
        )
        assert result is True  # still permissive at runtime
        assert len(events) == 1
        assert events[0]["kind"] == "stale_company_reference"
        assert events[0]["company_id"] == "rec_deleted_co"

    def test_malformed_timestamp_emits_audit_event(self):
        """PR-13 fold-in: a malformed `last_outreach_at` in Attio is
        silent corruption (operator manually pasted junk, partial
        migration, upstream writer regressed). The helper falls
        through permissively but emits a `malformed_company_last_outreach_at`
        audit event mirroring the daily_check.py:96-104 precedent
        (PR-12's `malformed_next_eligible_send_date`)."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("not-an-iso-date")

        events: list[dict] = []

        class _FakeAuditLogger:
            def event(self, kind: str, **fields: object) -> None:
                events.append({"kind": kind, **fields})

        result = company_throttle_permits(
            "rec_co",
            date(2026, 5, 22),
            attio=attio,
            audit_logger=_FakeAuditLogger(),  # type: ignore[arg-type]
        )
        assert result is True
        assert len(events) == 1
        assert events[0]["kind"] == "malformed_company_last_outreach_at"
        assert events[0]["company_id"] == "rec_co"
        assert events[0]["raw_value"] == "not-an-iso-date"


# ==================================================================
# ensure_throttle_policy_decision_opened — idempotency
# ==================================================================


class TestEnsureThrottlePolicyDecisionOpened:
    def test_calls_escalate_with_configuration_decision_type(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """The helper opens a `configuration_decision` row with
        decision_key='throttle_ttl_policy' and the 14d default-on-expiry."""
        captured: dict = {}

        def _fake_escalate(**kwargs):
            captured.update(kwargs)
            return {"id": "rec_queue_row"}

        monkeypatch.setattr(
            "workflows.throttle.escalate", _fake_escalate,
        )
        attio = MagicMock()
        ensure_throttle_policy_decision_opened(attio)

        assert captured["type"] == "configuration_decision"
        assert captured["decision_key"] == "throttle_ttl_policy"
        assert captured["payload"]["default_on_expiry"] == "14d"
        assert captured["payload"]["recommended_option"] == "14d"
        # All three option keys present.
        keys = [opt["key"] for opt in captured["payload"]["options"]]
        assert "30d" in keys
        assert "14d" in keys
        assert "60d" in keys

    def test_deadline_is_14_days(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """PR-13 fold-in: pr-test-analyzer + comment-analyzer caught a
        drift bug where the docstring promised a deadline that the code
        contradicted. The runtime default window is 14d, so the deadline
        delta must be 14. Locked in by this assertion so a future
        regression fails loud."""
        from datetime import timedelta

        captured: dict = {}

        def _fake_escalate(**kwargs):
            captured.update(kwargs)
            return {"id": "x"}

        monkeypatch.setattr(
            "workflows.throttle.escalate", _fake_escalate,
        )
        attio = MagicMock()
        ensure_throttle_policy_decision_opened(attio)

        assert "deadline" in captured
        # Deadline = today + 14 days. We can't pin the exact date
        # without freezing the clock, but the delta must be 14.
        assert (captured["deadline"] - date.today()) == timedelta(days=14)

    def test_repeated_calls_use_same_idempotency_key(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """escalate()'s `(type, decision_key, idempotency_key)`
        uniqueness ensures repeated calls deduplicate — the helper
        must pass the SAME idempotency_key every time."""
        keys_seen: list[str] = []

        def _fake_escalate(**kwargs):
            keys_seen.append(kwargs["idempotency_key"])
            return {"id": "rec_x"}

        monkeypatch.setattr(
            "workflows.throttle.escalate", _fake_escalate,
        )
        attio = MagicMock()
        ensure_throttle_policy_decision_opened(attio)
        ensure_throttle_policy_decision_opened(attio)

        assert len(keys_seen) == 2
        assert keys_seen[0] == keys_seen[1]


def _make_recording_escalate(calls: list[dict]):
    """Build an escalate() stand-in that records calls + returns a stub row."""
    def _fake(**kw):
        calls.append(kw)
        return {"id": "x"}
    return _fake


# ==================================================================
# _check_company_throttle_or_skip — daily_check integration
# ==================================================================


class TestCheckCompanyThrottleOrSkip:
    def test_permitted_company_returns_true_no_escalation(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        from workflows.daily_check import _check_company_throttle_or_skip

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )

        attio = MagicMock()
        attio._person_to_company = {}  # no company link
        attrs = {"record_id": "rec_person"}

        assert _check_company_throttle_or_skip(
            attrs, attio=attio, today=date(2026, 5, 22)
        ) is True
        assert escalate_calls == []

    def test_throttled_company_returns_false_opens_queue_row(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        from workflows.daily_check import _check_company_throttle_or_skip

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )

        attio = MagicMock()
        attio._person_to_company = {"rec_person": "rec_co"}
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        attrs = {"record_id": "rec_person"}

        result = _check_company_throttle_or_skip(
            attrs, attio=attio, today=date(2026, 5, 22)
        )

        assert result is False
        assert len(escalate_calls) == 1
        assert escalate_calls[0]["type"] == "company_throttled"
        payload = escalate_calls[0]["payload"]
        assert payload["record_id"] == "rec_person"
        assert payload["company_id"] == "rec_co"
        assert payload["throttle_date"] == "2026-05-22"
        assert payload["window_days"] == 14
        # Idempotency key carries (record_id, throttle_date) so each
        # prospect generates at most one queue row per day.
        assert escalate_calls[0]["idempotency_key"] == "company-throttled|rec_person|2026-05-22"

    def test_dry_run_skips_decision_but_writes_no_queue_row(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """Under dry_run a throttled prospect is still skipped (returns
        False) but no `company_throttled` row is written to Attio — a
        preview must stay read-only."""
        from workflows.daily_check import _check_company_throttle_or_skip

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )

        attio = MagicMock()
        attio._person_to_company = {"rec_person": "rec_co"}
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        attrs = {"record_id": "rec_person"}

        result = _check_company_throttle_or_skip(
            attrs, attio=attio, today=date(2026, 5, 22), dry_run=True,
        )

        # Skip DECISION preserved...
        assert result is False
        # ...but the Attio write is suppressed.
        assert escalate_calls == []

    def test_dry_run_still_logs_audit_event(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """The audit event is a local trail entry, not an Attio write, so
        it still fires under dry_run to record the previewed skip."""
        from workflows.daily_check import _check_company_throttle_or_skip

        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate([]),
        )

        attio = MagicMock()
        attio._person_to_company = {"rec_person": "rec_co"}
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        audit_logger = MagicMock()
        attrs = {"record_id": "rec_person"}

        _check_company_throttle_or_skip(
            attrs, attio=attio, today=date(2026, 5, 22),
            audit_logger=audit_logger, dry_run=True,
        )

        audit_logger.event.assert_called_once()
        assert audit_logger.event.call_args[0][0] == "company_throttled_skip"


# ==================================================================
# _write_company_throttle_tally — §3.15 sole-writer contract
# ==================================================================


class TestWriteCompanyThrottleTally:
    def test_writes_four_companies_attrs_for_dm1(self):
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        _write_company_throttle_tally(
            attio=attio,
            company_id="rec_co",
            person_record_id="rec_person",
            step_label="dm1",
            experiment_id="exp_xyz",
            today=date(2026, 5, 22),
        )

        attio.update_company.assert_called_once()
        call_args = attio.update_company.call_args
        assert call_args[0][0] == "rec_co"
        attrs = call_args[0][1]
        assert attrs["last_outreach_at"].startswith("2026-05-22T")
        assert attrs["last_outreach_step"] == "DM1"
        assert attrs["last_outreach_experiment_id"] == "exp_xyz"
        # record_reference shape per Attio v2.
        assert attrs["last_outreach_person_id"] == [
            {"target_object": "people", "target_record_id": "rec_person"}
        ]

    def test_step_invite_maps_to_connection_sent(self):
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        _write_company_throttle_tally(
            attio=attio,
            company_id="rec_co",
            person_record_id="rec_person",
            step_label="invite",
            experiment_id="exp_xyz",
            today=date(2026, 5, 22),
        )
        attrs = attio.update_company.call_args[0][1]
        assert attrs["last_outreach_step"] == "CONNECTION_SENT"

    @pytest.mark.parametrize(
        "step_label,expected",
        [("dm1", "DM1"), ("dm2", "DM2"), ("dm3", "DM3"),
         ("invite", "CONNECTION_SENT"), ("connection_note", "CONNECTION_SENT")],
    )
    def test_step_label_normalization(self, step_label: str, expected: str):
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        _write_company_throttle_tally(
            attio=attio, company_id="rec_co", person_record_id="rec_p",
            step_label=step_label, experiment_id=None,
            today=date(2026, 5, 22),
        )
        attrs = attio.update_company.call_args[0][1]
        assert attrs["last_outreach_step"] == expected

    def test_none_company_id_skips_write(self):
        """Defensive: if the prospect has no linked company, no write
        is attempted (and no error raised)."""
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        _write_company_throttle_tally(
            attio=attio, company_id=None, person_record_id="rec_p",
            step_label="dm1", experiment_id="exp",
            today=date(2026, 5, 22),
        )
        attio.update_company.assert_not_called()

    def test_unknown_step_label_raises_typed_value_error(self):
        """Refuse to write a malformed select value — programmer error
        per the docstring contract (callers always pass one of five
        known values). PR-13 fold-in: raise typed ValueError instead
        of silent-skip-with-optional-audit, so a future refactor
        introducing dm4 fails LOUD on the first call.

        §0 #9: missing data → typed error (not None, not silent skip).
        """
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        with pytest.raises(ValueError) as exc_info:
            _write_company_throttle_tally(
                attio=attio, company_id="rec_co", person_record_id="rec_p",
                step_label="dm99", experiment_id="exp",
                today=date(2026, 5, 22),
            )
        assert "dm99" in str(exc_info.value)
        assert "dm1/dm2/dm3/invite/connection_note" in str(exc_info.value)
        attio.update_company.assert_not_called()

    def test_none_experiment_id_writes_empty_string(self):
        """The Attio attribute is a `text` type — None gets coerced to
        empty string so the value is valid."""
        from workflows.daily_check import _write_company_throttle_tally

        attio = MagicMock()
        _write_company_throttle_tally(
            attio=attio, company_id="rec_co", person_record_id="rec_p",
            step_label="dm1", experiment_id=None,
            today=date(2026, 5, 22),
        )
        attrs = attio.update_company.call_args[0][1]
        assert attrs["last_outreach_experiment_id"] == ""

    def test_update_company_failure_opens_attio_write_failed_queue(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """A 5xx from Attio mid-write must NOT silently lose the
        throttle update — escalate `attio_write_failed`."""
        import httpx

        from workflows.daily_check import _write_company_throttle_tally

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )

        attio = MagicMock()
        attio.update_company.side_effect = httpx.RequestError("transport boom")

        _write_company_throttle_tally(
            attio=attio, company_id="rec_co", person_record_id="rec_p",
            step_label="dm1", experiment_id="exp",
            today=date(2026, 5, 22),
        )

        assert len(escalate_calls) == 1
        assert escalate_calls[0]["type"] == "attio_write_failed"
        assert escalate_calls[0]["payload"]["object"] == "companies"
        assert escalate_calls[0]["payload"]["record_id"] == "rec_co"


# ==================================================================
# CompanyThrottledPayload TypedDict registration
# ==================================================================


class TestCompanyThrottledEscalationSchema:
    def test_schema_registered(self):
        from workflows.escalation_schemas import (
            ESCALATION_SCHEMAS,
            CompanyThrottledPayload,
        )

        assert ESCALATION_SCHEMAS.get("company_throttled") is CompanyThrottledPayload

    def test_payload_required_fields(self):
        from workflows.escalation_schemas import CompanyThrottledPayload

        assert CompanyThrottledPayload.__required_keys__ == {
            "record_id",
            "company_id",
            "throttle_date",
            "window_days",
        }


# ==================================================================
# Multi-thread ABM safety — Round-4 D32 contract
# ==================================================================


class TestMultiThreadAbmSafety:
    """Round-4 D32: 'if two persons at the same company are both in
    the due queue in the same daily run, the second one should be
    throttled by the throttle check that fires AFTER the first one's
    DM sends and writes `last_outreach_at`.'

    This integration-style test simulates the read-after-write flow:
    person A's send updates Attio's `last_outreach_at`, then person
    B's throttle check reads the fresh value and blocks.
    """

    def test_sibling_person_blocked_after_first_send(self):
        """Person A and Person B share a company. After A's send
        writes `last_outreach_at=today`, B's pre-send throttle check
        reads that value and returns blocked."""
        from workflows.daily_check import _write_company_throttle_tally

        # State that the AttioClient mock will track.
        company_state: dict = {"last_outreach_at": None}

        def _get_company(rec_id: str) -> dict | None:
            if rec_id != "rec_co":
                return None
            return _company_payload(company_state["last_outreach_at"])

        def _update_company(rec_id: str, attrs: dict) -> None:
            if rec_id == "rec_co":
                company_state["last_outreach_at"] = attrs["last_outreach_at"]

        attio = MagicMock()
        attio.get_company.side_effect = _get_company
        attio.update_company.side_effect = _update_company

        today = date(2026, 5, 22)

        # Before any send: both persons would be permitted.
        assert company_throttle_permits("rec_co", today, attio=attio) is True

        # Person A's confirmed DM1 write.
        _write_company_throttle_tally(
            attio=attio, company_id="rec_co",
            person_record_id="rec_person_a",
            step_label="dm1", experiment_id="exp",
            today=today,
        )

        # After the write settles, sibling Person B's throttle check
        # reads the fresh value and returns blocked.
        assert company_throttle_permits("rec_co", today, attio=attio) is False


# ==================================================================
# Dry-run threading through the Part B call site (run_dm_sequencing)
# ==================================================================


class TestDryRunThreadingThroughDmSequencing:
    """The unit tests above pin `_check_company_throttle_or_skip` in
    isolation. These integration tests pin that `run_dm_sequencing`
    actually THREADS `dry_run` down to the helper — so a future plumbing
    refactor that drops the kwarg fails loudly instead of silently
    writing `company_throttled` rows to production Attio during a preview.
    """

    _ENV = {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake", "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
    }

    def _run(self, *, dry_run: bool, escalate_calls: list[dict]):
        import os
        from datetime import timedelta
        from unittest.mock import patch

        from tests.test_integration import _attio_with_full_schema, _make_attio_entry
        from workflows.daily_check import run_dm_sequencing
        from workflows.daily_run import DailyRun as _DailyRun

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-throttle-001",
            record_id="rec-throttle-001",
            stage="Accepted",
            quality_score=70,
            last_contact_date=two_days_ago,
            dm_step=0,
        )

        attio = _attio_with_full_schema()
        pb = MagicMock()
        attio.query_list_entries.return_value = [entry]
        attio.is_person_company_corrupted.return_value = False

        _fake_dr = MagicMock(spec=_DailyRun)
        _fake_dr.remaining.return_value = 30

        with patch.dict(os.environ, self._ENV), \
                patch(
                    "workflows.daily_check.escalate",
                    _make_recording_escalate(escalate_calls),
                ), \
                patch(
                    "workflows.daily_check.company_throttle_permits",
                    return_value=False,  # force the throttle-skip branch
                ), \
                patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
                patch("workflows.daily_check.can_send_messages", return_value=True), \
                patch(
                    "workflows.daily_check.write_prospects_to_sheet",
                    return_value="https://docs.google.com/spreadsheets/d/fake",
                ):
            mock_cache_get.return_value = (
                "Ana Ejemplo", "Cementra",
                "https://linkedin.com/in/throttled", "", "VP Operations",
            )
            return run_dm_sequencing(
                attio=attio, pb=pb, message_sender_id="agent-msg",
                daily_run=_fake_dr, dry_run=dry_run, auto_confirm=True,
            )

    def test_dry_run_does_not_write_company_throttled_row(self):
        """Regression guard: the Part B call site must thread dry_run=True
        so a preview writes no `company_throttled` row to production Attio."""
        escalate_calls: list[dict] = []
        self._run(dry_run=True, escalate_calls=escalate_calls)

        throttle_rows = [c for c in escalate_calls if c["type"] == "company_throttled"]
        assert throttle_rows == []

    def test_wet_run_still_writes_company_throttled_row(self):
        """Same harness, default wet run: the throttle skip still opens
        exactly one `company_throttled` row — proving the gate flips with
        dry_run rather than suppressing the write unconditionally."""
        escalate_calls: list[dict] = []
        self._run(dry_run=False, escalate_calls=escalate_calls)

        throttle_rows = [c for c in escalate_calls if c["type"] == "company_throttled"]
        assert len(throttle_rows) == 1
        assert throttle_rows[0]["payload"]["record_id"] == "rec-throttle-001"


# ==================================================================
# Same-person exemption (2026-06-09): a prospect's OWN prior touch
# must not block their next cadence step. Without this, every
# confirmed send froze that prospect's own DM2/DM3 for the full
# §3.8 window (14d) — structurally killing the 5-8 day DM cadence.
# ==================================================================


def _company_payload_with_person(
    last_outreach_at: str, person_record_id: str, step: str | None = "DM1",
) -> dict:
    """Company payload carrying the throttle timestamp, the
    record-reference stamp of who received that outreach, and the step
    select (None omits the step attr — legacy/malformed rows)."""
    values = {
        "last_outreach_at": [{"value": last_outreach_at}],
        "last_outreach_person_id": [{
            "target_object": "people",
            "target_record_id": person_record_id,
        }],
    }
    if step is not None:
        values["last_outreach_step"] = [{"option": {"title": step}}]
    return {"values": values}


class TestSamePersonExemption:
    def test_self_stamped_consistent_step_is_permitted(self):
        """The blocking outreach went to THIS prospect AND their own
        dm_step already accounts for it — their cadence guards own the
        pacing; the company throttle stands aside."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM1",
        )
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
        ) is True

    def test_self_stamped_desynced_step_blocks_and_audits(self):
        """F1 (PR #170 review): stamp says DM1 was confirmed-sent but the
        person's dm_step is still 0 — the person-side advance failed
        after the send. Exempting would re-queue the SAME DM to the
        same human. Block, and emit a distinct desync audit event so
        the operator reconciles instead of rediscovering a mystery
        double-DM."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM1",
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=0,
            audit_logger=audit,
        ) is False
        audit.event.assert_any_call(
            "company_throttle_desync_blocked",
            company_id="rec_co",
            person_record_id="rec_person",
            stamped_step="DM1",
            person_dm_step=0,
        )

    def test_dm2_stamp_requires_dm_step_two(self):
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM2",
        )
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=2,
        ) is True
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
        ) is False

    def test_invite_stamp_exempts_engaged_prospect(self):
        """An invite stamp + an engaged (post-accept) candidate is
        consistent: the invite was recorded, the prospect accepted, the
        DM cadence may proceed."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="Connection Request",
        )
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=0,
        ) is True

    def test_missing_step_on_stamp_blocks_and_audits_malformed(self):
        """F3: a self-stamp whose step is unreadable cannot be verified
        for consistency — fail closed AND make it observable, so a
        re-frozen cadence is distinguishable from a sibling block."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step=None,
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
            audit_logger=audit,
        ) is False
        audit.event.assert_any_call(
            "malformed_company_last_outreach_person_stamp",
            company_id="rec_co",
            person_record_id="rec_person",
            reason="unreadable_step",
            stamped_person="rec_person",
            stamped_step="",
        )

    def test_sibling_stamped_within_window_still_blocks(self):
        """ABM protection unchanged: a DIFFERENT person at the company
        was contacted within the window — blocked."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_other_person", step="DM1",
        )
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
        ) is False

    def test_no_person_record_id_keeps_blocking(self):
        """Backward compat: callers that don't pass person_record_id
        get the original (blocking) behavior."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM1",
        )
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
        ) is False

    def test_unstamped_person_id_keeps_blocking(self):
        """A company with last_outreach_at but NO person stamp (legacy
        rows pre-dating the tally writer) cannot prove the contact was
        self — fail closed, keep blocking."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload("2026-05-17T12:00:00Z")
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
        ) is False

    @pytest.mark.parametrize("bad_value", [
        [None],
        ["rec_bare_string"],
        [42],
        {"target_record_id": "rec_person"},  # bare dict, not list-wrapped
        "rec_person",
        7,
    ])
    def test_malformed_stamp_shapes_block_without_raising(self, bad_value):
        """F2: a malformed reference shape must fail CLOSED per-row, not
        raise mid-queue-loop (one bad company record must never abort
        the whole day's Part A/B)."""
        attio = MagicMock()
        attio.get_company.return_value = {"values": {
            "last_outreach_at": [{"value": "2026-05-17T12:00:00Z"}],
            "last_outreach_person_id": bad_value,
        }}
        assert company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
        ) is False

    def test_self_exemption_emits_audit_event_with_step(self):
        """The exemption must be observable (§0 #9) and carry enough
        context to distinguish a DM-cadence exemption from anything
        else (F4 review note)."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM1",
        )
        audit = MagicMock()
        company_throttle_permits(
            "rec_co", date(2026, 5, 22), attio=attio,
            person_record_id="rec_person", person_dm_step=1,
            audit_logger=audit,
        )
        audit.event.assert_any_call(
            "company_throttle_self_exempt",
            company_id="rec_co",
            person_record_id="rec_person",
            stamped_step="DM1",
            person_dm_step=1,
        )

    def test_check_company_throttle_threads_engaged_candidate(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """_check_company_throttle_or_skip must thread record_id +
        dm_step for an ENGAGED (post-accept) candidate so the DM-path
        exemption can fire."""
        from workflows.daily_check import _check_company_throttle_or_skip

        attio = MagicMock()
        attio._person_to_company = {"rec_person": "rec_co"}
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="DM1",
        )
        assert _check_company_throttle_or_skip(
            {"record_id": "rec_person", "stage": "DM1 Sent", "dm_step": 1},
            attio=attio, today=date(2026, 5, 22),
        ) is True

    def test_check_company_throttle_never_exempts_prospect_stage(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """F4: the invite path (PROSPECT stage) must NEVER self-exempt —
        a PROSPECT with a self-stamp is by definition corrupted state
        (their invite went out but the stage advance never landed);
        the 14-day quarantine is the correct outcome."""
        from workflows.daily_check import _check_company_throttle_or_skip

        attio = MagicMock()
        attio._person_to_company = {"rec_person": "rec_co"}
        attio.get_company.return_value = _company_payload_with_person(
            "2026-05-17T12:00:00Z", "rec_person", step="Connection Request",
        )
        assert _check_company_throttle_or_skip(
            {"record_id": "rec_person", "stage": "Prospect", "dm_step": 0},
            attio=attio, today=date(2026, 5, 22), dry_run=True,
        ) is False


# ==================================================================
# Sibling-invite exemption (2026-09-02): Part A kept inviting colleagues
# at the same companies, re-arming the 14-day window every day, so
# accepted prospects sat at dm_step 0 forever (2026-09-01/02: nine
# Accepted rows never received DM1). An invite to a colleague must not
# freeze a DM to a prospect whose connection already exists; a sibling
# DM-class stamp keeps blocking (§3.8 one thread per company).
# ==================================================================


def _company_payload_with_step_only(last_outreach_at: str, step: str) -> dict:
    """Legacy-shaped company row: throttle timestamp + step, no person stamp."""
    return {"values": {
        "last_outreach_at": [{"value": last_outreach_at}],
        "last_outreach_step": [{"option": {"title": step}}],
    }}


class TestSiblingInviteExemption:
    _TODAY = date(2026, 9, 2)
    _YESTERDAY = "2026-09-01T00:00:00+00:00"

    def test_accepted_prospect_permitted_when_colleague_invited_yesterday(self):
        """The incident shape: Accepted, dm_step 0, DM1 due; the company's
        stamp is yesterday's CONNECTION_SENT to a colleague."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step="CONNECTION_SENT",
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
            audit_logger=audit,
        ) is True
        audit.event.assert_called_once_with(
            "company_throttle_sibling_invite_exempt",
            company_id="rec_co",
            person_record_id="rec_accepted",
            stamped_person="rec_colleague",
            stamped_step="CONNECTION_SENT",
            person_dm_step=0,
        )

    @pytest.mark.parametrize("dm_step", [1, 2])
    def test_dm2_dm3_due_prospect_also_permitted_past_colleague_invite(self, dm_step):
        """The 2026-09-02 run also skipped two DM2s for the same reason —
        every DM step of an engaged prospect is exempt from invite stamps."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step="CONNECTION_SENT",
        )
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_engaged", person_dm_step=dm_step,
        ) is True

    @pytest.mark.parametrize("step", ["DM1", "DM2", "DM3"])
    def test_colleague_dm_stamp_still_blocks_accepted_prospect(self, step):
        """§3.8 intact: a colleague's DM within the window blocks this
        prospect's DM1 — never two DM threads at one company per window."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step=step,
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
            audit_logger=audit,
        ) is False
        # A DM-class sibling block is the designed path — no exemption
        # event and no malformed event.
        audit.event.assert_not_called()

    def test_legacy_invite_stamp_without_person_permits_engaged_row(self):
        """A CONNECTION_SENT stamp with no readable person is still an
        invite: it cannot be a same-message collision with a DM."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_step_only(
            self._YESTERDAY, "CONNECTION_SENT",
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
            audit_logger=audit,
        ) is True
        assert audit.event.call_args[1]["stamped_person"] == ""

    def test_legacy_dm_stamp_without_person_keeps_blocking(self):
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_step_only(
            self._YESTERDAY, "DM1",
        )
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
        ) is False

    def test_colleague_stamp_with_unreadable_step_blocks_and_audits(self):
        """Fail closed AND loud: without a readable step we cannot tell an
        invite from a DM, so the block stands and the operator can see why."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step=None,
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
            audit_logger=audit,
        ) is False
        audit.event.assert_called_once_with(
            "malformed_company_last_outreach_person_stamp",
            company_id="rec_co",
            person_record_id="rec_accepted",
            reason="unreadable_step",
            stamped_person="rec_colleague",
            stamped_step="",
        )

    def test_colleague_stamp_with_unknown_step_blocks_and_audits(self):
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step="EMAIL_1",
        )
        audit = MagicMock()
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
            audit_logger=audit,
        ) is False
        assert audit.event.call_args[1]["reason"] == "unknown_step"
        assert audit.event.call_args[1]["stamped_step"] == "EMAIL_1"

    def test_no_person_record_id_still_blocks_on_invite_stamp(self):
        """Invites throttle invites: a caller that threads no candidate
        identity (the invite path) gets the full block on an invite stamp."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            self._YESTERDAY, "rec_colleague", step="CONNECTION_SENT",
        )
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
        ) is False

    def test_exemption_does_not_outlive_unrelated_blocks(self):
        """Outside the window nothing changes: permitted regardless of stamp."""
        attio = MagicMock()
        attio.get_company.return_value = _company_payload_with_person(
            "2026-08-01T00:00:00+00:00", "rec_colleague", step="DM1",
        )
        assert company_throttle_permits(
            "rec_co", self._TODAY, attio=attio,
            person_record_id="rec_accepted", person_dm_step=0,
        ) is True


class TestSiblingInviteExemptionThroughCallSite:
    """`_check_company_throttle_or_skip` threads identity only for engaged
    rows — the exemption reaches the DM path and never the invite path."""

    def test_accepted_row_passes_no_queue_row_opened(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        from workflows.daily_check import _check_company_throttle_or_skip

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )
        attio = MagicMock()
        attio._person_to_company = {"rec_accepted": "rec_co"}
        attio.get_company.return_value = _company_payload_with_person(
            "2026-09-01T00:00:00+00:00", "rec_colleague", step="CONNECTION_SENT",
        )
        assert _check_company_throttle_or_skip(
            {"record_id": "rec_accepted", "stage": "Accepted", "dm_step": 0},
            attio=attio, today=date(2026, 9, 2),
        ) is True
        assert escalate_calls == []

    def test_prospect_row_invite_path_still_throttled(
        self, monkeypatch: pytest.MonkeyPatch,
    ):
        """Part A must keep spacing invites: a PROSPECT at a company that
        got an invite yesterday is still skipped with a queue row."""
        from workflows.daily_check import _check_company_throttle_or_skip

        escalate_calls: list[dict] = []
        monkeypatch.setattr(
            "workflows.daily_check.escalate",
            _make_recording_escalate(escalate_calls),
        )
        attio = MagicMock()
        attio._person_to_company = {"rec_prospect": "rec_co"}
        attio.get_company.return_value = _company_payload_with_person(
            "2026-09-01T00:00:00+00:00", "rec_colleague", step="CONNECTION_SENT",
        )
        assert _check_company_throttle_or_skip(
            {"record_id": "rec_prospect", "stage": "Prospect", "dm_step": 0},
            attio=attio, today=date(2026, 9, 2),
        ) is False
        assert [c["type"] for c in escalate_calls] == ["company_throttled"]


# ==================================================================
# Within-run same-company DM dedup: the exemption lets two engaged
# colleagues both clear an invite-stamped company at queue build (the
# tally only lands post-send), so Part B needs the guard the invite
# path already has.
# ==================================================================


class TestDedupeDmQueuesByCompany:
    def _queues(self, **rows):
        from models.campaign import MessageStep
        q = {MessageStep.DM1: [], MessageStep.DM2: [], MessageStep.DM3: []}
        for step_name, attrs_list in rows.items():
            q[MessageStep(step_name)] = list(attrs_list)
        return q

    def test_two_colleagues_same_step_keeps_first(self):
        from models.campaign import MessageStep
        from workflows.daily_check import _dedupe_dm_queues_by_company

        attio = MagicMock()
        attio._person_to_company = {"a": "co", "b": "co"}
        q = self._queues(dm1=[{"record_id": "a"}, {"record_id": "b"}])
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 1
        assert [r["record_id"] for r in q[MessageStep.DM1]] == ["a"]

    def test_dm1_outranks_dm3_outranks_dm2(self):
        from models.campaign import MessageStep
        from workflows.daily_check import _dedupe_dm_queues_by_company

        attio = MagicMock()
        attio._person_to_company = {"d1": "co", "d2": "co", "d3": "co"}
        q = self._queues(
            dm2=[{"record_id": "d2"}], dm3=[{"record_id": "d3"}], dm1=[{"record_id": "d1"}],
        )
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 2
        assert [r["record_id"] for r in q[MessageStep.DM1]] == ["d1"]
        assert q[MessageStep.DM2] == [] and q[MessageStep.DM3] == []

        attio._person_to_company = {"d2": "co", "d3": "co"}
        q = self._queues(dm2=[{"record_id": "d2"}], dm3=[{"record_id": "d3"}])
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 1
        assert [r["record_id"] for r in q[MessageStep.DM3]] == ["d3"]
        assert q[MessageStep.DM2] == []

    def test_deferral_line_names_the_person_when_cache_given(self, capsys):
        """Operators triage by name: the console line must read like the
        neighbouring skip lines, not expose a bare record id."""
        from workflows.daily_check import _dedupe_dm_queues_by_company

        attio = MagicMock()
        attio._person_to_company = {"a": "co", "b": "co"}
        cache = MagicMock()
        cache.get.side_effect = lambda rid: (
            {"a": "Ana García", "b": "Bruno Lima"}[rid], "Cemex", "", "", "",
        )
        q = self._queues(dm1=[{"record_id": "a"}, {"record_id": "b"}])
        assert _dedupe_dm_queues_by_company(q, attio=attio, cache=cache) == 1
        out = capsys.readouterr().out
        assert "Bruno Lima (Cemex)" in out
        assert "for b —" not in out

    def test_oldest_last_contact_wins_within_a_step(self):
        """Queue order is raw Attio pagination order; the colleague who has
        waited longest must be the one kept, regardless of listing order."""
        from models.campaign import MessageStep
        from workflows.daily_check import _dedupe_dm_queues_by_company

        attio = MagicMock()
        attio._person_to_company = {"newer": "co", "older": "co"}
        q = self._queues(dm1=[
            {"record_id": "newer", "last_contact_date": "2026-08-30"},
            {"record_id": "older", "last_contact_date": "2026-08-01"},
        ])
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 1
        assert [r["record_id"] for r in q[MessageStep.DM1]] == ["older"]

    def test_deferral_emits_audit_event_naming_the_kept_sibling(self):
        """§0 #9: the console line scrolls away and the summary is only a
        count, so each deferral must leave an audit record that says WHO
        was deferred and WHICH colleague held the company — a row deferred
        run after run is otherwise the same invisible freeze LUC-269 fixed."""
        from models.campaign import MessageStep
        from workflows.daily_check import _dedupe_dm_queues_by_company

        class _FakeAuditLogger:
            def __init__(self):
                self.events: list[tuple[str, dict]] = []

            def event(self, name, **kw):
                self.events.append((name, kw))

        attio = MagicMock()
        attio._person_to_company = {"a": "co", "b": "co", "c": "co", "z": "co_z"}
        audit = _FakeAuditLogger()
        q = self._queues(
            dm1=[{"record_id": "a", "last_contact_date": "2026-08-01"}],
            dm3=[{"record_id": "b"}],
            dm2=[{"record_id": "c"}, {"record_id": "z"}],
        )
        assert _dedupe_dm_queues_by_company(
            q, attio=attio, audit_logger=audit,  # type: ignore[arg-type]
        ) == 2
        assert [n for n, _ in audit.events] == [
            "same_company_run_deferred", "same_company_run_deferred",
        ]
        assert audit.events[0][1] == {
            "record_id": "b", "company_id": "co", "step": "dm3",
            "kept_record_id": "a", "kept_step": "dm1",
        }
        assert audit.events[1][1]["record_id"] == "c"
        assert audit.events[1][1]["kept_record_id"] == "a"
        # "z" is at a different company: kept, no event.
        assert [r["record_id"] for r in q[MessageStep.DM2]] == ["z"]

    def test_no_audit_logger_and_no_deferral_emit_nothing(self):
        from workflows.daily_check import _dedupe_dm_queues_by_company

        class _FakeAuditLogger:
            def __init__(self):
                self.events: list = []

            def event(self, name, **kw):
                self.events.append((name, kw))

        attio = MagicMock()
        attio._person_to_company = {"a": "co", "b": "co", "x": "co_x"}
        audit = _FakeAuditLogger()
        q = self._queues(dm1=[{"record_id": "a"}], dm2=[{"record_id": "x"}])
        assert _dedupe_dm_queues_by_company(
            q, attio=attio, audit_logger=audit,  # type: ignore[arg-type]
        ) == 0
        assert audit.events == []
        # audit_logger=None with a real deferral: still defers, never raises.
        q = self._queues(dm1=[{"record_id": "a"}, {"record_id": "b"}])
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 1

    def test_no_company_rows_and_distinct_companies_untouched(self):
        from models.campaign import MessageStep
        from workflows.daily_check import _dedupe_dm_queues_by_company

        attio = MagicMock()
        attio._person_to_company = {"x": "co_x", "y": "co_y"}  # "n1"/"n2" unlinked
        q = self._queues(
            dm1=[{"record_id": "x"}, {"record_id": "n1"}, {"record_id": "n2"}],
            dm2=[{"record_id": "y"}],
        )
        assert _dedupe_dm_queues_by_company(q, attio=attio) == 0
        assert len(q[MessageStep.DM1]) == 3 and len(q[MessageStep.DM2]) == 1


class TestIncidentReplayThroughDmSequencing:
    """End-to-end through `run_dm_sequencing` (dry-run, zero writes): an
    Accepted prospect whose company got a colleague's invite yesterday is
    queued for DM1; two such colleagues yield one DM1 and one deferral."""

    _ENV = TestDryRunThreadingThroughDmSequencing._ENV

    def _run(self, record_ids: list[str], escalate_calls: list[dict]):
        import os
        from datetime import timedelta
        from unittest.mock import patch

        from tests.test_integration import _attio_with_full_schema, _make_attio_entry
        from workflows.daily_check import run_dm_sequencing
        from workflows.daily_run import DailyRun as _DailyRun

        today = date.today()
        two_days_ago = (today - timedelta(days=2)).isoformat()
        yesterday = (today - timedelta(days=1)).isoformat() + "T00:00:00+00:00"
        entries = [
            _make_attio_entry(
                entry_id=f"entry-{rid}", record_id=rid, stage="Accepted",
                quality_score=70, last_contact_date=two_days_ago, dm_step=0,
            )
            for rid in record_ids
        ]

        attio = _attio_with_full_schema()
        attio.query_list_entries.return_value = entries
        attio._person_to_company = {rid: "rec_co" for rid in record_ids}
        attio.get_company.return_value = _company_payload_with_person(
            yesterday, "rec_colleague_invited", step="CONNECTION_SENT",
        )
        _fake_dr = MagicMock(spec=_DailyRun)
        _fake_dr.remaining.return_value = 30

        with patch.dict(os.environ, self._ENV), \
                patch(
                    "workflows.daily_check.escalate",
                    _make_recording_escalate(escalate_calls),
                ), \
                patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
                patch("workflows.daily_check.can_send_messages", return_value=True), \
                patch(
                    "workflows.daily_check.write_prospects_to_sheet",
                    return_value="https://docs.google.com/spreadsheets/d/fake",
                ):
            mock_cache_get.side_effect = lambda rid: (
                f"Person {rid}", "Cemex",
                f"https://linkedin.com/in/{rid}", "", "VP Operations",
            )
            return run_dm_sequencing(
                attio=attio, pb=MagicMock(), message_sender_id="agent-msg",
                daily_run=_fake_dr, dry_run=True, auto_confirm=True,
            )

    def test_accepted_prospect_gets_dm1_despite_colleague_invite(self):
        escalate_calls: list[dict] = []
        result = self._run(["rec-accepted-001"], escalate_calls)
        assert result["skipped_company_throttled"] == 0
        assert result["skipped_same_company_run"] == 0
        assert result["dry_run"]["dm1"] == 1
        assert [c for c in escalate_calls if c["type"] == "company_throttled"] == []

    def test_two_accepted_colleagues_yield_one_dm1_this_run(self):
        escalate_calls: list[dict] = []
        result = self._run(["rec-accepted-001", "rec-accepted-002"], escalate_calls)
        assert result["skipped_company_throttled"] == 0
        assert result["skipped_same_company_run"] == 1
        assert result["dry_run"]["dm1"] == 1


@pytest.fixture(autouse=True)
def _fresh_stage_unit_boundary():
    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
