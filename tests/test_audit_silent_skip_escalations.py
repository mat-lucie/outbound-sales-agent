"""TDD tests for fix/audit-silent-skip-escalations.

Covers:
  L1-4  missing LinkedIn URL → escalate missing_linkedin_url
  L1-5  missing quality_score → escalate missing_quality_score;
         score < 60 → visible in skip-counts output
  L1-6  ACCEPTED with null last_contact_date → escalate accepted_missing_last_contact_date
  L1-2  stale CONNECTION_SENT sweep → aggregated stale_connection_sent escalation
  L3-3  run_connection_requests honest send counts (pb_queued + attio_updated keys)
  L3-7  run_dm_sequencing honest send counts (confirmed sent vs queued)
  L6-7  escalations_fired in DailyRunMetrics + render output
  L1-7  throttle.py stale docstring / idempotency key → updated to 14d
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tests.fakes import fake_daily_run

# ==================================================================
# L1-7 throttle stale text/key
# ==================================================================


class TestThrottleStaleDocs:
    """throttle.ensure_throttle_policy_decision_opened idempotency_key
    must be 'default-14d-v2' (not the old 'default-30d-v1') and the
    question text must reference 14 days (not 30)."""

    def test_idempotency_key_is_14d_v2(self, monkeypatch: pytest.MonkeyPatch):
        from workflows.throttle import ensure_throttle_policy_decision_opened

        captured: dict = {}

        def _fake_escalate(**kwargs):
            captured.update(kwargs)
            return {"id": "row"}

        monkeypatch.setattr("workflows.throttle.escalate", _fake_escalate)
        ensure_throttle_policy_decision_opened(MagicMock())

        assert captured["idempotency_key"] == "default-14d-v2", (
            "idempotency_key must be 'default-14d-v2' after the 30→14 window change"
        )

    def test_question_text_mentions_14(self, monkeypatch: pytest.MonkeyPatch):
        from workflows.throttle import ensure_throttle_policy_decision_opened

        captured: dict = {}

        def _fake_escalate(**kwargs):
            captured.update(kwargs)
            return {"id": "row"}

        monkeypatch.setattr("workflows.throttle.escalate", _fake_escalate)
        ensure_throttle_policy_decision_opened(MagicMock())

        question = captured["payload"]["question"]
        assert "14" in question, (
            f"Question should reference 14-day window, got: {question!r}"
        )
        # Must not describe the old default
        assert "Default 30" not in question, (
            "Question text still says 'Default 30'; update to 'Default 14'"
        )

    def test_old_key_not_used(self, monkeypatch: pytest.MonkeyPatch):
        from workflows.throttle import ensure_throttle_policy_decision_opened

        captured: dict = {}

        def _fake_escalate(**kwargs):
            captured.update(kwargs)
            return {"id": "row"}

        monkeypatch.setattr("workflows.throttle.escalate", _fake_escalate)
        ensure_throttle_policy_decision_opened(MagicMock())

        assert captured["idempotency_key"] != "default-30d-v1", (
            "Old key 'default-30d-v1' is still in use; bump to 'default-14d-v2'"
        )


# ==================================================================
# L6-7 escalations_fired in DailyRunMetrics
# ==================================================================


class TestDailyRunMetricsEscalations:
    """DailyRunMetrics must have an escalations_fired counter and a
    per-type dict; render() must surface them when non-zero."""

    def test_escalations_fired_starts_at_zero(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        assert m.escalations_fired == 0

    def test_escalations_by_type_starts_empty(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        assert m.escalations_by_type == {}

    def test_bump_escalation_increments_counter(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.bump_escalation("missing_linkedin_url")
        assert m.escalations_fired == 1
        assert m.escalations_by_type == {"missing_linkedin_url": 1}

    def test_bump_escalation_accumulates(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.bump_escalation("missing_linkedin_url")
        m.bump_escalation("missing_linkedin_url")
        m.bump_escalation("missing_quality_score")
        assert m.escalations_fired == 3
        assert m.escalations_by_type["missing_linkedin_url"] == 2
        assert m.escalations_by_type["missing_quality_score"] == 1

    def test_render_shows_escalations_when_nonzero(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.bump_escalation("missing_linkedin_url")
        m.bump_escalation("missing_linkedin_url")
        m.bump_escalation("degree_unknown")
        rendered = m.render()
        assert "escalations:" in rendered or "escalations_fired" in rendered
        assert "2" in rendered  # two missing_linkedin_url
        assert "missing_linkedin_url" in rendered
        assert "degree_unknown" in rendered

    def test_render_omits_escalations_when_zero(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        rendered = m.render()
        assert "escalation" not in rendered.lower()

    def test_to_dict_includes_escalation_keys(self):
        from workflows.metrics import DailyRunMetrics

        m = DailyRunMetrics()
        m.bump_escalation("foo")
        d = m.to_dict()
        assert "escalations_fired" in d
        assert "escalations_by_type" in d


# ==================================================================
# L1-4 missing LinkedIn URL escalation
# ==================================================================


class TestMissingLinkedinUrlEscalation:
    """_build_invite_send_data must escalate missing_linkedin_url (not
    just silently skip) when cache.get returns no URL."""

    def _make_prospect(self, record_id: str = "rec_abc", stage: str | None = None):
        from models.pipeline import PipelineStage
        return {
            "record_id": record_id,
            "entry_id": "ent_abc",
            "stage": stage or PipelineStage.PROSPECT.value,
            "quality_score": 75,
            "persona": "operations_leaders",
            "language": "es",
            "last_contact_date": None,
            "invite_eligible_after": None,
        }

    def test_missing_url_opens_escalation_queue_row(self, monkeypatch):
        from workflows.daily_check import _build_invite_send_data

        escalate_calls: list[dict] = []

        def _fake_escalate(**kw):
            escalate_calls.append(kw)
            return {"id": "row"}

        monkeypatch.setattr("workflows.daily_check.escalate", _fake_escalate)

        cache = MagicMock()
        # name, company, linkedin_url (empty!), industry_raw, title
        cache.get.return_value = ("Alice", "Acme", "", "food", "VP Ops")

        attio = MagicMock()
        # company_throttle_permits should not be called (URL check is first)
        attio.get_company.return_value = None

        prospect = self._make_prospect("rec_no_url")
        to_send, counts = _build_invite_send_data(
            [prospect],
            target=5,
            attio=attio,
            cache=cache,
            today=date.today(),
            audit_logger=None,
            dry_run=False,
        )

        assert counts["missing_url"] == 1
        assert len(to_send) == 0

        url_escalations = [c for c in escalate_calls if c.get("type") == "missing_linkedin_url"]
        assert url_escalations, (
            "Expected a missing_linkedin_url queue row; none opened. "
            "Add escalate(type='missing_linkedin_url', ...) in _build_invite_send_data."
        )
        row = url_escalations[0]
        assert "rec_no_url" in row["idempotency_key"]
        assert row["payload"]["record_id"] == "rec_no_url"

    def test_missing_url_escalation_is_idempotent_per_record(self, monkeypatch):
        """idempotency_key must include the record_id so the same record
        does not open duplicate rows on repeated daily runs."""
        from workflows.daily_check import _build_invite_send_data

        captured_keys: list[str] = []

        def _fake_escalate(**kw):
            if kw.get("type") == "missing_linkedin_url":
                captured_keys.append(kw["idempotency_key"])
            return {"id": "row"}

        monkeypatch.setattr("workflows.daily_check.escalate", _fake_escalate)

        cache = MagicMock()
        cache.get.return_value = ("Alice", "Acme", "", "food", "VP Ops")
        attio = MagicMock()

        prospect = self._make_prospect("rec_idem")
        _build_invite_send_data(
            [prospect], target=5, attio=attio, cache=cache,
            today=date.today(), audit_logger=None, dry_run=False,
        )
        _build_invite_send_data(
            [prospect], target=5, attio=attio, cache=cache,
            today=date.today(), audit_logger=None, dry_run=False,
        )

        assert len(captured_keys) == 2
        assert captured_keys[0] == captured_keys[1], (
            "idempotency_key must be stable across runs so escalate()'s "
            "dedup prevents duplicate queue rows"
        )


# ==================================================================
# L1-5 missing quality_score escalation + low-score visibility
# ==================================================================


class TestMissingQualityScoreEscalation:
    """run_connection_requests must escalate missing_quality_score when
    quality_score is None (data-pipeline gap), not silently skip.
    score < 60 (legitimate filter) must remain a silent skip but appear
    in structured skip-counts so it shows in the run summary."""

    def _make_minimal_entry(self, record_id: str, score):
        from models.pipeline import PipelineStage
        return {
            "record_id": record_id,
            "entry_id": "ent_" + record_id,
            "stage": PipelineStage.PROSPECT.value,  # "Prospect"
            "quality_score": score,
            "persona": "operations_leaders",
            "language": "es",
            "last_contact_date": None,
            "invite_eligible_after": None,
            "experiment_id": None,
            "experiment_id_frozen_at": None,
        }

    def test_none_score_opens_missing_quality_score_escalation(self, monkeypatch):
        from workflows import daily_check

        escalate_calls: list[dict] = []

        def _fake_escalate(**kw):
            escalate_calls.append(kw)
            return {"id": "row"}

        monkeypatch.setattr(daily_check, "escalate", _fake_escalate)
        monkeypatch.setattr(daily_check, "ensure_throttle_policy_decision_opened", MagicMock())
        monkeypatch.setattr(daily_check, "can_send_connections", lambda n: True)
        monkeypatch.setattr(daily_check, "get_remaining", lambda: {"connections": 25, "messages": 30, "visits": 50})
        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: [
            self._make_minimal_entry("rec_no_score", None),
        ])
        monkeypatch.setattr(daily_check, "_build_invite_send_data", lambda *a, **kw: ([], {"company_throttled": 0, "same_company_run": 0, "missing_language": 0, "missing_copy": 0, "missing_url": 0}))
        monkeypatch.setattr(daily_check, "_dedupe_by_linkedin_url", lambda d: (d, []))
        monkeypatch.setattr(daily_check, "_assert_no_unresolved_placeholders", MagicMock())
        monkeypatch.setattr(daily_check, "_pre_invite_degree_check", lambda *a, **kw: ([], []))

        attio = MagicMock()
        pb = MagicMock()
        cache = MagicMock()

        daily_check.run_connection_requests(
            attio, pb, "phantom_id",
            batch_size=5, dry_run=True, auto_confirm=True,
            cache=cache, daily_run=fake_daily_run(),
        )

        none_score_escalations = [
            c for c in escalate_calls if c.get("type") == "missing_quality_score"
        ]
        assert none_score_escalations, (
            "A PROSPECT with quality_score=None must open a missing_quality_score "
            "queue row — it signals the weekly pipeline never stamped this record."
        )
        row = none_score_escalations[0]
        assert "rec_no_score" in row["idempotency_key"]
        assert row["payload"]["record_id"] == "rec_no_score"

    def test_low_score_appears_in_structured_output(self, monkeypatch):
        """score < 60 stays a silent skip but must be counted in the
        return dict so the run summary can report it."""
        from workflows import daily_check

        # We don't need actual run mechanics here — just verify the
        # return dict from run_connection_requests includes
        # 'skipped_low_score' after filtering.
        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
        monkeypatch.setattr(daily_check, "ensure_throttle_policy_decision_opened", MagicMock())
        monkeypatch.setattr(daily_check, "can_send_connections", lambda n: True)
        monkeypatch.setattr(daily_check, "get_remaining", lambda: {"connections": 25, "messages": 30, "visits": 50})
        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: [
            self._make_minimal_entry("rec_low_score", 50),  # below 60
        ])
        monkeypatch.setattr(daily_check, "_build_invite_send_data", lambda *a, **kw: ([], {"company_throttled": 0, "same_company_run": 0, "missing_language": 0, "missing_copy": 0, "missing_url": 0}))
        monkeypatch.setattr(daily_check, "_dedupe_by_linkedin_url", lambda d: (d, []))
        monkeypatch.setattr(daily_check, "_assert_no_unresolved_placeholders", MagicMock())
        monkeypatch.setattr(daily_check, "_pre_invite_degree_check", lambda *a, **kw: ([], []))

        result = daily_check.run_connection_requests(
            MagicMock(), MagicMock(), "phantom_id",
            batch_size=5, dry_run=True, auto_confirm=True,
            daily_run=fake_daily_run(),
        )

        assert "skipped_low_score" in result, (
            "run_connection_requests must return skipped_low_score count "
            "so operators can see how many PROSPECTs were filtered on quality."
        )
        assert result["skipped_low_score"] >= 1


# ==================================================================
# L1-6 ACCEPTED with null last_contact_date escalation
# ==================================================================


class TestAcceptedMissingLastContactDate:
    """run_dm_sequencing must escalate accepted_missing_last_contact_date
    when stage=ACCEPTED but last_contact_date is null/empty.
    Must NOT auto-backfill the date."""

    def _make_accepted_entry(self, record_id: str, last_contact_date=None):
        from models.pipeline import PipelineStage
        return {
            "record_id": record_id,
            "entry_id": "ent_" + record_id,
            "stage": PipelineStage.ACCEPTED.value,
            "quality_score": 80,
            "persona": "operations_leaders",
            "language": "es",
            "last_contact_date": last_contact_date,
            "invite_eligible_after": None,
            "dm_step": 0,
            "experiment_id": None,
            "experiment_id_frozen_at": None,
            "next_eligible_send_date": None,
        }

    def test_accepted_null_last_contact_date_opens_escalation(self, monkeypatch):
        from workflows import daily_check

        escalate_calls: list[dict] = []

        def _fake_escalate(**kw):
            escalate_calls.append(kw)
            return {"id": "row"}

        monkeypatch.setattr(daily_check, "escalate", _fake_escalate)

        # Patch all the parts of run_dm_sequencing we don't need
        daily_run_mock = MagicMock()
        daily_run_mock.remaining.return_value = 30

        # Fork: run_dm_sequencing reads via _get_all_entries_with_raw, which
        # returns (raw_snapshot, parsed) — patch that, not _get_all_entries_parsed.
        _accepted = [self._make_accepted_entry("rec_accepted_no_lcd")]
        monkeypatch.setattr(daily_check, "_get_all_entries_with_raw", lambda _: (None, _accepted))
        monkeypatch.setattr(daily_check, "can_send_messages", lambda n: True)
        cache = MagicMock()
        # cache.get must return a 5-tuple (name, company, url, industry, title)
        cache.get.return_value = ("Alice", "Acme", "https://linkedin.com/in/alice", "food", "VP")
        # is_send_eligible must pass
        monkeypatch.setattr(daily_check, "is_send_eligible", lambda attrs: True)
        # Fork preflight/sweep guards not present upstream: neutralize so these
        # tests isolate the escalation logic (they pass MagicMock attio + no
        # ATTIO_LIST_ID, which the real DM-writer schema preflight rejects).
        monkeypatch.setattr(daily_check, "assert_dm_writer_schema", lambda *a, **kw: None)
        monkeypatch.setattr(daily_check, "run_company_tally_consistency_sweep", lambda *a, **kw: None)

        attio = MagicMock()

        daily_check.run_dm_sequencing(
            attio, MagicMock(), "msg_sender_id", daily_run_mock,
            dry_run=True, auto_confirm=True, cache=cache,
        )

        null_lcd_escalations = [
            c for c in escalate_calls
            if c.get("type") == "accepted_missing_last_contact_date"
        ]
        assert null_lcd_escalations, (
            "An ACCEPTED row with null last_contact_date can never receive DM1. "
            "Must open accepted_missing_last_contact_date queue row."
        )
        row = null_lcd_escalations[0]
        assert "rec_accepted_no_lcd" in row["idempotency_key"]
        assert row["payload"]["record_id"] == "rec_accepted_no_lcd"

    def test_accepted_null_lcd_does_not_backfill_date(self, monkeypatch):
        """No Attio writes must happen for the backfill case."""
        from workflows import daily_check

        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))

        daily_run_mock = MagicMock()
        daily_run_mock.remaining.return_value = 30

        _accepted = [self._make_accepted_entry("rec_no_backfill")]
        monkeypatch.setattr(daily_check, "_get_all_entries_with_raw", lambda _: (None, _accepted))
        monkeypatch.setattr(daily_check, "can_send_messages", lambda n: True)
        monkeypatch.setattr(daily_check, "is_send_eligible", lambda attrs: True)
        # Fork preflight/sweep guards not present upstream: neutralize so these
        # tests isolate the escalation logic (they pass MagicMock attio + no
        # ATTIO_LIST_ID, which the real DM-writer schema preflight rejects).
        monkeypatch.setattr(daily_check, "assert_dm_writer_schema", lambda *a, **kw: None)
        monkeypatch.setattr(daily_check, "run_company_tally_consistency_sweep", lambda *a, **kw: None)

        attio = MagicMock()
        cache = MagicMock()
        cache.get.return_value = ("Alice", "Acme", "https://linkedin.com/in/alice", "food", "VP")

        daily_check.run_dm_sequencing(
            attio, MagicMock(), "msg_sender_id", daily_run_mock,
            dry_run=True, auto_confirm=True, cache=cache,
        )

        # No Attio list entry writes should happen for the null-LCD row
        write_calls = [
            c for c in attio.mock_calls
            if "update" in str(c).lower() or "patch" in str(c).lower()
        ]
        # The key check: attio.update_list_entry or similar must NOT be
        # called with last_contact_date for the accepted-null-lcd row.
        for call_args in attio.method_calls:
            call_str = str(call_args)
            if "last_contact_date" in call_str and "rec_no_backfill" in call_str:
                pytest.fail(
                    "run_dm_sequencing must NOT auto-backfill last_contact_date "
                    f"for ACCEPTED rows with null LCD. Unexpected call: {call_str}"
                )


# ==================================================================
# L1-2 stale CONNECTION_SENT sweep
# ==================================================================


class TestStaleConnectionSentSweep:
    """detect_accepted_connections must count CONNECTION_SENT rows older
    than STALE_CONNECTION_SENT_ESCALATE_DAYS and emit a single aggregated
    stale_connection_sent queue row per day when the count > 0."""

    def test_stale_connection_sent_constant_defined(self):
        from workflows.daily_check import STALE_CONNECTION_SENT_ESCALATE_DAYS

        assert isinstance(STALE_CONNECTION_SENT_ESCALATE_DAYS, int)
        assert STALE_CONNECTION_SENT_ESCALATE_DAYS >= 30, (
            "STALE_CONNECTION_SENT_ESCALATE_DAYS should be >= 30 "
            "(the spec says 45)"
        )

    def test_stale_rows_open_aggregated_escalation(self, monkeypatch):
        """When CONNECTION_SENT rows older than STALE_CONNECTION_SENT_ESCALATE_DAYS
        exist, detect_accepted_connections opens ONE stale_connection_sent
        queue row for that day (not one per record)."""
        from workflows import daily_check

        escalate_calls: list[dict] = []

        def _fake_escalate(**kw):
            escalate_calls.append(kw)
            return {"id": "row"}

        monkeypatch.setattr(daily_check, "escalate", _fake_escalate)

        from models.pipeline import PipelineStage
        stale_days = daily_check.STALE_CONNECTION_SENT_ESCALATE_DAYS
        very_old = (date.today() - timedelta(days=stale_days + 10)).isoformat()

        stale_entries = [
            {
                "record_id": f"rec_stale_{i}",
                "entry_id": f"ent_stale_{i}",
                "stage": PipelineStage.CONNECTION_SENT.value,
                "last_contact_date": very_old,
                "quality_score": 70,
                "persona": "operations_leaders",
                "language": "es",
                "invite_eligible_after": None,
                "dm_step": 0,
                "experiment_id": None,
                "experiment_id_frozen_at": None,
            }
            for i in range(3)
        ]

        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: stale_entries)

        cache = MagicMock()
        cache.get.return_value = ("Alice", "Acme", "https://linkedin.com/in/alice", "food", "VP")

        # Patch out the PB scrape path — we just want the stale sweep
        monkeypatch.setattr(daily_check, "recheck_cache", MagicMock(
            partition=lambda urls: ({}, list(urls)),
            RECHECK_TTL_DAYS=7,
        ))
        monkeypatch.setattr(daily_check, "_resolve_degree_check_backend", MagicMock(return_value="regular"))

        attio = MagicMock()
        pb = MagicMock()
        # Make PB return immediately with empty result so we don't need real launch
        pb.launch_agent.return_value = MagicMock(container_id="cid")
        pb.wait_for_completion.return_value = MagicMock(status="finished")
        pb.download_result_csv.return_value = ""

        daily_check.detect_accepted_connections(attio, pb, "scraper_id", cache=cache)

        stale_escalations = [
            c for c in escalate_calls
            if c.get("type") == "stale_connection_sent"
        ]
        assert stale_escalations, (
            "detect_accepted_connections must emit a stale_connection_sent "
            "queue row when CONNECTION_SENT rows older than "
            f"STALE_CONNECTION_SENT_ESCALATE_DAYS={stale_days} exist."
        )
        # Must be aggregated (one row per day, not per record)
        todays_key = f"stale_cs|{date.today().isoformat()}"
        assert stale_escalations[0]["idempotency_key"] == todays_key, (
            f"Expected idempotency_key={todays_key!r}, "
            f"got {stale_escalations[0]['idempotency_key']!r}"
        )
        payload = stale_escalations[0]["payload"]
        assert payload["count"] == 3
        assert "record_ids" in payload

    def test_no_stale_rows_no_escalation(self, monkeypatch):
        """When all CONNECTION_SENT rows are recent, no stale_connection_sent
        queue row is opened."""
        from workflows import daily_check

        escalate_calls: list[dict] = []

        def _fake_escalate(**kw):
            escalate_calls.append(kw)
            return {"id": "row"}

        monkeypatch.setattr(daily_check, "escalate", _fake_escalate)

        from models.pipeline import PipelineStage
        recent = (date.today() - timedelta(days=5)).isoformat()
        recent_entries = [
            {
                "record_id": "rec_recent",
                "entry_id": "ent_recent",
                "stage": PipelineStage.CONNECTION_SENT.value,
                "last_contact_date": recent,
                "quality_score": 70,
                "persona": "operations_leaders",
                "language": "es",
                "invite_eligible_after": None,
                "dm_step": 0,
                "experiment_id": None,
                "experiment_id_frozen_at": None,
            }
        ]

        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: recent_entries)

        cache = MagicMock()
        cache.get.return_value = ("Alice", "Acme", "https://linkedin.com/in/alice", "food", "VP")

        monkeypatch.setattr(daily_check, "recheck_cache", MagicMock(
            partition=lambda urls: ({}, list(urls)),
            RECHECK_TTL_DAYS=7,
        ))
        monkeypatch.setattr(daily_check, "_resolve_degree_check_backend", MagicMock(return_value="regular"))
        monkeypatch.setattr(daily_check, "write_prospects_to_sheet", lambda *a, **kw: "sheet_url")

        attio = MagicMock()
        pb = MagicMock()
        pb.launch_agent.return_value = MagicMock(container_id="cid")
        pb.wait_for_completion.return_value = MagicMock(status="finished")
        pb.download_result_csv.return_value = ""

        daily_check.detect_accepted_connections(attio, pb, "scraper_id", cache=cache)

        stale_escalations = [
            c for c in escalate_calls
            if c.get("type") == "stale_connection_sent"
        ]
        assert not stale_escalations


# ==================================================================
# L3-3 honest send counts — run_connection_requests
# ==================================================================


class TestHonestInviteSendCounts:
    """run_connection_requests must return pb_queued (prepared count)
    and attio_updated (Attio-confirmed count) alongside 'sent', so
    operators reading the summary can see discrepancies."""

    def test_return_dict_has_pb_queued_key(self, monkeypatch):
        from workflows import daily_check

        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
        monkeypatch.setattr(daily_check, "ensure_throttle_policy_decision_opened", MagicMock())
        monkeypatch.setattr(daily_check, "can_send_connections", lambda n: False)  # daily limit

        result = daily_check.run_connection_requests(
            MagicMock(), MagicMock(), "pid",
            batch_size=5, dry_run=False, auto_confirm=True,
            daily_run=fake_daily_run(),
        )

        # When daily limit reached the short-circuit fires; ensure the
        # honest-count keys exist in the normal path by testing the
        # dry_run path instead.
        # The actual test is below — this one just confirms the code path exists.
        assert isinstance(result, dict)

    def test_dry_run_result_has_pb_queued_key(self, monkeypatch):
        from workflows import daily_check

        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
        monkeypatch.setattr(daily_check, "ensure_throttle_policy_decision_opened", MagicMock())
        monkeypatch.setattr(daily_check, "can_send_connections", lambda n: True)
        monkeypatch.setattr(daily_check, "get_remaining", lambda: {"connections": 25, "messages": 30, "visits": 50})
        monkeypatch.setattr(daily_check, "_get_all_entries_parsed", lambda _: [])
        monkeypatch.setattr(daily_check, "_build_invite_send_data", lambda *a, **kw: ([], {"company_throttled": 0, "same_company_run": 0, "missing_language": 0, "missing_copy": 0, "missing_url": 0}))
        monkeypatch.setattr(daily_check, "_dedupe_by_linkedin_url", lambda d: (d, []))
        monkeypatch.setattr(daily_check, "_assert_no_unresolved_placeholders", MagicMock())
        monkeypatch.setattr(daily_check, "_pre_invite_degree_check", lambda *a, **kw: ([], []))

        result = daily_check.run_connection_requests(
            MagicMock(), MagicMock(), "pid",
            batch_size=5, dry_run=True, auto_confirm=True,
            daily_run=fake_daily_run(),
        )

        assert "pb_queued" in result, (
            "run_connection_requests must return 'pb_queued' (the number of "
            "rows prepared for PB) so operators can distinguish prep from confirmed."
        )


# ==================================================================
# L3-7 honest send counts — run_dm_sequencing
# ==================================================================


class TestHonestDmSendCounts:
    """run_dm_sequencing must set results[step.value] to confirmed
    sent count (outcome.sent_count), not the queued row count.
    A separate 'queued' key must carry the pre-PB count."""

    def test_results_dm_key_is_confirmed_not_queued(self, monkeypatch):
        """When PB confirms 5 of 7 prepared DMs, results['dm1'] must be
        5 (confirmed), not 7 (queued). A 'dm1_queued' key carries 7."""
        from models.pipeline import PipelineStage
        from workflows import daily_check

        # Build a minimal ACCEPTED entry with a valid last_contact_date
        # so DM1 is due.
        last_contact = (date.today() - timedelta(days=3)).isoformat()
        accepted_entries = [
            {
                "record_id": f"rec_dm1_{i}",
                "entry_id": f"ent_dm1_{i}",
                "stage": PipelineStage.ACCEPTED.value,
                "last_contact_date": last_contact,
                "quality_score": 75,
                "persona": "operations_leaders",
                "language": "es",
                "invite_eligible_after": None,
                "dm_step": 0,
                "experiment_id": None,
                "experiment_id_frozen_at": None,
                "next_eligible_send_date": None,
            }
            for i in range(7)
        ]

        monkeypatch.setattr(daily_check, "escalate", MagicMock(return_value={"id": "r"}))
        monkeypatch.setattr(daily_check, "_get_all_entries_with_raw", lambda _: (None, accepted_entries))
        monkeypatch.setattr(daily_check, "can_send_messages", lambda n: True)
        monkeypatch.setattr(daily_check, "is_send_eligible", lambda attrs: True)
        # Fork preflight/sweep guards not present upstream: neutralize so these
        # tests isolate the escalation logic (they pass MagicMock attio + no
        # ATTIO_LIST_ID, which the real DM-writer schema preflight rejects).
        monkeypatch.setattr(daily_check, "assert_dm_writer_schema", lambda *a, **kw: None)
        monkeypatch.setattr(daily_check, "run_company_tally_consistency_sweep", lambda *a, **kw: None)

        cache = MagicMock()
        # Each record_id gets a unique URL so dedupe doesn't collapse them.
        def _cache_side_effect(record_id):
            idx = record_id.split("_")[-1]  # "rec_dm1_0" → "0"
            return (f"Alice{idx}", "Acme", f"https://linkedin.com/in/alice{idx}", "food", "VP")
        cache.get.side_effect = _cache_side_effect

        # Patch _is_blocked_by_stored_floor so nothing is blocked
        monkeypatch.setattr(daily_check, "_is_blocked_by_stored_floor", lambda *a, **kw: False)
        # Patch company throttle
        monkeypatch.setattr(daily_check, "_check_company_throttle_or_skip", lambda *a, **kw: True)
        # Patch placeholder guard
        monkeypatch.setattr(daily_check, "_assert_no_unresolved_placeholders", MagicMock())
        # Patch write_prospects_to_sheet
        monkeypatch.setattr(daily_check, "write_prospects_to_sheet", lambda rows: "sheet_url")

        # Build a fake PB client where 5 of 7 URLs are confirmed sent
        pb = MagicMock()
        launch_obj = MagicMock(container_id="cid_dm1")
        pb.launch_agent.return_value = launch_obj
        pb.wait_for_completion.return_value = MagicMock(status="finished")

        # 5 confirmed sent, 2 unreported (alice0..alice4 confirmed; alice5, alice6 not)
        confirmed_urls = {f"https://linkedin.com/in/alice{i}" for i in range(5)}

        from clients.pb_envelope import SendOutcome
        fake_outcome = SendOutcome(
            csv_status="Message sent",
            sent_count=5,
            requested_count=7,
            sent_urls=confirmed_urls,
            skipped_urls=set(),
            already_processed=False,
            drift_skipped_reason=None,
            container_id="cid_dm1",
            next_day_drift_key="drift_key_dm1",
        )
        # Sender seam: the CSV parse now lives in clients.sender (PBSender
        # owns the transport hop), so the patch binds there.
        import clients.sender as sender_mod
        monkeypatch.setattr(sender_mod, "parse_send_outcome", lambda **kw: fake_outcome)
        monkeypatch.setattr(daily_check, "should_advance_batch", lambda *a, **kw: True)
        monkeypatch.setattr(daily_check, "get_current_experiment_id", lambda: None)

        # Patch resolve_language and get_message to return valid values
        monkeypatch.setattr(daily_check, "resolve_language", lambda *a, **kw: "es")
        from unittest.mock import MagicMock as MM
        fake_tpl = MM()
        monkeypatch.setattr(daily_check, "get_message", lambda *a, **kw: fake_tpl)
        monkeypatch.setattr(daily_check, "get_industry_label", lambda *a, **kw: "Manufacturing")
        monkeypatch.setattr(daily_check, "personalize", lambda tpl, *a, **kw: "Hello Alice")

        daily_run_mock = MagicMock()
        daily_run_mock.remaining.return_value = 30
        daily_run_mock.reserve_send.return_value = "lease_tok"
        daily_run_mock.confirm_lease.return_value = None
        daily_run_mock.release_lease.return_value = None
        monkeypatch.setattr(daily_check, "_attio_advance_with_escalation", lambda **kw: True)
        # Prevent company-corruption guard from skipping all rows
        MagicMock_attio = MagicMock()
        MagicMock_attio._person_to_company = {}
        MagicMock_attio.is_person_company_corrupted.return_value = False
        monkeypatch.setattr(daily_check, "_write_company_throttle_tally", MagicMock())
        monkeypatch.setattr(daily_check, "emit_pb_inmail_dead_end", MagicMock())
        monkeypatch.setattr(daily_check, "emit_pb_silent_no_op", MagicMock())
        pb.download_result_csv.return_value = ""

        result = daily_check.run_dm_sequencing(
            MagicMock_attio, pb, "msg_sender_id", daily_run_mock,
            dry_run=False, auto_confirm=True, cache=cache,
        )

        # The confirmed count must be 5, not 7
        assert result.get("dm1") == 5, (
            f"results['dm1'] should be confirmed sent count (5), got {result.get('dm1')}. "
            "Set results[step.value] = outcome.sent_count (confirmed), not len(rows) (queued)."
        )
        # A queued key must exist carrying the prep count
        assert "dm1_queued" in result or result.get("dm1_queued") is not None or (
            # Alternative: check any key that carries the 7 count
            any(v == 7 for k, v in result.items() if "queue" in k.lower() or "prep" in k.lower())
        ), (
            "run_dm_sequencing must return a 'dm1_queued' (or similar) key "
            "carrying the queued/prepared count (7) distinct from the confirmed sent (5)."
        )


# ==================================================================
# New escalation types must be registered in ESCALATION_TYPES
# ==================================================================


class TestNewEscalationTypeRegistration:
    """All new escalation types introduced by this fix must appear in
    ESCALATION_TYPES and have a TypedDict in ESCALATION_SCHEMAS."""

    @pytest.mark.parametrize("slug", [
        "missing_linkedin_url",
        "missing_quality_score",
        "accepted_missing_last_contact_date",
        "stale_connection_sent",
    ])
    def test_slug_in_escalation_types(self, slug: str):
        from workflows.escalation_schemas import ESCALATION_TYPES_SET

        assert slug in ESCALATION_TYPES_SET, (
            f"New escalation type {slug!r} must be added to ESCALATION_TYPES "
            "in workflows/escalation_schemas.py before calling escalate(type=...)"
        )

    @pytest.mark.parametrize("slug", [
        "missing_linkedin_url",
        "missing_quality_score",
        "accepted_missing_last_contact_date",
        "stale_connection_sent",
    ])
    def test_slug_has_typeddict_in_schemas(self, slug: str):
        from workflows.escalation_schemas import ESCALATION_SCHEMAS

        assert slug in ESCALATION_SCHEMAS, (
            f"Escalation type {slug!r} must have a TypedDict registered in "
            "ESCALATION_SCHEMAS so payload validation works at runtime."
        )


@pytest.fixture(autouse=True)
def _fresh_stage_unit_boundary():
    from workflows.email_send_guard import GuardResult
    with patch("workflows.daily_check.verify_send_preconditions", return_value=GuardResult(True)), patch("workflows.dm_quality_gate.require_clear_dm_quality_queue"):
        yield
