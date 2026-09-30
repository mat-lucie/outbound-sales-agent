"""Invite backfill (port of upstream #156): Part A fills to the daily cap by
scanning the full sorted eligible pool past throttled/duplicate-company rows.

Unit tests target the pure accumulator `_build_invite_send_data`. The
per-prospect content path (resolve_language / get_message / personalize)
is patched to deterministic stubs so these tests isolate the SELECTION
logic: target fill, §3.8 throttle skip, within-run company dedup, and
language/copy skip-continue.

A second class drives only the target-computation + early-return path of
`run_connection_requests` with its collaborators patched. Fork adaptation:
the invite path now carries the daily_run cap lease (port of upstream #182),
so `target = min(local remaining, daily_run remaining, batch_size)`. These
tests pin a non-binding daily_run remaining so the local-file term governs.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tests.fakes import fake_daily_run
from workflows.daily_check import _build_invite_send_data

TODAY = date(2026, 6, 2)


def _attrs(record_id: str, stage: str = "Prospect") -> dict:
    return {
        "record_id": record_id,
        "entry_id": f"entry-{record_id}",
        "stage": stage,
        "persona": "operations_leaders",
        "language": "es",
        "quality_score": 70,
        "invite_eligible_after": None,
        "experiment_id": None,
        "experiment_id_frozen_at": None,
    }


def _fake_cache(url_for_record):
    """A stand-in RecordCache whose .get(record_id) returns the 5-tuple
    (name, company, linkedin_url, industry_raw, title)."""
    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Name {rid}", f"Company {rid}", url_for_record(rid), None, "Director"
    )
    return cache


def _attio_with_companies(record_to_company, throttled_companies):
    """MagicMock AttioClient: _person_to_company maps record_id->company_id;
    get_company returns a record whose last_outreach_at is recent (throttled)
    for company_ids in `throttled_companies`, else permissive."""
    attio = MagicMock()
    attio._person_to_company = dict(record_to_company)

    def get_company(cid):
        if cid in throttled_companies:
            # recent outreach -> throttled
            return {"values": {"last_outreach_at": [{"value": "2026-06-01"}]}}
        return {"values": {}}  # no last_outreach_at -> permitted

    attio.get_company.side_effect = get_company
    return attio


@pytest.fixture
def patched_content():
    """resolve_language -> 'es'; get_message -> a fixed template; personalize
    -> echoes a deterministic note. Keeps selection tests content-agnostic."""
    with patch("workflows.daily_check.resolve_language", return_value="es"), \
         patch("workflows.daily_check.get_message", return_value="TEMPLATE"), \
         patch("workflows.daily_check.personalize", side_effect=lambda *a, **k: "NOTE"), \
         patch("workflows.daily_check.get_industry_label", return_value="manufactura"):
        yield


class TestBuildInviteSendData:
    def test_next_container_excludes_stale_duplicate_url_and_company(self, patched_content):
        # Attio may return a stale pool after the previous container held
        # its rows. A second list entry for the same person and a colleague
        # at the same company must both be excluded before PB sees them.
        prospects = [_attrs("same-person"), _attrs("colleague"), _attrs("fresh")]
        attio = _attio_with_companies(
            {"same-person": "company-1", "colleague": "company-1",
             "fresh": "company-2"}, set(),
        )
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")
        rows, _ = _build_invite_send_data(
            prospects, target=10, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
            seen_company_ids={"company-1"},
            excluded_linkedin_urls={"https://linkedin.com/in/same-person"},
        )
        assert [row["record_id"] for row in rows] == ["fresh"]

    def test_backfill_past_throttle_reaches_target(self, patched_content):
        """20 distinct-company prospects, the first 11 throttled. The
        accumulator must SKIP the 11 throttled and keep scanning to fill
        the target from later rows."""
        prospects = [_attrs(f"r{i}") for i in range(20)]
        r2c = {f"r{i}": f"c{i}" for i in range(20)}
        throttled = {f"c{i}" for i in range(11)}  # first 11 companies throttled
        attio = _attio_with_companies(r2c, throttled)
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        to_send, counts = _build_invite_send_data(
            prospects, target=9, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
        )

        assert len(to_send) == 9  # filled from the 9 un-throttled tail
        assert counts["company_throttled"] == 11
        # none of the throttled companies' records made it in
        sent_ids = {row["record_id"] for row in to_send}
        assert sent_ids == {f"r{i}" for i in range(11, 20)}

    def test_within_run_company_dedup(self, patched_content):
        """Two un-throttled prospects at the SAME company -> only one sent."""
        prospects = [_attrs("a"), _attrs("b")]
        attio = _attio_with_companies({"a": "shared", "b": "shared"}, set())
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        to_send, counts = _build_invite_send_data(
            prospects, target=25, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
        )

        assert len(to_send) == 1
        assert counts["same_company_run"] == 1

    def test_none_company_not_deduped(self, patched_content):
        """company_id None prospects each take a slot (no dedup against None)."""
        prospects = [_attrs("a"), _attrs("b")]
        attio = _attio_with_companies({"a": None, "b": None}, set())
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        to_send, counts = _build_invite_send_data(
            prospects, target=25, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
        )

        assert len(to_send) == 2
        assert counts["same_company_run"] == 0

    def test_pool_smaller_than_target(self, patched_content):
        """6 eligible, target 25 -> sends 6, no error."""
        prospects = [_attrs(f"r{i}") for i in range(6)]
        attio = _attio_with_companies({f"r{i}": f"c{i}" for i in range(6)}, set())
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        to_send, _ = _build_invite_send_data(
            prospects, target=25, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
        )

        assert len(to_send) == 6

    def test_missing_language_skip_does_not_waste_slot(self):
        """A language-resolution failure on one prospect must skip it and
        keep scanning so the target is still filled from later rows."""
        from models.resolution import MissingLanguageError

        prospects = [_attrs("bad"), _attrs("good")]
        attio = _attio_with_companies({"bad": "c1", "good": "c2"}, set())
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        def resolve(attrs_in, **kwargs):
            if attrs_in["record_id"] == "bad":
                raise MissingLanguageError(
                    persona="operations_leaders", language=None,
                    dm_step="connection_note",
                )
            return "es"

        with patch("workflows.daily_check.resolve_language", side_effect=resolve), \
             patch("workflows.daily_check.get_message", return_value="TEMPLATE"), \
             patch("workflows.daily_check.personalize", side_effect=lambda *a, **k: "NOTE"), \
             patch("workflows.daily_check.get_industry_label", return_value="x"), \
             patch("workflows.daily_check.escalate"):
            to_send, counts = _build_invite_send_data(
                prospects, target=1, attio=attio, cache=cache, today=TODAY,
                audit_logger=None, dry_run=True,
            )

        # target=1: "bad" is skipped (missing language), "good" fills the slot.
        assert len(to_send) == 1
        assert to_send[0]["record_id"] == "good"
        assert counts["missing_language"] == 1

    def test_missing_copy_skip_does_not_waste_slot(self):
        """A missing-message-copy failure skips the row and keeps scanning."""
        from models.campaign import MissingMessageError

        prospects = [_attrs("bad"), _attrs("good")]
        attio = _attio_with_companies({"bad": "c1", "good": "c2"}, set())
        cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")

        def get_msg(persona, language, step, *, record_id):
            if record_id == "bad":
                raise MissingMessageError(
                    persona="operations_leaders", language="es",
                    dm_step="connection_note", variant="default",
                )
            return "TEMPLATE"

        with patch("workflows.daily_check.resolve_language", return_value="es"), \
             patch("workflows.daily_check.get_message", side_effect=get_msg), \
             patch("workflows.daily_check.personalize", side_effect=lambda *a, **k: "NOTE"), \
             patch("workflows.daily_check.get_industry_label", return_value="x"), \
             patch("workflows.daily_check.escalate"):
            to_send, counts = _build_invite_send_data(
                prospects, target=1, attio=attio, cache=cache, today=TODAY,
                audit_logger=None, dry_run=True,
            )

        assert len(to_send) == 1
        assert to_send[0]["record_id"] == "good"
        assert counts["missing_copy"] == 1

    def test_missing_url_skip_counted(self, patched_content):
        """A row with no LinkedIn URL is skipped and tallied (not silent)."""
        prospects = [_attrs("nourl"), _attrs("good")]
        attio = _attio_with_companies({"nourl": "c1", "good": "c2"}, set())
        cache = _fake_cache(
            lambda rid: None if rid == "nourl" else f"https://linkedin.com/in/{rid}"
        )

        to_send, counts = _build_invite_send_data(
            prospects, target=5, attio=attio, cache=cache, today=TODAY,
            audit_logger=None, dry_run=True,
        )

        assert len(to_send) == 1
        assert to_send[0]["record_id"] == "good"
        assert counts["missing_url"] == 1


class TestTargetComputation:
    def _run(self, *, remaining, batch_size, prospects_len, spy, can_send=True,
             daily_run_remaining=1000):
        """Drive only the target-computation + early-return path of
        run_connection_requests by patching its collaborators. `can_send`
        models the canonical can_send_connections(1) gate.

        Fork adaptation: the invite path takes the cap lease, so a daily_run
        is required. `daily_run_remaining` is set non-binding by default so
        the local-file `remaining["connections"]` governs the target, matching
        upstream's single-source target math."""
        from workflows import daily_check

        prospects = [_attrs(f"r{i}") for i in range(prospects_len)]
        dr = fake_daily_run()
        dr.remaining.side_effect = lambda kind: (
            daily_run_remaining if kind == "connections" else 1000
        )
        # _get_all_entries_parsed returns these as Prospect-stage, score 70.
        with patch.object(daily_check, "can_send_connections", return_value=can_send), \
             patch.object(daily_check, "get_remaining", return_value=remaining), \
             patch.object(daily_check, "_get_all_entries_parsed", return_value=prospects), \
             patch.object(daily_check, "ensure_throttle_policy_decision_opened"), \
             patch("models.pipeline.is_invite_eligible", return_value=True), \
             patch("models.pipeline.is_send_eligible", return_value=True), \
             patch.object(daily_check, "_build_invite_send_data", side_effect=spy) as m, \
             patch.object(daily_check, "_pre_invite_degree_check", side_effect=lambda td, *a, **k: (td, [])), \
             patch.object(daily_check, "write_prospects_to_sheet"), \
             patch.object(daily_check, "recheck_cache") as rc:
            rc.partition.return_value = ([], [])
            rc.RECHECK_TTL_DAYS = 3
            attio = MagicMock()
            attio._person_to_company = {}
            cache = _fake_cache(lambda rid: f"https://linkedin.com/in/{rid}")
            daily_check.run_connection_requests(
                attio, MagicMock(), "nb-id",
                batch_size=batch_size, dry_run=True, auto_confirm=True,
                cache=cache, today=TODAY, daily_run=dr,
            )
            return m

    def test_target_is_min_of_remaining_and_batch(self):
        captured = {}

        def spy(prospects, *, target, **kw):
            captured["target"] = target
            return [], {"company_throttled": 0, "same_company_run": 0,
                        "missing_language": 0, "missing_copy": 0,
                        "missing_url": 0}
        self._run(remaining={"connections": 25, "messages": 30, "visits": 50},
                  batch_size=25, prospects_len=40, spy=spy)
        assert captured["target"] == 10

    def test_target_clamped_to_remaining_connections(self):
        captured = {}

        def spy(prospects, *, target, **kw):
            captured["target"] = target
            return [], {"company_throttled": 0, "same_company_run": 0,
                        "missing_language": 0, "missing_copy": 0,
                        "missing_url": 0}
        # 15 already sent today -> 10 remaining connections (local file).
        self._run(remaining={"connections": 10, "messages": 30, "visits": 50},
                  batch_size=25, prospects_len=40, spy=spy)
        assert captured["target"] == 10

    def test_target_clamped_to_daily_run_remaining(self):
        """Fork-specific: when the daily_run ledger is the lower of the two
        cap sources, it binds the target (the #182 dual-source rule)."""
        captured = {}

        def spy(prospects, *, target, **kw):
            captured["target"] = target
            return [], {"company_throttled": 0, "same_company_run": 0,
                        "missing_language": 0, "missing_copy": 0,
                        "missing_url": 0}
        self._run(remaining={"connections": 25, "messages": 30, "visits": 50},
                  batch_size=25, prospects_len=40, spy=spy, daily_run_remaining=7)
        assert captured["target"] == 7

    def test_limit_reached_short_circuits(self):
        """can_send_connections(1) False -> early-out, accumulator never runs."""
        called = {"build": False}

        def spy(*a, **k):
            called["build"] = True
            return [], {}
        m = self._run(remaining={"connections": 0, "messages": 30, "visits": 50},
                      batch_size=25, prospects_len=40, spy=spy, can_send=False)
        m.assert_not_called()
        assert called["build"] is False
