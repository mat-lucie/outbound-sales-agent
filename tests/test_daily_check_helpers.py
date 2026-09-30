"""Tests for daily_check personalization, dedup, placeholder guard, and PB parsing.

These are regression tests for a production incident where:
  * DM2 messages went out with literal `[indústria similar]` / `[industria similar]`
    placeholders because personalization silently skipped when industry was empty.
  * Duplicate Attio records caused the same prospect to receive two DM1s.
  * PB's "InMail required" per-profile skip still advanced the Attio stage, so a
    prospect who never received the DM got marked as if they had.
"""

from unittest.mock import patch

import pytest

from models.campaign import Language, MessageStep, Persona, get_industry_label, get_message, personalize
from workflows.daily_check import (
    BlankMessageError,
    RecordCache,
    UnresolvedPlaceholderError,
    _assert_no_unresolved_placeholders,
    _dedupe_by_linkedin_url,
)
from workflows.daily_check_helpers import (
    _assert_email_not_blank,
    _assert_email_renderable,
    _parse_pb_sent_urls,
)
from workflows.record_cache import preload_pipeline_persons

# ── personalize() fallback ────────────────────────────────────────────────────

class TestPersonalizeIndustryFallback:
    """Industry placeholders must NEVER survive into an outgoing message."""

    def test_empty_industry_falls_back_to_manufatura_pt(self):
        tmpl = get_message(Persona.OPERATIONS_LEADERS, Language.PT, MessageStep.DM2)
        msg = personalize(
            tmpl,
            "Arianne",
            "Stellantis",
            industry=get_industry_label("", Language.PT),
            language=Language.PT,
        )
        assert "[indústria similar]" not in msg
        assert "manufatura" in msg
        assert msg.startswith("Arianne, ")
        assert "[Name]" not in msg

    def test_empty_industry_falls_back_to_manufactura_es(self):
        tmpl = get_message(Persona.OPERATIONS_LEADERS, Language.ES, MessageStep.DM2)
        msg = personalize(
            tmpl,
            "Vanessa",
            "Metalsa",
            industry=get_industry_label("", Language.ES),
            language=Language.ES,
        )
        assert "[industria similar]" not in msg
        assert "manufactura" in msg
        assert msg.startswith("Vanessa, ")
        assert "[Name]" not in msg

    def test_personalize_with_empty_industry_drops_industry_clause(self):
        """When industry is unknown, the [industry_clause_pt] token renders as
        "em outra planta" — no generic "manufatura" fill that deflates the claim."""
        tmpl = get_message(Persona.OPERATIONS_LEADERS, Language.PT, MessageStep.DM2)
        msg = personalize(tmpl, "Leo", "bioMérieux", industry="", language=Language.PT)
        assert "[indústria similar]" not in msg
        assert "[industry_clause_pt]" not in msg
        # Falls back to the no-industry clause, not a generic label
        assert "em outra planta" in msg
        assert msg.startswith("Leo, ")
        assert "[Name]" not in msg

    def test_known_industry_still_substitutes(self):
        tmpl = get_message(Persona.OPERATIONS_LEADERS, Language.ES, MessageStep.DM2)
        msg = personalize(
            tmpl,
            "Daniel",
            "Nissan",
            industry=get_industry_label("Automotive", Language.ES),
            language=Language.ES,
        )
        assert "[industria similar]" not in msg
        assert "[industry_clause_es]" not in msg
        assert "automotriz" in msg
        assert msg.startswith("Daniel, ")
        assert "[Name]" not in msg

    def test_personalize_operations_leaders_dm2_end_to_end(self):
        """End-to-end: verify that full personalization of operations_leaders DM2
        produces a message starting with the prospect's name, with no unresolved placeholders."""
        tmpl = get_message(Persona.OPERATIONS_LEADERS, Language.ES, MessageStep.DM2)
        msg = personalize(
            tmpl,
            "Arianne",
            "Stellantis",
            industry=get_industry_label("Automotive", Language.ES),
            language=Language.ES,
        )
        assert msg.startswith("Arianne, te pongo un ejemplo real: en una planta de automotriz")
        assert "[" not in msg  # No unresolved bracket placeholders
        assert "]" not in msg


# ── _dedupe_by_linkedin_url ────────────────────────────────────────────────────

class TestDedupeByLinkedinUrl:
    def test_drops_duplicates_keeping_first(self):
        rows = [
            {"linkedInUrl": "https://www.linkedin.com/in/edgar", "message": "m", "entry_id": "a"},
            {"linkedInUrl": "https://linkedin.com/in/edgar/", "message": "m", "entry_id": "b"},
            {"linkedInUrl": "https://www.linkedin.com/in/mario", "message": "m", "entry_id": "c"},
        ]
        deduped, dropped = _dedupe_by_linkedin_url(rows)
        assert len(deduped) == 2
        assert len(dropped) == 1
        assert deduped[0]["linkedInUrl"] == "https://www.linkedin.com/in/edgar"

    def test_accumulates_entry_ids_across_duplicates(self):
        """Duplicate Attio entries must advance together — otherwise the
        unadvanced one gets re-queued tomorrow and the prospect gets another DM."""
        rows = [
            {"linkedInUrl": "https://www.linkedin.com/in/leo", "message": "m", "entry_id": "leo-1"},
            {"linkedInUrl": "https://www.linkedin.com/in/leo", "message": "m", "entry_id": "leo-2"},
        ]
        deduped, _ = _dedupe_by_linkedin_url(rows)
        assert len(deduped) == 1
        assert deduped[0]["entry_ids"] == ["leo-1", "leo-2"]

    def test_preserves_entry_ids_key_when_no_dupes(self):
        rows = [
            {"linkedInUrl": "https://www.linkedin.com/in/solo", "message": "m", "entry_id": "solo-1"},
        ]
        deduped, dropped = _dedupe_by_linkedin_url(rows)
        assert deduped[0]["entry_ids"] == ["solo-1"]
        assert dropped == []

    def test_handles_rows_without_entry_id(self):
        rows = [{"linkedInUrl": "https://linkedin.com/in/a", "message": "m"}]
        deduped, _ = _dedupe_by_linkedin_url(rows)
        assert deduped[0]["entry_ids"] == []


# ── _assert_no_unresolved_placeholders ─────────────────────────────────────────

class TestPlaceholderGuard:
    def test_raises_on_unresolved_industry_placeholder(self):
        rows = [
            {"linkedInUrl": "https://linkedin.com/in/a",
             "message": "Dou um exemplo real: em uma planta de [indústria similar]..."},
        ]
        with pytest.raises(UnresolvedPlaceholderError) as exc:
            _assert_no_unresolved_placeholders(rows, "dm2")
        assert "[indústria similar]" in str(exc.value)

    def test_raises_on_unresolved_name_placeholder(self):
        rows = [{"linkedInUrl": "x", "message": "Hola [Name], ..."}]
        with pytest.raises(UnresolvedPlaceholderError):
            _assert_no_unresolved_placeholders(rows, "connection_note")

    def test_passes_fully_personalized_message(self):
        rows = [{"linkedInUrl": "x", "message": "Hola Mario, todo bien."}]
        _assert_no_unresolved_placeholders(rows, "dm1")  # no raise

    def test_flags_all_offenders(self):
        rows = [
            {"linkedInUrl": "a", "message": "clean"},
            {"linkedInUrl": "b", "message": "has [placeholder]"},
            {"linkedInUrl": "c", "message": "[another]"},
        ]
        with pytest.raises(UnresolvedPlaceholderError) as exc:
            _assert_no_unresolved_placeholders(rows, "dm2")
        assert "2 message(s)" in str(exc.value)


# ── _assert_email_not_blank ────────────────────────────────────────────────────

class TestEmailBlankGuard:
    """Email counterpart of the blank-message guard. The email paths render
    subject and body separately but run the placeholder guard over their
    "{subject}\\n{body}" join — a blank subject is masked by a non-blank body
    (and vice versa), so each part must be validated on its own.
    """

    def test_blank_subject_raises(self):
        with pytest.raises(BlankMessageError) as exc:
            _assert_email_not_blank("", "<p>Hola Mario</p>", "mario@corp.com", "email1")
        msg = str(exc.value)
        assert "subject" in msg
        assert "email1" in msg
        assert "mario@corp.com" in msg

    def test_blank_body_raises(self):
        with pytest.raises(BlankMessageError) as exc:
            _assert_email_not_blank("Asunto claro", "", "mario@corp.com", "email2")
        assert "body" in str(exc.value)

    def test_both_blank_names_both_parts(self):
        with pytest.raises(BlankMessageError) as exc:
            _assert_email_not_blank("", "", "x@y.com", "wave2")
        msg = str(exc.value)
        assert "subject" in msg
        assert "body" in msg

    def test_whitespace_only_subject_counts_as_blank(self):
        with pytest.raises(BlankMessageError):
            _assert_email_not_blank("  \n\t ", "<p>ok</p>", "x@y.com", "email3")

    def test_whitespace_only_body_counts_as_blank(self):
        with pytest.raises(BlankMessageError):
            _assert_email_not_blank("Asunto", "   ", "x@y.com", "email1")

    def test_markup_skeleton_body_counts_as_blank(self):
        """A bad template edit typically leaves the HTML skeleton behind —
        the body must be judged on its rendered text, not the raw markup."""
        for skeleton in ("<p></p>", "<div><br></div>", "<p>&nbsp;</p>"):
            with pytest.raises(BlankMessageError) as exc:
                _assert_email_not_blank("Asunto", skeleton, "x@y.com", "email2")
            assert "body" in str(exc.value)

    def test_passes_when_both_rendered(self):
        _assert_email_not_blank("Asunto", "<p>Hola Mario</p>", "x@y.com", "email1")  # no raise


# ── _assert_email_renderable ───────────────────────────────────────────────────

class TestEmailRenderableGuard:
    """Combined per-send guard: one call runs the placeholder check and the
    blank check, so a new email path can't adopt one and forget the other."""

    def test_raises_on_unresolved_placeholder(self):
        with pytest.raises(UnresolvedPlaceholderError):
            _assert_email_renderable(
                "Hola", "Plantas de [industria] pierden horas.", "x@y.com", "email1"
            )

    def test_raises_on_blank_subject(self):
        with pytest.raises(BlankMessageError):
            _assert_email_renderable("", "<p>Hola Mario</p>", "x@y.com", "email1")

    def test_raises_on_markup_skeleton_body(self):
        with pytest.raises(BlankMessageError):
            _assert_email_renderable("Asunto", "<p>&nbsp;</p>", "x@y.com", "wave2")

    def test_passes_clean_email(self):
        _assert_email_renderable("Asunto", "<p>Hola Mario</p>", "x@y.com", "email1")  # no raise


# ── _parse_pb_sent_urls ────────────────────────────────────────────────────────

class TestParsePbSentUrls:
    def test_detects_sent_vs_inmail_skip(self):
        log = (
            "[load_]Converting regular URL https://www.linkedin.com/in/arianne...\n"
            "[done_]Sending message to Arianne: hello\n"
            "[load_]Converting regular URL https://www.linkedin.com/in/daniel...\n"
            "[warn_]Can't send message, InMail required.\n"
            "[load_]Converting regular URL https://www.linkedin.com/in/leo...\n"
            "[done_]Sending message to Leo: hello\n"
        )
        sent, skipped = _parse_pb_sent_urls(log)
        assert "https://linkedin.com/in/arianne" in sent
        assert "https://linkedin.com/in/leo" in sent
        assert "https://linkedin.com/in/daniel" in skipped
        assert "https://linkedin.com/in/daniel" not in sent

    def test_silent_profile_open_without_send_is_skipped(self):
        """Apr-16 regression: PB opened Galo's profile but never sent and never
        errored. That block must be classified as skipped, not sent."""
        log = (
            "[load_]Converting regular URL https://linkedin.com/in/galoleon...\n"
            "[done_]Opened profile of Galo Leon.\n"
            "[load_]Converting regular URL https://linkedin.com/in/next...\n"
            "[done_]Sending message to Next: hi\n"
        )
        sent, skipped = _parse_pb_sent_urls(log)
        assert "https://linkedin.com/in/galoleon" in skipped
        assert "https://linkedin.com/in/next" in sent

    def test_handles_trailing_ellipsis_in_url(self):
        log = "[load_]Converting regular URL https://www.linkedin.com/in/x...\n[done_]Sending message to X: hi\n"
        sent, _ = _parse_pb_sent_urls(log)
        assert "https://linkedin.com/in/x" in sent

    def test_empty_log_returns_empty_sets(self):
        sent, skipped = _parse_pb_sent_urls("")
        assert sent == set()
        assert skipped == set()


# ── preload_pipeline_persons ──────────────────────────────────────────────────

class _StubAttio:
    """Minimal AttioClient stub that returns canned data for bulk fetch + extract."""

    def __init__(self, records: dict[str, dict], info_by_record: dict[str, tuple[str, str, str, str, str]]):
        self._records = records
        self._info = info_by_record
        self.bulk_calls = 0
        self.bulk_company_calls: list[set[str]] = []
        self.get_calls: list[str] = []

    def bulk_fetch_persons_by_record_ids(self, record_ids, max_workers=8, *, metrics=None):
        self.bulk_calls += 1
        return {rid: rec for rid, rec in self._records.items() if rid in record_ids}

    def bulk_prime_company_caches(self, company_ids, max_workers=8, *, metrics=None):
        # Without this method AttioProvider.prefetch_companies_for_persons
        # would raise AttributeError into the preload's fail-open catch, and
        # these tests would silently pin the serial-fallback path instead of
        # the real wiring.
        self.bulk_company_calls.append(set(company_ids))
        return 0

    def extract_record_info(self, record):
        rid = record["id"]["record_id"]
        return self._info[rid]

    def get_person(self, record_id):
        self.get_calls.append(record_id)
        return self._records.get(record_id)


class TestPreloadPipelinePersons:
    """Bulk preload primes RecordCache so phase loops never hit the network for
    pipeline records they already know about."""

    def test_primes_cache_for_returned_records(self):
        records = {
            "rec-1": {"id": {"record_id": "rec-1"}, "values": {}},
            "rec-2": {"id": {"record_id": "rec-2"}, "values": {}},
        }
        info = {
            "rec-1": ("Alice", "AcmeCo", "https://linkedin.com/in/alice", "Automotive", "Plant Director"),
            "rec-2": ("Bob", "BetaCo", "https://linkedin.com/in/bob", "Food", "VP Operations"),
        }
        attio = _StubAttio(records, info)
        cache = RecordCache(attio)  # type: ignore[arg-type]

        n = preload_pipeline_persons(attio, cache, {"rec-1", "rec-2"})  # type: ignore[arg-type]

        assert n == 2
        assert attio.bulk_calls == 1
        # Subsequent lookups must NOT hit the network.
        assert cache.get("rec-1") == info["rec-1"]
        assert cache.get("rec-2") == info["rec-2"]
        assert attio.get_calls == [], "Cache hit should bypass get_person entirely"

    def test_empty_record_ids_skips_bulk_fetch(self):
        attio = _StubAttio({}, {})
        cache = RecordCache(attio)  # type: ignore[arg-type]

        n = preload_pipeline_persons(attio, cache, set())  # type: ignore[arg-type]

        assert n == 0
        assert attio.bulk_calls == 0

    def test_falls_back_silently_on_bulk_failure(self):
        """If the bulk POST fails (rate limit, network), preload returns 0 and
        the caller continues. Phases will fetch on-demand via RecordCache.get."""
        attio = _StubAttio({}, {})
        cache = RecordCache(attio)  # type: ignore[arg-type]

        with patch.object(attio, "bulk_fetch_persons_by_record_ids",
                          side_effect=RuntimeError("boom")):
            n = preload_pipeline_persons(attio, cache, {"rec-1"})  # type: ignore[arg-type]

        assert n == 0
        # Cache is still empty — next get() will fall through to get_person.
        assert "rec-1" not in cache._cache

    def test_partial_coverage_leaves_unknowns_to_lazy_fetch(self):
        """When bulk fetch returns a subset (e.g. one record was deleted from
        Attio between snapshots), only the returned records are primed. Misses
        fall through to RecordCache.get → get_person, same as today."""
        records = {"rec-1": {"id": {"record_id": "rec-1"}, "values": {}}}
        info = {"rec-1": ("Alice", "AcmeCo", "https://linkedin.com/in/alice", "", "Plant Director")}
        attio = _StubAttio(records, info)
        cache = RecordCache(attio)  # type: ignore[arg-type]

        n = preload_pipeline_persons(attio, cache, {"rec-1", "rec-2"})  # type: ignore[arg-type]

        assert n == 1
        assert "rec-1" in cache._cache
        assert "rec-2" not in cache._cache

    def test_per_record_extract_failure_is_skipped_not_fatal(self):
        """A transient failure resolving one record's company link must not
        kill the whole preload — that record is logged and lazy-fetched, the
        rest still prime. Real-world reproduction: 2026-04-27 dry run hit a
        DNS blip on a single company GET mid-loop."""
        records = {
            "rec-good": {"id": {"record_id": "rec-good"}, "values": {}},
            "rec-bad": {"id": {"record_id": "rec-bad"}, "values": {}},
        }
        info = {"rec-good": ("Alice", "AcmeCo", "https://linkedin.com/in/alice", "", "Plant Director")}
        attio = _StubAttio(records, info)

        def flaky_extract(record):
            rid = record["id"]["record_id"]
            if rid == "rec-bad":
                raise RuntimeError("transient DNS failure on company resolve")
            return info[rid]

        cache = RecordCache(attio)  # type: ignore[arg-type]
        with patch.object(attio, "extract_record_info", side_effect=flaky_extract):
            n = preload_pipeline_persons(attio, cache, {"rec-good", "rec-bad"})  # type: ignore[arg-type]

        assert n == 1
        assert "rec-good" in cache._cache
        assert "rec-bad" not in cache._cache


class TestGetAllEntriesParsedSoftDeleteFilter:
    """§3.11 soft-delete defense — `_get_all_entries_parsed` MUST drop
    any entry whose `merged_into` is set so downstream daily_check queues
    can never re-send to a union-merge loser (§3.1 hard red line).
    """

    def test_filters_out_merged_into_losers(self, monkeypatch):
        from workflows.daily_check_helpers import _get_all_entries_parsed

        raw_entries = [
            {"id": {"entry_id": "ent-winner"}, "entry_values": {
                "stage": [{"status": {"title": "DM1 Sent"}}],
            }},
            # Loser: soft-deleted via merged_into pointing at the winner.
            {"id": {"entry_id": "ent-loser"}, "entry_values": {
                "stage": [{"status": {"title": "DM1 Sent"}}],
                "merged_into": [{"target_object": "people",
                                 "target_record_id": "rec-winner"}],
            }},
        ]

        class _StubAttio:
            def query_list_entries(self, list_id):
                return raw_entries

        monkeypatch.setenv("ATTIO_LIST_ID", "list-x")
        out = _get_all_entries_parsed(_StubAttio())  # type: ignore[arg-type]
        # Only the winner survives the filter.
        assert len(out) == 1
        assert out[0]["entry_id"] == "ent-winner"
        assert all(not e.get("merged_into") for e in out)
