"""Pain-signal discovery lane (workflows/pain_signal.py + wiring).

The lane drives a post-engager-scraper workflow's WORKER phantoms directly
(posts worker save-then-launch; the commenters/likers workers optional behind
their own agent ids), with CLIENT-SIDE recency on postTimestamp (LinkedIn's
datePosted search filter is broken) and SN profile enrichment at ingest.

Covers: the env + keyword-approval gates (incl. the shipped-placeholder
sentinel refusal, unbalanced-quote validation, and the 168h window ceiling),
the search URL (regression guard: datePosted must never return), post-timestamp
parsing (ISO/epoch/relative EN-ES-PT; future stamps fail closed), the topic gate
(phrase folding, word boundaries, paired-query all-terms, cross-query
re-attribution), posts/engager-row normalization (incl. abbreviated engagement
counts — 1.2K must not read as zero), the save-then-launch worker contract (save
verification, console-field clobber check, quiet-zero vs failure,
PBRunTimeout-finished salvage), the circuit breaker (stops launches, keeps the
harvest), seam pacing hygiene, the ingest-time never-contact denylist block, the
in-run identity-key dedup with source-priority upgrade, the pre-enrichment
in-pipeline drop, the per-run caps (engager posts + candidates), the enrichment
launch/merge (+ loud degrade), lane entry-attr assembly (incl. the explicit
cohort stamp), the pain-signal note templates, `_commit_prospect`'s lane-attrs
merge, the daily invite path's pain-note selection with loud persona fallback,
and the end-to-end dry/wet honesty of `run_pain_signal_discovery`.

Every identity, company and post in this file is SYNTHETIC (the bundled Acme
reference operator). The denylist token exercised end to end is Acme's own
configured never-contact entry — see `examples/acme/config/botdog.yaml`.
"""

import json
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from clients.crm.attio_provider import AttioProvider
from models.campaign import (
    Language,
    MissingMessageError,
    get_pain_signal_note,
    load_messages,
    personalize,
)
from workflows.content_guard import PLACEHOLDER_SENTINEL
from workflows.pain_signal import (
    PAIN_SIGNAL_ENABLED_ENV,
    PAIN_SIGNAL_EXPERIMENT_ID,
    PAIN_SIGNAL_POST_MAX_AGE_ENV,
    PainKeywordsNotApprovedError,
    PainLaneDisabledError,
    _clean_snippet,
    _drop_denylisted,
    _engager_worker_candidate,
    _launch_enrichment_scrape,
    _launch_posts_scrape,
    _merge_enrichment,
    _post_from_row,
    _poster_candidate,
    assert_keywords_approved,
    content_search_url,
    filter_recent_posts,
    is_pain_signal_enabled,
    lane_entry_attrs_for,
    load_pain_keywords,
    parse_post_timestamp,
    post_matches_query,
    query_match_terms,
    run_pain_signal_discovery,
)

# The synthetic never-contact token the bundled Acme operator configures
# (examples/acme/config/botdog.yaml → blacklist.denylist_companies). The suite
# resolves that config via conftest's OUTBOUND_CONFIG_DIR pin.
_DENYLISTED_COMPANY = "Contoso Holdings"

# ── gates ────────────────────────────────────────────────────────────


class TestEnabledGate:
    def test_default_off(self, monkeypatch):
        monkeypatch.delenv(PAIN_SIGNAL_ENABLED_ENV, raising=False)
        assert is_pain_signal_enabled() is False

    @pytest.mark.parametrize("value", ["0", "true", "yes", ""])
    def test_only_literal_1_enables(self, monkeypatch, value):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, value)
        assert is_pain_signal_enabled() is False

    def test_literal_1_enables(self, monkeypatch):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        assert is_pain_signal_enabled() is True

    def test_env_var_is_outbound_namespaced(self):
        """Fork seam: the fork's flags live under OUTBOUND_*."""
        assert PAIN_SIGNAL_ENABLED_ENV == "OUTBOUND_PAIN_SIGNAL_ENABLED"
        assert PAIN_SIGNAL_POST_MAX_AGE_ENV.startswith("OUTBOUND_")

    def test_workflow_raises_when_disabled(self, monkeypatch):
        monkeypatch.delenv(PAIN_SIGNAL_ENABLED_ENV, raising=False)
        with pytest.raises(PainLaneDisabledError):
            run_pain_signal_discovery(
                MagicMock(), MagicMock(), "posts-w", "com-w", "lik-w", "sn-w"
            )


def _registry(status="approved", approved_by="acme-operator", queries=None):
    return {
        "_meta": {"status": status, "approved_by": approved_by},
        "config": {},
        "queries": queries or [
            {"id": "q1", "language": "en", "query": '"production plan"'},
        ],
    }


class TestKeywordRegistry:
    def test_active_registry_loads_and_is_approved(self):
        """The registry the suite resolves (the bundled Acme content dir)
        must parse cleanly and carry an approval stamp. A future edit that
        resets the stamp must flip this test, not sneak through."""
        data = load_pain_keywords()
        assert len(data["queries"]) >= 5
        assert all(
            q["language"] in ("es", "pt", "en") for q in data["queries"]
        )
        assert_keywords_approved(data)  # raises if the stamp regresses
        assert data["_meta"]["approved_by"]

    def test_active_config_carries_the_spend_knobs(self):
        config = load_pain_keywords()["config"]
        assert config["post_max_age_hours_default"] == 24
        assert config["max_engagers_per_run"] > 0
        # The knob that bounds engager-worker launch spend must be
        # discoverable in the shipped registry, not only a code default.
        assert config["max_engager_scrape_posts_per_run"] == 8

    def test_shipped_neutral_registry_is_unapproved_and_placeholder(self):
        """The repo-root default MUST fail both gates on a fresh install:
        it is a placeholder, and it is unapproved. A default install can
        never scrape (let alone invite off) the engine's own template
        text."""
        from pathlib import Path

        repo_root = Path(__file__).resolve().parent.parent
        data = json.loads(
            (repo_root / "content" / "pain_keywords.json").read_text()
        )
        assert data["_meta"]["status"] != "approved"
        assert any(
            PLACEHOLDER_SENTINEL in q["query"] for q in data["queries"]
        )
        with pytest.raises(PainKeywordsNotApprovedError):
            assert_keywords_approved(data)

    def test_placeholder_sentinel_refuses_even_when_stamped_approved(self):
        """Flipping _meta.status without replacing the queries must not
        unlock the lane — the sentinel check is independent of status."""
        data = _registry(queries=[{
            "id": "q1", "language": "en",
            "query": f'"{PLACEHOLDER_SENTINEL} pain phrase"',
        }])
        with pytest.raises(
            PainKeywordsNotApprovedError, match=PLACEHOLDER_SENTINEL
        ):
            assert_keywords_approved(data)

    def test_window_wider_than_note_claim_refuses(self, monkeypatch):
        """The invite notes place the post inside the past week — a window
        wider than 168h would ship that as an overclaim."""
        from workflows.pain_signal import post_max_age_hours

        monkeypatch.delenv(PAIN_SIGNAL_POST_MAX_AGE_ENV, raising=False)
        assert post_max_age_hours({"post_max_age_hours_default": 168}) == 168
        with pytest.raises(ValueError, match="past week"):
            post_max_age_hours({"post_max_age_hours_default": 169})

    def test_env_override_wins_over_registry_default(self, monkeypatch):
        from workflows.pain_signal import post_max_age_hours

        monkeypatch.setenv(PAIN_SIGNAL_POST_MAX_AGE_ENV, "48")
        assert post_max_age_hours({"post_max_age_hours_default": 24}) == 48

    def test_approved_passes(self):
        assert_keywords_approved(_registry())

    @pytest.mark.parametrize(
        "meta",
        [
            {"status": "placeholder", "approved_by": None},
            {"status": "approved", "approved_by": None},
            {"status": "approved", "approved_by": ""},
            {},
        ],
    )
    def test_unapproved_variants_raise(self, meta):
        with pytest.raises(PainKeywordsNotApprovedError):
            assert_keywords_approved({"_meta": meta, "queries": []})

    def test_malformed_query_raises(self, tmp_path):
        bad = _registry(queries=[{"id": "q1", "language": "en"}])
        path = tmp_path / "kw.json"
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="missing"):
            load_pain_keywords(path)

    def test_bad_language_raises(self, tmp_path):
        bad = _registry(
            queries=[{"id": "q1", "language": "fr", "query": "plan"}]
        )
        path = tmp_path / "kw.json"
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="unsupported language"):
            load_pain_keywords(path)

    def test_empty_queries_raise(self, tmp_path):
        path = tmp_path / "kw.json"
        path.write_text(json.dumps({"_meta": {}, "queries": []}))
        with pytest.raises(ValueError, match="non-empty"):
            load_pain_keywords(path)

    def test_unbalanced_quote_raises(self, tmp_path):
        """An unclosed quote makes the topic gate's terms unmatchable —
        that query would silently drop 100% of its posts as off-topic.
        Fail loud at load instead."""
        bad = _registry(
            queries=[{
                "id": "q1", "language": "en",
                "query": '"production plan',
            }],
        )
        path = tmp_path / "kw.json"
        path.write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="unbalanced quote"):
            load_pain_keywords(path)

    def test_registry_path_follows_the_content_dir_override(
        self, monkeypatch, tmp_path
    ):
        """Fork seam: the registry lives beside messages.json/personas.json
        and follows OUTBOUND_CONTENT_DIR through models.campaign."""
        from models import campaign
        from workflows.pain_signal import pain_keywords_path

        monkeypatch.setattr(campaign, "CONTENT_DIR", tmp_path)
        assert pain_keywords_path() == tmp_path / "pain_keywords.json"


# ── content-search URL (client-side recency era) ─────────────────────


class TestContentSearchUrl:
    def test_keywords_and_sort_no_date_filter(self):
        url = content_search_url('"production plan" spreadsheet')
        assert url.startswith(
            "https://www.linkedin.com/search/results/content/?keywords="
        )
        assert "sortBy=%22date_posted%22" in url
        assert " " not in url
        # REGRESSION GUARD: LinkedIn's datePosted content-search filter
        # returns ZERO results — it must never come back; recency is
        # client-side.
        assert "datePosted" not in url


# ── client-side topic gate ───────────────────────────────────────────


class TestQueryMatchTerms:
    def test_exact_phrase_is_one_term(self):
        assert query_match_terms('"plan de producción"') == [
            "plan de producción"
        ]

    def test_paired_query_requires_phrase_and_bare_terms(self):
        assert query_match_terms('"rush order" scheduling') == [
            "rush order", "scheduling"
        ]

    def test_bare_terms_split_individually(self):
        assert query_match_terms("PCP produção planejamento") == [
            "PCP", "produção", "planejamento"
        ]


class TestPostMatchesQuery:
    def test_accent_and_case_folded_match(self):
        assert post_matches_query(
            "Cada semana la PROGRAMACION de la produccion se rehace",
            '"programación de la producción"',
        )

    def test_whitespace_collapsed_across_linebreaks(self):
        assert post_matches_query(
            "programación de\nla   producción en planta",
            '"programación de la producción"',
        )

    def test_offtopic_search_noise_is_rejected(self):
        """LinkedIn's 'exact-phrase' search is not exact — it returns
        work-anniversary posts and job ads that merely share vocabulary.
        Those must not pass as 'a post about production scheduling'."""
        anniversary = (
            "🎉 ¡30 años de historia, evolución y orgullo en Northwind "
            "Foods! Hoy celebro tres décadas en esta gran compañía."
        )
        assert not post_matches_query(
            anniversary, '"programación de la producción"'
        )

    def test_word_boundaries_prevent_substring_overclaim(self):
        assert not post_matches_query(
            "hicimos el replan de produccion",
            '"plan de producción"',
        )

    def test_paired_query_needs_all_terms(self):
        assert post_matches_query(
            "A rush order landed and the scheduling had to be redone",
            '"rush order" scheduling',
        )
        assert not post_matches_query(
            "A rush order landed at the warehouse",  # no "scheduling"
            '"rush order" scheduling',
        )

    def test_run_together_hashtag_does_not_match(self):
        """Conservative by design: dropping a hashtag-only post is cheap;
        an overclaiming invite note is not."""
        assert not post_matches_query(
            "Great week #programaciondelaproduccion",
            '"programación de la producción"',
        )

    def test_empty_post_text_never_matches(self):
        assert not post_matches_query("", '"production plan"')
        assert not post_matches_query("   ", '"production plan"')

    def test_termless_query_never_matches(self):
        assert not post_matches_query("any text at all", "")


_NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


class TestParsePostTimestamp:
    def test_iso_with_offset(self):
        parsed = parse_post_timestamp("2026-08-23T10:00:00+00:00", now=_NOW)
        assert parsed == datetime(2026, 8, 23, 10, 0, tzinfo=UTC)

    def test_iso_z_suffix(self):
        parsed = parse_post_timestamp("2026-08-23T10:00:00Z", now=_NOW)
        assert parsed == datetime(2026, 8, 23, 10, 0, tzinfo=UTC)

    def test_offsetless_assumed_utc(self):
        parsed = parse_post_timestamp("2026-08-23T10:00:00", now=_NOW)
        assert parsed is not None and parsed.tzinfo is not None

    @pytest.mark.parametrize(
        ("label", "hours"),
        [
            ("30m", 0.5), ("45min", 0.75), ("4h", 4), ("3d", 72),
            ("2w", 336), ("5mo", 3600), ("1yr", 8760),
            # ES/PT locale variants — LinkedIn renders relative labels in
            # the SCRAPING SESSION's locale, not the engine's.
            ("3 sem", 504), ("hace 2 días", 48), ("há 3 semanas", 504),
            ("1 mes", 720), ("2 años", 17520), ("2 anos", 17520),
            ("4 hr", 4), ("5 minutos", 5 / 60), ("2 horas", 2),
        ],
    )
    def test_relative_labels(self, label, hours):
        parsed = parse_post_timestamp(label, now=_NOW)
        assert parsed == _NOW - timedelta(hours=hours)

    def test_epoch_digit_string(self):
        # csv.DictReader only yields strings — epoch millis arrive as text.
        target = datetime(2026, 8, 23, 10, 0, tzinfo=UTC)
        millis = str(int(target.timestamp() * 1000))
        assert parse_post_timestamp(millis, now=_NOW) == target
        seconds = str(int(target.timestamp()))
        assert parse_post_timestamp(seconds, now=_NOW) == target

    def test_mo_is_months_not_minutes(self):
        # "5mo" must not parse as 5 minutes — the regex orders mo before m.
        parsed = parse_post_timestamp("5mo", now=_NOW)
        assert parsed is not None and parsed < _NOW - timedelta(days=100)

    @pytest.mark.parametrize("garbage", ["", "   ", "a while ago", None, {}])
    def test_unparseable_returns_none(self, garbage):
        assert parse_post_timestamp(garbage, now=_NOW) is None


def _post(url="https://li.example/posts/1", raw_ts="2h",
          text="the production plan fell apart"):
    return {"post_url": url, "text": text, "raw_timestamp": raw_ts}


class TestRecencyFilter:
    def _summary(self):
        return {"posts_dropped_stale": 0, "posts_dropped_no_timestamp": 0}

    def test_fresh_kept_with_posted_at(self):
        summary = self._summary()
        fresh = filter_recent_posts(
            [_post(raw_ts="2h")], max_age_hours=72, now=_NOW, summary=summary,
        )
        assert len(fresh) == 1
        assert fresh[0]["posted_at"] == _NOW - timedelta(hours=2)
        assert summary == self._summary()

    def test_stale_dropped_with_counter(self):
        summary = self._summary()
        fresh = filter_recent_posts(
            [_post(raw_ts="4d")], max_age_hours=72, now=_NOW, summary=summary,
        )
        assert fresh == []
        assert summary["posts_dropped_stale"] == 1

    def test_unparseable_dropped_fail_closed(self):
        summary = self._summary()
        fresh = filter_recent_posts(
            [_post(raw_ts="???")], max_age_hours=72, now=_NOW, summary=summary,
        )
        assert fresh == []
        assert summary["posts_dropped_no_timestamp"] == 1

    def test_future_timestamp_dropped_fail_closed(self):
        """A stamp hours in the future means the column's meaning drifted —
        trusting it would let the whole export read as fresh forever (the
        inverse of the filter's job)."""
        summary = self._summary()
        future = (_NOW + timedelta(hours=5)).isoformat()
        fresh = filter_recent_posts(
            [_post(raw_ts=future)], max_age_hours=72, now=_NOW,
            summary=summary,
        )
        assert fresh == []
        assert summary["posts_dropped_no_timestamp"] == 1

    def test_window_boundary_env_style(self):
        summary = self._summary()
        fresh = filter_recent_posts(
            [_post(raw_ts="47h"), _post(url="u2", raw_ts="49h")],
            max_age_hours=48, now=_NOW, summary=summary,
        )
        assert len(fresh) == 1
        assert summary["posts_dropped_stale"] == 1


# ── row normalization (posts + engager workers) ──────────────────────


def _posts_row(**overrides):
    """One posts-worker export row (the worker's column set)."""
    base = {
        "postUrl": "https://www.linkedin.com/posts/poster-1_share-123",
        "postContent": (
            "Another week rebuilding the production plan by hand"
        ),
        "likeCount": "5",
        "commentCount": "2",
        "postTimestamp": "2h",
        "imgUrl": "", "videoUrl": "",
        "profileUrl": "https://www.linkedin.com/in/poster-1",
        "author": "Dana Okafor",
        "authorUrl": "https://www.linkedin.com/in/poster-1",
        "action": "Post",
        "timestamp": "2026-08-24T10:00:00.000Z",
    }
    base.update(overrides)
    return base


def _gated_post(**overrides):
    """A post dict as the orchestrator hands it to candidate builders
    (post-recency, post-topic-gate: language + posted_at attached)."""
    post = _post_from_row(_posts_row())
    post["language"] = "en"
    post["query_id"] = "q1"
    post["posted_at"] = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
    post.update(overrides)
    return post


class TestPostFromRow:
    def test_worker_columns_map(self):
        post = _post_from_row(_posts_row())
        assert post["post_url"].startswith("https://www.linkedin.com/posts/")
        assert "production plan" in post["text"]
        assert post["raw_timestamp"] == "2h"
        assert post["poster_profile_url"] == (
            "https://www.linkedin.com/in/poster-1"
        )
        assert post["poster_name"] == "Dana Okafor"
        assert post["like_count"] == 5
        assert post["comment_count"] == 2

    def test_author_url_wins_over_profile_url(self):
        post = _post_from_row(_posts_row(
            authorUrl="https://www.linkedin.com/in/the-author",
            profileUrl="https://www.linkedin.com/in/other",
        ))
        assert post["poster_profile_url"] == (
            "https://www.linkedin.com/in/the-author"
        )

    def test_absent_counts_read_zero_unparseable_read_none(self):
        """An EMPTY count column reads 0 (a renamed column then trips the
        all-zero alarm); a present-but-unparseable value reads None — the
        orchestrator treats unknown as engagement-present, so an unknown
        abbreviation never silently skips a live post."""
        post = _post_from_row(_posts_row(likeCount="", commentCount="n/a"))
        assert post["like_count"] == 0
        assert post["comment_count"] is None

    @pytest.mark.parametrize(
        ("raw", "value"),
        [
            ("1,204", 1204), ("1.204", 1204), ("12", 12),
            # LinkedIn abbreviates the biggest counts — the ones that must
            # NOT read as zero.
            ("1.2K", 1200), ("3K", 3000), ("1,2 mil", 1200),
            ("2M", 2_000_000),
        ],
    )
    def test_count_formats_parse(self, raw, value):
        post = _post_from_row(_posts_row(likeCount=raw))
        assert post["like_count"] == value


class TestPosterCandidate:
    def test_poster_shape(self):
        cand = _poster_candidate(_gated_post(), snippet_max_chars=280)
        assert cand is not None
        assert cand["_pain_source_type"] == "poster"
        assert cand["fullName"] == "Dana Okafor"
        assert cand["title"] == ""  # only exists post-enrichment
        assert cand["company"] == ""
        assert cand["defaultProfileUrl"] == (
            "https://www.linkedin.com/in/poster-1"
        )
        assert "production plan" in cand["_pain_snippet"]
        assert cand["_pain_language"] == "en"
        assert cand["_pain_post_at"].startswith("2026-08-24T10:00")

    def test_requires_author_url(self):
        assert (
            _poster_candidate(
                _gated_post(poster_profile_url=""), snippet_max_chars=280
            )
            is None
        )


def _worker_row(**overrides):
    base = {
        "profileLink": "https://www.linkedin.com/in/rin-alvarez",
        "firstName": "Rin", "lastName": "Alvarez", "fullName": "Rin Alvarez",
        "occupation": "Head of Planning",
        "degree": "2nd",
        "comment": "",
    }
    base.update(overrides)
    return base


class TestEngagerWorkerCandidate:
    def test_requires_profile_url(self):
        assert (
            _engager_worker_candidate(
                _worker_row(profileLink=""), _gated_post(),
                source_type="liker", snippet_max_chars=280,
            )
            is None
        )

    def test_liker_shape_context_from_post(self):
        cand = _engager_worker_candidate(
            _worker_row(), _gated_post(),
            source_type="liker", snippet_max_chars=280,
        )
        assert cand is not None
        assert cand["_pain_source_type"] == "liker"
        assert cand["fullName"] == "Rin Alvarez"
        assert cand["title"] == "Head of Planning"
        assert cand["company"] == ""  # only exists post-enrichment
        assert cand["_pain_degree"] == "2nd"
        # A liker didn't write anything — the matched post is the context.
        assert "production plan" in cand["_pain_snippet"]
        assert cand["_pain_post_url"].startswith(
            "https://www.linkedin.com/posts/"
        )
        assert cand["_pain_language"] == "en"

    def test_commenter_uses_their_own_words(self):
        cand = _engager_worker_candidate(
            _worker_row(comment="This happens to me every single week"),
            _gated_post(),
            source_type="commenter", snippet_max_chars=280,
        )
        assert cand is not None
        assert cand["_pain_source_type"] == "commenter"
        assert cand["_pain_snippet"] == (
            "This happens to me every single week"
        )

    def test_commenter_empty_comment_falls_back_to_post_text(self):
        cand = _engager_worker_candidate(
            _worker_row(comment="  "), _gated_post(),
            source_type="commenter", snippet_max_chars=280,
        )
        assert cand is not None
        assert "production plan" in cand["_pain_snippet"]

    def test_split_name_columns_combine(self):
        cand = _engager_worker_candidate(
            _worker_row(fullName=""), _gated_post(),
            source_type="liker", snippet_max_chars=280,
        )
        assert cand is not None
        assert cand["fullName"] == "Rin Alvarez"

    def test_clean_snippet_collapses_and_truncates(self):
        assert _clean_snippet("a  b\n\nc", 280) == "a b c"
        long = "x" * 300
        out = _clean_snippet(long, 280)
        assert len(out) <= 280 and out.endswith("…")


# ── never-contact denylist hard block ────────────────────────────────


class TestDenylistBlock:
    def _summary(self):
        return {"denylist_blocked": 0}

    def test_denylisted_company_dropped(self):
        summary = self._summary()
        kept = _drop_denylisted(
            [
                {"fullName": "Ola Berg", "company": _DENYLISTED_COMPANY,
                 "title": "", "defaultProfileUrl": "u1"},
                {"fullName": "Rin Alvarez", "company": "Northwind Foods",
                 "title": "", "defaultProfileUrl": "u2"},
            ],
            summary,
        )
        assert [r["fullName"] for r in kept] == ["Rin Alvarez"]
        assert summary["denylist_blocked"] == 1

    def test_denylisted_in_headline_dropped_pre_enrichment(self):
        """Pre-enrichment the company field is empty — the check must catch
        the denylisted org in the engager headline so no SN scrape is
        spent."""
        summary = self._summary()
        kept = _drop_denylisted(
            [{"fullName": "Ola Berg", "company": "",
              "title": f"Director @ {_DENYLISTED_COMPANY}",
              "defaultProfileUrl": "u1"}],
            summary,
        )
        assert kept == []
        assert summary["denylist_blocked"] == 1

    def test_unconfigured_denylist_is_a_no_op(self, monkeypatch):
        """A default install configures no denylist — the gate must then
        pass every candidate through, not blank the lane."""
        from workflows import weekly_prospect

        monkeypatch.setattr(
            "scripts.seed_botdog_blacklist.denylist_tokens", lambda: ()
        )
        assert weekly_prospect.is_denylisted_candidate(
            _DENYLISTED_COMPANY, "Ola Berg", "Director"
        ) is False


# ── lane entry attrs ─────────────────────────────────────────────────


class TestLaneEntryAttrs:
    def test_full_metadata_and_cohort_stamp(self):
        attrs = lane_entry_attrs_for({
            "_pain_source_type": "commenter",
            "_pain_snippet": "the plan falls apart mid-week",
            "_pain_post_url": "https://li.example/posts/1",
            "_pain_post_at": "2026-08-24T10:00:00+00:00",
        })
        assert attrs["prospect_source"] == "pain_signal"
        assert attrs["pain_source_type"] == "commenter"
        assert attrs["pain_snippet"] == "the plan falls apart mid-week"
        assert attrs["source_post_url"] == "https://li.example/posts/1"
        assert attrs["source_post_at"] == "2026-08-24T10:00:00+00:00"
        assert attrs["experiment_id"] == PAIN_SIGNAL_EXPERIMENT_ID
        assert attrs["experiment_id_frozen_at"] == "prospect"

    def test_empty_optionals_omitted_and_safe_default_frame(self):
        attrs = lane_entry_attrs_for({})
        assert "pain_snippet" not in attrs
        assert "source_post_url" not in attrs
        assert "source_post_at" not in attrs
        # Missing source type defaults to the SAFE reference frame
        # (engagement — never claims authorship), not "poster".
        assert attrs["pain_source_type"] == "liker"
        assert attrs["prospect_source"] == "pain_signal"
        assert attrs["experiment_id"] == PAIN_SIGNAL_EXPERIMENT_ID


# ── templates ────────────────────────────────────────────────────────

_TEMPLATE_LANGUAGES = [Language.ES, Language.EN, Language.PT]


class TestPainSignalTemplates:
    @pytest.mark.parametrize("language", _TEMPLATE_LANGUAGES)
    @pytest.mark.parametrize(
        "source_type", ["poster", "commenter", "liker"]
    )
    def test_all_languages_and_source_types_resolve(
        self, language, source_type
    ):
        body = get_pain_signal_note(language, source_type=source_type)
        assert "[Name]" in body

    def test_commenter_shares_the_engagement_frame(self):
        """A commenter did not write the post — they get the liker
        (engagement-frame) template verbatim."""
        for language in _TEMPLATE_LANGUAGES:
            assert get_pain_signal_note(
                language, source_type="commenter"
            ) == get_pain_signal_note(language, source_type="liker")

    def test_unknown_source_type_raises(self):
        with pytest.raises(MissingMessageError):
            get_pain_signal_note(Language.ES, source_type="reactor")

    def test_missing_language_body_raises(self, monkeypatch):
        """A language the operator hasn't written pain copy for must RAISE
        so the caller falls back to the persona note LOUDLY — never
        silently ship another language's note."""
        from models import campaign

        messages = json.loads(json.dumps(load_messages()))
        del messages["pain_signal"]["connection_note_poster"]["pt"]
        monkeypatch.setattr(campaign, "load_messages", lambda: messages)
        with pytest.raises(MissingMessageError):
            get_pain_signal_note(Language.PT, source_type="poster")

    @pytest.mark.parametrize("language", _TEMPLATE_LANGUAGES)
    @pytest.mark.parametrize(
        "source_type", ["poster", "commenter", "liker"]
    )
    def test_rendered_notes_clean_and_within_invite_limit(
        self, language, source_type
    ):
        """No unresolved [placeholders] after personalize (the pre-send
        guard would abort the batch) and within LinkedIn's 300-char
        invitation-note ceiling even with a long first name."""
        import re

        body = get_pain_signal_note(language, source_type=source_type)
        rendered = personalize(body, "Maximiliano", "", language=language)
        assert not re.search(r"\[[^\[\]\n]+\]", rendered)
        assert len(rendered) <= 300

    def test_group_not_a_persona(self):
        """pain_signal must never become a Persona enum value —
        Persona.from_attio falls back silently on unknown values, so a
        pseudo-persona would ship wrong copy without a trace."""
        from models.campaign import Persona

        assert "pain_signal" not in {p.value for p in Persona}
        assert "pain_signal" in load_messages()

    def test_only_name_placeholder(self):
        """Pain templates must carry ONLY [Name] — the dry-run preview
        renders without industry resolution, so any other placeholder would
        diverge from the wire (or trip the pre-send guard)."""
        import re

        for source_type in ("poster", "commenter", "liker"):
            for language in _TEMPLATE_LANGUAGES:
                body = get_pain_signal_note(language, source_type=source_type)
                assert set(re.findall(r"\[[^\]]+\]", body)) == {"[Name]"}

    def test_shipped_neutral_group_carries_the_sentinel(self):
        """The repo-root default must ship placeholder pain copy so the
        content guard blocks a live send under the engine's own text."""
        from pathlib import Path

        repo_root = Path(__file__).resolve().parent.parent
        shipped = json.loads(
            (repo_root / "content" / "messages.json").read_text()
        )
        group = shipped["pain_signal"]
        assert set(group) == {
            "connection_note_poster", "connection_note_liker"
        }
        for bodies in group.values():
            assert set(bodies) == {"es", "en", "pt"}
            assert all(PLACEHOLDER_SENTINEL in b for b in bodies.values())


# ── phantom launchers (csvName namespaces + arg shapes) ──────────────


# Posts-worker saved argument: spreadsheetUrl is the input; the volume
# knobs are console-managed and must survive the save.
_SAVED_POSTS_ARGS = json.dumps({
    "spreadsheetUrl": "https://stale-console-url",
    "numberMaxOfPosts": 50,
    "numberOfLinesPerLaunch": 1,
    "sortByRecentPosts": True,
    "reprocessAll": True,
    "sessionCookie": "stale-console-cookie",
    "userAgent": "stale-console-ua",
})

_SEARCH_URL = (
    "https://www.linkedin.com/search/results/content/"
    "?keywords=plan&sortBy=%22date_posted%22"
)


class TestWorkerLaunch:
    """Save-then-launch contract for the workflow's worker phantoms:
    workers read their SAVED argument; per-launch `arguments` are never
    relied on."""

    def _pb(
        self, *, save_sticks=True, clobber_field=None,
        log_output="[done_]✅ CSV saved at https://s3/result.csv",
        csv="postUrl\n",
    ):
        pb = MagicMock()
        state = {
            "argument": _SAVED_POSTS_ARGS,
            "name": "posts worker", "fileMgmt": "delete",
            "launchType": "repeatedly",
        }

        def _agent(agent_id):
            return dict(state)

        pb.get_agent.side_effect = _agent

        def _save(agent_id, argument):
            if save_sticks:
                state["argument"] = json.dumps(argument)
            if clobber_field:
                state[clobber_field] = "CLOBBERED"

        pb.save_agent_argument.side_effect = _save
        pb.wait_for_completion.return_value = MagicMock(
            container_id="c-1", log_output=log_output
        )
        pb.download_result_csv.return_value = csv
        return pb

    def test_saves_merged_args_then_launches_bare(self, monkeypatch):
        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "fresh-cookie")
        monkeypatch.setenv("PB_LI_USER_AGENT", "fresh-ua")
        pb = self._pb()
        assert _launch_posts_scrape(pb, "posts-w", _SEARCH_URL) == "postUrl\n"
        saved = pb.save_agent_argument.call_args.args[1]
        # Saved console shape rides along (the volume knobs are
        # console-managed and must survive) …
        assert saved["numberMaxOfPosts"] == 50
        assert saved["sortByRecentPosts"] is True
        # … with only the managed keys overridden per query.
        assert saved["spreadsheetUrl"] == _SEARCH_URL
        assert saved["sessionCookie"] == "fresh-cookie"
        assert saved["userAgent"] == "fresh-ua"
        # csvName is NOT sent — the worker writes its own per-launch
        # result.csv (file storage: delete previous).
        assert "csvName" not in saved
        # The launch is BARE: the worker reads the SAVED args.
        assert pb.launch_agent.call_args.args == ("posts-w",)
        assert pb.wait_for_completion.call_args.kwargs["max_wait"] == 900
        assert pb.download_result_csv.call_args.kwargs == {}

    def test_engager_worker_gets_post_url_and_keeps_watcher_mode(
        self, monkeypatch
    ):
        """The engager workers get the postUrl override; their saved
        watcherMode flag is deliberately untouched (vendor-version
        dependent semantics)."""
        from workflows.pain_signal import _launch_engager_worker_scrape

        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb()
        state_arg = json.loads(_SAVED_POSTS_ARGS)
        state_arg["watcherMode"] = True
        saved_state = {"argument": json.dumps(state_arg),
                       "name": "w", "fileMgmt": "delete",
                       "launchType": "repeatedly"}
        pb.get_agent.side_effect = lambda agent_id: dict(saved_state)

        def _save(agent_id, argument):
            saved_state["argument"] = json.dumps(argument)

        pb.save_agent_argument.side_effect = _save
        _launch_engager_worker_scrape(
            pb, "com-w", "https://li.example/posts/1"
        )
        saved = pb.save_agent_argument.call_args.args[1]
        assert saved["postUrl"] == "https://li.example/posts/1"
        assert saved["watcherMode"] is True  # untouched
        assert pb.launch_agent.call_args.args == ("com-w",)

    def test_save_that_does_not_stick_refuses_to_launch(self, monkeypatch):
        """A failed/partial /agents/save would silently re-run the PREVIOUS
        query's saved search — verify-then-launch."""
        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb(save_sticks=False)
        with pytest.raises(RuntimeError, match="did not stick"):
            _launch_posts_scrape(pb, "posts-w", _SEARCH_URL)
        pb.launch_agent.assert_not_called()

    def test_clobbered_console_field_refuses_to_launch(self, monkeypatch):
        """The vendor's /agents/save partial-update semantics are assumed,
        not owned — if a save wipes a console-managed field (file storage,
        name, launch type), stop before launching anything."""
        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb(clobber_field="fileMgmt")
        with pytest.raises(RuntimeError, match="clobbered"):
            _launch_posts_scrape(pb, "posts-w", _SEARCH_URL)
        pb.launch_agent.assert_not_called()

    def test_no_results_marker_is_quiet_zero(self, monkeypatch):
        """No CSV + 'No results found' in the log = the worker ran and found
        nothing new (its cross-launch dedup) — returned as "" so the caller
        counts a quiet day, not a failure."""
        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb(
            log_output="[info_]ℹ️ No results found for this search.",
            csv=None,
        )
        assert _launch_posts_scrape(pb, "posts-w", _SEARCH_URL) == ""

    def test_missing_csv_without_marker_is_failure(self, monkeypatch):
        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb(log_output="[done_]✅ done", csv=None)
        assert _launch_posts_scrape(pb, "posts-w", _SEARCH_URL) is None

    def test_timeout_with_finished_status_salvages_csv(self, monkeypatch):
        """PB's latest-run pointer can lag, so wait_for_completion times out
        with last_observed_status='finished'. The worker's per-launch file
        IS this launch's CSV — salvage instead of consuming the posts for
        nothing."""
        from clients.pb_envelope import PBRunTimeout

        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb(csv="postUrl\nrow\n")
        pb.wait_for_completion.side_effect = PBRunTimeout(
            container_id="c-1", agent_id="posts-w", elapsed_seconds=900,
            last_observed_status="finished", last_observed_output={},
        )
        assert _launch_posts_scrape(pb, "posts-w", _SEARCH_URL) == (
            "postUrl\nrow\n"
        )

    def test_timeout_without_finished_status_propagates(self, monkeypatch):
        from clients.pb_envelope import PBRunTimeout

        monkeypatch.setenv("PB_LI_SESSION_COOKIE", "cookie")
        pb = self._pb()
        pb.wait_for_completion.side_effect = PBRunTimeout(
            container_id="c-1", agent_id="posts-w", elapsed_seconds=900,
            last_observed_status="running", last_observed_output={},
        )
        with pytest.raises(PBRunTimeout):
            _launch_posts_scrape(pb, "posts-w", _SEARCH_URL)

    def test_requires_cookie(self, monkeypatch):
        monkeypatch.delenv("PB_LI_SESSION_COOKIE", raising=False)
        pb = self._pb()
        with pytest.raises(RuntimeError, match="PB_LI_SESSION_COOKIE"):
            _launch_posts_scrape(pb, "posts-w", _SEARCH_URL)
        pb.save_agent_argument.assert_not_called()
        pb.launch_agent.assert_not_called()


class TestEnrichmentLaunch:
    def test_single_url_uses_ps_enr_namespace(self, monkeypatch):
        pb = MagicMock()
        pb.download_result_csv.return_value = "linkedinProfileUrl\n"
        with patch(
            "workflows.daily_check_helpers.build_sales_nav_launch_args",
            return_value={"identities": [{}], "spreadsheetUrl": "u",
                          "numberOfProfilesPerLaunch": 1},
        ) as build_mock:
            _launch_enrichment_scrape(
                pb, "sn-agent", ["https://linkedin.com/in/ana"],
                dry_run=False,
            )
        # Bare URL → no sheet write, raw count of 1.
        assert build_mock.call_args.kwargs["spreadsheet_url"] == (
            "https://linkedin.com/in/ana"
        )
        assert build_mock.call_args.kwargs["launch_count"] == 1
        args = pb.launch_agent.call_args.args[1]
        assert args["csvName"].startswith("ps-enr")
        assert not args["csvName"].startswith(("deg", "wk"))
        assert (
            pb.download_result_csv.call_args.kwargs["csv_name"]
            == args["csvName"]
        )

    def test_multi_url_goes_through_headerless_sheet(
        self, monkeypatch
    ):
        pb = MagicMock()
        pb.download_result_csv.return_value = "linkedinProfileUrl\n"
        urls = [f"https://linkedin.com/in/p{i}" for i in range(3)]
        with (
            patch(
                "clients.google_sheets.write_prospects_to_sheet",
                return_value="https://docs.google.com/spreadsheets/d/sheet-1",
            ) as sheet_mock,
            patch(
                "workflows.daily_check_helpers.build_sales_nav_launch_args",
                return_value={"identities": [{}]},
            ) as build_mock,
        ):
            _launch_enrichment_scrape(pb, "sn-agent", urls, dry_run=False)
        assert sheet_mock.call_args.kwargs["columns"] == ["profileUrl"]
        assert sheet_mock.call_args.kwargs["spreadsheet_id"] is None
        assert sheet_mock.call_args.kwargs["include_header"] is False
        assert build_mock.call_args.kwargs["launch_count"] == 3

    def test_dry_run_multi_url_requires_sandbox_sheet(self, monkeypatch):
        monkeypatch.delenv("GSHEET_DRYRUN_ID", raising=False)
        with pytest.raises(RuntimeError, match="GSHEET_DRYRUN_ID"):
            _launch_enrichment_scrape(
                MagicMock(), "sn-agent",
                ["https://linkedin.com/in/a", "https://linkedin.com/in/b"],
                dry_run=True,
            )

    def test_dry_run_multi_url_targets_sandbox_sheet(self, monkeypatch):
        monkeypatch.setenv("GSHEET_DRYRUN_ID", "sandbox-sheet")
        pb = MagicMock()
        pb.download_result_csv.return_value = "linkedinProfileUrl\n"
        with (
            patch(
                "clients.google_sheets.write_prospects_to_sheet",
                return_value="https://docs.google.com/spreadsheets/d/sandbox",
            ) as sheet_mock,
            patch(
                "workflows.daily_check_helpers.build_sales_nav_launch_args",
                return_value={"identities": [{}]},
            ),
        ):
            _launch_enrichment_scrape(
                pb, "sn-agent",
                ["https://linkedin.com/in/a", "https://linkedin.com/in/b"],
                dry_run=True,
            )
        assert sheet_mock.call_args.kwargs["spreadsheet_id"] == "sandbox-sheet"


class TestEnrichmentMerge:
    def test_merges_on_query_echo_and_counts(self):
        candidates = [
            {"defaultProfileUrl": "https://www.linkedin.com/in/rin-alvarez",
             "title": "Head of Planning", "company": "", "location": ""},
            {"defaultProfileUrl": "https://www.linkedin.com/in/no-row",
             "title": "Headline only", "company": "", "location": ""},
        ]
        csv_text = (
            "query,linkedinProfileUrl,headline,currentCompanyName,location\n"
            "https://www.linkedin.com/in/rin-alvarez,"
            "https://www.linkedin.com/in/rin-alvarez-canonical-123,"
            "Head of Planning at Northwind,Northwind Foods,Lisbon\n"
        )
        summary = {"enriched": 0}
        _merge_enrichment(candidates, csv_text, summary)
        assert summary["enriched"] == 1
        assert candidates[0]["company"] == "Northwind Foods"
        assert candidates[0]["title"] == "Head of Planning at Northwind"
        assert candidates[0]["location"] == "Lisbon"
        # The unmatched candidate keeps its engager headline — an SN column
        # rename must never blank titles.
        assert candidates[1]["title"] == "Headline only"
        assert candidates[1]["company"] == ""

    def test_blank_sn_fields_never_erase_engager_data_or_count(self):
        """A matched-but-EMPTY row (the SN phantom's per-profile error
        shape) must neither blank engager data NOR count as enriched —
        counting it would let a fully-dead enrichment print 'Enriched N/N'
        and structurally suppress the ENRICHMENT DEGRADED alarm."""
        candidates = [
            {"defaultProfileUrl": "https://linkedin.com/in/rin",
             "title": "Head of Planning", "company": "", "location": ""},
        ]
        csv_text = (
            "query,linkedinProfileUrl,headline,currentCompanyName\n"
            "https://linkedin.com/in/rin,https://linkedin.com/in/rin,,\n"
        )
        summary = {"enriched": 0}
        _merge_enrichment(candidates, csv_text, summary)
        assert candidates[0]["title"] == "Head of Planning"
        assert summary["enriched"] == 0
        assert not candidates[0].get("_enriched")

    def test_slug_variant_bridged_by_identity_key(self):
        """The SN scraper can echo a profile under its CURRENT slug while
        the engager export carried an old vanity slug (the PR-276/278
        lesson). When the slug carries a member-id suffix, the li-id
        identity key bridges the rename."""
        candidates = [
            {"defaultProfileUrl":
             "https://www.linkedin.com/in/rin-a-123456789",
             "title": "old headline", "company": "", "location": ""},
        ]
        csv_text = (
            "linkedinProfileUrl,headline,currentCompanyName\n"
            "https://www.linkedin.com/in/"
            "rin-alvarez-mba-123456789,"
            "Plant Director,Northwind Foods\n"
        )
        summary = {"enriched": 0}
        _merge_enrichment(candidates, csv_text, summary)
        assert summary["enriched"] == 1
        assert candidates[0]["company"] == "Northwind Foods"


# ── _commit_prospect lane-attrs merge ────────────────────────────────


def _commit_attio_mock():
    attio = MagicMock()
    attio.search_company_by_domain.return_value = None
    attio.search_companies.return_value = []
    attio.create_company.return_value = {"id": {"record_id": "comp-X"}}
    attio.upsert_person.return_value = {"id": {"record_id": "person-X"}}
    attio.add_list_entry.return_value = {}
    attio.create_note.return_value = None
    attio._find_list_entries_for_record.return_value = []
    attio._filter_and_rank_entries_for_record.return_value = []
    attio.query_list_entries.return_value = []
    return attio


class TestCommitLaneAttrsMerge:
    def test_lane_attrs_merge_and_override_cohort_stamp(self, monkeypatch):
        from workflows import weekly_prospect
        from workflows.weekly_prospect import _commit_prospect

        monkeypatch.setattr(
            weekly_prospect, "get_current_experiment_id",
            lambda *a, **k: "exp-global-running",
        )
        attio = _commit_attio_mock()
        ok = _commit_prospect(
            AttioProvider(attio),
            {"name": "Rin Alvarez", "title": "Head of Planning",
             "company": "Northwind Foods", "location": "",
             "linkedin_url": "https://linkedin.com/in/rin"},
            {"fullName": "Rin Alvarez"},
            {"pass": True, "score": 80, "persona": "operations_leaders",
             "language": "en", "reasons": []},
            "list-id",
            "2026-08-24",
            lane_entry_attrs=lane_entry_attrs_for({
                "_pain_source_type": "commenter",
                "_pain_snippet": "the plan falls apart mid-week",
                "_pain_post_url": "https://li.example/posts/1",
            }),
        )
        assert ok is True
        entry_attrs = attio.add_list_entry.call_args.kwargs["entry_attributes"]
        assert entry_attrs["prospect_source"] == "pain_signal"
        assert entry_attrs["pain_source_type"] == "commenter"
        assert entry_attrs["pain_snippet"] == "the plan falls apart mid-week"
        assert entry_attrs["source_post_url"] == "https://li.example/posts/1"
        # The lane's explicit cohort stamp WINS over the running experiment.
        assert entry_attrs["experiment_id"] == PAIN_SIGNAL_EXPERIMENT_ID
        assert entry_attrs["experiment_id_frozen_at"] == "prospect"

    def test_default_none_is_behavior_preserving(self, monkeypatch):
        from workflows import weekly_prospect
        from workflows.weekly_prospect import _commit_prospect

        monkeypatch.setattr(
            weekly_prospect, "get_current_experiment_id",
            lambda *a, **k: "exp-global-running",
        )
        attio = _commit_attio_mock()
        _commit_prospect(
            AttioProvider(attio),
            {"name": "Rin Alvarez", "title": "Head of Planning",
             "company": "Northwind Foods", "location": "",
             "linkedin_url": "https://linkedin.com/in/rin"},
            {"fullName": "Rin Alvarez"},
            {"pass": True, "score": 80, "persona": "operations_leaders",
             "language": "en", "reasons": []},
            "list-id",
            "2026-08-24",
        )
        entry_attrs = attio.add_list_entry.call_args.kwargs["entry_attributes"]
        assert entry_attrs["experiment_id"] == "exp-global-running"
        assert "prospect_source" not in entry_attrs


# ── daily invite path: pain-note selection ───────────────────────────


def _invite_attrs(record_id: str, **extra) -> dict:
    return {
        "record_id": record_id,
        "entry_id": f"entry-{record_id}",
        "stage": "Prospect",
        "persona": "operations_leaders",
        "language": "es",
        "scoring_lane": "legacy",
        "quality_score": 70,
        "invite_eligible_after": None,
        "experiment_id": PAIN_SIGNAL_EXPERIMENT_ID,
        "experiment_id_frozen_at": "prospect",
        **extra,
    }


def _invite_attio():
    attio = MagicMock()
    attio._person_to_company = {}
    attio.get_company.return_value = {"values": {}}
    attio.company_hq_country_code.return_value = None
    attio.person_language_override.return_value = None
    return attio


def _invite_cache():
    cache = MagicMock()
    cache.get.side_effect = lambda rid: (
        f"Name {rid}", f"Company {rid}", f"https://linkedin.com/in/{rid}",
        None, "Director",
    )
    return cache


class TestInvitePathPainNoteSelection:
    def _run(self, attrs):
        from workflows.daily_check import _build_invite_send_data

        personalize_mock = MagicMock(return_value="NOTE")
        with (
            patch(
                "workflows.daily_check.get_message",
                return_value="PERSONA TEMPLATE",
            ),
            patch("workflows.daily_check.personalize", personalize_mock),
            patch(
                "workflows.daily_check.get_industry_label",
                return_value="manufacturing",
            ),
            patch("workflows.daily_check.escalate"),
        ):
            to_send, counts = _build_invite_send_data(
                [attrs],
                target=1,
                attio=_invite_attio(),
                cache=_invite_cache(),
                today=date(2026, 8, 24),
                audit_logger=None,
                dry_run=True,
            )
        return to_send, counts, personalize_mock

    def test_pain_entry_gets_pain_template(self):
        to_send, counts, personalize_mock = self._run(
            _invite_attrs(
                "r1", prospect_source="pain_signal", pain_source_type="poster",
            )
        )
        assert len(to_send) == 1
        template = personalize_mock.call_args.args[0]
        assert template == get_pain_signal_note(
            Language.ES, source_type="poster"
        )
        # `counts` is the skip-count contract summed by the lane merge and
        # the backfill predicate — the pain branch must never add keys to
        # it (a success-shaped counter would KeyError the merge or suppress
        # the residual re-scan).
        assert set(counts) == {
            "company_throttled", "same_company_run", "missing_language",
            "language_mismatch", "missing_copy", "missing_url", "send_guard_skipped",
        }

    @pytest.mark.parametrize("source_type", ["commenter", "liker"])
    def test_engager_entry_gets_engagement_template(self, source_type):
        """Commenter entries resolve to the same engagement-frame note as
        likers."""
        _, _, personalize_mock = self._run(
            _invite_attrs(
                "r1", prospect_source="pain_signal",
                pain_source_type=source_type,
            )
        )
        template = personalize_mock.call_args.args[0]
        assert template == get_pain_signal_note(
            Language.ES, source_type="liker"
        )

    def test_non_pain_entry_untouched(self):
        _, _, personalize_mock = self._run(_invite_attrs("r1"))
        assert personalize_mock.call_args.args[0] == "PERSONA TEMPLATE"

    def test_missing_pain_copy_falls_back_to_persona_note_loudly(
        self, monkeypatch, capsys
    ):
        from models import campaign

        messages = json.loads(json.dumps(load_messages()))
        messages["pain_signal"] = {}
        monkeypatch.setattr(campaign, "load_messages", lambda: messages)
        _, _, personalize_mock = self._run(
            _invite_attrs(
                "r1", prospect_source="pain_signal", pain_source_type="poster",
            )
        )
        assert personalize_mock.call_args.args[0] == "PERSONA TEMPLATE"
        assert "falling back" in capsys.readouterr().err

    def test_missing_source_type_never_guesses_poster(self, capsys):
        """An entry whose pain_source_type select came back empty must NOT
        get the poster note ("you wrote this post") — the safe direction is
        the persona note, loudly."""
        _, counts, personalize_mock = self._run(
            _invite_attrs("r1", prospect_source="pain_signal")
        )
        assert personalize_mock.call_args.args[0] == "PERSONA TEMPLATE"
        assert counts["missing_copy"] == 0
        assert "falling back" in capsys.readouterr().err

    def test_pain_note_survives_missing_persona_copy(self):
        """Pain-first resolution: a pain entry whose persona template is
        MISSING still sends its pain note (no missing_copy skip)."""
        from workflows.daily_check import _build_invite_send_data

        personalize_mock = MagicMock(return_value="NOTE")
        with (
            patch(
                "workflows.daily_check.get_message",
                side_effect=MissingMessageError(
                    persona="operations_leaders", language="es",
                    dm_step="connection_note",
                ),
            ),
            patch("workflows.daily_check.personalize", personalize_mock),
            patch(
                "workflows.daily_check.get_industry_label",
                return_value="manufacturing",
            ),
            patch("workflows.daily_check.escalate") as escalate_mock,
        ):
            to_send, counts = _build_invite_send_data(
                [_invite_attrs(
                    "r1", prospect_source="pain_signal",
                    pain_source_type="commenter",
                )],
                target=1,
                attio=_invite_attio(),
                cache=_invite_cache(),
                today=date(2026, 8, 24),
                audit_logger=None,
                dry_run=True,
            )
        assert len(to_send) == 1
        assert counts["missing_copy"] == 0
        escalate_mock.assert_not_called()
        assert personalize_mock.call_args.args[0] == get_pain_signal_note(
            Language.ES, source_type="liker"
        )


# ── end-to-end (test seams, mocked CRM) ──────────────────────────────


_POSTS_HEADER = (
    "postUrl,postContent,likeCount,commentCount,postTimestamp,imgUrl,"
    "videoUrl,profileUrl,author,authorUrl,action,timestamp\n"
)

# On-topic post text for the default `"production plan"` query — posts must
# pass the topic gate to reach the downstream phases.
_ON_TOPIC = "Another week rebuilding the production plan by hand"

_POSTS_CSV = _POSTS_HEADER + (
    # One fresh on-topic post; its author becomes the poster candidate and
    # its counts trigger both engager-worker scrapes.
    f'https://li.example/posts/fresh,"{_ON_TOPIC}",5,2,2h,,,'
    "https://www.linkedin.com/in/poster-1,Dana Okafor,"
    "https://www.linkedin.com/in/poster-1,Post,"
    "2026-08-24T10:00:00.000Z\n"
)

_COMMENTERS_HEADER = (
    "profileLink,firstName,lastName,fullName,occupation,degree,comment\n"
)

_COMMENTERS_CSV = _COMMENTERS_HEADER + (
    # A commenter — their own words become the snippet.
    "https://www.linkedin.com/in/commenter-1,Rin,Alvarez,Rin Alvarez,"
    "Head of Planning,3rd,This happens to me every week with rush orders\n"
)

_LIKERS_HEADER = (
    "profileLink,firstName,lastName,fullName,occupation,degree\n"
)

_LIKERS_CSV = _LIKERS_HEADER + (
    # A liker at the operator's denylisted org (headline) → hard-blocked.
    "https://www.linkedin.com/in/liker-denied,Ola,Berg,Ola Berg,"
    f"Director @ {_DENYLISTED_COMPANY},2nd\n"
    # A 1st-degree liker (already connected) → dropped, no invite to send.
    "https://www.linkedin.com/in/liker-first,Tomas,Rey,Tomas Rey,"
    "Plant Manager,1st\n"
    # A row without a profile URL → counted, dropped.
    ",No,Url,No Url,Analyst,2nd\n"
)

_ENRICH_CSV = (
    "query,linkedinProfileUrl,fullName,headline,currentCompanyName,location\n"
    "https://www.linkedin.com/in/poster-1,"
    "https://www.linkedin.com/in/poster-1,Dana Okafor,"
    "Operations Director,Northwind Foods,Lisbon\n"
    "https://www.linkedin.com/in/commenter-1,"
    "https://www.linkedin.com/in/commenter-1,Rin Alvarez,"
    "Head of Planning,Contoso Mills,Porto\n"
)


def _migrated_schema_request(method, path, **kwargs):
    """crm.inner_client._request stub for a fully-migrated workspace (pain
    attrs + the commenter select option)."""
    if path.endswith("/options"):
        return {"data": [
            {"title": "poster"}, {"title": "commenter"}, {"title": "liker"},
        ]}
    return {"data": [{"api_slug": "prospect_source"}]}


def _score_pass_stub(prospect_data, persona_config=None, **kwargs):
    return {
        "pass": True,
        "score": 80,
        "persona": "operations_leaders",
        "language": "en",
        "reasons": [],
        "scoring_lane": "legacy",
        "verdict_path": "enterprise_pass",
    }


def _lane_crm(attio=None):
    """A CRMProvider over a commit-ready raw mock, plus the reads the lane's
    pipeline snapshot performs."""
    attio = attio or _commit_attio_mock()
    attio.search_person_by_linkedin.return_value = None
    attio.bulk_fetch_persons_by_record_ids.return_value = {}
    return attio, AttioProvider(attio)


def _stub_commit_deps(monkeypatch):
    from workflows import weekly_prospect

    monkeypatch.setattr(weekly_prospect, "score_prospect", _score_pass_stub)
    monkeypatch.setattr(
        weekly_prospect, "_enrich_prospect_industry", lambda *a, **k: None
    )
    monkeypatch.setattr(
        weekly_prospect, "match_or_create_company", lambda *a, **k: "comp-1"
    )
    monkeypatch.setattr(
        weekly_prospect, "get_current_experiment_id",
        lambda *a, **k: "exp-global-running",
    )


@pytest.fixture
def _lane_env(monkeypatch, tmp_path):
    """Approved registry + enabled lane + fully mocked commit deps."""
    monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
    monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
    registry = _registry(
        queries=[
            {"id": "en-plan", "language": "en",
             "query": '"production plan"'},
        ],
    )
    registry["config"] = {
        "post_max_age_hours_default": 24,
        "max_engagers_per_run": 40,
        "pain_snippet_max_chars": 280,
    }
    path = tmp_path / "pain_keywords.json"
    path.write_text(json.dumps(registry))

    _stub_commit_deps(monkeypatch)
    import workflows.industry_classifier as industry_classifier

    monkeypatch.setattr(
        industry_classifier, "build_anthropic_client", lambda: None
    )
    # The wet path hard-requires the LLM dispatch path (borderline verdict
    # availability gate); the score stub bypasses quality_gate so no
    # dispatch actually fires.
    import workflows.llm_dispatch as llm_dispatch

    monkeypatch.setattr(llm_dispatch, "is_dispatch_enabled", lambda: True)

    attio, crm = _lane_crm()
    # Schema preflight (wet): the list carries the pain attrs AND the
    # 'commenter' select option.
    attio._request.side_effect = _migrated_schema_request
    # Distinct record ids per upsert — a single shared id would make the
    # second commit read as already-listed (restamp skip) and hide it.
    attio.upsert_person.side_effect = (
        {"id": {"record_id": f"person-{n}"}} for n in range(100)
    )

    def run(dry_run, scrape_enrichment=None):
        return attio, run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "com-w", "lik-w", "sn-agent",
            dry_run=dry_run,
            keywords_path=path,
            scrape_posts=lambda url: _POSTS_CSV,
            scrape_commenters=lambda post_url: _COMMENTERS_CSV,
            scrape_likers=lambda post_url: _LIKERS_CSV,
            scrape_enrichment=scrape_enrichment
            or (lambda urls: _ENRICH_CSV),
        )

    return run


class TestEndToEnd:
    def test_dry_run_writes_nothing(self, _lane_env, capsys):
        attio, summary = _lane_env(dry_run=True)
        attio.add_list_entry.assert_not_called()
        attio.upsert_person.assert_not_called()
        assert summary["posts_found"] == 1
        assert summary["posts_fresh"] == 1
        assert summary["posts_on_topic"] == 1
        assert summary["posts_dropped_offtopic"] == 0
        assert summary["commenter_scrapes"] == 1
        assert summary["liker_scrapes"] == 1
        assert summary["engager_rows"] == 4  # 1 commenter + 3 liker rows
        assert summary["engager_rows_no_url"] == 1
        assert summary["engagers_first_degree"] == 1
        # The liker at the operator's denylisted org.
        assert summary["denylist_blocked"] == 1
        # poster + commenter survive to scoring
        assert summary["candidates"] == 2
        assert summary["scored"] == 2
        assert summary["enriched"] == 2
        out = capsys.readouterr().out
        assert "[DRY RUN]" in out
        assert "client-side recency window 24h" in out
        # The author's poster-note preview vs the commenter's engagement note.
        assert "I saw your post" in out
        assert "we crossed paths" in out
        # Enrichment surfaced the SN company in the preview.
        assert "Contoso Mills" in out

    def test_wet_run_commits_with_lane_metadata(self, _lane_env):
        attio, summary = _lane_env(dry_run=False)
        assert summary["added"] == 2
        assert attio.add_list_entry.call_count == 2
        by_source = {}
        for call in attio.add_list_entry.call_args_list:
            attrs = call.kwargs["entry_attributes"]
            by_source[attrs["pain_source_type"]] = attrs
        poster = by_source["poster"]
        commenter = by_source["commenter"]
        assert poster["prospect_source"] == "pain_signal"
        assert poster["source_post_url"] == "https://li.example/posts/fresh"
        assert "production plan" in poster["pain_snippet"]
        assert poster["experiment_id"] == PAIN_SIGNAL_EXPERIMENT_ID
        # The commenter's snippet is THEIR comment, not the post text.
        assert commenter["pain_snippet"] == (
            "This happens to me every week with rush orders"
        )
        assert commenter["experiment_id"] == PAIN_SIGNAL_EXPERIMENT_ID
        assert poster["source_post_at"]
        assert commenter["source_post_at"]

    def test_scrape_failure_is_contained(self, monkeypatch, tmp_path):
        # One failing query must not kill the run: the second query still
        # scrapes and the failure is counted.
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        registry = _registry(
            queries=[
                {"id": "q1", "language": "en", "query": "a"},
                {"id": "q2", "language": "en", "query": "b"},
            ],
        )
        path = tmp_path / "kw2.json"
        path.write_text(json.dumps(registry))
        calls = {"n": 0}

        def flaky(url):
            calls["n"] += 1
            if calls["n"] == 1:
                raise ValueError("phantom exploded")
            return _POSTS_HEADER

        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=flaky,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["scrape_failures"] == 1
        assert summary["queries_run"] == 1

    def test_worker_quiet_zero_is_not_a_failure(self, monkeypatch, tmp_path):
        """The posts worker's cross-launch dedup makes '' (its 'No results
        found' log line) a legitimate quiet day, distinct from a missing
        CSV."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        registry = _registry()
        path = tmp_path / "kwq.json"
        path.write_text(json.dumps(registry))
        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: "",
            scrape_enrichment=lambda urls: "",
        )
        assert summary["scrape_failures"] == 0
        assert summary["queries_run"] == 1
        assert summary["posts_found"] == 0

    def test_circuit_breaker_stops_launches_after_consecutive_failures(
        self, monkeypatch, tmp_path, capsys
    ):
        """A deterministic launch failure must not burn a live launch per
        remaining query — but the run itself continues (salvage over
        abort)."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        registry = _registry(
            queries=[
                {"id": f"q{i}", "language": "en", "query": "a"}
                for i in range(6)
            ],
        )
        path = tmp_path / "kwcb.json"
        path.write_text(json.dumps(registry))
        calls = {"n": 0}

        def always_broken(url):
            calls["n"] += 1
            raise ValueError("deterministic regression")

        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=always_broken,
            scrape_enrichment=lambda urls: "",
        )
        assert calls["n"] == 3  # not 6 — the breaker stopped the burn
        assert summary["circuit_breaker_tripped"] is True
        assert summary["scrape_failures"] == 3
        assert "CIRCUIT BREAKER" in capsys.readouterr().err

    def test_circuit_breaker_in_engager_phase_keeps_posters(
        self, monkeypatch, tmp_path, capsys
    ):
        """When the engager-worker launches fail deterministically, the
        already-harvested posters (the high-precision, no-extra-scrape half)
        must still flow to scoring, not be discarded with the abort. Posts
        are consumed by the worker's cross-launch dedup, so a discard is
        permanent."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        registry = _registry()
        path = tmp_path / "kwcb2.json"
        path.write_text(json.dumps(registry))
        # Four on-topic engaged posts → four poster candidates.
        posts_csv = _POSTS_HEADER + "".join(
            f'https://li.example/posts/{i},"{_ON_TOPIC}",1,1,2h,,,'
            f"https://www.linkedin.com/in/author-{i},Author {i},"
            f"https://www.linkedin.com/in/author-{i},Post,x\n"
            for i in range(4)
        )
        engager_calls = {"n": 0}

        def broken_engagers(post_url):
            engager_calls["n"] += 1
            raise ValueError("worker contract regression")

        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "com-w", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: posts_csv,
            scrape_commenters=broken_engagers,
            scrape_likers=broken_engagers,
            scrape_enrichment=lambda urls: "",
        )
        assert engager_calls["n"] == 3  # breaker stopped the burn
        assert summary["circuit_breaker_tripped"] is True
        # The harvest survived: all four posters scored.
        assert summary["candidates"] == 4
        assert summary["scored"] == 4
        assert "CIRCUIT BREAKER" in capsys.readouterr().err

    def test_seams_are_never_paced(self, monkeypatch, tmp_path):
        """Test seams must never hit the real inter-launch sleep — a seam
        supplied for one worker type must not turn pacing on for
        another."""
        import workflows.pain_signal as ps

        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)

        def no_sleep(seconds):
            raise AssertionError(f"real sleep({seconds}) fired in a test")

        monkeypatch.setattr(ps.time, "sleep", no_sleep)
        registry = _registry()
        path = tmp_path / "kwpace.json"
        path.write_text(json.dumps(registry))
        two_posts = _POSTS_HEADER + "".join(
            f'https://li.example/posts/{i},"{_ON_TOPIC}",1,1,2h,,,'
            f"https://www.linkedin.com/in/a{i},Author {i},"
            f"https://www.linkedin.com/in/a{i},Post,x\n"
            for i in range(2)
        )
        _, crm = _lane_crm()
        # Likers seam only — commenters unconfigured: pacing must not fire
        # for either.
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: two_posts,
            scrape_likers=lambda post_url: _LIKERS_HEADER,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["liker_scrapes"] == 2

    def test_unparseable_engagement_count_still_scrapes(
        self, monkeypatch, tmp_path, capsys
    ):
        """An abbreviated/unknown count must not read as zero — the densest
        posts are exactly the abbreviated ones."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        registry = _registry()
        path = tmp_path / "kwunp.json"
        path.write_text(json.dumps(registry))
        weird_counts = _POSTS_HEADER + (
            f'https://li.example/posts/1,"{_ON_TOPIC}",n/a,0,2h,,,'
            "https://www.linkedin.com/in/a1,Author One,"
            "https://www.linkedin.com/in/a1,Post,x\n"
        )
        liker_calls: list[str] = []

        def capture_likers(post_url):
            liker_calls.append(post_url)
            return _LIKERS_HEADER

        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: weird_counts,
            scrape_likers=capture_likers,
            scrape_enrichment=lambda urls: "",
        )
        assert liker_calls == ["https://li.example/posts/1"]
        assert summary["posts_engagement_unparseable"] == 1
        assert "can't parse" in capsys.readouterr().err

    def test_engager_workers_unconfigured_is_posters_only_and_loud(
        self, monkeypatch, tmp_path, capsys
    ):
        """Missing engager-worker ids skip those scrapes LOUDLY; the poster
        half still runs (the PR-282 posters-only posture)."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        registry = _registry()
        path = tmp_path / "kwpo.json"
        path.write_text(json.dumps(registry))
        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: _POSTS_CSV,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["candidates"] == 1  # the poster only
        assert summary["commenter_scrapes"] == 0
        assert summary["liker_scrapes"] == 0
        err = capsys.readouterr().err
        assert "engager worker(s) not configured" in err
        assert "commenters, likers" in err

    def test_enrichment_failure_degrades_loudly(self, _lane_env, capsys):
        def broken(urls):
            raise ValueError("SN scraper exploded")

        _, summary = _lane_env(dry_run=True, scrape_enrichment=broken)
        assert summary["enrich_failures"] == 1
        assert summary["enriched"] == 0
        # Candidates still scored — on their engager headline.
        assert summary["scored"] == 2
        err = capsys.readouterr().err
        assert "enrichment scrape failed" in err
        assert "ENRICHMENT DEGRADED" in err

    def test_empty_enrichment_csv_counts_as_failure(self, _lane_env, capsys):
        _, summary = _lane_env(
            dry_run=True, scrape_enrichment=lambda urls: None
        )
        assert summary["enrich_failures"] == 1
        assert summary["scored"] == 2
        assert "returned no CSV" in capsys.readouterr().err


class TestTopicGateE2E:
    """LinkedIn's 'exact-phrase' search matches off-topic posts (a
    work-anniversary post, a job ad) — a post's people are accepted only
    when its text carries an enabled query's phrase."""

    def _env(self, monkeypatch, tmp_path, *, queries=None):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        registry = _registry(queries=queries)
        path = tmp_path / "kw-topic.json"
        path.write_text(json.dumps(registry))
        _, crm = _lane_crm()
        return crm, path

    def _run(self, crm, path, csv_text, **kwargs):
        kwargs.setdefault("scrape_posts", lambda url: csv_text)
        kwargs.setdefault("scrape_enrichment", lambda urls: "")
        return run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True, keywords_path=path,
            **kwargs,
        )

    def test_offtopic_post_dropped_loudly(
        self, monkeypatch, tmp_path, capsys
    ):
        """A work-anniversary post that LinkedIn returned for a scheduling
        phrase: its author/engagers must never get an invite note claiming
        the post was about scheduling."""
        crm, path = self._env(
            monkeypatch, tmp_path,
            queries=[{
                "id": "en-sched", "language": "en",
                "query": '"production scheduling"',
            }],
        )
        csv_text = _POSTS_HEADER + (
            # Off-topic: the anniversary post (phrase absent).
            'https://li.example/posts/anniv,"30 years of history and pride '
            'at Northwind Foods",8,0,3h,,,'
            "https://www.linkedin.com/in/anniv-1,Rosa Lima,"
            "https://www.linkedin.com/in/anniv-1,Post,x\n"
            # On-topic: phrase present.
            'https://li.example/posts/real,"Production scheduling here is '
            'still a spreadsheet",1,0,2h,,,'
            "https://www.linkedin.com/in/ontopic-1,Rin Alvarez,"
            "https://www.linkedin.com/in/ontopic-1,Post,x\n"
        )
        summary = self._run(crm, path, csv_text)
        assert summary["posts_dropped_offtopic"] == 1
        assert summary["posts_on_topic"] == 1
        assert summary["candidates"] == 1  # the on-topic post's author
        err = capsys.readouterr().err
        assert "off-topic post dropped" in err
        assert "Northwind Foods" in err

    def test_empty_post_text_drops_fail_closed(self, monkeypatch, tmp_path):
        """No post text = no way to verify the note's topic claim."""
        crm, path = self._env(monkeypatch, tmp_path)
        csv_text = _POSTS_HEADER + (
            "https://li.example/posts/1,,1,0,2h,,,"
            "https://www.linkedin.com/in/empty-1,Rin Alvarez,"
            "https://www.linkedin.com/in/empty-1,Post,x\n"
        )
        summary = self._run(crm, path, csv_text)
        assert summary["posts_dropped_empty_text"] == 1
        assert summary["candidates"] == 0

    def test_cross_query_reattribution_switches_language(
        self, monkeypatch, tmp_path, capsys
    ):
        """The posts worker dedups across launches, so a post surfaces
        exactly ONCE — possibly under a sibling query. A post carrying
        another enabled query's phrase is re-attributed (that query's
        language then picks the note template) instead of dropped."""
        crm, path = self._env(
            monkeypatch, tmp_path,
            queries=[
                {"id": "en-q", "language": "en",
                 "query": '"production plan"'},
                {"id": "pt-q", "language": "pt",
                 "query": '"plano de produção"'},
            ],
        )
        # Served under the EN query's launch, but the text is PT.
        csv_text = _POSTS_HEADER + (
            'https://li.example/posts/pt,"O plano de produção se desfaz na '
            'metade da semana",1,0,2h,,,'
            "https://www.linkedin.com/in/pt-author,Joana Silva,"
            "https://www.linkedin.com/in/pt-author,Post,x\n"
        )

        calls = {"n": 0}

        def seam(url):
            # Only the first (EN) launch returns the post; the PT launch
            # finds nothing new (worker cross-launch dedup).
            result = csv_text if calls["n"] == 0 else ""
            calls["n"] += 1
            return result

        summary = self._run(crm, path, None, scrape_posts=seam)
        assert summary["posts_dropped_offtopic"] == 0
        assert summary["posts_on_topic"] == 1
        assert summary["candidates"] == 1
        # The preview renders under the PT frame — re-attributed.
        assert "[pt/poster]" in capsys.readouterr().out

    def test_stale_post_dropped(self, monkeypatch, tmp_path):
        crm, path = self._env(monkeypatch, tmp_path)
        csv_text = _POSTS_HEADER + (
            f'https://li.example/posts/old,"{_ON_TOPIC}",1,0,4d,,,'
            "https://www.linkedin.com/in/old-1,Rin Alvarez,"
            "https://www.linkedin.com/in/old-1,Post,x\n"
        )
        summary = self._run(crm, path, csv_text)
        assert summary["posts_dropped_stale"] == 1
        assert summary["candidates"] == 0

    def test_unparseable_post_timestamp_drops_fail_closed(
        self, monkeypatch, tmp_path
    ):
        crm, path = self._env(monkeypatch, tmp_path)
        csv_text = _POSTS_HEADER + (
            f'https://li.example/posts/1,"{_ON_TOPIC}",1,0,???,,,'
            "https://www.linkedin.com/in/x,Rin Alvarez,"
            "https://www.linkedin.com/in/x,Post,x\n"
        )
        summary = self._run(crm, path, csv_text)
        assert summary["posts_dropped_no_timestamp"] == 1
        assert summary["candidates"] == 0


class TestDedupCapAndPreDrop:
    def _base_env(self, monkeypatch, tmp_path, *, queries, config=None):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        registry = _registry(queries=queries)
        if config is not None:
            registry["config"] = config
        path = tmp_path / "kw-dedup.json"
        path.write_text(json.dumps(registry))
        _, crm = _lane_crm()
        return crm, path

    @staticmethod
    def _one_post_csv(
        post_url="https://li.example/posts/1", text="post a",
        likes="1", comments="1",
        author_url="https://www.linkedin.com/in/the-author",
        author="Pat Author",
    ):
        return _POSTS_HEADER + (
            f'{post_url},"{text}",{likes},{comments},2h,,,'
            f"{author_url},{author},{author_url},Post,x\n"
        )

    def test_engager_of_two_posts_dedupes_to_richer_source(
        self, monkeypatch, tmp_path
    ):
        """The same person liking one matched post and commenting on another
        collapses to ONE candidate — the commenter row (their words beat a
        bare reaction)."""
        crm, path = self._base_env(
            monkeypatch, tmp_path,
            queries=[
                {"id": "q1", "language": "en", "query": "a"},
                {"id": "q2", "language": "en", "query": "b"},
            ],
        )
        posts = iter([
            self._one_post_csv(
                post_url="https://li.example/posts/1", text="post a",
                likes="1", comments="0",
                author_url="https://www.linkedin.com/in/author-1",
            ),
            self._one_post_csv(
                post_url="https://li.example/posts/2", text="post b",
                likes="0", comments="1",
                author_url="https://www.linkedin.com/in/author-2",
            ),
        ])
        rin_liker = _LIKERS_HEADER + (
            "https://www.linkedin.com/in/rin-alvarez,Rin,Alvarez,Rin Alvarez,"
            "Head of Planning,2nd\n"
        )
        rin_commenter = _COMMENTERS_HEADER + (
            "https://www.linkedin.com/in/rin-alvarez,Rin,Alvarez,Rin Alvarez,"
            "Head of Planning,2nd,same here\n"
        )
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "com-w", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: next(posts),
            scrape_commenters=lambda post_url: rin_commenter,
            scrape_likers=lambda post_url: rin_liker,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["engagers_deduped"] == 1
        # 2 authors + Rin (deduped to her commenter row) = 3
        assert summary["candidates"] == 3
        assert summary["posts_on_topic"] == 2

    def test_candidate_cap_keeps_richest_sources_first(
        self, monkeypatch, tmp_path, capsys
    ):
        crm, path = self._base_env(
            monkeypatch, tmp_path,
            queries=[{"id": "q1", "language": "en", "query": "a"}],
            config={"max_engagers_per_run": 1},
        )
        enriched_urls: list[list[str]] = []

        def capture_enrich(urls):
            enriched_urls.append(list(urls))
            return ""

        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: self._one_post_csv(likes="1"),
            scrape_likers=lambda post_url: _LIKERS_HEADER + (
                "https://www.linkedin.com/in/liker-1,Rin,Alvarez,"
                "Rin Alvarez,Head of Planning,2nd\n"
            ),
            scrape_enrichment=capture_enrich,
        )
        assert summary["engagers_capped"] == 1
        assert summary["candidates"] == 1
        # The poster outranks the liker AND the cap bounds the SN scrape
        # input (a viral post must not flood the phantom).
        assert enriched_urls == [
            ["https://www.linkedin.com/in/the-author"]
        ]
        assert "capped at 1" in capsys.readouterr().err

    def test_engager_post_cap_bounds_worker_launches(
        self, monkeypatch, tmp_path, capsys
    ):
        """A broad day must not burn an engager-worker launch per post —
        only the freshest max_engager_scrape_posts_per_run posts get engager
        scrapes (posters of the rest still processed)."""
        crm, path = self._base_env(
            monkeypatch, tmp_path,
            queries=[{"id": "q1", "language": "en", "query": "a"}],
            config={"max_engager_scrape_posts_per_run": 1},
        )
        two_posts = _POSTS_HEADER + (
            'https://li.example/posts/1,"post a",1,0,2h,,,'
            "https://www.linkedin.com/in/a1,A One,"
            "https://www.linkedin.com/in/a1,Post,x\n"
            'https://li.example/posts/2,"post a",1,0,3h,,,'
            "https://www.linkedin.com/in/a2,A Two,"
            "https://www.linkedin.com/in/a2,Post,x\n"
        )
        liker_calls: list[str] = []

        def capture_likers(post_url):
            liker_calls.append(post_url)
            return _LIKERS_HEADER

        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: two_posts,
            scrape_likers=capture_likers,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["posts_engager_capped"] == 1
        # Freshest post (2h) got the scrape; both posters survive.
        assert liker_calls == ["https://li.example/posts/1"]
        assert summary["candidates"] == 2
        assert "engager scrapes capped at 1" in capsys.readouterr().err

    def test_in_pipeline_people_dropped_before_enrichment(
        self, monkeypatch, tmp_path, capsys
    ):
        """A person already in the pipeline list must cost no SN scrape."""
        from clients.attio import _canonical_linkedin_url
        from workflows import weekly_prospect

        crm, path = self._base_env(
            monkeypatch, tmp_path,
            queries=[{"id": "q1", "language": "en", "query": "a"}],
        )
        known = _canonical_linkedin_url(
            "https://www.linkedin.com/in/the-author"
        )
        monkeypatch.setattr(
            weekly_prospect, "_load_in_list_canonical_urls",
            lambda *a, **k: {known},
        )
        enriched_urls: list[list[str]] = []

        def capture_enrich(urls):
            enriched_urls.append(list(urls))
            return ""

        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "lik-w", "sn-w",
            dry_run=True, keywords_path=path,
            scrape_posts=lambda url: self._one_post_csv(likes="1"),
            scrape_likers=lambda post_url: _LIKERS_HEADER + (
                "https://www.linkedin.com/in/fresh-1,Rin,Alvarez,"
                "Rin Alvarez,Head of Planning,2nd\n"
            ),
            scrape_enrichment=capture_enrich,
        )
        assert summary["engagers_already_in_pipeline"] == 1
        assert summary["candidates"] == 1
        assert enriched_urls == [["https://www.linkedin.com/in/fresh-1"]]
        assert "already in the pipeline list" in capsys.readouterr().out


class TestGuardsAndAlarms:
    """Dispatch gate, schema preflight, empty-candidates guard, silent-drop
    counters, shared denylist gate."""

    def _minimal_registry(self, tmp_path, name="kw3.json", config=None):
        registry = _registry()
        if config is not None:
            registry["config"] = config
        path = tmp_path / name
        path.write_text(json.dumps(registry))
        return path

    def _posts_csv(self):
        return _POSTS_CSV

    def test_wet_without_dispatch_refuses_to_score(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        attio, crm = _lane_crm()
        # The schema preflight runs pre-spend — satisfy it so the run
        # reaches the dispatch gate this test is about.
        attio._request.side_effect = _migrated_schema_request
        with pytest.raises(RuntimeError, match="OUTBOUND_USE_LLM_DISPATCH"):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "sn-w",
                dry_run=False,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=lambda url: self._posts_csv(),
                scrape_enrichment=lambda urls: "",
            )
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()

    def test_dry_without_dispatch_warns_but_previews(
        self, monkeypatch, tmp_path, capsys
    ):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _stub_commit_deps(monkeypatch)
        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True,
            keywords_path=self._minimal_registry(tmp_path),
            scrape_posts=lambda url: self._posts_csv(),
            scrape_enrichment=lambda urls: "",
        )
        assert summary["candidates"] == 1
        assert (
            "OUTBOUND_USE_LLM_DISPATCH is not enabled"
            in capsys.readouterr().err
        )

    def test_empty_candidates_skip_pipeline_load(self, monkeypatch, tmp_path):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        attio, crm = _lane_crm()
        enrich_seam = MagicMock(
            side_effect=AssertionError("no candidates → no enrichment")
        )
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True,
            keywords_path=self._minimal_registry(tmp_path),
            scrape_posts=lambda url: _POSTS_HEADER,
            scrape_enrichment=enrich_seam,
        )
        assert summary["candidates"] == 0
        attio.query_list_entries.assert_not_called()
        enrich_seam.assert_not_called()

    def test_wet_unmigrated_schema_fails_before_any_write(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        import workflows.llm_dispatch as llm_dispatch

        monkeypatch.setattr(llm_dispatch, "is_dispatch_enabled", lambda: True)
        attio, crm = _lane_crm()
        attio._request.return_value = {"data": [{"api_slug": "stage"}]}
        with pytest.raises(RuntimeError, match="setup_attio_schema"):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "sn-w",
                dry_run=False,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=lambda url: self._posts_csv(),
                scrape_enrichment=lambda urls: "",
            )
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()

    def test_wet_schema_without_commenter_option_fails_before_writes(
        self, monkeypatch, tmp_path
    ):
        """A workspace migrated against an earlier revision has the pain
        attrs but not the 'commenter' select option — the preflight must
        refuse (the first commenter commit would otherwise 400 AFTER
        upsert_person: orphaned person, dead run)."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        import workflows.llm_dispatch as llm_dispatch

        monkeypatch.setattr(llm_dispatch, "is_dispatch_enabled", lambda: True)
        _stub_commit_deps(monkeypatch)
        attio, crm = _lane_crm()

        def stale_schema(method, path, **kwargs):
            if path.endswith("/options"):
                return {"data": [{"title": "poster"}, {"title": "liker"}]}
            return {"data": [{"api_slug": "prospect_source"}]}

        attio._request.side_effect = stale_schema
        with pytest.raises(RuntimeError, match="commenter"):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "sn-w",
                dry_run=False,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=lambda url: self._posts_csv(),
                scrape_enrichment=lambda urls: "",
            )
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()

    def test_missing_sn_cookie_refuses_pre_spend(self, monkeypatch, tmp_path):
        """A deterministic enrichment-config failure (missing SN cookie)
        must refuse BEFORE the first scrape — not burn every scrape and
        degrade per chunk."""
        from workflows.daily_check_helpers import SalesNavConfigError

        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        monkeypatch.delenv("PB_LI_SALES_NAV_SESSION_COOKIE", raising=False)
        seam = MagicMock(side_effect=AssertionError("must not scrape"))
        _, crm = _lane_crm()
        with pytest.raises(SalesNavConfigError):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "sn-agent",
                dry_run=True,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=seam,
            )
        seam.assert_not_called()

    def test_dry_run_without_sandbox_sheet_refuses_pre_spend(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "sn-cookie")
        monkeypatch.delenv("GSHEET_DRYRUN_ID", raising=False)
        seam = MagicMock(side_effect=AssertionError("must not scrape"))
        _, crm = _lane_crm()
        with pytest.raises(RuntimeError, match="GSHEET_DRYRUN_ID"):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "sn-agent",
                dry_run=True,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=seam,
            )
        seam.assert_not_called()

    def test_all_queries_dead_raises_discovery_alarm(
        self, monkeypatch, tmp_path, capsys
    ):
        """A 0-for-N scrape sweep is an infrastructure failure and must
        never read as a quiet no-results day."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True,
            keywords_path=self._minimal_registry(tmp_path),
            scrape_posts=lambda url: None,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["queries_run"] == 0
        assert "DISCOVERY DEAD" in capsys.readouterr().err

    def test_missing_csv_counts_as_scrape_failure(self, monkeypatch, tmp_path):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _, crm = _lane_crm()
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True,
            keywords_path=self._minimal_registry(tmp_path),
            scrape_posts=lambda url: None,  # download failed / no file
            scrape_enrichment=lambda urls: "",
        )
        assert summary["scrape_failures"] == 1
        assert summary["queries_run"] == 0

    def test_posters_without_author_url_alarm(
        self, monkeypatch, tmp_path, capsys
    ):
        """Column drift on the author-URL fields must not silently zero the
        poster half of the lane."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _, crm = _lane_crm()
        csv_text = _POSTS_HEADER + (
            f'https://li.example/posts/1,"{_ON_TOPIC}",0,0,2h,,,'
            ",No Url,,Post,x\n"
        )
        summary = run_pain_signal_discovery(
            crm, MagicMock(), "posts-w", "", "", "sn-w",
            dry_run=True,
            keywords_path=self._minimal_registry(tmp_path),
            scrape_posts=lambda url: csv_text,
            scrape_enrichment=lambda urls: "",
        )
        assert summary["posters_no_profile_url"] == 1
        assert summary["candidates"] == 0
        assert "POSTERS DEAD" in capsys.readouterr().err

    def test_missing_posts_worker_refuses_without_seam(
        self, monkeypatch, tmp_path
    ):
        """No posts worker means no lane — refuse before any spend."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        _, crm = _lane_crm()
        with pytest.raises(RuntimeError, match="PB_PAIN_POSTS_WORKER_ID"):
            run_pain_signal_discovery(
                crm, MagicMock(), "", "", "", "sn-w",
                dry_run=True,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_enrichment=lambda urls: "",
            )

    def test_missing_sn_scraper_refuses_without_seam(
        self, monkeypatch, tmp_path
    ):
        """Enrichment is a lane premise (candidates carry no company) — no
        SN scraper id and no seam must refuse before any spend."""
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        monkeypatch.setenv("ATTIO_LIST_ID", "list-1")
        seam = MagicMock(side_effect=AssertionError("must not scrape"))
        _, crm = _lane_crm()
        with pytest.raises(RuntimeError, match="SALES_NAV"):
            run_pain_signal_discovery(
                crm, MagicMock(), "posts-w", "", "", "",
                dry_run=True,
                keywords_path=self._minimal_registry(tmp_path),
                scrape_posts=seam,
            )
        seam.assert_not_called()


class TestSharedIngestGates:
    def test_process_prospects_blocks_denylist_for_every_lane(self):
        """The denylist gate lives in _process_prospects itself — the weekly
        lane (no lane params at all) inherits it."""
        from workflows.weekly_prospect import (
            _process_prospects,
            new_process_summary,
        )

        _, crm = _lane_crm()
        summary = new_process_summary()
        _process_prospects(
            [{
                "fullName": "Ola Berg",
                "title": "Director",
                "companyName": _DENYLISTED_COMPANY,
                "defaultProfileUrl": "https://linkedin.com/in/ola",
            }],
            crm,
            list_id="list-1",
            today="2026-08-24",
            dry_run=True,
            summary=summary,
            seen_urls=set(),
            in_list_record_ids=set(),
        )
        assert summary["denylist_blocked"] == 1
        assert summary["scored"] == 0

    def test_new_process_summary_covers_contract_keys(self):
        from workflows.weekly_prospect import new_process_summary

        keys = set(new_process_summary())
        assert {
            "exported", "scored", "qualified", "duplicates", "rejected",
            "added", "net_new_created", "restamped_existing",
            "borderline_staged", "reprospect_review", "dedup_gate_degraded",
            "deterministic_qualified", "size_abstained", "industry_missing",
            "write_errors", "denylist_blocked", "rejected_by_path",
        } <= keys

    def test_default_language_only_applies_without_a_location(
        self, monkeypatch
    ):
        """The lane's per-query language wins only when the row carries NO
        location; a row WITH one keeps the scorer's verdict."""
        from workflows import weekly_prospect
        from workflows.weekly_prospect import (
            _process_prospects,
            new_process_summary,
        )

        def _score_es(prospect_data, persona_config=None, **kwargs):
            return {**_score_pass_stub(prospect_data), "language": "es"}

        monkeypatch.setattr(weekly_prospect, "score_prospect", _score_es)
        monkeypatch.setattr(
            weekly_prospect, "_enrich_prospect_industry",
            lambda *a, **k: None,
        )

        def _capture(sink):
            def _commit(crm_, pd, raw, sr, *a, **k):
                sink.update(sr)
                return True

            return _commit

        _, crm = _lane_crm()
        for row, expected in (
            ({"fullName": "A", "defaultProfileUrl": "https://li/in/a"}, "pt"),
            ({"fullName": "B", "defaultProfileUrl": "https://li/in/b",
              "location": "Madrid, Spain"}, "es"),
        ):
            captured: dict = {}
            monkeypatch.setattr(
                weekly_prospect, "_commit_prospect", _capture(captured)
            )
            _process_prospects(
                [row], crm, list_id="list-1", today="2026-08-24",
                dry_run=False, summary=new_process_summary(),
                seen_urls=set(), in_list_record_ids=set(),
                default_language="pt",
            )
            assert captured["language"] == expected


# ── CLI wiring: the off-by-default posture ───────────────────────────


class TestPainSignalCli:
    def _invoke(self, argv):
        from click.testing import CliRunner

        from cli import cli

        return CliRunner().invoke(cli, argv, catch_exceptions=False)

    def test_disabled_aborts_before_any_work(self, monkeypatch):
        """Default install: the command refuses and names the flag. Nothing
        is constructed — no CRM client, no PB client, no lock."""
        monkeypatch.delenv(PAIN_SIGNAL_ENABLED_ENV, raising=False)
        result = self._invoke(
            ["pain-signal", "--posts-worker-id", "w", "--dry-run"]
        )
        assert result.exit_code == 1
        assert "ABORT" in result.stderr
        assert PAIN_SIGNAL_ENABLED_ENV in result.stderr

    def test_enabled_but_unconfigured_posts_worker_aborts(self, monkeypatch):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        result = self._invoke(
            ["pain-signal", "--posts-worker-id", "",
             "--sales-nav-profile-scraper-id", "sn", "--dry-run"]
        )
        assert result.exit_code == 1
        assert "posts worker is not configured" in result.stderr

    def test_placeholder_worker_id_is_treated_as_unconfigured(
        self, monkeypatch
    ):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        result = self._invoke(
            ["pain-signal",
             "--posts-worker-id", "REPLACE_WITH_POSTS_WORKER_PHANTOM_ID",
             "--sales-nav-profile-scraper-id", "sn", "--dry-run"]
        )
        assert result.exit_code == 1
        assert "posts worker is not configured" in result.stderr

    def test_missing_sn_scraper_aborts(self, monkeypatch):
        monkeypatch.setenv(PAIN_SIGNAL_ENABLED_ENV, "1")
        result = self._invoke(
            ["pain-signal", "--posts-worker-id", "w",
             "--sales-nav-profile-scraper-id", "", "--dry-run"]
        )
        assert result.exit_code == 1
        assert "PB_SALES_NAV_PROFILE_SCRAPER_ID" in result.stderr

    @pytest.mark.parametrize(
        ("value", "unconfigured"),
        [
            ("", True), ("REPLACE_WITH_X", True), ("todo", True),
            ("1111111111111111", False),  # synthetic — never a real agent id
        ],
    )
    def test_agent_unconfigured_predicate(self, value, unconfigured):
        from cli import _pb_agent_unconfigured

        assert _pb_agent_unconfigured(value) is unconfigured

    def test_worker_ids_default_from_pb_config(self, monkeypatch):
        """Fork seam: the ids resolve through clients/pb_config (yaml → env →
        ""), read LIVE, so no agent id is ever baked into the tree."""
        from clients.pb_config import load_pb_config

        monkeypatch.setenv("PB_PAIN_POSTS_WORKER_ID", "posts-123")
        monkeypatch.setenv("PB_PAIN_COMMENTERS_WORKER_ID", "com-456")
        monkeypatch.setenv("PB_PAIN_LIKERS_WORKER_ID", "lik-789")
        cfg = load_pb_config()
        assert cfg.pain_posts_worker_id == "posts-123"
        assert cfg.pain_commenters_worker_id == "com-456"
        assert cfg.pain_likers_worker_id == "lik-789"

    def test_worker_ids_default_empty_without_config(self, monkeypatch):
        from clients.pb_config import load_pb_config

        for var in (
            "PB_PAIN_POSTS_WORKER_ID",
            "PB_PAIN_COMMENTERS_WORKER_ID",
            "PB_PAIN_LIKERS_WORKER_ID",
        ):
            monkeypatch.delenv(var, raising=False)
        cfg = load_pb_config()
        assert cfg.pain_posts_worker_id == ""
        assert cfg.pain_commenters_worker_id == ""
        assert cfg.pain_likers_worker_id == ""

    def test_example_yaml_ships_no_agent_ids(self):
        """The shipped template must never carry a real phantom id."""
        from pathlib import Path

        import yaml

        repo_root = Path(__file__).resolve().parent.parent
        agents = yaml.safe_load(
            (repo_root / "config" / "phantombuster.example.yaml").read_text()
        )["agents"]
        for key in (
            "pain_posts_worker",
            "pain_commenters_worker",
            "pain_likers_worker",
        ):
            assert agents[key] == ""


class TestDailyPhase09Gate:
    """The daily run degrades to ONE visible status line when the lane is
    off — the same ship posture as the radar / email lanes."""

    def test_daily_source_wires_phase_09_behind_the_gate(self):
        """Structural guard: Phase 0.9 must stay gated on
        is_pain_signal_enabled() and must never halt the daily run."""
        import inspect

        import cli as cli_module

        source = inspect.getsource(cli_module)
        assert "--- Phase 0.9: Pain-Signal Discovery ---" in source
        phase = source.split("--- Phase 0.9: Pain-Signal Discovery ---", 1)[1]
        phase = phase.split("Pipeline starvation check", 1)[0]
        assert "is_pain_signal_enabled()" in phase
        assert "unset — " in phase and "off by default" in phase
        # The lane must never take down the sends that run after it: a
        # broad except, a warn, and no re-raise.
        assert "Pain-signal discovery SKIPPED" in phase
        assert "raise" not in phase
