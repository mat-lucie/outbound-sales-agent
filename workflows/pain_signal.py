"""Pain-signal discovery lane: LinkedIn post keyword search → POSTS
(client-side recency + topic gate) → post authors + commenters + likers
→ Sales Nav profile enrichment → the existing qualify pipeline.

OFF BY DEFAULT. Nothing in this module runs until
``OUTBOUND_PAIN_SIGNAL_ENABLED=1`` AND the operator has approved the
keyword registry (``content/pain_keywords.json``). A fresh install ships
a neutral, DELIBERATELY UNAPPROVED placeholder registry, so the lane
fails closed on both axes. See GETTING_STARTED.md § "Pain-signal
discovery lane".

Architecture (PR-284/PR-285 worker reshape). The vendor's "post
commenter and liker scraper" agent is a WORKFLOW SHELL — an orchestrator
whose API launches are no-ops (no worker stage, no search; it re-serves
its cumulative leads DB). The lane therefore drives the workflow's
WORKER phantoms directly:

- POSTS worker (a LinkedIn activity extractor): takes one content-search
  URL per approved keyword query via save-then-launch and exports
  matching POSTS (postUrl, postContent, postTimestamp, likeCount,
  commentCount, author, authorUrl) to its OWN per-launch result.csv. It
  dedups incrementally across launches — a re-run of a query yields only
  posts it has never processed ("No results found" when there are none:
  a quiet zero, not a failure).
- COMMENTERS / LIKERS workers: one launch per surviving post URL,
  exporting the people who engaged with it. Launched only for posts
  whose likeCount/commentCount say there is someone to collect. Both are
  OPTIONAL — an unset worker id skips that engager type LOUDLY.
- Post AUTHORS become `poster` candidates directly from the posts
  export — no extra scrape.

Recency is CLIENT-SIDE on `postTimestamp`: LinkedIn's `datePosted`
content-search filter is broken (differential-tested — with the filter
the same query returns zero results). Candidates carry no company or
location and ICP scoring runs on title, so the capped batch is enriched
through the EXISTING Sales Navigator Profile Scraper before scoring.

Source types:
- `poster` — the post's author (from the posts export). Gets the
  authorship note — the high-precision half.
- `commenter` — wrote a comment; their own comment text becomes
  `pain_snippet` (their own words — the best review context).
- `liker` — reacted only; the matched post's text is the snippet.
Commenters and likers both get the engagement-frame note
(`connection_note_liker`) — it never claims authorship.

Every standard ingest gate applies UNCHANGED because candidates go
through `weekly_prospect._process_prospects` itself: ICP scoring
(`quality_gate.score_prospect`, borderlines resolved inline via the LLM
dispatch path), all four dedup layers (in-run identity keys, in-list
canonical URLs + profile-id keys, live person search, name+company
index), and the operator-review staging for re-prospect candidates. The
per-company throttle and the pre-invite degree check apply at invite
time in the daily run, unchanged. The operator's configured never-contact
denylist is enforced here at ingest, BEFORE any preview or enrichment
spend.

Safety posture:
- The whole lane is gated behind `OUTBOUND_PAIN_SIGNAL_ENABLED=1`
  (default OFF) — the daily run degrades to one status line.
- No scrape launches until the keyword registry's `_meta.status` is
  "approved" with an `approved_by`, and never while it still carries the
  shipped placeholder sentinel.
- Recency is fail-closed CLIENT-SIDE: posts whose `postTimestamp` cannot
  be parsed are DROPPED (never assumed fresh), with counters.
- TOPIC GATE: LinkedIn's exact-phrase content search is not exact (it
  matches work-anniversary posts and job ads that merely mention the
  phrase's words). The invite note claims the post was ABOUT the pain
  phrase, so a post's text must actually contain an enabled query's
  phrase (accent/case-folded; paired queries require every term) before
  the post's people are accepted — attribution goes to the matching
  query (its language picks the note template). Fail-closed: a post with
  EMPTY text is dropped — an unverifiable topic claim never ships. See
  `post_matches_query`.
- LAUNCH CONTRACT: these workers configure themselves from the SAVED
  console argument; per-launch `arguments` on the parent are silent
  no-ops. Every launch SAVES the merged argument, re-fetches to verify
  every overridden key landed, then launches BARE. Three consecutive
  launch failures stop further launches (circuit breaker) — a
  deterministic regression must not burn a live launch per query.
- Engager scrapes are bounded twice: only posts with a nonzero
  like/comment count get the matching worker launch, and at most
  `max_engager_scrape_posts_per_run` posts get engager scrapes per run.
  The assembled batch is then CAPPED (`max_engagers_per_run`) before the
  SN enrichment scrape, so one viral post can never flood the Sales Nav
  phantom.
- 1st-degree engagers are dropped at parse time when the worker export
  carries a degree column (already connected — nothing to invite; the
  daily run's degree check stays authoritative for everyone else).
- People already in the pipeline list are dropped BEFORE enrichment
  (same canonical-URL + profile-id keys `_process_prospects` uses) — a
  known person must not cost an SN scrape every day.
- The lane only COMMITS prospects (Prospect stage, standard quarantine).
  It never sends anything: invites go out later through the daily run
  behind the operator's per-batch review.
- Cohort identity: commits are stamped
  `experiment_id=PAIN_SIGNAL_EXPERIMENT_ID` explicitly (overriding the
  globally-running experiment's stamp) so the learning loop can measure
  pain-signal vs cold without polluting the running DM experiment's
  cohort. The experiments-registry row is the operator's to append at
  wet-go time — the engine allows only ONE `running` experiment.
"""

import csv
import io
import json
import os
import re
import time
import unicodedata
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import click
import httpx

from clients.attio import (
    AttioClient,
    _canonical_linkedin_url,
    linkedin_identity_key,
)
from clients.crm.base import CRMProvider
from clients.pb_envelope import PBRunTimeout
from clients.phantombuster import PhantomBusterClient
from workflows.content_guard import PLACEHOLDER_SENTINEL

PAIN_SIGNAL_ENABLED_ENV = "OUTBOUND_PAIN_SIGNAL_ENABLED"
PAIN_SIGNAL_POST_MAX_AGE_ENV = "OUTBOUND_PAIN_SIGNAL_POST_MAX_AGE_HOURS"

# Cohort identity for every prospect this lane commits. Stamped
# explicitly at commit (frozen_at="prospect") so the cohort is
# measurable by the learning loop regardless of which experiment is
# globally `running`. Appending the matching experiments-registry row is
# the operator's wet-go step.
PAIN_SIGNAL_EXPERIMENT_ID = "exp-pain-signal-invite"

PROSPECT_SOURCE_PAIN_SIGNAL = "pain_signal"

PAIN_KEYWORDS_FILENAME = "pain_keywords.json"


def pain_keywords_path() -> Path:
    """Path to the active keyword registry.

    Resolved at CALL time off `models.campaign.CONTENT_DIR`, so the
    registry follows `OUTBOUND_CONTENT_DIR` exactly like messages.json /
    personas.json (and so a test that repoints the content dir is
    honored).
    """
    from models import campaign

    return Path(campaign.CONTENT_DIR) / PAIN_KEYWORDS_FILENAME


# ── worker launch contract ───────────────────────────────────────────
# The workflow PARENT is a shell: API launches — bare or with
# `arguments` — are no-ops, and setting its launchType to "manually"
# breaks the whole workflow (worker launches then 412 with
# workflowRequiresAutoLaunch). NEVER launch the parent; leave its launch
# type alone. The lane drives the WORKERS directly, save-then-launch
# (persist the merged argument via /agents/save, verify it landed,
# launch BARE):
# - Posts worker: input arg `spreadsheetUrl` = the content-search URL.
#   Writes its OWN per-launch result.csv (file storage: delete previous)
#   with columns postUrl, postContent, likeCount, commentCount,
#   postTimestamp, imgUrl, videoUrl, profileUrl, author, authorUrl,
#   action, timestamp. Dedups incrementally across launches — a re-run
#   yields only never-processed posts and logs "No results found" when
#   there are none (a quiet zero, NOT a failure).
# - Commenters / likers workers: input arg `postUrl`, one launch per
#   post. Their saved watcherMode flag is left UNTOUCHED (its semantics
#   are vendor-version dependent).
POSTS_SEARCH_URL_ARG = "spreadsheetUrl"
ENGAGER_POST_URL_ARG = "postUrl"

# The posts worker's quiet-zero log line: the search ran and every
# matching post was already in its processed DB (or none matched).
# Distinguishes "no new posts" from "no CSV = infra failure".
NO_RESULTS_LOG_MARKER = "no results found"

# Rapid sequential launches on one agent can leave PB's latest-run
# pointer lagging behind our container (observed: PBRunTimeout at 600s
# with last_observed_status="finished"). Wider ceiling + inter-launch
# pacing both address it.
WORKER_LAUNCH_MAX_WAIT = 900
WORKER_INTER_LAUNCH_DELAY = 20  # seconds between real PB launches

# Circuit breaker: after this many CONSECUTIVE scrape failures the run
# stops launching instead of burning a real launch per remaining
# query/post. Auth (401/403) still aborts on the FIRST failure.
MAX_CONSECUTIVE_SCRAPE_FAILURES = 3

# Candidate precedence when the same person surfaces more than once
# (e.g. commented on one matched post, liked another): keep the row
# with the richest — and most precise — review context.
_SOURCE_PRIORITY = {"poster": 2, "commenter": 1, "liker": 0}

# SN Profile Scraper per-launch ceiling (matches the repair pipeline's
# posture; the phantom's argument schema rejects launches above 150
# lines — see clients.google_sheets).
ENRICH_MAX_PER_LAUNCH = 50


class PainKeywordsNotApprovedError(RuntimeError):
    """The keyword registry has not been operator-approved yet."""


class PainLaneDisabledError(RuntimeError):
    """OUTBOUND_PAIN_SIGNAL_ENABLED is not set to '1'."""


def is_pain_signal_enabled() -> bool:
    """Strict opt-in: only the literal '1' enables the lane."""
    return os.environ.get(PAIN_SIGNAL_ENABLED_ENV, "").strip() == "1"


def load_pain_keywords(path: Path | None = None) -> dict:
    """Load and shape-validate the keyword registry.

    Malformed registries fail loud with the offending query named — a
    typo'd keyword file must never silently scrape nothing.
    """
    with open(path or pain_keywords_path()) as f:
        data = json.load(f)
    queries = data.get("queries")
    if not isinstance(queries, list) or not queries:
        raise ValueError(
            "pain_keywords.json: `queries` must be a non-empty list"
        )
    for i, q in enumerate(queries):
        missing = [
            k for k in ("id", "language", "query") if not (q or {}).get(k)
        ]
        if missing:
            raise ValueError(
                f"pain_keywords.json: queries[{i}] "
                f"(id={q.get('id') if isinstance(q, dict) else None!r}) "
                f"is missing {missing}"
            )
        if q["language"] not in ("es", "pt", "en"):
            raise ValueError(
                f"pain_keywords.json: queries[{i}] id={q['id']!r} has "
                f"unsupported language {q['language']!r}"
            )
        if q["query"].count('"') % 2 != 0:
            # An unclosed quote makes the topic gate's terms unmatchable
            # (`"plan` can never sit at a word boundary) — the query
            # would silently drop 100% of its posts as off-topic. A
            # malformed registry must fail loud instead.
            raise ValueError(
                f"pain_keywords.json: queries[{i}] id={q['id']!r} has an "
                f"unbalanced quote in {q['query']!r}"
            )
    return data


def assert_keywords_approved(data: dict) -> None:
    """Refuse to scrape until the operator has reviewed + approved the
    registry — and never while it still carries shipped placeholder copy.

    The sentinel check is deliberately independent of `_meta.status`: an
    operator who flips the status without replacing the placeholder
    queries would otherwise scrape (and invite off) the engine's own
    template text.
    """
    meta = data.get("_meta") or {}
    placeholders = sorted(
        q.get("id") or f"queries[{i}]"
        for i, q in enumerate(data.get("queries") or [])
        if isinstance(q, dict) and PLACEHOLDER_SENTINEL in str(q.get("query"))
    )
    if placeholders:
        raise PainKeywordsNotApprovedError(
            f"content/{PAIN_KEYWORDS_FILENAME} still ships the placeholder "
            f"sentinel {PLACEHOLDER_SENTINEL!r} in query id(s) "
            f"{placeholders} — replace them with your own search phrases "
            "(or point OUTBOUND_CONTENT_DIR at a filled-in content "
            "directory) before enabling the lane. See GETTING_STARTED.md."
        )
    if meta.get("status") != "approved" or not meta.get("approved_by"):
        raise PainKeywordsNotApprovedError(
            f"content/{PAIN_KEYWORDS_FILENAME} is not operator-approved "
            f"(status={meta.get('status')!r}, "
            f"approved_by={meta.get('approved_by')!r}). Review the query "
            "list, then set _meta.status='approved', _meta.approved_by "
            "and _meta.approved_at. No scrape runs until then — see "
            "GETTING_STARTED.md."
        )


# The engagement note references a post the prospect engaged with "this
# week", so a window wider than one week would put that time claim on
# the wire as an overclaim: wider windows REFUSE to run.
_MAX_POST_AGE_HOURS = 168


def post_max_age_hours(config: dict) -> int:
    """Recency window: env override > registry config > 24.

    24h default: the lane runs daily from the daily check's Phase 0.9.
    Enforced CLIENT-SIDE on each post's `postTimestamp`
    (`filter_recent_posts`) — LinkedIn's server-side `datePosted`
    content-search filter returns zero results (broken) and is
    deliberately not used. Ceiling: 168h (see _MAX_POST_AGE_HOURS).
    """
    from models.env import env_int_positive

    value = env_int_positive(
        PAIN_SIGNAL_POST_MAX_AGE_ENV,
        int(config.get("post_max_age_hours_default", 24)),
    )
    if value > _MAX_POST_AGE_HOURS:
        raise ValueError(
            f"pain-signal recency window {value}h exceeds "
            f"{_MAX_POST_AGE_HOURS}h — the invite notes place the post "
            "inside the past week, so a wider window would ship a time "
            f"overclaim. Narrow {PAIN_SIGNAL_POST_MAX_AGE_ENV} / "
            "post_max_age_hours_default (or change the note copy first)."
        )
    return value


def content_search_url(query: str) -> str:
    """LinkedIn content-search URL: keyword query, newest first.

    Deliberately WITHOUT `datePosted`: differential-tested — the same
    query returns a full page bare and ZERO with
    `datePosted=%22past-week%22` (with or without
    origin=FACETED_SEARCH). Recency is client-side
    (`filter_recent_posts`); `sortBy=date_posted` keeps the freshest
    posts inside the worker's per-launch post cap."""
    from urllib.parse import quote

    return (
        "https://www.linkedin.com/search/results/content/"
        f"?keywords={quote(query)}"
        "&sortBy=%22date_posted%22"
    )


# ── client-side topic gate ───────────────────────────────────────────
# LinkedIn's "exact-phrase" content search is NOT exact. The invite note
# tells the prospect the post was ABOUT the pain phrase, so accepting
# engagers off a merely-adjacent post would put an overclaim on the
# wire. Gate: the matching query's phrase must actually appear in the
# row's `postContent`.


def _fold_for_match(text: str) -> str:
    """Normalize text for topic matching: strip accents (NFKD, drop
    combining marks), casefold, collapse whitespace."""
    decomposed = unicodedata.normalize("NFKD", text or "")
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.casefold().split())


def query_match_terms(query: str) -> list[str]:
    """Split a registry query into its required match terms.

    Quoted phrases stay whole; unquoted words are individual terms. ALL
    terms must appear in a post's text for it to count as on-topic — for
    paired queries that means the phrase AND every bare term, mirroring
    how the query was built to cut noise in the first place."""
    phrases = re.findall(r'"([^"]+)"', query)
    bare = re.sub(r'"[^"]*"', " ", query).split()
    return [t.strip() for t in (*phrases, *bare) if t.strip()]


def post_matches_query(post_text: str, query: str) -> bool:
    """True when every one of the query's terms appears in the post text
    (accent/case-folded, word-bounded — a phrase does not match inside a
    longer word).

    Conservative by design: a post that carries the topic only as a
    run-together hashtag does not match — dropping a borderline
    on-topic post is cheap; an overclaiming invite note is not.
    Fail-closed on the degenerate cases: empty post text and term-less
    queries never match."""
    folded_post = _fold_for_match(post_text)
    if not folded_post:
        return False
    terms = query_match_terms(query)
    if not terms:
        return False
    for term in terms:
        folded_term = _fold_for_match(term)
        if not folded_term:
            return False
        if not re.search(
            rf"(?<!\w){re.escape(folded_term)}(?!\w)", folded_post
        ):
            return False
    return True


# ── post-timestamp parsing (client-side recency) ─────────────────────

# LinkedIn/PB post exports carry ISO timestamps, epoch numbers (often as
# CSV digit-strings), or LinkedIn's relative labels — and the labels come
# in the SESSION's locale, so ES ("3 sem", "1 mes", "hace 2 días") and PT
# ("há 3 semanas", "2 anos") are as likely as EN ("4h", "2w", "5mo").
# Missing the locale variants would fail-closed drop 100% of posts
# (visible in the counter, but the lane would be dead). Alternation is
# ordered longest-first so "mes"/"min" match before the bare "m"
# (minutes) branch and "sem" before "s".
_RELATIVE_TS_RE = re.compile(
    r"^\s*(?:hace\s+|há\s+)?(\d+)\s*"
    r"(minutos?|min|meses|mes|mo|semanas?|sem|horas?|hr|"
    r"d[ií]as?|a[ñn]os?|yr|y|m|h|d|w)\b",
    re.IGNORECASE,
)
_RELATIVE_UNIT_HOURS = {
    "min": 1 / 60, "minuto": 1 / 60, "minutos": 1 / 60, "m": 1 / 60,
    "h": 1.0, "hr": 1.0, "hora": 1.0, "horas": 1.0,
    "d": 24.0, "dia": 24.0, "dias": 24.0,
    "w": 24.0 * 7, "sem": 24.0 * 7, "semana": 24.0 * 7, "semanas": 24.0 * 7,
    "mo": 24.0 * 30, "mes": 24.0 * 30, "meses": 24.0 * 30,
    "y": 24.0 * 365, "yr": 24.0 * 365, "ano": 24.0 * 365, "anos": 24.0 * 365,
}


def _fold_relative_unit(unit: str) -> str:
    """Lowercase + strip the ES accents the regex admits (día, año)."""
    return (
        unit.lower()
        .replace("í", "i")
        .replace("ñ", "n")
    )


def parse_post_timestamp(value: object, *, now: datetime) -> datetime | None:
    """Parse a post timestamp to UTC-aware datetime, or None.

    Accepts ISO-8601 (offset-less assumed UTC, mirroring
    clients.sender._parse_utc_timestamp), epoch seconds/millis (numeric
    OR as the digit-string a CSV actually delivers), and LinkedIn
    relative labels (EN/ES/PT locale variants) anchored at `now`. None
    means "unparseable" — the recency filter treats that as a DROP,
    never as fresh.
    """
    if (
        isinstance(value, str)
        and value.strip().isdigit()
        and len(value.strip()) >= 10
    ):
        # Epoch written as text — csv.DictReader only ever yields strings.
        value = float(value.strip())
    if isinstance(value, (int, float)) and value > 0:
        # Epoch millis or seconds — PB exports vary.
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
    if parsed is not None:
        return (
            parsed if parsed.tzinfo is not None
            else parsed.replace(tzinfo=UTC)
        )
    match = _RELATIVE_TS_RE.match(text)
    if match:
        count = int(match.group(1))
        unit = _fold_relative_unit(match.group(2))
        hours = _RELATIVE_UNIT_HOURS.get(unit)
        if hours is not None:
            return now - timedelta(hours=count * hours)
    return None


def filter_recent_posts(
    posts: list[dict], *, max_age_hours: int, now: datetime, summary: dict
) -> list[dict]:
    """Client-side recency filter. Fail-closed on unparseable timestamps.

    A timestamp more than 2h in the FUTURE is also treated as
    unparseable (clock skew tops out far below that; a future stamp
    means the column's meaning drifted, and trusting it would let the
    whole export read as fresh forever — the inverse of the filter's
    job).

    Mutates each kept post: sets `posted_at` (aware datetime).
    Counters: posts_dropped_stale, posts_dropped_no_timestamp.
    """
    cutoff = now - timedelta(hours=max_age_hours)
    future_limit = now + timedelta(hours=2)
    fresh: list[dict] = []
    for post in posts:
        posted_at = parse_post_timestamp(post.get("raw_timestamp"), now=now)
        if posted_at is None or posted_at > future_limit:
            summary["posts_dropped_no_timestamp"] += 1
            continue
        if posted_at < cutoff:
            summary["posts_dropped_stale"] += 1
            continue
        post["posted_at"] = posted_at
        fresh.append(post)
    return fresh


# ── CSV row extraction (defensive fallback chains, weekly-style) ─────


def _first_str(row: dict, *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _clean_snippet(text: str, max_chars: int) -> str:
    """Whitespace-collapsed, truncated pain snippet for the CRM entry."""
    collapsed = " ".join((text or "").split())
    if len(collapsed) <= max_chars:
        return collapsed
    return collapsed[: max(0, max_chars - 1)].rstrip() + "…"


_COUNT_SUFFIX_RE = re.compile(r"^(\d+(?:\.\d+)?)(k|m|mil)$")
_COUNT_SUFFIX_MULT = {"k": 1_000, "m": 1_000_000, "mil": 1_000}


def _int_count(row: dict, *keys: str) -> int | None:
    """Parse an engagement-count column to an int, or None for
    present-but-unparseable.

    Handles plain ints, thousands separators ("1,204"), and LinkedIn's
    abbreviated forms in the session's locale ("1.2K", "3K", "1,2 mil")
    — the ABBREVIATED forms are precisely the most-engaged posts, so
    reading them as 0 would skip engager scrapes on the densest posts
    while counting them "zero-engagement". Absent/empty reads 0 (a
    renamed column then trips the all-zero alarm); a non-empty value
    that still doesn't parse returns None — the caller treats unknown as
    engagement-present (one bounded launch beats silently skipping a
    live post) and counts it.
    """
    raw = _first_str(row, *keys)
    if not raw:
        return 0
    compact = raw.strip().lower().replace(",", ".").replace(" ", "")
    match = _COUNT_SUFFIX_RE.match(compact)
    if match:
        return int(float(match.group(1)) * _COUNT_SUFFIX_MULT[match.group(2)])
    digits = re.sub(r"[.,\s]", "", raw)
    if digits.isdigit():
        return int(digits)
    return None


def _post_from_row(row: dict) -> dict:
    """Normalize one posts-worker export row to the lane's post shape.

    Column names from the posts worker: postUrl, postContent, likeCount,
    commentCount, postTimestamp, imgUrl, videoUrl, profileUrl, author,
    authorUrl, action, timestamp. Fallback chains cover worker-version
    drift.
    """
    poster_name = _first_str(row, "author", "fullName", "name", "posterName")
    if not poster_name:
        first = _first_str(row, "firstName")
        last = _first_str(row, "lastName")
        poster_name = f"{first} {last}".strip()
    return {
        "post_url": _first_str(
            row, "postUrl", "url", "postLink", "sharedPostUrl"
        ),
        "text": _first_str(
            row, "postContent", "textContent", "content", "text",
            "description",
        ),
        "raw_timestamp": _first_str(
            row, "postTimestamp", "postDate", "publishedAt", "date"
        ),
        # `authorUrl` first: it is unambiguously the author;
        # `profileUrl` is the fallback (in search-input mode the two
        # coincide).
        "poster_profile_url": _first_str(
            row, "authorUrl", "profileUrl", "posterProfileUrl",
            "profileLink",
        ),
        "poster_name": poster_name,
        "like_count": _int_count(row, "likeCount", "likesCount", "likes"),
        "comment_count": _int_count(
            row, "commentCount", "commentsCount", "comments"
        ),
    }


def _poster_candidate(post: dict, *, snippet_max_chars: int) -> dict | None:
    """The post's author as a weekly-CSV-shaped `poster` candidate.

    Title/company are empty — the posts export carries neither; the SN
    enrichment phase fills them before scoring (same as engagers).
    Returns None when the export carried no author profile URL.
    """
    if not post.get("poster_profile_url"):
        return None
    raw = {
        "fullName": post.get("poster_name") or "",
        "title": "",
        "company": "",
        "location": "",
        "defaultProfileUrl": post["poster_profile_url"],
        "_pain_degree": "",
    }
    return _attach_pain_metadata(
        raw, post, source_type="poster", snippet_max_chars=snippet_max_chars
    )


def _engager_worker_candidate(
    row: dict, post: dict, *, source_type: str, snippet_max_chars: int
) -> dict | None:
    """Normalize one commenters/likers-worker row to a candidate.

    The fallback chains cover the shapes this phantom family has shipped
    (the parent's cumulative export used
    profileLink/fullName/occupation/degree/comments). Post context
    (snippet, URL, timestamp, language) comes from the POST we launched
    the worker for — never from the row. Returns None when no profile
    URL is recognizable (counted by the caller: a column rename must not
    read as "nobody engaged").
    """
    profile_url = _first_str(
        row, "profileLink", "profileUrl", "linkedinProfileUrl", "url"
    )
    if not profile_url:
        return None
    name = _first_str(row, "fullName", "name")
    if not name:
        first = _first_str(row, "firstName")
        last = _first_str(row, "lastName")
        name = f"{first} {last}".strip()
    raw = {
        "fullName": name,
        "title": _first_str(row, "occupation", "headline", "title"),
        "company": "",
        "location": "",
        "defaultProfileUrl": profile_url,
        "_pain_degree": _first_str(row, "degree", "connectionDegree"),
    }
    cand = _attach_pain_metadata(
        raw, post, source_type=source_type,
        snippet_max_chars=snippet_max_chars,
    )
    if source_type == "commenter":
        # A commenter's own words are the best review context; the
        # matched post's text stays the fallback.
        comment = _first_str(
            row, "comment", "commentText", "comments", "commentContent"
        )
        if comment:
            cand["_pain_snippet"] = _clean_snippet(comment, snippet_max_chars)
    return cand


def _attach_pain_metadata(
    raw: dict, post: dict, *, source_type: str, snippet_max_chars: int
) -> dict:
    """Stamp lane metadata onto a weekly-shaped raw row (underscore keys
    so they can never collide with a PB CSV column)."""
    raw["_pain_source_type"] = source_type
    raw["_pain_snippet"] = _clean_snippet(
        post.get("text") or "", snippet_max_chars
    )
    raw["_pain_post_url"] = post.get("post_url") or ""
    raw["_pain_language"] = post.get("language") or "es"
    posted_at = post.get("posted_at")
    raw["_pain_post_at"] = posted_at.isoformat() if posted_at else ""
    return raw


def lane_entry_attrs_for(raw: dict) -> dict:
    """Extra CRM entry attrs for one pain-lane commit.

    Includes the explicit cohort stamp — the pain cohort must never
    inherit the globally-running DM experiment's id (measurement
    integrity), so this OVERRIDES `_build_prospect_entry_attrs`' default
    via `_commit_prospect(lane_entry_attrs=...)` merge order.

    `pain_source_type` defaults to "liker" — the safe reference frame
    (the engagement note never claims authorship; a wrongly-defaulted
    "poster" would tell someone they wrote a post they didn't).
    """
    attrs = {
        "prospect_source": PROSPECT_SOURCE_PAIN_SIGNAL,
        "pain_source_type": raw.get("_pain_source_type") or "liker",
        "experiment_id": PAIN_SIGNAL_EXPERIMENT_ID,
        "experiment_id_frozen_at": "prospect",
    }
    if raw.get("_pain_snippet"):
        attrs["pain_snippet"] = raw["_pain_snippet"]
    if raw.get("_pain_post_url"):
        attrs["source_post_url"] = raw["_pain_post_url"]
    if raw.get("_pain_post_at"):
        attrs["source_post_at"] = raw["_pain_post_at"]
    return attrs


# ── never-contact denylist (ingest-time) ─────────────────────────────


def _drop_denylisted(candidates: list[dict], summary: dict) -> list[dict]:
    """Hard-block the operator's configured never-contact denylist
    BEFORE preview/scoring.

    `_process_prospects` enforces the same rule for every lane (shared
    `is_denylisted_candidate`); this early pass exists so a denylisted
    candidate never renders a dry-run note preview AND never costs an SN
    enrichment scrape. Pre-enrichment the company field is empty, so the
    check leans on name + title (engagers carry their company only in
    the headline) — the post-enrichment gate inside `_process_prospects`
    re-checks with the real company."""
    from workflows.weekly_prospect import is_denylisted_candidate

    kept: list[dict] = []
    for raw in candidates:
        if is_denylisted_candidate(
            raw.get("company"), raw.get("fullName"), raw.get("title")
        ):
            summary["denylist_blocked"] += 1
            click.echo(
                f"  ⛔ DENYLIST HARD BLOCK: dropped "
                f"{raw.get('fullName') or raw.get('defaultProfileUrl')!r} "
                f"({raw.get('title')!r}) at ingest — never contact.",
                err=True,
            )
            continue
        kept.append(raw)
    return kept


# ── phantom launches ─────────────────────────────────────────────────


def _save_then_launch_worker(
    pb: PhantomBusterClient, worker_id: str, overrides: dict
) -> str | None:
    """Save-then-launch one workflow worker; return its result CSV.

    These phantoms configure themselves from the SAVED console argument,
    so per-launch `arguments` must never be relied on. Sequence, each
    step fail-closed:

    1. Fetch the saved argument; merge `overrides` on top (the saved
       shape guards required fields this module doesn't know about).
    2. Persist via /agents/save, then RE-FETCH and verify every
       overridden key landed AND the agent's console-managed fields
       (name, file storage, launch type) were not clobbered — the save
       endpoint's partial-update semantics are the vendor's, not ours.
    3. Launch BARE and wait (WORKER_LAUNCH_MAX_WAIT; pacing lives in the
       caller).
    4. Download the worker's own result.csv (per-launch file — the posts
       worker's file storage deletes previous files). No CSV + "No
       results found" in the log = a quiet zero, returned as "" so
       callers can tell it from an infra failure (None).
    """
    agent = pb.get_agent(worker_id)
    raw_arg = agent.get("argument") or "{}"
    saved = json.loads(raw_arg) if isinstance(raw_arg, str) else raw_arg
    merged = {**saved, **overrides}
    pb.save_agent_argument(worker_id, merged)
    check_agent = pb.get_agent(worker_id)
    check_raw = check_agent.get("argument") or "{}"
    check = json.loads(check_raw) if isinstance(check_raw, str) else check_raw
    for key, value in overrides.items():
        if check.get(key) != value:
            raise RuntimeError(
                f"worker {worker_id} argument save did not stick "
                f"({key!r} is {check.get(key)!r}) — refusing to launch "
                "against a stale saved argument."
            )
    for field in ("name", "fileMgmt", "launchType"):
        if check_agent.get(field) != agent.get(field):
            raise RuntimeError(
                f"worker {worker_id} /agents/save clobbered console "
                f"field {field!r} ({agent.get(field)!r} → "
                f"{check_agent.get(field)!r}) — the vendor's save "
                "semantics changed; stop and restore the worker in the "
                "console before running the lane again."
            )
    launch = pb.launch_agent(worker_id)  # bare — workers read SAVED args
    try:
        completion = pb.wait_for_completion(
            launch, poll_interval=15, max_wait=WORKER_LAUNCH_MAX_WAIT
        )
    except PBRunTimeout as err:
        if err.last_observed_status == "finished":
            # PB's latest-run pointer lags and wait_for_completion never
            # sees OUR container despite the run having finished. The
            # worker's per-launch file storage means the CSV at the
            # agent path IS this launch's file — salvage it rather than
            # consuming the posts for nothing (the worker's cross-launch
            # dedup never re-serves them).
            click.echo(
                f"  ⚠ worker {worker_id} wait timed out with "
                "last_observed_status='finished' (PB latest-run pointer "
                "lag) — salvaging the launch's CSV.",
                err=True,
            )
            salvage = pb.download_result_csv(launch)
            if salvage:
                return salvage
        raise
    csv_text = pb.download_result_csv(launch)
    if csv_text:
        return csv_text
    if NO_RESULTS_LOG_MARKER in (completion.log_output or "").lower():
        return ""  # quiet zero: the worker ran, nothing new
    return None  # no CSV, no explanation — infra failure (caller counts)


def _worker_session_overrides() -> dict:
    """TOP-LEVEL sessionCookie/userAgent for the workflow workers,
    refreshed from env so a stale console cookie never rides along."""
    from clients.phantombuster import get_phantombuster_credentials

    cookie, ua = get_phantombuster_credentials()
    if not cookie:
        raise RuntimeError(
            "PB_LI_SESSION_COOKIE not set — the pain-signal scrapes need "
            "the LinkedIn session cookie (same as the weekly run)."
        )
    return {"sessionCookie": cookie, "userAgent": ua}


def _launch_posts_scrape(
    pb: PhantomBusterClient, posts_worker_id: str, search_url: str
) -> str | None:
    """One posts-worker launch for one content-search URL.

    The search URL goes in `spreadsheetUrl`. The worker's saved knobs
    (numberMaxOfPosts, sortByRecentPosts, numberOfLinesPerLaunch) are
    console-managed and ride along untouched. The URL deliberately has
    NO datePosted filter — LinkedIn's server-side filter returns zero
    results; recency is client-side on postTimestamp.
    """
    return _save_then_launch_worker(pb, posts_worker_id, {
        POSTS_SEARCH_URL_ARG: search_url,
        **_worker_session_overrides(),
    })


def _launch_engager_worker_scrape(
    pb: PhantomBusterClient, worker_id: str, post_url: str
) -> str | None:
    """One commenters- or likers-worker launch for one post URL.

    Failures are loud and contained per post. The saved watcherMode flag
    is deliberately NOT overridden — its semantics are vendor-version
    dependent.
    """
    return _save_then_launch_worker(pb, worker_id, {
        ENGAGER_POST_URL_ARG: post_url,
        **_worker_session_overrides(),
    })


def _launch_enrichment_scrape(
    pb: PhantomBusterClient,
    sn_profile_scraper_id: str,
    urls: list[str],
    *,
    dry_run: bool,
) -> str | None:
    """One SN Profile Scraper launch over ≤ENRICH_MAX_PER_LAUNCH URLs.

    Reuses the daily run's saved-args + identities-inject contract
    (`build_sales_nav_launch_args` — raises SalesNavConfigError with the
    fix named when the SN cookie env var is missing). Multi-URL batches
    go through a Google Sheet; in dry-run that write MUST target the
    sandbox sheet (GSHEET_DRYRUN_ID) so a preview never mutates the
    production autoconnect sheet (same rule as the pre-invite degree
    check).

    Own csvName namespace (`ps-enr-*`) — never `deg-*`/`wk-*`: PB's
    processed-inputs dedup is csvName-keyed, and the enrichment scrape
    must never inherit (or poison) the degree-check phantom's state.
    """
    from clients.google_sheets import (
        write_prospects_to_sheet,
    )
    from workflows.daily_check_helpers import (
        _fresh_csv_name,
        build_sales_nav_launch_args,
    )

    if len(urls) == 1:
        # Bare profile URL as input — no sheet, no header line.
        sheet_url = urls[0]
        launch_count = 1
    else:
        dry_sheet_id = os.environ.get("GSHEET_DRYRUN_ID") if dry_run else None
        if dry_run and not dry_sheet_id:
            raise RuntimeError(
                "dry-run pain-signal enrichment requires GSHEET_DRYRUN_ID "
                "(a sandbox sheet) so the preview never writes the "
                "production autoconnect sheet."
            )
        sheet_url = write_prospects_to_sheet(
            [{"profileUrl": u} for u in urls],
            columns=["profileUrl"],
            spreadsheet_id=dry_sheet_id,
            include_header=False,
        )
        # +1 for the sheet header row PB counts as a processable line
        # (clients.google_sheets.profiles_per_launch — the header
        # otherwise eats one slot and the last profile of every batch
        # goes unscraped).
        # The scraper sheet contains URLs only.
        launch_count = len(urls)

    csv_name = _fresh_csv_name("ps-enr")
    launch_args = {
        **build_sales_nav_launch_args(
            pb,
            sn_profile_scraper_id,
            spreadsheet_url=sheet_url,
            launch_count=launch_count,
        ),
        "csvName": csv_name,
    }
    launch = pb.launch_agent(sn_profile_scraper_id, launch_args)
    pb.wait_for_completion(launch, poll_interval=15, max_wait=900)
    return pb.download_result_csv(launch, csv_name=csv_name)


def _merge_enrichment(
    candidates: list[dict], csv_text: str, summary: dict
) -> None:
    """Fold one SN Profile Scraper result CSV into the candidate rows.

    Match-back keys, per URL: the normalized string form AND the
    `li-id:` profile-id identity key — the SN scraper can echo a profile
    under its CURRENT slug while the engager export carried a
    vanity/old slug; the id key bridges renames when the slug carries a
    member-id suffix. Both the `query` echo column (input as passed) and
    `linkedinProfileUrl` (the scraper's form) seed the index.

    A candidate counts as `enriched` only when at least one field
    actually merged: the SN phantom returns matched-but-EMPTY rows on
    per-profile errors (the degree check guards the same shape), and
    counting those would let a fully-dead enrichment read as healthy
    ("Enriched N/N") while every candidate scores headline-only.
    Candidates without a merge keep their engager-export headline title
    — an SN column rename or partial scrape must never read as "these
    people have no company".
    """
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    by_key: dict[str, dict] = {}
    for row in rows:
        for url in (
            _first_str(row, "query"),
            _first_str(row, "linkedinProfileUrl"),
        ):
            for key in _enrichment_match_keys(url):
                by_key[key] = row
    for cand in candidates:
        if cand.get("_enriched"):
            continue
        sn_row = None
        for key in _enrichment_match_keys(cand["defaultProfileUrl"]):
            sn_row = by_key.get(key)
            if sn_row is not None:
                break
        if sn_row is None:
            continue
        merged_any = False
        title = _first_str(sn_row, "headline", "title", "occupation")
        if title:
            cand["title"] = title
            merged_any = True
        company = _first_str(
            sn_row, "currentCompanyName", "companyName", "company"
        )
        if company:
            cand["company"] = company
            merged_any = True
        location = _first_str(sn_row, "location")
        if location:
            cand["location"] = location
            merged_any = True
        if merged_any:
            cand["_enriched"] = True
            summary["enriched"] += 1


def _enrichment_match_keys(url: str) -> tuple[str, ...]:
    """Comparison keys for one profile URL: the degree-check's normalized
    string form plus the profile-id identity key (which survives slug
    renames — falls back to the canonical URL when the slug has no
    member-id suffix)."""
    from workflows.daily_check_helpers import _normalize_linkedin_url

    if not url:
        return ()
    return (
        _normalize_linkedin_url(url),
        linkedin_identity_key(_canonical_linkedin_url(url)),
    )


# ── candidate assembly ───────────────────────────────────────────────


def _preview_invite_note(raw: dict) -> str | None:
    """Render the pain-signal note this candidate would get (dry-run
    preview). Returns None when no pain template exists for the
    candidate's language — the daily run would then fall back to the
    persona note.

    Renders WITHOUT industry resolution (unlike the wet path in
    `_build_invite_send_data`, which passes `industry=`): this preview is
    what the operator approves, so pain templates must never carry
    industry placeholders or the preview would diverge from the wire.
    """
    from models.campaign import (
        Language,
        MissingMessageError,
        get_pain_signal_note,
        personalize,
    )

    language_code = raw.get("_pain_language") or "es"
    try:
        template = get_pain_signal_note(
            Language(language_code),
            source_type=raw.get("_pain_source_type") or "liker",
        )
    except (MissingMessageError, ValueError):
        return None
    name = (raw.get("fullName") or "").split()
    return personalize(
        template,
        name[0] if name else "",
        raw.get("company") or "",
        language=Language(language_code),
    )


def _assert_pain_schema(attio: AttioClient, list_id: str) -> None:
    """Wet-run preflight: refuse to commit against an unmigrated schema.

    Without this the FIRST commit 400s AFTER `upsert_person` has already
    created the Person record — an orphaned person, a dead run
    mid-batch, and a misleading Phase-1-attrs retry message. Fail BEFORE
    any write, with the fix in the message. Only `prospect_source` is
    probed for presence — the five lane attrs ship in one
    setup_attio_schema run — plus the `commenter` select option, which a
    workspace migrated against an earlier revision can be missing while
    still passing the slug probe.
    """
    try:
        resp = attio._request(
            "GET", f"/lists/{list_id}/attributes", params={"limit": 100}
        )
        slugs = {a.get("api_slug", "") for a in resp.get("data", [])}
    except httpx.HTTPStatusError as err:
        if err.response.status_code in (400, 404):
            slugs = set()
        else:
            raise  # transient infra — don't misdirect to the migration
    if "prospect_source" not in slugs:
        raise RuntimeError(
            "The pipeline list is missing the pain-signal entry "
            "attributes (prospect_source, pain_source_type, pain_snippet, "
            "source_post_url, source_post_at) — run: python3 "
            "scripts/setup_attio_schema.py --feature pain_signal "
            "before a wet pain-signal run. Nothing was written."
        )
    try:
        resp = attio._request(
            "GET", f"/lists/{list_id}/attributes/pain_source_type/options"
        )
        option_titles = {o.get("title", "") for o in resp.get("data", [])}
    except httpx.HTTPStatusError as err:
        if err.response.status_code in (400, 404):
            option_titles = set()
        else:
            raise
    if "commenter" not in option_titles:
        raise RuntimeError(
            "The pain_source_type select is missing the 'commenter' "
            "option — re-run: python3 scripts/setup_attio_schema.py "
            "--feature pain_signal before a wet pain-signal run. "
            "Nothing was written."
        )


# ── orchestrator ─────────────────────────────────────────────────────


def run_pain_signal_discovery(
    crm: CRMProvider,
    pb: PhantomBusterClient,
    posts_worker_id: str,
    commenters_worker_id: str,
    likers_worker_id: str,
    sn_profile_scraper_id: str,
    *,
    dry_run: bool = False,
    keywords_path: Path | None = None,
    now: datetime | None = None,
    scrape_posts: Callable[[str], str | None] | None = None,
    scrape_commenters: Callable[[str], str | None] | None = None,
    scrape_likers: Callable[[str], str | None] | None = None,
    scrape_enrichment: Callable[[list[str]], str | None] | None = None,
) -> dict:
    """Run the pain-signal discovery lane end to end.

    dry_run: scrapes DO run (PB spend — previewing requires real
    posts/people, and scoring previews require the SN enrichment) but
    NOTHING is written to the CRM; candidate + invite-note previews are
    echoed instead. Wet runs commit qualified candidates at Prospect
    stage behind the standard quarantine — no sends happen from this
    workflow ever.

    The posts worker id is mandatory (no posts, no lane). The
    commenters/likers worker ids are OPTIONAL: a missing one skips that
    engager type LOUDLY (posters are always processed).

    `scrape_posts` / `scrape_commenters` / `scrape_likers` /
    `scrape_enrichment` are test seams; production leaves them None and
    uses the PB launchers above. `now` is a determinism seam for the
    recency filter. The SN scraper id is required unless a
    `scrape_enrichment` seam is supplied — enrichment is a lane premise
    (candidates carry no company, and ICP scoring runs on title).
    """
    from workflows.daily_check_helpers import (
        SalesNavConfigError,
        _pb_sales_nav_session_args,
    )
    from workflows.industry_classifier import build_anthropic_client
    from workflows.weekly_prospect import (
        CompanySignals,
        _attio_inner_client,
        _build_name_index,
        _load_in_list_canonical_urls,
        _process_prospects,
        new_process_summary,
    )

    if not is_pain_signal_enabled():
        raise PainLaneDisabledError(
            f"pain-signal lane is disabled — set {PAIN_SIGNAL_ENABLED_ENV}=1 "
            "to run it (default off by design; see GETTING_STARTED.md)."
        )

    data = load_pain_keywords(keywords_path)
    assert_keywords_approved(data)
    config = data.get("config") or {}
    max_age_hours = post_max_age_hours(config)
    max_engagers = int(config.get("max_engagers_per_run", 40))
    max_engager_posts = int(config.get("max_engager_scrape_posts_per_run", 8))
    snippet_max_chars = int(config.get("pain_snippet_max_chars", 280))
    now = now or datetime.now(UTC)

    if scrape_posts is None and not posts_worker_id:
        raise RuntimeError(
            "pain-signal requires the posts worker "
            "(PB_PAIN_POSTS_WORKER_ID — the workflow's post-extractor "
            "phantom). Without posts there is no lane; see "
            "GETTING_STARTED.md."
        )
    if scrape_enrichment is None:
        if not sn_profile_scraper_id:
            raise RuntimeError(
                "pain-signal requires the Sales Nav Profile Scraper for "
                "candidate enrichment (PB_SALES_NAV_PROFILE_SCRAPER_ID — "
                "the same phantom the daily degree check uses). Candidates "
                "carry no company and ICP scoring runs on title; refusing "
                "to scrape without an enrichment path."
            )
        # Deterministic enrichment-config failures refuse PRE-SPEND,
        # matching the scraper-id refusal above — NOT per-chunk degrade
        # after every discovery scrape has already been burned.
        # _pb_sales_nav_session_args raises SalesNavConfigError with the
        # fix named when the SN cookie env var is missing; the
        # sandbox-sheet check mirrors the pre-invite degree check's
        # pre-loop gate.
        _pb_sales_nav_session_args()
        if dry_run and not os.environ.get("GSHEET_DRYRUN_ID"):
            raise RuntimeError(
                "dry-run pain-signal enrichment requires GSHEET_DRYRUN_ID "
                "(a sandbox sheet) so the preview never writes the "
                "production autoconnect sheet. Set it in .env."
            )

    # Wet-run schema preflight BEFORE any PB spend: an unmigrated
    # workspace must cost two CRM GETs, not a full discovery sweep whose
    # posts the worker's cross-launch dedup then never re-serves.
    if not dry_run:
        _assert_pain_schema(
            _attio_inner_client(crm), os.environ.get("ATTIO_LIST_ID", "")
        )

    # Engager workers are optional per type (posters-always posture): a
    # missing worker id skips that engager type loudly rather than
    # killing the lane.
    commenters_enabled = bool(scrape_commenters or commenters_worker_id)
    likers_enabled = bool(scrape_likers or likers_worker_id)

    # Real PB launches are paced (WORKER_INTER_LAUNCH_DELAY between
    # launches): rapid sequential launches on one agent can leave PB's
    # latest-run pointer behind. Test seams are never paced.
    paced_posts = scrape_posts is None
    paced_commenters = scrape_commenters is None
    paced_likers = scrape_likers is None
    did_real_launch = False
    scrape_posts = scrape_posts or (
        lambda url: _launch_posts_scrape(pb, posts_worker_id, url)
    )
    if commenters_enabled and scrape_commenters is None:
        scrape_commenters = lambda post_url: _launch_engager_worker_scrape(  # noqa: E731
            pb, commenters_worker_id, post_url
        )
    if likers_enabled and scrape_likers is None:
        scrape_likers = lambda post_url: _launch_engager_worker_scrape(  # noqa: E731
            pb, likers_worker_id, post_url
        )
    if scrape_enrichment is None:
        scrape_enrichment = lambda urls: _launch_enrichment_scrape(  # noqa: E731
            pb, sn_profile_scraper_id, urls, dry_run=dry_run
        )

    # Lane counters + the shared _process_prospects contract keys (single
    # source: `new_process_summary` — a counter added there reaches this
    # lane without a KeyError trap).
    summary: dict = {
        "queries_run": 0,
        "queries_skipped_disabled": 0,
        "scrape_failures": 0,
        "circuit_breaker_tripped": False,
        "posts_found": 0,
        "posts_no_url": 0,
        "posts_deduped": 0,
        "posts_fresh": 0,
        "posts_dropped_stale": 0,
        "posts_dropped_no_timestamp": 0,
        "posts_dropped_offtopic": 0,
        "posts_dropped_empty_text": 0,
        "posts_on_topic": 0,
        "posts_no_engagement": 0,
        "posts_engagement_unparseable": 0,
        "posts_engager_capped": 0,
        "commenter_scrapes": 0,
        "liker_scrapes": 0,
        "engager_rows": 0,
        "engager_rows_no_url": 0,
        "posters_no_profile_url": 0,
        "engagers_first_degree": 0,
        "engagers_deduped": 0,
        "engagers_already_in_pipeline": 0,
        "engagers_capped": 0,
        "enrich_scrapes": 0,
        "enrich_failures": 0,
        "enriched": 0,
        "candidates": 0,
        **new_process_summary(),
    }

    # Circuit breaker: per-item containment keeps one bad query or post
    # from killing the run, but consecutive failures STOP the remaining
    # launches — each retry costs a real launch on the operator's live
    # cookie. Tripping the breaker does NOT abort the run: everything
    # already harvested still flows through enrichment/qualify (the
    # posts worker's cross-launch dedup makes discarded harvest
    # PERMANENTLY lost, so salvage beats a clean abort). Auth failures
    # (401/403) still abort on the first.
    consecutive_failures = 0
    breaker_tripped = False

    def _contained_scrape(
        fn: Callable[[str], str | None], arg: str, what: str
    ) -> str | None:
        """One scrape with containment + the circuit breaker. Returns
        CSV text ("" = the worker's quiet zero), or None when the scrape
        failed (already counted + echoed)."""
        nonlocal consecutive_failures, breaker_tripped

        def failed(reason: str) -> None:
            nonlocal consecutive_failures, breaker_tripped
            summary["scrape_failures"] += 1
            consecutive_failures += 1
            click.echo(f"  ⚠ {what} failed ({reason}) — continuing.", err=True)
            if consecutive_failures >= MAX_CONSECUTIVE_SCRAPE_FAILURES:
                breaker_tripped = True
                summary["circuit_breaker_tripped"] = True
                click.echo(
                    f"  ⚠ CIRCUIT BREAKER: {consecutive_failures} "
                    f"consecutive scrape failures (last: {what} — "
                    f"{reason}). No further discovery launches this run "
                    "— a deterministic infrastructure failure must not "
                    "burn a live launch per remaining query/post. "
                    "Everything already harvested continues through "
                    "enrichment/qualify; fix the cause before the next "
                    "run.",
                    err=True,
                )

        try:
            result = fn(arg)
        except httpx.HTTPStatusError as err:
            if err.response.status_code in (401, 403):
                raise  # auth failure cascades — abort loud, mirror weekly
            failed(f"HTTP {err.response.status_code}")
            return None
        except Exception as err:  # noqa: BLE001 — one item must not kill the run
            failed(f"{type(err).__name__}: {err}")
            return None
        if result is None:
            # download_result_csv returns None on a missing CSV URL AND
            # on download errors, and the launcher only converts that to
            # "" when the log carries the no-results marker — anything
            # else is an infrastructure failure, never a quiet day.
            failed("no CSV retrieved (download failed or PB wrote no file)")
            return None
        consecutive_failures = 0
        return result

    enabled_queries = []
    for q in data["queries"]:
        if q.get("enabled", True):
            enabled_queries.append(q)
        else:
            summary["queries_skipped_disabled"] += 1

    click.echo(
        f"=== Pain-signal discovery: {len(enabled_queries)} queries "
        f"({summary['queries_skipped_disabled']} disabled), client-side "
        f"recency window {max_age_hours}h, engager scrapes on ≤"
        f"{max_engager_posts} post(s)/run, candidate cap "
        f"{max_engagers}/run ===\n"
    )
    if not (commenters_enabled and likers_enabled):
        missing = [
            name for name, on in (
                ("commenters", commenters_enabled), ("likers", likers_enabled)
            ) if not on
        ]
        click.echo(
            f"  ℹ engager worker(s) not configured: {', '.join(missing)} — "
            "those engagers will NOT be harvested this run (posters are "
            "still processed). Set PB_PAIN_COMMENTERS_WORKER_ID / "
            "PB_PAIN_LIKERS_WORKER_ID to enable them.",
            err=True,
        )

    # ── Phase 1a: posts scrape per query ─────────────────────────────
    # The posts worker dedups incrementally across launches (per-agent
    # processed DB), so each launch yields only never-seen posts — the
    # daily cadence naturally collects the fresh ones and "No results
    # found" is a quiet zero, not a failure.
    posts_by_url: dict[str, dict] = {}
    for i, q in enumerate(enabled_queries, 1):
        if breaker_tripped:
            break
        click.echo(
            f"[{i}/{len(enabled_queries)}] {q['id']} ({q['language']}): "
            f"{q['query']}"
        )
        if paced_posts:
            if did_real_launch:
                time.sleep(WORKER_INTER_LAUNCH_DELAY)
            did_real_launch = True
        csv_text = _contained_scrape(
            scrape_posts, content_search_url(q["query"]),
            f"posts scrape ({q['id']})",
        )
        if csv_text is None:
            continue
        summary["queries_run"] += 1
        if not csv_text.strip():
            click.echo(
                "  0 new posts (worker: no results — every matching post "
                "already processed, or none matched)."
            )
            continue
        rows = list(csv.DictReader(io.StringIO(csv_text)))
        found = 0
        new_posts = 0
        for row in rows:
            post = _post_from_row(row)
            if not post["post_url"]:
                summary["posts_no_url"] += 1
                continue
            found += 1
            summary["posts_found"] += 1
            if post["post_url"] in posts_by_url:
                summary["posts_deduped"] += 1
                continue
            post["query_id"] = q["id"]
            post["language"] = q["language"]
            post["query"] = q["query"]
            posts_by_url[post["post_url"]] = post
            new_posts += 1
        click.echo(
            f"  {found} post row(s) ({len(rows)} CSV rows, "
            f"{new_posts} new this run)."
        )

    if enabled_queries and summary["queries_run"] == 0:
        # A dead cookie/phantom surfaces as PBRunFailed (not HTTP 401),
        # so the per-query containment above catches every query — a
        # 0-for-N sweep is a scrape-infrastructure failure and must
        # never read as a quiet day (mirror of ENRICHMENT DEGRADED).
        click.echo(
            f"  ⚠ DISCOVERY DEAD: 0/{len(enabled_queries)} queries "
            "returned a CSV — the posts scrape path is down "
            "(LinkedIn cookie? worker args? PB?). This is an "
            "infrastructure failure, NOT a no-results day.",
            err=True,
        )

    # ── Phase 1b: client-side recency (postTimestamp, fail-closed) ───
    fresh_posts = filter_recent_posts(
        list(posts_by_url.values()),
        max_age_hours=max_age_hours, now=now, summary=summary,
    )
    fresh_posts.sort(key=lambda p: p["posted_at"], reverse=True)
    summary["posts_fresh"] = len(fresh_posts)
    click.echo(
        f"\nRecency: {summary['posts_fresh']} fresh / "
        f"{summary['posts_dropped_stale']} stale (>{max_age_hours}h) / "
        f"{summary['posts_dropped_no_timestamp']} unparseable-timestamp "
        "(dropped fail-closed)."
    )

    # ── Phase 1c: topic gate ─────────────────────────────────────────
    # A post is accepted only when its text carries SOME enabled query's
    # full phrase — first the query that surfaced it, else any other
    # (the worker's cross-launch dedup means a post surfaces exactly
    # once, possibly under a sibling query; attribution switches to the
    # query that actually matches, which also picks the note language
    # honestly).
    #
    # SCOPE HONESTY: this proves the post contains the query's phrase —
    # NOT that the invite note's fixed topic line describes the post.
    # For queries whose phrase stretches that claim, disabling them is
    # the operator's call at the wet gate.
    on_topic: list[dict] = []
    for post in fresh_posts:
        if not (post.get("text") or "").strip():
            # Fail-closed: an unverifiable topic claim never ships.
            summary["posts_dropped_empty_text"] += 1
            click.echo(
                f"  ⚠ post dropped (export carried no text): "
                f"{post['post_url']} — topic cannot be verified.",
                err=True,
            )
            continue
        if not post_matches_query(post["text"], post["query"]):
            matched = next(
                (
                    other for other in enabled_queries
                    if other["id"] != post["query_id"]
                    and other["language"] == post["language"]
                    and post_matches_query(post["text"], other["query"])
                ),
                None,
            ) or next(
                (
                    other for other in enabled_queries
                    if other["id"] != post["query_id"]
                    and post_matches_query(post["text"], other["query"])
                ),
                None,
            )
            if matched is None:
                summary["posts_dropped_offtopic"] += 1
                click.echo(
                    f"  ⚠ off-topic post dropped (query {post['query_id']}: "
                    "no enabled query's phrase in the post text — LinkedIn "
                    f'search noise): "{_clean_snippet(post["text"], 90)}" '
                    "— its people are not accepted (the invite note would "
                    "overclaim the post's topic).",
                    err=True,
                )
                continue
            post["query_id"] = matched["id"]
            post["language"] = matched["language"]
            post["query"] = matched["query"]
        on_topic.append(post)
    summary["posts_on_topic"] = len(on_topic)
    click.echo(
        f"Topic gate: {summary['posts_on_topic']} on-topic / "
        f"{summary['posts_dropped_offtopic']} off-topic / "
        f"{summary['posts_dropped_empty_text']} no-text "
        "(dropped fail-closed).\n"
    )

    # ── Phase 1d: engager scrapes + candidate assembly ───────────────
    candidates_by_key: dict[str, dict] = {}

    def _add_candidate(cand: dict) -> None:
        # In-run dedup on the profile-id identity key (slug variants of
        # one person collapse). On collision keep the richer source type,
        # ranked poster then commenter then liker (_SOURCE_PRIORITY).
        key = linkedin_identity_key(
            _canonical_linkedin_url(cand["defaultProfileUrl"])
        )
        existing = candidates_by_key.get(key)
        if existing is not None:
            summary["engagers_deduped"] += 1
            if (
                _SOURCE_PRIORITY.get(cand["_pain_source_type"], 0)
                > _SOURCE_PRIORITY.get(existing["_pain_source_type"], 0)
            ):
                candidates_by_key[key] = cand
            return
        candidates_by_key[key] = cand

    # Posters first: every on-topic post's author, no extra scrape.
    for post in on_topic:
        cand = _poster_candidate(post, snippet_max_chars=snippet_max_chars)
        if cand is None:
            # Column drift on the author-URL field must not silently
            # zero the poster half of the lane.
            summary["posters_no_profile_url"] += 1
            continue
        _add_candidate(cand)
    if on_topic and summary["posters_no_profile_url"] == len(on_topic):
        click.echo(
            "  ⚠ POSTERS DEAD: no on-topic post carried an author "
            "profile URL — the posts worker's author columns drifted "
            "(check _post_from_row).",
            err=True,
        )

    # Engager scrapes: only posts whose counts say someone engaged, and
    # at most max_engager_scrape_posts_per_run posts (freshest first —
    # fresh_posts ordering carried through the gate) so a broad day
    # cannot burn a launch per post.
    any_engager_worker = commenters_enabled or likers_enabled
    engager_posts: list[dict] = []
    if any_engager_worker:
        # None = the count column was present but unparseable — treat as
        # engagement-PRESENT (one bounded launch beats silently skipping
        # what is often the most-engaged post: LinkedIn abbreviates big
        # counts, and an unknown abbreviation must not read as zero).
        # Explicit zeros skip the scrape.
        engaged = [
            p for p in on_topic
            if p["comment_count"] != 0 or p["like_count"] != 0
        ]
        unparseable = [
            p for p in on_topic
            if p["comment_count"] is None or p["like_count"] is None
        ]
        summary["posts_no_engagement"] = len(on_topic) - len(engaged)
        summary["posts_engagement_unparseable"] = len(unparseable)
        if unparseable:
            click.echo(
                f"  ⚠ {len(unparseable)} post(s) carry an engagement "
                "count _int_count can't parse — treated as "
                "engagement-present (scraped). If this repeats, teach "
                "_int_count the new format.",
                err=True,
            )
        if on_topic and not engaged:
            click.echo(
                "  ⚠ every on-topic post reads zero likes AND zero "
                "comments — plausible on a thin day, but if it repeats "
                "the likeCount/commentCount columns drifted "
                "(check _post_from_row). No engager scrapes launched.",
                err=True,
            )
        engager_posts = engaged[:max_engager_posts]
        if len(engaged) > max_engager_posts:
            summary["posts_engager_capped"] = (
                len(engaged) - max_engager_posts
            )
            click.echo(
                f"  ℹ engager scrapes capped at {max_engager_posts} "
                f"post(s) (max_engager_scrape_posts_per_run) — "
                f"{summary['posts_engager_capped']} engaged post(s) "
                "skipped for engagers this run (posters still "
                "processed).",
                err=True,
            )

    for post in engager_posts:
        if breaker_tripped:
            break
        for source_type, seam, enabled, count, paced in (
            ("commenter", scrape_commenters, commenters_enabled,
             post["comment_count"], paced_commenters),
            ("liker", scrape_likers, likers_enabled,
             post["like_count"], paced_likers),
        ):
            if breaker_tripped:
                break
            if not enabled or count == 0 or seam is None:
                continue
            if paced:
                if did_real_launch:
                    time.sleep(WORKER_INTER_LAUNCH_DELAY)
                did_real_launch = True
            csv_text = _contained_scrape(
                seam, post["post_url"],
                f"{source_type}s scrape ({post['post_url']})",
            )
            if csv_text is None:
                continue
            summary[f"{source_type}_scrapes"] += 1
            rows = list(csv.DictReader(io.StringIO(csv_text)))
            summary["engager_rows"] += len(rows)
            kept = 0
            no_url = 0
            for row in rows:
                cand = _engager_worker_candidate(
                    row, post, source_type=source_type,
                    snippet_max_chars=snippet_max_chars,
                )
                if cand is None:
                    no_url += 1
                    summary["engager_rows_no_url"] += 1
                    continue
                degree = cand["_pain_degree"].strip().lower()
                if degree.startswith("1"):
                    # Already connected ("1st"/"1er"/"1°" — the session
                    # locale varies, so the digit prefix is the check) —
                    # there is no invite to send; the daily run's degree
                    # check stays authoritative for everyone else (2nd/
                    # 3rd/blank — and for rows with no degree column at
                    # all).
                    summary["engagers_first_degree"] += 1
                    continue
                _add_candidate(cand)
                kept += 1
            if no_url:
                # A worker-export column rename must not read as
                # "nobody engaged" — surface the rows-vs-kept gap.
                click.echo(
                    f"  ⚠ {no_url}/{len(rows)} {source_type} row(s) for "
                    f"{post['post_url']} had no recognizable profile "
                    "URL column — dropped (schema drift? check "
                    "_engager_worker_candidate).",
                    err=True,
                )
            click.echo(
                f"  {source_type}s({post['post_url'][:60]}…): "
                f"{kept} kept / {len(rows)} rows."
            )

    candidates = list(candidates_by_key.values())
    click.echo(
        f"\nCandidates: {len(candidates)} unique from "
        f"{summary['posts_on_topic']} on-topic post(s) — "
        f"{summary['engagers_first_degree']} already-connected (1st) "
        f"dropped, {summary['engagers_deduped']} duplicates collapsed.\n"
    )

    # Denylist hard block BEFORE enrichment (title/headline only — the
    # post-enrichment gate in _process_prospects re-checks with company).
    candidates = _drop_denylisted(candidates, summary)

    reprospect_review: list[dict] = []
    llm_no_verdict = 0
    list_id = os.environ.get("ATTIO_LIST_ID", "")
    existing_entries: list = []
    in_list_record_ids: set[str] = set()
    in_list_canonical_urls: set[str] = set()
    name_index: dict = {}

    if candidates:
        # ── Phase 2: pipeline snapshot + pre-enrichment in-list drop ─
        # The daily cadence re-surfaces the same viral posts' engagers;
        # exact in-list matches (canonical URL or profile-id key — the
        # SAME keys _process_prospects would dedup them on) must not
        # cost an SN scrape every day.
        click.echo("Loading existing pipeline list...")
        existing_entries = crm.query_list_entries(list_id=list_id)
        in_list_record_ids = {e.record_id for e in existing_entries}
        in_list_canonical_urls = _load_in_list_canonical_urls(
            crm, existing_entries
        )
        name_index = _build_name_index(crm, existing_entries)
        click.echo(
            f"  {len(in_list_record_ids)} records in pipeline, "
            f"{len(in_list_canonical_urls)} dedup keys, "
            f"{len(name_index)} names indexed."
        )

        fresh: list[dict] = []
        for cand in candidates:
            canonical = _canonical_linkedin_url(cand["defaultProfileUrl"])
            identity = linkedin_identity_key(canonical)
            if (
                canonical in in_list_canonical_urls
                or identity in in_list_canonical_urls
            ):
                summary["engagers_already_in_pipeline"] += 1
                continue
            fresh.append(cand)
        if summary["engagers_already_in_pipeline"]:
            click.echo(
                f"  {summary['engagers_already_in_pipeline']} engager(s) "
                "already in the pipeline list — dropped before enrichment "
                "(no SN scrape spent)."
            )
        candidates = fresh

    # ── Phase 3: per-run cap, richest sources first ──────────────────
    if len(candidates) > max_engagers:
        summary["engagers_capped"] = len(candidates) - max_engagers
        candidates.sort(
            key=lambda c: -_SOURCE_PRIORITY.get(c["_pain_source_type"], 0)
        )
        candidates = candidates[:max_engagers]
        click.echo(
            f"  ℹ engager batch capped at {max_engagers} "
            f"(max_engagers_per_run) — {summary['engagers_capped']} "
            "candidate(s) dropped this run (posters/commenters kept "
            "first). A viral post must not flood the SN scrape.",
            err=True,
        )

    summary["candidates"] = len(candidates)
    by_type = {"poster": 0, "commenter": 0, "liker": 0}
    for cand in candidates:
        by_type[cand["_pain_source_type"]] = (
            by_type.get(cand["_pain_source_type"], 0) + 1
        )
    click.echo(
        f"Candidates: {summary['candidates']} "
        f"({by_type['poster']} posters, {by_type['commenter']} commenters, "
        f"{by_type['liker']} likers).\n"
    )

    # ── Phase 4: SN profile enrichment (title/company/location) ─────
    if candidates:
        urls = [c["defaultProfileUrl"] for c in candidates]
        for start in range(0, len(urls), ENRICH_MAX_PER_LAUNCH):
            chunk = urls[start : start + ENRICH_MAX_PER_LAUNCH]
            click.echo(
                f"Enriching {len(chunk)} profile(s) via the Sales Nav "
                "Profile Scraper..."
            )
            try:
                enrich_csv = scrape_enrichment(chunk)
            except httpx.HTTPStatusError as err:
                if err.response.status_code in (401, 403):
                    raise
                summary["enrich_failures"] += 1
                click.echo(
                    f"  ⚠ enrichment scrape failed "
                    f"(HTTP {err.response.status_code}) — continuing "
                    "with headline-only titles for this chunk.",
                    err=True,
                )
                continue
            except SalesNavConfigError:
                # Deterministic config error (cookie rotated mid-run,
                # scraper deleted) — every later chunk would fail the
                # same way; cascade instead of degrading per chunk.
                raise
            except Exception as err:  # noqa: BLE001 — degrade loud, not dead
                summary["enrich_failures"] += 1
                click.echo(
                    f"  ⚠ enrichment scrape failed "
                    f"({type(err).__name__}: {err}) — continuing with "
                    "headline-only titles for this chunk.",
                    err=True,
                )
                continue
            if not enrich_csv:
                summary["enrich_failures"] += 1
                click.echo(
                    "  ⚠ enrichment scrape returned no CSV (download "
                    "failed or PB wrote no file) — continuing with "
                    "headline-only titles for this chunk.",
                    err=True,
                )
                continue
            summary["enrich_scrapes"] += 1
            _merge_enrichment(candidates, enrich_csv, summary)
        unmatched = summary["candidates"] - summary["enriched"]
        unenriched_posters = sum(
            1 for c in candidates
            if not c.get("_enriched") and c["_pain_source_type"] == "poster"
        )
        click.echo(
            f"  Enriched {summary['enriched']}/{summary['candidates']} "
            f"candidate(s)"
            + (
                f" — {unmatched} scored without SN data "
                f"({unenriched_posters} of them POSTERS, who carry no "
                "headline at all — they score on an empty title and "
                "will almost certainly reject)."
                if unmatched
                else "."
            )
        )
        if summary["enriched"] == 0:
            click.echo(
                "  ⚠ ENRICHMENT DEGRADED: 0 candidates enriched — "
                "engagers score on their headline alone and POSTERS on "
                "nothing (the posts export has no title/company). "
                "Expect a high borderline/reject share, and the loss is "
                "permanent (the posts worker never re-serves these "
                "posts). Check the SN scraper + cookie — or, if scrapes "
                "succeeded, match-back drift in _merge_enrichment — "
                "before the next run.",
                err=True,
            )

    candidates_by_language: dict[str, list[dict]] = {}
    for cand in candidates:
        candidates_by_language.setdefault(
            cand["_pain_language"], []
        ).append(cand)

    if dry_run:
        click.echo(
            "\n--- [DRY RUN] invite-note previews (no CRM writes) ---"
        )
        click.echo(
            "  (Preview language = the matching query's language. The "
            "commit resolves language from the enriched location, so an "
            "engager whose location maps elsewhere ships that language's "
            "note instead — the daily run's per-batch preview always "
            "shows the exact wire note before any send.)"
        )
        for language, rows in sorted(candidates_by_language.items()):
            for raw in rows:
                note = _preview_invite_note(raw)
                click.echo(
                    f"  [{language}/{raw['_pain_source_type']}] "
                    f"{raw.get('fullName') or '?'} — "
                    f"{raw.get('title') or '(no title)'}"
                    + (
                        f" @ {raw['company']}" if raw.get("company") else ""
                    )
                )
                click.echo(f"    post: {raw.get('_pain_post_url')}")
                click.echo(f"    pain: {raw.get('_pain_snippet', '')[:100]}")
                click.echo(
                    f"    note: {note}" if note else
                    "    note: (no pain template for this language — the "
                    "daily run would fall back to the persona note, loudly)"
                )
        click.echo()

    # ── Phase 5: existing qualify pipeline, gates unchanged ──────────
    has_candidates = any(candidates_by_language.values())

    # LLM-verdict availability gate. With `agent_gate=False`,
    # `score_prospect` resolves borderlines (the DOMINANT band here,
    # since persona_config=None means the size component always
    # abstains) inline via the LLM dispatch path. If dispatch is OFF,
    # quality_gate degrades to the bare threshold: borderlines above it
    # would commit UNVETTED. A wet run therefore REFUSES to score
    # without dispatch; a dry run warns (previews stay useful, nothing
    # commits).
    from workflows.llm_dispatch import DISPATCH_ENABLED_ENV, is_dispatch_enabled
    if has_candidates and not is_dispatch_enabled():
        if not dry_run:
            raise RuntimeError(
                "pain-signal wet run requires the LLM dispatch path "
                f"({DISPATCH_ENABLED_ENV}=1, exported by the operator "
                "skill environment) — without it, borderline candidates "
                "would commit with NO LLM verdict. Refusing to score."
            )
        click.echo(
            f"  ⚠ [DRY RUN] {DISPATCH_ENABLED_ENV} is not enabled — "
            "borderline verdicts are threshold-only in this preview and "
            "will differ from a wet run.",
            err=True,
        )

    if not has_candidates:
        click.echo(
            "No candidates to qualify — skipping. (If scrape failures "
            "are reported above, that is the cause — not filtering.)"
        )
    else:
        anthropic_client = build_anthropic_client()
        # (Schema preflight already ran pre-spend, before Phase 1.)

        seen_urls: set[str] = set()
        industry_cache: dict[str, CompanySignals] = {}
        today = date.today().isoformat()
        for language, rows in sorted(candidates_by_language.items()):
            if not rows:
                continue
            click.echo(f"--- Qualifying {len(rows)} {language} candidates ---")
            summary["exported"] += len(rows)
            _process_prospects(
                rows, crm, list_id, today, dry_run, summary, seen_urls,
                in_list_record_ids,
                persona_config=None,
                borderline_stage=None,
                reprospect_review=reprospect_review,
                anthropic_client=anthropic_client,
                existing_entries=existing_entries,
                in_list_canonical_urls=in_list_canonical_urls,
                name_index=name_index,
                industry_cache=industry_cache,
                lane_entry_attrs=lane_entry_attrs_for,
                default_language=language,
                agent_gate=False,
            )

        # With agent_gate=False the fail-open staging paths (budget
        # ledger down / cost ceiling / transient LLM error) become
        # REJECTS with a typed verdict_path — not staged artifacts. The
        # posts worker's cross-launch dedup will NOT re-serve their
        # posts on a re-run, so the loss is permanent — the condition
        # must be loud.
        llm_no_verdict = summary["rejected_by_path"].get(
            "borderline_llm_error", 0
        ) + summary["rejected_by_path"].get("borderline_cost_exhausted", 0)

        if reprospect_review:
            from workflows.weekly_prospect import (
                EXPORTS_DIR,
                _write_reprospect_review_csv,
            )
            review_path = EXPORTS_DIR / f"pain_reprospect_review_{today}.csv"
            review_path.parent.mkdir(parents=True, exist_ok=True)
            _write_reprospect_review_csv(review_path, reprospect_review)
            click.echo(
                f"  Staged {len(reprospect_review)} re-prospect candidates "
                f"for review → {review_path}"
            )

    # ── Summary ──────────────────────────────────────────────────────
    click.echo("\n--- Pain-Signal Discovery Summary ---")
    click.echo(
        f"Recency:    client-side postTimestamp window {max_age_hours}h "
        "(LinkedIn's datePosted search filter returns zero results — "
        "broken, not used); unparseable timestamps drop fail-closed."
    )
    click.echo(
        f"Queries:    {summary['queries_run']} run · "
        f"{summary['queries_skipped_disabled']} disabled · "
        f"{summary['scrape_failures']} scrape failures"
        + (
            " · ⚠ CIRCUIT BREAKER TRIPPED (remaining launches skipped; "
            "harvested candidates were still processed)"
            if summary["circuit_breaker_tripped"]
            else ""
        )
    )
    click.echo(
        f"Posts:      {summary['posts_found']} found · "
        f"{summary['posts_deduped']} duplicates · "
        f"{summary['posts_no_url']} no-URL rows · "
        f"{summary['posts_fresh']} fresh "
        f"({summary['posts_dropped_stale']} stale, "
        f"{summary['posts_dropped_no_timestamp']} no-timestamp) · "
        f"{summary['posts_on_topic']} on-topic "
        f"({summary['posts_dropped_offtopic']} off-topic, "
        f"{summary['posts_dropped_empty_text']} no-text)"
    )
    click.echo(
        f"Engagers:   {summary['commenter_scrapes']} commenter + "
        f"{summary['liker_scrapes']} liker scrape(s) · "
        f"{summary['engager_rows']} rows · "
        f"{summary['engager_rows_no_url']} no-URL rows · "
        f"{summary['engagers_first_degree']} already-connected · "
        f"{summary['engagers_deduped']} duplicates · "
        f"{summary['posts_no_engagement']} zero-engagement post(s) · "
        f"{summary['posts_engager_capped']} post(s) over engager cap · "
        f"{summary['posters_no_profile_url']} poster(s) without URL"
    )
    click.echo(
        f"Pipeline:   {summary['engagers_already_in_pipeline']} already "
        f"in pipeline (pre-drop) · {summary['engagers_capped']} over the "
        f"{max_engagers}-candidate cap"
    )
    click.echo(
        f"Enrichment: {summary['enrich_scrapes']} scrape(s) · "
        f"{summary['enriched']} enriched · "
        f"{summary['enrich_failures']} failures"
    )
    click.echo(
        f"Candidates: {summary['candidates']} · "
        f"{summary['denylist_blocked']} denylist-blocked · "
        f"{summary['scored']} scored · {summary['qualified']} qualified · "
        f"{summary['rejected']} rejected · {summary['duplicates']} duplicates"
    )
    if summary["rejected_by_path"]:
        for path, count in sorted(
            summary["rejected_by_path"].items(), key=lambda kv: -kv[1]
        ):
            click.echo(f"            {count} via {path}")
    if llm_no_verdict:
        click.echo(
            f"⚠️  LLM QUALIFIER ALARM: {llm_no_verdict} borderline "
            "candidate(s) rejected WITHOUT an LLM verdict (ledger/cost/"
            "transient failure) — and a re-run canNOT recover them (the "
            "posts worker's cross-launch dedup will not re-serve their "
            "posts). Fix the dispatch/ledger issue BEFORE the next run.",
            err=True,
        )
    if summary.get("dedup_gate_degraded"):
        click.echo(
            f"⚠ Dedup gate: {summary['dedup_gate_degraded']} suspected "
            "duplicate(s) committed WITHOUT company confirmation (CRM "
            "fetch failed) — spot-check for dup records.",
            err=True,
        )
    if summary.get("write_errors"):
        click.echo(
            f"⚠ Writes:   {summary['write_errors']} commit(s) failed on a "
            "CRM validation error — those candidates were dropped this "
            "run.",
            err=True,
        )
    click.echo(
        f"Added:      {summary['added']} "
        f"({summary['net_new_created']} net-new; "
        f"{summary.get('reprospect_review', 0)} staged for re-prospect review)"
    )
    if summary.get("borderline_staged"):
        # agent_gate=False should make staging unreachable; if a
        # quality-gate change re-opens it, the count must not be silent
        # (nothing writes an artifact on this path any more).
        click.echo(
            f"⚠ Unexpected: {summary['borderline_staged']} candidate(s) "
            "flagged for agent staging despite agent_gate=False — NOT "
            "committed and NOT staged to any artifact; check "
            "quality_gate.score_prospect's staging conditions.",
            err=True,
        )
    if dry_run:
        click.echo(
            "\n[DRY RUN] Nothing was written to the CRM. Committed "
            "prospects would enter at Prospect stage behind the standard "
            "quarantine; invites go out via the daily run with per-batch "
            "review."
        )
    return summary
