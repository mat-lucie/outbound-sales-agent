"""Shared test fixtures."""

import os
import sys
from pathlib import Path

import pytest

# Ensure project root is on path
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

# Point the engine at the bundled synthetic reference operator content
# (examples/acme/content/) for the whole suite. The repo-root `content/` ships
# NEUTRAL placeholder defaults with a REPLACE_THIS_TEMPLATE sentinel that the
# live-send gate refuses to send; tests assert against the Acme reference content
# (schema-complete, non-sentinel) and never trip the sentinel. Set at module
# import time — BEFORE any production module is imported during collection —
# because the four `CONTENT_DIR` constants resolve `OUTBOUND_CONTENT_DIR` once at
# import. A test that wants the shipped placeholders can override this env var and
# reimport, or monkeypatch the module constant directly.
#
# Set UNCONDITIONALLY (not setdefault): if a developer has OUTBOUND_CONTENT_DIR
# exported in their shell, setdefault would let that ambient value silently
# hijack the whole suite (tests would assert against an arbitrary directory and
# could mask a regression). Pinning it guarantees the suite always runs against
# the bundled reference content; per-test monkeypatch.setenv still overrides for
# the duration of a test and is restored afterward.
os.environ["OUTBOUND_CONTENT_DIR"] = str(
    _REPO_ROOT / "examples" / "acme" / "content"
)

# Point the engine at the bundled synthetic reference operator CONFIG
# (examples/acme/config/icp.yaml) for the whole suite — the exact parallel to the
# OUTBOUND_CONTENT_DIR pin above. The repo-root config/ ships the same NEUTRAL
# config/icp.example.yaml default; the Acme config mirrors it. The golden +
# scoring tests assert Acme's exact scores/bands and the byte-identical rendered
# qualifier prompt, so the suite must resolve the Acme ICP. Set at module import
# time — BEFORE workflows.quality_gate is imported during collection — because
# that module bakes `_ICP = load_icp_config()` and renders QUALIFIER_SYSTEM_PROMPT
# once at import from config_dir().
#
# NOTE (baseline semantics): the goldens are regenerated against this synthetic
# operator. They are self-consistent regression guards on the scoring/render
# LOGIC — not the original post==pre-refactor equivalence proof, which was retired
# with the original operator's reference data. See examples/acme/README.md.
#
# config_dir()/OUTBOUND_CONFIG_DIR is ALL-OR-NOTHING across crm/icp/phantombuster,
# but only the ICP loader and the qualifier-template render consult it eagerly:
#   * icp.yaml             -> examples/acme/config/icp.yaml (synthetic ICP)
#   * prompts/qualifier.md.j2 -> examples/acme/config/prompts/ (byte-identical copy
#     of the operator-neutral shipped template; byte-identical render)
#   * crm.yaml / phantombuster.yaml are ABSENT here, and their loaders read only
#     the live *.yaml (never *.example.yaml), so they fall back to the exact same
#     attio/identity/env defaults they use at repo-root — no ConfigError.
#
# Set UNCONDITIONALLY (not setdefault) for the same reason as the content pin: a
# developer's ambient OUTBOUND_CONFIG_DIR must not silently hijack the suite.
# Tests that need the repo-root neutral default (e.g. the shipped-config guard)
# or a toy config (the operator-driven prompt tests) delenv/override this var in
# their own scope, which monkeypatch restores afterward.
os.environ["OUTBOUND_CONFIG_DIR"] = str(_REPO_ROOT / "examples" / "acme" / "config")

# Refresh the mtimes of the bundled Acme target-company lists so the weekly
# freshness gate (models/freshness.py, mtime-based, STALE at 60d) never trips
# inside the suite. These are checked-in example fixtures: their CONTENT is
# stable but their file mtime ages with the checkout, so integration tests that
# drive the real `run_weekly_prospecting` path started failing ~60 days after
# the files were last touched (a date-bomb, not a product defect). Touching
# mtime only — git does not track it, and the gate's PRODUCTION behavior is
# covered by dedicated freshness tests that construct their own aged files.
for _target_list in (
    _REPO_ROOT / "examples" / "acme" / "content"
).glob("*-targets.json"):
    os.utime(_target_list, None)


@pytest.fixture(autouse=True)
def _isolate_run_provenance(monkeypatch):
    """Pin the PR-228 checkout-staleness preflight to a current/clean stub.

    ``assert_checkout_current`` (wired into the ``daily`` / ``send-dms`` /
    ``weekly`` CLI entrypoints) runs real git against the developer's
    checkout — including a network ``git fetch`` on wet paths. Without this
    stub, every CLI-level wet test aborts with StaleCheckoutError whenever
    the checkout happens to be behind origin/main (and dials the network
    from inside the suite). Tests that exercise the preflight itself
    (test_run_provenance.py) stack their own patch of
    ``collect_run_provenance`` — or patch ``_git`` and call the real
    function via their direct import, which this module-attr setattr does
    not rebind.
    """
    from workflows import run_provenance

    monkeypatch.setattr(
        run_provenance, "collect_run_provenance",
        lambda **kwargs: {
            "sha": "testsha00000", "branch": "test",
            "dirty": False, "behind_origin_main": False,
        },
    )


@pytest.fixture(autouse=True)
def _reset_denylist_token_cache():
    """Drop the process-level denylist-token memo between tests.

    ``weekly_prospect._denylist_tokens_cached`` parses the Botdog YAML once per
    process (``is_denylisted_candidate`` runs per candidate ROW, so re-parsing
    per call meant thousands of parses per ingest). The cache is keyed on
    ``OUTBOUND_CONFIG_DIR``, which the suite pins globally — so a test that
    monkeypatches ``denylist_tokens`` itself would otherwise be served an
    earlier test's tokens. Cleared on both sides so neither direction leaks.
    """
    from workflows.weekly_prospect import _denylist_tokens_cached

    _denylist_tokens_cached.cache_clear()
    yield
    _denylist_tokens_cached.cache_clear()


@pytest.fixture(autouse=True)
def _isolate_anthropic_api_key(monkeypatch):
    """Guarantee no test ever hits the real Anthropic API.

    Tests that exercise the LLM path set ANTHROPIC_API_KEY explicitly via
    monkeypatch inside their own scope — this autouse fixture removes any
    ambient key first so the isolation is reliable regardless of CI env.
    """
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


@pytest.fixture(autouse=True)
def _isolate_llm_dispatch_env(monkeypatch):
    """Guarantee no test ever picks up an ambient ``OUTBOUND_USE_LLM_DISPATCH=1``.

    Per silent-failure-hunter HIGH (F-PR-9): a developer-shell-local
    export of this var would silently flip every no-client caller into
    dispatch mode and either hang on the 300s default timeout or hit a
    real Claude Code session. Tests that exercise dispatch set the env
    var explicitly inside their own scope (see
    ``test_response_classifier_env_var_enables_dispatch``).
    """
    monkeypatch.delenv("OUTBOUND_USE_LLM_DISPATCH", raising=False)


@pytest.fixture(autouse=True)
def _isolate_attio_api_key(monkeypatch):
    """Guarantee no test ever hits the real Attio API.

    When ATTIO_API_KEY is present (developer .env, exported shell var),
    LLM-dispatch tests that don't pass an explicit AttioClient mock leak
    through ``LLMBudgetLedger.try_reserve`` into live ``api.attio.com``
    requests against the ``llm_budget_ledger`` object — failing with 404
    (object not provisioned) or httpx.LocalProtocolError (empty bearer)
    instead of exercising the dispatch round-trip they intend to test.
    Deleting the env var here routes those paths through the documented
    KeyError fail-soft branch in ``LLMBudgetLedger.try_reserve``. Tests
    that exercise live-Attio code paths (PR-35 ledger, PR-9b backfill,
    suppress-re-engagement backfill) set ATTIO_API_KEY explicitly via
    their own monkeypatch fixture — those stack on top of this delenv.
    """
    monkeypatch.delenv("ATTIO_API_KEY", raising=False)


@pytest.fixture(autouse=True)
def _isolate_degree_check_backend_env(monkeypatch):
    """Pin every test to a deterministic degree-check backend baseline.

    ``PRE_INVITE_DEGREE_CHECK_BACKEND`` (+ the SN scraper-id / cookie vars
    it gates on) is read at call time by both the pre-invite degree check
    and Phase 0 ``detect_accepted_connections``. Without pinning, whatever
    the operator has in their developer ``.env`` (or a prior test left set)
    leaks into the suite and tests fail order-dependently.

    The baseline is an EXPLICIT ``regular``: the code default is now
    ``sales_nav`` (DEGREE_CHECK_BACKEND_DEFAULT — the legacy agent was
    deleted from the PB workspace), and under that default a bare
    environment raises SalesNavConfigError from
    ``_resolve_degree_check_backend`` (missing SN scraper-id/cookie). The
    bulk of the degree-check suite predates the flip and exercises the
    legacy code path with mocked PB clients — explicit ``regular`` keeps
    that coverage meaningful, mirroring how a re-deployed legacy phantom
    would be selected in production. Tests that exercise the sales_nav
    branch, or the default resolution itself, monkeypatch.setenv/delenv on
    top of this fixture.
    """
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "regular")
    for var in (
        "PB_SALES_NAV_PROFILE_SCRAPER_ID",
        "PB_LI_SALES_NAV_SESSION_COOKIE",
        "OUTBOUND_INBOX_IDENTITY_MAP",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(autouse=True)
def _isolate_recheck_cache(monkeypatch, tmp_path):
    """Redirect the recheck cache file to a per-test temp path.

    Without this, tests that exercise daily_check would read/write the real
    ~/.outbound-agent/recheck_cache.json and contaminate one another (and the
    user's live cache). ``_LOCK_FILE`` is patched alongside (ported from
    upstream PR-224): ``_exclusive_lock`` creates it on first use, so leaving
    it unpatched drops a real ``~/.outbound-agent/recheck_cache.lock`` (a
    fake-HOME sweep of the suite confirmed the leak).
    """
    from workflows import recheck_cache

    monkeypatch.setattr(recheck_cache, "CACHE_FILE", tmp_path / "recheck_cache.json")
    monkeypatch.setattr(recheck_cache, "_LOCK_FILE", tmp_path / "recheck_cache.lock")


@pytest.fixture(autouse=True)
def _isolate_safety_limits(monkeypatch, tmp_path):
    """Redirect the daily-limits ledger file to a per-test temp path.

    Ported from upstream PR-223. Without this, any test that reaches
    ``get_remaining()`` / ``record_connections()`` — e.g. via
    ``run_connection_requests`` — reads and writes the operator's real
    ``~/.outbound-agent/daily_limits.json``. That made multi-row invite tests
    fail only on days when production runs had already consumed the real cap
    (the ledger trims the invite target, so a 2-row batch collapses to 1 row),
    and risked test runs charging fake sends against the live ledger. Tests
    that need specific ledger state (test_safety_limits.py's
    ``_patch_limits_file``) stack their own setattr on top of this baseline.
    """
    from workflows import safety_limits

    monkeypatch.setattr(safety_limits, "LIMITS_DIR", tmp_path)
    monkeypatch.setattr(safety_limits, "LIMITS_FILE", tmp_path / "daily_limits.json")


@pytest.fixture(autouse=True)
def _isolate_botdog_state(monkeypatch, tmp_path):
    """Redirect the Botdog submission ledger + poll cursor to temp paths.

    Both live under ``~/.outbound-agent/`` alongside the other local state.
    ``record_submission`` and ``write_cursor`` are plain file writes with no
    dry-run guard, so any test that reaches the submission ledger or the
    event drain unpatched would write into the operator's real home — and
    the ledger is a duplicate-SEND guard, so a stale test entry there could
    suppress a real DM. Isolated for every test by default; tests that need
    specific state stack their own setattr on top of this baseline.
    """
    from workflows import botdog_ingest, botdog_ledger

    ledger_dir = tmp_path / "botdog-ledger"
    poll_dir = tmp_path / "botdog-poll"
    monkeypatch.setattr(botdog_ledger, "LEDGER_DIR", ledger_dir)
    monkeypatch.setattr(
        botdog_ledger, "LEDGER_FILE", ledger_dir / "botdog_submissions.json"
    )
    monkeypatch.setattr(botdog_ingest, "POLL_STATE_DIR", poll_dir)
    monkeypatch.setattr(
        botdog_ingest, "POLL_STATE_FILE", poll_dir / "botdog_poll.json"
    )


@pytest.fixture(autouse=True)
def _isolate_audit_dir(monkeypatch, tmp_path):
    """Redirect the audit JSONL dir to a per-test temp path.

    Ported from upstream PR-224. Without this, every test that enters a real
    ``AuditLogger`` — directly or transitively via CLI / daily / weekly
    integration paths — writes a ``run-<date>-<id>.jsonl`` of fixture events
    into the operator's real ``~/.outbound-agent/audit/`` (a fake-HOME sweep of
    the suite left such files behind), polluting the forensic log that
    ``resume_from_audit`` / ``audit_stats`` read in production. Opt-in setattrs
    (test_audit.py) stack on top; the deliberate real-HOME F-PR-3 bootstrap
    read in test_migrations_idempotent.py uses its own ``Path.home()`` constant
    (``F_PR_3_AUDIT_DIR``) and is read-only, so it is unaffected.
    """
    from workflows import audit

    monkeypatch.setattr(audit, "AUDIT_DIR", tmp_path / "audit")


@pytest.fixture(autouse=True)
def _isolate_run_lock_dir(monkeypatch, tmp_path):
    """Redirect the default run-lock dir to a per-test temp path.

    Ported from upstream PR-224. Without this, tests that reach
    ``acquire_run_lock`` / ``run_with_lock`` without an explicit ``lock_dir``
    — the email-daily and learn CLI paths do (a fake-HOME sweep left
    ``locks/*.lock`` files behind) — create lock files in the operator's real
    ``~/.outbound-agent/locks/`` and could collide with a genuinely running
    production lock, failing the test or confusing stale-holder triage.
    test_run_lock.py passes ``lock_dir`` explicitly and stacks fine.
    """
    from workflows import run_lock

    monkeypatch.setattr(run_lock, "DEFAULT_LOCK_DIR", tmp_path / "locks")


@pytest.fixture(autouse=True)
def _isolate_association_sent_state(monkeypatch, tmp_path):
    """Redirect the association-outreach sent-state file to a per-test temp path.

    Ported from upstream PR-224. Without this, tests that call
    ``get_pending_association_emails`` / ``run_association_outreach`` unpatched
    read the operator's real ``~/.outbound-agent/association_outreach_sent.json``
    — pass/fail would depend on which contacts production has already marked
    sent — and the divergence-repair path could write fixture entries back into
    the live file. Per-test ``patch(...)`` calls stack on top of this baseline.
    """
    from workflows import association_outreach

    monkeypatch.setattr(
        association_outreach,
        "SENT_STATE_FILE",
        tmp_path / "association_outreach_sent.json",
    )


@pytest.fixture(autouse=True)
def _isolate_llm_dispatch_dirs(monkeypatch, tmp_path):
    """Redirect the LLM-dispatch inbox/outbox dirs to per-test temp paths.

    Ported from upstream PR-224. ``request_llm_dispatch`` has no env gate of
    its own — a single call without explicit ``inbox_dir``/``outbox_dir``
    writes a request file into the operator's real
    ``~/.outbound-agent/llm_dispatch/inbox/`` and could consume a stale
    production outbox response as its result. No current test reaches the
    defaults unpatched (the fake-HOME sweep showed no leak), so this is
    defense-in-depth against the same class of bug the sibling fixtures fix.
    test_llm_dispatch.py's explicit dirs and test_llm_dispatch_callers.py's
    setattrs stack on top.
    """
    from workflows import llm_dispatch

    dispatch_dir = tmp_path / "llm_dispatch"
    monkeypatch.setattr(llm_dispatch, "DEFAULT_DISPATCH_DIR", dispatch_dir)
    monkeypatch.setattr(llm_dispatch, "DEFAULT_INBOX_DIR", dispatch_dir / "inbox")
    monkeypatch.setattr(llm_dispatch, "DEFAULT_OUTBOX_DIR", dispatch_dir / "outbox")


@pytest.fixture(autouse=True)
def _arm_email_lane(monkeypatch):
    """Arm the email kill switch for the suite.

    ``OUTBOUND_EMAIL_ENABLED`` ships UNSET (the drip senders are disarmed by
    default — see workflows/email_lane_gate.py). The email send-path tests
    predate the switch and deliberately exercise live-send branches against
    mocks, so arm it here rather than editing every one of them. Tests that
    assert the gate itself delete the var in their own scope
    (tests/test_email_lane_gate.py).
    """
    monkeypatch.setenv("OUTBOUND_EMAIL_ENABLED", "1")


@pytest.fixture(autouse=True)
def _email_compliance_baseline_env(monkeypatch):
    """Give the suite a compliant email baseline so the CAN-SPAM send-gate
    (workflows.email_compliance.assert_email_compliance_ready) doesn't block the
    many existing live-send tests, which legitimately exercise the send path.

    Sets the three hard gates (physical address, resolvable sender org,
    unsubscribe address). Tests that verify the gate's fail-loud behavior
    delenv the specific var in their own scope (monkeypatch restores it
    afterward).
    """
    monkeypatch.setenv("EMAIL_PHYSICAL_ADDRESS", "123 Test St, Test City, TC 00000")
    monkeypatch.setenv("EMAIL_SENDER_ORG", "Test Org")
    monkeypatch.setenv("EMAIL_UNSUBSCRIBE_MAILTO", "unsubscribe@example.com")


@pytest.fixture(autouse=True)
def _isolate_email_sent_ledger(monkeypatch, tmp_path):
    """Redirect the email idempotency sent-ledger to a per-test temp path.

    Without this, any test that drives run_email_daily into a live send would
    append to the operator's real ~/.outbound-agent/email_sent.json and leak state
    across tests (a contact "already sent" in one test would be skipped in
    another). Per-test isolation keeps the ledger deterministic.
    """
    from workflows import email_compliance

    monkeypatch.setattr(email_compliance, "LEDGER_DIR", tmp_path)
    monkeypatch.setattr(
        email_compliance, "LEDGER_FILE", tmp_path / "email_sent.json"
    )


@pytest.fixture(autouse=True)
def _isolate_attio_dlq(monkeypatch, tmp_path):
    """Redirect the Attio dead-letter-queue dir to a per-test temp path.

    Without this, any test that drives AttioWriter into ``_dlq_and_escalate`` —
    directly, or transitively via daily_check / detect_responses — appends a
    forensic row to the operator's real ``~/.outbound-agent/dlq/attio-<date>.jsonl``.
    That both pollutes the prod DLQ and risks a genuine prod failure being lost
    among test fixtures. test_attio_writer.py's ``dlq_tmp`` only covered tests
    that opt in; this autouse fixture covers the whole suite. Returns the dir so
    opt-in fixtures (``dlq_tmp``) can delegate to it by requesting the fixture by
    name — leading underscore kept for convention parity with the siblings above.
    """
    from clients import attio_writer

    dlq_dir = tmp_path / "dlq"
    monkeypatch.setattr(attio_writer, "DLQ_DIR", dlq_dir)
    return dlq_dir


@pytest.fixture(autouse=True)
def _isolate_weekly_kpi_reports(monkeypatch, tmp_path):
    """Redirect the weekly KPI sidecar dir to a per-test temp path.

    Without this, any test that reaches ``run_weekly_report`` without the
    opt-in ``kpi_dir`` fixture (e.g. the merged_into filter test, which
    drives the dry-run integration path) writes a fixture-data snapshot to
    the real ``reports/weekly-kpi/`` — and because the supersede-on-rerun
    audit logic renames the prior file aside, every suite run grows a new
    ``<week>_superseded_<n>.json``. Returns the dir so opt-in fixtures can
    delegate; ``kpi_dir`` in test_pr30_weekly_report stacks on top via its
    own setattr.
    """
    from workflows import weekly_report

    kpi_dir = tmp_path / "weekly-kpi"
    monkeypatch.setattr(weekly_report, "REPORTS_DIR", kpi_dir)
    return kpi_dir


@pytest.fixture(autouse=True)
def _isolate_scrape_cursors(monkeypatch, tmp_path):
    """Redirect the weekly ingest-cursor state file to a per-test temp path.

    Same rationale as the ledger fixtures above. The weekly search loop
    reads and advances ``exports/scrape_cursors.json`` on every search;
    without isolation the integration tests both write fixture cursors into
    the operator's real exports dir AND contaminate each other (a 1-row
    fixture CSV consumed by one test makes the next test's identical CSV a
    ZERO-DELTA no-op). Tests that assert cursor behavior pass an explicit
    path or read through this same default.
    """
    from workflows import scrape_cursor

    monkeypatch.setattr(
        scrape_cursor, "DEFAULT_CURSOR_PATH", tmp_path / "scrape_cursors.json"
    )


def scrape_delta_from_csv(csv_text: str, *, csv_name: str = "wk-test-search"):
    """Build the ``ScrapeDelta`` a stubbed ``_launch_and_download`` returns.

    Tests that stub the harvest layer only care about the rows; this keeps
    them from hand-rolling the parse (and from drifting when the delta's
    shape changes). Mirrors the real function: parse once, drop DictReader's
    ragged-row ``None`` key, report the file total.
    """
    import csv as _csv
    import io as _io

    from workflows.weekly_prospect import ScrapeDelta, ScrapeStatus

    rows = list(_csv.DictReader(_io.StringIO(csv_text)))
    for row in rows:
        row.pop(None, None)
    status = ScrapeStatus.OK if rows else ScrapeStatus.NO_DATA
    return ScrapeDelta(status, rows, csv_name, len(rows))


def transient_attio_500():
    """Build the transient Attio 500 used by retry-path tests.

    Shared by test_escalation.py-style suites and
    test_finalize_idempotency.py so the error shape can't drift per file
    when the retry contract changes.
    """
    import httpx

    request = httpx.Request("POST", "https://api.attio.com/v2/x")
    response = httpx.Response(500, request=request)
    return httpx.HTTPStatusError("500", request=request, response=response)


@pytest.fixture
def no_retry_sleep(monkeypatch):
    """Neutralize request_with_retry's jittered backoff sleeps.

    Opt-in (not autouse): only retry-path suites need it, and
    test_attio_client.py::TestRequestWithRetry keeps its own
    mock-returning patch for wait assertions.
    """
    import clients.attio as attio_mod

    monkeypatch.setattr(attio_mod.time, "sleep", lambda _s: None)


@pytest.fixture(autouse=True)
def _isolate_acceptance_checkpoint(monkeypatch, tmp_path):
    """Daily tests must never consume or overwrite the operator checkpoint."""
    from workflows import phase_checkpoint
    monkeypatch.setattr(phase_checkpoint, "CHECKPOINT_PATH", tmp_path / "acceptance.json")
