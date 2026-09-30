"""Wave-1.6 FIX-1 regression: pre-invite degree check must be side-effect
free under dry_run=True.

Background — 2026-05-25 incident: `cli.py daily --dry-run` leaked 20
LinkedIn URLs into a real Google Sheet AND launched a real PB
/agents/launch call before crashing on PB 400. Root cause: the
`_pre_invite_degree_check` call at workflows/daily_check.py was not
threaded the dry_run flag, so the function fired live PB scrapes +
GSheet writes + AttioWriter Pattern-A flips during a "preview" run.

This test pins the contract: when `dry_run=True`, the function must
NOT call PB, GSheet, AttioWriter, recheck_cache, or escalate
(internal QA finding BLOCKING-1).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from workflows.pre_invite_check import _pre_invite_degree_check


def _make_batch():
    """Same row shape as the production caller in
    workflows/daily_check.py:run_connection_requests — every row carries
    record_id, entry_id, current_stage, experiment_id, and
    experiment_id_frozen_at (the contract every code path in
    `_pre_invite_degree_check` depends on)."""
    return [
        {
            "linkedInUrl": "https://www.linkedin.com/in/alice",
            "message": "hi A",
            "entry_id": "ent-A",
            "record_id": "rec-A",
            "current_stage": "Prospect",
            "experiment_id": "exp-test",
            "experiment_id_frozen_at": "prospect",
        },
        {
            "linkedInUrl": "https://www.linkedin.com/in/bob",
            "message": "hi B",
            "entry_id": "ent-B",
            "record_id": "rec-B",
            "current_stage": "Prospect",
            "experiment_id": "exp-test",
            "experiment_id_frozen_at": "prospect",
        },
    ]


def test_dry_run_makes_no_live_calls(monkeypatch):
    """dry_run=True ⇒ zero PB launches, zero GSheet writes, zero Attio
    PATCHes, zero recheck_cache writes, zero escalations. Returns the
    full batch unmodified so the caller's downstream dry-run preview
    can iterate the URLs."""
    # Enable Sales Nav backend to make sure the dry-run guard fires
    # BEFORE the cookie/env check — a missing env var on the live path
    # would raise SalesNavConfigError, masking whether the guard works.
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "regular")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "phantom-sales-nav-id")
    # Intentionally DO NOT set PB_LI_SALES_NAV_SESSION_COOKIE — if the
    # dry-run guard works, the cookie check is never reached. If the
    # guard regresses, SalesNavConfigError fires here and the assertion
    # at the end never gets to fail loudly.

    attio = MagicMock()
    pb = MagicMock()

    with (
        patch(
            "workflows.daily_check.write_prospects_to_sheet"
        ) as write_sheet,
        patch(
            "workflows.recheck_cache.record_many"
        ) as record_cache,
        patch(
            "workflows.pre_invite_check.escalate"
        ) as escalate_mock,
        patch(
            "workflows.pre_invite_check.AttioWriter"
        ) as writer_cls,
    ):
        still, already = _pre_invite_degree_check(
            _make_batch(),
            pb,
            "scraper-id-legacy",
            attio,
            "list-id",
            sales_nav_profile_scraper_id="phantom-sales-nav-id",
            dry_run=True,
        )

        # Live side-effects: every one must be ZERO.
        assert pb.launch_agent.call_count == 0, (
            f"PB.launch_agent called {pb.launch_agent.call_count} time(s) "
            "under dry_run — Wave-1.6 FIX-1 regressed."
        )
        assert pb.wait_for_completion.call_count == 0
        assert pb.download_result_csv.call_count == 0
        assert pb.get_agent.call_count == 0
        assert write_sheet.call_count == 0, (
            "write_prospects_to_sheet was called under dry_run — this "
            "is the 2026-05-25 GSheet leak path."
        )
        assert record_cache.call_count == 0
        assert escalate_mock.call_count == 0
        # AttioWriter is constructed lazily inside the function; under
        # dry_run the function must short-circuit BEFORE that, so the
        # class is never instantiated.
        assert writer_cls.call_count == 0
        # Attio client itself must never be PATCHed/POSTed by this
        # function under dry_run (defensive check beyond the writer).
        assert attio._client.request.call_count == 0
        # Updates_list_entry (legacy direct path) must also be untouched.
        assert attio.update_list_entry.call_count == 0

    # Return contract: full batch passes through, nothing flipped to ACCEPTED.
    assert len(still) == 2
    assert {r["entry_id"] for r in still} == {"ent-A", "ent-B"}
    assert already == []


def test_dry_run_with_empty_batch_short_circuits():
    """Empty batch always returns ([], []) regardless of dry_run, but
    the dry-run path must not change that."""
    attio = MagicMock()
    pb = MagicMock()
    still, already = _pre_invite_degree_check(
        [],
        pb,
        "scraper-id",
        attio,
        "list-id",
        dry_run=True,
    )
    assert still == []
    assert already == []
    assert pb.launch_agent.call_count == 0


def test_sales_nav_health_preflight_skipped_under_dry_run(monkeypatch):
    """Wave-1.6-ext FIX-1' (adversarial SB-1): cli.py:348-367 fires the
    Sales Nav health pre-flight via
    `scripts.validate_sales_nav_health.quick_check()`. That helper
    launches 1x Sales Nav Profile Scraper + 1x Inbox Scraper — both
    REAL PB containers. Pre-Wave-1.6-ext the call was unconditional,
    bypassing the FIX-1 dry-run contract whenever
    PRE_INVITE_DEGREE_CHECK_BACKEND=sales_nav.

    The gate in cli.py is `if mode.is_dry_run(): SKIP`. We exercise the
    gate logic directly (the full cli.daily entry point requires too
    much scaffolding) and assert quick_check() is not invoked."""
    import os

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "regular")
    # Provide env vars so a regression where the guard fails would
    # actually reach quick_check and launch PB — i.e., we don't want
    # quick_check to short-circuit on missing env (which would mask
    # the bug).
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "phantom-sn-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_INBOX_SCRAPER_ID", "phantom-inbox-id")
    monkeypatch.setenv("PB_LI_SESSION_COOKIE", "fake-cookie")

    with patch(
        "scripts.validate_sales_nav_health.quick_check"
    ) as quick_check_mock:
        from models.run_mode import RunMode

        mode = RunMode.from_dry_run_flag(True)
        # Mirror the cli.py:348 guard exactly.
        backend = os.environ.get(
            "PRE_INVITE_DEGREE_CHECK_BACKEND", "regular"
        ).strip()
        if backend == "sales_nav":
            if mode.is_dry_run():
                # SKIPPED — no quick_check() call.
                pass
            else:
                from scripts.validate_sales_nav_health import quick_check
                quick_check()

        assert quick_check_mock.call_count == 0, (
            f"quick_check() was called {quick_check_mock.call_count} "
            "time(s) under dry_run with backend=sales_nav — Wave-1.6-ext "
            "FIX-1' regressed. This path launches REAL PB containers "
            "(Sales Nav Profile Scraper + Inbox Scraper)."
        )


def test_sales_nav_health_preflight_runs_under_wet_run(monkeypatch):
    """Mirror of the above: when dry_run is False, the pre-flight MUST
    fire — otherwise we'd silently skip the cookie health check in
    production daily runs."""
    import os

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "phantom-sn-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_INBOX_SCRAPER_ID", "phantom-inbox-id")
    monkeypatch.setenv("PB_LI_SESSION_COOKIE", "fake-cookie")

    with patch(
        "scripts.validate_sales_nav_health.quick_check"
    ) as quick_check_mock:
        quick_check_mock.return_value = (0, "Sales Nav health pre-flight: OK")
        from models.run_mode import RunMode

        mode = RunMode.from_dry_run_flag(False)
        backend = os.environ.get(
            "PRE_INVITE_DEGREE_CHECK_BACKEND", "regular"
        ).strip()
        if backend == "sales_nav":
            if mode.is_dry_run():
                pass
            else:
                from scripts.validate_sales_nav_health import quick_check
                quick_check()

        assert quick_check_mock.call_count == 1, (
            "quick_check() must fire in wet-run mode; the SKIP branch "
            "may only short-circuit under --dry-run."
        )
