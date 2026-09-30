"""Pre-flight health check for the Sales Navigator pre-invite degree check.

Runs THREE checks before any production scrape. Designed to wedge into
``/sales-daily`` Phase 0 so the operator sees green-light vs warn vs fail
before approving any batch — closes GTM-lens gap on cookie-rotation
surprise (per plan reflective-singing-waterfall.md PR-B.7).

1. **ENV** — `PB_SALES_NAV_PROFILE_SCRAPER_ID` and
   `PB_LI_SALES_NAV_SESSION_COOKIE` are set. Fast, no network.
2. **SALES NAV SCRAPE** — launch the Sales Nav Profile Scraper on a
   known-good URL (defaults to the operator's own profile via
   ``--self-url``). Check ``connectionDegree="You"`` comes back. Confirms
   the Sales Nav cookie + phantom config are alive.
3. **REGULAR COOKIE** — launch the SN Inbox Scraper with
   ``numberOfThreadsToScrape=1`` using the regular ``PB_LI_SESSION_COOKIE``.
   Confirms the parallel-session-invalidation risk (adversarial review F6)
   is not currently triggered — i.e., the Sales Nav cookie didn't kill
   the regular cookie.

Exit codes:

- ``0`` — OK (all 3 checks pass). Safe to proceed.
- ``1`` — FAIL (any check failed). Stop, fix the named issue.
- ``2`` — WARN (a non-fatal check failed, e.g., regular cookie still works
  but the Sales Nav scrape returned a degraded response). Proceed with
  caution.

Usage::

    python3 scripts/validate_sales_nav_health.py
    python3 scripts/validate_sales_nav_health.py --self-url https://linkedin.com/in/<your-slug>
    python3 scripts/validate_sales_nav_health.py --skip-parallel-check

The wedge call in workflows/daily_check.py looks like::

    from scripts.validate_sales_nav_health import quick_check
    rc, summary = quick_check()
    click.echo(summary)
    if rc == 1:
        raise click.ClickException("Sales Nav pre-flight FAILED — see " + summary)
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Literal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv  # noqa: E402

from clients.pb_envelope import AUTH_FAILURE_MARKERS, PBRunFailed, PBRunTimeout  # noqa: E402
from clients.phantombuster import PhantomBusterClient  # noqa: E402

CheckStatus = Literal["ok", "warn", "fail", "skipped"]

DEFAULT_SELF_URL = os.environ.get("OUTBOUND_SELF_LINKEDIN_URL", "")


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    detail: str = ""


@dataclass
class HealthReport:
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        if any(c.status == "fail" for c in self.checks):
            return 1
        if any(c.status == "warn" for c in self.checks):
            return 2
        return 0

    def summary(self) -> str:
        emoji = {"ok": "✓", "warn": "⚠", "fail": "✗", "skipped": "—"}
        lines = ["Sales Nav health pre-flight:"]
        for c in self.checks:
            lines.append(f"  {emoji[c.status]} {c.name}: {c.detail}")
        overall = {0: "OK", 1: "FAIL", 2: "WARN"}[self.exit_code]
        lines.append(f"  → {overall}")
        return "\n".join(lines)


def check_env() -> CheckResult:
    scraper_id = os.environ.get(
        "PB_SALES_NAV_PROFILE_SCRAPER_ID", ""
    ).strip()
    cookie = os.environ.get("PB_LI_SALES_NAV_SESSION_COOKIE", "").strip()
    missing = []
    if not scraper_id:
        missing.append("PB_SALES_NAV_PROFILE_SCRAPER_ID")
    if not cookie:
        missing.append("PB_LI_SALES_NAV_SESSION_COOKIE")
    if missing:
        return CheckResult(
            "env",
            "fail",
            f"missing {missing} — see docs/runbooks/phantombuster-cookie-rotation.md",
        )
    return CheckResult("env", "ok", "scraper-id + cookie set")


def check_sales_nav_scrape(self_url: str) -> CheckResult:
    """Launch one scrape against `self_url`. degree="You" is the expected
    return — confirms cookies + phantom config work."""
    scraper_id = os.environ.get(
        "PB_SALES_NAV_PROFILE_SCRAPER_ID", ""
    ).strip()
    cookie = os.environ.get("PB_LI_SALES_NAV_SESSION_COOKIE", "").strip()
    ua = os.environ.get("PB_LI_USER_AGENT", "").strip()
    if not scraper_id or not cookie:
        return CheckResult("sales_nav_scrape", "skipped", "env check failed")
    with PhantomBusterClient() as pb:
        try:
            agent = pb.get_agent(scraper_id)
            raw = agent.get("argument") or "{}"
            saved = json.loads(raw) if isinstance(raw, str) else raw
            identities = saved.get("identities") or [{}]
            identities[0]["sessionCookie"] = cookie
            if ua:
                identities[0]["userAgent"] = ua
            launch_args = {
                **saved,
                "identities": identities,
                "spreadsheetUrl": self_url,
                "numberOfProfilesPerLaunch": 1,
            }
            launch = pb.launch_agent(scraper_id, launch_args)
            pb.wait_for_completion(launch, poll_interval=10, max_wait=300)
            csv_text = pb.download_result_csv(launch) or ""
        except PBRunFailed as exc:
            return CheckResult(
                "sales_nav_scrape",
                "fail",
                f"PBRunFailed: {exc} — rotate Sales Nav cookies "
                "(docs/runbooks/phantombuster-cookie-rotation.md)",
            )
        except PBRunTimeout as exc:
            return CheckResult(
                "sales_nav_scrape",
                "fail",
                f"PBRunTimeout after {exc.elapsed_seconds}s — PB queue is "
                "saturated or cookies are stale",
            )
    if not csv_text:
        return CheckResult(
            "sales_nav_scrape",
            "fail",
            "scrape returned empty CSV — typically a cookie-expired symptom",
        )
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    if not rows:
        return CheckResult(
            "sales_nav_scrape", "fail", "scrape CSV had headers but zero rows"
        )
    degree = (rows[0].get("connectionDegree") or "").strip()
    if not degree:
        return CheckResult(
            "sales_nav_scrape",
            "warn",
            "scrape returned row but degree is blank — partial success, "
            "may signal degraded cookie",
        )
    return CheckResult(
        "sales_nav_scrape",
        "ok",
        f"scraped {self_url!r} → degree={degree!r}",
    )


def check_regular_cookie_health() -> CheckResult:
    """Launch SN Inbox Scraper with limit=1 to verify the regular cookie
    still works (closes adversarial F6 risk that a Sales Nav scrape can
    invalidate the regular cookie by triggering LinkedIn's concurrent-
    session detection)."""
    inbox_id = os.environ.get("PB_INBOX_SCRAPER_ID", "").strip()
    cookie = os.environ.get("PB_LI_SESSION_COOKIE", "").strip()
    ua = os.environ.get("PB_LI_USER_AGENT", "").strip()
    if not inbox_id:
        return CheckResult(
            "regular_cookie_health",
            "skipped",
            "PB_INBOX_SCRAPER_ID not set — can't probe regular cookie health",
        )
    if not cookie:
        return CheckResult(
            "regular_cookie_health",
            "fail",
            "PB_LI_SESSION_COOKIE not set — regular phantoms unusable",
        )
    args: dict = {
        "inboxFilter": "all",
        "numberOfThreadsToScrape": 1,
        "sessionCookie": cookie,
    }
    if ua:
        args["userAgent"] = ua
    with PhantomBusterClient() as pb:
        try:
            launch = pb.launch_agent(inbox_id, args)
            pb.wait_for_completion(launch, poll_interval=10, max_wait=180)
            last_output = pb.get_container_output(launch.container_id, launch.agent_id)
        except (PBRunFailed, PBRunTimeout) as exc:
            return CheckResult(
                "regular_cookie_health",
                "fail",
                f"{type(exc).__name__}: regular cookie likely invalidated "
                "— rotate PB_LI_SESSION_COOKIE",
            )
    log = (last_output.get("output", "") or "").lower()
    # T1.1: check canonical AUTH_FAILURE_MARKERS from pb_envelope in addition
    # to the legacy "401" / "session cookie ... expired" pair. This closes
    # the 2026-06-05 false-positive where "no valid credentials" / "network-
    # cookie-invalid" log text went undetected and green-lit a dead cookie.
    auth_failed = "401" in log or any(marker in log for marker in AUTH_FAILURE_MARKERS)
    if auth_failed:
        return CheckResult(
            "regular_cookie_health",
            "fail",
            "PB log indicates LinkedIn auth failure (401 / auth marker hit) "
            "— rotate PB_LI_SESSION_COOKIE",
        )
    # T1.1 addendum: a non-zero/failed last-run status is itself a signal.
    if last_output.get("status") == "error":
        return CheckResult(
            "regular_cookie_health",
            "fail",
            "PB container status=error — regular cookie likely invalidated "
            "or phantom mis-configured",
        )
    return CheckResult(
        "regular_cookie_health",
        "ok",
        "inbox scrape completed with regular cookie",
    )


def quick_check(*, self_url: str = "", skip_parallel: bool = False) -> tuple[int, str]:
    """Programmatic entry point for ``/sales-daily`` Phase 0 wedge.

    Returns ``(exit_code, summary_text)``. Exit code 0=OK, 1=FAIL, 2=WARN.
    """
    report = HealthReport()
    env_result = check_env()
    report.checks.append(env_result)
    if env_result.status == "fail":
        # Don't waste PB launches if the env is broken.
        return report.exit_code, report.summary()
    # Resolve here (not at import) so .env loaded by the caller is seen.
    self_url = self_url or os.environ.get("OUTBOUND_SELF_LINKEDIN_URL", "")
    if not self_url:
        report.checks.append(CheckResult(
            name="self_url",
            status="fail",
            detail=(
                "OUTBOUND_SELF_LINKEDIN_URL is not set — the health check "
                "scrapes your own LinkedIn profile as a probe. Set it in "
                ".env or pass --self-url."
            ),
        ))
        return report.exit_code, report.summary()
    report.checks.append(check_sales_nav_scrape(self_url))
    if not skip_parallel:
        report.checks.append(check_regular_cookie_health())
    return report.exit_code, report.summary()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--self-url",
        default=DEFAULT_SELF_URL,
        help=(
            "LinkedIn /in/<your-slug> URL to scrape as the health-check "
            "probe. Defaults to $OUTBOUND_SELF_LINKEDIN_URL; required if "
            "that env var is unset."
        ),
    )
    parser.add_argument(
        "--skip-parallel-check",
        action="store_true",
        help="Skip the regular-cookie-health check (faster, less complete).",
    )
    args = parser.parse_args(argv)
    load_dotenv()
    # Re-check the env after load_dotenv so a .env-provided value is seen
    # (DEFAULT_SELF_URL is read at import time, before .env is loaded).
    self_url = args.self_url or os.environ.get("OUTBOUND_SELF_LINKEDIN_URL", "")
    if not self_url:
        parser.error(
            "--self-url is required (or set $OUTBOUND_SELF_LINKEDIN_URL): "
            "the health check scrapes your own LinkedIn profile as a probe."
        )
    rc, summary = quick_check(
        self_url=self_url, skip_parallel=args.skip_parallel_check
    )
    print(summary)
    return rc


if __name__ == "__main__":
    sys.exit(main())
