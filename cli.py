"""CLI entry point for the Outbound Agent."""

import contextlib
import os
import sys
from datetime import UTC, date, datetime

import click
import httpx
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

load_dotenv()

from clients.outreach_config import load_outreach_config  # noqa: E402 — after sys.path/dotenv setup
from clients.pb_config import load_pb_config  # noqa: E402 — after sys.path/dotenv setup

# Module-level so the send-dms reattach is monkeypatchable as
# ``cli.attach_daily_run`` in tests (the name the gate guards resolve through).
from workflows.daily_run import attach_daily_run  # noqa: E402 — after sys.path/dotenv setup

EXIT_TEMPLATE_REFUSE = 78  # EX_CONFIG: deterministic copy defect needs an operator fix.


@contextlib.contextmanager
def _maybe_resend_client():
    """Yield a ``ResendClient`` if ``RESEND_API_KEY`` is set, else None.

    PR-18 B-SD-012 wiring: production wants the hot-lead alert email
    channel live; dev/test environments without the key get the
    queue-row-only path. ``ResendClient.__init__`` raises ``KeyError``
    on missing env, so we guard explicitly here rather than catching
    the constructor exception inside detect_responses.
    """
    from clients.resend_client import ResendClient

    if not os.environ.get("RESEND_API_KEY"):
        yield None
        return
    client = ResendClient()
    try:
        yield client
    finally:
        client.close()


@contextlib.contextmanager
def _attio_client():
    """Yield the raw ``AttioClient``, constructed through the CRM factory.

    This is the cli's CRM construction seam (P1c). Construction now flows
    through ``clients.crm.factory.get_crm_provider`` — which reads
    ``config/crm.yaml`` when present and otherwise defaults to Attio-from-env,
    the pre-seam behavior. The factory builds the inner ``AttioClient`` and
    composes an ``AttioProvider`` around it; for this increment we hand the
    *raw* inner client (``bundle.attio``) to the workflow call sites unchanged,
    so no workflow signature or behavior changes. Later increments swap callers
    to ``bundle.provider`` and drop this raw accessor.

    The bundle's exit closes the inner client, matching today's
    ``with AttioClient() as attio:`` lifecycle exactly.
    """
    from clients.crm.factory import get_crm_provider

    with get_crm_provider() as bundle:
        yield bundle.attio


@contextlib.contextmanager
def _crm_provider():
    """Yield the vendor-neutral ``CRMProvider``, constructed through the factory.

    This is the migrated counterpart to ``_attio_client`` (P1c-increment-2). A
    command whose whole call-tree reads the CRM only through the ``CRMProvider``
    contract uses this accessor and receives ``bundle.provider`` instead of the
    raw ``bundle.attio`` client. Commands not yet migrated keep using
    ``_attio_client``; both share the same factory + lifecycle, so the bundle's
    exit closes the inner client either way. As more slices migrate, callers move
    from ``_attio_client`` to ``_crm_provider`` one command at a time.
    """
    from clients.crm.factory import get_crm_provider

    with get_crm_provider() as bundle:
        yield bundle.provider


class _DryRunDailyRun:
    """Stub DailyRun for cli.py's --dry-run mode (PR-17).

    PR-17 made ``daily_run`` required on ``run_dm_sequencing`` (removed
    the legacy ``record_messages`` None-branch fallback). Dry-run mode
    still needs a handle so the queue-building / trim-to-cap logic can
    query ``remaining("messages")``, but it must NOT open a real Attio
    daily_run row — that would burn the ``(run_date, machine_id)``
    uniqueness slot and block the real same-day invocation.
    """

    record_id = "dry-run"

    def remaining(self, kind: str) -> int:
        from workflows.daily_run import (
            MAX_CONNECTIONS_PER_DAY,
            MAX_MESSAGES_PER_DAY,
            MAX_VISITS_PER_DAY,
        )
        return {
            "messages": MAX_MESSAGES_PER_DAY,
            "connections": MAX_CONNECTIONS_PER_DAY,
            "visits": MAX_VISITS_PER_DAY,
        }.get(kind, 0)

    def reserve_send(self, kind: str, count: int) -> str:
        return "dry-run-token"

    def confirm_lease(self, token: str, confirmed_count: int | None = None) -> None:
        pass

    def release_lease(self, token: str) -> None:
        pass

    def get_reply_detection_status(self) -> str | None:
        # PR-19 fold-in (salesman-daily-QA-build19 BLOCKING): dry-run
        # must not crash with AttributeError when run_dm_sequencing
        # calls this as its first line. Dry-run always returns None so
        # the Part-B short-circuit guard (`== "failed"`) is False and
        # the function proceeds to its existing `if dry_run: continue`
        # branches.
        return None

    def set_reply_detection_status(self, status: str) -> None:
        # Dry-run no-op: never writes Attio. Phase 0.5 is itself
        # skipped in dry-run mode (cli.py gates it on mode.is_dry_run()),
        # so this is defensive — if a future code path ever calls it
        # under dry-run, no Attio write fires.
        pass


def _pb_agent_unconfigured(value: str) -> bool:
    """True when a PhantomBuster agent id is blank or still a placeholder.

    The shipped `config/phantombuster.example.yaml` carries
    ``REPLACE_WITH_...`` placeholders, and an operator half-way through
    setup may leave a ``TODO``. Treating those as "configured" would launch
    against a nonexistent agent and 404 mid-run, after spend.
    """
    return not value or "REPLACE_WITH" in value or "TODO" in value.upper()


def _experiment_registry_preflight(command: str) -> None:
    """Experiment-registry pre-flight shared by daily / send-dms / weekly /
    pain-signal.

    §0 invariant #9 + the 2026-08-23 silent-failure review: the registry is
    re-read per prospect commit (workflows.weekly_prospect.
    _build_prospect_entry_attrs → get_current_experiment_id), so a persistent
    registry failure (unreadable/malformed experiments.tsv, dead registry
    store) would otherwise recur MID-BATCH — after PhantomBuster spend and
    partial CRM commits. Three arms:

    - MultipleRunningExperimentsError → abort: ambiguous cohort; the
      operator must close one experiment before any send.
    - Any other exception → abort: the registry exists but cannot be read,
      so cohort stamping would fail at every commit anyway. Failing here —
      before the run lock, PB launch, or any CRM write — beats a mid-batch
      traceback with partial state.
    - Clean return (an experiment_id or None) → proceed. A genuinely
      absent registry (missing TSV) reads as no running experiment.
    """
    from models.experiment import (
        MultipleRunningExperimentsError,
        get_current_experiment_id,
    )
    try:
        _ = get_current_experiment_id()
    except MultipleRunningExperimentsError as exc:
        click.echo(
            f"ABORT: cannot run {command} — {exc} "
            "Close one experiment in experiments.tsv and re-run.",
            err=True,
        )
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — any registry failure is terminal here
        click.echo(
            f"ABORT: cannot run {command} — experiment registry is "
            f"unreadable ({type(exc).__name__}: {exc}). The registry is "
            "re-read on every prospect commit, so this failure would recur "
            "mid-batch after PhantomBuster spend. Fix experiments.tsv and "
            "re-run.",
            err=True,
        )
        sys.exit(1)


@click.group()
def cli():
    """Outbound Agent -- LinkedIn outreach automation."""
    pass


def _raise_failed_dm_batches(result: dict) -> None:
    """Make approved delivery failures non-successful without retrying sends."""
    failures = result.get("failed_batches") or []
    if not failures:
        return
    sent = sum(int(result.get(step, 0) or 0) for step in ("dm1", "dm2", "dm3"))
    status = "PARTIAL FAILURE" if sent else "FAILED"
    details = "; ".join(
        f"{batch['step']} container={batch['container_id']} "
        f"requested={batch['requested']} confirmed={batch['confirmed_sent']}"
        for batch in failures
    )
    raise click.ClickException(
        f"DM delivery {status}: {len(failures)} failed approved DM batch(es) "
        f"({details}). Earlier confirmed sends remain counted. "
        "Review pb_silent_no_op evidence before any retry."
    )


@cli.command()
@click.option("--dry-run", is_flag=True, help="Read-only pipeline inventory; no scrapes or writes; live readiness unchecked")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts (for cron)")
@click.option("--batch-size", type=click.IntRange(min=1), default=lambda: load_outreach_config().invite_batch_size, help="Max connection requests per run (default: config/outreach.yaml → caps.invite_batch_size; capped by caps.invites_per_day)")
@click.option("--network-booster-id", default=lambda: load_pb_config().network_booster_id or None, help="PhantomBuster Network Booster agent ID (default: config/phantombuster.yaml → PB_NETWORK_BOOSTER_ID)")
@click.option("--message-sender-id", default=lambda: load_pb_config().message_sender_id or None, help="PhantomBuster Message Sender agent ID (default: config/phantombuster.yaml → PB_MESSAGE_SENDER_ID)")
@click.option("--profile-scraper-id", default=lambda: load_pb_config().profile_scraper_id or None, help="PhantomBuster Profile Scraper agent ID, legacy backend — documented-dead: the agent was deleted from the PB workspace; only useful with a re-deployed phantom (default: config/phantombuster.yaml → PB_PROFILE_SCRAPER_ID)")
@click.option("--sales-nav-profile-scraper-id", default=lambda: load_pb_config().sales_nav_profile_scraper_id or None, help="PhantomBuster Sales Navigator Profile Scraper agent ID, sales_nav backend (the default) (default: config/phantombuster.yaml → PB_SALES_NAV_PROFILE_SCRAPER_ID)")
@click.option("--inbox-scraper-id", default=lambda: load_pb_config().inbox_scraper_id or None, help="PhantomBuster Inbox Scraper agent ID (default: config/phantombuster.yaml → PB_INBOX_SCRAPER_ID)")
@click.option("--skip-dms", is_flag=True, help="Skip Part B DM sequencing (connections only)")
@click.option("--preview-dms-after-invites", is_flag=True, help="Rehearse DMs with the shared record cache after live invites; requires --skip-dms")
@click.option("--skip-followups", is_flag=True, help="Skip Phase C (warm follow-up radar). Phase C is read-only detection; it never sends. Fails-closed-clean when the radar schema is absent, so a schemaless install runs it as a no-op regardless.")
@click.option("--force-weekend", is_flag=True, help="Override the Mon-Fri-only outreach rule")
@click.option(
    "--allow-stale", is_flag=True,
    help="Proceed with a wet run even when the checkout is behind origin/main "
         "(the staleness is still stamped into the run's provenance).",
)
@click.option(
    "--botdog-send-enabled", is_flag=True, envvar="BOTDOG_SEND_ENABLED",
    help="Turn on the OPTIONAL Botdog event drain (default off). It does "
         "NOT enable Botdog sending — PhantomBuster owns all invites and "
         "DMs regardless of this flag, and no send path routes to Botdog. "
         "ON only makes Phase 0.7 poll Botdog lead events so rows stamped "
         "send_channel=botdog can absorb their confirming advances. Set it "
         "in .env — a shell export does not reach a scheduled run. Rows "
         "stamped send_channel=botdog are held out of PB sends either way "
         "until they are re-stamped send_channel=pb.",
)
def daily(dry_run, yes, batch_size, network_booster_id, message_sender_id, profile_scraper_id, sales_nav_profile_scraper_id, inbox_scraper_id, skip_dms, preview_dms_after_invites, skip_followups, force_weekend, allow_stale, botdog_send_enabled):
    """Daily check: send connections, queue DMs, detect responses."""
    if preview_dms_after_invites and (not skip_dms or dry_run):
        raise click.UsageError("--preview-dms-after-invites requires --skip-dms in a live daily run")
    if dry_run:
        from workflows.daily_preview import preview_daily
        preview_daily(skip_dms=skip_dms, force_weekend=force_weekend)
        return
    from clients.gmail import GmailClient, GmailCredentialsMissing
    from clients.phantombuster import PhantomBusterClient
    from models.business_calendar import is_send_day, operator_today
    from models.run_mode import RunMode
    from workflows.audit import AuditLogger
    from workflows.daily_check import (
        BlankMessageError,
        UnresolvedPlaceholderError,
        compute_dm1_sent_cohort_by_date,
        compute_due_dm_counts,
        detect_accepted_connections,
        drain_connection_invites,
        run_connection_requests,
        run_dm_sequencing,
        run_end_summary,
    )
    from workflows.daily_check_helpers import _VALID_BACKENDS
    from workflows.daily_run import (
        ConcurrentRunInAttio,
        DailyRun,
        MalformedDailyRunRow,
        open_daily_run,
    )
    from workflows.detect_email_responses import detect_email_responses
    from workflows.detect_responses import IdentityResolutionHalt, NoCSVHalt, detect_responses
    from workflows.escalation import escalate
    from workflows.metrics import (
        DailyRunMetrics,
        phase_timer,
        record_phase_or_skip,
    )
    from workflows.phase_checkpoint import AcceptanceCheckpoint
    from workflows.record_cache import RecordCache, preload_pipeline_persons
    from workflows.run_evidence import record_run
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )
    from workflows.run_provenance import (
        assert_checkout_current,
        format_provenance,
    )
    from workflows.safety_limits import get_status
    from workflows.starvation import evaluate_pipeline_starvation
    from workflows.weekly_prospect import _attio_inner_client

    mode = RunMode.from_dry_run_flag(dry_run)
    metrics = DailyRunMetrics()

    click.echo("=== Outbound Agent -- Daily Check ===\n")
    click.echo(f"Mode: {mode.value}\n")
    click.echo(f"Safety limits:\n{get_status()}\n")

    # PR-228 preflight: refuse a wet run from a checkout that is missing
    # merged work — BEFORE the run lock, any PB launch, or any CRM write.
    # Dry runs and --allow-stale warn instead. Mirrors weekly.
    code_provenance = assert_checkout_current(
        dry_run=dry_run, allow_stale=allow_stale
    )

    # PR-11 pre-flight (§0 invariant #9): abort on multi-running ambiguity
    # or an unreadable registry BEFORE the run lock + CRM client open.
    _experiment_registry_preflight("daily")

    # operator_today() reads OUTBOUND_TZ (default America/Lima)
    # so a UTC cron clock doesn't shift the quarantine + send-day calendar.
    today = operator_today()
    # Policy: invites run every day; DMs are Mon-Fri only (override with --force-weekend).
    dms_allowed = is_send_day(today) or force_weekend
    lock_name = "sales-daily"
    run_id = f"{lock_name}-{today.isoformat()}-{os.getpid()}"

    # Lock acquired BEFORE the AuditLogger opens so a refused run doesn't
    # pollute the audit log with an aborted-start row.
    # try/finally guarantees `metrics.render()` fires even when an
    # unhandled exception escapes the run — the operator's only signal
    # for partial-state runs lives in those counters.
    try:
        with acquire_run_lock(lock_name, run_id=run_id), \
                AuditLogger(workflow="daily_check", dry_run=dry_run) as audit_logger, \
                record_run(audit_logger, code_provenance), \
                _crm_provider() as crm, PhantomBusterClient() as pb:
            if not dry_run:
                pb.reconcile_workspace()
            # PR-17 B-SD-001: open the daily_run row before any send-path
            # work. F-PR-8's ``(run_date, machine_id)`` uniqueness raises
            # ConcurrentRunInAttio on collision — surface as typed
            # ``daily_run_collision`` operator escalation and exit
            # EX_TEMPFAIL so a same-day second invocation backs off
            # without overwriting state. Dry-run uses ``_DryRunDailyRun``
            # stub so the trim path still has a ``remaining()`` source
            # without burning the uniqueness slot.
            # open_daily_run now talks to the CRM through the vendor-neutral
            # provider (its uniqueness-collision guard catches the provider's
            # UniquenessConflictError, not a raw httpx error), so it takes the
            # provider directly — no inner-client conversion here anymore.
            daily_run_cm = (
                contextlib.nullcontext(_DryRunDailyRun())
                if mode.is_dry_run()
                else open_daily_run(crm, run_id=run_id, run_date=today)
            )

            phase0_backend = load_pb_config().degree_check_backend_raw.strip()
            phase0_required_id = sales_nav_profile_scraper_id if phase0_backend == "sales_nav" else profile_scraper_id
            try:
                with daily_run_cm as daily_run, AcceptanceCheckpoint(
                    operator_id="single", day=today, provenance=code_provenance,
                    backend=phase0_backend,
                    scraper_id=phase0_required_id,
                ) as acceptance_checkpoint:
                    # Bulk-preload person records once so all four phases share a warm cache.
                    # Skips ~593 redundant get_person calls per run vs. the per-phase cache pattern.
                    list_id = os.environ.get("ATTIO_LIST_ID", "")
                    _t_scan = phase_timer()
                    entries = crm.query_list_entries(list_id=list_id)
                    record_phase_or_skip(metrics, "list_scan_preload_ids", _t_scan)
                    all_record_ids = {e.record_id for e in entries}
                    all_record_ids.discard(None)
                    all_record_ids.discard("")
                    click.echo(f"Preloading {len(all_record_ids)} pipeline person records...")
                    cache = RecordCache(crm)
                    preloaded = preload_pipeline_persons(
                        crm, cache, all_record_ids, metrics=metrics,
                    )
                    click.echo(f"  Preloaded {preloaded} records.\n")

                    # Pre-flight: Attio schema-drift gate (wet path only).
                    # Before ANY send, verify the schema manifest is valid and
                    # every shipped/verified attribute actually exists in
                    # production Attio. This is the gate that would have caught
                    # the 2026-05-28 partial-send halt, where merged code wrote
                    # experiment_id_frozen_at while the attribute was still
                    # undeployed. Runs before Phase 0 so a schema gap aborts
                    # the run before any PB launch or Attio mutation.
                    if not mode.is_dry_run():
                        click.echo("--- Pre-flight: Attio schema-drift check ---")
                        from scripts.validate_attio_schema_deltas import (
                            DEFAULT_MANIFEST,
                            check_attio_shipped,
                            load_manifest,
                            validate,
                        )
                        _manifest = load_manifest(DEFAULT_MANIFEST)
                        _schema_errors = validate(_manifest) + check_attio_shipped(
                            _manifest, statuses=frozenset({"shipped", "verified"})
                        )
                        if _schema_errors:
                            for _e in _schema_errors:
                                click.echo(f"  ✗ {_e}", err=True)
                            raise click.ClickException(
                                f"Attio schema-drift pre-flight FAILED "
                                f"({len(_schema_errors)} error(s) above). Refusing "
                                f"to send. Deploy/repair the schema (e.g. `python "
                                f"scripts/migrate_attio_linkedin_outreach_schema.py "
                                f"--apply`) and re-run /sales-daily in a fresh shell."
                            )
                        click.echo(
                            "  ✓ manifest valid + all shipped/verified attrs live\n"
                        )

                    # Crash-recovery scan: reconcile any DMs that PB sent but
                    # Attio never recorded (process-killed-mid-run scenario).
                    # Runs before Phase 0 so prior-run unrecorded sends are
                    # reconciled before any new sends fire.  Best-effort: a
                    # PB API hiccup must not block the daily run.
                    click.echo("--- Pre-run: DM send crash recovery ---")
                    if message_sender_id:
                        # Recovery now routes its advance through the §3.15
                        # AttioWriter path, which RE-RAISES the programmer-bug
                        # class (unauthorized-writer / monotonicity /
                        # terminal-class) precisely so the run HALTS. Those must
                        # NOT be downgraded to a best-effort warning: a recovery
                        # that mis-advances (or fails to advance) leaves the DM
                        # sequencer free to re-send — the §3.1 duplicate-DM red
                        # line. Let them propagate; only infra hiccups (PB fetch
                        # / CSV download) are best-effort below.
                        from clients.attio_writer import (
                            AttioMonotonicityViolation,
                            AttioTerminalClassRegression,
                            UnauthorizedAttioWriteError,
                        )
                        from workflows.pb_send_recovery import (
                            recover_unrecorded_dm_sends,
                        )
                        try:
                            # recover_unrecorded_dm_sends parses entries via the
                            # raw ``AttioClient.parse_entry`` classmethod and
                            # advances through ``_attio_advance_with_escalation``
                            # — Attio-coupled, daily-only, unmigrated. Convert at
                            # the boundary (escape hatch) to keep it byte-identical.
                            recovery_summary = recover_unrecorded_dm_sends(
                                _attio_inner_client(crm),
                                pb,
                                message_sender_id=message_sender_id,
                                list_id=list_id,
                                today=today.isoformat(),
                                dry_run=dry_run,
                            )
                        except (
                            UnauthorizedAttioWriteError,
                            AttioMonotonicityViolation,
                            AttioTerminalClassRegression,
                        ):
                            raise
                        except Exception as _rec_exc:  # noqa: BLE001
                            # Best-effort: a PB API / CSV-download hiccup must
                            # not block the daily run.
                            click.echo(
                                f"  [pb_send_recovery] WARNING: recovery scan failed "
                                f"({type(_rec_exc).__name__}: {_rec_exc}); "
                                f"continuing daily run.",
                                err=True,
                            )
                        else:
                            recovered = recovery_summary.get("recovered", 0)
                            write_failed = recovery_summary.get(
                                "skipped_write_failed", 0
                            )
                            if recovered:
                                click.echo(
                                    f"  Recovered {recovered} unrecorded DM "
                                    f"send(s) from last run."
                                )
                            if write_failed:
                                # Loud: these entries are stranded at their prior
                                # stage and WILL be re-DM'd next run unless the
                                # operator reconciles the attio_write_failed rows.
                                click.echo(
                                    f"  ⚠ {write_failed} unrecorded send(s) FAILED "
                                    f"to record — reconcile attio_write_failed "
                                    f"queue rows before the next run.",
                                    err=True,
                                )
                            if not recovered and not write_failed:
                                click.echo("  No unrecorded sends to recover.")
                            click.echo("")
                    else:
                        click.echo("  Skipping (no PB_MESSAGE_SENDER_ID set)\n")

                    # Phase 0: Live-check CONNECTION_SENT profiles for acceptances.
                    # Gate: when the SN backend is selected, require the SN
                    # scraper id; otherwise the legacy id. The two scrapers
                    # have different argument shapes and different CSV column
                    # contracts, and the workspace currently only has the SN
                    # phantom deployed (the legacy phantom was deleted as part
                    # of the same migration that introduced PRE_INVITE_DEGREE_CHECK_BACKEND).
                    click.echo("--- Phase 0: Detect Accepted Connections ---")
                    phase0_backend = load_pb_config().degree_check_backend_raw.strip()
                    if phase0_backend not in _VALID_BACKENDS:
                        # Validate the VALUE at the routing gate. A typo'd
                        # backend would otherwise route Phase 0 to the legacy
                        # id; with that id unset, Phase 0 (and the SN health
                        # gate, which keys on the same comparison) silently
                        # skips — and on a day with an empty invite pool
                        # nothing downstream calls the validating resolver,
                        # so the run exits 0 with acceptance detection off.
                        raise click.ClickException(
                            f"PRE_INVITE_DEGREE_CHECK_BACKEND="
                            f"{phase0_backend!r} is not a valid backend "
                            f"({' | '.join(_VALID_BACKENDS)}) — fix .env; "
                            "refusing to guess which scraper to launch."
                        )
                    phase0_required_id = (
                        sales_nav_profile_scraper_id
                        if phase0_backend == "sales_nav"
                        else profile_scraper_id
                    )
                    phase0_missing_env = (
                        "PB_SALES_NAV_PROFILE_SCRAPER_ID"
                        if phase0_backend == "sales_nav"
                        else "PB_PROFILE_SCRAPER_ID"
                    )
                    if mode.is_dry_run():
                        click.echo("Skipping (dry run)\n")
                        metrics.pb_launches_skipped_dry_run += 1
                    elif phase0_required_id:
                        if acceptance_checkpoint.resume_result is None:
                            metrics.pb_launches_attempted += 1
                        accept_result = acceptance_checkpoint.run(lambda: detect_accepted_connections(
                            _attio_inner_client(crm), pb, profile_scraper_id, cache=cache,
                            sales_nav_profile_scraper_id=sales_nav_profile_scraper_id,
                        ))
                        _phase0_deferred = accept_result.get("deferred", 0)
                        _phase0_deferred_note = (
                            f" ({_phase0_deferred} stale profile(s) beyond the "
                            f"per-run scrape cap, deferred to subsequent runs)"
                            if _phase0_deferred
                            else ""
                        )
                        click.echo(
                            f"Auto-detected {accept_result.get('accepted', 0)} "
                            f"accepted connections.{_phase0_deferred_note}\n"
                        )
                    else:
                        click.echo(f"Skipping (no {phase0_missing_env} set for backend={phase0_backend})\n")

                    # An identity hold blocks every DM, including rehearsal,
                    # while independent invitation checks may still proceed.
                    reply_identity_hold: IdentityResolutionHalt | None = None
                    # Phase 0.5: Detect message responses.
                    click.echo("--- Phase 0.5: Detect Message Responses ---")
                    if mode.is_dry_run():
                        click.echo("Skipping (dry run)\n")
                        metrics.pb_launches_skipped_dry_run += 1
                    elif inbox_scraper_id:
                        metrics.pb_launches_attempted += 1
                        # PR-18 B-SD-012 wiring: ResendClient enables the
                        # hot-lead email alert channel. Queue-row
                        # durability holds either way (resend=None still
                        # opens the operator queue row); the email is
                        # the convenience nudge that fires only when
                        # ``RESEND_API_KEY`` is set.
                        with _maybe_resend_client() as resend_client:
                            try:
                                # detect_responses is §7-shared (imported by
                                # hot_lead_alert / escalation_schemas) and stays
                                # on the raw AttioClient; convert at the boundary.
                                resp_result = detect_responses(
                                    _attio_inner_client(crm), pb, inbox_scraper_id,
                                    cache=cache, resend=resend_client,
                                    daily_run=daily_run if isinstance(daily_run, DailyRun) else None,
                                )
                            except IdentityResolutionHalt as exc:
                                reply_identity_hold = exc
                                resp_result = {"detected": 0}
                                click.echo(
                                    f"  ⚠ Reply identity check held: {exc}. "
                                    "Continuing invitation checks; all DMs remain held.",
                                    err=True,
                                )
                                metrics.warn("reply_identity_hold: DMs held; invites continue")
                            except NoCSVHalt as exc:
                                # PR-19 B-SD-005: typed halt with exit
                                # code 2 (operator-visible non-success;
                                # distinct from EX_TEMPFAIL=75 for lock
                                # contention). The pb_csv_empty queue
                                # row + reply_detection_status='failed'
                                # were already written by detect_responses
                                # before it raised.
                                click.echo(
                                    f"  ⚠ Phase 0.5 halted: {exc}",
                                    err=True,
                                )
                                metrics.warn(
                                    f"pb_csv_empty halt: container={exc.container_id}"
                                )
                                raise SystemExit(2) from exc
                        click.echo(f"Detected {resp_result.get('detected', 0)} new responses.\n")
                    else:
                        click.echo("Skipping (no PB_INBOX_SCRAPER_ID set)\n")

                    # Phase 0.6: Detect email responses (Gmail read-only,
                    # PR-243). Email counterpart of Phase 0.5 — without it,
                    # an emailed rejection never reaches the CRM and the
                    # sequencer keeps emailing the prospect. OFF by default:
                    # no Gmail token => skip (Gmail is an optional data
                    # source, not part of the CRM contract).
                    click.echo("--- Phase 0.6: Detect Email Responses ---")
                    if mode.is_dry_run():
                        click.echo("Skipping (dry run)\n")
                    elif os.environ.get("OUTBOUND_DISABLE_EMAIL_RESPONSE_DETECTION"):
                        click.echo(
                            "Skipping (OUTBOUND_DISABLE_EMAIL_RESPONSE_DETECTION set)\n"
                        )
                    else:
                        try:
                            gmail_client = GmailClient.from_credentials()
                        except GmailCredentialsMissing:
                            gmail_client = None
                        if gmail_client is None:
                            click.echo("Skipping (no Gmail credentials)\n")
                        else:
                            # detect_email_responses writes via AttioWriter +
                            # direct update_person; stays on the raw
                            # AttioClient, converted at the boundary like
                            # Phase 0.5.
                            email_resp = detect_email_responses(
                                _attio_inner_client(crm), gmail_client,
                            )
                            click.echo(
                                f"Detected {email_resp.get('detected', 0)} "
                                f"new email responses."
                            )
                            # Failure/skip rollup (mirrors Phase 0.5's
                            # end-of-phase visibility): a swallowed write or a
                            # run of auto-reply skips must not be invisible in
                            # the run log.
                            for _key, _label in (
                                ("gmail_errors", "Gmail error(s) — affected prospects retry next run"),
                                ("attio_update_failures", "CRM write failure(s) — see stderr / attio_write_failed queue"),
                                ("auto_generated_skipped", "auto-reply/bounce message(s) skipped"),
                                ("no_start_timestamp", "prospect(s) unscannable — no email_campaign_started/last_sent timestamp"),
                            ):
                                _n = email_resp.get(_key, 0)
                                if _n:
                                    click.echo(f"  ⚠ {_n} {_label}", err=_key not in ("auto_generated_skipped", "no_start_timestamp"))
                            click.echo("")

                    # Phase 0.7: Botdog event ingestion — OPTIONAL drain
                    # only. PhantomBuster owns sending; this phase exists
                    # so an operator with rows stamped
                    # send_channel=botdog can absorb their confirming
                    # accept / DM-advance / reply events. Gated on
                    # BOTDOG_SEND_ENABLED so an ordinary PB run never
                    # needs BOTDOG_API_KEY. Dry-run polls (read-only) +
                    # reports what WOULD change, writes nothing.
                    click.echo("--- Phase 0.7: Botdog Event Ingestion ---")
                    #
                    # BROAD except by design: this is a read-and-reconcile
                    # phase — it sends nothing. A Botdog API outage, an
                    # expired key, or a schema change here must NEVER take
                    # down the PhantomBuster sends that run after it. The
                    # consequence of skipping ingestion is a one-run delay
                    # in event-confirmed advances, which the next run's
                    # poll cursor recovers.
                    if not botdog_send_enabled:
                        click.echo("Skipping (BOTDOG_SEND_ENABLED off)\n")
                    else:
                        try:
                            from workflows.botdog_ingest import (
                                format_report,
                                ingest_botdog_events,
                            )
                            botdog_report = ingest_botdog_events(
                                _attio_inner_client(crm),
                                dry_run=mode.is_dry_run(),
                                audit_logger=audit_logger,
                            )
                            click.echo(format_report(botdog_report) + "\n")
                            if botdog_report.get("failures"):
                                metrics.warn(
                                    f"botdog_ingest failures="
                                    f"{botdog_report['failures']} — "
                                    f"attio_write_failed queue row(s) opened"
                                )
                        except Exception as exc:  # noqa: BLE001 — see comment above
                            click.echo(
                                f"  ⚠ Phase 0.7 (Botdog event ingestion) FAILED "
                                f"[{type(exc).__name__}: {exc}] — "
                                f"event-confirmed advances for "
                                f"botdog-stamped rows are delayed to the "
                                f"next run (the poll cursor is not advanced "
                                f"on failure, so the next run re-polls "
                                f"them). PhantomBuster sends are "
                                f"unaffected.\n",
                                err=True,
                            )
                            metrics.warn(
                                f"botdog_ingest_phase_failed="
                                f"{type(exc).__name__}"
                            )

                    # Phase 0.9: pain-signal discovery — OFF BY DEFAULT.
                    #
                    # One-daily-command policy: the lane's 24h client-side
                    # recency window pairs with the daily cadence, so it runs
                    # here, inside the daily lock (which is what serializes it
                    # against the degree check's shared autoconnect sheet).
                    # Every gate travels with the lane; when any gate is
                    # closed this phase is ONE status line, never a halt, and
                    # the daily run continues regardless. The lane only
                    # COMMITS Prospect-stage rows behind the standard
                    # quarantine — it sends nothing, and the invites those
                    # rows earn still go through this run's per-batch review
                    # on a later day.
                    #
                    # BROAD except by design, same as Phase 0.7: an
                    # optional-by-construction discovery lane must never take
                    # down the sends that run after it. Skipping it costs one
                    # day of discovery supply, nothing else.
                    click.echo("--- Phase 0.9: Pain-Signal Discovery ---")
                    from workflows.pain_signal import (
                        PAIN_SIGNAL_ENABLED_ENV,
                        is_pain_signal_enabled,
                    )
                    if not is_pain_signal_enabled():
                        click.echo(
                            f"Skipping ({PAIN_SIGNAL_ENABLED_ENV} unset — "
                            f"the lane is off by default)\n"
                        )
                    else:
                        pain_posts_worker = load_pb_config().pain_posts_worker_id
                        pain_sn_scraper = (
                            load_pb_config().sales_nav_profile_scraper_id
                        )
                        if _pb_agent_unconfigured(pain_posts_worker):
                            click.echo(
                                f"  ⚠ Skipping: {PAIN_SIGNAL_ENABLED_ENV} is "
                                f"on but the posts worker is not configured "
                                f"(agents.pain_posts_worker / "
                                f"PB_PAIN_POSTS_WORKER_ID). No posts, no "
                                f"lane.\n",
                                err=True,
                            )
                        else:
                            try:
                                from workflows.pain_signal import (
                                    run_pain_signal_discovery,
                                )
                                run_pain_signal_discovery(
                                    crm, pb,
                                    pain_posts_worker,
                                    load_pb_config().pain_commenters_worker_id,
                                    load_pb_config().pain_likers_worker_id,
                                    pain_sn_scraper,
                                    dry_run=mode.is_dry_run(),
                                )
                                click.echo("")
                            except Exception as exc:  # noqa: BLE001 — see above
                                click.echo(
                                    f"  ⚠ Pain-signal discovery SKIPPED "
                                    f"[{type(exc).__name__}: {exc}] — no "
                                    f"prospects were sourced from this lane "
                                    f"today. Invites and DMs are "
                                    f"unaffected.\n",
                                    err=True,
                                )
                                metrics.warn(
                                    f"pain_signal_phase_failed="
                                    f"{type(exc).__name__}"
                                )

                    # Pipeline-starvation check (PR-43).
                    click.echo("--- Pipeline starvation check ---")
                    if mode.is_dry_run():
                        click.echo("Skipping (dry run)\n")
                    else:
                        try:
                            # evaluate_pipeline_starvation reads entries through
                            # the shared raw _get_all_entries_parsed path, so it
                            # takes the inner AttioClient (escape hatch).
                            starvation_summary = evaluate_pipeline_starvation(
                                _attio_inner_client(crm), today
                            )
                            fired = starvation_summary.get("triggers_fired") or []
                            metrics.starvation_triggers_fired += len(fired)
                            if fired:
                                click.echo(
                                    f"⚠ Starvation triggers fired: {', '.join(fired)} "
                                    f"(invite_eligible_pool="
                                    f"{starvation_summary.get('invite_eligible_pool')}). "
                                    f"Queue row(s) opened.\n"
                                )
                            else:
                                click.echo(
                                    f"OK (pool={starvation_summary.get('invite_eligible_pool')}, "
                                    f"runway="
                                    f"{starvation_summary.get('runway_bdays_remaining')} bdays)\n"
                                )
                        except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                            # Wave-1.6 FIX-3a: narrow catch + escalate + re-raise.
                            # The pre-Wave-1.6 broad `except Exception` swallowed
                            # this same path with a click.echo warning, defeating
                            # `evaluate_pipeline_starvation`'s documented `raise`
                            # contract (workflows/starvation.py:16) and the §0
                            # invariant #9 (no silent fallbacks). Only transient
                            # Attio failures (timeout / 5xx) escalate-and-re-raise;
                            # programmer bugs (KeyError, TypeError) bubble up
                            # unfiltered as they did before.
                            transient = (
                                isinstance(exc, httpx.TimeoutException)
                                or (
                                    isinstance(exc, httpx.HTTPStatusError)
                                    and exc.response.status_code in {502, 503, 504}
                                )
                            )
                            if not transient:
                                raise
                            escalate(
                                type="pipeline_starvation_check_failed",
                                idempotency_key=(
                                    f"starvation-check-failed|{today.isoformat()}"
                                ),
                                payload={
                                    "today": today.isoformat(),
                                    "error_class": type(exc).__name__,
                                    "error_msg": str(exc),
                                    "context": "evaluator_failed_to_run",
                                },
                                attio=crm,
                            )
                            raise

                    # PR-B.7: Sales Nav health pre-flight (only when
                    # backend=sales_nav). Checks env, runs a 1-profile
                    # Sales Nav scrape + 1-thread inbox scrape to confirm
                    # cookies coexist before the operator approves any
                    # invite batch. Closes the GTM-lens cookie-rotation
                    # gap — see plan reflective-singing-waterfall.md PR-B.7.
                    if load_pb_config().degree_check_backend_raw.strip() == "sales_nav":
                        if mode.is_dry_run():
                            # Wave-1.6-ext FIX-1' (adversarial SB-1):
                            # the health pre-flight invokes
                            # validate_sales_nav_health.quick_check()
                            # which launches REAL PB Sales Nav Profile
                            # Scraper + Inbox Scraper containers. Same
                            # dry-run discipline as FIX-1 in
                            # pre_invite_check.py — under --dry-run no
                            # live PB calls fire, period. Operators who
                            # want to validate Sales Nav cookies invoke
                            # `python scripts/validate_sales_nav_health.py`
                            # directly. See done-qa-adversarial round-2
                            # finding F1 (cli.py:348-367 was bypassing
                            # FIX-1 whenever backend=sales_nav).
                            click.echo(
                                "--- Pre-flight: Sales Nav health check "
                                "(SKIPPED in --dry-run) ---"
                            )
                            metrics.pb_launches_skipped_dry_run += 2
                        else:
                            click.echo("--- Pre-flight: Sales Nav health check ---")
                            from scripts.validate_sales_nav_health import quick_check
                            rc, summary = quick_check()
                            click.echo(summary)
                            if rc == 1:
                                # FAIL: stop the daily run. Operator must rotate
                                # cookies. (The old advice to flip
                                # PRE_INVITE_DEGREE_CHECK_BACKEND=regular is dead:
                                # the legacy Profile Scraper agent was deleted from
                                # the PB workspace — there is no backend to roll
                                # back to.) Per
                                # docs/runbooks/phantombuster-cookie-rotation.md.
                                raise click.ClickException(
                                    "Sales Nav pre-flight FAILED — fix the named "
                                    "issue above (typically rotate cookies via "
                                    "docs/runbooks/phantombuster-cookie-rotation.md), "
                                    "then re-run /sales-daily in a fresh shell."
                                )
                            # rc=2 (WARN) prints the warning and continues —
                            # caller still gets to approve the batch interactively.

                    # Part A: Connection requests.
                    click.echo("--- Part A: Connection Requests ---")
                    # Pre-send template-guard halt (UnresolvedPlaceholderError /
                    # BlankMessageError from run_connection_requests /
                    # run_dm_sequencing): remember the halt, keep emitting the
                    # run summary + rollup + Phase C for the parts that DID
                    # run, then re-raise at the end of the run body so the
                    # daily_run row closes as failed and the matching handler
                    # below applies the curated ⚠ REFUSE exit.
                    conn_result: dict
                    dm_result: dict
                    guard_halt: Exception | None = None
                    if network_booster_id:

                        try:
                            def _run_one_invite_container(
                                remaining_target: int,
                                remaining_us: int | None,
                                attempted_entry_ids: set[str],
                                attempted_company_ids: set[str],
                                attempted_urls: set[str],
                            ) -> dict:
                                return run_connection_requests(
                                    _attio_inner_client(crm), pb, network_booster_id,
                                    batch_size=remaining_target, dry_run=dry_run,
                                    auto_confirm=yes, cache=cache,
                                    profile_scraper_id=profile_scraper_id,
                                    sales_nav_profile_scraper_id=sales_nav_profile_scraper_id,
                                    audit_logger=audit_logger, today=today,
                                    daily_run=daily_run,
                                    exclude_entry_ids=attempted_entry_ids,
                                    exclude_company_ids=attempted_company_ids,
                                    exclude_linkedin_urls=attempted_urls,
                                )

                            conn_result = drain_connection_invites(
                                _run_one_invite_container,
                                batch_size=batch_size,
                                daily_run=daily_run,  # type: ignore[arg-type]
                                dry_run=dry_run,
                            )
                        except (BlankMessageError, UnresolvedPlaceholderError) as exc:
                            # Guard fires BEFORE any invite ships — nothing
                            # went out for Part A. (The invite path today
                            # raises only UnresolvedPlaceholderError — it has
                            # no blank-copy guard; BlankMessageError is caught
                            # for parity with Part B so adding that guard
                            # later needs no CLI change.)
                            guard_halt = exc
                            conn_result = dict(getattr(exc, "partial_results", None) or {"sent": 0})
                            conn_result["reason"] = "template_guard_halt"
                            click.echo(f"  ⚠ REFUSE: {exc}", err=True)
                            metrics.warn(
                                f"template_guard_halt part=A "
                                f"{type(exc).__name__}"
                            )

                    else:
                        click.echo("Skipping connections (no PB_NETWORK_BOOSTER_ID set)")
                        conn_result = {"sent": 0, "reason": "no_agent_id"}

                    # Part B: DM sequencing (Mon-Fri only)
                    click.echo("\n--- Part B: DM Sequencing ---")
                    if reply_identity_hold is not None:
                        click.echo("Skipping DMs and rehearsal (reply identity check held).")
                        dm_result = {"dm1": 0, "dm2": 0, "dm3": 0, "reason": "reply_identity_hold"}
                    elif guard_halt is not None:
                        # A systemic template break halted Part A — don't
                        # keep sending on the other lane of the same
                        # content pipeline.
                        click.echo(
                            "Skipping DMs (Part A halted on a pre-send "
                            "template guard — fix the template, then re-run)."
                        )
                        dm_result = {"dm1": 0, "dm2": 0, "dm3": 0, "reason": "template_guard_halt"}
                    elif not dms_allowed:
                        click.echo(f"Weekend ({today}) — skipping DM sends. Use --force-weekend to override.")
                        dm_result = {"dm1": 0, "dm2": 0, "dm3": 0, "reason": "weekend"}
                    elif skip_dms:
                        click.echo("Skipping DMs (--skip-dms)")
                        dm_result = {"dm1": 0, "dm2": 0, "dm3": 0, "reason": "skip_dms"}
                        if preview_dms_after_invites and message_sender_id:
                            click.echo("DM rehearsal using this run's record cache (no DM sends):")
                            run_dm_sequencing(
                                _attio_inner_client(crm), pb, message_sender_id,
                                dry_run=True, auto_confirm=False, cache=cache,
                                audit_logger=audit_logger,
                                daily_run=daily_run, metrics=metrics,
                                preview_no_writes=True,
                            )
                        elif preview_dms_after_invites:
                            click.echo("DM rehearsal unavailable (no PB_MESSAGE_SENDER_ID set)")
                    elif message_sender_id:
                        # _DryRunDailyRun structurally satisfies DailyRun
                        # for run_dm_sequencing's needs but isn't a
                        # subclass — accept the type mismatch.

                        try:
                            dm_result = run_dm_sequencing(
                                _attio_inner_client(crm), pb, message_sender_id,
                                dry_run=dry_run, auto_confirm=yes, cache=cache,
                                audit_logger=audit_logger,
                                daily_run=daily_run,
                                metrics=metrics,
                                # Same freeze-and-drain flag as Part A: the
                                # DM channel split honors it, so a stamped
                                # row never sends while the flag is off.
                            )
                        except (BlankMessageError, UnresolvedPlaceholderError) as exc:
                            # Guard fires BEFORE the halted step's preview /
                            # lease / transport (nothing sent for it), but
                            # Part A invites are already out and EARLIER DM
                            # steps may have shipped (the guard runs per
                            # step). run_dm_sequencing attaches its realized
                            # counts to the exception so the rollup below
                            # reports what actually went out — never a
                            # fabricated zero.
                            guard_halt = exc
                            _partial = getattr(exc, "partial_results", None)
                            dm_result = {
                                **(_partial or {"dm1": 0, "dm2": 0, "dm3": 0}),
                                "reason": "template_guard_halt",
                            }
                            click.echo(f"  ⚠ REFUSE: {exc}", err=True)
                            metrics.warn(
                                f"template_guard_halt part=B "
                                f"{type(exc).__name__}"
                            )

                    else:
                        click.echo("Skipping DMs (no PB_MESSAGE_SENDER_ID set)")
                        dm_result = {"dm1": 0, "dm2": 0, "dm3": 0, "reason": "no_agent_id"}

                    # PR-17 B-SD-011: write the run-end summary onto the
                    # daily_run row before context exit. Skipped on
                    # dry-run because daily_run is a ``_DryRunDailyRun``
                    # stub with no real Attio record_id.
                    if not mode.is_dry_run() and isinstance(daily_run, DailyRun):
                        try:
                            due_counts = compute_due_dm_counts(
                                crm, cache=cache, today=today
                            )
                            run_summary = run_end_summary(
                                crm, daily_run,
                                prospect_pool_size=due_counts["prospect_pool_size"],
                                due_dm1_count=due_counts["due_dm1_count"],
                                due_dm2_count=due_counts["due_dm2_count"],
                                due_dm3_count=due_counts["due_dm3_count"],
                                today=today,
                            )
                            click.echo(
                                f"\n  Run-end summary: pool={run_summary['prospect_pool_size']}, "
                                f"due_dm1={run_summary['due_dm1_count']}, "
                                f"due_dm2={run_summary['due_dm2_count']}, "
                                f"due_dm3={run_summary['due_dm3_count']}, "
                                f"degree_unknown={run_summary['degree_unknown_count']}, "
                                f"signal={run_summary['starvation_signal']}."
                            )
                            # Read-only cohort visibility: how many rows landed in
                            # DM1_SENT per send-date over the last business week, so
                            # a genuine daily cohort is legible against same-day
                            # re-prospected duplicates inflating the stage total.
                            dm1_cohort = compute_dm1_sent_cohort_by_date(
                                _attio_inner_client(crm), today=today
                            )
                            if dm1_cohort:
                                breakdown = ", ".join(
                                    f"{day}={count}" for day, count in dm1_cohort
                                )
                                click.echo(
                                    f"  DM1 Sent cohort by send-date (last 5 business "
                                    f"days): {breakdown}."
                                )
                            else:
                                click.echo(
                                    "  DM1 Sent cohort by send-date (last 5 business "
                                    "days): none."
                                )
                        except Exception as _sum_exc:  # noqa: BLE001 — guard-halt shield, see below
                            # On a template or identity halt this is best-effort:
                            # letting a transient Attio failure here propagate
                            # would DISCARD guard_halt — the run would exit with
                            # a raw traceback blaming the blip instead of the
                            # broken template. Without a halt, the pre-existing
                            # contract holds: summary failures propagate.
                            if guard_halt is None and reply_identity_hold is None:
                                raise
                            click.echo(
                                f"  ⚠ Run-end summary failed after a send "
                                f"hold ({type(_sum_exc).__name__}: "
                                f"{_sum_exc}) — continuing to the curated "
                                f"refusal.",
                                err=True,
                            )

                    click.echo(
                        "\n=== Daily Check FAILED ===" if guard_halt is not None or reply_identity_hold is not None or dm_result.get("failed_batches")
                        else "\n=== Daily Check Complete ==="
                    )
                    click.echo(f"Connections sent: {conn_result.get('sent', 0)}")
                    total_dms = dm_result.get("dm1", 0) + dm_result.get("dm2", 0) + dm_result.get("dm3", 0)
                    click.echo(f"DMs sent: {total_dms}")
                    # The only botdog counters are hold-outs: rows stamped
                    # send_channel=botdog that no transport touches until
                    # they are re-stamped send_channel=pb.
                    _bd_dm_skipped = sum(
                        int(v or 0)
                        for v in (dm_result.get("botdog_channel_skipped") or {}).values()
                    )
                    if _bd_dm_skipped:
                        click.echo(
                            f"  ⚠ {_bd_dm_skipped} botdog-stamped DM(s) "
                            f"SKIPPED — PhantomBuster owns sending; "
                            f"re-stamp these rows send_channel=pb (no "
                            f"sends from any transport until then)",
                            err=True,
                        )
                    _bd_excluded = int(conn_result.get("botdog_excluded", 0) or 0)
                    if _bd_excluded:
                        click.echo(
                            f"  ⚠ {_bd_excluded} botdog-stamped prospect(s) "
                            f"excluded from PB invites (awaiting a "
                            f"send_channel=pb re-stamp)",
                            err=True,
                        )
                    _bd_census = int(conn_result.get("botdog_stamped_total", 0) or 0)
                    if _bd_census:
                        click.echo(
                            f"  ⚠ Botdog residual census: {_bd_census} "
                            f"row(s) stamped send_channel=botdog across "
                            f"ALL stages — no sends, no scrape detection "
                            f"for them until they are re-stamped "
                            f"send_channel=pb (see the Part A warning "
                            f"above)",
                            err=True,
                        )
                    click.echo(f"Code: {format_provenance(code_provenance)}")

                    # Phase C: warm follow-up radar (read-only detection, PR-211).
                    # STRICTLY downstream of Phase A/B and fully isolated: any
                    # failure here degrades to a WARN and never fails invites/DMs
                    # or the daily run's exit code. Fails-closed-clean on a
                    # schemaless install (the engine's schema probes degrade to a
                    # digest, never crash), so the radar is inert until the
                    # operator provisions its attributes (--feature radar). The
                    # skill layer turns this digest into drafts — the CLI only
                    # surfaces it.
                    if not skip_followups:
                        click.echo("\n--- Phase C: Follow-up Radar ---")
                        try:
                            from workflows.followup_radar import run_followup_radar
                            fu = run_followup_radar(crm, today=today)
                            from workflows.gmail_inventory import save_inventory
                            radar_path = audit_logger.path.with_suffix(".radar.json")
                            save_inventory(radar_path, {
                                "schema_version": 1, "run_id": run_id,
                                "code": code_provenance, "as_of": datetime.now(UTC).isoformat(),
                                "radar": fu,
                            })
                            click.echo(f"Radar evidence: {radar_path}")
                            click.echo(fu["digest"])
                            click.echo(
                                f"  ({fu['surfaced']} surfaced"
                                f"{_followup_lane_counts(fu)})"
                            )
                        except Exception as exc:  # noqa: BLE001 — never break the daily run
                            import traceback
                            # Swallow the control flow (Phase C must not fail
                            # A/B) but NOT the diagnostics — a bare type+message
                            # is undebuggable when the failure is 3 calls deep.
                            click.echo(
                                f"  ⚠ Phase C (follow-up radar) errored and was "
                                f"skipped: {type(exc).__name__}: {exc}\n"
                                f"{traceback.format_exc()}",
                                err=True,
                            )


                    # A pre-send template guard halted Part A or Part B: the
                    # summary/rollup/Phase C above already covered the parts
                    # that ran. Re-raise the ORIGINAL exception (not a
                    # SystemExit) so the daily_run row closes as "failed" —
                    # same terminal status as before the guards got curated
                    # handling — before the matching handler below converts
                    # it into the ⚠ REFUSE + EX_TEMPFAIL exit.
                    if guard_halt is not None:
                        raise guard_halt

                    if reply_identity_hold is not None:
                        raise reply_identity_hold

                    _raise_failed_dm_batches(dm_result)

            except MalformedDailyRunRow as exc:
                # The pre-open same-day scan (multi-row incident guard)
                # fails closed on a prior row with a corrupt counter.
                # EXIT_TEMPFAIL keeps launchd's retry/backoff semantics;
                # the WARN + this message are the operator's signal to fix
                # or archive the bad row in the CRM.
                click.echo(
                    f"  ⚠ REFUSE: {exc} Fix or archive the bad daily_run "
                    f"row in your CRM, then re-run. Exiting EX_TEMPFAIL (75).",
                    err=True,
                )
                raise SystemExit(EXIT_TEMPFAIL) from exc
            except ConcurrentRunInAttio as exc:
                # PR-17 fold-in (engineer-QA IMPORTANT): use exc.run_date,
                # not today.isoformat(). The exception carries the exact
                # string the Attio uniqueness_key was opened with.
                collision_key = f"{exc.run_date}|{exc.machine_id}"
                # PR-17 fold-in (silent-failure-hunter IMPORTANT): if
                # escalate() itself fails, still exit 75 so launchd
                # retries instead of seeing a raw traceback.
                try:
                    escalate(
                        type="daily_run_collision",
                        idempotency_key=collision_key,
                        payload={
                            "run_date": exc.run_date,
                            "machine_id": exc.machine_id,
                            "attempted_run_id": run_id,
                            "existing": exc.existing or {},
                        },
                        attio=crm,
                    )
                except Exception as escalate_exc:
                    click.echo(
                        f"  ⚠ daily_run_collision escalation FAILED to "
                        f"open queue row: "
                        f"{type(escalate_exc).__name__}: {escalate_exc}. "
                        f"Original collision: {collision_key}. Exiting "
                        f"EX_TEMPFAIL anyway so launchd retries.",
                        err=True,
                    )
                click.echo(
                    f"  ⚠ daily_run uniqueness collision on "
                    f"{collision_key} — another machine is already "
                    f"running. Opening daily_run_collision queue row "
                    f"and exiting EX_TEMPFAIL (75).",
                    err=True,
                )
                raise SystemExit(EXIT_TEMPFAIL) from exc
            except (BlankMessageError, UnresolvedPlaceholderError) as exc:
                # Pre-send template guard (re-raised above after the partial
                # summary/rollup). A broken template is operator-fixable —
                # curated refusal instead of a raw traceback. The full guard
                # message repeats here on purpose: the tail of stderr is what
                # a truncated launchd log or a skimming operator actually reads.
                # This is deterministic, so a retry could send another Part A
                # invite batch before reaching the same broken DM template.
                click.echo(
                    f"  ⚠ REFUSE: {exc} Fix the template, then re-run. "
                    f"Exiting EX_CONFIG ({EXIT_TEMPLATE_REFUSE}).",
                    err=True,
                )
                raise SystemExit(EXIT_TEMPLATE_REFUSE) from exc
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc
    finally:
        # Survives any unhandled exception escaping the try-block above.
        click.echo("", err=True)
        click.echo(metrics.render(), err=True)


@cli.command(name="send-dms")
@click.option("--dry-run", is_flag=True, help="Resolve + print the DM list; send nothing")
@click.option("--yes", "-y", is_flag=True, help="Send the resolved DMs (re-runs response detection first)")
@click.option("--batch-size", default=25, help="Reserved for cap math parity with daily")
@click.option("--message-sender-id", envvar="PB_MESSAGE_SENDER_ID", help="PhantomBuster Message Sender agent ID")
@click.option("--inbox-scraper-id", envvar="PB_INBOX_SCRAPER_ID", help="PhantomBuster Inbox Scraper agent ID (response re-detect)")
@click.option("--force-weekend", is_flag=True, help="Override the Mon-Fri-only DM rule")
@click.option(
    "--allow-stale", is_flag=True,
    help="Proceed with a wet run even when the checkout is behind origin/main "
         "(the staleness is still stamped into the run's provenance).",
)
@click.option("--exclude", "exclude_ids", multiple=True, help="entry_id or record_id to skip this run (repeatable)")
def send_dms(dry_run, yes, batch_size, message_sender_id, inbox_scraper_id, force_weekend, allow_stale, exclude_ids):
    """Send-DMs phase: reattach to today's daily_run row and send Part B only.

    Runs ONLY after `daily --yes --skip-dms` opened the day. `--dry-run`
    resolves + prints the DM list against committed CRM state (sends nothing);
    `--yes` re-runs response detection then sends. The reattach
    (workflows.daily_run.attach_daily_run) binds the EXISTING daily_run row so
    the per-day message cap survives across the invite and DM phases. A bare
    `send-dms` with neither flag is an interactive confirm (run_dm_sequencing
    prompts before sending).
    """
    from clients.phantombuster import PhantomBusterClient
    from models.business_calendar import is_send_day, operator_today
    from models.run_mode import RunMode
    from workflows.audit import AuditLogger
    from workflows.daily_check import (
        BlankMessageError,
        UnresolvedPlaceholderError,
        run_dm_sequencing,
    )
    from workflows.daily_run import (
        MalformedDailyRunRow,
        NoDailyRunRow,
        ReopenCollision,
    )
    from workflows.detect_responses import NoCSVHalt, detect_responses
    from workflows.metrics import (
        DailyRunMetrics,
        phase_timer,
        record_phase_or_skip,
    )
    from workflows.record_cache import RecordCache, preload_pipeline_persons
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )
    from workflows.run_provenance import (
        assert_checkout_current,
        format_provenance,
    )
    from workflows.weekly_prospect import _attio_inner_client

    mode = RunMode.from_dry_run_flag(dry_run)
    metrics = DailyRunMetrics()

    click.echo("=== Outbound Agent -- Send DMs ===\n")
    click.echo(f"Mode: {mode.value}\n")

    # PR-228 preflight: refuse a wet run from a checkout that is missing
    # merged work — BEFORE the run lock, any PB launch, or any CRM write.
    # Dry runs and --allow-stale warn instead. Mirrors weekly.
    code_provenance = assert_checkout_current(
        dry_run=dry_run, allow_stale=allow_stale
    )

    # Experiment pre-flight (parity with daily): ambiguity or an unreadable
    # registry is a hard abort BEFORE any send.
    _experiment_registry_preflight("send-dms")

    today = operator_today()
    lock_name = "sales-daily"  # SAME lock as daily — serializes step 1 / step 2
    run_id = f"send-dms-{today.isoformat()}-{os.getpid()}"

    try:
        with acquire_run_lock(lock_name, run_id=run_id), \
                AuditLogger(workflow="send_dms", dry_run=dry_run) as audit_logger, \
                _crm_provider() as crm, PhantomBusterClient() as pb:
            if not dry_run:
                pb.reconcile_workspace()
            # Schema-drift pre-flight (wet path only) — parity with daily.
            if not mode.is_dry_run():
                click.echo("--- Pre-flight: Attio schema-drift check ---")
                from scripts.validate_attio_schema_deltas import (
                    DEFAULT_MANIFEST,
                    check_attio_shipped,
                    load_manifest,
                    validate,
                )
                _manifest = load_manifest(DEFAULT_MANIFEST)
                _schema_errors = validate(_manifest) + check_attio_shipped(
                    _manifest, statuses=frozenset({"shipped", "verified"})
                )
                if _schema_errors:
                    for _e in _schema_errors:
                        click.echo(f"  ✗ {_e}", err=True)
                    raise click.ClickException(
                        f"Attio schema-drift pre-flight FAILED "
                        f"({len(_schema_errors)} error(s) above). Refusing to send."
                    )
                click.echo("  ✓ manifest valid + all shipped/verified attrs live\n")

            # Reattach to today's existing daily_run row (never open a new one).
            # --dry-run attaches read-only: a preview must never reopen a
            # terminal row and re-close it "completed", silently rewriting a
            # prior "failed" status.
            try:
                with attach_daily_run(
                    crm, run_id=run_id, run_date=today,
                    read_only=mode.is_dry_run(),
                ) as daily_run:
                    # --- EARLY GUARD: staleness ---
                    if daily_run.run_date != today.isoformat():
                        click.echo(
                            f"  ⚠ REFUSE: bound daily_run row is for "
                            f"{daily_run.run_date}, not today ({today.isoformat()}). "
                            f"Run `daily --skip-dms` today first.",
                            err=True,
                        )
                        raise SystemExit(EXIT_TEMPFAIL)

                    # --- EARLY GUARD: weekend (invites-only days refuse DMs) ---
                    if not is_send_day(today) and not force_weekend:
                        click.echo(
                            f"  ⚠ REFUSE: {today} is a weekend — DMs are invites-"
                            f"only policy. Use --force-weekend to override.",
                            err=True,
                        )
                        raise SystemExit(EXIT_TEMPFAIL)

                    # --- EARLY GUARD: reply-status fail-closed ---
                    # `!= "ok"` (covers None/missing/failed) — NOT `== "failed"`.
                    status = daily_run.get_reply_detection_status()
                    if status != "ok":
                        click.echo(
                            f"  ⚠ REFUSE: reply_detection_status={status!r} (not "
                            f"'ok'). Today's inbox scrape did not complete cleanly; "
                            f"do not send. Re-run `daily --skip-dms` to re-detect.",
                            err=True,
                        )
                        raise SystemExit(EXIT_TEMPFAIL)

                    list_id = os.environ.get("ATTIO_LIST_ID", "")
                    _t_scan = phase_timer()
                    entries = crm.query_list_entries(list_id=list_id)
                    record_phase_or_skip(metrics, "list_scan_preload_ids", _t_scan)
                    all_record_ids = {e.record_id for e in entries}
                    all_record_ids.discard(None)
                    all_record_ids.discard("")
                    cache = RecordCache(crm)
                    preload_pipeline_persons(
                        crm, cache, all_record_ids, metrics=metrics
                    )

                    # --- Re-detect responses BEFORE sending (close review-gap) ---
                    # WET --yes only: --dry-run must NEVER trigger a live PB inbox
                    # scrape (parity with daily, which gates detection on
                    # `not mode.is_dry_run()`).
                    if yes and not mode.is_dry_run() and inbox_scraper_id:
                        click.echo("--- Re-detect: response detection (close review-gap race) ---")
                        with _maybe_resend_client() as resend_client:
                            try:
                                detect_responses(
                                    _attio_inner_client(crm), pb, inbox_scraper_id,
                                    cache=cache, resend=resend_client,
                                    daily_run=daily_run,
                                )
                            except NoCSVHalt as exc:
                                # A failed re-scrape aborts BEFORE any send.
                                click.echo(
                                    f"  ⚠ Response re-detect halted: {exc}", err=True
                                )
                                raise SystemExit(2) from exc
                        # --- RE-CHECK the guard on the POST-re-detect value:
                        # detect_responses overwrote reply_detection_status; the
                        # stale step-1 value is gone.
                        status = daily_run.get_reply_detection_status()
                        if status != "ok":
                            click.echo(
                                f"  ⚠ REFUSE: post-re-detect reply_detection_status="
                                f"{status!r} (not 'ok'). Aborting before any DM.",
                                err=True,
                            )
                            raise SystemExit(EXIT_TEMPFAIL)
                    elif yes and not mode.is_dry_run() and not inbox_scraper_id:
                        # WET send with no inbox-scraper-id: the review-gap race-
                        # closer can't run. Sending on the step-1 reply status alone
                        # is still guarded by the early reply-status check, but the
                        # operator must SEE that the race was not re-closed — never
                        # skip this silently.
                        click.echo(
                            "  ⚠ --yes set but no inbox-scraper-id — skipping "
                            "response re-detect; sending on step-1 reply status only "
                            "(review-gap race NOT re-closed).",
                            err=True,
                        )

                    if not message_sender_id:
                        click.echo("Skipping DMs (no PB_MESSAGE_SENDER_ID set)")
                        return

                    click.echo("\n--- Part B: DM Sequencing ---")
                    dm_result = run_dm_sequencing(
                        _attio_inner_client(crm), pb, message_sender_id,
                        dry_run=dry_run, auto_confirm=yes, cache=cache,
                        audit_logger=audit_logger,
                        daily_run=daily_run,
                        exclude_ids=set(exclude_ids),
                        metrics=metrics,
                    )
                    total_dms = sum(
                        int(dm_result.get(step, 0) or 0)
                        for step in ("dm1", "dm2", "dm3")
                    )
                    click.echo(f"\nDMs sent: {total_dms}")
                    click.echo(f"\nCode: {format_provenance(code_provenance)}")
                    _raise_failed_dm_batches(dm_result)
            except NoDailyRunRow as exc:
                click.echo(f"  ⚠ {exc} Exiting EX_TEMPFAIL.", err=True)
                raise SystemExit(EXIT_TEMPFAIL) from exc
            except ReopenCollision as exc:
                click.echo(f"  ⚠ {exc} Exiting EX_TEMPFAIL.", err=True)
                raise SystemExit(EXIT_TEMPFAIL) from exc
            except MalformedDailyRunRow as exc:
                # Fail-closed guard: a fetched row with a missing/unparseable
                # counter is untrustworthy — defaulting it to 0 would silently
                # reset the message cap and permit a 60-DM breach. Refuse to send
                # (same EX_TEMPFAIL treatment as the other reattach failures).
                click.echo(
                    f"  ⚠ {exc} Today's daily_run row is untrustworthy — refusing "
                    f"to send. Exiting EX_TEMPFAIL.",
                    err=True,
                )
                raise SystemExit(EXIT_TEMPFAIL) from exc
            except (BlankMessageError, UnresolvedPlaceholderError) as exc:
                # Pre-send template guard from run_dm_sequencing: nothing was
                # previewed or sent for the HALTED step (earlier steps in the
                # same call may have shipped — their advances are already in
                # Attio). A broken template is operator-fixable — curated
                # refusal instead of a raw traceback. This deterministic
                # content defect requires an operator fix, not an auto retry.
                metrics.warn(
                    f"template_guard_halt {type(exc).__name__}"
                )
                click.echo(
                    f"  ⚠ REFUSE: {exc} Fix the template, then re-run. "
                    f"Exiting EX_CONFIG ({EXIT_TEMPLATE_REFUSE}).",
                    err=True,
                )
                raise SystemExit(EXIT_TEMPLATE_REFUSE) from exc
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc
    finally:
        click.echo("", err=True)
        click.echo(metrics.render(), err=True)


@cli.command()
@click.option("--dry-run", is_flag=True, help="Score prospects without writing to Attio")
@click.option("--batch-size", default=lambda: load_outreach_config().weekly_scrape_batch_size, help="Max prospects to export per search (default: config/outreach.yaml → scrape.weekly_batch_size). The Search Export phantom re-exports the same top-of-search window each run; a deeper batch reaches past the recycled results.")
@click.option("--search-export-id", default=lambda: load_pb_config().search_export_id or None, help="PhantomBuster Search Export agent ID (default: config/phantombuster.yaml → PB_SEARCH_EXPORT_ID)")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts (for cron)")
@click.option(
    "--allow-stale", is_flag=True,
    help="Proceed with a wet run even when the checkout is behind origin/main "
         "(the staleness is still stamped into the run's provenance).",
)
def weekly(dry_run, batch_size, search_export_id, yes, allow_stale):
    """Weekly prospecting: export, qualify, and load new prospects."""
    from clients.phantombuster import PhantomBusterClient
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )
    from workflows.run_provenance import assert_checkout_current
    from workflows.weekly_prospect import run_weekly_prospecting

    click.echo("=== Outbound Agent -- Weekly Prospecting ===\n")

    # PR-228 preflight: refuse a wet run from a checkout that is missing
    # merged work — BEFORE the run lock, any PB launch, or any CRM write.
    # Dry runs and --allow-stale warn instead. The collected provenance is
    # stamped into the summary + staged JSONL either way.
    code_provenance = assert_checkout_current(
        dry_run=dry_run, allow_stale=allow_stale
    )

    # PR-21 pre-flight (mirrors daily's §0 invariant #9 guard): abort on
    # multi-running ambiguity or an unreadable registry BEFORE any prospects
    # commit, so we never get partial-batch state with mixed cohort tags.
    _experiment_registry_preflight("weekly")

    if not search_export_id:
        click.echo("Error: PB_SEARCH_EXPORT_ID not set. Set it in .env or pass --search-export-id.")
        raise SystemExit(1)

    lock_name = "sales-weekly"
    run_id = f"{lock_name}-{date.today().isoformat()}-{os.getpid()}"
    try:
        with acquire_run_lock(lock_name, run_id=run_id), \
                _crm_provider() as crm, PhantomBusterClient() as pb:
            if not dry_run:
                pb.reconcile_workspace()
            run_weekly_prospecting(
                crm, pb, search_export_id,
                batch_size=batch_size, dry_run=dry_run,
                code_provenance=code_provenance,
            )
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc


@cli.command("pain-signal")
@click.option(
    "--dry-run", is_flag=True,
    help="Launch the discovery + enrichment scrapes (PhantomBuster spend — "
         "previews need real posts and real engagers with real titles) but "
         "write NOTHING to the CRM; echo candidate + invite-note previews.",
)
@click.option(
    "--yes", "-y", is_flag=True, help="Skip the wet-run confirmation prompt"
)
@click.option(
    "--posts-worker-id",
    default=lambda: load_pb_config().pain_posts_worker_id or "",
    help="PhantomBuster posts-worker agent ID — the post-extractor phantom "
         "inside the engager-scraper workflow (takes a content-search URL, "
         "exports matching posts). Default: config/phantombuster.yaml "
         "agents.pain_posts_worker -> PB_PAIN_POSTS_WORKER_ID.",
)
@click.option(
    "--commenters-worker-id",
    default=lambda: load_pb_config().pain_commenters_worker_id or "",
    help="PhantomBuster commenters-worker agent ID (one launch per post "
         "URL). OPTIONAL: unset skips commenter harvesting loudly. Default: "
         "config/phantombuster.yaml agents.pain_commenters_worker -> "
         "PB_PAIN_COMMENTERS_WORKER_ID.",
)
@click.option(
    "--likers-worker-id",
    default=lambda: load_pb_config().pain_likers_worker_id or "",
    help="PhantomBuster likers-worker agent ID (one launch per post URL). "
         "OPTIONAL: unset skips liker harvesting loudly. Default: "
         "config/phantombuster.yaml agents.pain_likers_worker -> "
         "PB_PAIN_LIKERS_WORKER_ID.",
)
@click.option(
    "--sales-nav-profile-scraper-id",
    default=lambda: load_pb_config().sales_nav_profile_scraper_id or "",
    help="PhantomBuster Sales Navigator Profile Scraper agent ID (the same "
         "phantom the daily degree check uses) — enriches candidates with "
         "title/company/location before ICP scoring.",
)
def pain_signal(
    dry_run, yes, posts_worker_id, commenters_worker_id, likers_worker_id,
    sales_nav_profile_scraper_id,
):
    """Pain-signal discovery: post keyword search -> posts -> authors +
    commenters + likers -> enrich -> qualify.

    OFF BY DEFAULT. Gated behind OUTBOUND_PAIN_SIGNAL_ENABLED=1 and an
    operator-approved content/pain_keywords.json (the shipped registry is a
    placeholder and is refused). Recency is client-side on postTimestamp
    (LinkedIn's datePosted search filter returns zero results). Commits
    prospects at Prospect stage only — invites go out later via the daily
    run, behind its per-batch review. See GETTING_STARTED.md.
    """
    from clients.phantombuster import PhantomBusterClient
    from workflows.pain_signal import (
        PAIN_SIGNAL_ENABLED_ENV,
        is_pain_signal_enabled,
        run_pain_signal_discovery,
    )
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )

    click.echo("=== Outbound Agent -- Pain-Signal Discovery ===\n")

    if not is_pain_signal_enabled():
        click.echo(
            f"ABORT: the pain-signal lane is disabled. Set "
            f"{PAIN_SIGNAL_ENABLED_ENV}=1 in .env to enable it (default off "
            "by design — see GETTING_STARTED.md).",
            err=True,
        )
        raise SystemExit(1)

    if _pb_agent_unconfigured(posts_worker_id):
        click.echo(
            "ABORT: the pain-signal posts worker is not configured. The lane "
            "drives the workflow's POSTS worker directly — the workflow "
            "parent's API launches are no-ops, so never use the parent's id. "
            "Set agents.pain_posts_worker in config/phantombuster.yaml (or "
            "PB_PAIN_POSTS_WORKER_ID in .env).",
            err=True,
        )
        raise SystemExit(1)
    if _pb_agent_unconfigured(sales_nav_profile_scraper_id):
        click.echo(
            "ABORT: PB_SALES_NAV_PROFILE_SCRAPER_ID not configured. The pain "
            "lane enriches candidates through the Sales Nav Profile Scraper "
            "(the daily degree-check phantom) before scoring — candidates "
            "carry no company and ICP scoring runs on title.",
            err=True,
        )
        raise SystemExit(1)
    # Placeholder-shaped OPTIONAL worker ids are treated as unset (the lane
    # skips that engager type loudly) rather than aborting.
    if _pb_agent_unconfigured(commenters_worker_id):
        commenters_worker_id = ""
    if _pb_agent_unconfigured(likers_worker_id):
        likers_worker_id = ""

    # The pain lane stamps its OWN experiment_id, but the commit path still
    # resolves the global one first — so the shared registry pre-flight
    # applies here too.
    _experiment_registry_preflight("pain-signal")

    if not dry_run and not yes:
        click.confirm(
            "WET run: qualified candidates will be committed to the CRM at "
            "Prospect stage (no sends). Continue?",
            abort=True,
        )

    # Shares the weekly lock: both are prospect-commit paths whose dedup
    # snapshots are per-process — running them concurrently could
    # double-commit the same person. KNOWN HAZARD: the enrichment scrape's
    # multi-URL wet path writes the SHARED production autoconnect sheet that
    # the daily run's degree check also clears and rewrites, and daily holds
    # a DIFFERENT lock ("sales-daily") — nothing serializes the two. Failure
    # directions are degrade-safe (a clobbered sheet yields an empty scrape,
    # a dropped batch, a loud enrichment degrade), but do not run pain-signal
    # concurrently with a daily run.
    lock_name = "sales-weekly"
    run_id = f"pain-signal-{date.today().isoformat()}-{os.getpid()}"
    try:
        with acquire_run_lock(lock_name, run_id=run_id), \
                _crm_provider() as crm, PhantomBusterClient() as pb:
            if not dry_run:
                pb.reconcile_workspace()
            run_pain_signal_discovery(
                crm, pb, posts_worker_id, commenters_worker_id,
                likers_worker_id, sales_nav_profile_scraper_id,
                dry_run=dry_run,
            )
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc


@cli.command()
@click.option(
    "--send", is_flag=True,
    help="Actually send the report email (default: dry-run; sidecar Attio row is always written).",
)
@click.option("--email", envvar="REPORT_EMAIL", default="ops@example.com", help="Report recipient")
def report(send, email):
    """Weekly KPI report: compute metrics, write JSON sidecar, optionally email.

    PR-30: defaults to DRY-RUN per §0 #9 (no silent side-effects).
    Operators must pass --send explicitly to dispatch the Resend email.
    The Weekly KPI Snapshot JSON sidecar is written to
    reports/weekly-kpi/<week_starting>.json on every run so subagents
    read historical reports by `week_starting`.
    """
    from clients.crm.factory import get_crm_provider
    from clients.resend_client import ResendClient
    from workflows.weekly_report import run_weekly_report

    dry_run = not send
    click.echo("=== Outbound Agent -- Weekly Report ===\n")
    if dry_run:
        click.echo(
            "[DRY-RUN MODE] Sidecar row will be written; email will NOT "
            "be sent. Pass --send to dispatch the email.",
        )

    with get_crm_provider() as bundle:
        assert bundle.attio is not None, "Attio adapter required for weekly report"
        resend = None if dry_run else ResendClient()
        try:
            run_weekly_report(
                bundle.attio,
                resend,
                report_email=email,
                dry_run=dry_run,
                crm=bundle.provider,
            )
        finally:
            if resend:
                resend.close()


@cli.command("check-responses")
def check_responses():
    """Manual response check: input which prospects responded."""
    from workflows.daily_check import check_responses_manual

    click.echo("=== Outbound Agent -- Check Responses ===\n")

    with _attio_client() as attio:
        check_responses_manual(attio)


@cli.command()
def limits():
    """Show current daily safety limits."""
    from workflows.safety_limits import get_status

    click.echo("=== Daily Safety Limits ===\n")
    click.echo(get_status())


@cli.command()
def canary():
    """§3.20 Attio scope canary — verify the REST write+delete path is live.

    Every skill's Step-0 preflight runs this AFTER an MCP read-liveness check
    (whoami + list-lists). It does a create-note → delete-note round-trip
    through the REST client, exercising the SAME ATTIO_API_KEY credential the
    daily run mutates prospect data with. The round-trip targets a dedicated,
    inert canary record (CANARY_PERSON_RECORD_ID) — never a real prospect.

    Exit 0 = full read+write+delete scope confirmed. Non-zero + a typed
    `mcp_scope_insufficient` line on stderr = halt the skill before any
    real Attio write.
    """
    from clients.attio import AttioClient
    from clients.attio_writer_registry import CANARY_PERSON_RECORD_ID

    click.echo("=== Attio scope canary (REST write+delete round-trip) ===\n")

    record_id = CANARY_PERSON_RECORD_ID
    if not record_id:
        click.echo(
            "mcp_scope_insufficient: canary_record_unconfigured — "
            "CANARY_PERSON_RECORD_ID is empty in clients/attio_writer_registry.py. "
            "Create an inert Person record (not in the linkedin_outreach list) "
            "and pin its id before running any skill.",
            err=True,
        )
        raise SystemExit(1)

    # Missing credential is itself a scope problem — emit the typed line
    # rather than letting AttioClient() raise a bare KeyError the skill
    # can't recognize.
    if not os.environ.get("ATTIO_API_KEY"):
        click.echo(
            "mcp_scope_insufficient: attio_api_key_unset — ATTIO_API_KEY is not "
            "in the environment (.env); cannot run the scope canary.",
            err=True,
        )
        raise SystemExit(1)

    note_id = None
    with AttioClient() as attio:
        # ---- write leg ----
        click.echo(
            f"Running round-trip on canary record {record_id} "
            "(create → delete; may take up to ~35s per leg if Attio is degraded)…"
        )
        try:
            note = attio.create_note(
                record_id,
                "Outbound agent scope canary",
                "Transient read+write+delete scope check. Auto-deleted; safe to ignore.",
                parent_object="people",
            )
            # Guard `note` itself (not note.get("id")) — this is the most
            # safety-critical line: a non-dict return must fall to None and
            # fail closed, never raise past the contract.
            note_id = note.get("id", {}).get("note_id") if isinstance(note, dict) else None
        except Exception as exc:  # noqa: BLE001 — fail closed with a compatible halt token
            from workflows.canary_diagnostics import canary_failure_detail
            click.echo(
                f"mcp_scope_insufficient: write leg failed on canary record "
                f"{record_id} — {canary_failure_detail(exc)} "
                f"({type(exc).__name__}: {exc})",
                err=True,
            )
            raise SystemExit(1) from exc

        if not note_id:
            click.echo(
                "mcp_scope_insufficient: write leg returned no note_id "
                f"(canary record {record_id}) — cannot verify delete scope.",
                err=True,
            )
            raise SystemExit(1)

        # ---- delete leg ----
        try:
            deleted = attio.delete_note(note_id)
        except Exception as exc:  # noqa: BLE001 — delete scope is not live
            from workflows.canary_diagnostics import canary_failure_detail
            click.echo(
                f"mcp_scope_insufficient: delete leg failed — note {note_id} on "
                f"canary record {record_id} was created; deletion could not be confirmed "
                f"({type(exc).__name__}: {exc}). {canary_failure_detail(exc)} "
                f"Possible orphan note; verify whether it remains, then "
                f"delete it manually (DELETE /v2/notes/{note_id}).",
                err=True,
            )
            raise SystemExit(1) from exc

        if not deleted:
            click.echo(
                f"mcp_scope_insufficient: delete leg reported note {note_id} as "
                f"not present (404/absent) — the note we just created could not "
                f"be deleted; scope unverifiable.",
                err=True,
            )
            raise SystemExit(1)

    click.echo(
        f"OK — read+write+delete confirmed. Round-trip note {note_id} "
        f"created and deleted on canary record {record_id}."
    )


@cli.command()
def pipeline():
    """Show current pipeline status from Attio."""
    from clients.attio import AttioClient
    from models.pipeline import PipelineStage

    click.echo("=== Outbound Agent Pipeline ===\n")

    with _attio_client() as attio:
        list_id = os.environ.get("ATTIO_LIST_ID", "")
        entries = attio.query_list_entries(list_id=list_id)

        counts: dict[str, int] = {}
        for entry in entries:
            stage = AttioClient.parse_entry(entry)["stage"] or "Unknown"
            counts[stage] = counts.get(stage, 0) + 1

        total = sum(counts.values())
        click.echo(f"Total entries: {total}\n")
        for stage_enum in PipelineStage:
            count = counts.get(stage_enum.value, 0)
            bar = "#" * count
            click.echo(f"  {stage_enum.value:20s} {count:3d} {bar}")


@cli.command("backfill-export")
def backfill_export_cmd():
    """Export pipeline records missing company links for PB enrichment."""
    from workflows.backfill_companies import backfill_export

    click.echo("=== Outbound Agent -- Backfill Export ===\n")

    with _crm_provider() as crm:
        file_path = backfill_export(crm)

    click.echo("\nDone. Run this CSV through PhantomBuster LinkedIn Profile Scraper,")
    click.echo(f"then import with: python3 cli.py backfill-import <pb_output.csv> --export-csv {file_path}")


@cli.command("backfill-import")
@click.argument("pb_csv", type=click.Path(exists=True))
@click.option("--export-csv", required=True, type=click.Path(exists=True), help="Original export CSV from backfill-export")
def backfill_import_cmd(pb_csv, export_csv):
    """Import PB-enriched data and link companies in Attio."""
    from workflows.backfill_companies import backfill_import

    click.echo("=== Outbound Agent -- Backfill Import ===\n")

    with _attio_client() as attio:
        summary = backfill_import(attio, pb_csv, export_csv)

    click.echo(f"\nDone. {summary['linked']} records linked to companies.")


@cli.command("backfill-industries")
@click.option("--dry-run", is_flag=True, help="Classify without writing to Attio")
@click.option("--limit", type=int, default=None, help="Max companies to classify in this run")
def backfill_industries_cmd(dry_run: bool, limit: int | None):
    """Classify companies missing industry_vertical via Haiku."""
    from workflows.industry_classifier import (
        backfill_missing_industries,
        build_anthropic_client,
    )

    click.echo("=== Outbound Agent -- Backfill Industries ===\n")

    # Per §0 invariant #11, build_anthropic_client returns None and the engine
    # does NOT hold ANTHROPIC_API_KEY. The classifier routes to the LLM
    # dispatch path when OUTBOUND_USE_LLM_DISPATCH=1 (set by the parent
    # skill). Without dispatch enabled, classify_industry returns None
    # for every company, which the loop reports as `api_errors`.
    anthropic_client = build_anthropic_client()
    if not os.environ.get("OUTBOUND_USE_LLM_DISPATCH"):
        click.echo(
            "WARNING: OUTBOUND_USE_LLM_DISPATCH is not set. Industry classifier "
            "will return None for all companies (no LLM access). Invoke via the "
            "parent slash-command skill (which exports the env var) for real "
            "classification, or pass a mock client from a test."
        )

    with _attio_client() as attio:
        summary = backfill_missing_industries(
            attio,
            anthropic_client=anthropic_client,
            limit=limit,
            dry_run=dry_run,
        )

    click.echo("")
    click.echo(f"Total scanned: {summary['total_scanned']}")
    click.echo(f"Missing industry: {summary['missing']}")
    click.echo(f"Classified:      {summary['classified']}")
    click.echo(f"Written:         {summary['written']}" + (" (dry run)" if dry_run else ""))
    click.echo(f"Skipped:         {summary['skipped']}")
    click.echo(f"API errors:      {summary['api_errors']}")


@cli.command("email-import")
@click.argument("csv_path", type=click.Path(exists=True), default="exports/tier1_email_campaign_2026-04-07.csv")
@click.option("--dry-run", is_flag=True, help="Preview imports without writing to Attio")
def email_import_cmd(csv_path, dry_run):
    """Import email campaign contacts from CSV into Attio."""
    from workflows.email_campaign import import_contacts

    click.echo("=== Outbound Agent -- Email Import ===\n")

    with _attio_client() as attio:
        import_contacts(attio, csv_path, dry_run=dry_run)


@cli.command("email-daily")
@click.option("--dry-run", is_flag=True, help="Preview sends without emailing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts (for cron)")
@click.option("--force-weekend", is_flag=True, help="Override the Mon-Fri-only outreach rule")
def email_daily_cmd(dry_run, yes, force_weekend):
    """Daily email drip: send sequenced emails to campaign contacts."""
    from clients.resend_client import ResendClient
    from workflows.email_campaign import run_email_daily
    from workflows.email_lane_gate import EmailLaneDisabledError, assert_email_lane_enabled
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )

    click.echo("=== Outbound Agent -- Email Daily ===\n")

    # Email-lane kill switch: the drip is disarmed unless armed on purpose.
    # Checked here (before the lock) so a disarmed run costs nothing and prints
    # one clear line; run_email_daily re-checks for programmatic callers.
    try:
        assert_email_lane_enabled("email-daily", dry_run=dry_run)
    except EmailLaneDisabledError as exc:
        click.echo(f"ABORT: {exc}", err=True)
        raise SystemExit(1) from exc

    # email-wave2 shares this namespace — both write contact email state
    # and must mutually exclude. See cli.py::email_wave2_cmd.
    lock_name = "sales-email-daily"
    run_id = f"{lock_name}-{date.today().isoformat()}-{os.getpid()}"
    try:
        with acquire_run_lock(lock_name, run_id=run_id), _attio_client() as attio:
            resend = None if dry_run else ResendClient()
            try:
                run_email_daily(
                    attio, resend, dry_run=dry_run, auto_confirm=yes, force_weekend=force_weekend
                )
            finally:
                if resend:
                    resend.close()
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc


@cli.command("email-unsubscribe")
@click.argument("email")
def email_unsubscribe_cmd(email):
    """Honor an opt-out: mark ALL contacts with this email UNSUBSCRIBED so the
    email campaign never sends to them again."""
    from workflows.email_compliance import UNSUBSCRIBE_LOOKUP_LIMIT, unsubscribe_email

    click.echo("=== Outbound Agent -- Email Unsubscribe ===\n")
    with _attio_client() as attio:
        updated, maybe_more = unsubscribe_email(attio, email)
    if updated:
        click.echo(
            f"  Unsubscribed {email}: {len(updated)} record(s) marked "
            f"UNSUBSCRIBED ({', '.join(updated)}). The email campaign will not "
            f"send to them again."
        )
        if maybe_more:
            click.echo(
                f"  ⚠ The lookup hit the {UNSUBSCRIBE_LOOKUP_LIMIT}-record cap — "
                f"more duplicate records for {email} may exist. Dedupe the "
                f"workspace and re-run to be certain all are covered.",
                err=True,
            )
    else:
        click.echo(f"  No contact found with email {email}. Nothing to do.")


@cli.command("email-wave2")
@click.option("--dry-run", is_flag=True, help="Preview sends without emailing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts (for cron)")
@click.option("--force-weekend", is_flag=True, help="Override the Mon-Fri-only outreach rule")
@click.option("--max", "max_n", type=int, default=100, show_default=True, help="Max contacts to send in one run")
def email_wave2_cmd(dry_run, yes, force_weekend, max_n):
    """Wave-2 re-engage blast: send a fresh email to contacts stalled mid-sequence."""
    from clients.resend_client import ResendClient
    from workflows.email_lane_gate import EmailLaneDisabledError, assert_email_lane_enabled
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
        log_lock_refused,
    )
    from workflows.wave2_blast import run_wave2_blast

    click.echo("=== Outbound Agent -- Wave 2 Blast ===\n")

    # Email-lane kill switch: the drip is disarmed unless armed on purpose.
    # Checked here (before the lock) so a disarmed run costs nothing and prints
    # one clear line; run_wave2_blast re-checks for programmatic callers.
    try:
        assert_email_lane_enabled("email-wave2", dry_run=dry_run)
    except EmailLaneDisabledError as exc:
        click.echo(f"ABORT: {exc}", err=True)
        raise SystemExit(1) from exc

    # Shares the sales-email-daily namespace with `email-daily` — both
    # write contact email state and must mutually exclude. Concurrent
    # runs were a real §3.1-adjacent risk (two waves of email on the
    # same contact in the same minute).
    lock_name = "sales-email-daily"
    run_id = f"sales-email-wave2-{date.today().isoformat()}-{os.getpid()}"
    try:
        with acquire_run_lock(lock_name, run_id=run_id), _attio_client() as attio:
            resend = None if dry_run else ResendClient()
            try:
                run_wave2_blast(
                    attio,
                    resend,
                    dry_run=dry_run,
                    auto_confirm=yes,
                    force_weekend=force_weekend,
                    max_n=max_n,
                )
            finally:
                if resend:
                    resend.close()
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc


@cli.command("email-association")
@click.option("--dry-run", is_flag=True, help="Preview sends without emailing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts (for cron)")
@click.option("--force-weekend", is_flag=True, help="Override the Mon-Fri-only outreach rule")
def email_association_cmd(dry_run, yes, force_weekend):
    """Send one-shot association outreach emails (e.g., AFAMO partnership ask). Idempotent."""
    from clients.resend_client import ResendClient
    from workflows.association_outreach import run_association_outreach
    from workflows.audit import AuditLogger
    from workflows.run_lock import EXIT_TEMPFAIL, RunLockHeld, acquire_run_lock, log_lock_refused

    click.echo("=== Outbound Agent -- Association Outreach ===\n")

    resend = None if dry_run else ResendClient()
    lock_name = "sales-email-daily"
    run_id = f"{lock_name}-{date.today().isoformat()}-{os.getpid()}"
    try:
        with acquire_run_lock(lock_name, run_id=run_id), \
                AuditLogger(workflow="association_outreach", dry_run=dry_run) as audit_logger, \
                _attio_client() as attio:
            run_association_outreach(
                resend, dry_run=dry_run, auto_confirm=yes, force_weekend=force_weekend,
                attio=attio, audit_logger=audit_logger,
            )
    except RunLockHeld as exc:
        log_lock_refused(lock_name, exc)
        raise SystemExit(EXIT_TEMPFAIL) from exc
    finally:
        if resend:
            resend.close()


@cli.command()
@click.option("--dry-run", is_flag=True, help="Measure and report without updating experiment statuses")
def learn(dry_run):
    """Measure experiment cohorts and evaluate results."""
    from workflows.learn import apply_verdict, evaluate_experiments, measure_cohorts, to_experiment_verdict

    click.echo("=== Outbound Agent -- Experiment Learning ===\n")

    # L4-3: acquire a run lock so concurrent invocations of `learn` serialize
    # rather than racing over experiment TSV writes and operator_review_queue
    # openings. Uses the same EXIT_TEMPFAIL (75) pattern as daily/weekly.
    from workflows.run_lock import (
        EXIT_TEMPFAIL,
        RunLockHeld,
        acquire_run_lock,
    )

    lock_name = "sales-learn"
    run_id = f"{lock_name}-{date.today().isoformat()}-{os.getpid()}"

    try:
        with acquire_run_lock(lock_name, run_id=run_id), _attio_client() as attio:
            # 1. Measure all cohorts
            click.echo("--- Cohort Measurements ---")
            cohorts = measure_cohorts(attio)
            for c in cohorts:
                maturity = "MATURE" if c["is_mature"] else f"immature ({c['days_running']}d)"
                click.echo(
                    f"  {c['experiment_id']:30s}  "
                    f"n={c['cohort_size']:3d}  "
                    f"DM'd={c['dmed']:3d}  "
                    f"resp={c['responded']:2d}  "
                    f"rate={c['dm_response_rate']:.1%}  "
                    f"[{maturity}]"
                )
                for n in (1, 2, 3):
                    received = c.get(f"dm{n}_received", 0)
                    replies = c.get(f"dm{n}_replies", 0)
                    if received == 0:
                        click.echo(f"      DM{n}: n/a")
                    else:
                        rate = c.get(f"dm{n}_response_rate", 0.0)
                        click.echo(f"      DM{n}: {replies}/{received} = {rate:.1%}")

            # 2. Evaluate running experiments
            click.echo("\n--- Experiment Verdicts ---")
            verdicts = evaluate_experiments(cohorts)
            if not verdicts:
                click.echo("  No running experiments with mature cohorts to evaluate.")
            for v in verdicts:
                line = (
                    f"  {v['experiment_id']:30s}  "
                    f"verdict={v['verdict']:12s}  "
                    f"rate={v['step_rate']:.1%} vs baseline={v['step_baseline_rate']:.1%}"
                )
                if v.get("step", "overall") != "overall":
                    line += f" (driven by {v['rate_metric']})"
                click.echo(line)

            # 3. Update experiment statuses (if not dry-run).
            # PR-31 / Wave-2-A: persist all 4 terminal verdicts (WON / LOST /
            # REJECTED_NULL / REJECTED_DEFENSIVE) to experiments.tsv (system of
            # record) via apply_verdict; REJECTED_DEFENSIVE additionally opens
            # variant_paused_defensive in the (live) operator_review_queue.
            # apply_verdict now owns the single TSV write for every verdict — the
            # freshly-measured cohort metrics are forwarded via `metrics=v` so the
            # persisted terminal row reflects the measured cohort, not the running
            # row's placeholders. There is NO separate tail mirror (a second
            # append_experiment would duplicate the row — append is not upsert).
            if not dry_run:
                for v in verdicts:
                    eval_verdict_str = v["verdict"]
                    exp_verdict = to_experiment_verdict(eval_verdict_str)
                    if exp_verdict is None:
                        # Non-terminal verdict (e.g. evaluator emits a new state we
                        # don't yet persist). §0 #9: silent skip is correct only
                        # for unrecognized terminal-states; surface a breadcrumb on
                        # stderr so operators can grep for data-quality signals.
                        click.echo(
                            f"  Skipped {v['experiment_id']}: "
                            f"verdict={eval_verdict_str!r} is not in ExperimentVerdict.",
                            err=True,
                        )
                        continue

                    result = apply_verdict(
                        v["experiment_id"], exp_verdict, attio=attio, metrics=v,
                    )
                    msg = (
                        f"  Updated {v['experiment_id']} → "
                        f"{result['status_written']}"
                    )
                    if result["queue_opened"]:
                        msg += " [variant_paused_defensive queued]"
                    click.echo(msg)

    except RunLockHeld as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(EXIT_TEMPFAIL)

    click.echo("\n=== Learning Complete ===")


@cli.command("detect-bad-companies")
def detect_bad_companies_cmd():
    """Detect pipeline records linked to companies with linkedin.com domain."""
    from workflows.backfill_companies import detect_bad_company_links

    click.echo("=== Outbound Agent -- Detect Bad Company Links ===\n")

    with _crm_provider() as crm:
        file_path = detect_bad_company_links(crm)

    click.echo("\nReview the CSV, then run:")
    click.echo(f"  python3 cli.py repair-companies --csv {file_path}")


@cli.command("repair-companies")
@click.option("--csv", "detect_csv", required=True, type=click.Path(exists=True), help="CSV from detect-bad-companies")
@click.option("--sales-nav-profile-scraper-id", envvar="PB_SALES_NAV_PROFILE_SCRAPER_ID", required=True, help="PhantomBuster Sales Navigator Profile Scraper agent ID (the legacy Profile Scraper agent was deleted from the PB workspace)")
@click.option("--dry-run", is_flag=True, help="Show what would be repaired without making changes")
def repair_companies_cmd(detect_csv, sales_nav_profile_scraper_id, dry_run):
    """Repair pipeline records with bad company links via PB re-scraping."""
    import csv as csv_mod

    from clients.phantombuster import PhantomBusterClient
    from workflows.backfill_companies import repair_bad_companies

    click.echo("=== Outbound Agent -- Repair Bad Company Links ===\n")

    if dry_run:
        with open(detect_csv, newline="", encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        click.echo(f"Would repair {len(rows)} records:")
        for row in rows:
            click.echo(f"  {row['name']} — currently: {row.get('current_wrong_company', '?')}")
        from workflows.backfill_companies import REPAIR_MAX_PROFILES_PER_LAUNCH
        if len(rows) > REPAIR_MAX_PROFILES_PER_LAUNCH:
            click.echo(
                f"\nNote: the wet run scrapes at most "
                f"{REPAIR_MAX_PROFILES_PER_LAUNCH} profiles per launch — "
                f"{len(rows) - REPAIR_MAX_PROFILES_PER_LAUNCH} row(s) would be "
                f"deferred to a follow-up detect/repair cycle."
            )
        return

    from workflows.daily_check_helpers import SalesNavConfigError

    try:
        with _attio_client() as attio, PhantomBusterClient() as pb:
            summary = repair_bad_companies(attio, pb, sales_nav_profile_scraper_id, detect_csv)
    except SalesNavConfigError as exc:
        # Missing SN cookie / dead-or-mistyped SN scraper id — operator
        # config problems, not crashes. Surface the named fix, not a
        # traceback.
        raise click.ClickException(str(exc)) from exc

    if summary.get("error"):
        # The scrape produced no CSV — nothing was re-linked. Distinct from
        # the zero-counter success over an already-clean detect CSV.
        click.echo(
            "\n=== Repair FAILED: scrape returned no CSV — nothing was "
            "re-linked. Re-run repair-companies (the detect CSV is still "
            "valid). ===",
            err=True,
        )
        if summary.get("deferred"):
            click.echo(
                f"Deferred: {summary['deferred']} row(s) beyond the per-launch "
                f"scrape cap remain queued behind the failed batch.",
                err=True,
            )
        raise SystemExit(1)

    click.echo("\n=== Repair Complete ===")
    click.echo(f"Linked: {summary['linked']}, Failed: {summary['failed']}, Skipped: {summary['skipped']}")
    if summary.get("deferred"):
        click.echo(
            f"Deferred: {summary['deferred']} row(s) beyond the per-launch scrape "
            f"cap — re-run detect-bad-companies, then repair-companies again."
        )


@cli.command("reconcile-pending-invites")
@click.option("--sales-nav-profile-scraper-id", envvar="PB_SALES_NAV_PROFILE_SCRAPER_ID", required=True, help="PhantomBuster Sales Navigator Profile Scraper agent ID")
@click.option("--dry-run/--wet", "dry_run", default=True, help="Preview PROSPECT candidates without scraping or writing (default). Pass --wet to scrape and apply.")
@click.option("--force", "force_rescrape", is_flag=True, help="Bypass the recheck-cache TTL and re-scrape every PROSPECT candidate")
@click.option("--batch-size", default=25, show_default=True, type=int, help="Max profiles scraped this run (PB cost bound)")
def reconcile_pending_invites_cmd(sales_nav_profile_scraper_id, dry_run, force_rescrape, batch_size):
    """Flip PROSPECT rows that are already pending on LinkedIn to CONNECTION_SENT.

    Phase C backlog-clearer for the re-selection leak. On --wet, re-scrapes
    PROSPECT rows via Sales Nav and flips ONLY those returning
    hasPendingInvitation=true (a fresh, positive already-invited signal — never
    ghost-advances). Defaults to --dry-run (candidate preview, no scrape/write);
    review, then re-run with --wet.
    """
    from clients.attio import AttioClient
    from clients.phantombuster import PhantomBusterClient
    from workflows.daily_check_helpers import SalesNavConfigError
    from workflows.pending_invite_reconciliation import (
        run_pending_invite_reconciliation,
    )
    from workflows.pre_invite_check import ConfigError

    click.echo("=== Sales Agent -- Reconcile Pending Invites ===\n")
    list_id = os.environ.get("ATTIO_LIST_ID", "")
    if not list_id:
        raise click.ClickException("ATTIO_LIST_ID is not set.")

    try:
        with AttioClient() as attio, PhantomBusterClient() as pb:
            summary = run_pending_invite_reconciliation(
                attio=attio,
                pb=pb,
                sales_nav_profile_scraper_id=sales_nav_profile_scraper_id,
                list_id=list_id,
                dry_run=dry_run,
                force_rescrape=force_rescrape,
                batch_size=batch_size,
            )
    except (SalesNavConfigError, ConfigError) as exc:
        # Config failures (e.g. a missing Sales Nav session cookie on the wet
        # scrape path) surface the named fix, not a traceback. No write happened.
        raise click.ClickException(str(exc)) from exc

    click.echo(f"\nSummary: {summary}")
    if dry_run and summary.get("candidates"):
        click.echo(
            f"\n{summary['candidates']} candidate(s) previewed. "
            f"Re-run with --wet to scrape and flip the genuinely-pending rows."
        )


@cli.command("detect-deal-dupes")
@click.option(
    "--out",
    "out_path",
    default="deal_dedup_report.json",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Where to write the JSON report",
)
def detect_deal_dupes_cmd(out_path: str):
    """Scan Attio deals for duplicates → emit deal_dedup_report.json.

    Read-only. Bucket by associated_company UUID (auto-safe) and by normalized
    name with diverging UUIDs (conflict; needs the operator to review).
    """
    import json as json_mod

    from scripts.attio_dedup import build_deal_report

    click.echo("=== Outbound Agent -- Detect Deal Dupes ===\n")
    with _attio_client() as attio:
        report = build_deal_report(attio)

    with open(out_path, "w") as f:
        json_mod.dump(report, f, indent=2, default=str)

    s = report["summary"]
    click.echo(
        f"\nSummary: {s['deals_groups_auto_safe']} auto-safe + "
        f"{s['deals_groups_conflict']} conflict groups → "
        f"{s['deals_records_to_delete']} records would be deleted (auto-safe only).",
    )
    click.echo(f"\nReport: {out_path}")
    if s["deals_groups_conflict"]:
        click.echo(
            "Review conflict_groups in the report. Move each into "
            "approved_conflict_groups (to merge) or skipped_groups (to leave alone) "
            "before running apply-deal-dedup."
        )
    click.echo("\nNext: python3 cli.py apply-deal-dedup --report " + out_path)


@cli.command("apply-deal-dedup")
@click.option("--report", "report_path", required=True, type=click.Path(exists=True, dir_okay=False), help="Path to deal_dedup_report.json")
@click.option("--pretend", is_flag=True, help="Exercise code paths without calling Attio write APIs")
@click.option("--log", "log_path", default="deal_dedup_apply_log.jsonl", show_default=True, help="Path for the apply log (JSONL)")
def apply_deal_dedup_cmd(report_path: str, pretend: bool, log_path: str):
    """Apply a triaged deal_dedup_report.json. Writes a JSONL audit log."""
    import json as json_mod

    from scripts.attio_dedup import apply_deal_report

    click.echo("=== Outbound Agent -- Apply Deal Dedup ===\n")
    report = json_mod.loads(__import__("pathlib").Path(report_path).read_text())
    totals = apply_deal_report(report, pretend=pretend, log_path=log_path)
    click.echo(
        f"\nApplied: {totals['deals_groups']} deal groups, "
        f"{totals['errors']} errors → log: {log_path}",
    )
    if totals["errors"]:
        raise SystemExit(1)


@cli.command("detect-deal-contact-gaps")
@click.option(
    "--out",
    "out_path",
    default="deal_contact_backfill_report.csv",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Where to write the CSV report",
)
@click.option(
    "--only-empty",
    is_flag=True,
    help="Restrict to deals where associated_people is currently empty",
)
def detect_deal_contact_gaps_cmd(out_path: str, only_empty: bool):
    """Scan deals for missing contact links → emit deal_contact_backfill_report.csv.

    For each deal, finds candidate people that point at the same company UUID
    but are not yet linked. Scores each candidate via quality_gate and labels
    rows: link / review / skip-disqualified / already-linked.
    """
    from workflows.repair_deal_contacts import detect_deal_contact_gaps

    click.echo("=== Outbound Agent -- Detect Deal Contact Gaps ===\n")
    with _attio_client() as attio:
        summary = detect_deal_contact_gaps(attio, out_path=out_path, only_empty=only_empty)

    click.echo(f"Scanned: {summary['deals_scanned']} deals, {summary['people_indexed']} people")
    click.echo(
        f"Candidates: {summary['candidate_pairs']} (link={summary['link']}, "
        f"review={summary['review']}, skip-disqualified={summary['skip_disqualified']}, "
        f"already-linked={summary['already_linked']})"
    )
    click.echo(f"Deals with no associated_company: {summary['deals_with_no_company']}")
    click.echo(f"\nReport: {summary['report_path']}")
    click.echo(
        "\nReview rows marked 'review' or 'skip-disqualified' in the CSV.\n"
        f"Next: python3 cli.py apply-deal-contact-backfill --report {summary['report_path']}"
    )


@cli.command("apply-deal-contact-backfill")
@click.option("--report", "report_path", required=True, type=click.Path(exists=True, dir_okay=False), help="Path to deal_contact_backfill_report.csv")
@click.option("--pretend", is_flag=True, help="Exercise code paths without calling Attio write APIs")
@click.option("--log", "log_path", default="deal_contact_link_audit.jsonl", show_default=True, help="Path for the audit log (JSONL)")
def apply_deal_contact_backfill_cmd(report_path: str, pretend: bool, log_path: str):
    """Apply rows marked 'link' in the report — PATCH each deal's associated_people."""
    from workflows.repair_deal_contacts import apply_deal_contact_backfill

    click.echo("=== Outbound Agent -- Apply Deal Contact Backfill ===\n")
    with _attio_client() as attio:
        totals = apply_deal_contact_backfill(
            attio, report_path=report_path, pretend=pretend, log_path=log_path,
        )

    click.echo(
        f"\nDeals touched: {totals['deals_touched']}, "
        f"people linked: {totals['people_linked']}, "
        f"skipped (idempotent): {totals['deals_skipped_idempotent']}, "
        f"errors: {totals['errors']} → log: {log_path}",
    )
    if totals["errors"]:
        raise SystemExit(1)


@cli.command(name="weekly-finalize")
@click.option("--batch", required=True, help="Date batch in YYYY-MM-DD format")
@click.option("--dry-run", is_flag=True, help="Print verdicts without writing to Attio")
def weekly_finalize_cmd(batch: str, dry_run: bool):
    """Commit agent-qualified borderlines to Attio after Haiku verdict pass."""
    import json
    from pathlib import Path

    from workflows.industry_classifier import build_anthropic_client
    from workflows.weekly_prospect import _commit_prospect

    anthropic_client = build_anthropic_client()

    click.echo(f"=== Outbound Agent -- Weekly Finalize ({batch}) ===\n")

    borderline_path = Path("exports") / f"weekly_borderline_{batch}.jsonl"
    verdicts_path = Path("exports") / f"weekly_verdicts_{batch}.jsonl"

    if not borderline_path.exists():
        click.echo(f"Error: borderline file not found: {borderline_path}")
        raise SystemExit(1)
    if not verdicts_path.exists():
        click.echo(f"Error: verdicts file not found: {verdicts_path}")
        raise SystemExit(1)

    with borderline_path.open() as f:
        staged = [json.loads(line) for line in f if line.strip()]

    with verdicts_path.open() as f:
        verdicts_raw = [json.loads(line) for line in f if line.strip()]

    verdicts_by_url = {v["linkedin_url"]: v for v in verdicts_raw}

    list_id = os.environ.get("ATTIO_LIST_ID", "")
    today = batch

    passed = 0
    failed = 0
    missing = 0
    write_errors = 0
    # Supply accounting (2026-07-16): `_commit_prospect` returns True for a
    # record that was ALREADY in the pipeline (the re-stamp guard correctly
    # skips the write), so "Passed: N (committed)" said nothing about net-new
    # supply — a finalize run can print "309 committed" while the pipeline
    # list grows by ~20. Thread the summary dict it already supports and
    # split the counts per scoring lane so search exhaustion is visible in
    # the run output itself.
    summary: dict = {"net_new_created": 0, "restamped_existing": 0}
    lane_stats: dict[str, dict[str, int]] = {}

    with _crm_provider() as crm:
        # PR-207: pre-fetch the pipeline list once so the truth-based
        # already-listed guard in _commit_prospect resolves in-memory (no
        # per-prospect CRM scan) and a borderline already in the pipeline is
        # never re-stamped back to a fresh stage. This CLI path passed no
        # snapshot, so the re-stamp skip was a silent no-op (re-stamp incident).
        existing_entries = (
            crm.query_list_entries(list_id=list_id) if not dry_run else None
        )
        if existing_entries is not None:
            # Surface the snapshot size — a suspiciously low count is the tell
            # that the guard is running against a truncated/empty list.
            click.echo(
                f"  Pre-fetched {len(existing_entries)} pipeline entries for the "
                "re-stamp guard."
            )
        for entry in staged:
            url = entry["linkedin_url"]
            verdict = verdicts_by_url.get(url)
            if verdict is None:
                missing += 1
                click.echo(f"  [MISSING VERDICT] {entry['prospect_data'].get('name', url)}")
                continue

            name = entry["prospect_data"].get("name", url)
            rationale = verdict.get("rationale", "")

            if not verdict.get("pass"):
                failed += 1
                click.echo(f"      → [AGENT FAIL] {name} — {rationale}")
                continue

            if dry_run:
                click.echo(f"      → [DRY RUN] Would commit: {name} (icp_lane={verdict.get('icp_lane')})")
                passed += 1
                continue

            # PR-28 fold (salesman-weekly QA convergence): propagate
            # `icp_lane` from the Haiku verdict so `icp_lane_persisted`
            # writes through `_build_prospect_entry_attrs` for the
            # borderline-LLM commit path. Pre-fold, the field was
            # echoed to the terminal at line above and then dropped.
            # Full wiring to `weekly_finalize_idempotent` (14-day
            # idempotency + ICP-2 geo gate on the CLI path) is a
            # tracked follow-up — this fold closes the more urgent
            # `icp_lane_persisted` write gap.
            score_result_fields = {
                "persona": entry["persona"],
                "language": entry["language"],
                "score": entry["score"],
                "score_breakdown": entry.get("score_breakdown"),
                "scoring_lane": entry.get("scoring_lane"),
                "verdict_path": "borderline_pass",
                "llm_rationale": rationale,
                "icp_lane": verdict.get("icp_lane"),
            }
            # Per-lane net-new accounting via before/after diff:
            # _commit_prospect bumps exactly one summary counter per
            # successful call (and none on a False return), so the delta
            # attributes this entry to its lane without widening
            # _commit_prospect's contract.
            net_new_before = summary["net_new_created"]
            ok = _commit_prospect(
                crm,
                entry["prospect_data"],
                entry["raw_csv_row"],
                score_result_fields,
                list_id,
                today,
                anthropic_client=anthropic_client,
                existing_entries=existing_entries,
                summary=summary,
            )
            if ok:
                passed += 1
                lane = entry.get("scoring_lane") or "unknown"
                bucket = lane_stats.setdefault(
                    lane, {"net_new": 0, "already_listed": 0}
                )
                if summary["net_new_created"] > net_new_before:
                    bucket["net_new"] += 1
                    click.echo(
                        f"      → [AGENT PASS] {name} committed NET-NEW "
                        f"(icp_lane={verdict.get('icp_lane')})"
                    )
                else:
                    bucket["already_listed"] += 1
                    click.echo(
                        f"      → [AGENT PASS] {name} already in pipeline — "
                        f"skipped (re-stamp guard, cadence preserved)"
                    )
            else:
                write_errors += 1
                click.echo(f"      → [WRITE ERROR] {name} — failed to commit to Attio")

    click.echo("\n--- Weekly Finalize Summary ---")
    click.echo(f"Staged:    {len(staged)}")
    click.echo(f"Verdicts:  {len(verdicts_raw)}")
    click.echo(f"Passed:    {passed}  (agent-approved)")
    if not dry_run:
        net_new = summary["net_new_created"]
        restamped = summary["restamped_existing"]
        click.echo(f"  ├ net-new pipeline entries:                 {net_new}")
        click.echo(f"  ├ already in pipeline (skipped, no write):  {restamped}")
        click.echo(f"  └ write errors:                             {write_errors}")
        for lane in sorted(lane_stats):
            stats = lane_stats[lane]
            click.echo(
                f"     {lane}: {stats['net_new']} net-new / "
                f"{stats['already_listed']} already-listed"
            )
    click.echo(f"Failed:    {failed}  (rejected by agent)")
    click.echo(f"Missing:   {missing}  (staged but no verdict — skipped)")
    # Recycling alarm (mirrors the bulk weekly's zero-net-new warning in
    # workflows/weekly_prospect.py — see the run-summary supply block): when
    # the majority of agent passes were already in the pipeline, the saved
    # searches are re-serving the same population. Relative threshold, not a
    # flat floor, so a small-but-fresh batch doesn't false-alarm.
    if not dry_run and passed > 0 and summary["net_new_created"] * 2 < passed:
        click.echo(
            f"\n⚠️  SUPPLY WARNING: only {summary['net_new_created']} of "
            f"{passed} agent-passed prospects were NET-NEW pipeline entries. "
            "The saved searches are recycling people already in the "
            "pipeline — refresh or expand the saved searches before the "
            "next weekly run."
        )


@cli.command(name="weekly-brain")
@click.option("--experiment-id", required=True, help="Target experiment id, e.g. exp-003")
@click.option("--dry-run", is_flag=True, help="Print the proposal path without writing the file")
@click.option(
    "--open-pr",
    is_flag=True,
    help="PR-32: open a draft PR + variant_proposal_pending queue row after writing the proposal",
)
def weekly_brain_cmd(experiment_id: str, dry_run: bool, open_pr: bool):
    """Run the weekly brain: measure cohorts, pre-screen variants, write proposal."""
    from workflows.weekly_brain import load_dm1_variants, run_weekly_brain

    click.echo(f"=== Outbound Agent Weekly Brain — {experiment_id} ===\n")
    variants_by_persona = load_dm1_variants()
    click.echo(
        f"Loaded {sum(len(v) for v in variants_by_persona.values())} DM1 variants "
        f"across {len(variants_by_persona)} personas."
    )

    with _attio_client() as attio:
        out_path = run_weekly_brain(
            attio,
            experiment_id=experiment_id,
            variants_by_persona=variants_by_persona,
            dry_run=dry_run,
            open_pr=open_pr,
        )

    action = "would write" if dry_run else "wrote"
    click.echo(f"\n✓ {action} proposal: {out_path}")


@cli.command(name="sales-approve")
@click.argument("experiment_id", required=False)
@click.option("--approve", "decision", flag_value="approve", help="Approve + merge the proposal PR")
@click.option("--reject", "decision", flag_value="reject", help="Reject the proposal (requires --rationale)")
@click.option("--rationale", default=None, help="Required for --reject; recorded on the queue row")
def sales_approve_cmd(experiment_id: str | None, decision: str | None, rationale: str | None):
    """PR-32: list / approve / reject pending variant_proposal_pending queue rows.

    Examples:

        sales-approve                        # list pending proposals
        sales-approve exp-003 --approve      # merge PR + close queue row
        sales-approve exp-003 --reject --rationale="reactance still too high"
    """
    from workflows.sales_approve import (
        ProposalNotFoundError,
        approve_proposal,
        list_pending_proposals,
        reject_proposal,
    )

    # The pending-row reads go through the vendor-neutral provider
    # (``bundle.provider``); the approve/reject writes still use the raw
    # client (``bundle.attio``) via ``AttioWriter``. Both come from the same
    # factory/lifecycle, so opening both context managers is safe.
    with _crm_provider() as crm, _attio_client() as attio:
        if experiment_id is None:
            pending = list_pending_proposals(crm)
            if not pending:
                click.echo("No pending variant_proposal_pending rows.")
                return
            click.echo(f"Pending proposals ({len(pending)}):")
            for prop in pending:
                click.echo(
                    f"  {prop['experiment_id']:25s}  PR: {prop['pr_url']}  "
                    f"branch: {prop['branch_name']}"
                )
            return

        if decision is None:
            click.echo(
                "Pass --approve or --reject (--reject also requires --rationale).",
                err=True,
            )
            raise click.exceptions.Exit(2)

        try:
            if decision == "approve":
                result = approve_proposal(experiment_id, crm=crm, attio=attio)
                click.echo(
                    f"✓ Approved + merged {experiment_id} ({result['pr_url']})"
                )
            else:
                if not rationale:
                    click.echo("--reject requires --rationale", err=True)
                    raise click.exceptions.Exit(2)
                result = reject_proposal(
                    experiment_id, crm=crm, attio=attio, rationale=rationale,
                )
                click.echo(
                    f"✓ Rejected {experiment_id} "
                    f"(PR closed: {result['pr_closed']})"
                )
        except ProposalNotFoundError as exc:
            click.echo(f"error: {exc}", err=True)
            raise click.exceptions.Exit(1) from exc


@cli.command(name="industry-approve")
@click.argument("company_name", required=False)
@click.option("--approve", "decision", flag_value="approve", help="Confirm the operator's industry choice")
@click.option("--reject", "decision", flag_value="reject", help="Reject (requires --rationale; Company record stays low_confidence)")
@click.option("--vertical", default=None, help="Required for --approve; one of the 11 INDUSTRY_LABELS")
@click.option("--rationale", default=None, help="Required for --reject; recorded on the queue row")
@click.option("--record-id", "record_id", default=None, help="Override Company record_id when name resolution is ambiguous")
def industry_approve_cmd(
    company_name: str | None,
    decision: str | None,
    vertical: str | None,
    rationale: str | None,
    record_id: str | None,
):
    """FU-2: list / approve / reject pending industry_low_confidence queue rows.

    Examples:

        industry-approve                                       # list pending rows
        industry-approve "Acme Foods" --approve --vertical "Food & Beverage"
        industry-approve "Acme Foods" --reject --rationale="not a manufacturer"
    """
    from workflows.industry_approve import (
        AmbiguousCompanyError,
        CompanyNotFoundError,
        IndustryRowNotFoundError,
        InvalidVerticalError,
        approve_industry_classification,
        list_pending_industry_low_confidence,
        reject_industry_classification,
    )

    # Pending-row reads go through the vendor-neutral provider
    # (``bundle.provider``); the company-resolution search + approve/reject
    # writes still use the raw client (``bundle.attio``). Both share the
    # same factory/lifecycle.
    with _crm_provider() as crm, _attio_client() as attio:
        if company_name is None:
            pending = list_pending_industry_low_confidence(crm)
            if not pending:
                click.echo("No pending industry_low_confidence rows.")
                return
            click.echo(f"Pending industry classifications ({len(pending)}):")
            for row in pending:
                click.echo(
                    f"  {row['company_name']:40s}  suggested: {row['suggested_vertical']:20s}  "
                    f"conf: {row['confidence']:.2f}"
                )
            return

        if decision is None:
            click.echo(
                "Pass --approve --vertical <label> or --reject --rationale <text>.",
                err=True,
            )
            raise click.exceptions.Exit(2)

        try:
            if decision == "approve":
                if not vertical:
                    click.echo("--approve requires --vertical", err=True)
                    raise click.exceptions.Exit(2)
                result = approve_industry_classification(
                    company_name,
                    vertical=vertical,
                    crm=crm,
                    attio=attio,
                    record_id=record_id,
                )
                click.echo(
                    f"✓ Confirmed {company_name!r} as {vertical!r} "
                    f"(company={result['company_record_id']})"
                )
            else:
                if not rationale:
                    click.echo("--reject requires --rationale", err=True)
                    raise click.exceptions.Exit(2)
                reject_industry_classification(
                    company_name, rationale=rationale, crm=crm, attio=attio,
                )
                click.echo(f"✓ Rejected industry suggestion for {company_name!r}")
        except (
            IndustryRowNotFoundError,
            CompanyNotFoundError,
            AmbiguousCompanyError,
            InvalidVerticalError,
        ) as exc:
            click.echo(f"error: {exc}", err=True)
            raise click.exceptions.Exit(1) from exc


@cli.command(name="weekly-diagnostic")
@click.option(
    "--since",
    "since_str",
    default=None,
    help="ISO date (YYYY-MM-DD). Currently informational; the diagnostic pulls all entries.",
)
@click.option(
    "--out-dir",
    "out_dir_str",
    default="docs/experiments",
    help="Where to write the markdown report.",
)
@click.option("--no-llm", is_flag=True, help="Skip the Sonnet critique step (diagnostic only)")
def weekly_diagnostic_cmd(since_str: str | None, out_dir_str: str, no_llm: bool):
    """Phase 1 auto-research: stratified funnel diagnostic + LLM critique.

    Writes a markdown report to docs/experiments/<date>-diagnostic.md with
    the verdict (ICP / message / both / insufficient), top/bottom segments,
    disagreement examples, and a single LLM-proposed change with hypothesis.
    """
    from datetime import date as _date
    from pathlib import Path

    from workflows.industry_classifier import build_anthropic_client
    from workflows.qualifier_critique import propose_change
    from workflows.qualifier_diagnostic import diagnose_funnel

    since_date = _date.fromisoformat(since_str) if since_str else None
    today = _date.today()
    out_dir = Path(out_dir_str)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today.isoformat()}-diagnostic.md"

    click.echo(f"=== Phase 1 auto-research diagnostic — {today.isoformat()} ===\n")

    with _attio_client() as attio:
        click.echo("Pulling list entries from Attio...")
        report = diagnose_funnel(attio, since_date=since_date)

    click.echo(
        f"\nVerdict: {report.verdict}"
        f"\nOverall: {report.n_total_dmed} DM'd · "
        f"reply rate {report.overall_response_rate:.1%} · "
        f"defensive rate {report.overall_defensive_rate:.1%} · "
        f"connect accept rate {report.overall_accept_rate:.1%}\n"
    )

    if no_llm:
        click.echo("--no-llm flag set — skipping LLM critique.\n")
        proposal_md = "_LLM critique skipped (--no-llm)._\n"
    else:
        click.echo("Running LLM critique (Claude Sonnet)...")
        anthropic_client = build_anthropic_client()
        proposal = propose_change(report, anthropic_client)
        click.echo(
            f"  Proposal: {proposal.action} (predicted lift {proposal.predicted_lift_pp}pp)\n"
        )
        proposal_md = proposal.to_markdown()

    body = report.to_markdown() + "\n" + proposal_md
    out_path.write_text(body, encoding="utf-8")
    click.echo(f"\n✓ Wrote {out_path}")
    click.echo(
        "\nNext step: review the markdown, and if the proposal action is "
        "weight_change/keyword_change/threshold_change, open a PR by hand "
        "with the specific_change applied to workflows/quality_gate.py."
    )


@cli.command("replay-run")
@click.argument("run_id")
def replay_run(run_id):
    """Print the resume manifest for a prior run's audit log.

    Operator-facing inspect-only command. Does NOT mutate state — the
    output tells you what was already done so you can plan a follow-up
    run that skips completed record_ids.
    """
    import json

    from workflows.audit import resume_from_audit

    try:
        manifest = resume_from_audit(run_id)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc

    # Record-id sets aren't JSON-serializable — flatten to sorted lists.
    serializable = dict(manifest)
    for key in ("record_ids_seen", "record_ids_failed"):
        serializable[key] = sorted(manifest[key])
    click.echo(json.dumps(serializable, indent=2, default=str))


@cli.command("audit-tail")
@click.option("--follow", "-f", is_flag=True, help="Stream new lines as they're appended (tail -F semantics)")
@click.option("--run-id", default=None, help="Specific run to tail (default: newest audit file)")
def audit_tail(follow, run_id):
    """Tail the most recent audit file (or a specific run-id)."""
    import time

    from workflows.audit import AUDIT_DIR, _find_audit_path, list_runs

    if run_id:
        try:
            path = _find_audit_path(run_id)
        except FileNotFoundError as exc:
            raise click.ClickException(str(exc)) from exc
    else:
        runs = list_runs(since_days=30)
        if not runs:
            raise click.ClickException(f"No audit files in {AUDIT_DIR}")
        path = runs[0]

    click.echo(f"# tailing {path}")
    with open(path) as f:
        # Stream existing content first.
        for line in f:
            click.echo(line.rstrip())
        if not follow:
            return
        # tail -F: re-read on append, sleeping briefly between polls.
        while True:
            line = f.readline()
            if line:
                click.echo(line.rstrip())
            else:
                time.sleep(0.5)


@cli.command("audit-stats")
@click.option("--since-days", default=7, show_default=True, help="Aggregate over the last N days")
def audit_stats_cmd(since_days):
    """Print aggregated stats over recent audit logs."""
    import json

    from workflows.audit import audit_stats

    stats = audit_stats(since_days=since_days)
    click.echo(json.dumps(stats, indent=2))


@cli.command("audit-list")
@click.option("--since-days", default=7, show_default=True, help="List runs from the last N days")
def audit_list(since_days):
    """List recent audit runs with status and event count.

    Operator's first-stop after a cron failure alert: shows every run_id
    in the window, newest first, so `replay-run <id>` is a single
    additional command away.
    """
    import json

    from workflows.audit import list_runs

    runs = list_runs(since_days=since_days)
    if not runs:
        click.echo(f"# No audit files in the last {since_days} days.")
        return
    for path in runs:
        # path shape: run-{YYYY-MM-DD}-{run_id}.jsonl
        stem = path.stem  # strips .jsonl
        try:
            _, yyyy, mm, dd, run_id = stem.split("-", 4)
            run_date = f"{yyyy}-{mm}-{dd}"
        except ValueError:
            run_date = "?"
            run_id = stem
        # Read last line for status + scan for total event count.
        # Linear scan is O(file) but the operator runs this rarely.
        last_event_name = "?"
        event_count = 0
        crash_msg = ""
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event_count += 1
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                last_event_name = ev.get("event", "?")
                if last_event_name == "run_crashed":
                    crash_msg = f" — {ev.get('exc_type', '?')}: {ev.get('exc_msg', '')[:60]}"
        status = {
            "run_completed": "completed",
            "run_crashed": "crashed",
        }.get(last_event_name, "in_progress")
        click.echo(f"{run_date}  {run_id}  {status:13s}  events={event_count}{crash_msg}")


# =====================================================================
# Slash-command-driven workflow registry. Each subcommand below is the
# Python entry point invoked by a SKILL.md in `skills/`. Operator
# triggers in Claude Code via the slash command; the skill body
# resolves to the matching `sales <name>` call here.
# =====================================================================


@cli.command("data-quality-report")
@click.option("--period-days", default=7, show_default=True,
              help="Lookback window in days.")
@click.option("--no-write", is_flag=True,
              help="Compute + print the report without writing the Attio row.")
def data_quality_report_cmd(period_days: int, no_write: bool) -> None:
    """Compute the 8-metric Data Quality Report and emit P0/P1 alarms.

    Exit code 0 = clean, 1 = P1 alarm, 70 = P0 alarm (downstream
    consumer halts DM sends). See scripts/data_quality_report.py.
    """
    from scripts.data_quality_report import main as dqr_main

    argv: list[str] = ["--period-days", str(period_days)]
    if no_write:
        argv.append("--no-write")
    raise SystemExit(dqr_main(argv))


@cli.command("threshold-calibration")
@click.option("--dry-run", is_flag=True,
              help="Compute the ROC sweep without opening a recommendation queue row.")
def threshold_calibration_cmd(dry_run: bool) -> None:
    """Run the score-threshold ROC calibration."""
    try:
        from scripts.threshold_calibration import main as tc_main
    except ImportError as exc:
        # Avoid asserting WHY the import failed in operator-visible
        # text — the script may legitimately not be installed yet,
        # OR a real bug could break the import. Either way the
        # operator's recovery is the same: ensure the file is present.
        click.echo(
            f"threshold-calibration script unavailable: {exc}",
            err=True,
        )
        raise SystemExit(2) from exc
    argv: list[str] = []
    if dry_run:
        argv.append("--dry-run")
    raise SystemExit(tc_main(argv))


@cli.command("audit-reminder-sweep")
@click.option("--stale-days", default=7, show_default=True,
              help="Open queue rows older than this many days are reported as stale.")
def audit_reminder_sweep_cmd(stale_days: int) -> None:
    """Scan Operator Review Queue for rows that have been `open` for
    longer than `stale-days` and print a digest the operator can scan
    in one screen. Read-only — no Attio mutations."""
    from datetime import UTC, datetime, timedelta

    from clients.attio import AttioClient

    cutoff = datetime.now(UTC) - timedelta(days=stale_days)
    cutoff_iso = cutoff.isoformat()

    with _attio_client() as attio:
        body = {
            "filter": {
                "status": {"$eq": "open"},
                "opened_at": {"$lte": cutoff_iso},
            },
            "limit": 200,
        }
        data = attio._request(
            "POST",
            "/objects/operator_review_queue/records/query",
            json=body,
        )

    # Validate shape — `data.get("data", [])` would silently treat a
    # missing key as "no rows" and the operator would see "Clean."
    # without ever having queried.
    if not isinstance(data, dict) or "data" not in data:
        click.echo(
            f"error: Attio response shape unexpected: {data!r}", err=True,
        )
        raise SystemExit(1)
    rows = data["data"] or []

    if not rows:
        click.echo(f"No queue rows open for more than {stale_days} day(s). Clean.")
        return

    click.echo(f"=== Stale Operator Review Queue rows (>{stale_days}d) ===\n")
    click.echo(f"{len(rows)} row(s):\n")
    for row in rows:
        values = row.get("values", {}) or {}
        row_type = AttioClient.object_record_first_value(values, "type") or "?"
        idempotency_key = AttioClient.object_record_first_value(
            values, "idempotency_key",
        ) or "?"
        opened_at = AttioClient.object_record_first_value(values, "opened_at") or "?"
        click.echo(f"  type={row_type}  opened={opened_at}  key={idempotency_key}")


@cli.command("health-check")
def health_check_cmd() -> None:
    """Verify Attio + PB connectivity without mutating state.

    Operator runs this ad-hoc when something feels off. Exit 0 on
    healthy, 1 on any reachability failure."""

    failures: list[str] = []
    try:
        with _attio_client() as attio:
            # whoami / health-equivalent: query the operator_review_queue
            # with limit=1. Cheap and proves API + auth work.
            attio._request(
                "POST",
                "/objects/operator_review_queue/records/query",
                json={"limit": 1},
            )
        click.echo("Attio: OK")
    except Exception as exc:
        failures.append(f"Attio: FAIL ({type(exc).__name__}: {exc})")

    try:
        from clients.phantombuster import PhantomBusterClient

        with PhantomBusterClient() as _pb:
            _pb.list_agents()
        click.echo("PhantomBuster: OK")
        click.echo("Session-cookie liveness requires a separate scraper health check.")
    except Exception as exc:
        failures.append(f"PhantomBuster: FAIL ({type(exc).__name__}: {exc})")

    # Shipped-placeholder content check. Not fatal to connectivity, but a live
    # send would be BLOCKED until these are replaced — surface it loudly here so
    # the operator fixes it before their first real run.
    from workflows.content_guard import content_has_placeholders

    placeholder_files = content_has_placeholders()
    if placeholder_files:
        failures.append(
            "Content: FAIL — shipped placeholder content still in place "
            f"({', '.join(placeholder_files)}). Replace it or set "
            "OUTBOUND_CONTENT_DIR before any live send. See GETTING_STARTED.md."
        )
    else:
        click.echo("Content: OK")

    if failures:
        for f in failures:
            click.echo(f, err=True)
        raise SystemExit(1)
    click.echo("\nAll health checks passed.")


@cli.command("sales-finalize-borderline")
@click.option("--batch", required=True, help="Batch date YYYY-MM-DD.")
@click.option("--dry-run", is_flag=True,
              help="Compute the finalize but skip the Attio writes.")
@click.pass_context
def sales_finalize_borderline_cmd(ctx, batch: str, dry_run: bool) -> None:
    """Finalize the morning's borderline batch with idempotency-key
    collision protection. See workflows/auto_finalize.py for the
    idempotency contract.
    """
    from datetime import date as _date

    from workflows.auto_finalize import (
        FinalizeRunFailed,
        auto_finalize_borderline_batch,
    )

    try:
        batch_date = _date.fromisoformat(batch)
    except ValueError as exc:
        click.echo(f"error: --batch must be YYYY-MM-DD; got {batch!r}", err=True)
        raise SystemExit(2) from exc

    # Invoke the sibling weekly-finalize command directly through
    # Click rather than via subprocess. Direct invocation preserves
    # tracebacks, exit codes, and the Python process — subprocess
    # would truncate stderr at 500 chars and swallow any in-process
    # exception state.
    def _finalize_fn(batch_iso: str, decision_run_id: str) -> dict:
        try:
            ctx.invoke(
                weekly_finalize_cmd,
                batch=batch_iso,
                dry_run=dry_run,
            )
            exit_code = 0
        except SystemExit as exc:
            exit_code = int(exc.code or 0)
        return {
            "exit_code": exit_code,
            "decision_run_id": decision_run_id,
        }

    try:
        with _attio_client() as attio:
            out = auto_finalize_borderline_batch(
                attio,
                batch_date=batch_date,
                finalize_fn=_finalize_fn,
            )
    except FinalizeRunFailed as exc:
        # Surface the inner failure exit code to a calling skill so
        # the operator sees a non-zero exit, not a misleading 0.
        click.echo(
            f"sales-finalize-borderline FAILED: {exc.payload}", err=True,
        )
        raise SystemExit(exc.payload.get("exit_code") or 1) from exc

    click.echo(
        f"action={out['action']}  decision_run_id={out['decision_run_id']}  "
        f"key={out['idempotency_key']}"
    )
    if out["finalize_result"] is not None:
        click.echo(f"finalize_result: {out['finalize_result']}")


# ── Follow-up Radar (Phase C) ──────────────────────────────────────────────
def _followup_lane_counts(summary: dict) -> str:
    """Lane-count suffix for the radar footer, mirroring render_digest's
    split-count convention: partner → owed → waiting → cold responder →
    LinkedIn-warm → nudge, with zero-count lanes omitted so an empty lane
    doesn't add noise."""
    parts = []
    if summary["partner"]:
        parts.append(f"{summary['partner']} partner intro")
    if summary["owed"]:
        parts.append(f"{summary['owed']} owed")
    if summary.get("waiting"):
        parts.append(f"{summary['waiting']} waiting")
    if summary.get("cold_responder"):
        parts.append(f"{summary['cold_responder']} cold responder")
    if summary["linkedin_warm"]:
        parts.append(f"{summary['linkedin_warm']} LinkedIn-warm")
    if summary["nudge"]:
        parts.append(f"{summary['nudge']} nudge")
    return "".join(f" · {p}" for p in parts)


@cli.command("followup")
@click.option("--dry-run", is_flag=True, help="No effect on this command — detection is read-only; kept for interface parity with the daily run.")
@click.option("--limit", type=int, default=0, help="Cap the number of surfaced candidates (0 = all). The skill layer drafts for the top-N; keep this small when drafting.")
@click.option("--json", "json_out", is_flag=True, help="Emit the candidate list as JSON for the skill layer to consume (in addition to the human digest).")
@click.option("--full", is_flag=True, help="Show the entire Nudge lane instead of a top-3 preview.")
def followup_cmd(dry_run, limit, json_out, full):
    """Follow-up Radar: detect warm-but-stale accounts and render a ranked digest.

    Read-only. Detects accounts that engaged (replied, call booked, demo'd,
    open deal) then went quiet, ranks them Owed-first, and prints a digest.
    The skill layer (Phase C) consumes ``--json`` to verify last-touch and
    draft follow-ups; this command never sends or writes. Fails-closed-clean
    when the radar schema is absent (the engine degrades to an inert digest),
    so a schemaless install runs it as a no-op.
    """
    import json as _json

    from workflows.followup_radar import run_followup_radar

    click.echo("=== Follow-up Radar ===\n")
    # Standalone diagnostic command: unlike Phase C (firewalled inside the
    # daily run), a bad ATTIO_LIST_ID or a CRM outage here should print a
    # clean error and exit 1 rather than dump a raw traceback at the operator.
    try:
        with _crm_provider() as crm:
            summary = run_followup_radar(crm, limit=(limit or None), full=full)
    except Exception as exc:  # noqa: BLE001 — operator-facing diagnostic
        import traceback
        click.echo(
            f"ERROR: follow-up radar failed: {type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}",
            err=True,
        )
        raise SystemExit(1) from exc

    click.echo(summary["digest"])
    click.echo(
        f"\n({summary['surfaced']} of {summary['total']} surfaced"
        f"{_followup_lane_counts(summary)})"
    )
    if json_out:
        click.echo("\n--- candidates (json) ---")
        click.echo(_json.dumps(summary["candidates"], indent=2))


_FOLLOWUP_OBJECTS = ("linkedin_outreach", "deals")
# A snooze/callback more than this far out is almost certainly a typo (a year
# instead of a date, a fat-fingered 2099). Parking an account that far silently
# removes it from the radar with no signal — reject it loudly instead.
_FOLLOWUP_MAX_HORIZON_DAYS = 400


def _reject_absurd_followup_date(d, label):
    """Exit 1 if a snooze/callback date is absurdly far in the future."""
    from datetime import timedelta

    from models.business_calendar import operator_today

    horizon = operator_today() + timedelta(days=_FOLLOWUP_MAX_HORIZON_DAYS)
    if d > horizon:
        click.echo(
            f"ERROR: {label}={d.isoformat()} is more than "
            f"{_FOLLOWUP_MAX_HORIZON_DAYS} days out — likely a typo. Parking an "
            f"account that far silently hides it. Refusing.",
            err=True,
        )
        raise SystemExit(1)


def _followup_state_call(fn_name: str, object_: str, target_id: str, **kwargs):
    """Shared plumbing for the followup state-write commands.

    ``target_id`` is the list ENTRY id for linkedin_outreach, or the deal
    record_id for deals. Resolves the list_id from env for the list case,
    builds an AttioWriter, and dispatches to workflows.followup_state.<fn_name>.
    Prints a clean error + exits 1 on failure (operator-facing).
    """
    from clients.attio_writer import AttioWriter
    from workflows import followup_state

    list_id = os.environ.get("ATTIO_LIST_ID", "").strip() if object_ == "linkedin_outreach" else None
    if object_ == "linkedin_outreach" and not list_id:
        click.echo("ERROR: ATTIO_LIST_ID not set (needed for linkedin_outreach writes)", err=True)
        raise SystemExit(1)
    try:
        fn = getattr(followup_state, fn_name)
        fn(AttioWriter(), object=object_, record_id=target_id, list_id=list_id, **kwargs)
    except Exception as exc:  # noqa: BLE001 — operator-facing
        click.echo(f"ERROR: {fn_name} failed: {type(exc).__name__}: {exc}", err=True)
        raise SystemExit(1) from exc
    click.echo(f"ok: {fn_name} {object_}:{target_id}")


@cli.command("followup-stamp")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), required=True)
@click.option("--id", "target_id", required=True, help="Entry id (linkedin_outreach) or deal record_id.")
@click.option("--draft-id", required=True, help="Gmail draft id just created.")
def followup_stamp_cmd(object_, target_id, draft_id):
    """Record that a follow-up draft was created (followup_draft_at + _draft_id).

    Called by the skill layer (Phase C) right after it creates a Gmail draft, so
    the radar won't re-draft this account until it's acted on or the draft goes
    stale."""
    _followup_state_call("stamp_draft", object_, target_id, draft_id=draft_id)


@cli.command("followup-snooze")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), required=True)
@click.option("--id", "target_id", required=True)
@click.option("--until", required=True, help="YYYY-MM-DD — radar skips this account until then.")
def followup_snooze_cmd(object_, target_id, until):
    """Park an account until a date (one-click operator snooze)."""
    until_d = date.fromisoformat(until)
    _reject_absurd_followup_date(until_d, "snooze --until")
    _followup_state_call("set_snooze", object_, target_id, until=until_d)


@cli.command("followup-mute")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), required=True)
@click.option("--id", "target_id", required=True)
@click.option("--unmute", is_flag=True, help="Re-include a previously-muted account.")
def followup_mute_cmd(object_, target_id, unmute):
    """Permanently exclude (or with --unmute, re-include) an account. Used to
    park the stale Partner-Intro backlog so it stops re-surfacing."""
    _followup_state_call("set_muted", object_, target_id, muted=not unmute)


@cli.command("followup-mute-batch")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), default="linkedin_outreach", show_default=True)
@click.option("--file", "id_file", required=True, type=click.Path(exists=True, dir_okay=False), help="Path to a file of ids, one per line (blank lines and #-comments skipped).")
def followup_mute_batch_cmd(object_, id_file):
    """Mute many accounts from a file — the first-run backlog flush.

    One id per line (entry_id for linkedin_outreach, deal record_id for deals);
    blank lines and lines starting with '#' are skipped, duplicates are
    collapsed. Writes are sequential and idempotent, so re-running the same file
    is safe.

    Output contract (pipe-clean): stdout carries ONLY the failed ids, one per
    line, nothing else — so `... > retry.txt` yields a directly re-runnable
    retry file. Everything human-facing (the per-failure 'FAILED <id>: <err>'
    lines and the final 'Muted N of M' summary) goes to stderr. Exit 0 iff
    every id succeeded."""
    from clients.attio_writer import AttioWriter
    from workflows import followup_state

    # Parse: strip whitespace, drop blanks + #-comments, dedupe preserving order.
    seen: set[str] = set()
    record_ids: list[str] = []
    with open(id_file, encoding="utf-8") as fh:
        for raw in fh:
            rid = raw.strip()
            if not rid or rid.startswith("#") or rid in seen:
                continue
            seen.add(rid)
            record_ids.append(rid)

    if not record_ids:
        click.echo(f"ERROR: no ids in {id_file} (after skipping blanks and #-comments)", err=True)
        raise SystemExit(1)

    list_id = os.environ.get("ATTIO_LIST_ID", "").strip() if object_ == "linkedin_outreach" else None
    if object_ == "linkedin_outreach" and not list_id:
        click.echo("ERROR: ATTIO_LIST_ID not set (needed for linkedin_outreach writes)", err=True)
        raise SystemExit(1)

    total = len(record_ids)
    succeeded, failed = followup_state.mute_batch(
        AttioWriter(), object=object_, record_ids=record_ids, list_id=list_id
    )
    # Stream contract: stdout = failed ids ONLY (pipe-clean for a retry file);
    # all human-facing lines go to stderr so `> retry.txt` can't capture the
    # summary as a phantom record id.
    for rid, err in failed:
        click.echo(f"FAILED {rid}: {err}", err=True)
    click.echo(f"Muted {len(succeeded)} of {total}", err=True)
    if failed:
        click.echo("Failed ids:", err=True)
        for rid, _ in failed:
            click.echo(rid)
        raise SystemExit(1)


@cli.command("followup-callback")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), required=True)
@click.option("--id", "target_id", required=True)
@click.option("--date", "cb_date", default=None, help="YYYY-MM-DD — reconnect date; radar hard-surfaces it then.")
@click.option("--clear", is_flag=True, help="Null the callback instead — the tickler fired and you acted on it (run BEFORE followup-await --since; callback + waiting never coexist).")
def followup_callback_cmd(object_, target_id, cb_date, clear):
    """Set (or with --clear, null) a deferral tickler ('contáctame en agosto')
    — suppress until the date, then hard-surface as Owed. Clear it once the
    callback has been discharged, so the account can enter the WAITING cycle."""
    if clear == bool(cb_date):
        click.echo("ERROR: pass exactly one of --date or --clear", err=True)
        raise SystemExit(1)
    if clear:
        _followup_state_call("clear_callback", object_, target_id)
        return
    cb = date.fromisoformat(cb_date)
    _reject_absurd_followup_date(cb, "callback --date")
    _followup_state_call("set_callback", object_, target_id, callback=cb)


@cli.command("followup-refer")
@click.option("--id", "deal_id", required=True, help="Deal record_id to attribute.")
@click.option("--partner-email", required=True, help="Referring partner's email (normalized to lowercase).")
def followup_refer_cmd(deal_id, partner_email):
    """Stamp the referring partner's email on a deal (deals-only attribution).

    Called by the Phase C skill layer when it finds a known partner
    among a deal candidate's Gmail thread participants. Invalid email → clear
    stderr message, exit 1 (never stamps a bad value onto a deal)."""
    from clients.attio_writer import AttioWriter
    from workflows import followup_state

    try:
        followup_state.stamp_referred_by(
            AttioWriter(), deal_id=deal_id, partner_email=partner_email
        )
    except Exception as exc:  # noqa: BLE001 — operator-facing
        click.echo(f"ERROR: stamp_referred_by failed: {type(exc).__name__}: {exc}", err=True)
        raise SystemExit(1) from exc
    click.echo(f"ok: stamp_referred_by deals:{deal_id}")


@cli.command("followup-touch")
@click.option("--id", "deal_id", required=True, help="Deal record_id to stamp.")
@click.option("--date", "touch_date", default=None, help="YYYY-MM-DD — the C.2-verified true last-touch date (from the email thread / call transcript).")
@click.option("--clear", is_flag=True, help="Null the verified touch instead (correction path for a bad stamp) — deal recency falls back to the interaction join / creation date.")
def followup_touch_cmd(deal_id, touch_date, clear):
    """Stamp (or with --clear, null) the verified true last-touch on a deal.

    Deals-only. Called by the Phase C skill layer after each
    successful C.2 email/call verification, so engine ranking (recency
    tier 1) gets truer every run. Dates are guarded in both directions
    (future beyond 1 day of UTC skew, or >400 days past = likely year typo)
    — rejected loudly, never a silent bad stamp."""
    from clients.attio_writer import AttioWriter
    from workflows import followup_state

    if clear == bool(touch_date):
        click.echo("ERROR: pass exactly one of --date or --clear", err=True)
        raise SystemExit(1)
    try:
        if clear:
            followup_state.clear_verified_touch(AttioWriter(), deal_id=deal_id)
        else:
            touch = date.fromisoformat(touch_date)
            followup_state.stamp_verified_touch(AttioWriter(), deal_id=deal_id, touch=touch)
    except Exception as exc:  # noqa: BLE001 — operator-facing (incl. a malformed --date)
        click.echo(f"ERROR: followup-touch failed: {type(exc).__name__}: {exc}", err=True)
        raise SystemExit(1) from exc
    verb = "clear_verified_touch" if clear else "stamp_verified_touch"
    click.echo(f"ok: {verb} deals:{deal_id}")


def _read_awaiting_state(object_: str, target_id: str) -> dict | None:
    """Read the awaiting_reply_* attrs for one deal/entry (parsed dict or None).

    Used by the ``followup-await`` clear/nudge paths, which need the CURRENT
    since-date and nudge count (the resolved event's latency data, the nudge
    ceiling). Returns None when the record can't be read — callers decide how
    loudly to fail.
    """
    from clients.attio import AttioClient

    try:
        with AttioClient() as attio:
            if object_ == "deals":
                rec = attio.get_deal(target_id)
                return AttioClient.parse_deal(rec) if rec else None
            entry = attio.get_list_entry(target_id)
            return AttioClient.parse_entry(entry) if entry else None
    except Exception as exc:  # noqa: BLE001 — callers refuse/fail per-path
        click.echo(
            f"WARNING: could not read awaiting state for {object_}:{target_id}: "
            f"{type(exc).__name__}: {exc}",
            err=True,
        )
        return None


@cli.command("followup-await")
@click.option("--object", "object_", type=click.Choice(_FOLLOWUP_OBJECTS), required=True)
@click.option("--id", "target_id", required=True, help="Entry id (linkedin_outreach) or deal record_id.")
@click.option("--since", "since_date", default=None, help="YYYY-MM-DD — your most recent unanswered send (C.2-verified). Overwrites any prior stamp; deals also co-stamp last_verified_touch.")
@click.option("--thread", "thread_id", default=None, help="Gmail thread id of the unanswered send (required with --since; advisory — drafts re-resolve the live thread).")
@click.option("--clear", is_flag=True, help="End the waiting cycle (nulls since/thread/nudge count; the canonical note id is kept).")
@click.option("--resolved", "resolved_date", default=None, help="YYYY-MM-DD the reply arrived (only with --clear) — emits the awaiting_reply_resolved learning event BEFORE clearing.")
@click.option("--nudged", is_flag=True, help="Count a nudge draft against the 2-nudge ceiling (exit 1 at the ceiling — no more auto-nudges).")
@click.option("--note-id", "note_id", default=None, help="Canonical waiting-note id to persist (standalone, or alongside --nudged).")
def followup_await_cmd(object_, target_id, since_date, thread_id, clear, resolved_date, nudged, note_id):
    """Manage WAITING-lane state: you sent an email, no reply yet.

    Modes (exactly one): --since (stamp/re-stamp after a C.2-verified Gmail
    check — never from the raw sweep), --clear [--resolved] (reply arrived or
    operator reset; --resolved emits the reply-latency learning event first),
    --nudged (count a nudge draft; hard max 2 per cycle), or --note-id alone
    (persist the canonical note id after creating the note via MCP).
    Dates are guarded like followup-touch (future >1d skew / >400d past =
    exit 1)."""
    modes = [bool(since_date), clear, nudged, bool(note_id) and not nudged and not since_date]
    if sum(modes) != 1:
        click.echo(
            "ERROR: pass exactly one mode: --since (+--thread), --clear "
            "[--resolved], --nudged [--note-id], or --note-id alone",
            err=True,
        )
        raise SystemExit(1)
    if resolved_date and not clear:
        click.echo("ERROR: --resolved only makes sense with --clear", err=True)
        raise SystemExit(1)

    if since_date:
        if not thread_id:
            click.echo("ERROR: --since requires --thread (the Gmail thread id)", err=True)
            raise SystemExit(1)
        since = date.fromisoformat(since_date)
        _followup_state_call(
            "set_awaiting_reply", object_, target_id,
            since=since, thread_id=thread_id,
        )
        if note_id:
            _followup_state_call("set_awaiting_note_id", object_, target_id, note_id=note_id)
        return

    if nudged:
        state = _read_awaiting_state(object_, target_id)
        if state is None:
            click.echo(
                "ERROR: could not read the current nudge count — refusing to "
                "increment blind (the 2-nudge ceiling would be unenforceable).",
                err=True,
            )
            raise SystemExit(1)
        try:
            current = int(state.get("awaiting_reply_nudge_count") or 0)
        except (TypeError, ValueError):
            # A malformed count must NOT read as 0 — that would silently
            # reset the ceiling and re-nudge an account that had its 2.
            click.echo(
                f"ERROR: awaiting_reply_nudge_count is malformed "
                f"({state.get('awaiting_reply_nudge_count')!r}) — refusing to "
                "increment; repair the value (or followup-await --clear) first.",
                err=True,
            )
            raise SystemExit(1) from None
        _followup_state_call(
            "increment_awaiting_nudge", object_, target_id,
            current_count=current, note_id=note_id,
        )
        return

    if clear:
        if resolved_date:
            # Emit the learning event BEFORE clearing — the clear destroys the
            # latency data the event carries. Idempotent on (type, key), so a
            # re-run after a failed clear refreshes the row, never duplicates.
            resolved = date.fromisoformat(resolved_date)
            _reject_absurd_followup_date(resolved, "await --resolved")
            state = _read_awaiting_state(object_, target_id)
            if state is None:
                # A failed READ is not "no stamp" — clearing here would
                # silently destroy the real awaiting_reply_since and the
                # latency data point with it. Refuse; plain --clear (no
                # --resolved) remains the explicit no-event escape hatch.
                click.echo(
                    "ERROR: could not read the record — refusing to clear "
                    "blind with --resolved (the latency event would be lost "
                    "with no trace). Retry, or use plain --clear to skip the "
                    "event deliberately.",
                    err=True,
                )
                raise SystemExit(1)
            since_raw = state.get("awaiting_reply_since")
            since_d = date.fromisoformat(str(since_raw)[:10]) if since_raw else None
            if since_d is None:
                click.echo(
                    "WARNING: no awaiting_reply_since on the record — clearing "
                    "anyway, but no resolved event (latency unknowable).",
                    err=True,
                )
            elif resolved < since_d:
                click.echo(
                    f"ERROR: --resolved {resolved.isoformat()} predates the "
                    f"stamped send {since_d.isoformat()} — a reply can't "
                    "arrive before the send; check the date. Nothing written.",
                    err=True,
                )
                raise SystemExit(1)
            else:
                try:
                    nudges = int((state or {}).get("awaiting_reply_nudge_count") or 0)
                except (TypeError, ValueError):
                    nudges = 0
                try:
                    from workflows.escalation import escalate

                    escalate(
                        type="awaiting_reply_resolved",
                        idempotency_key=f"awaiting-resolved|{target_id}|{resolved.isoformat()}",
                        payload={
                            "record_id": target_id,
                            "object": object_,
                            "awaiting_reply_since": since_d.isoformat(),
                            "resolved": resolved.isoformat(),
                            "latency_days": (resolved - since_d).days,
                            "nudge_count": nudges,
                        },
                    )
                except Exception as esc_exc:  # noqa: BLE001 — best-effort: a queue hiccup must not strand the clear
                    click.echo(
                        f"WARNING: awaiting_reply_resolved event failed "
                        f"({type(esc_exc).__name__}: {esc_exc}) — clearing anyway; "
                        "the latency data point is lost.",
                        err=True,
                    )
        _followup_state_call("clear_awaiting_reply", object_, target_id)
        return

    # --note-id alone: persist the canonical note id post-creation.
    _followup_state_call("set_awaiting_note_id", object_, target_id, note_id=note_id)


if __name__ == "__main__":
    cli()
