"""Association outreach workflow.

Sends one-shot outreach emails to industry associations (e.g., AFAMO) asking
for partnership intros or member directory access. NOT a drip campaign.

Reads contacts from `content/association_outreach.json` and tracks already-sent
state in `~/.outbound-agent/association_outreach_sent.json` so re-runs are idempotent.

# Multi-operator authority (Phase 6)

The local ``~/.outbound-agent/association_outreach_sent.json`` is a fast-path
negative cache only.  The AUTHORITY check is the ``outreach_channel`` select
attribute on the Attio Person record.  When ``attio`` is provided:

1. If the contact id is already in the local file → skip Attio (fast-path
   negative cache).
2. Otherwise, look up the contact in Attio and check whether
   ``"association_outreach"`` is in their ``outreach_channel`` values.
3. Three-state result from the authority check:
   - ``True``  (Attio confirms sent): divergence — Attio wins, local file is
     repaired (entry written with ``repaired_from_attio=True``), a
     ``local_ledger_divergence`` audit event is emitted.
   - ``None``  (check errored / Attio unreachable): contact is skipped THIS
     RUN ONLY — nothing is written to the local file, and an
     ``attio_authority_check_failed`` event is emitted.  A transient outage
     can never permanently poison the ledger.
   - ``False`` (Attio confirms not sent): contact is included in the pending
     list and will be sent to normally.

If ``attio`` is ``None`` (dry-run / legacy call-site), the local file is the
sole guard — pre-Phase-6 behaviour is preserved.

# Shared crash-safe sent-ledger

In addition to the two guards above, every live send is recorded in the shared
email sent-ledger (``~/.outbound-agent/email_sent.json``, step
``ASSOCIATION_LEDGER_STEP``) immediately after the Resend call returns and
BEFORE the local state save / Attio stamp. A contact found in that ledger at
send time is never re-sent — the run repairs the local file and Attio stamp
instead (``repaired_from_shared_ledger`` entry) so a crash between the send
and the state writes cannot cause a duplicate on any later run.

Usage (from cli.py):
    python3 cli.py email-association --dry-run
    python3 cli.py email-association --yes
"""

import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import click
import httpx

from clients.resend_client import ResendClient
from models.business_calendar import is_send_day
from models.campaign import CONTENT_DIR
from models.email_campaign import get_email_from, get_reply_to
from workflows.cross_channel_suppression import (
    OUTREACH_CHANNEL_ASSOCIATION,
    stamp_outreach_channel,
)
from workflows.daily_check_helpers import _assert_email_not_blank
from workflows.email_compliance import (
    LedgerCorruptError,
    already_sent,
    append_footer,
    assert_email_compliance_ready,
    list_unsubscribe_header,
    mark_sent,
)

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from clients.attio_writer import AttioWriter
    from workflows.audit import AuditLogger

CONTACTS_FILE = CONTENT_DIR / "association_outreach.json"
SENT_STATE_FILE = Path.home() / ".outbound-agent" / "association_outreach_sent.json"

# Step name in the shared crash-safe email sent-ledger (~/.outbound-agent/
# email_sent.json). Keyed alongside the email1/2/3 drip entries; the contact id
# from association_outreach.json plays the record_id role.
ASSOCIATION_LEDGER_STEP = "association"


def _load_contacts() -> list[dict]:
    """Load all association outreach contacts from the JSON file."""
    with open(CONTACTS_FILE) as f:
        data = json.load(f)
    return data.get("contacts", [])


def _load_sent_state() -> dict:
    """Load the set of contact IDs already sent."""
    if not SENT_STATE_FILE.exists():
        return {}
    with open(SENT_STATE_FILE) as f:
        return json.load(f)


def _save_sent_state(state: dict) -> None:
    """Persist the sent state atomically (temp-file + os.replace).

    os.replace is atomic on POSIX — a concurrent reader always sees either
    the old file or the fully-written new file, never a torn write.  A
    genuine OS error (disk full, permissions) propagates; losing the sent
    ledger is not a swallow-and-continue case.
    """
    SENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = SENT_STATE_FILE.with_name(f"{SENT_STATE_FILE.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, SENT_STATE_FILE)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def _matching_attio_person_ids(attio: "AttioClient", email: str) -> list[str]:
    """Resolve every matching Person, failing if the result is incomplete."""
    if not email:
        raise ValueError("contact has no email for Attio lookup")
    results = attio.search_people(
        filter_={"email_addresses": email}, limit=50, fail_if_truncated=True
    )
    if not isinstance(results, list) or not results:
        raise ValueError("Attio Person search did not resolve a record")
    record_ids: list[str] = []
    for result in results:
        if not isinstance(result, dict):
            raise ValueError("Attio Person search result is malformed")
        result_id = result.get("id")
        record_id = result_id.get("record_id") if isinstance(result_id, dict) else None
        if not isinstance(record_id, str) or not record_id:
            raise ValueError("Attio Person search result has no record ID")
        record_ids.append(record_id)
    return record_ids


def _attio_already_sent(
    attio: "AttioClient",
    contact: dict,
    *,
    audit_logger: "AuditLogger | None" = None,
) -> bool | None:
    """Three-state authority check against Attio.

    Returns:
        True  — Attio confirms the contact was already sent to
                (``"association_outreach"`` present in ``outreach_channel``).
        False — Attio confirms the contact has NOT been sent to.
        None  — Check failed (network error, unexpected response, etc.);
                caller must skip this contact for THIS RUN ONLY without
                writing anything to the local ledger.

    On ``None`` the helper prints a warning to stderr and emits an
    ``attio_authority_check_failed`` audit event so the operator can see
    the temporary skip.  It does NOT emit ``local_ledger_divergence`` —
    that event is reserved for genuine confirmed-divergences (Attio says
    sent, local file says not).

    An unresolved Person or malformed authority value is a failed check,
    never confirmation that a send is safe.
    """
    email = (contact.get("email") or "").strip()
    try:
        already_sent = False
        for record_id in _matching_attio_person_ids(attio, email):
            person = attio.get_person(record_id)
            if not isinstance(person, dict) or not person:
                raise ValueError("Attio Person detail is missing")
            values = person.get("values")
            if not isinstance(values, dict):
                raise ValueError("Attio Person values are missing or malformed")
            raw = values.get("outreach_channel")
            if not isinstance(raw, list):
                raise ValueError("Attio outreach_channel is missing or malformed")
            for item in raw:
                if not isinstance(item, dict) or not isinstance(item.get("option"), dict):
                    raise ValueError("Attio outreach_channel entry is malformed")
                title = item["option"].get("title")
                if not isinstance(title, str) or not title:
                    raise ValueError("Attio outreach_channel title is malformed")
                already_sent = already_sent or title == OUTREACH_CHANNEL_ASSOCIATION
        return already_sent
    except Exception as exc:  # noqa: BLE001 — return None on any error
        cid = contact.get("id")
        print(
            f"[association_outreach] Attio authority check failed for "
            f"contact_id={cid!r} email={email!r}: "
            f"{type(exc).__name__}: {exc} — skipping this run only (no local write)",
            file=sys.stderr,
        )
        if audit_logger is not None:
            audit_logger.event(
                "attio_authority_check_failed",
                contact_id=cid,
                email=email,
                exc_type=type(exc).__name__,
                exc_msg=str(exc)[:500],
                resolution="skip_this_run_only",
            )
        return None


def get_pending_association_emails(
    *,
    attio: "AttioClient | None" = None,
    audit_logger: "AuditLogger | None" = None,
    strict_authority: bool = False,
) -> list[dict]:
    """Return all contacts that haven't been sent yet.

    When ``attio`` is provided, Attio's ``outreach_channel`` attribute is
    the AUTHORITY check; the local file is a fast-path negative cache.

    When ``attio`` is None, the local file is the sole guard (legacy path).
    A run uses ``strict_authority`` so an Attio outage cannot look like a
    completed batch while direct read-only callers retain the skipped rows.
    """
    contacts = _load_contacts()
    sent = _load_sent_state()

    if attio is None:
        # Legacy path: local file is sole guard.
        return [c for c in contacts if c["id"] not in sent]

    # Attio-authority path.
    # Three-state routing for _attio_already_sent:
    #   True  → Attio confirmed sent: divergence repair (local write + audit event)
    #   None  → Check errored: skip contact THIS RUN ONLY, no local write, emit
    #            attio_authority_check_failed (already emitted by the helper)
    #   False → Attio confirmed not sent: add to pending
    pending: list[dict] = []
    repair_entries: dict[str, dict] = {}
    authority_errors = 0

    for c in contacts:
        cid = c["id"]
        # Fast-path negative cache: already in local file → skip Attio.
        if cid in sent:
            continue
        # Attio is the authority.
        authority_result = _attio_already_sent(attio, c, audit_logger=audit_logger)
        if authority_result is True:
            # Genuine divergence: Attio says sent, local file does not.
            # repaired_from_attio=True is human-forensics metadata only —
            # no code reads this flag; it exists solely so an operator
            # diffing the ledger can tell which entries were backfilled from
            # Attio vs written by a live send on this machine.
            repair_entries[cid] = {
                "repaired_from_attio": True,
                "repaired_at": datetime.now(UTC).isoformat(),
                "email": c.get("email", ""),
            }
            if audit_logger is not None:
                audit_logger.event(
                    "local_ledger_divergence",
                    contact_id=cid,
                    ledger="association_outreach_sent.json",
                    resolution="attio_wins",
                )
        elif authority_result is None:
            # Check failed — attio_authority_check_failed was already emitted
            # by the helper.  Skip this contact for THIS RUN ONLY; do not write
            # anything to the local file so a transient outage cannot poison the
            # ledger and permanently fast-path-skip this contact on all future runs.
            authority_errors += 1
        else:
            # authority_result is False: Attio confirmed not sent.
            pending.append(c)

    if repair_entries:
        # Repair the local file: merge in the confirmed-divergent entries.
        sent.update(repair_entries)
        _save_sent_state(sent)

    if strict_authority and authority_errors:
        raise RuntimeError(
            f"Attio authority checks failed for {authority_errors} association "
            "contact(s); no emails sent. Retry after Attio access is restored."
        )

    return pending


def _audit_best_effort(audit_logger: "AuditLogger | None", event: str, **payload) -> None:
    """Emit an audit event without ever raising. Used inside failure handlers:
    the audit log and the state files share a disk, so they fail together, and
    a failed audit write must never displace the delivery-safety error it is
    annotating."""
    if audit_logger is None:
        return
    try:
        audit_logger.event(event, **payload)
    except Exception as exc:  # noqa: BLE001 — see docstring
        print(
            f"[association_outreach] audit event {event!r} could not be written "
            f"({type(exc).__name__}: {exc}) — see stderr above for the "
            f"underlying failure.",
            file=sys.stderr,
        )


def _send_definitely_failed(exc: Exception) -> bool:
    """True only when a failed send provably never reached Resend, so it is
    safe to count as an error and move on. Anything else — HTTP responses,
    read timeouts, protocol errors, local bugs mid-call — is UNKNOWN: the
    message may already be queued, and the caller must fail closed."""
    return isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout))


def _stamp_attio_post_send(
    *,
    attio: "AttioClient",
    writer: "AttioWriter",
    contact: dict,
    audit_logger: "AuditLogger | None",
) -> bool:
    """Stamp ``association_outreach`` on the Person's ``outreach_channel`` post-send.

    Failure emits ``outreach_channel_stamp_failed`` audit event + stderr so the
    operator can see the duplicate-risk gap, but NEVER raises — the email has
    already been delivered. Returns True when the stamp landed, False when it
    failed (callers report which dedup records actually survive).
    """
    email = (contact.get("email") or "").strip()
    cid = contact.get("id")
    try:
        for record_id in _matching_attio_person_ids(attio, email):
            stamp_outreach_channel(
                writer,
                attio,
                person_record_id=record_id,
                channel=OUTREACH_CHANNEL_ASSOCIATION,
            )
        return True
    except Exception as exc:  # noqa: BLE001 — stamp failure must not abort the run
        print(
            f"[association_outreach] WARNING: Attio outreach_channel stamp FAILED for "
            f"contact_id={cid!r} email={email!r}: "
            f"{type(exc).__name__}: {exc} — "
            f"this send is invisible to the cross-machine authority until repaired.",
            file=sys.stderr,
        )
        _audit_best_effort(
            audit_logger,
            "outreach_channel_stamp_failed",
            contact_id=cid,
            email=email,
            exc_type=type(exc).__name__,
            exc_msg=str(exc)[:500],
        )
        return False


def run_association_outreach(
    resend: ResendClient | None,
    dry_run: bool = False,
    auto_confirm: bool = False,
    force_weekend: bool = False,
    *,
    attio: "AttioClient | None" = None,
    audit_logger: "AuditLogger | None" = None,
    writer: "AttioWriter | None" = None,
) -> dict:
    """Send pending association outreach emails. Idempotent.

    Args:
        resend: Resend client (None for dry-run only).
        dry_run: If True, print what would be sent without sending.
        auto_confirm: If True, skip the interactive confirmation prompt.
        force_weekend: If True, send even on Sat/Sun.
        attio: Optional AttioClient for Attio-authority dedup check (Phase 6).
               When provided, ``outreach_channel`` on the Person record is the
               authority; the local file is a fast-path negative cache only.
        audit_logger: Optional AuditLogger; receives ``local_ledger_divergence``
               events when the Attio authority and local file disagree.
        writer: Optional AttioWriter used to stamp ``association_outreach`` on
               the Person's ``outreach_channel`` after each successful send
               (Phase 6 write-side).  Required for cross-machine dedup;
               omitted only in dry-run / legacy call-sites.

    Returns:
        Summary dict with counts.
    """
    today = date.today()
    if not is_send_day(today) and not force_weekend:
        click.echo("Weekend — no association emails sent. Use --force-weekend to override.")
        return {"pending": 0, "sent": 0, "errors": 0, "skipped": 0, "reason": "weekend"}

    pending = get_pending_association_emails(
        attio=attio, audit_logger=audit_logger, strict_authority=True
    )
    summary = {"pending": len(pending), "sent": 0, "errors": 0, "skipped": 0}

    if not pending:
        click.echo("No pending association outreach emails. All recipients already contacted.")
        return summary

    # CAN-SPAM send-gate (no-op on dry_run): refuse to send live without a
    # configured physical postal address / sender org / unsubscribe address,
    # or with an unreadable shared sent-ledger. Runs after the empty-pending
    # return so a no-op run on an unconfigured box stays a clean no-op.
    assert_email_compliance_ready(dry_run=dry_run)

    # Shared-ledger snapshot for an honest preview/prompt: contacts already in
    # the crash-safe ledger get their state repaired (local file + Attio stamp)
    # instead of a re-send, and the preview and confirm prompt must say so.
    repair_ids: set[str] = set()
    if dry_run:
        # Dry-run is exempt from the compliance gate, so it is the ONLY caller
        # that can reach an unreadable ledger here; degrade the preview loudly
        # instead of crashing it.
        try:
            repair_ids = {
                c["id"] for c in pending if already_sent(c["id"], ASSOCIATION_LEDGER_STEP)
            }
        except (LedgerCorruptError, OSError, UnicodeDecodeError) as exc:
            click.echo(
                f"WARNING: shared sent-ledger is unreadable ({exc}) — this "
                f"preview cannot tell which contacts were already sent and may "
                f"over-count. A live run is blocked by the compliance gate "
                f"until the ledger is repaired.",
                err=True,
            )
    else:
        # No try/except: assert_email_compliance_ready probed the ledger above,
        # so an exception here is a real invariant violation and must fail
        # closed (an empty repair_ids would re-send the whole crash window).
        repair_ids = {
            c["id"] for c in pending if already_sent(c["id"], ASSOCIATION_LEDGER_STEP)
        }
    to_send_count = len(pending) - len(repair_ids)
    summary["repair_only"] = len(repair_ids)

    # Validate every contact that can actually be sent before the first send.
    # Repair-only rows need no copy; blocking them here would prevent the
    # shared-ledger state from being reconciled after a prior send.
    for c in pending:
        if c["id"] not in repair_ids:
            _assert_email_not_blank(c["subject"], c["body_html"], c["email"], "association")

    click.echo(f"=== Association Outreach — {len(pending)} pending ===\n")
    for c in pending:
        repair_tag = "  [repair-only: already sent on a prior run]" if c["id"] in repair_ids else ""
        click.echo(f"  → {c['name']} <{c['email']}>{repair_tag}")
        click.echo(f"    Subject: {c['subject']}")
        click.echo(f"    Org: {c['organization']}")
        click.echo()

    if dry_run:
        click.echo("[DRY RUN] No emails sent.")
        return summary

    if to_send_count and not auto_confirm and not click.confirm(
        f"Send {to_send_count} association outreach email(s)?"
    ):
        click.echo("Cancelled.")
        summary["skipped"] = len(pending)
        return summary

    if to_send_count and resend is None:
        # Repair-only batches never touch Resend, so they proceed without a
        # client; only actual sends require one.
        click.echo("Error: ResendClient is required for live send.")
        summary["errors"] = to_send_count
        return summary

    # Loop-invariant send identity/config, resolved once so an operator
    # misconfiguration fails loud here instead of masquerading as N per-contact
    # send failures inside the loop's except.
    from_address = get_email_from()
    reply_to = get_reply_to()
    unsubscribe_headers = list_unsubscribe_header()

    sent_state = _load_sent_state()
    for c in pending:
        # --- send domain ---------------------------------------------------
        ledger_exc: Exception | None = None
        send_unknown_exc: Exception | None = None
        is_repair = c["id"] in repair_ids
        if is_repair:
            # Sent on a prior (possibly crashed) run: the shared ledger was
            # written but the crash hit before the local state save / Attio
            # stamp. Do NOT re-send — fall through to repair those writes so
            # future runs skip this contact in the pending computation too.
            # Entry shape mirrors the repaired_from_attio convention: this is
            # a backfill, not a live send, so no fabricated sent_at/resend_id
            # (the true send date lives in the shared ledger).
            click.echo(
                f"  [SKIP] already sent on a prior run (state repaired) -> {c['email']}"
            )
            summary["skipped"] += 1
            sent_state[c["id"]] = {
                "repaired_from_shared_ledger": True,
                "repaired_at": datetime.now(UTC).isoformat(),
                "email": c["email"],
            }
            _audit_best_effort(
                audit_logger,
                "association_ledger_repair",
                contact_id=c["id"],
                email=c["email"],
                resolution="repair_local_and_attio_no_resend",
            )
        else:
            html, text = append_footer(c["body_html"])
            result: dict = {}
            try:
                assert resend is not None
                result = resend.send_email(
                    to=c["email"],
                    subject=c["subject"],
                    html=html,
                    from_address=from_address,
                    reply_to=reply_to,
                    text=text,
                    headers=unsubscribe_headers,
                )
            except Exception as e:
                if _send_definitely_failed(e):
                    # The request provably never reached Resend — safe to count
                    # as an error and move to the next contact.
                    click.echo(f"  ✗ Not sent to {c['email']}: {e}")
                    summary["errors"] += 1
                    continue
                # Outcome UNKNOWN (timeout / 5xx / bug after Resend may have
                # accepted the request): fail CLOSED. Record the send in every
                # dedup store below as if it succeeded — a possible duplicate
                # is worse than a possible missed send on this one-shot lane —
                # then halt at the end of this iteration.
                send_unknown_exc = e

            # Shared crash-safe ledger BEFORE the local state save and Attio
            # stamp: a crash in between must not re-send on the next run. A
            # ledger-write failure still falls through to BOTH state writes —
            # they are then the only durable dedup records of this send — and
            # halts the batch after they land (raise at the end of this
            # iteration).
            try:
                mark_sent(c["id"], ASSOCIATION_LEDGER_STEP, today)
            except Exception as exc:
                ledger_exc = exc

            if send_unknown_exc is None:
                click.echo(f"  ✓ Sent to {c['email']} (Resend ID: {result.get('id', 'unknown')})")
                summary["sent"] += 1
                sent_state[c["id"]] = {
                    "sent_at": datetime.now(UTC).isoformat(),
                    "email": c["email"],
                    "resend_id": result.get("id", ""),
                }
            else:
                click.echo(
                    f"  ? Send outcome UNKNOWN for {c['email']} — recording as "
                    f"sent (fail closed)"
                )
                sent_state[c["id"]] = {
                    "sent_at": datetime.now(UTC).isoformat(),
                    "email": c["email"],
                    "resend_id": "",
                    "send_outcome_unknown": True,
                }

        # --- ledger-save domain --------------------------------------------
        # A save failure must NOT suppress the Attio stamp — with the local
        # file unwritable the stamp may be the only durable record.  Emit a
        # local_ledger_save_failed audit event so the operator can see the
        # gap, and proceed to _stamp_attio_post_send unconditionally.
        save_ok = True
        try:
            _save_sent_state(sent_state)
        except Exception as save_exc:
            save_ok = False
            context_msg = (
                "while repairing state for a prior-run send (the shared "
                "sent-ledger still holds this send; the local file will be "
                "repaired on a later run)"
                if is_repair
                else "after a send — the shared ledger and/or Attio stamp are "
                "now the durable records of it"
            )
            print(
                f"[association_outreach] WARNING: local ledger save FAILED "
                f"{context_msg}. contact_id={c['id']!r} email={c['email']!r}: "
                f"{type(save_exc).__name__}: {save_exc}",
                file=sys.stderr,
            )
            _audit_best_effort(
                audit_logger,
                "local_ledger_save_failed",
                contact_id=c["id"],
                email=c["email"],
                exc_type=type(save_exc).__name__,
                exc_msg=str(save_exc)[:500],
            )

        # --- Attio stamp domain -------------------------------------------
        # Phase 6 write-side: stamp Attio so cross-machine authority is
        # current. Persist the local dedup records first, then halt if the
        # stamp failed; another machine cannot see those local records.
        stamp_ok: bool | None = None
        if attio is not None and writer is not None:
            stamp_ok = _stamp_attio_post_send(
                attio=attio,
                writer=writer,
                contact=c,
                audit_logger=audit_logger,
            )

        if send_unknown_exc is not None or ledger_exc is not None or stamp_ok is False:
            # Halt the batch loudly, reporting ONLY the dedup records that
            # actually landed — under the disk-full failures that trigger this
            # path, the local save and Attio stamp often fail too.
            surviving = [
                name
                for name, ok in (
                    ("shared sent-ledger", ledger_exc is None),
                    ("local sent-state", save_ok),
                    ("Attio outreach_channel stamp", stamp_ok is True),
                )
                if ok
            ]
            record_note = (
                f"Surviving dedup records for this send: {', '.join(surviving)}."
                if surviving
                else "NO dedup record of this send survives — the ledger, local "
                "state, and Attio stamp writes ALL failed. Record it manually "
                "before any re-run or it WILL be re-sent."
            )
            if send_unknown_exc is not None or ledger_exc is not None:
                _audit_best_effort(
                    audit_logger,
                    "send_outcome_unknown" if send_unknown_exc is not None
                    else "shared_ledger_write_failed",
                    contact_id=c["id"],
                    email=c["email"],
                    ledger_write_ok=ledger_exc is None,
                    local_save_ok=save_ok,
                    attio_stamp_ok=stamp_ok,
                )
            if send_unknown_exc is not None:
                raise RuntimeError(
                    f"association email to {c['email']} MAY have been delivered "
                    f"— the send errored after the request may have reached "
                    f"Resend ({send_unknown_exc!r}). Failing closed: the "
                    f"contact was recorded as sent. {record_note} Check the "
                    f"Resend dashboard for {c['email']} before removing those "
                    f"records and re-running. Halting the batch."
                ) from send_unknown_exc
            if ledger_exc is not None:
                raise RuntimeError(
                    f"association email WAS sent to {c['email']} but could NOT be "
                    f"recorded in the shared sent-ledger "
                    f"(~/.outbound-agent/email_sent.json): {ledger_exc!r}. "
                    f"{record_note} Halting the batch — repair the ledger before "
                    f"re-running."
                ) from ledger_exc
            raise RuntimeError(
                f"association email for {c['email']} was already sent, but its "
                f"Attio outreach_channel stamp FAILED. {record_note} Halting "
                f"the batch — repair the Attio stamp before running this lane "
                f"from another machine."
            )

    click.echo("\n--- Association Outreach Summary ---")
    click.echo(f"Sent:    {summary['sent']}")
    click.echo(f"Errors:  {summary['errors']}")
    click.echo(f"Skipped: {summary['skipped']}")
    return summary
