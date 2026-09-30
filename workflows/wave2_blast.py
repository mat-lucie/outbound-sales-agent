"""Wave-2 blast: one-off re-engage email for the 94 stalled drip-campaign contacts.

Targets people whose `email_campaign_stage` is `email1_sent` or `email2_sent` —
i.e. mid-sequence contacts who went idle when the daily cron stopped running.
Sends the `wave2` template (en/es/pt) and advances them to `wave2_sent` so
`email-daily` no longer picks them up.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import TYPE_CHECKING

import click
import httpx

from workflows.email_send_guard import verify_email_send_preconditions

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from clients.resend_client import ResendClient
    from workflows.audit import AuditLogger
from models.email_campaign import (
    CONTENT_DIR,
    EmailStage,
    detect_language,
    get_email_from,
    get_reply_to,
    personalize_email,
)
from workflows.content_guard import assert_content_replaced
from workflows.cross_channel_suppression import build_suppression_set
from workflows.daily_check_helpers import (
    _assert_email_not_blank,
    _assert_email_renderable,
)
from workflows.email_campaign import (
    _build_linkedin_collision_set,
    _domain_from_email,
    _is_collision,
    _record_send_or_raise,
)
from workflows.email_compliance import (
    already_sent,
    append_footer,
    assert_email_compliance_ready,
    list_unsubscribe_header,
    mark_sent,
)
from workflows.email_lane_gate import assert_email_lane_enabled
from workflows.email_sequencer import is_weekday

logger = logging.getLogger(__name__)

WAVE2_STAGE = "wave2_sent"
# Sent-ledger step key — must stay identical between the already_sent() read
# and the _record_send_or_raise() write, or the crash-window guard is a no-op.
WAVE2_LEDGER_STEP = "wave2"
WAVE2_UNKNOWN_LEDGER_STEP = "wave2_unknown_delivery"
WAVE2_SOURCE_STAGES = (EmailStage.EMAIL1_SENT, EmailStage.EMAIL2_SENT)


def _load_wave2_template(language: str) -> dict:
    with open(CONTENT_DIR / "emails.json") as f:
        templates = json.load(f)
    return templates["wave2"][language]


def run_wave2_blast(
    attio: AttioClient,
    resend: ResendClient | None,
    dry_run: bool = False,
    auto_confirm: bool = False,
    force_weekend: bool = False,
    max_n: int = 100,
    audit_logger: AuditLogger | None = None,
) -> dict:
    """Send the wave-2 re-engage email to mid-sequence stalled contacts."""
    # Kill switch first — before the weekday return, before any Attio read.
    # A disarmed live run must abort loudly, not exit 0 as "weekend".
    assert_email_lane_enabled("email-wave2", dry_run=dry_run)

    assert_content_replaced(dry_run=dry_run, filenames=("emails.json",))
    today = date.today()

    if not is_weekday(today) and not force_weekend:
        click.echo("Weekend — no emails sent. Use --force-weekend to override.")
        return {"sent": 0, "reason": "weekend"}

    # CAN-SPAM send-gate (no-op on dry_run): refuse to send live without a
    # configured physical postal address.
    assert_email_compliance_ready(dry_run=dry_run)

    click.echo("Building LinkedIn anti-collision set...")
    collision_set = _build_linkedin_collision_set(attio)
    click.echo(f"  {len(collision_set)} active LinkedIn contacts loaded.")

    # PR-37 (§3.1 hardest red line + §3.16): cross-channel suppression set.
    # Anyone marked suppress_re_engagement, classified negative/defensive, or
    # parked in NOT_INTERESTED/DEFENSIVE_HOLD must NOT receive a wave-2 email.
    click.echo("Building cross-channel suppression set...")
    suppression_set = build_suppression_set(attio)
    click.echo(f"  {len(suppression_set)} suppressed records loaded.")

    company_cache: dict[str, tuple[str, str]] = {}
    contacts: list[dict] = []
    for stage in WAVE2_SOURCE_STAGES:
        results = attio.search_people(
            filter_={"email_campaign_stage": stage.value},
            limit=50_000,
            fail_if_truncated=True,
        )
        for record in results:
            values = record.get("values", {})
            record_id = record.get("id", {}).get("record_id", "")

            name_data = values.get("name", [])
            first_name = name_data[0].get("first_name", "") if name_data else ""
            last_name = name_data[0].get("last_name", "") if name_data else ""

            email_data = values.get("email_addresses", [])
            email = ""
            if email_data:
                e = email_data[0]
                email = e.get("email_address", "") if isinstance(e, dict) else str(e)

            if not email or not first_name:
                continue

            person_country = ""
            ploc = values.get("primary_location", [])
            if ploc and isinstance(ploc, list):
                loc = ploc[0]
                if isinstance(loc, dict):
                    person_country = loc.get("country_code", "") or ""

            company = ""
            company_country = ""
            company_data = values.get("company", [])
            if company_data and isinstance(company_data, list):
                ref = company_data[0]
                if isinstance(ref, dict) and ref.get("target_record_id"):
                    cid = ref["target_record_id"]
                    if cid in company_cache:
                        company, company_country = company_cache[cid]
                    else:
                        # Exception discipline mirrors FU-3 (PR #111): a
                        # company-fetch failure for one record must NOT
                        # abort the blast, but programmer errors must
                        # not be demoted to silent skips either.
                        try:
                            cr = attio._request("GET", f"/objects/companies/records/{cid}")
                            cr_data = cr.get("data", cr)
                            cr_vals = cr_data.get("values", {})
                            cr_name = cr_vals.get("name", [])
                            if cr_name:
                                company = cr_name[0].get("value", "")
                            cr_loc = cr_vals.get("primary_location", [])
                            if cr_loc and isinstance(cr_loc, list):
                                company_country = cr_loc[0].get("country_code", "") or ""
                        except (SystemExit, KeyboardInterrupt):
                            # Never demote interpreter-control exceptions.
                            raise
                        except (ImportError, TypeError, AttributeError):
                            # Programmer errors (missing module, wrong call
                            # shape, renamed attribute) signal an actual
                            # bug — silently swallowing them as "transient
                            # company-fetch failure" would hide real
                            # regressions. Surface them so CI / monitoring
                            # sees the crash.
                            raise
                        except Exception as err:  # noqa: BLE001
                            # Transient runtime errors (Attio 5xx, network
                            # blip) — log with the failing record_id so an
                            # operator can audit blank-company rows in
                            # post-blast review. Do NOT cache the failed
                            # result (see else: clause below) — caching an
                            # empty-string default would poison the cache
                            # and silently ship blank-company emails for
                            # every subsequent person referencing the same
                            # company, with only ONE warning logged for N
                            # affected rows.
                            logger.warning(
                                "wave2_blast: company-fetch failed for record_id=%r, "
                                "person record_id=%r: %s",
                                cid, record_id, err,
                            )
                        else:
                            # Only cache successful fetches. Transient
                            # failures re-trigger the Attio call for the
                            # next person referencing the same company,
                            # which yields an independent warning log per
                            # affected row (correct oncall visibility) and
                            # naturally recovers once the infra is healthy.
                            company_cache[cid] = (company, company_country)

            domain = _domain_from_email(email)
            country_code = person_country or company_country
            contacts.append({
                "record_id": record_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "domain": domain,
                "company": company,
                "language": detect_language(domain, country_code),
                "stage": stage.value,
            })

    click.echo(f"Found {len(contacts)} stalled contacts (email1_sent + email2_sent).")

    if not contacts:
        click.echo("No contacts to wave-2 today.")
        return {"sent": 0, "collisions": 0}

    if max_n and len(contacts) > max_n:
        click.echo(f"Capping send at --max {max_n}.")
        contacts = contacts[:max_n]

    # Pre-flight: a blank template is systemic (per language), so validate
    # every distinct template the blast will use BEFORE any send — halting at
    # zero sends instead of mid-loop after earlier contacts already shipped.
    # The per-send guard below remains as backstop for render-level blanks.
    send_candidates = [
        c for c in contacts
        if dry_run or (
            not already_sent(c["record_id"], WAVE2_LEDGER_STEP)
            and not already_sent(c["record_id"], WAVE2_UNKNOWN_LEDGER_STEP)
        )
    ]
    for language in sorted({c["language"] for c in send_candidates}):
        template = _load_wave2_template(language)
        _assert_email_not_blank(
            template["subject"], template["body_html"],
            f"the {language} template", "wave2",
        )

    click.echo(f"\nReady to send wave-2 to {len(contacts)} contacts.")

    if not dry_run and not auto_confirm and not click.confirm(f"Send {len(contacts)} wave-2 emails?"):
        click.echo("Cancelled.")
        return {"sent": 0, "cancelled": True}

    if not dry_run and resend is None:
        raise RuntimeError("ResendClient not configured")

    today_str = today.isoformat()
    sent = 0
    collisions = 0
    suppressed = 0
    send_guard_skipped = 0
    already_sent_repaired = 0
    repair_failed = 0
    repair_failed_ids: list[str] = []

    for contact in contacts:
        if _is_collision(contact["first_name"], contact["last_name"], contact["domain"], collision_set):
            click.echo(f"  [SKIP] Collision: {contact['first_name']} {contact['last_name']} ({contact['domain']})")
            collisions += 1
            continue

        if contact["record_id"] in suppression_set:
            click.echo(
                f"  [SKIP] Suppressed (cross-channel): {contact['first_name']} {contact['last_name']}"
            )
            suppressed += 1
            continue

        unknown_delivery = not dry_run and already_sent(
            contact["record_id"], WAVE2_UNKNOWN_LEDGER_STEP
        )
        repair_only = not dry_run and already_sent(
            contact["record_id"], WAVE2_LEDGER_STEP
        )
        if unknown_delivery:
            raise RuntimeError(
                f"Wave-2 delivery for {contact['email']} has an unknown outcome. "
                "Check Resend and reconcile the sent ledger/Attio stage before rerunning."
            )
        subject = body = ""
        if not repair_only:
            template = _load_wave2_template(contact["language"])
            subject = personalize_email(template["subject"], contact["first_name"], contact["company"])
            body = personalize_email(template["body_html"], contact["first_name"], contact["company"])
            _assert_email_renderable(subject, body, contact["email"], "wave2")

        stage_update = {
            "email_campaign_stage": WAVE2_STAGE,
            "email_campaign_last_sent": today_str,
        }

        if dry_run:
            click.echo(
                f"  [DRY RUN] wave2 | {contact['first_name']} {contact['last_name']} | "
                f"{contact['email']} | {contact['company']} | {contact['language']} | {subject}"
            )
        elif repair_only:
            # Sent on a prior (possibly crashed) run: do NOT re-send. Repair the
            # stage write the crash missed so the next run won't re-send either.
            # This covers the window the send-guard can't — the send succeeded
            # but the Attio stage PATCH crashed, leaving the stage unchanged.
            # Guard the repair too: the stage may have moved since the batch
            # snapshot (reply detected, operator ran email-unsubscribe — that
            # command takes no lock), and a blind PATCH would overwrite the
            # terminal stage with wave2_sent, silently reverting an opt-out.
            guard = verify_email_send_preconditions(
                attio=attio,
                record_id=contact["record_id"],
                expected_email_stage=contact["stage"],
                audit_logger=audit_logger,
            )
            if not guard.allowed:
                send_guard_skipped += 1
                click.echo(
                    f"  [send_guard] wave2 stage-repair skipped for {contact['email']}: "
                    f"reason={guard.reason!r} "
                    f"(expected stage={contact['stage']!r}, "
                    f"actual stage={guard.actual_stage!r})",
                    err=True,
                )
                continue
            # Best-effort CRM catch-up — the ledger is the durable double-send
            # guard, so a failing PATCH (e.g. 404 after a record merge) must
            # not abort the rest of the blast.
            try:
                attio.update_person(contact["record_id"], stage_update)
            except (SystemExit, KeyboardInterrupt):
                raise
            except (ImportError, TypeError, AttributeError):
                # Programmer errors surface — same discipline as the
                # company-fetch block above.
                raise
            except Exception as err:  # noqa: BLE001
                repair_failed += 1
                repair_failed_ids.append(contact["record_id"])
                click.echo(
                    f"  [REPAIR FAILED] wave2 already sent to {contact['email']} "
                    f"but the stage PATCH failed "
                    f"(record_id={contact['record_id']!r}): {err}. No re-send "
                    f"risk (ledger holds), but the record stays at "
                    f"{contact['stage']!r} and will re-enter this branch next "
                    f"run — check for an archived/merged record.",
                    err=True,
                )
            else:
                already_sent_repaired += 1
                click.echo(f"  [SKIP] wave2 already sent (stage repaired) -> {contact['email']}")
            continue
        else:
            # Phase 3 optimistic send guard: re-read email_campaign_stage from
            # Attio immediately before the irreversible Resend API call.
            guard = verify_email_send_preconditions(
                attio=attio,
                record_id=contact["record_id"],
                expected_email_stage=contact["stage"],
                audit_logger=audit_logger,
            )
            if not guard.allowed:
                send_guard_skipped += 1
                click.echo(
                    f"  [send_guard] wave2 skipped for {contact['email']}: "
                    f"reason={guard.reason!r} "
                    f"(expected stage={contact['stage']!r}, "
                    f"actual stage={guard.actual_stage!r})",
                    err=True,
                )
                continue
            html, text = append_footer(body)
            try:
                assert resend is not None
                send_resp = resend.send_email(
                    to=contact["email"],
                    subject=subject,
                    html=html,
                    from_address=get_email_from(),
                    reply_to=get_reply_to(),
                    text=text,
                    headers=list_unsubscribe_header(),
                )
            except (httpx.ConnectError, httpx.ConnectTimeout):
                # The request never reached Resend; no dedup record is needed.
                raise
            except Exception as exc:
                # A response/timeout/error after the POST may hide a real send.
                try:
                    mark_sent(contact["record_id"], WAVE2_UNKNOWN_LEDGER_STEP, today)
                except Exception as ledger_exc:
                    raise RuntimeError(
                        f"Wave-2 delivery outcome for {contact['email']} is unknown "
                        f"and the protective ledger write failed: {ledger_exc!r}. "
                        "Do NOT rerun; inspect Resend and repair the ledger manually."
                    ) from exc
                raise RuntimeError(
                    f"Wave-2 delivery outcome for {contact['email']} is unknown. "
                    "A protective ledger entry blocks reruns. Check Resend and "
                    "reconcile the sent ledger/Attio stage before continuing."
                ) from exc
            # Ledger BEFORE the CRM stage write: a crash between the provider
            # send and the PATCH must not re-send on the next run.
            _record_send_or_raise(
                contact["record_id"], WAVE2_LEDGER_STEP, contact["email"], today, send_resp
            )
            attio.update_person(contact["record_id"], stage_update)
            click.echo(f"  Sent wave2 -> {contact['email']}")

        sent += 1

    guard_note = f", Send-guard skipped: {send_guard_skipped}" if send_guard_skipped else ""
    repair_note = f", Already-sent repaired: {already_sent_repaired}" if already_sent_repaired else ""
    repair_fail_note = f", Repair FAILED: {repair_failed}" if repair_failed else ""
    click.echo(
        f"\nDone. Sent: {sent}, Collisions skipped: {collisions}, "
        f"Suppressed: {suppressed}{guard_note}{repair_note}{repair_fail_note}"
    )
    if repair_failed_ids:
        raise RuntimeError(
            "Wave-2 sent-ledger stage repair failed for Attio record IDs "
            f"{', '.join(repair_failed_ids)}. No resend was attempted for these records; "
            "repair their CRM stages before treating this batch as complete."
        )
    return {
        "sent": sent,
        "collisions": collisions,
        "suppressed": suppressed,
        "send_guard_skipped": send_guard_skipped,
        "already_sent_repaired": already_sent_repaired,
        "repair_failed": repair_failed,
    }
