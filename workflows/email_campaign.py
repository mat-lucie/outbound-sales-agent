"""Email campaign workflow: import contacts, run daily drip, anti-collision."""

import csv
import os
from datetime import date

import click
import httpx

from clients.attio import AttioClient
from clients.resend_client import ResendClient
from models.email_campaign import (
    ACTIVE_STAGES,
    EMAIL_DAILY_CAP,
    EMAIL_FROM,
    EMAIL_REPLY_TO,
    NEXT_STAGE,
    EmailStage,
    EmailStep,
    detect_language,
    get_email_template,
    personalize_email,
)
from models.pipeline import PipelineStage
from workflows.content_guard import assert_content_replaced
from workflows.cross_channel_suppression import build_suppression_set
from workflows.daily_check_helpers import (
    _assert_email_not_blank,
    _assert_email_renderable,
)
from workflows.email_compliance import (
    already_sent,
    append_footer,
    assert_email_compliance_ready,
    list_unsubscribe_header,
    mark_sent,
)
from workflows.email_lane_gate import assert_email_lane_enabled
from workflows.email_send_guard import verify_email_send_preconditions
from workflows.email_sequencer import get_pending_email, is_weekday

# LinkedIn pipeline stages that count as "active" for anti-collision
_LINKEDIN_ACTIVE_STAGES = {
    PipelineStage.CONNECTION_SENT.value,
    PipelineStage.ACCEPTED.value,
    PipelineStage.DM1_SENT.value,
    PipelineStage.DM2_SENT.value,
    PipelineStage.DM3_SENT.value,
}


def _domain_from_email(email: str) -> str:
    """Extract domain from an email address."""
    return email.split("@")[-1].lower() if "@" in email else ""


def _build_linkedin_collision_set(attio: AttioClient) -> set[tuple[str, str, str]]:
    """Build a set of (first_name_lower, last_name_lower, domain) from the LinkedIn pipeline.

    Used for anti-collision: skip email contacts that are active in LinkedIn outreach.
    """
    list_id = os.environ.get("ATTIO_LIST_ID", "")
    entries = attio.query_list_entries(list_id=list_id)
    collision_set: set[tuple[str, str, str]] = set()

    for entry in entries:
        attrs = AttioClient.parse_entry(entry)
        if attrs["stage"] not in _LINKEDIN_ACTIVE_STAGES:
            continue

        record = attio.get_person(attrs["record_id"])
        if not record:
            continue

        name, company, _, _, _ = attio.extract_record_info(record)
        # PR-14 (B-PD-006): name is None when the Attio record is
        # missing OR the name field is unset. Pre-PR-14 returned the
        # literal "Unknown" string; consumers now use `is None`.
        if name is None:
            continue

        parts = name.lower().split()
        first = parts[0] if parts else ""
        last = parts[-1] if len(parts) > 1 else ""

        # Extract domain from email if available
        values = record.get("values", {})
        email_data = values.get("email_addresses", [])
        domain = ""
        if email_data and isinstance(email_data, list):
            addr = email_data[0]
            email_str = addr.get("email_address", "") if isinstance(addr, dict) else str(addr)
            domain = _domain_from_email(email_str)

        if not domain:
            # Fallback: use company name as-is (lowercase).
            # PR-14 fold-in (silent-failure-hunter CRITICAL #2):
            # `company` may be None post-PR-14 (RecordCache "Unknown" →
            # None). Guard with `(company or "")` — a None company
            # produces an empty-string domain, which the `if first`
            # check below still accepts but the collision-set entry
            # becomes (first, last, "") — operationally a no-op
            # against the real-email path, which is the correct
            # fail-safe behavior.
            domain = (company or "").lower()

        if first:
            collision_set.add((first, last, domain))

    return collision_set


def _is_collision(
    first_name: str,
    last_name: str,
    domain: str,
    collision_set: set[tuple[str, str, str]],
) -> bool:
    """Check if a contact matches someone in the LinkedIn pipeline."""
    return (first_name.lower(), last_name.lower(), domain.lower()) in collision_set


def import_contacts(
    attio: AttioClient,
    csv_path: str,
    dry_run: bool = False,
) -> dict:
    """Set email_campaign_stage=queued on existing Attio contacts from CSV.

    Looks up each contact by email. If found, sets their campaign stage.
    Skips rows where name is 'Unknown' or first_name is empty.
    Does NOT create new records — contacts must already exist in Attio.

    Returns summary dict.
    """
    queued = 0
    skipped = 0
    not_found = 0
    skipped_reasons: list[str] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip()
            first_name = (row.get("first_name") or "").strip()
            name = (row.get("name") or "").strip()

            if not email:
                skipped += 1
                skipped_reasons.append(f"No email: {name or 'Unknown'}")
                continue

            # PR-14 (B-PD-006): legacy `name == "Unknown"` check
            # preserved as a string compare here because the value comes
            # from `row.get("name")` (a CSV/preview row), not from
            # RecordCache.get (which now returns None). CSV rows still
            # carry the literal "Unknown" when an upstream component
            # serialized it that way. Treat both forms as missing.
            if not name or name == "Unknown" or not first_name:
                skipped += 1
                skipped_reasons.append(f"No name: {email}")
                continue

            last_name = (row.get("last_name") or "").strip()

            if dry_run:
                click.echo(f"  [DRY RUN] Would queue: {first_name} {last_name} <{email}>")
                queued += 1
                continue

            # Find existing person by email
            results = attio.search_people(
                filter_={"email_addresses": email},
                limit=1,
            )
            if not results:
                not_found += 1
                click.echo(f"  [NOT FOUND] {first_name} {last_name} <{email}>")
                continue

            record_id = results[0].get("id", {}).get("record_id", "")
            attio.update_person(record_id, {
                "email_campaign_stage": EmailStage.QUEUED.value,
            })
            queued += 1
            click.echo(f"  Queued: {first_name} {last_name} <{email}>")

    click.echo(f"\nQueued: {queued}, Skipped: {skipped}, Not found: {not_found}")
    for reason in skipped_reasons:
        click.echo(f"  Skip: {reason}")

    return {"queued": queued, "skipped": skipped, "not_found": not_found}


def _update_person_with_resend_id_fallback(
    attio: AttioClient, record_id: str, attributes: dict
) -> None:
    """PATCH the person, dropping `email_last_resend_id` on a 400 (PR-243).

    Forward-compat (mirrors detect_responses' Phase-1 fallback): until the
    operator provisions the `email_last_resend_id` attribute
    (``scripts/setup_attio_schema.py --feature phase06``), writes including
    it 400. The stage advance MUST still land — a stranded stage after a
    successful Resend send means a duplicate email next run — so we retry
    without the optional key.
    """
    try:
        attio.update_person(record_id, attributes)
    except httpx.HTTPStatusError as exc:
        if (
            exc.response is not None
            and exc.response.status_code == 400
            and "email_last_resend_id" in attributes
        ):
            fallback = {k: v for k, v in attributes.items() if k != "email_last_resend_id"}
            attio.update_person(record_id, fallback)
            # Loud on purpose (no-silent-fallbacks): the id is dropped for
            # good on this send, and repeated firings mean the phase06
            # schema hasn't been provisioned against the workspace.
            click.echo(
                "  ⚠ email_last_resend_id dropped (Attio 400 — attribute "
                "missing?). Run scripts/setup_attio_schema.py --feature "
                "phase06 to provision it.",
                err=True,
            )
        else:
            raise


def run_email_daily(
    attio: AttioClient,
    resend: ResendClient | None,
    dry_run: bool = False,
    auto_confirm: bool = False,
    force_weekend: bool = False,
) -> dict:
    """Run the daily email campaign: stagger new sends + advance sequence.

    1. Skip if weekend (unless force_weekend)
    2. Build anti-collision set from LinkedIn pipeline
    3. Query Attio for contacts in active email stages
    4. Stagger: send Email 1 to up to EMAIL_DAILY_CAP queued contacts
    5. Sequence: send Email 2/3 to eligible contacts
    6. Update Attio after each send

    Returns summary dict.
    """
    # Kill switch first — before the weekday return, before any CRM read.
    # A disarmed live run must abort loudly, not exit 0 as "weekend".
    assert_email_lane_enabled("email-daily", dry_run=dry_run)

    today = date.today()

    if not is_weekday(today) and not force_weekend:
        click.echo("Weekend — no emails sent. Use --force-weekend to override.")
        return {"sent": 0, "reason": "weekend"}

    # Shipped-placeholder send gate (no-op on dry_run): a fresh install ships
    # neutral placeholder email copy with a sentinel; refuse to send it live.
    assert_content_replaced(dry_run=dry_run, filenames=("emails.json",))

    # CAN-SPAM send-gate (no-op on dry_run): refuse to send live without a
    # configured physical postal address.
    assert_email_compliance_ready(dry_run=dry_run)

    # Build anti-collision set
    click.echo("Building LinkedIn anti-collision set...")
    collision_set = _build_linkedin_collision_set(attio)
    click.echo(f"  {len(collision_set)} active LinkedIn contacts loaded.")

    # Cross-channel suppression (§3.1 hard red line): anyone marked
    # suppress_re_engagement, classified negative/defensive, or parked in
    # NOT_INTERESTED/DEFENSIVE_HOLD must NOT receive the email drip either.
    click.echo("Building cross-channel suppression set...")
    suppression_set = build_suppression_set(attio)
    click.echo(f"  {len(suppression_set)} suppressed records loaded.")

    # Query all people with active email campaign stages
    company_cache: dict[str, tuple[str, str]] = {}  # target_record_id -> (company_name, country_code)
    contacts: list[dict] = []
    for stage in ACTIVE_STAGES:
        results = attio.search_people(
            filter_={"email_campaign_stage": stage.value},
            limit=50_000,
            fail_if_truncated=True,
        )
        for record in results:
            values = record.get("values", {})
            record_id = record.get("id", {}).get("record_id", "")
            # A record with no id can't have its stage advanced (it would
            # re-send forever) and can't be matched against the suppression set
            # ("" is never a suppressed id) — skip it rather than send blind.
            if not record_id:
                continue

            name_data = values.get("name", [])
            first_name = name_data[0].get("first_name", "") if name_data else ""
            last_name = name_data[0].get("last_name", "") if name_data else ""

            email_data = values.get("email_addresses", [])
            email = ""
            if email_data:
                e = email_data[0]
                email = e.get("email_address", "") if isinstance(e, dict) else str(e)

            # Read campaign fields
            stage_data = values.get("email_campaign_stage", [])
            current_stage_str = ""
            if stage_data and isinstance(stage_data, list):
                item = stage_data[0]
                current_stage_str = item.get("value", item) if isinstance(item, dict) else str(item)

            last_sent_data = values.get("email_campaign_last_sent", [])
            last_sent_str = ""
            if last_sent_data and isinstance(last_sent_data, list):
                item = last_sent_data[0]
                last_sent_str = item.get("value", item) if isinstance(item, dict) else str(item)

            # Extract country code from person's primary_location
            person_country = ""
            loc_data = values.get("primary_location", [])
            if loc_data and isinstance(loc_data, list):
                loc = loc_data[0]
                if isinstance(loc, dict):
                    person_country = loc.get("country_code", "") or ""

            # Extract company name and country (company field is a record-reference)
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
                        except httpx.HTTPStatusError:
                            pass
                        company_cache[cid] = (company, company_country)

            # Language: person country → company country → domain TLD
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
                "stage": current_stage_str,
                "last_sent": last_sent_str,
            })

    click.echo(f"Found {len(contacts)} contacts in active email stages.")

    # Drop suppressed (cross-channel red line) + unsubscribed contacts before
    # any send. The ACTIVE_STAGES query already excludes UNSUBSCRIBED; the
    # explicit check here is belt-and-suspenders.
    suppressed_skipped = 0
    _kept: list[dict] = []
    for c in contacts:
        if c["record_id"] in suppression_set or c["stage"] == EmailStage.UNSUBSCRIBED.value:
            suppressed_skipped += 1
            continue
        _kept.append(c)
    if suppressed_skipped:
        click.echo(f"  Suppressed/unsubscribed: {suppressed_skipped} skipped.")
    contacts = _kept

    if not contacts:
        click.echo("No contacts to email today.")
        return {"sent": 0, "collisions": 0, "sequenced": 0, "suppressed": suppressed_skipped}

    # Separate queued vs in-sequence
    queued = [c for c in contacts if c["stage"] == EmailStage.QUEUED.value]
    in_sequence = [c for c in contacts if c["stage"] in {EmailStage.EMAIL1_SENT.value, EmailStage.EMAIL2_SENT.value}]

    # Phase 1: Stagger — up to EMAIL_DAILY_CAP new contacts
    email1_batch = queued[:EMAIL_DAILY_CAP]

    # Phase 2: Sequence — check timing for email2/email3
    sequence_batch: list[tuple[dict, EmailStep]] = []
    for contact in in_sequence:
        try:
            current_stage = EmailStage(contact["stage"])
        except ValueError:
            continue

        last_sent = None
        if contact["last_sent"]:
            last_sent = date.fromisoformat(str(contact["last_sent"])[:10])

        pending = get_pending_email(current_stage, last_sent, today)
        if pending:
            sequence_batch.append((contact, pending))

    total_to_send = len(email1_batch) + len(sequence_batch)
    collisions = 0
    sent = 0
    send_guard_skipped = 0

    if total_to_send == 0:
        click.echo("No emails due today.")
        return {"sent": 0, "collisions": 0}

    # Pre-flight: a blank template is systemic (per step+language, not per
    # contact), so validate every distinct template the batch will use BEFORE
    # any send. This halts at zero sends — mirroring the DM batch guard —
    # instead of mid-loop after earlier contacts already shipped. The
    # per-send guard below remains as backstop for render-level blanks.
    for language in sorted({c["language"] for c in email1_batch}):
        template = get_email_template(EmailStep.EMAIL1, language)
        _assert_email_not_blank(
            template["subject"], template["body_html"],
            f"the {language} template", "email1",
        )
    sequence_templates = {(step, c["language"]) for c, step in sequence_batch}
    for step, language in sorted(sequence_templates, key=lambda p: (p[0].value, p[1])):
        template = get_email_template(step, language)
        _assert_email_not_blank(
            template["subject"], template["body_html"],
            f"the {language} template", step.value,
        )

    click.echo(f"\nReady to send: {len(email1_batch)} new (Email 1) + {len(sequence_batch)} sequenced")

    if not dry_run and not auto_confirm and not click.confirm(f"Send {total_to_send} emails?"):
        click.echo("Cancelled.")
        return {"sent": 0, "cancelled": True}

    if not dry_run and resend is None:
        raise RuntimeError("ResendClient not configured")

    today_str = today.isoformat()

    # Send Email 1 to new contacts
    for contact in email1_batch:
        if _is_collision(contact["first_name"], contact["last_name"], contact["domain"], collision_set):
            click.echo(f"  [SKIP] Collision: {contact['first_name']} {contact['last_name']} ({contact['domain']})")
            collisions += 1
            continue

        template = get_email_template(EmailStep.EMAIL1, contact["language"])
        subject = personalize_email(template["subject"], contact["first_name"], contact["company"])
        body = personalize_email(template["body_html"], contact["first_name"], contact["company"])

        _assert_email_renderable(subject, body, contact["email"], "email1")

        stage_update = {
            "email_campaign_stage": EmailStage.EMAIL1_SENT.value,
            "email_campaign_last_sent": today_str,
            "email_campaign_started": today_str,
        }

        if not dry_run:
            guard = verify_email_send_preconditions(attio, contact["record_id"], contact["stage"])
            if not guard.allowed:
                send_guard_skipped += 1
                click.echo(f"  [send_guard] email1 skipped for {contact['email']}: {guard.reason}", err=True)
                continue
        if dry_run:
            click.echo(f"  [DRY RUN] email1 | {contact['first_name']} {contact['last_name']} | {contact['email']} | {contact['company']} | {contact['language']} | {subject}")
        elif already_sent(contact["record_id"], "email1"):
            # Sent on a prior (crashed) run today: do NOT re-send. Repair the
            # stage write the crash missed so tomorrow's run won't re-send either.
            attio.update_person(contact["record_id"], stage_update)
            click.echo(f"  [SKIP] email1 already sent today (stage repaired) -> {contact['email']}")
            continue
        else:
            html, text = append_footer(body)
            # Non-dry-run path: resend is guaranteed non-None by the guard at
            # the top of this function (raises RuntimeError if unset).
            assert resend is not None
            send_resp = resend.send_email(
                to=contact["email"],
                subject=subject,
                html=html,
                from_address=EMAIL_FROM,
                reply_to=EMAIL_REPLY_TO,
                text=text,
                headers=list_unsubscribe_header(),
            )
            _record_send_or_raise(contact["record_id"], "email1", contact["email"], today, send_resp)
            # Capture the Resend message id (PR-243, forward-compat thread
            # matching); falls back without it until the attribute exists.
            _update_person_with_resend_id_fallback(
                attio,
                contact["record_id"],
                {**stage_update, "email_last_resend_id": (send_resp or {}).get("id", "")},
            )
            click.echo(f"  Sent Email 1 -> {contact['email']}")

        sent += 1

    # Send Email 2/3 to sequenced contacts
    for contact, step in sequence_batch:
        if _is_collision(contact["first_name"], contact["last_name"], contact["domain"], collision_set):
            click.echo(f"  [SKIP] Collision: {contact['first_name']} {contact['last_name']} ({contact['domain']})")
            collisions += 1
            continue

        template = get_email_template(step, contact["language"])
        subject = personalize_email(template["subject"], contact["first_name"], contact["company"])
        body = personalize_email(template["body_html"], contact["first_name"], contact["company"])

        _assert_email_renderable(subject, body, contact["email"], step.value)

        next_stage = NEXT_STAGE[step]
        # After Email 3, mark as completed
        final_stage = EmailStage.COMPLETED if step == EmailStep.EMAIL3 else next_stage
        stage_update = {
            "email_campaign_stage": final_stage.value,
            "email_campaign_last_sent": today_str,
        }

        if not dry_run:
            guard = verify_email_send_preconditions(attio, contact["record_id"], contact["stage"])
            if not guard.allowed:
                send_guard_skipped += 1
                click.echo(f"  [send_guard] {step.value} skipped for {contact['email']}: {guard.reason}", err=True)
                continue
        if dry_run:
            click.echo(f"  [DRY RUN] {step.value} | {contact['first_name']} {contact['last_name']} | {contact['email']} | {contact['company']} | {contact['language']} | {subject}")
        elif already_sent(contact["record_id"], step.value):
            attio.update_person(contact["record_id"], stage_update)
            click.echo(f"  [SKIP] {step.value} already sent today (stage repaired) -> {contact['email']}")
            continue
        else:
            html, text = append_footer(body)
            # Non-dry-run path: resend is guaranteed non-None by the guard at
            # the top of this function (raises RuntimeError if unset).
            assert resend is not None
            send_resp = resend.send_email(
                to=contact["email"],
                subject=subject,
                html=html,
                from_address=EMAIL_FROM,
                reply_to=EMAIL_REPLY_TO,
                text=text,
                headers=list_unsubscribe_header(),
            )
            _record_send_or_raise(contact["record_id"], step.value, contact["email"], today, send_resp)
            # Capture the Resend message id (PR-243, forward-compat thread
            # matching); falls back without it until the attribute exists.
            _update_person_with_resend_id_fallback(
                attio,
                contact["record_id"],
                {**stage_update, "email_last_resend_id": (send_resp or {}).get("id", "")},
            )
            click.echo(f"  Sent {step.value} -> {contact['email']}")

        sent += 1

    click.echo(f"\nDone. Sent: {sent}, Collisions skipped: {collisions}, Suppressed: {suppressed_skipped}")

    return {
        "sent": sent,
        "collisions": collisions,
        "email1": len(email1_batch) - collisions,
        "send_guard_skipped": send_guard_skipped,
        "suppressed": suppressed_skipped,
    }


def _record_send_or_raise(
    record_id: str, step: str, email: str, day: date, send_resp: dict | None
) -> None:
    """Persist the sent-ledger entry immediately after a successful Resend send.

    If the ledger write itself fails, re-raise with an UNMISSABLE message: the
    email already went out, so the default "just re-run" instinct would
    double-send. The raised error names the recipient and Resend id and tells
    the operator not to blindly re-run.
    """
    try:
        mark_sent(record_id, step, day)
    except Exception as exc:
        resend_id = (send_resp or {}).get("id", "?")
        raise RuntimeError(
            f"{step} email WAS sent to {email} (Resend id {resend_id}) but could "
            f"NOT be recorded in the sent-ledger: {exc!r}. Do NOT blindly re-run "
            f"— the CRM stage may also be un-advanced, and a re-run without first "
            f"repairing the ledger WILL re-send this email. Fix the ledger "
            f"(~/.outbound-agent/email_sent.json) or record this send manually first."
        ) from exc
