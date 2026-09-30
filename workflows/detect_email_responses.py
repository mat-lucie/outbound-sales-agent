"""Phase 0.6: automated email response detection via Gmail (read-only, PR-243).

Email counterpart of Phase 0.5 (workflows/detect_responses.py, LinkedIn).
For every prospect with at least one outreach email sent
(``email_campaign_stage`` in REPLY_SCAN_STAGES), search Gmail for inbound
mail from their address since the campaign started, classify the newest
real reply with the shared LLM classifier, and:

  * flip ``email_campaign_stage`` to a terminal reply stage
    (negative -> email_not_interested, else email_responded) — terminal
    stages are outside ACTIVE_STAGES, so the sequencer stops emailing
    them with no further coordination;
  * stamp ``email_response_classification`` / ``email_response_received_at``
    / ``last_email_response_text`` on the person (via AttioWriter, §3.15
    registry-gated);
  * write a forensic CRM note and open an ``email_response_detected``
    Operator Review Queue row.

Write ordering is load-bearing: the STAGE FLIP lands first, the response
attrs (including ``email_response_received_at``, the idempotency marker)
second. A failed stage flip therefore leaves the marker unset and the
prospect retries cleanly next run; the reverse order would strand a
replied prospect in an active stage forever (marker set -> skipped, stage
active -> sequencer keeps emailing them). A failed attr write after a
landed stage flip only loses enrichment — the sequencer stop already
happened, and AttioWriter has DLQ'd + escalated the failure.

Provision the schema BEFORE the first wet run:
``python3 scripts/setup_attio_schema.py --feature phase06`` creates the two
new ``email_campaign_stage`` select options and the four new people
attributes; without it every write here 400s. (See GETTING_STARTED.md →
Email Response Detection.)

OFF BY DEFAULT: the engine no-ops unless a Gmail token exists (the daily
run passes ``gmail=None`` when ``GmailClient.from_credentials`` raises
``GmailCredentialsMissing``) — Gmail is an optional data source, not part
of the CRM contract.

Idempotency: a person with ``email_response_received_at`` already set is
skipped — one detection per prospect, ever. Bounces and auto-replies/OOO
never flip a stage (see clients/gmail.is_auto_generated).

Matching is by counterparty email address + date window: email sends
store no thread/message ids today (see people.email_last_resend_id for
the forward-looking capture). A reply sent from a different address is a
known v1 gap.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from functools import lru_cache

import click
import httpx

from clients.attio import AttioClient
from clients.attio_writer import (
    AttioError,
    AttioWriter,
    UnauthorizedAttioWriteError,
    WriteIntent,
)
from clients.gmail import GmailClient, is_auto_generated
from models.email_campaign import (
    EMAIL_CLASS_TO_STAGE,
    REPLY_SCAN_STAGES,
    EmailStep,
    detect_language,
    get_email_template,
    personalize_email,
)
from workflows.escalation import escalate
from workflows.escalation_schemas import MissingAttioCredentials
from workflows.response_classifier import classify_reply_llm

# Mirrors detect_responses.MAX_RESPONSE_TEXT_LEN (kept local so the two
# phases can diverge deliberately, not accidentally).
MAX_RESPONSE_TEXT_LEN = 1000

_WRITER_MODULE = "workflows.detect_email_responses.detect_email_responses"

_TAG_RE = re.compile(r"<[^>]+>")


def _empty_counts() -> dict:
    return {
        "scanned": 0,
        "detected": 0,
        "positive": 0,
        "question": 0,
        "neutral": 0,
        "negative": 0,
        "defensive": 0,
        "auto_generated_skipped": 0,
        "already_processed": 0,
        "no_start_timestamp": 0,
        "gmail_errors": 0,
        "attio_update_failures": 0,
        "classifier_llm": 0,
        "classifier_keyword": 0,
    }


def _first_value(values: dict, slug: str) -> str:
    """Scalar value of a CRM object-record attribute, '' when unset."""
    return str(AttioClient.object_record_first_value(values, slug) or "")


@lru_cache(maxsize=8)
def _reconstruct_opener(language: str) -> str:
    """Rebuild the email-1 text as classifier context (cached per language).

    Which exact step the prospect replied to doesn't matter for
    classification — the opener anchors what we pitched, and all three
    emails pitch the same thing. Personalization placeholders are left
    generic; the classifier only needs the substance.
    """
    try:
        template = get_email_template(EmailStep.EMAIL1, language or "en")
    except (KeyError, OSError):
        template = get_email_template(EmailStep.EMAIL1, "en")
    subject = personalize_email(template.get("subject", ""), "there", "your company")
    body = _TAG_RE.sub(" ", personalize_email(template.get("body_html", ""), "there", "your company"))
    if not subject.strip() or not body.strip():
        # The lru_cache means this fires once per language per run, not
        # per prospect. Loud but non-fatal: a blank opener degrades the
        # classifier's context, it doesn't invalidate the reply itself.
        blank = " + ".join(
            name for name, value in (("subject", subject), ("body", body))
            if not value.strip()
        )
        click.echo(
            f"  ⚠ Reconstructed email-1 opener has blank {blank} for "
            f"language '{language}' — check content/emails.json; the "
            f"reply classifier is running without pitch context and "
            f"classifications this run may be degraded.",
            err=True,
        )
    return f"Subject: {subject}\n\n{body}".strip()


def _detect_language_for(values: dict, email: str) -> str:
    """Best-effort language from the person's primary_location, else TLD."""
    country = ""
    loc_data = values.get("primary_location", [])
    if loc_data and isinstance(loc_data, list) and isinstance(loc_data[0], dict):
        country = loc_data[0].get("country_code", "") or ""
    domain = email.split("@", 1)[1] if "@" in email else ""
    return detect_language(domain, country)


def detect_email_responses(
    attio: AttioClient,
    gmail: GmailClient | None,
) -> dict:
    """Scan Gmail for prospect replies and flip CRM state. Returns counts."""
    counts = _empty_counts()
    if gmail is None:
        return {"skipped": True, "reason": "no_gmail_client", **counts}

    # Enumerate everyone who has received >= 1 outreach email.
    people: list[dict] = []
    for stage in REPLY_SCAN_STAGES:
        people.extend(
            attio.search_people(
                filter_={"email_campaign_stage": stage.value},
                limit=50_000,
                fail_if_truncated=True,
            )
        )

    today_iso = date.today().isoformat()
    writer = AttioWriter(attio=attio)

    for record in people:
        values = record.get("values", {})
        record_id = record.get("id", {}).get("record_id", "")
        counts["scanned"] += 1

        # Idempotency: one detection per prospect, ever.
        if _first_value(values, "email_response_received_at"):
            counts["already_processed"] += 1
            continue

        email_data = values.get("email_addresses", [])
        email = ""
        if email_data:
            e = email_data[0]
            email = e.get("email_address", "") if isinstance(e, dict) else str(e)
        if not email:
            continue

        started_str = (
            _first_value(values, "email_campaign_started")
            or _first_value(values, "email_campaign_last_sent")
        )
        if not started_str:
            # A prospect in a reply-scan stage with NO campaign-start
            # timestamp can't be date-bounded, so it's silently unscannable —
            # count it so the run rollup surfaces the data gap instead of
            # dropping it invisibly.
            counts["no_start_timestamp"] += 1
            continue
        try:
            after = date.fromisoformat(started_str[:10])
        except ValueError:
            continue

        try:
            hits = gmail.search_inbound(from_address=email, after=after)
        except Exception as exc:  # HttpError lives in a lazily-imported lib
            counts["gmail_errors"] += 1
            click.echo(
                f"  ⚠ Gmail search failed for {email}: "
                f"{type(exc).__name__}: {exc} — skipping prospect.",
                err=True,
            )
            continue
        if not hits:
            continue

        # Newest real (non-auto-generated) message wins; one flip per person.
        reply_body = ""
        reply_date_ms = -1
        for hit in hits:
            try:
                body, headers, internal_ms = gmail.get_message(hit["message_id"])
            except Exception as exc:
                counts["gmail_errors"] += 1
                click.echo(
                    f"  ⚠ Gmail fetch failed for {email} "
                    f"(message {hit['message_id']}): "
                    f"{type(exc).__name__}: {exc}",
                    err=True,
                )
                continue
            if is_auto_generated(headers):
                counts["auto_generated_skipped"] += 1
                continue
            if not body.strip():
                continue
            if internal_ms > reply_date_ms:
                reply_body = body
                reply_date_ms = internal_ms
        if not reply_body:
            continue

        language = _detect_language_for(values, email)
        opener = _reconstruct_opener(language)
        result = classify_reply_llm(opener, reply_body)
        classification = result.get("classification", "neutral")
        source = result.get("source", "keyword")

        new_stage = EMAIL_CLASS_TO_STAGE.get(
            classification, EMAIL_CLASS_TO_STAGE["neutral"]
        )
        received_at = (
            datetime.fromtimestamp(reply_date_ms / 1000, tz=UTC).isoformat()
            if reply_date_ms > 0
            else datetime.now(tz=UTC).isoformat()
        )
        truncated_body = reply_body[:MAX_RESPONSE_TEXT_LEN]

        click.echo(
            f"  Email response from {email} [{classification}/{source}] "
            f"-> moving to '{new_stage.value}'"
        )

        # Write 1: the STAGE FLIP, via the email lane's existing direct
        # update_person convention. Ordered FIRST: the idempotency marker
        # (email_response_received_at) must not land unless the sequencer
        # stop landed — otherwise a transient failure here would strand a
        # replied prospect in an active stage forever while the marker
        # blocks re-detection.
        try:
            attio.update_person(record_id, {
                "email_campaign_stage": new_stage.value,
            })
        except httpx.HTTPError as exc:
            counts["attio_update_failures"] += 1
            click.echo(
                f"  ⚠ Stage flip failed for {email}: {exc} — nothing "
                f"written; will retry next run.",
                err=True,
            )
            continue

        counts["detected"] += 1
        if classification in counts:
            counts[classification] += 1
        if source == "llm":
            counts["classifier_llm"] += 1
        else:
            counts["classifier_keyword"] += 1

        # Write 2: the three registry-gated response attrs via AttioWriter
        # (§3.15). No `stage` slug in the updates, so the LinkedIn
        # monotonicity/terminal-class gates don't apply here. A failure
        # here only loses enrichment (AttioWriter already DLQ'd +
        # escalated); the sequencer stop above already landed.
        try:
            writer.apply(WriteIntent(
                object="people",
                record_id=record_id,
                updates={
                    "email_response_classification": classification,
                    "email_response_received_at": received_at,
                    "last_email_response_text": truncated_body,
                },
                prior_values={},
                writer_module=_WRITER_MODULE,
            ))
        except UnauthorizedAttioWriteError:
            raise  # caller bug — halt loudly
        except AttioError as exc:
            counts["attio_update_failures"] += 1
            click.echo(
                f"  ⚠ Response-attr write failed for {email}: {exc}. Stage "
                f"flip landed (sequencer stopped) but the idempotency "
                f"marker is unset — AttioWriter opened an "
                f"attio_write_failed queue row.",
                err=True,
            )

        # Forensic note — best-effort, never fails the detection.
        try:
            attio.create_note(
                record_id=record_id,
                title=f"Auto-detected email response -- {classification}",
                content=(
                    f"Reply: {reply_body}\n\n"
                    f"Classification: {classification} (source: {source})\n"
                    f"Reasoning: {result.get('reasoning', '')}\n"
                    f"Action: {result.get('suggested_action', '')}\n"
                    f"Summary: {result.get('summary', '')}"
                ),
                parent_object="people",
            )
        except (httpx.HTTPStatusError, httpx.RequestError) as note_exc:
            click.echo(
                f"  ⚠ Detection landed for {email} but audit note failed: "
                f"{type(note_exc).__name__}: {note_exc}",
                err=True,
            )

        # Operator queue row — best-effort; idempotent on (type, key).
        try:
            escalate(
                type="email_response_detected",
                idempotency_key=f"email-reply|{record_id}|{received_at[:10]}",
                payload={
                    "record_id": record_id,
                    "email": email,
                    "classification": classification,
                    "new_stage": new_stage.value,
                    "reply_excerpt": truncated_body[:300],
                    "detected_on": today_iso,
                },
                attio=attio,
            )
        except (MissingAttioCredentials, httpx.HTTPError) as esc_exc:
            click.echo(
                f"  ⚠ Queue row failed for {email}: "
                f"{type(esc_exc).__name__}: {esc_exc}",
                err=True,
            )

    return counts
