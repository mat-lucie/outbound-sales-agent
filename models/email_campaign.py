"""Email campaign definitions: stages, timing, template loading."""

import json
import os
from enum import Enum
from pathlib import Path


class EmailStage(Enum):
    """Stages for the email drip campaign, stored in Attio person fields."""
    QUEUED = "queued"
    EMAIL1_SENT = "email1_sent"
    EMAIL2_SENT = "email2_sent"
    EMAIL3_SENT = "email3_sent"
    COMPLETED = "completed"
    UNSUBSCRIBED = "unsubscribed"
    # Terminal email-reply stages (Follow-up Radar WAITING lane, PR-247).
    # Deliberately absent from ACTIVE_STAGES so a replying/declining prospect
    # drops out of the drip — and out of the WAITING lane (the account is now
    # in a human's hands, or said no).
    RESPONDED = "email_responded"
    NOT_INTERESTED = "email_not_interested"


class EmailStep(Enum):
    """Which email in the sequence."""
    EMAIL1 = "email1"
    EMAIL2 = "email2"
    EMAIL3 = "email3"


# Business days after campaign start for each email
EMAIL_TIMING: dict[EmailStep, int] = {
    EmailStep.EMAIL1: 0,   # Sent immediately when dequeued
    EmailStep.EMAIL2: 3,   # 3 business days after Email 1
    EmailStep.EMAIL3: 7,   # 7 business days after Email 1
}

# Which stage a contact must be in to receive each email
STAGE_FOR_EMAIL: dict[EmailStep, EmailStage] = {
    EmailStep.EMAIL1: EmailStage.QUEUED,
    EmailStep.EMAIL2: EmailStage.EMAIL1_SENT,
    EmailStep.EMAIL3: EmailStage.EMAIL2_SENT,
}

# Which stage to advance to after sending each email
NEXT_STAGE: dict[EmailStep, EmailStage] = {
    EmailStep.EMAIL1: EmailStage.EMAIL1_SENT,
    EmailStep.EMAIL2: EmailStage.EMAIL2_SENT,
    EmailStep.EMAIL3: EmailStage.EMAIL3_SENT,
}

# Stages that are still "active" in the campaign (not terminal)
ACTIVE_STAGES = {EmailStage.QUEUED, EmailStage.EMAIL1_SENT, EmailStage.EMAIL2_SENT}

# Stages where the prospect has received at least one email and could reply.
# Phase 0.6 (workflows.detect_email_responses) scans Gmail for these; QUEUED
# is excluded (nothing sent yet), and the terminal reply stages are excluded
# via the email_response_received_at idempotency marker anyway. (PR-243)
REPLY_SCAN_STAGES = {
    EmailStage.EMAIL1_SENT,
    EmailStage.EMAIL2_SENT,
    EmailStage.EMAIL3_SENT,
    EmailStage.COMPLETED,
}

# Classification -> terminal stage for detected email replies. Mirrors the
# LinkedIn map in detect_responses (negative -> hard stop; everything else
# -> RESPONDED for a human to pick up). Email has no DEFENSIVE_HOLD
# equivalent in v1 — defensive replies also route to RESPONDED. (PR-243)
EMAIL_CLASS_TO_STAGE: dict[str, EmailStage] = {
    "negative": EmailStage.NOT_INTERESTED,
    "defensive": EmailStage.RESPONDED,
    "positive": EmailStage.RESPONDED,
    "question": EmailStage.RESPONDED,
    "neutral": EmailStage.RESPONDED,
}

# Daily cap for new contacts entering the sequence
EMAIL_DAILY_CAP = 20

# Sending identity (override via env; defaults are neutral placeholders)
EMAIL_FROM = os.environ.get("EMAIL_FROM", "Outbound Agent <outbound@example.com>")
EMAIL_REPLY_TO = os.environ.get("EMAIL_REPLY_TO", "outbound@example.com")

# Runtime content directory — see models/campaign.py for the OUTBOUND_CONTENT_DIR
# contract. Defaults to the repo-root `content/` when the env var is unset, so
# absent-env behavior is identical to the prior hardcoded path.
CONTENT_DIR = Path(
    os.environ.get("OUTBOUND_CONTENT_DIR")
    or (Path(__file__).resolve().parent.parent / "content")
)

# Country code to language mapping
_PT_COUNTRIES = {"BR"}
_ES_COUNTRIES = {"MX", "DO", "CL", "AR", "CO", "PE", "EC", "VE", "UY", "PY", "BO", "CR", "GT", "HN", "SV", "NI", "PA", "CU", "ES"}

# Fallback: TLD-based mapping when no country code is available
_PT_TLDS = {".com.br", ".net"}
_ES_TLDS = {".com.mx", ".com.do", ".cl"}


def detect_language_from_country(country_code: str) -> str | None:
    """Map country code to language. Returns None if unknown."""
    if not country_code:
        return None
    cc = country_code.upper()
    if cc in _PT_COUNTRIES:
        return "pt"
    if cc in _ES_COUNTRIES:
        return "es"
    return "en"


def detect_language(domain: str, country_code: str = "") -> str:
    """Detect language from country code, falling back to email domain TLD.

    Priority: country_code → domain TLD → default EN.
    """
    # Try country code first
    if country_code:
        result = detect_language_from_country(country_code)
        if result:
            return result

    # Fallback to domain TLD
    domain = domain.lower()
    for tld in _PT_TLDS:
        if domain.endswith(tld):
            return "pt"
    for tld in _ES_TLDS:
        if domain.endswith(tld):
            return "es"
    return "en"


def load_email_templates() -> dict:
    """Load email templates from emails.json."""
    with open(CONTENT_DIR / "emails.json") as f:
        return json.load(f)


def get_email_template(step: EmailStep, language: str = "en") -> dict:
    """Get subject and body_html for an email step and language."""
    templates = load_email_templates()
    return templates[step.value][language]


def personalize_email(template: str, first_name: str, company: str) -> str:
    """Replace {{first_name}} and {{company}} placeholders."""
    return template.replace("{{first_name}}", first_name).replace("{{company}}", company)


def get_email_from() -> str:
    """Read the configured public operator sender identity."""
    return os.environ.get("EMAIL_FROM", EMAIL_FROM)


def get_reply_to() -> str:
    """Read the configured public operator reply address."""
    return os.environ.get("EMAIL_REPLY_TO", EMAIL_REPLY_TO)
