"""Email compliance helpers: CAN-SPAM/GDPR footer, send-gate, List-Unsubscribe
header, plaintext rendering, and an idempotency sent-ledger.

The email campaign path must, by law (CAN-SPAM in the US, comparable rules
elsewhere), include in every commercial message: a clear opt-out mechanism and
a valid physical postal address. This module centralizes those requirements:

  * ``assert_email_compliance_ready`` — fail-loud send-gate (dry-run exempt):
    refuses to send live when no physical address is configured.
  * ``list_unsubscribe_header`` — the RFC 2369/8058 ``List-Unsubscribe`` mailto
    header (one-click unsubscribe in Gmail/Outlook → lands in the operator's
    unsubscribe inbox).
  * ``append_footer`` — appends the visible CAN-SPAM footer (sender org +
    physical address + opt-out line) and returns both the HTML body and a
    plaintext alternative.
  * sent-ledger (``already_sent``/``mark_sent``) — a local record (rewritten
    atomically on each ``mark_sent``) so a crash between the provider send and
    the CRM stage write does not re-send the same email on the next run. A
    corrupt/unreadable ledger fails loud and BLOCKS live sends (dry-run exempt)
    rather than silently treating history as empty and re-emailing everyone.

Env config (read at call time so tests can monkeypatch; consistent with the
existing env-only EMAIL_FROM/EMAIL_REPLY_TO):
  * EMAIL_PHYSICAL_ADDRESS  — required for live sends (CAN-SPAM postal address)
  * EMAIL_SENDER_ORG        — legal sender name (default: EMAIL_FROM display name)
  * EMAIL_UNSUBSCRIBE_MAILTO— unsubscribe inbox (default: EMAIL_REPLY_TO)

What this module does NOT do (documented operator infra): host an HTTP
unsubscribe endpoint (RFC 8058 one-click POST) or receive Resend
bounce/complaint webhooks. Those require an internet-reachable service the
operator stands up; see GETTING_STARTED.md.
"""

from __future__ import annotations

import json
import os
import re
from html import escape, unescape
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date


class ComplianceError(Exception):
    """Raised when a live email send would violate a compliance requirement
    (e.g. no physical postal address configured). Always fails loud — never a
    silent send of a non-compliant message."""


class LedgerCorruptError(ComplianceError):
    """Raised when the email sent-ledger exists but can't be parsed. Blocks
    live sends (dry-run exempt): an unreadable ledger must never be treated as
    empty history, which would re-email every contact in the crash window."""


# ── Env-backed config (read live) ────────────────────────────────────────────


def _physical_address() -> str:
    return os.environ.get("EMAIL_PHYSICAL_ADDRESS", "").strip()


def _unsubscribe_mailto() -> str:
    return (
        os.environ.get("EMAIL_UNSUBSCRIBE_MAILTO")
        or os.environ.get("EMAIL_REPLY_TO")
        or ""
    ).strip()


def _sender_org() -> str:
    """Legal sender name for the footer. Defaults to the display name parsed
    from EMAIL_FROM (e.g. ``"Acme <hi@acme.com>"`` → ``"Acme"``), else the
    bare EMAIL_FROM value."""
    explicit = os.environ.get("EMAIL_SENDER_ORG", "").strip()
    if explicit:
        return explicit
    email_from = os.environ.get("EMAIL_FROM", "").strip()
    if "<" in email_from:
        return email_from.split("<", 1)[0].strip() or email_from
    return email_from


# ── Send-gate ─────────────────────────────────────────────────────────────────


def assert_email_compliance_ready(*, dry_run: bool = False) -> None:
    """Fail loud before a LIVE email send if a hard compliance requirement is
    unmet. ``dry_run`` is exempt so an operator can preview without configuring
    everything first. Enforced (all must hold for a live send):

      * a physical postal address (CAN-SPAM) — EMAIL_PHYSICAL_ADDRESS;
      * a resolvable sender org so the footer never ships the literal
        ``[EMAIL_SENDER_ORG not set]`` placeholder;
      * an unsubscribe address so the List-Unsubscribe header + footer opt-out
        actually reach an inbox — EMAIL_UNSUBSCRIBE_MAILTO or EMAIL_REPLY_TO;
      * a readable sent-ledger — a corrupt ledger blocks live sends so a
        crash-window contact is never re-emailed against evaporated history.
    """
    if dry_run:
        return
    if not _physical_address():
        raise ComplianceError(
            "EMAIL_PHYSICAL_ADDRESS is not set. CAN-SPAM requires a valid "
            "physical postal address in every commercial email. Set "
            "EMAIL_PHYSICAL_ADDRESS in your .env before sending live, or use "
            "--dry-run to preview."
        )
    if not _sender_org():
        raise ComplianceError(
            "Sender org is empty. The footer would ship the literal "
            "'[EMAIL_SENDER_ORG not set]' placeholder. Set EMAIL_SENDER_ORG "
            "(or a display-named EMAIL_FROM like 'Acme <hi@acme.com>') "
            "before sending live, or use --dry-run to preview."
        )
    if list_unsubscribe_header() is None:
        raise ComplianceError(
            "No unsubscribe address configured. Every commercial email must "
            "carry a working opt-out (RFC 2369 List-Unsubscribe + footer). Set "
            "EMAIL_UNSUBSCRIBE_MAILTO (or EMAIL_REPLY_TO) before sending live, "
            "or use --dry-run to preview."
        )
    # Probe the ledger so corruption fails HERE (before any send) rather than
    # per-contact deep in the loop. Raises LedgerCorruptError on a bad file.
    _load_ledger()


# ── List-Unsubscribe header ──────────────────────────────────────────────────


def list_unsubscribe_header() -> dict[str, str] | None:
    """Return the ``List-Unsubscribe`` mailto header, or ``None`` if no
    unsubscribe address is configured (no EMAIL_UNSUBSCRIBE_MAILTO / EMAIL_REPLY_TO).

    The mailto variant needs no hosted endpoint: the recipient's client shows a
    one-click "Unsubscribe" that emails the operator's inbox. (The RFC 8058
    one-click HTTP POST variant requires an operator-hosted endpoint and is out
    of scope — see module docstring.)"""
    mailto = _unsubscribe_mailto()
    if not mailto:
        return None
    return {"List-Unsubscribe": f"<mailto:{mailto}?subject=unsubscribe>"}


# ── Footer (visible CAN-SPAM block) + plaintext rendering ─────────────────────


def build_footer() -> tuple[str, str]:
    """Return ``(html_footer, text_footer)`` carrying the sender org, physical
    address, and opt-out line. When the address is unset (dry-run preview), a
    visible placeholder is rendered so the operator notices it's missing.

    Language-neutral copy: the opt-out line pairs the English "reply with
    UNSUBSCRIBE" with the Spanish "responde UNSUBSCRIBE", so ES/EN prospects
    both get an intelligible instruction (the keyword itself stays constant so
    the operator's inbox filter can match it)."""
    org = _sender_org() or "[EMAIL_SENDER_ORG not set]"
    addr = _physical_address() or "[EMAIL_PHYSICAL_ADDRESS not set]"
    optout = (
        "Reply with UNSUBSCRIBE to stop receiving these emails. "
        "Responde UNSUBSCRIBE para no recibir más correos."
    )
    # Escape org/addr for the HTML part: a legitimate "&" in an address (e.g.
    # "Smith & Co, 1 Main St") would otherwise render as a broken entity.
    html = (
        '<hr style="border:none;border-top:1px solid #ddd;margin:24px 0 12px">'
        '<p style="color:#888;font-size:12px;line-height:1.5">'
        f"{escape(org)}<br>{escape(addr)}<br>{optout}</p>"
    )
    text = f"\n\n--\n{org}\n{addr}\n{optout}"
    return html, text


_TAG_RE = re.compile(r"<[^>]+>")
_BLOCK_BREAK_RE = re.compile(r"(?i)</p>|<br\s*/?>")
_WS_RUN_RE = re.compile(r"[ \t]+")
_NEWLINE_RUN_RE = re.compile(r"\n{3,}")


def html_to_text(html: str) -> str:
    """Render a minimal plaintext alternative from an HTML body.

    Not a full HTML renderer — converts block breaks (</p>, <br>) to newlines,
    strips remaining tags, and unescapes entities. Sufficient for the simple
    drip templates; keeps a text/plain part present for deliverability."""
    text = _BLOCK_BREAK_RE.sub("\n", html)
    text = _TAG_RE.sub("", text)
    text = unescape(text)
    text = _WS_RUN_RE.sub(" ", text)
    text = _NEWLINE_RUN_RE.sub("\n\n", text)
    return text.strip()


def append_footer(html_body: str) -> tuple[str, str]:
    """Append the compliance footer to ``html_body`` and return
    ``(html_with_footer, text_with_footer)`` — the HTML to send and a plaintext
    alternative (body rendered to text + the text footer)."""
    html_footer, text_footer = build_footer()
    html = html_body + html_footer
    text = html_to_text(html_body) + text_footer
    return html, text


# ── Idempotency sent-ledger ──────────────────────────────────────────────────

LEDGER_DIR = Path.home() / ".outbound-agent"
LEDGER_FILE = LEDGER_DIR / "email_sent.json"


def _ledger_key(record_id: str, step: str) -> str:
    # Keyed by (record_id, step) WITHOUT the date: each email step is a
    # once-ever event per contact, so a send must never repeat even across
    # days. A date-scoped key would only dedupe same-day re-runs and re-send a
    # crash-interrupted contact (send ok, CRM write failed) on the NEXT day.
    return f"{record_id}|{step}"


def _load_ledger() -> dict[str, str]:
    try:
        with open(LEDGER_FILE) as f:
            data = json.load(f)
    except FileNotFoundError:
        # Legitimate first run: no ledger yet → empty history.
        return {}
    except json.JSONDecodeError as exc:
        # NOT recoverable as {}: silently treating a corrupt ledger as empty
        # history re-sends every crash-window contact. Fail loud and block.
        raise LedgerCorruptError(
            f"Email sent-ledger at {LEDGER_FILE} is corrupt and cannot be "
            f"parsed ({exc}). Live email sends are BLOCKED until it is repaired "
            f"or removed: this file is the only record of which contacts were "
            f"already emailed, and sending with it unreadable risks re-emailing "
            f"everyone in the crash window. Inspect the file, fix or delete it, "
            f"then re-run. (--dry-run does not touch the ledger and is exempt.)"
        ) from exc
    if not isinstance(data, dict):
        # Valid JSON but not an object (null / [] / "…"): NOT a usable send
        # history. Treating it as empty would re-email every contact in the
        # crash window — the exact outcome the fail-loud contract forbids.
        raise LedgerCorruptError(
            f"Email sent-ledger at {LEDGER_FILE} parsed as "
            f"{type(data).__name__}, not an object. Live email sends are "
            f"BLOCKED until it is repaired: repair it to a JSON object "
            f"({{}} for a genuine reset — not [] or null) and re-run."
        )
    return data


def already_sent(record_id: str, step: str) -> bool:
    """True if this ``(record_id, step)`` email was already sent (on any prior
    run, possibly one that crashed before the CRM stage write) and must not be
    re-sent — same-day OR cross-day."""
    return _ledger_key(record_id, step) in _load_ledger()


def mark_sent(record_id: str, step: str, day: date) -> None:
    """Record ``(record_id, step)`` in the sent-ledger (value = send date for
    audit). Call IMMEDIATELY after the provider send returns and BEFORE the CRM
    stage write, so a crash in between never causes a re-send on any later run.

    Rewrites the whole ledger atomically (write to a temp file in the same dir,
    fsync, then ``os.replace``) so an interrupted write can never leave a
    truncated/corrupt ledger — which would otherwise trip the fail-loud gate."""
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    ledger = _load_ledger()
    ledger[_ledger_key(record_id, step)] = day.isoformat()
    tmp = LEDGER_FILE.with_name(LEDGER_FILE.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(ledger, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LEDGER_FILE)


# ── Honoring opt-outs ─────────────────────────────────────────────────────────


UNSUBSCRIBE_LOOKUP_LIMIT = 100


def unsubscribe_email(
    attio, email: str, *, limit: int = UNSUBSCRIBE_LOOKUP_LIMIT
) -> tuple[list[str], bool]:
    """Mark ALL Persons with ``email`` as ``email_campaign_stage=UNSUBSCRIBED``.

    The operator's way to honor an opt-out (e.g. a reply to the List-Unsubscribe
    mailto, or a manual request). An UNSUBSCRIBED contact is excluded from
    ``ACTIVE_STAGES`` and the send loops' explicit skip, so they are never
    emailed again. Idempotent (re-setting UNSUBSCRIBED is a no-op write).

    CRM workspaces routinely carry duplicate person records, so more than one
    Person can share an address — updating only the first match would leave a
    duplicate still emailable. Every match is updated.

    Returns ``(updated_record_ids, maybe_more)`` where ``maybe_more`` is True
    when the search returned exactly ``limit`` rows (there may be further
    duplicates beyond the lookup cap). ``updated_record_ids`` is empty when no
    Person matches.
    """
    from models.email_campaign import EmailStage

    results = attio.search_people(filter_={"email_addresses": email}, limit=limit)
    updated: list[str] = []
    for record in results:
        rid = record.get("id", {}).get("record_id", "")
        if rid:
            attio.update_person(
                rid, {"email_campaign_stage": EmailStage.UNSUBSCRIBED.value}
            )
            updated.append(rid)
    return updated, len(results) >= limit
