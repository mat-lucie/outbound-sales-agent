#!/usr/bin/env python3
"""Gmail conversation sweep — 90d partition, full pagination.

An OPTIONAL operator tool for the warm follow-up review: it computes true
recency and direction-of-ball for every live email counterparty, including
accounts the engine cannot see because they have no CRM record.

Two queries that PARTITION the last 90 days:
  1. in:sent newer_than:90d          (threads the operator touched)
  2. newer_than:90d -in:sent         (inbound-only, incl. archived)

Groups by external counterparty, computes recency + direction-of-ball.

Requires the optional ``[gmail]`` install extra and a minted Gmail token —
see ``clients/gmail.py``. The operator's OWN domains must be declared or the
sweep cannot tell a sent message from a received one; set
``OUTBOUND_INTERNAL_DOMAINS`` (comma-separated) or a display-named
``EMAIL_FROM`` / ``EMAIL_REPLY_TO``.

Usage:
    python3 scripts/gmail_sweep.py

Output contract (parsed by the follow-up review step — keep it stable):
    a single JSON object on stdout with keys ``sent_msgs``, ``sent_pages``,
    ``inbound_msgs``, ``inbound_pages``, ``unique_threads``, ``fetch_errors``,
    ``sweep_complete``, ``failed_thread_ids`` and ``counterparties`` (a list,
    owed-first then newest-first). Warnings go to stderr — stdout stays
    parseable.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path

# Ensure the project root (parent of scripts/) is on sys.path so that
# ``from clients.gmail import ...`` works whether this module is invoked as
# ``python3 scripts/gmail_sweep.py`` (which adds scripts/ to sys.path) or as
# ``python3 -m scripts.gmail_sweep`` (which has the root already).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from clients.gmail import GmailClient  # noqa: E402

# ---------------------------------------------------------------------------
# Sender classification
#
# Every rule below is a STRUCTURAL SILENCER: a matching address is dropped
# from the sweep entirely, so the counterparty disappears from the owed/
# waiting radar with no trace anywhere in the digest. Over-matching here is
# far costlier than under-matching (a stray newsletter row is visible and can
# be ignored; a hidden prospect is not), so every rule is an ALLOW-LIST or an
# exact match — never a shape heuristic.
#
# Regression that set the current shape: the bulk-mail rule was a bare
# ``@mail\.``, which matched an address at ``mail.<brand>`` — a live
# counterparty's ACTUAL corporate domain — hiding the account from every
# sweep run until a missing follow-up forced a manual reconciliation.
#
# The obvious repair (require a further label, so ``mail.<brand>.<tld>`` still
# matches) is NOT sufficient and was rejected: ``mail.<brand>.com.br``,
# ``email.<brand>.com`` and ``mail.<brand>.com`` are corporate mail subdomains
# with the exact same shape as ``mail.sendgrid.net``. A bare ``mail.<brand>``
# survives a label count only when the brand happens to own a brand gTLD — an
# accident of registry policy, not a structural property. Bulk senders are
# therefore recognised by their ESP's registrable domain, never by a subdomain
# label.
#
# Matching is split local part vs domain on purpose: a domain rule must never
# fire on a local part and vice versa. `invoice` used to match anywhere in the
# address, silencing `cuentas@invoicehub.com`; `support`/`help` used to match
# as substrings, silencing `it.support@<corporate-domain>`.
# ---------------------------------------------------------------------------

# 1a. Robot markers — matched anywhere INSIDE the local part, because senders
#     compose them (`x-noreply@`, `bounces-123@`).
_ROBOT_LOCALPART = re.compile(
    r"no-?reply|donotreply|mailer-daemon|postmaster|newsletter|"
    r"notifications?|bounces?|messages-noreply|drive-shares-noreply|"
    r"comments-noreply|meet-recordings-noreply",
    re.I,
)

# 1b. Role mailboxes — matched as the WHOLE local part. A human may well write
#     from `it.support@` or `facturacion.invoices@`; only the bare role
#     mailbox is treated as automated.
_ROLE_LOCALPARTS = frozenset({
    "support", "help", "marketing", "updates", "billing",
    "payment", "payments", "receipt", "receipts", "invoice", "invoices",
})

# 2. Vendor domains — tools an operator uses, never prospects. Extend for your
#    own stack with OUTBOUND_SWEEP_VENDOR_DOMAINS rather than editing here.
_VENDOR_DOMAINS = frozenset({
    "google.com", "stripe.com", "brex.com", "mercury.com",
    "attio.com", "linear.app", "slack.com", "github.com", "notion.so",
    "phantombuster.com", "anthropic.com", "openai.com", "intuit.com",
    "docusign.com", "docusign.net", "zoom.us", "calendly.com",
    "amazonaws.com", "azure.com",
})

# Vendors we must stay reachable on: silence their robot mailboxes, never the
# whole domain. LinkedIn runs the outreach program — a human writing about an
# account restriction or a Sales Navigator escalation has to reach the radar.
_VENDOR_ROBOT_MAILBOXES = {
    "linkedin.com": frozenset({"info", "invitations"}),
}

# 3. ESP / bulk-mail infrastructure — the parents that brands rent when they
#    blast. An allow-list is the only honest way to express this: a bulk
#    subdomain and a corporate mail subdomain are indistinguishable by shape.
#    Missing an ESP costs one visible, ignorable digest row; guessing by shape
#    costs a hidden prospect. Add entries as real senders show up.
_ESP_DOMAINS = frozenset({
    "sendgrid.net", "sendgrid.com", "amazonses.com", "mailgun.org",
    "mailgun.net", "mailchimp.com", "mcsv.net", "mcdlv.net", "rsgsv.net",
    "list-manage.com", "campaign-archive.com", "mandrillapp.com",
    "sparkpostmail.com", "sendinblue.com", "brevo.com", "klaviyomail.com",
    "hubspotemail.net", "pardot.com", "exacttarget.com", "constantcontact.com",
    "ccsend.com", "substack.com", "intercom-mail.com", "customeriomail.com",
    "postmarkapp.com", "mailjet.com", "mailerlite.com", "activehosted.com",
    "aweber.com", "getresponse.com",
})

# 4. Freemail providers — a domain here is a mailbox host, not a company, so
#    it can never BE a counterparty: grouping by it welds unrelated people
#    into one row (a live sweep rendered 25 threads from 25 different people
#    as a single "gmail.com" counterparty). Addresses on these domains group
#    per-address instead.
_FREEMAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com", "hotmail.com", "hotmail.es",
    "hotmail.com.mx", "outlook.com", "outlook.es", "outlook.com.mx",
    "yahoo.com", "yahoo.com.mx", "yahoo.es", "icloud.com", "me.com",
    "live.com", "live.com.mx", "msn.com", "aol.com", "proton.me",
    "protonmail.com", "gmx.com", "gmx.net", "prodigy.net.mx",
    "terra.com.mx",
})


def _split_domains(raw: str) -> frozenset[str]:
    """Parse a comma-separated domain list from an env var."""
    return frozenset(
        d.strip().lstrip("@").lower() for d in raw.split(",") if d.strip()
    )


@lru_cache(maxsize=8)
def _automated_domain_re(extra: str) -> re.Pattern[str]:
    """Registrable-domain suffix match: `sns.amazonaws.com` and `amazonaws.com`
    both hit; `azurewind.com` and `invoicehub.com` do not.

    Cached on the raw OUTBOUND_SWEEP_VENDOR_DOMAINS string so the pattern is
    compiled once per distinct operator configuration (and a test that changes
    the env var still gets a freshly compiled pattern).
    """
    domains = _VENDOR_DOMAINS | _ESP_DOMAINS | _split_domains(extra)
    return re.compile(
        r"^(?:[\w-]+\.)*(?:"
        + "|".join(re.escape(d) for d in sorted(domains))
        + r")$",
        re.I,
    )


# Any address-looking token; group 1 is the domain.
_ADDRESS_DOMAIN = re.compile(r"[\w.+-]+@([\w-]+\.[\w.-]+)")


def _under(domain: str, parent: str) -> bool:
    """True when domain IS parent or a subdomain of it."""
    return domain == parent or domain.endswith("." + parent)


# IANA-reserved documentation domains (RFC 2606). Nobody owns them, so they can
# never be an operator identity — and `.env.example` ships EMAIL_FROM /
# EMAIL_REPLY_TO as `outbound@example.com`, uncommented. Without this filter an
# operator who copied the example without editing it would "configure"
# example.com as their own domain, the fail-loud UNCONFIGURED guard in main()
# would pass, and EVERY thread would misclassify silently — the exact failure
# the guard exists to prevent.
_RESERVED_EXAMPLE_PARENTS = ("example.com", "example.org", "example.net", "example")


def _is_reserved_example_domain(domain: str) -> bool:
    """True for example.com/.org/.net, the `.example` TLD, and subdomains."""
    return any(_under(domain, parent) for parent in _RESERVED_EXAMPLE_PARENTS)


def internal_domains() -> frozenset[str]:
    """The operator's OWN mail domains — addresses there are never a
    counterparty, and a message from one of them means "we replied last".

    Operator identity, so it is configuration, never a constant: read from
    ``OUTBOUND_INTERNAL_DOMAINS`` (comma-separated), falling back to the
    domain of ``EMAIL_FROM`` / ``EMAIL_REPLY_TO`` (the campaign identity the
    email lane already requires). Empty means UNCONFIGURED — :func:`main`
    refuses to run rather than report every thread as owed.

    Reserved example domains are dropped from BOTH paths (see
    ``_is_reserved_example_domain``): a shipped placeholder is not
    configuration, and letting one through defeats the UNCONFIGURED guard.
    """
    declared = {
        d for d in _split_domains(os.environ.get("OUTBOUND_INTERNAL_DOMAINS", ""))
        if not _is_reserved_example_domain(d)
    }
    if declared:
        return frozenset(declared)
    derived = set()
    for var in ("EMAIL_FROM", "EMAIL_REPLY_TO"):
        match = _ADDRESS_DOMAIN.search(os.environ.get(var, "") or "")
        if match and not _is_reserved_example_domain(match.group(1).lower()):
            derived.add(match.group(1).lower())
    return frozenset(derived)


def is_internal(address: str) -> bool:
    """True for our own addresses (never a counterparty)."""
    domain = address.strip().rpartition("@")[2].lower()
    if not domain:
        return False
    return any(_under(domain, parent) for parent in internal_domains())


def is_automated(address: str) -> bool:
    """True for robots/vendors/bulk senders — dropped from the sweep."""
    local, _, domain = address.strip().rpartition("@")
    local, domain = local.strip().lower(), domain.strip().lower()
    if not local or not domain:
        return False
    if _ROBOT_LOCALPART.search(local):
        return True
    if local in _ROLE_LOCALPARTS:
        return True
    for parent, mailboxes in _VENDOR_ROBOT_MAILBOXES.items():
        if local in mailboxes and _under(domain, parent):
            return True
    extra = os.environ.get("OUTBOUND_SWEEP_VENDOR_DOMAINS", "")
    return bool(_automated_domain_re(extra).match(domain))


# ---------------------------------------------------------------------------
# Auto-response messages — RSVPs and out-of-office replies
#
# These come FROM the counterparty's real address, so the sender filter above
# can never catch them, and they must NOT be dropped from the sweep (the
# thread is real). But they are not messages anyone answers, so they must not
# set direction-of-ball: on a live run a calendar "Accepted:" arriving after
# our real reply flipped a counterparty back to "we owe a reply", and
# "Respuesta automática:" OOO replies did the same for another.
#
# Detection, in order of trust:
#   1. Auto-Submitted header != no (RFC 3834) — Outlook OOO sets
#      "auto-generated"; verified on live OOO messages.
#   2. RSVP subject verbs — Outlook calendar responses carry NO structural
#      marker at all (verified on live "Accepted:"/"Tentative:" messages),
#      so the localized verb + colon prefix is the only signal. The list is
#      EN/ES/PT calendar verbs only — over-matching here hides a real inbound
#      reply from direction math, so no generic words.
# ---------------------------------------------------------------------------
_RSVP_SUBJECT = re.compile(
    r"^\s*(?:accepted|declined|tentative|new time proposed|"
    r"aceptada|aceptado|rechazada|rechazado|provisional|"
    r"aceite|aceito|recusada|recusado|provis[oó]rio)\s*:",
    re.I,
)


def is_auto_response(msg) -> bool:
    """True for calendar RSVPs and auto-replies — real threads, but their
    messages never set direction-of-ball (see block comment above)."""
    auto_submitted = hdr(msg, "Auto-Submitted").strip().lower()
    if auto_submitted and auto_submitted != "no":
        return True
    return bool(_RSVP_SUBJECT.match(hdr(msg, "Subject")))


def counterparty_key(address: str) -> str:
    """Grouping key for one external address: its domain (one company, one
    row, however many people write) — except freemail, where the domain is
    just a mailbox host and the address itself is the counterparty."""
    domain = address.rsplit("@", 1)[1]
    return address if domain in _FREEMAIL_DOMAINS else domain


# 500 messages per page. Real 90-day volume is a few thousand; this is a
# runaway guard, not a limit anyone should hit.
MAX_PAGES = 100


def warn(message: str) -> None:
    """Loud on stderr — stdout is the JSON contract and must stay parseable."""
    print(f"gmail_sweep: {message}", file=sys.stderr)


class ReadPacer:
    """One admission clock and cooldown shared by every sweep worker.

    Two reads/second cost at most 4,800 units/minute at Google's current
    40-unit threads.get price, below the 6,000-unit new-project quota.
    Other processes can consume the same user's budget; retries still matter.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        while True:
            with self._lock:
                now = time.monotonic()
                delay = self._next - now
                if delay <= 0:
                    self._next = now + 0.5
                    return
            time.sleep(delay)

    def cooldown(self, seconds):
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)


def execute_read(request, pacer):
    """Retry only typed transient API errors; keep exhausted reads visible."""
    from googleapiclient.errors import HttpError

    for attempt in range(8):
        pacer.wait()
        try:
            return request.execute()
        except HttpError as exc:
            status = exc.resp.status
            try:
                reasons = {
                    error.get("reason")
                    for error in json.loads(exc.content).get("error", {}).get("errors", [])
                }
            except (ValueError, TypeError, AttributeError):
                reasons = set()
            transient = status in {429, 500, 502, 503, 504} or (
                status == 403
                and bool(reasons & {"rateLimitExceeded", "userRateLimitExceeded"})
            )
            if not transient or attempt == 7:
                raise
            delay = min(2 ** attempt, 64) + random.random()
            # Honor both Retry-After formats. An excessive or invalid wait
            # remains a reported failure instead of retrying prematurely.
            retry_after = exc.resp.get("retry-after")
            if retry_after is not None:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    try:
                        delay = max(delay, parsedate_to_datetime(retry_after).timestamp() - time.time())
                    except (ValueError, TypeError, OverflowError):
                        raise exc from None
            if not 0 <= delay <= 120:
                raise
            pacer.cooldown(delay)
            warn(f"temporary Gmail read error {status}; retry {attempt + 1}/7 after {delay:.1f}s")


def list_all(svc, q, *, pacer=None):
    """Page through users().messages().list until exhausted.

    Returns ``(messages, pages, complete)``. ``complete`` is False when the
    sweep stopped early — an API error, a repeated page token, or the page cap
    — so the caller can report "sweep incomplete" with the partial result
    instead of dying and printing nothing at all.
    """
    pacer = pacer or ReadPacer()
    out, token, pages, seen = [], None, 0, set()
    while True:
        try:
            resp = execute_read(svc.users().messages().list(
                userId="me", q=q, maxResults=500, pageToken=token), pacer)
        except Exception as exc:
            warn(f"page {pages + 1} of {q!r} failed: {exc!r}")
            return out, pages, False
        out.extend(resp.get("messages", []) or [])
        pages += 1
        token = resp.get("nextPageToken")
        if not token:
            return out, pages, True
        if token in seen:
            warn(f"{q!r} repeated a page token at page {pages}")
            return out, pages, False
        if pages >= MAX_PAGES:
            warn(f"{q!r} hit the {MAX_PAGES}-page cap")
            return out, pages, False
        seen.add(token)


def fetch_threads(svc, thread_ids, *, worker_services=None, pacer=None):
    """Fetch thread metadata. Returns ``(threads, failed_ids)``.

    A thread that fails to fetch vanishes from the radar in exactly the way
    this script's worst bug did, so failures are NAMED — ids returned and
    warned to stderr — never just counted.
    """
    pacer = pacer or ReadPacer()

    def fetch_chunk(service, ids):
        found, errors = {}, []
        for tid in ids:
            try:
                found[tid] = execute_read(service.users().threads().get(
                    userId="me", id=tid, format="metadata",
                    metadataHeaders=["From", "To", "Cc", "Subject", "Date",
                                     "Auto-Submitted"]
                ), pacer)
            except Exception as exc:
                errors.append(tid)
                warn(f"thread {tid} fetch failed: {exc!r}")
        return found, errors

    services = [svc, *(worker_services or [])]
    ids = list(thread_ids)
    if len(services) == 1:
        return fetch_chunk(svc, ids)
    # googleapiclient services share an httplib2 transport and are not safe
    # to call from multiple threads. Give each worker its own service.
    chunks = [ids[i::len(services)] for i in range(len(services))]
    threads, failed = {}, []
    with ThreadPoolExecutor(max_workers=len(services)) as pool:
        for found, errors in pool.map(fetch_chunk, services, chunks):
            threads.update(found)
            failed.extend(errors)
    return threads, failed


def hdr(msg, name):
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def addrs(raw):
    return [a.lower() for a in re.findall(r"[\w.+-]+@[\w-]+\.[\w.-]+", raw or "")]


def external_addresses(messages) -> set[str]:
    """Real external participants across a thread's messages."""
    ext = set()
    for m in messages:
        for a in addrs(hdr(m, "From")) + addrs(hdr(m, "To")) + addrs(hdr(m, "Cc")):
            if not is_internal(a) and not is_automated(a):
                ext.add(a)
    return ext


def _last(messages):
    """(ms, from_me, subject) of the newest message, or None if empty."""
    if not messages:
        return None
    m = max(messages, key=lambda m: int(m.get("internalDate", 0)))
    from_me = any(is_internal(a) for a in addrs(hdr(m, "From")))
    return int(m.get("internalDate", 0)), from_me, hdr(m, "Subject")[:80]


def group_counterparties(threads):
    """Threads (id -> payload) -> one row per counterparty, unsorted.

    Every thread contributes to the group of EVERY counterparty on it
    (`counterparty_key` per external address), so one conversation can
    never scatter across a domain key and an address key. Regression of
    record: the old single-key-per-thread scheme keyed a live thread (which
    CC'd a second company) by address and the same person's stale
    calendar-invite thread by domain — the domain row then reported "we owe
    a reply" for a counterparty we had already answered.

    Direction-of-ball and recency come from the group's newest HUMAN
    message: drafts and auto-responses (RSVPs, OOO — `is_auto_response`)
    never set direction. Auto-responses are a group-level fallback so that
    a counterparty whose only traffic is a bare calendar invite still
    surfaces instead of vanishing.
    """
    def _slot():
        return {"ms": 0, "from_me": None, "subject": ""}

    groups = defaultdict(lambda: {"threads": [], "addresses": set(),
                                  "human": _slot(), "fallback": _slot()})
    for tid, t in threads.items():
        msgs = t.get("messages", []) or []
        # drafts have DRAFT label; exclude them from last-message math
        real = [m for m in msgs if "DRAFT" not in (m.get("labelIds") or [])]
        if not real:
            continue
        ext = external_addresses(real)
        if not ext:
            continue
        human_last = _last([m for m in real if not is_auto_response(m)])
        any_last = _last(real)

        by_key = defaultdict(set)
        for a in ext:
            by_key[counterparty_key(a)].add(a)
        for key, key_addrs in by_key.items():
            g = groups[key]
            g["threads"].append(tid)
            g["addresses"] |= key_addrs
            for slot, last in (("human", human_last), ("fallback", any_last)):
                if last and last[0] > g[slot]["ms"]:
                    g[slot] = {"ms": last[0], "from_me": last[1],
                               "subject": last[2]}

    out = []
    for key, g in groups.items():
        latest = g["human"] if g["human"]["ms"] else g["fallback"]
        out.append({
            "counterparty": key,
            "addresses": sorted(g["addresses"])[:6],
            "n_threads": len(g["threads"]),
            "thread_ids": g["threads"][:8],
            "latest_ms": latest["ms"],
            "ball_mine": (not latest["from_me"]),
            "latest_subject": latest["subject"],
        })
    return out


def run_sweep(*, cache_path=None, force_full=False):
    if not internal_domains():
        raise ValueError("Configure OUTBOUND_INTERNAL_DOMAINS or EMAIL_FROM before a Gmail sweep")
    started = time.monotonic()
    client = GmailClient.from_credentials()
    svc = client._service
    pacer = ReadPacer()
    workers = None

    def fetch(service, ids):
        nonlocal workers
        if workers is None:
            try:
                workers = [GmailClient.from_credentials()._service for _ in range(3)] if ids else []
            except Exception as exc:
                warn(f"parallel Gmail setup failed ({type(exc).__name__}); fetching serially")
                workers = []
        return fetch_threads(service, ids, worker_services=workers, pacer=pacer)

    if cache_path is not None:
        from workflows.gmail_inventory import collect_inventory
        threads, result = collect_inventory(
            svc, cache_path, list_all=lambda service, q: list_all(service, q, pacer=pacer),
            fetch_threads=fetch, read=lambda request: execute_read(request, pacer),
            warn=warn, max_pages=MAX_PAGES, force_full=force_full,
        )
    else:
        sent, sent_pages, sent_ok = list_all(svc, "in:sent newer_than:90d", pacer=pacer)
        inbound, inbound_pages, inbound_ok = list_all(svc, "newer_than:90d -in:sent", pacer=pacer)
        ids = {m["threadId"] for m in sent + inbound}
        threads, failed = fetch(svc, ids)
        result = {
            "sent_msgs": len(sent), "sent_pages": sent_pages,
            "inbound_msgs": len(inbound), "inbound_pages": inbound_pages,
            "unique_threads": len(ids), "fetch_errors": len(failed),
            "sweep_complete": bool(sent_ok and inbound_ok and not failed),
            "failed_thread_ids": sorted(failed), "inventory_mode": "uncached",
            "cache_hits": 0, "thread_reads_requested": len(ids),
        }
    out = group_counterparties(threads)
    out.sort(key=lambda r: (-int(r["ball_mine"]), -r["latest_ms"]))
    result.update(counterparties=out, wall_seconds=round(time.monotonic() - started, 3))
    return result



def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=Path(os.environ.get(
        "OUTBOUND_GMAIL_INVENTORY", str(Path.home() / ".outbound-agent/gmail-inventory.json"))))
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore metadata cache and perform an audited full refresh")
    parser.add_argument("--no-cache", action="store_true", help="Do not read or write local inventory")
    args = parser.parse_args()
    print(json.dumps(run_sweep(cache_path=None if args.no_cache else args.cache,
                               force_full=args.full_refresh), default=str))





def main():
    """Uncached programmatic inventory entry point."""
    print(json.dumps(run_sweep(), default=str))


if __name__ == "__main__":
    cli()
