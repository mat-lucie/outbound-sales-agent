"""Crash-recovery scan for unrecorded PhantomBuster DM sends.

MOTIVATION
----------
The daily DM flow is: launch PB Message Sender (async on PB's servers) →
wait → download result.csv → write stage advances to Attio. If the local
process is killed between PB finishing and the Attio writeback, DMs are
live on LinkedIn but Attio never recorded them. On the next daily run
those entries are still at DM{n}_SENT, so the sequencer re-sends a
duplicate DM — a hard violation of the sales-program.md "never message
the same person twice" brand rule.

This module provides ``recover_unrecorded_dm_sends``, a best-effort
startup scan that reconciles the LAST Message Sender run's result.csv
against current Attio state. Call it EARLY in the daily run, before any
new sends. It is idempotent: entries where last_contact_date >= send_date
are silently skipped.

ALGORITHM (summary)
-------------------
1. Fetch the last Message Sender output via ``pb.get_output(agent_id)``.
   Parse the ``✅ CSV saved at <url>`` line from the console log. Return
   early if absent (nothing to recover).
2. Download that CSV (plain HTTP GET — S3 URL needs no auth).
3. For each row with status == "Message sent": canonicalize the URL and
   look up the Attio list entry. Build a url→entry index once over all
   list entries.
4. IDEMPOTENCY: skip if entry.last_contact_date >= send_date (already
   recorded).
5. Compute target stage from STAGE_TRANSITIONS (DM transitions only:
   ACCEPTED→DM1_SENT, DM1_SENT→DM2_SENT, DM2_SENT→DM3_SENT). Skip if
   current stage is not a DM-step key in that map.
6. Unless dry_run, advance via the §3.15 AttioWriter path
   (``daily_check._attio_advance_with_escalation``): writes new stage +
   dm_step + dm{k}_sent_at + last_contact_date with registry / monotonicity
   enforcement and retry/DLQ/attio_write_failed escalation — the same path
   the DM sequencer uses, not a raw update_list_entry bypass.
7. Return a summary dict and print a concise operator line.
"""

from __future__ import annotations

import csv
import io
import re
import sys
from datetime import date
from typing import TYPE_CHECKING

import httpx

from clients.attio import AttioClient
from models.pipeline import PipelineStage

if TYPE_CHECKING:
    from clients.attio import AttioClient as AttioClientType
    from clients.phantombuster import PhantomBusterClient as PBClientType


# Only DM-step forward transitions are recoverable via this scan — the ones
# the Message Sender phantom actually performs: it sends DM1 to ACCEPTED
# entries, DM2 to DM1_SENT, DM3 to DM2_SENT. A confirmed "Message sent" whose
# Attio entry is still at the pre-send stage means that DM went out but the
# advance was never recorded; we move it one step forward. DM3_SENT is
# deliberately absent — recovery must never push DM3_SENT→RESPONDED (a send
# does not imply a reply), and never regress.
_RECOVERABLE_TRANSITIONS: dict[PipelineStage, PipelineStage] = {
    PipelineStage.ACCEPTED: PipelineStage.DM1_SENT,
    PipelineStage.DM1_SENT: PipelineStage.DM2_SENT,
    PipelineStage.DM2_SENT: PipelineStage.DM3_SENT,
}

# Target stage → (dm_step number, sent_at attr slug)
_DM_STAGE_META: dict[PipelineStage, tuple[int, str]] = {
    PipelineStage.DM1_SENT: (1, "dm1_sent_at"),
    PipelineStage.DM2_SENT: (2, "dm2_sent_at"),
    PipelineStage.DM3_SENT: (3, "dm3_sent_at"),
}

# Regex to extract the "CSV saved at <url>" line that PB Message Sender
# prints in its console log on a successful run.
_CSV_URL_RE = re.compile(r"CSV saved at (https://\S+\.csv)")


def _parse_csv_url_from_output(output_text: str) -> str | None:
    """Extract the S3 result.csv URL from PB console log text.

    Returns None if the expected line is absent (the phantom may have run
    but produced no sends, or the run failed before completing).
    """
    m = _CSV_URL_RE.search(output_text)
    return m.group(1) if m else None


def _download_csv(url: str, *, timeout: float = 30.0) -> str:
    """Download a plain CSV from an S3 URL (no auth needed)."""
    resp = httpx.get(url, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _build_record_index(
    attio: AttioClientType,
    list_id: str,
) -> dict[str, dict]:
    """Build a person-record_id → parsed-entry index from ONE list query.

    Each list entry already carries its parent person `record_id`, so no
    per-person fetch is needed here. Sent URLs are matched to entries in the
    caller by resolving url → person via ``search_person_by_linkedin`` (a
    handful of lookups) — far cheaper than fetching all ~700 person records
    on every daily startup.
    """
    raw_entries = attio.query_list_entries(list_id=list_id)
    index: dict[str, dict] = {}
    for raw in raw_entries:
        parsed = AttioClient.parse_entry(raw)
        record_id = parsed.get("record_id")
        if record_id:
            index[record_id] = parsed
    return index


def recover_unrecorded_dm_sends(
    attio: AttioClientType,
    pb: PBClientType,
    message_sender_id: str,
    list_id: str,
    today: str,
    *,
    dry_run: bool = False,
    expected_container_id: str | None = None,
) -> dict:
    """Reconcile the last Message Sender run's result.csv against Attio.

    Parameters
    ----------
    attio:
        Live AttioClient (or mock in tests).
    pb:
        Live PhantomBusterClient (or mock in tests).
    message_sender_id:
        The PB agent ID for the Message Sender phantom.
    list_id:
        Attio LinkedIn Outreach list ID.
    today:
        ISO date string ("YYYY-MM-DD") representing the current operator
        date. Used only as fallback; send_date is read from the CSV row's
        ``timestamp`` column when available.
    dry_run:
        When True, compute + return the summary but make no Attio writes.

    Returns
    -------
    dict with keys:
        examined            int   rows with status=="Message sent"
        recovered           int   entries advanced in Attio (or would be)
        skipped_already_recorded   int   send already in Attio (idempotent)
        skipped_no_entry    int   no matching list entry found
        skipped_failed      int   CSV row had an error / non-sent status
        skipped_no_transition int   stage not in recoverable transitions
        skipped_write_failed int   AttioWriter exhausted retries / permanent
                                   error (DLQ + escalation already landed)
    """
    summary = {
        "examined": 0,
        "recovered": 0,
        "skipped_already_recorded": 0,
        "skipped_no_entry": 0,
        "skipped_failed": 0,
        "skipped_no_transition": 0,
        "skipped_write_failed": 0,
        "skipped_wrong_run": 0,
    }

    # ── Step 1: fetch last PB run output ─────────────────────────────────
    out = pb.get_output(message_sender_id)
    output_text = out.get("output", "") or ""
    if expected_container_id is not None:
        observed_container = out.get("containerId")
        if observed_container != expected_container_id:
            print(f"[pb_send_recovery] containerId={observed_container!r} does not match expected {expected_container_id!r}; skipping recovery", file=sys.stderr)
            summary["skipped_wrong_run"] = -1
            return summary
    else:
        print("[pb_send_recovery] WARN: no expected_container_id provided; cannot verify run attribution", file=sys.stderr)
    csv_url = _parse_csv_url_from_output(output_text)
    if not csv_url:
        print(
            "[pb_send_recovery] No 'CSV saved at' line in last Message Sender "
            "output — nothing to recover.",
            file=sys.stderr,
        )
        return summary

    # ── Step 2: download the result.csv ──────────────────────────────────
    try:
        csv_text = _download_csv(csv_url)
    except httpx.HTTPError as exc:
        print(
            f"[pb_send_recovery] Failed to download result CSV from {csv_url}: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return summary

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    # ── Step 3: record_id→entry from ONE list query (no per-person fetch) ──
    record_index = _build_record_index(attio, list_id)

    # ── Step 4–6: process each row ───────────────────────────────────────
    for row in rows:
        row_status = (row.get("status") or "").strip()
        row_error = (row.get("error") or "").strip()
        raw_url = (row.get("linkedinProfileUrl") or "").strip()

        # Rows with errors or non-sent status are never recovered.
        if row_error or row_status != "Message sent":
            summary["skipped_failed"] += 1
            continue

        summary["examined"] += 1

        # Resolve the sent URL → person → entry (only the handful of sent
        # rows hit the network, not the whole pipeline).
        person = attio.search_person_by_linkedin(raw_url) if raw_url else None
        record_id = (
            (person.get("id", {}) or {}).get("record_id") or person.get("record_id")
            if person else None
        )
        entry = record_index.get(record_id) if record_id else None
        if entry is None:
            summary["skipped_no_entry"] += 1
            continue

        # Resolve the send date from the CSV row timestamp.
        raw_ts = (row.get("timestamp") or "").strip()
        send_date: str
        if raw_ts:  # noqa: SIM108 — block form keeps the inline format-explanation comments
            # Timestamp may be "2026-06-01T14:32:00.000Z" or "2026-06-01 14:32:00"
            send_date = raw_ts[:10]  # take YYYY-MM-DD
        else:
            send_date = today

        # ── Step 4: idempotency check ─────────────────────────────────────
        last_contact = entry.get("last_contact_date")
        if last_contact:
            # Normalize: may be a date object, ISO string, or None
            last_contact_str = last_contact.isoformat() if isinstance(last_contact, date) else str(last_contact)[:10]
            if last_contact_str >= send_date:
                summary["skipped_already_recorded"] += 1
                continue

        # ── Step 5: compute target stage ─────────────────────────────────
        current_stage_str = entry.get("stage") or ""
        try:
            current_stage = PipelineStage(current_stage_str)
        except ValueError:
            summary["skipped_no_transition"] += 1
            continue

        target_stage = _RECOVERABLE_TRANSITIONS.get(current_stage)
        if target_stage is None:
            summary["skipped_no_transition"] += 1
            continue

        dm_meta = _DM_STAGE_META.get(target_stage)
        if dm_meta is None:
            # Defensive: target stage is not a DM stage — should never happen
            # given _RECOVERABLE_TRANSITIONS, but skip rather than panic.
            summary["skipped_no_transition"] += 1
            continue

        dm_step, sent_at_slug = dm_meta

        # ── Step 6: advance (unless dry_run) ─────────────────────────────
        entry_id = entry.get("entry_id")
        if not entry_id:
            summary["skipped_no_entry"] += 1
            continue

        update_payload = {
            "stage": target_stage.value,
            "dm_step": dm_step,
            sent_at_slug: send_date,
            "last_contact_date": send_date,
        }

        if dry_run:
            summary["recovered"] += 1
            continue

        # §3.15: route the advance through the same AttioWriter path the
        # DM sequencer uses (registry / monotonicity / terminal-class
        # enforcement + retry/DLQ/attio_write_failed escalation) instead
        # of the raw update_list_entry bypass. Deferred import: daily_check
        # is heavy and pb_send_recovery is imported at daily startup; this
        # keeps module load light and avoids a top-level coupling. The
        # forward-only DM transitions here satisfy monotonicity by
        # construction (ACCEPTED→DM1_SENT→DM2_SENT→DM3_SENT).
        from workflows.daily_check import _attio_advance_with_escalation

        ok = _attio_advance_with_escalation(
            attio=attio,
            entry_id=entry_id,
            entry_attributes=update_payload,
            list_id=list_id,
            linkedin_url=raw_url,
            today=send_date,
            step_label=f"dm{dm_step}",
            writer_module="workflows.pb_send_recovery.recover_unrecorded_dm_sends",
            prior_stage=current_stage_str,
            person_record_id=record_id,
        )
        if ok:
            summary["recovered"] += 1
        else:
            # AttioWriter exhausted retries / hit a permanent error; it has
            # already DLQ'd + opened an attio_write_failed queue row. Count
            # it distinctly rather than as a (false) recovery — the entry
            # stays at its prior stage and the operator is alerted.
            summary["skipped_write_failed"] += 1

    # ── Step 7: print summary + return ───────────────────────────────────
    mode_label = "[DRY RUN] " if dry_run else ""
    print(
        f"[pb_send_recovery] {mode_label}"
        f"examined={summary['examined']} "
        f"recovered={summary['recovered']} "
        f"already_recorded={summary['skipped_already_recorded']} "
        f"no_entry={summary['skipped_no_entry']} "
        f"failed_rows={summary['skipped_failed']} "
        f"no_transition={summary['skipped_no_transition']} "
        f"write_failed={summary['skipped_write_failed']}",
        file=sys.stderr,
    )
    return summary
