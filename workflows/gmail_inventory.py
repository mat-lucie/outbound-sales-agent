"""Mailbox-bound metadata cache. Fresh listings define coverage on every sweep.

History invalidation only avoids unchanged thread GETs; it never substitutes
for the live candidate checks required before drafting or sending. No bodies,
OAuth tokens, or CRM eligibility are stored here.
"""
from __future__ import annotations

import fcntl
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

VERSION = 1
AUDIT_SECONDS = 7 * 24 * 60 * 60


@contextmanager
def inventory_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.with_suffix(path.suffix + ".lock").open("a") as handle:
        os.chmod(handle.name, 0o600)
        # A second sweep must not overwrite a newer cursor with older data.
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def load_inventory(path: Path, mailbox: str, now: float) -> dict | None:
    try:
        data = json.loads(path.read_text())
        valid = (
            data["version"] == VERSION and data["mailbox"] == mailbox
            and 0 <= now - data["audit_started_at"] < AUDIT_SECONDS
            and isinstance(data["history_id"], str) and data["history_id"].isdigit()
            and isinstance(data["threads"], dict)
            and isinstance(data["failed_ids"], list)
            and all(isinstance(t, str) for t in data["failed_ids"])
            and all(isinstance(t, dict) and t.get("id") == tid
                    and isinstance(t.get("messages"), list)
                    for tid, t in data["threads"].items())
        )
        return data if valid else None
    except (OSError, ValueError, KeyError, TypeError):
        return None


def save_inventory(path: Path, data: dict) -> None:
    fd, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(data, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def changed_threads(service, history_id: str, read, *, max_pages: int) -> set[str]:
    """All history types invalidate metadata, including draft/label changes.

    A failed, expired or truncated history walk must trigger a full fetch.
    """
    changed, seen = set(), set()
    token = None
    for _ in range(max_pages):
        response = read(service.users().history().list(
            userId="me", startHistoryId=history_id, maxResults=500,
            pageToken=token,
        ))
        if not isinstance(response.get("historyId"), str) or not response["historyId"].isdigit():
            raise ValueError("Gmail history response missing a valid cursor")
        for entry in response.get("history", []):
            messages = list(entry.get("messages", []))
            for key in ("messagesAdded", "messagesDeleted", "labelsAdded", "labelsRemoved"):
                messages.extend(item["message"] for item in entry.get(key, []))
            for message in messages:
                changed.add(message["threadId"])
        token = response.get("nextPageToken")
        if not token:
            return changed
        if token in seen:
            raise ValueError("Gmail history repeated a page token")
        seen.add(token)
    raise ValueError("Gmail history page limit reached")


def collect_inventory(service, path, *, list_all, fetch_threads, read,
                      warn, max_pages, force_full=False, now=None):
    """Incremental fetch with fresh complete membership and atomic progress.

    Capture the cursor BEFORE listing/fetching: activity during this sweep
    must remain discoverable on the next run. Failed reads never advance it.
    A fresh full listing every time preserves rolling 90-day membership,
    pagination, inbound-only accounts and deletion handling.
    """
    now = time.time() if now is None else now
    path = Path(path)
    with inventory_lock(path):
        profile = read(service.users().getProfile(userId="me"))
        mailbox, start_cursor = profile["emailAddress"].lower(), profile["historyId"]
        if not mailbox or not isinstance(start_cursor, str) or not start_cursor.isdigit():
            raise ValueError("Gmail profile missing mailbox identity or history cursor")
        state = None if force_full else load_inventory(path, mailbox, now)
        mode, changed = "full", set()
        if state is not None:
            try:
                changed = changed_threads(service, state["history_id"], read, max_pages=max_pages)
                mode = "incremental"
            except Exception as exc:
                warn(f"Gmail history unavailable ({type(exc).__name__}); full metadata refresh required")
                state = None
        sent, sent_pages, sent_ok = list_all(service, "in:sent newer_than:90d")
        inbound, inbound_pages, inbound_ok = list_all(service, "newer_than:90d -in:sent")
        ids = {message["threadId"] for message in sent + inbound}
        cached = state["threads"] if state else {}
        retry = set(state["failed_ids"]) if state else set()
        needed = (ids - cached.keys()) | (ids & (changed | retry))
        threads = {tid: cached[tid] for tid in ids - needed}
        pending = set(needed)
        failed = []
        ordered = sorted(needed)
        for start in range(0, len(needed), 100):
            batch = ordered[start:start + 100]
            fetched, errors = fetch_threads(service, batch)
            threads.update(fetched)
            failed.extend(errors)
            pending.difference_update(fetched)
            if sent_ok and inbound_ok:
                save_inventory(path, {
                    "version": VERSION, "mailbox": mailbox,
                    "history_id": state["history_id"] if state else start_cursor,
                    "audit_started_at": state["audit_started_at"] if state else now,
                    "threads": threads, "failed_ids": sorted(pending), "complete": False,
                })
        complete = bool(sent_ok and inbound_ok and not failed)
        if sent_ok and inbound_ok:
            # Even incomplete thread fetches preserve successful metadata;
            # failed IDs remain explicitly pending, never stale radar rows.
            save_inventory(path, {
                "version": VERSION, "mailbox": mailbox,
                "history_id": start_cursor if complete else (
                    state["history_id"] if state else start_cursor),
                "audit_started_at": state["audit_started_at"] if state else now,
                "threads": threads, "failed_ids": sorted(failed),
                "complete": complete,
            })
        return threads, {
            "sent_msgs": len(sent), "sent_pages": sent_pages,
            "inbound_msgs": len(inbound), "inbound_pages": inbound_pages,
            "unique_threads": len(ids), "fetch_errors": len(failed),
            "sweep_complete": complete, "failed_thread_ids": sorted(failed),
            "inventory_mode": mode, "cache_hits": len(ids - needed),
            "thread_reads_requested": len(needed),
        }
