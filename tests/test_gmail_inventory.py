"""Deterministic coverage and request-count scenarios; no live mailbox access."""
import json
from unittest.mock import MagicMock

import pytest

from workflows.gmail_inventory import AUDIT_SECONDS, collect_inventory, inventory_lock


class Mailbox:
    def __init__(self):
        self.service = MagicMock()
        self.profile = {"emailAddress": "mat@example.com", "historyId": "100"}
        self.history = {"historyId": "100"}
        self.ids = ["a", "b"]
        self.failed = set()
        self.requested = []
        self.list_complete = True
        self.history_error = None

    def read(self, request):
        if request is self.service.users().getProfile.return_value:
            return self.profile.copy()
        if self.history_error:
            raise self.history_error
        return {"historyId": "100", **self.history}

    def listing(self, service, query):
        return ([{"threadId": tid} for tid in self.ids] if query.startswith("in:") else [],
                1, self.list_complete)

    def fetch(self, service, ids):
        self.requested.append(set(ids))
        return ({tid: {"id": tid, "messages": [{"id": tid + "-message"}]}
                 for tid in ids if tid not in self.failed}, sorted(set(ids) & self.failed))

    def run(self, path, **kwargs):
        return collect_inventory(self.service, path, list_all=self.listing,
                                 fetch_threads=self.fetch, read=self.read,
                                 warn=lambda _: None, max_pages=3,
                                 now=kwargs.pop("now", 1000), **kwargs)


def test_clean_then_unchanged_avoids_all_thread_gets(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    first, stats = box.run(path)
    assert stats["sweep_complete"] and stats["thread_reads_requested"] == 2
    second, stats = box.run(path)
    assert first == second
    assert stats["sweep_complete"] and stats["thread_reads_requested"] == 0
    assert stats["inventory_mode"] == "incremental"
    assert len(box.requested) == 1


def test_large_delta_fetches_only_changed_and_new_threads(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.ids = [str(i) for i in range(1761)]
    box.run(path)
    changed = [str(i) for i in range(100)]
    box.history = {"history": [{"labelsRemoved": [{"message": {"threadId": t}} for t in changed]}]}
    box.ids.append("new")
    _, stats = box.run(path)
    assert stats["thread_reads_requested"] == 101
    assert stats["cache_hits"] == 1661
    assert set.union(*box.requested[-2:]) == set(changed + ["new"])


@pytest.mark.parametrize("reason", ["expired", "corrupt", "mailbox", "audit", "repeat"])
def test_invalid_or_unaudited_cache_forces_complete_refresh(tmp_path, reason):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.run(path)
    kwargs = {}
    if reason == "expired":
        box.history_error = RuntimeError("history 404")
    elif reason == "corrupt":
        path.write_text("broken")
    elif reason == "mailbox":
        box.profile["emailAddress"] = "other@example.com"
    elif reason == "audit":
        kwargs["now"] = 1000 + AUDIT_SECONDS
    else:
        box.history = {"nextPageToken": "repeat"}
    _, stats = box.run(path, **kwargs)
    assert stats["inventory_mode"] == "full"
    assert stats["sweep_complete"]
    assert box.requested[-1] == {"a", "b"}


def test_partial_reads_are_named_and_retried_without_stale_rows(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.run(path)
    box.profile["historyId"] = "200"
    box.history = {"history": [{"messagesAdded": [{"message": {"threadId": "a"}}]}]}
    box.failed = {"a"}
    threads, stats = box.run(path)
    assert not stats["sweep_complete"]
    assert "a" not in threads and stats["failed_thread_ids"] == ["a"]
    assert json.loads(path.read_text())["history_id"] == "100"
    box.history, box.failed = {}, set()
    threads, stats = box.run(path)
    assert stats["sweep_complete"] and set(threads) == {"a", "b"}
    assert box.requested[-1] == {"a"}
    assert json.loads(path.read_text())["history_id"] == "200"


def test_initial_partial_fetch_keeps_successes_for_retry(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.failed = {"b"}
    assert not box.run(path)[1]["sweep_complete"]
    box.failed = set()
    assert box.run(path)[1]["sweep_complete"]
    assert box.requested[-1] == {"b"}


def test_incomplete_listing_never_replaces_checkpoint(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.run(path)
    previous = path.read_bytes()
    box.list_complete, box.ids = False, ["a"]
    assert not box.run(path)[1]["sweep_complete"]
    assert path.read_bytes() == previous


def test_fresh_membership_removes_deleted_and_expired_threads(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.run(path)
    box.ids = []
    threads, stats = box.run(path)
    assert threads == {} and stats["sweep_complete"]
    assert json.loads(path.read_text())["threads"] == {}


def test_concurrent_sweep_refuses_to_overwrite_inventory(tmp_path):
    path = tmp_path / "inventory.json"
    with inventory_lock(path), pytest.raises(BlockingIOError), inventory_lock(path):
        pytest.fail("second sweep acquired the lock")


def test_interrupted_full_fetch_resumes_atomic_progress(tmp_path):
    box, path = Mailbox(), tmp_path / "inventory.json"
    box.ids = [str(i) for i in range(104)]
    fetch = box.fetch
    calls = 0

    def interrupt_second(service, ids):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise InterruptedError("interrupted between chunks")
        return fetch(service, ids)

    box.fetch = interrupt_second
    with pytest.raises(InterruptedError):
        box.run(path)
    assert len(json.loads(path.read_text())["threads"]) == 100
    box.fetch = fetch
    _, result = box.run(path)
    assert result["sweep_complete"]
    assert result["cache_hits"] == 100 and result["thread_reads_requested"] == 4
