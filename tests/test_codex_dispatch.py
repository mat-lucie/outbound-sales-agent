import json
import threading
import time

import pytest

from workflows.codex_dispatch import (
    create_session,
    expire_request,
    pending,
    respond,
    session_lock,
    validate_session,
)
from workflows.llm_dispatch import (
    LLMDispatchFailed,
    LLMDispatchResult,
    LLMDispatchTimeout,
    _atomic_write_json,
    _poll_for_outbox,
    request_llm_dispatch,
    write_dispatch_response,
)


def test_session_roundtrip_isolated_atomic_and_cleanup(tmp_path, monkeypatch):
    root = create_session(tmp_path)
    other = create_session(tmp_path)
    monkeypatch.setenv("OUTBOUND_LLM_DISPATCH_SESSION", str(root))
    monkeypatch.setenv("OUTBOUND_LLM_BUDGET_LEDGER_DISABLED", "1")
    results = []
    errors = []
    def requester():
        try:
            results.append(request_llm_dispatch("quality_gate_haiku", "p", "s", timeout_s=2, poll_interval_s=.01))
        except Exception as exc:
            errors.append(exc)
    thread = threading.Thread(target=requester)
    thread.start()
    deadline = time.monotonic() + 2
    requests = []
    while time.monotonic() < deadline and not requests:
        requests = pending(root)
        time.sleep(.01)
    assert len(requests) == 1
    assert pending(other) == []
    with pytest.raises(ValueError, match="absent"):
        respond(other, requests[0]["file"], {"raw_text": "wrong"})
    respond(root, requests[0]["file"], {"success": True, "raw_text": '{"pass": true}'})
    thread.join(2)
    assert not thread.is_alive() and not errors
    assert results[0].raw_text == '{"pass": true}'
    assert pending(root) == []
    assert not list((root / "outbox").iterdir())
    with pytest.raises(ValueError, match="absent"):
        respond(root, requests[0]["file"], {"raw_text": "late"})


def test_timeout_expires_session_request(tmp_path, monkeypatch):
    root = create_session(tmp_path)
    monkeypatch.setenv("OUTBOUND_LLM_DISPATCH_SESSION", str(root))
    monkeypatch.setenv("OUTBOUND_LLM_BUDGET_LEDGER_DISABLED", "1")
    with pytest.raises(LLMDispatchTimeout):
        request_llm_dispatch("quality_gate_haiku", "p", "s", timeout_s=.02, poll_interval_s=.001)
    assert pending(root) == []
    assert len(list((root / "inbox").glob("*.expired"))) == 1
    (root / "active").unlink()
    with pytest.raises(ValueError, match="closed"):
        validate_session(root)


def test_response_id_mismatch_fails(tmp_path):
    path = tmp_path / "result.json"
    _atomic_write_json(path, {"dispatch_id": "wrong", "success": True, "raw_text": "x"})
    with pytest.raises(LLMDispatchFailed, match="mismatch"):
        _poll_for_outbox(path, .001, .1, "quality_gate_haiku", "right")


@pytest.mark.parametrize("payload", [
    {"success": "false"}, {"success": True, "raw_text": {}},
    {"success": False, "error": ""}, {"success": True, "error": "oops"},
])
def test_invalid_result_rejected(payload):
    with pytest.raises(ValueError):
        LLMDispatchResult(dispatch_id="abc", **payload)


def test_response_path_traversal_rejected(tmp_path):
    with pytest.raises(ValueError):
        write_dispatch_response(tmp_path, "../../escape", "quality_gate_haiku")
    assert not list(tmp_path.iterdir())


def test_atomic_failure_preserves_old_response(tmp_path, monkeypatch):
    path = tmp_path / "response.json"
    path.write_text('{"old": true}')
    def fail(*args):
        raise OSError("disk error")
    monkeypatch.setattr("os.replace", fail)
    with pytest.raises(OSError):
        _atomic_write_json(path, {"new": True})
    assert json.loads(path.read_text()) == {"old": True}
    assert list(tmp_path.iterdir()) == [path]


def test_explicit_error_result_surfaces(tmp_path):
    root = create_session(tmp_path)
    request = {"dispatch_id": "abc", "step": "quality_gate_haiku", "prompt": "p",
               "system": "s", "model_class": "haiku", "max_tokens": 50}
    name = "quality_gate_haiku-abc.json"
    _atomic_write_json(root / "inbox" / name, request)
    respond(root, name, {"success": False, "error": "agent unavailable"})
    result = _poll_for_outbox(root / "outbox" / name, .001, .1, "quality_gate_haiku", "abc")
    assert result.success is False and result.error == "agent unavailable"
    with pytest.raises(ValueError, match="absent"):
        respond(root, name, {"success": True, "raw_text": "overwrite"})


def test_closed_session_fails_before_ledger(tmp_path, monkeypatch):
    from unittest.mock import Mock
    root = create_session(tmp_path)
    (root / "active").unlink()
    monkeypatch.setenv("OUTBOUND_LLM_DISPATCH_SESSION", str(root))
    ledger = Mock(side_effect=AssertionError("ledger touched"))
    monkeypatch.setattr("workflows.llm_dispatch.LLMBudgetLedger.try_reserve", ledger)
    with pytest.raises(ValueError, match="closed"):
        request_llm_dispatch("quality_gate_haiku", "p", "s")
    ledger.assert_not_called()


def _pending_fixture(root):
    name = "quality_gate_haiku-abc.json"
    _atomic_write_json(root / "inbox" / name,
                       {"dispatch_id": "abc", "step": "quality_gate_haiku", "prompt": "p",
                        "system": "s", "model_class": "haiku", "max_tokens": 50})
    return name


def test_simultaneous_responders_have_one_winner(tmp_path):
    root = create_session(tmp_path)
    name = _pending_fixture(root)
    barrier = threading.Barrier(3)
    outcomes = []
    def worker(text):
        barrier.wait()
        try:
            respond(root, name, {"success": True, "raw_text": text})
            outcomes.append(("won", text))
        except ValueError:
            outcomes.append(("refused", text))
    threads = [threading.Thread(target=worker, args=(text,)) for text in ("first", "second")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive()
    assert sorted(item[0] for item in outcomes) == ["refused", "won"]
    winner = next(text for status, text in outcomes if status == "won")
    assert json.loads((root / "outbox" / name).read_text())["raw_text"] == winner


def test_responder_waiting_for_expiry_cannot_publish(tmp_path, monkeypatch):
    from pathlib import Path

    from workflows.codex_dispatch import expire_request
    root = create_session(tmp_path)
    name = _pending_fixture(root)
    path = root / "inbox" / name
    expiry_locked = threading.Event()
    responder_started = threading.Event()
    release_expiry = threading.Event()
    outcomes = []
    original_rename = Path.rename
    def paused_rename(self, target):
        if self == path:
            expiry_locked.set()
            assert release_expiry.wait(2)
        return original_rename(self, target)
    monkeypatch.setattr(Path, "rename", paused_rename)
    def responder():
        responder_started.set()
        try:
            respond(root, name, {"success": True, "raw_text": "late"})
        except ValueError:
            outcomes.append("refused")
    expiry = threading.Thread(target=expire_request, args=(root, path))
    expiry.start()
    assert expiry_locked.wait(1)
    thread = threading.Thread(target=responder)
    thread.start()
    assert responder_started.wait(1)
    release_expiry.set()
    expiry.join(2)
    thread.join(2)
    assert not expiry.is_alive() and not thread.is_alive() and outcomes == ["refused"]
    assert path.with_suffix(".expired").exists()
    assert not list((root / "outbox").iterdir())


def test_exclusive_publication_never_overwrites(tmp_path):
    path = tmp_path / "response.json"
    _atomic_write_json(path, {"winner": 1}, exclusive=True)
    with pytest.raises(FileExistsError):
        _atomic_write_json(path, {"winner": 2}, exclusive=True)
    assert json.loads(path.read_text()) == {"winner": 1}
    assert list(tmp_path.iterdir()) == [path]


def test_close_during_ledger_reservation_prevents_publication(tmp_path, monkeypatch):
    root = create_session(tmp_path)
    monkeypatch.setenv("OUTBOUND_LLM_DISPATCH_SESSION", str(root))
    reservation_started = threading.Event()
    release_reservation = threading.Event()
    errors = []

    def delayed_reservation(*args, **kwargs):
        reservation_started.set()
        assert release_reservation.wait(2)

    monkeypatch.setattr("workflows.llm_dispatch.LLMBudgetLedger.try_reserve", delayed_reservation)

    def requester():
        try:
            request_llm_dispatch("quality_gate_haiku", "p", "s", timeout_s=.1)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=requester)
    thread.start()
    assert reservation_started.wait(1)
    with session_lock(root):
        (root / "active").unlink()
    release_reservation.set()
    thread.join(2)
    assert not thread.is_alive()
    assert len(errors) == 1 and "closed" in str(errors[0])
    assert not list((root / "inbox").iterdir())


def test_malformed_atomic_response_fails_immediately(tmp_path):
    path = tmp_path / "response.json"
    path.write_text("{")
    start = time.monotonic()
    with pytest.raises(LLMDispatchFailed, match="Malformed response"):
        _poll_for_outbox(path, .001, 1, "quality_gate_haiku", "abc", atomic_published=True)
    assert time.monotonic() - start < .5


def test_response_published_after_final_poll_is_quarantined(tmp_path):
    root = create_session(tmp_path)
    name = _pending_fixture(root)
    (root / "outbox" / name).write_text('{"success": true}')
    expire_request(root, root / "inbox" / name)
    assert not (root / "inbox" / name).exists()
    assert not (root / "outbox" / name).exists()
    assert (root / "inbox" / name).with_suffix(".expired").exists()
    assert (root / "outbox" / name).with_suffix(".late").exists()


def test_expiry_rename_failure_keeps_response_visible(tmp_path, monkeypatch):
    from pathlib import Path

    root = create_session(tmp_path)
    name = _pending_fixture(root)
    inbox_path = root / "inbox" / name
    outbox_path = root / "outbox" / name
    outbox_path.write_text('{"success": true}')
    original_rename = Path.rename

    def fail_inbox_rename(self, target):
        if self == inbox_path:
            raise OSError("inbox unavailable")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", fail_inbox_rename)
    with pytest.raises(OSError, match="inbox unavailable"):
        expire_request(root, inbox_path)
    assert inbox_path.exists() and outbox_path.exists()
    assert not outbox_path.with_suffix(".late").exists()


def test_wait_pending_wakes_when_request_is_published(monkeypatch, tmp_path):
    from workflows import codex_dispatch as dispatch
    root = create_session(tmp_path)
    values = iter([[], [{"file": "request.json"}]])
    monkeypatch.setattr(dispatch, "pending", lambda _: next(values))
    monkeypatch.setattr(dispatch.time, "sleep", lambda _: None)
    assert dispatch.wait_pending(root, timeout=1) == [{"file": "request.json"}]


def test_wait_pending_zero_timeout_and_invalid_timeout(tmp_path):
    from workflows.codex_dispatch import wait_pending
    root = create_session(tmp_path)
    assert wait_pending(root, timeout=0) == []
    with pytest.raises(ValueError):
        wait_pending(root, timeout=61)
