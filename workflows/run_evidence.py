"""Bounded, timestamped run evidence without response bodies or credentials.

The active journal is process-local so the existing bounded read pools share
one writer. It records observations only; it never grants send authorization.
"""
from __future__ import annotations

import hashlib
import threading
import time
import uuid
from contextlib import contextmanager
from functools import wraps
from typing import Any

_active = None
_lock = threading.RLock()


@contextmanager
def record_run(audit, provenance):
    global _active
    with _lock:
        if _active is not None:
            raise RuntimeError("run evidence already active")
        _active = audit
    try:
        emit("evidence_manifest", code=provenance, schema_version=1)
        yield
    finally:
        with _lock:
            _active = None


def emit(event, **payload):
    with _lock:
        if _active is not None:
            _active.event(event, **payload)


def observed(name, kind="phase"):
    """Emit start/end even on failure. Nested times must never be summed."""
    def decorate(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            if _active is None:
                return function(*args, **kwargs)
            started = time.monotonic()
            context = {"operation_id": uuid.uuid4().hex}
            if name in {"attio.request", "pb.request"}:
                method = str(args[1] if len(args) > 1 else kwargs.get("method", ""))
                path = str(args[2] if len(args) > 2 else kwargs.get("path", ""))
                context["method"] = method
                context["access"] = "read" if method == "GET" or path.endswith("/query") else "write"
            emit("operation_started", operation=name, kind=kind, **context)
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                emit("operation_finished", operation=name, kind=kind,
                     status="failed", error_type=type(exc).__name__,
                     wall_seconds=time.monotonic() - started, **context)
                raise
            payload: dict[str, Any] = {}
            if name == "pb.launch":
                payload["container_id"] = str(result.container_id)
                payload["agent_id"] = str(args[1] if len(args) > 1 else kwargs.get("agent_id", ""))
            elif name == "pb.result_csv":
                launch = args[1] if len(args) > 1 else kwargs.get("launch")
                payload["container_id"] = str(getattr(launch, "container_id", ""))
                if isinstance(result, str):
                    payload["sha256"] = hashlib.sha256(result.encode()).hexdigest()
                    payload["bytes"] = len(result.encode())
            emit("operation_finished", operation=name, kind=kind, status="ok",
                 wall_seconds=time.monotonic() - started, **context, **payload)
            return result
        return wrapped
    return decorate


def timed_call(name, kind, function, *args, **kwargs):
    return observed(name, kind)(function)(*args, **kwargs)
