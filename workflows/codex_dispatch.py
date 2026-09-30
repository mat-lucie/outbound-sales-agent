"""Local Codex inbox/outbox helper. Never starts an LLM or calls a provider."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from workflows.llm_dispatch import LLMDispatchRequest, write_dispatch_response


def create_session(parent: Path) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="codex-", dir=parent))
    (root / "inbox").mkdir(mode=0o700)
    (root / "outbox").mkdir(mode=0o700)
    (root / "active").write_text("codex-dispatch-v1\n")
    return root


def validate_session(root: Path) -> Path:
    root = root.resolve()
    if not root.is_dir() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise ValueError("Dispatch session must be a private directory owned by this user")
    if not (root / "active").is_file():
        raise ValueError("Dispatch session is closed or uninitialized")
    for name in ("inbox", "outbox"):
        if (root / name).is_symlink() or not (root / name).is_dir():
            raise ValueError("Dispatch inbox/outbox must be local session directories")
    return root


def pending(root: Path) -> list[dict]:
    root = validate_session(root)
    requests = []
    for path in sorted((root / "inbox").glob("*.json")):
        if path.is_symlink():
            raise ValueError("Refusing symlink request")
        try:
            payload = json.loads(path.read_text())
        except FileNotFoundError:  # requester timed out between listing and read
            continue
        request = LLMDispatchRequest(**payload)
        if path.name != f"{request.step}-{request.dispatch_id}.json":
            raise ValueError("Request filename does not match payload")
        if not (root / "outbox" / path.name).exists():
            requests.append({"file": path.name, **payload})
    return requests


@contextmanager
def session_lock(root: Path):
    """Serialize response publication and timeout across helper processes."""
    root = validate_session(root)
    with (root / "lifecycle.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            validate_session(root)
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def expire_request(root: Path, request_path: Path) -> None:
    with session_lock(root):
        # Expire first: if this rename fails, the response stays visible and
        # the request cannot be redispatched with its answer hidden.
        request_path.rename(request_path.with_suffix(".expired"))
        response_path = root / "outbox" / request_path.name
        if response_path.exists():
            # A responder may have published after the requester's final poll.
            # Keep the late response for diagnosis, outside the active outbox.
            response_path.rename(response_path.with_suffix(".late"))


def respond(root: Path, name: str, result: dict) -> None:
    root = validate_session(root)
    with session_lock(root):
        matches = [item for item in pending(root) if item["file"] == name]
        if len(matches) != 1:
            raise ValueError("Request is absent, expired, or already answered")
        request = matches[0]
        if not isinstance(result, dict) or type(result.get("success")) is not bool:
            raise ValueError("Response requires an explicit boolean success field")
        if result["success"] and "raw_text" not in result:
            raise ValueError("Successful response requires raw_text")
        if set(result) - {"raw_text", "success", "error"}:
            raise ValueError("Unexpected response keys")
        write_dispatch_response(root / "outbox", request["dispatch_id"], request["step"],
                                exclusive=True, **result)


def wait_pending(root: Path, timeout: float = 60) -> list[dict]:
    """Wake promptly on a published request, with one bounded tool response."""
    if not 0 <= timeout <= 60:
        raise ValueError("wait timeout must be between 0 and 60 seconds")
    deadline = time.monotonic() + timeout
    while True:
        requests = pending(root)
        if requests or time.monotonic() >= deadline:
            return requests
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    init = commands.add_parser("init")
    init.add_argument("parent", type=Path)
    for action in ("pending", "wait", "close", "respond"):
        command = commands.add_parser(action)
        command.add_argument("root", type=Path)
        if action == "respond":
            command.add_argument("request_name")
            command.add_argument("result_file", type=Path)
    args = parser.parse_args()
    if args.action == "init":
        print(create_session(args.parent))
    elif args.action == "pending":
        print(json.dumps(pending(args.root)))
    elif args.action == "wait":
        print(json.dumps(wait_pending(args.root)))
    elif args.action == "respond":
        respond(args.root, args.request_name, json.loads(args.result_file.read_text()))
    else:
        root = validate_session(args.root)
        with session_lock(root):
            if list((root / "inbox").glob("*.json")):
                raise ValueError("Pending requests remain; stop/wait for the requester before closing")
            (root / "active").unlink()


if __name__ == "__main__":
    main()
