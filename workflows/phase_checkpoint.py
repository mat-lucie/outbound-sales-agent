"""Resume completed acceptance work only after an exact-identity gate halt.

Replies, schema, ownership, caps and send decisions are never checkpointed.
The caller holds the daily sender lock throughout this context.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

from workflows.gmail_inventory import save_inventory
from workflows.run_evidence import emit

CHECKPOINT_PATH = Path.home() / ".outbound-agent/acceptance-checkpoint.json"


class AcceptanceCheckpoint:
    def __init__(self, *, operator_id, day, provenance, backend, scraper_id, path=None):
        self.path = Path(path) if path else CHECKPOINT_PATH
        self.resume_allowed = (
            provenance.get("dirty") is False
            and bool(provenance.get("sha")) and provenance.get("sha") != "unknown"
        )
        identity = {
            "operator": operator_id, "day": str(day), "code": provenance,
            "backend": backend, "scraper": scraper_id,
            "list": os.environ.get("ATTIO_LIST_ID", ""),
            # Credential changes invalidate a prior observation, without
            # writing the credentials themselves to the checkpoint.
            "cookies": {key: value for key, value in os.environ.items()
                        if "COOKIE" in key and key.startswith(("PB_", "LINKEDIN_"))},
        }
        self.key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        self.result = None
        self.resume_result = None

    def __enter__(self):
        try:
            state = json.loads(self.path.read_text())
            result = state.get("result")
            if (self.resume_allowed and state.get("version") == 1 and state.get("key") == self.key
                    and state.get("resume_after_identity_halt") is True
                    and isinstance(result, dict) and result.get("complete") is True):
                self.resume_result = result
        except FileNotFoundError:
            pass
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            print(
                f"Acceptance resume unavailable at {self.path} "
                f"({type(exc).__name__}); acceptance will run fresh.",
                file=sys.stderr,
            )
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._save(False)  # Consume old permission before any phase work.
        return self

    def run(self, action):
        if self.resume_result is not None:
            self.result = self.resume_result
            print("Resuming completed acceptance phase; counts are prior observations.")
            emit("phase_resumed", phase="acceptance", checkpoint_key=self.key)
        else:
            self.result = action()
        return self.result

    def _save(self, resumable):
        save_inventory(self.path, {
            "version": 1, "key": self.key, "result": self.result if resumable else None,
            "resume_after_identity_halt": resumable,
        })

    def __exit__(self, exc_type, exc, traceback):
        from workflows.detect_responses import IdentityResolutionHalt
        resumable = (
            self.resume_allowed and isinstance(exc, IdentityResolutionHalt)
            and isinstance(self.result, dict) and self.result.get("complete") is True
        )
        try:
            self._save(resumable)
        except (OSError, TypeError, ValueError) as checkpoint_error:
            if exc is None:
                raise
            # The prior marker was consumed on entry. Preserve the original
            # halt and report that this run cannot grant a resume checkpoint.
            print(f"Acceptance checkpoint write failed ({type(checkpoint_error).__name__}); "
                  "the original run failure remains authoritative.", file=sys.stderr)
        return False
