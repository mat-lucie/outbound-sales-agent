"""Claude Code subagent dispatch for the engine's LLM steps (F-PR-9).

Per plan §0 invariant #11, no engine process holds ``ANTHROPIC_API_KEY``
or imports the ``anthropic`` SDK. LLM work surfaces to the parent
slash-command session via a structured file-based handoff; the parent
session dispatches an Agent subagent, writes the response back to a
sibling file, and the engine's script picks the response up and continues.

# Handoff mechanism (per F-PR-9 brainstorm)

Codex uses a private per-run ``OUTBOUND_LLM_DISPATCH_SESSION`` directory.
See ``docs/llm_dispatch_skill_handoff.md`` for the supervised lifecycle.
Legacy callers retain inbox/outbox under ``~/.outbound-agent/llm_dispatch/``:

  - The engine script writes ``inbox/{step}-{dispatch_id}.json`` with the
    request payload and blocks on ``outbox/{step}-{dispatch_id}.json``.
  - Parent skill (``~/.agents/skills/sales-daily/SKILL.md``) polls the
    inbox directory, dispatches an Agent subagent for each new file,
    and writes the response to the outbox.
  - The engine script polls every ``poll_interval_s`` (default 0.5s) up to
    ``timeout_s`` (default 300s) and raises ``LLMDispatchTimeout`` if
    the outbox never appears.

Preserves the operator's "one command daily / one command weekly" UX: from the
operator's view they invoke ``/sales-daily`` once; the polling dance
happens between the script and the skill under the hood.

# Cost gate

``LLMBudgetLedger.try_reserve(step, estimated_cost)`` MUST succeed
before the inbox file is written. F-PR-9 ships a stub that always
returns True; PR-35 swaps in the real Attio-backed ledger. The shape
is fixed so PR-35's diff is a single-class replacement.

# Token estimates (from plan §4 F-PR-9 line 529-540)

Initial calibration values for ``estimated_cost`` computation. Update
from production actuals after PR-35 lands and the ``llm_cost_ceiling_
calibration`` typed escalation expires.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from workflows.run_evidence import observed

DEFAULT_DISPATCH_DIR = Path.home() / ".outbound-agent" / "llm_dispatch"
DISPATCH_ENABLED_ENV = "OUTBOUND_USE_LLM_DISPATCH"
DEFAULT_INBOX_DIR = DEFAULT_DISPATCH_DIR / "inbox"
DEFAULT_OUTBOX_DIR = DEFAULT_DISPATCH_DIR / "outbox"

DEFAULT_POLL_INTERVAL_S = 0.5
DEFAULT_TIMEOUT_S = 300.0


ModelClass = Literal["haiku", "sonnet", "opus"]


# Per plan §4 F-PR-9 line 529-540. (input_tokens, output_tokens). The TOKEN_ESTIMATES
# dict's keys are the canonical list of valid `step` identifiers; a typo here
# raises in `estimate_cost_usd` rather than silently bypassing the cost gate.
# Forward declarations for downstream PRs (PR-25/33/34/35/36+) intentionally
# included so those PRs don't need to touch this dict.
TOKEN_ESTIMATES: dict[str, tuple[int, int]] = {
    "quality_gate_haiku": (400, 50),
    "response_classification": (1200, 200),
    "industry_classification": (800, 100),
    "synthetic_prescreen": (2000, 300),
    "borderline_verdict": (1200, 200),
    "weekly_brain_critique": (4000, 500),
    "company_hq_classifier": (600, 80),
    "synth_holdout": (2000, 300),
    "diagnostic_critique": (4000, 500),
}

# Rough $/1k tokens at current rates. Used by the stub ledger; PR-35
# will replace with live rate-card lookup.
_HAIKU_INPUT_USD_PER_1K = 0.001
_HAIKU_OUTPUT_USD_PER_1K = 0.005
_SONNET_INPUT_USD_PER_1K = 0.003
_SONNET_OUTPUT_USD_PER_1K = 0.015


def is_dispatch_enabled() -> bool:
    """True iff ``$OUTBOUND_USE_LLM_DISPATCH=1`` in the environment.

    Production slash commands (skills) export this before invoking
    ``cli daily`` / ``cli weekly`` so the script's LLM steps surface
    to the parent session. Tests + dev shells leave it unset so the
    no-client legacy paths return their existing fallback envelopes.
    """
    return os.environ.get(DISPATCH_ENABLED_ENV, "").strip() == "1"


def estimate_cost_usd(step: str, model_class: ModelClass = "haiku") -> float:
    """Rough $ cost estimate from the token table + model class.

    Conservative — uses the higher Sonnet rates if model_class is
    "sonnet" or "opus". **Raises `KeyError` on unknown steps** so a
    typo in a new LLM step's identifier can't silently bypass the
    cost gate. Per type-design QA + AI-engineer F-1 finding, the
    original `(0, 0)` fallback let unknown steps slip through as
    $0 estimates, defeating PR-35's `configuration_decision`
    escalation entry point.
    """
    if step not in TOKEN_ESTIMATES:
        raise KeyError(
            f"Unknown LLM step {step!r}. Add an entry to TOKEN_ESTIMATES "
            f"or escalate `configuration_decision` for the new step. Known "
            f"steps: {sorted(TOKEN_ESTIMATES.keys())}"
        )
    in_tok, out_tok = TOKEN_ESTIMATES[step]
    if model_class == "haiku":
        return (in_tok / 1000) * _HAIKU_INPUT_USD_PER_1K + (
            out_tok / 1000
        ) * _HAIKU_OUTPUT_USD_PER_1K
    return (in_tok / 1000) * _SONNET_INPUT_USD_PER_1K + (
        out_tok / 1000
    ) * _SONNET_OUTPUT_USD_PER_1K


# PR-35: LLMBudgetLedger + CostCeilingExhausted live in workflows.llm_budget.
# Re-export here so the F-PR-9 import path stays valid for every existing
# caller and test (`from workflows.llm_dispatch import LLMBudgetLedger,
# CostCeilingExhausted`). workflows.llm_budget owns the canonical defs.
from workflows.llm_budget import (  # noqa: E402,F401
    CostCeilingExhausted,
    LLMBudgetLedger,
)


class LLMDispatchTimeout(Exception):
    """Raised when the parent skill never writes the outbox file within
    ``timeout_s``. Likely causes: skill not running, skill crashed,
    inbox path mis-configured. The dispatch record is left in the
    inbox for forensics — the next invocation can be re-driven by the
    skill picking up the orphan."""

    def __init__(self, step: str, dispatch_id: str, timeout_s: float) -> None:
        self.step = step
        self.dispatch_id = dispatch_id
        self.timeout_s = timeout_s
        super().__init__(
            f"LLM dispatch for {step!r} (id={dispatch_id!r}) timed out after "
            f"{timeout_s:.1f}s. Parent skill did not write outbox file. "
            f"Check inbox at {DEFAULT_INBOX_DIR}."
        )


class LLMDispatchFailed(Exception):
    """Raised when the parent skill wrote an outbox file with
    ``success=False``. The skill's error is surfaced as ``self.error``;
    common causes are subagent timeouts or model overloads at the
    parent layer."""

    def __init__(self, step: str, dispatch_id: str, error: str) -> None:
        self.step = step
        self.dispatch_id = dispatch_id
        self.error = error
        super().__init__(
            f"LLM dispatch for {step!r} (id={dispatch_id!r}) failed: {error}"
        )


class LLMBudgetLedgerUnavailable(LLMDispatchFailed):
    """Raised when the CRM-backed budget ledger cannot be read or
    written for INFRA reasons (HTTP error, network failure, AttioWriter
    exhaustion) while reserving budget for a dispatch.

    Semantics (PR-216 incident follow-up):
      - The dispatch is still REFUSED — no LLM call runs without a
        reservation, so §3.7 is never bypassed.
      - Distinct from ``CostCeilingExhausted`` (cap known-exceeded →
        fail closed) and ``LLMBudgetLedgerNotInitialized``
        (misconfiguration → fail loud).
      - Subclasses ``LLMDispatchFailed`` so every caller that already
        degrades gracefully on dispatch failure (response classifier,
        HQ classifier, synthetic prescreen) handles ledger outages the
        same way instead of crashing on a raw httpx error.
      - The quality gate catches this subclass explicitly to fail OPEN
        to the agent staging path when ``agent_gate=True``.

    Defined here (not in workflows.llm_budget) because the parent class
    lives in this module and llm_budget already lazy-imports from it.
    """

    def __init__(self, step: str, cause: BaseException) -> None:
        self.cause = cause
        super().__init__(
            step=step,
            dispatch_id="<budget-ledger>",
            error=(
                f"budget ledger unavailable ({type(cause).__name__}: {cause}); "
                "dispatch refused — reservation could not be recorded"
            ),
        )


@dataclass
class LLMDispatchRequest:
    """The structured handoff payload the engine writes to the inbox file.

    Field semantics:
      step: the LLM-step identifier from TOKEN_ESTIMATES.
      prompt: the user message.
      system: the system prompt (pre-rendered with any caching markers).
      model_class: "haiku" | "sonnet" | "opus" — parent skill maps to
        the concrete subagent.
      max_tokens: max response tokens, mirrored from the original
        Anthropic API call.
      schema_hint: a short string telling the subagent what shape to
        return (JSON keys, label list, etc.). Subagent uses this to
        constrain its output.
    """

    dispatch_id: str
    step: str
    prompt: str
    system: str
    model_class: str
    max_tokens: int
    schema_hint: str | None = None


@dataclass
class LLMDispatchResult:
    """The structured response the parent skill writes to the outbox file.

    On success, ``raw_text`` contains the subagent's verbatim output.
    On failure, ``success=False`` and ``error`` carries the reason.

    The ``__post_init__`` validator rejects incoherent combinations
    (success=True with non-None error; success=False without error)
    so the round-trip-through-JSON pattern can't silently produce a
    result that callers can't interpret.
    """

    dispatch_id: str
    success: bool
    raw_text: str = ""
    error: str | None = None

    def __post_init__(self) -> None:
        if type(self.success) is not bool or not isinstance(self.raw_text, str):
            raise ValueError("Dispatch success must be boolean and raw_text must be text")
        if not isinstance(self.dispatch_id, str) or not self.dispatch_id:
            raise ValueError("Dispatch ID must be non-empty text")
        if self.error is not None and not isinstance(self.error, str):
            raise ValueError("Dispatch error must be text or null")
        if self.success and self.error is not None:
            raise ValueError(
                f"LLMDispatchResult dispatch_id={self.dispatch_id!r}: "
                "success=True must have error=None"
            )
        if not self.success and not self.error:
            raise ValueError(
                f"LLMDispatchResult dispatch_id={self.dispatch_id!r}: "
                "success=False requires a non-empty error string"
            )


def _inbox_dir(override: Path | None = None) -> Path:
    return override or DEFAULT_INBOX_DIR


def _outbox_dir(override: Path | None = None) -> Path:
    return override or DEFAULT_OUTBOX_DIR


def request_llm_dispatch(
    step: str,
    prompt: str,
    system: str,
    *,
    model_class: ModelClass = "haiku",
    max_tokens: int = 500,
    schema_hint: str | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float | None = None,
    inbox_dir: Path | None = None,
    outbox_dir: Path | None = None,
) -> LLMDispatchResult:
    """Surface an LLM call to the parent slash-command session.

    Workflow:
      1. ``LLMBudgetLedger.try_reserve(step, ...)`` — raises
         ``CostCeilingExhausted`` if denied (no inbox write, no
         subagent dispatched).
      2. Write the request payload to
         ``inbox_dir / "{step}-{dispatch_id}.json"``.
      3. Poll ``outbox_dir / "{step}-{dispatch_id}.json"`` every
         ``poll_interval_s`` up to ``timeout_s``.
      4. On outbox arrival: parse, unlink BOTH files, return
         ``LLMDispatchResult``. Raises ``LLMDispatchFailed`` if
         ``success=False``.
      5. On timeout: raise ``LLMDispatchTimeout`` (inbox file left
         in place for forensics + retry).
    """
    # Resolve defaults at call-time (not function-def-time) so tests can
    # monkeypatch DEFAULT_TIMEOUT_S / DEFAULT_POLL_INTERVAL_S on the module.
    if timeout_s is None:
        timeout_s = DEFAULT_TIMEOUT_S
    if poll_interval_s is None:
        poll_interval_s = DEFAULT_POLL_INTERVAL_S

    session_root = os.environ.get("OUTBOUND_LLM_DISPATCH_SESSION")
    if session_root:
        from workflows.codex_dispatch import validate_session
        root = validate_session(Path(session_root))
        if inbox_dir is not None or outbox_dir is not None:
            raise ValueError("Session dispatch cannot override inbox/outbox paths")
        inbox_dir, outbox_dir = root / "inbox", root / "outbox"
    estimated = estimate_cost_usd(step, model_class)
    # `try_reserve` raises `CostCeilingExhausted` itself (with the real
    # cap + consumed numbers when PR-35's ledger is wired). Stub raises
    # nothing.
    #
    # Wave-2-B fix-up (code-reviewer B4): pass require_attio_ledger=True
    # so the ledger fails loud even when callers reached this function
    # via an explicit `use_llm_dispatch=True` parameter rather than via
    # the env var. Reaching `request_llm_dispatch` IS the positive
    # signal that production dispatch is active — there's no path here
    # that isn't dispatching.
    LLMBudgetLedger.try_reserve(step, estimated, require_attio_ledger=True)

    dispatch_id = uuid.uuid4().hex
    inbox = _inbox_dir(inbox_dir)
    outbox = _outbox_dir(outbox_dir)
    inbox.mkdir(parents=True, exist_ok=True)
    outbox.mkdir(parents=True, exist_ok=True)
    inbox_file = inbox / f"{step}-{dispatch_id}.json"
    outbox_file = outbox / f"{step}-{dispatch_id}.json"

    request = LLMDispatchRequest(
        dispatch_id=dispatch_id,
        step=step,
        prompt=prompt,
        system=system,
        model_class=model_class,
        max_tokens=max_tokens,
        schema_hint=schema_hint,
    )
    # Atomic write via tempfile + rename so a polling reader never
    # observes a torn JSON.
    if session_root:
        from workflows.codex_dispatch import session_lock
        # Closing and publishing must share one lock. The earlier validation
        # can become stale while the budget reservation is in flight.
        with session_lock(root):
            _atomic_write_json(inbox_file, asdict(request))
    else:
        _atomic_write_json(inbox_file, asdict(request))
    log_dispatch(step, dispatch_id, f"inbox written; blocking on outbox (timeout={timeout_s}s)")

    try:
        result = _poll_for_outbox(
            outbox_file=outbox_file,
            poll_interval_s=poll_interval_s,
            timeout_s=timeout_s,
            step=step,
            dispatch_id=dispatch_id,
            atomic_published=bool(session_root),
        )
    except LLMDispatchTimeout:
        # Session requests are no longer actionable after requester timeout.
        # Preserve legacy non-session forensics behavior for existing callers.
        if session_root:
            from workflows.codex_dispatch import expire_request
            expire_request(root, inbox_file)
        state = "expired" if session_root else "preserved for diagnosis"
        log_dispatch(step, dispatch_id, f"timeout after {timeout_s}s — request {state}")
        raise

    # Clean up BOTH files. The `__post_init__` validator on
    # LLMDispatchResult guarantees success XOR error, so we can trust
    # result.success here.
    for f in (inbox_file, outbox_file):
        try:
            f.unlink(missing_ok=True)
        except OSError as cleanup_err:
            log_dispatch(step, dispatch_id, f"cleanup warning: {cleanup_err}")

    if not result.success:
        # `error` is non-None by validator invariant.
        assert result.error is not None
        log_dispatch(step, dispatch_id, f"failure: {result.error}")
        raise LLMDispatchFailed(step, dispatch_id, result.error)
    log_dispatch(step, dispatch_id, "success")
    return result


def _atomic_write_json(path: Path, payload: dict, *, exclusive: bool = False) -> None:
    """Write JSON via tempfile + os.replace so concurrent readers never
    see a torn write. Mirrors the F-PR-8 daily_run.pid pattern."""
    import contextlib

    tmp_path = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex}")
    try:
        with tmp_path.open("x", encoding="utf-8") as handle:
            os.chmod(tmp_path, 0o600)
            json.dump(payload, handle, indent=2)
        if exclusive:
            # Hard-link publication is atomic and fails if a winner exists.
            os.link(tmp_path, path)
            tmp_path.unlink()
        else:
            os.replace(str(tmp_path), str(path))
    except OSError:
        # Best-effort tempfile cleanup; we re-raise the original OSError
        # so the caller sees the real failure, not the cleanup error.
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


@observed("llm.dispatch_wait", "agent_wait")
def _poll_for_outbox(
    outbox_file: Path,
    poll_interval_s: float,
    timeout_s: float,
    step: str,
    dispatch_id: str,
    atomic_published: bool = False,
) -> LLMDispatchResult:
    """Block until ``outbox_file`` is readable JSON or ``timeout_s``
    elapses. Legacy writers may expose a partial JSON file, so retry those
    parse errors. Session writers publish atomically; malformed JSON fails.

    Other errors propagate. Specifically, ``TypeError`` from
    ``LLMDispatchResult(**payload)`` indicates schema drift (extra or
    missing keys after a future PR-35 change) — silently swallowing it
    would mask a real contract violation as a 5-min timeout. Per
    silent-failure-hunter CRIT, the catch is now narrow.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if outbox_file.exists():
            try:
                payload = json.loads(outbox_file.read_text())
            except json.JSONDecodeError as exc:
                if atomic_published:
                    raise LLMDispatchFailed(
                        step, dispatch_id, f"Malformed response at {outbox_file}: {exc}"
                    ) from exc
                # Mid-write race — keep polling within the deadline.
                time.sleep(poll_interval_s)
                continue
            # JSON parsed cleanly — schema-drift errors (TypeError,
            # KeyError, ValueError from __post_init__) propagate so the
            # operator sees the real failure, not a generic timeout.
            result = LLMDispatchResult(**payload)
            if result.dispatch_id != dispatch_id:
                raise LLMDispatchFailed(step, dispatch_id, "Response dispatch ID mismatch")
            return result
        time.sleep(poll_interval_s)
    raise LLMDispatchTimeout(step, dispatch_id, timeout_s)


def write_dispatch_response(
    outbox_dir: Path,
    dispatch_id: str,
    step: str,
    *,
    raw_text: str = "",
    success: bool = True,
    error: str | None = None,
    exclusive: bool = False,
) -> None:
    """Helper for the parent skill (or tests) to write a response to the
    outbox. Production skill writes JSON directly; this helper exists
    so the round-trip shape is testable + the parent skill's SKILL.md
    can document the exact file format."""
    import re
    if not re.fullmatch(r"[A-Za-z0-9_-]+", dispatch_id) or step not in TOKEN_ESTIMATES:
        raise ValueError("Invalid dispatch ID or step")
    outbox_dir.mkdir(parents=True, exist_ok=True)
    outbox_file = outbox_dir / f"{step}-{dispatch_id}.json"
    result = LLMDispatchResult(
        dispatch_id=dispatch_id, success=success, raw_text=raw_text, error=error
    )
    _atomic_write_json(outbox_file, asdict(result), exclusive=exclusive)


def log_dispatch(step: str, dispatch_id: str, msg: str) -> None:
    """Standard stderr breadcrumb for the dispatch lifecycle. Used by
    request_llm_dispatch + the skill side so production logs show a
    consistent trail."""
    print(f"[llm_dispatch:{step}:{dispatch_id[:8]}] {msg}", file=sys.stderr)


__all__ = [
    "DEFAULT_DISPATCH_DIR",
    "DEFAULT_INBOX_DIR",
    "DEFAULT_OUTBOX_DIR",
    "DEFAULT_POLL_INTERVAL_S",
    "DEFAULT_TIMEOUT_S",
    "DISPATCH_ENABLED_ENV",
    "TOKEN_ESTIMATES",
    "CostCeilingExhausted",
    "LLMBudgetLedger",
    "LLMBudgetLedgerUnavailable",
    "LLMDispatchFailed",
    "LLMDispatchRequest",
    "LLMDispatchResult",
    "LLMDispatchTimeout",
    "estimate_cost_usd",
    "is_dispatch_enabled",
    "log_dispatch",
    "request_llm_dispatch",
    "write_dispatch_response",
]
