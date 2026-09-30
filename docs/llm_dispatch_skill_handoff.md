# Attended LLM dispatch

The engine writes typed requests and waits for the attending agent. It never
starts an LLM. Claude's existing inbox/outbox protocol remains supported; Codex
uses a private session owned by the current user.

1. Initialize with `python -m workflows.codex_dispatch init <private-parent>`.
2. Set `OUTBOUND_USE_LLM_DISPATCH=1` and
   `OUTBOUND_LLM_DISPATCH_SESSION=<returned-directory>` only in the engine child.
3. Use `pending` or the bounded `wait` command with that session directory.
   Read each request's step, prompt, constraints and dispatch ID; perform the
   step using the attending agent's native capabilities and configured budget.
4. Write a JSON result with boolean `success`, `raw_text` for success and
   optional `error`. Submit with `python -m workflows.codex_dispatch respond
   <session> <request-filename> <result-file>`.
5. Wait for the child to finish. Expired requests cannot be answered; late
   responses are quarantined. Close with `python -m workflows.codex_dispatch
   close <session>` only after the child stops and its inbox is empty.

Keep session data outside the repository. Never export dispatch enablement
globally, share a session between runs, enable an API fallback, or treat
dispatch completion as permission for outreach.
