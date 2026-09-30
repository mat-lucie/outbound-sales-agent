# Agent guide

You are likely an AI coding agent whose user wants this outbound engine
configured and running. This file tells you how to do that safely. (Human
readers: the same material at a human pace is in [GETTING_STARTED.md](GETTING_STARTED.md).)

## What this is

A CRM-agnostic autonomous outbound sales engine: staged LinkedIn + email
cadence, two-stage ICP qualifier (deterministic keyword scoring + LLM
tiebreaker), and a learning loop. The engine's math and cadence logic are
fixed; everything operator-specific lives in `config/*.yaml`, `content/`,
and `.env`.

## Your job: onboarding a new operator

The entry point is the **`/onboard` skill** — `skills/onboard/SKILL.md`, with
the full machine-followable procedure in
`skills/onboard/references/onboarding-spec.md`. It is an interview +
orchestration skill: ask the user about their CRM, PhantomBuster account, and
ICP, then generate the config artifacts. Do not improvise your own setup flow;
the skill encodes the seams and the verification steps.

Prerequisites you can run for them:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
```

Outputs of a successful onboard: `config/crm.yaml`,
`config/phantombuster.yaml`, `config/icp.yaml`, `.env`, and — for non-Attio
CRMs — a generated `clients/crm/<vendor>_provider.py` that passes the
conformance suite (`tests/crm/test_provider_contract.py`). A generated adapter
is correct **iff** that suite is green; read `clients/crm/CONTRACT.md` before
writing one.

## Hard safety rails

1. **Dry-run first, always.** `sales weekly --dry-run` previews work; `sales daily
   --dry-run` reads pipeline inventory with live send readiness unchecked. Never drop `--dry-run`
   unless the user explicitly tells you to send for real — sending DMs/invites/
   emails is an outward-facing, irreversible action with legal exposure
   (LinkedIn ToS, CAN-SPAM/GDPR — see [COMPLIANCE.md](COMPLIANCE.md)).
2. **The shipped content is an example program, not yours to send.** Repo-root
   `content/*.json`, `sales-program.md`, and `config/icp.example.yaml` are the
   neutral defaults. They must be replaced with the user's
   own ICP and copy before any wet run (`examples/acme/` is a synthetic worked
   example; gaps are listed in [docs/LIMITATIONS.md](docs/LIMITATIONS.md)).
3. **Secrets live only in `.env`.** The YAML configs reference env-var *names*,
   never values. Never write a secret into a tracked file;
   `scripts/check_no_secrets.py` is the gate and must stay green.
4. **LLM steps route through you.** The engine never holds an Anthropic API
   key. With `OUTBOUND_USE_LLM_DISPATCH=1`, LLM work (qualifier tiebreaks,
   reply classification) is surfaced as JSON handoff files under
   `~/.outbound-agent/llm_dispatch/inbox/` for Claude, or the private per-run
   `OUTBOUND_LLM_DISPATCH_SESSION` for Codex; the operational skills
   (`skills/sales-daily/`, `skills/sales-weekly/`) tell you how to poll the
   inbox, answer each request, and write the response to `outbox/`.

## Running it

CLI installs as `sales` (or `python cli.py`). Daily cadence: `sales daily`;
weekly prospecting: `sales weekly`; replies: `sales check-responses`;
connectivity: `sales health-check`. The recurring operations are wrapped as
skills under `skills/` — prefer those, they encode the gates and review steps.

## Verification gates (run before claiming anything works)

```bash
ruff check
mypy clients models workflows content cli.py
python scripts/check_no_secrets.py
python -m pytest -q
```

All four must pass — they are the CI hard gates.

## Code conventions

- Engine code stays operator-neutral: operator data belongs in config/content,
  never hardcoded (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- Fail loud: missing/invalid config raises `ConfigError`; no silent defaults.
- `§N.N` and `PR-NN` markers in comments are design-history provenance from
  the original internal plan — explained in CONTRIBUTING.md, safe to ignore,
  don't add new ones.
