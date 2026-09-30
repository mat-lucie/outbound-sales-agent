# Compliance & responsible-use guide

`outbound-agent` automates B2B outreach over LinkedIn and email. Automating
outreach carries legal and platform-policy obligations. This document explains
what the engine does to help you stay compliant, **what you must do yourself**,
and the known gaps. It is not legal advice — consult counsel for your
jurisdiction and use case.

> **You are the data controller and the sender of record.** Consent, opt-outs,
> data protection, platform terms, and local outreach law are your
> responsibility, not the engine's.

## LinkedIn (Terms of Service)

**Automating LinkedIn activity violates LinkedIn's User Agreement and can get an
account restricted or permanently banned.** You accept that risk by running the
LinkedIn cadence.

What the engine does to reduce (not eliminate) risk:

- **Per-day caps** on connection invites and DMs (`config/outreach.yaml` →
  `caps`), enforced before sending.
- **Weekend gating** (`schedule.send_days`) and **per-company throttling**
  (`throttle.company_window_days`) to avoid bursty, obviously-automated patterns.
- **Cross-channel suppression**: a prospect who declined or pushed back is not
  re-contacted (see below).

These are mitigations, not a safe harbor. Keep volumes conservative and human.

## Email (CAN-SPAM / GDPR / similar)

The email path is **compliance-capable**: the engine provides the mechanisms,
but you must configure and operate them.

What the engine does:

- **Refuses to send live without a physical postal address.**
  `assert_email_compliance_ready` fails loud unless `EMAIL_PHYSICAL_ADDRESS` is
  set (CAN-SPAM requires a valid postal address in every commercial email).
  `--dry-run` is exempt for previews.
- **Footer on every campaign email**: sender org + physical address + a visible
  opt-out line.
- **`List-Unsubscribe` mailto header**: one-click unsubscribe in Gmail/Outlook,
  delivered to your `EMAIL_UNSUBSCRIBE_MAILTO` inbox.
- **Suppression on send**: `email-daily` and `email-wave2` skip anyone marked
  negative/defensive, `suppress_re_engagement`, `NOT_INTERESTED`/`DEFENSIVE_HOLD`,
  or `UNSUBSCRIBED`.
- **`sales email-unsubscribe <email>`** to record an opt-out (`UNSUBSCRIBED`),
  after which that contact is never emailed again.

What **you** must do:

1. Set `EMAIL_PHYSICAL_ADDRESS`, `EMAIL_FROM`, and `EMAIL_UNSUBSCRIBE_MAILTO` in
   `.env`. Use a verified sending domain.
2. **Monitor the unsubscribe inbox** and run `sales email-unsubscribe <email>`
   to honor each opt-out **promptly** (CAN-SPAM requires honoring within 10
   business days).
3. Only email people you have a lawful basis to contact. Under GDPR/PECR and
   similar regimes, that basis (consent or legitimate interest) is yours to
   establish and document.
4. Keep the content truthful — accurate `From`, no deceptive subject lines.

### Known gaps (operator must close)

- **No hosted one-click unsubscribe / webhook.** The RFC 8058 one-click HTTP
  `List-Unsubscribe-Post` endpoint and the Resend bounce/complaint webhook
  require an internet-reachable service you stand up; they are not shipped. Until
  then, the mailto header + manual `email-unsubscribe` is the supported flow.
- **Association outreach requires configured Attio authority through the CLI.**
  It checks cross-channel suppression and shared sent state, and serializes
  with the other email commands. Direct library callers that omit an Attio
  client retain local-only behavior and must curate suppression manually.

## Data protection

- **Secrets** never live in the repo — only in `.env` (git-ignored). A secrets
  gate (`scripts/check_no_secrets.py`) enforces this. See [SECURITY.md](SECURITY.md).
- **Prospect data** lives in your CRM and in local run-state under
  `~/.outbound-agent/`. Apply your own retention, access-control, and
  data-subject-rights (access/erasure) processes.
- **Data minimization**: only enrich and store what you need to run outreach.

## Bottom line

The engine gives you caps, suppression, opt-out mechanics, and fail-loud gates.
It cannot establish your lawful basis, monitor your unsubscribe inbox, or accept
the platform-ToS risk for you. Operate it deliberately, conservatively, and with
legal advice appropriate to your market.
