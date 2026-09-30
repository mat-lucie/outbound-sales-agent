"""Tests for clients/pb_envelope.py (F-PR-5).

Covers:
- Typed envelope construction (PBLaunch, PBCompletion, SendOutcome).
- `hash_arguments` determinism + null handling.
- `parse_send_outcome` for the 4 csv_status states (Message sent /
  Skipped / Error / Empty).
- `should_advance_batch` truth table (3-condition AND).
- `PBRunFailed` / `PBRunTimeout` carry container_id.
"""

from __future__ import annotations

from datetime import UTC, datetime

from clients.pb_envelope import (
    PBCompletion,
    PBLaunch,
    PBRunFailed,
    PBRunTimeout,
    SendOutcome,
    compute_invite_outcome,
    hash_arguments,
    invite_launch_advanceable,
    invite_launch_authenticated,
    parse_send_outcome,
    should_advance_batch,
)


def _launch(container_id: str = "c_123", agent_id: str = "ag_msg") -> PBLaunch:
    return PBLaunch(
        container_id=container_id,
        agent_id=agent_id,
        launched_at=datetime(2026, 5, 21, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments({"foo": "bar"}),
        request_id="req_x",
    )


def _completion(container_id: str = "c_123", log: str = "ok") -> PBCompletion:
    return PBCompletion(
        container_id=container_id,
        status="finished",
        log_output=log,
        raw_output={"status": "finished", "output": log},
    )


class TestHashArguments:
    def test_stable_across_key_order(self) -> None:
        assert hash_arguments({"a": 1, "b": 2}) == hash_arguments({"b": 2, "a": 1})

    def test_none_and_empty_dict_match(self) -> None:
        assert hash_arguments(None) == hash_arguments({})

    def test_different_args_differ(self) -> None:
        assert hash_arguments({"a": 1}) != hash_arguments({"a": 2})


class TestParseSendOutcome:
    """The 4 csv_status states + per-URL sent/skipped tracking."""

    def test_no_csv_returns_empty(self) -> None:
        launch = _launch()
        outcome = parse_send_outcome(
            launch=launch,
            completion=_completion(),
            csv_text=None,
            requested_urls={"linkedin.com/in/foo"},
        )
        assert outcome.csv_status == "Empty"
        assert outcome.sent_count == 0
        assert outcome.requested_count == 1
        assert outcome.drift_skipped_reason == "pb_returned_no_csv"
        assert outcome.container_id == launch.container_id

    def test_empty_csv_string_returns_empty(self) -> None:
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text="",
            requested_urls={"linkedin.com/in/foo"},
        )
        assert outcome.csv_status == "Empty"

    def test_csv_with_zero_rows_returns_empty(self) -> None:
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text="query,status\n",  # header only
            requested_urls=set(),
        )
        assert outcome.csv_status == "Empty"
        assert outcome.drift_skipped_reason == "pb_csv_had_zero_rows"

    def test_all_skipped_returns_skipped(self) -> None:
        csv_text = (
            "query,status\n"
            "https://linkedin.com/in/a,Can't send message\n"
            "https://linkedin.com/in/b,InMail required\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text=csv_text,
            requested_urls={"linkedin.com/in/a", "linkedin.com/in/b"},
        )
        assert outcome.csv_status == "Skipped"
        assert outcome.sent_count == 0
        assert len(outcome.skipped_urls) == 2
        assert outcome.drift_skipped_reason == "pb_csv_zero_sent_rows"

    def test_at_least_one_sent_returns_message_sent(self) -> None:
        csv_text = (
            "query,status\n"
            "https://linkedin.com/in/a,Message sent\n"
            "https://linkedin.com/in/b,InMail required\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text=csv_text,
            requested_urls={"https://linkedin.com/in/a", "https://linkedin.com/in/b"},
        )
        assert outcome.csv_status == "Message sent"
        assert outcome.sent_count == 1
        assert "https://linkedin.com/in/a" in outcome.sent_urls
        assert "https://linkedin.com/in/b" in outcome.skipped_urls

    def test_partial_send_records_drift_reason(self) -> None:
        """sent_count < requested_count → drift_skipped_reason surfaces gap."""
        csv_text = (
            "query,status\n"
            "https://linkedin.com/in/a,Message sent\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text=csv_text,
            requested_urls={"linkedin.com/in/a", "linkedin.com/in/b", "linkedin.com/in/c"},
        )
        assert outcome.csv_status == "Message sent"
        assert outcome.sent_count == 1
        assert outcome.requested_count == 3
        assert outcome.drift_skipped_reason == "pb_sent_1_of_3_requested"

    def test_input_already_processed_sets_flag(self) -> None:
        """Auto Connect dedup ('We already processed every profile from this
        spreadsheet') = the explicit already-invited signal Layer 2 advances on.
        It must be distinguishable from a generic Skipped."""
        log = (
            "✅ Got 6 lines from csv.\n"
            "⚠️ We already processed every profile from this spreadsheet.\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(log=log),
            csv_text="",  # dedup run writes no sendable rows
            requested_urls={"linkedin.com/in/a"},
        )
        assert outcome.already_processed is True

    def test_near_miss_log_is_not_already_processed(self) -> None:
        """A partial-process / similar-but-different log line must NOT trip the
        flag — a false positive cascades into a false CONNECTION_SENT advance."""
        for log in (
            "ℹ️ Processed 3 of 6 profiles; 3 remaining.",
            "✅ Got 6 lines from csv.\n🔄 Processing profiles...",
            "⚠️ Weekly invitation limit reached; skipped remaining profiles.",
        ):
            outcome = parse_send_outcome(
                launch=_launch(),
                completion=_completion(log=log),
                csv_text="",
                requested_urls={"linkedin.com/in/a"},
            )
            assert outcome.already_processed is False, log

    def test_generic_skip_is_not_already_processed(self) -> None:
        """A cap/error Skipped must NOT set already_processed — the safety
        guard that prevents Layer 2 from falsely marking un-invited prospects
        as CONNECTION_SENT."""
        csv_text = (
            "query,status\n"
            "https://linkedin.com/in/a,Can't send message\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(log="processing... done"),
            csv_text=csv_text,
            requested_urls={"linkedin.com/in/a"},
        )
        assert outcome.csv_status == "Skipped"
        assert outcome.already_processed is False

    def test_url_normalization_strips_www_and_trailing_slash(self) -> None:
        csv_text = (
            "query,status\n"
            "https://www.linkedin.com/in/alice/,Message sent\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text=csv_text,
            requested_urls={"https://linkedin.com/in/alice"},
        )
        # Matches `daily_check_helpers._normalize_linkedin_url` — keeps
        # the protocol, drops `www.` + trailing slash + lowercases.
        assert "https://linkedin.com/in/alice" in outcome.sent_urls

    def test_drift_key_is_stable(self) -> None:
        """Same launch + same requested_urls → same drift key."""
        launch = _launch()
        urls = {"a", "b", "c"}
        o1 = parse_send_outcome(launch, _completion(), None, urls)
        o2 = parse_send_outcome(launch, _completion(), None, urls)
        assert o1.next_day_drift_key == o2.next_day_drift_key

    def test_drift_key_changes_with_urls(self) -> None:
        launch = _launch()
        o1 = parse_send_outcome(launch, _completion(), None, {"a"})
        o2 = parse_send_outcome(launch, _completion(), None, {"b"})
        assert o1.next_day_drift_key != o2.next_day_drift_key

    def test_alternate_url_column_names(self) -> None:
        """CSV can come with `query` / `linkedInUrl` / `profileUrl` columns."""
        csv_text = (
            "linkedInUrl,status\n"
            "https://linkedin.com/in/a,Message sent\n"
        )
        outcome = parse_send_outcome(
            launch=_launch(),
            completion=_completion(),
            csv_text=csv_text,
            requested_urls={"https://linkedin.com/in/a"},
        )
        assert "https://linkedin.com/in/a" in outcome.sent_urls


class TestAdvanceGate:
    """§3.1 chokepoint: stage advances iff ALL THREE conditions hold."""

    def _outcome(self, **kwargs) -> SendOutcome:
        defaults = dict(
            container_id="c_123",
            csv_status="Message sent",
            sent_count=1,
            requested_count=1,
            drift_skipped_reason=None,
            next_day_drift_key="ag_msg:2026-05-21:abc",
            sent_urls=frozenset({"linkedin.com/in/a"}),
            skipped_urls=frozenset(),
        )
        defaults.update(kwargs)
        return SendOutcome(**defaults)

    def test_all_three_pass(self) -> None:
        assert should_advance_batch(_launch(), self._outcome()) is True

    def test_csv_status_skipped_fails(self) -> None:
        # Skipped state requires empty sent_urls + sent_count=0 per the
        # SendOutcome.__post_init__ invariants.
        outcome = self._outcome(
            csv_status="Skipped",
            sent_count=0,
            sent_urls=frozenset(),
            skipped_urls=frozenset({"linkedin.com/in/a"}),
        )
        assert should_advance_batch(_launch(), outcome) is False

    def test_csv_status_empty_fails(self) -> None:
        outcome = self._outcome(
            csv_status="Empty",
            sent_count=0,
            sent_urls=frozenset(),
        )
        assert should_advance_batch(_launch(), outcome) is False

    def test_csv_status_error_fails(self) -> None:
        outcome = self._outcome(
            csv_status="Error",
            sent_count=0,
            sent_urls=frozenset(),
        )
        assert should_advance_batch(_launch(), outcome) is False

    def test_container_id_mismatch_fails(self) -> None:
        # Outcome reports container_id="c_OTHER" — looks like a stale CSV
        # from a prior run. Gate MUST reject.
        assert (
            should_advance_batch(_launch(), self._outcome(container_id="c_OTHER"))
            is False
        )

    def test_zero_sent_count_fails(self) -> None:
        outcome = self._outcome(
            csv_status="Empty",  # consistent with sent_count=0
            sent_count=0,
            sent_urls=frozenset(),
        )
        assert should_advance_batch(_launch(), outcome) is False

    def test_send_count_one_passes(self) -> None:
        # Per spec: sent_count >= 1, not >= requested_count
        assert (
            should_advance_batch(
                _launch(),
                self._outcome(sent_count=1, requested_count=10),
            )
            is True
        )


class TestUrlNormalizationParity:
    """Engineer-QA-build5 nit: `clients.pb_envelope._normalize_url_for_match`
    duplicates the logic in `workflows.daily_check_helpers._normalize_linkedin_url`
    (intentionally — pb_envelope can't depend on workflows/). If either
    drifts, the advance-gate per-URL match silently mis-matches and the
    §3.1 chokepoint becomes ineffective for `www.` / trailing-slash
    variants. This test pins parity across a fixture set.
    """

    def test_normalizers_produce_identical_output(self) -> None:
        from clients.pb_envelope import _normalize_url_for_match
        from workflows.daily_check_helpers import _normalize_linkedin_url

        fixtures = [
            "https://www.linkedin.com/in/alice",
            "https://www.linkedin.com/in/alice/",
            "https://linkedin.com/in/alice",
            "https://linkedin.com/in/alice/",
            "HTTPS://WWW.LINKEDIN.COM/IN/ALICE/",
            "https://linkedin.com/in/bob-smith-12345",
            "https://www.linkedin.com/sales/lead/AB_1-2-3",
            "https://linkedin.com/in/jos%C3%A9-mart%C3%ADnez",  # URL-encoded
        ]
        for url in fixtures:
            assert (
                _normalize_url_for_match(url) == _normalize_linkedin_url(url)
            ), (
                f"Normalizer drift on {url!r}: "
                f"pb_envelope={_normalize_url_for_match(url)!r} vs "
                f"daily_check_helpers={_normalize_linkedin_url(url)!r}"
            )

    def test_empty_url_returns_empty(self) -> None:
        from clients.pb_envelope import _normalize_url_for_match
        assert _normalize_url_for_match("") == ""


class TestExceptions:
    def test_pb_run_failed_carries_container(self) -> None:
        exc = PBRunFailed("c_123", "ag_msg", "log tail")
        assert exc.container_id == "c_123"
        assert exc.agent_id == "ag_msg"
        assert "c_123" in str(exc)
        assert "ag_msg" in str(exc)

    def test_pb_run_timeout_carries_container(self) -> None:
        exc = PBRunTimeout("c_456", "ag_inb", 600)
        assert exc.container_id == "c_456"
        assert exc.elapsed_seconds == 600
        assert "600s" in str(exc)


# ── L10-8: PBRunFailed scrubs profile URLs from exception message ────────────


class TestPBRunFailedUrlScrubbing:
    """PBRunFailed must scrub linkedin.com/in/ URLs from its str(exc) message
    so they don't leak into audit logs, DLQ rows, or queue serializations.
    The raw log_tail attribute must retain the full unscrubbed text.
    (L10-8 audit fix.)"""

    def test_exception_message_has_no_profile_url(self) -> None:
        log = (
            "Starting run...\n"
            "Visiting https://www.linkedin.com/in/jane-smith-xyz456\n"
            "Error: could not send\n"
        )
        exc = PBRunFailed(container_id="c_1", agent_id="ag_1", log_tail=log)
        assert "linkedin.com/in/" not in str(exc), (
            f"Profile URL leaked into exception message: {str(exc)}"
        )

    def test_exception_message_uses_placeholder(self) -> None:
        log = "Error at https://linkedin.com/in/prospect-slug"
        exc = PBRunFailed("c_1", "ag_1", log)
        assert "<profile-url>" in str(exc)

    def test_raw_log_tail_attribute_is_unscrubbed(self) -> None:
        """The .log_tail attribute retains the full text for in-process diagnostics."""
        log = "Error at https://linkedin.com/in/prospect-slug"
        exc = PBRunFailed("c_1", "ag_1", log)
        assert "linkedin.com/in/prospect-slug" in exc.log_tail

    def test_message_without_urls_unaffected(self) -> None:
        log = "Generic error: phantom timed out after 30s"
        exc = PBRunFailed("c_1", "ag_1", log)
        assert "timed out after 30s" in str(exc)
        assert "<profile-url>" not in str(exc)

    def test_multiple_urls_all_scrubbed(self) -> None:
        log = (
            "https://www.linkedin.com/in/alice/ failed\n"
            "https://linkedin.com/in/bob succeeded\n"
            "https://www.linkedin.com/in/carol/ skipped\n"
        )
        exc = PBRunFailed("c_1", "ag_1", log)
        msg = str(exc)
        assert "alice" not in msg
        assert "bob" not in msg
        assert "carol" not in msg
        assert msg.count("<profile-url>") == 3

    def test_log_tail_capped_at_300_chars_before_scrub(self) -> None:
        """The message uses at most 300 chars of the raw log (tail)
        then applies scrubbing — so even a 10KB log doesn't bloat the exception."""
        long_url = "https://linkedin.com/in/very-long-slug-" + "x" * 200
        log = "padding " * 50 + long_url
        exc = PBRunFailed("c_1", "ag_1", log)
        assert len(str(exc)) < 1000


class TestInviteOptimisticAdvance:
    """Invite path: the Network Booster result.csv is unreliable for per-run
    send confirmation (no `status` column, accumulates stale rows), so a clean
    AUTHENTICATED launch optimistically advances all requested invites. Auth
    failures (dead cookie) must NOT advance."""

    def _parsed_skipped(self, requested: set[str]) -> SendOutcome:
        # What parse_send_outcome currently yields on an NB CSV (the bug):
        # no row matches status=="message sent" -> Skipped, sent_count=0.
        return SendOutcome(
            container_id="c_123",
            csv_status="Skipped",
            sent_count=0,
            requested_count=len(requested),
            drift_skipped_reason="pb_csv_zero_sent_rows",
            next_day_drift_key="k",
            sent_urls=frozenset(),
            skipped_urls=frozenset(requested),
            already_processed=False,
        )

    def test_clean_log_is_authenticated(self) -> None:
        assert invite_launch_authenticated(_completion(log="Adding X ... CSV saved ... finished")) is True

    def test_verify_manually_warning_still_authenticated(self) -> None:
        # Benign LinkedIn warning — the invites still go out (operator-confirmed).
        log = "Please check on LinkedIn that you can manually invite profiles"
        assert invite_launch_authenticated(_completion(log=log)) is True

    def test_no_valid_credentials_not_authenticated(self) -> None:
        assert invite_launch_authenticated(_completion(log="Connecting... No valid credentials found")) is False

    def test_network_cookie_invalid_not_authenticated(self) -> None:
        assert invite_launch_authenticated(_completion(log="error: network-cookie-invalid")) is False

    def test_clean_launch_without_per_person_confirmation_does_not_advance(self) -> None:
        req = {"https://www.linkedin.com/in/a", "https://www.linkedin.com/in/b"}
        out = compute_invite_outcome(self._parsed_skipped(req), _completion(log="Adding... finished"), req)
        assert out.csv_status == "Skipped"
        assert out.sent_urls == frozenset()
        assert out.sent_count == 0
        assert should_advance_batch(_launch(), out) is False

    def test_auth_failure_does_not_advance(self) -> None:
        req = {"https://www.linkedin.com/in/a"}
        out = compute_invite_outcome(self._parsed_skipped(req), _completion(log="No valid credentials found"), req)
        assert out.csv_status == "Skipped"
        assert should_advance_batch(_launch(), out) is False

    def test_already_processed_is_not_delivery_evidence(self) -> None:
        req = {"https://www.linkedin.com/in/a"}
        parsed = SendOutcome(
            container_id="c_123", csv_status="Skipped", sent_count=0, requested_count=1,
            drift_skipped_reason="x", next_day_drift_key="k", sent_urls=frozenset(),
            skipped_urls=frozenset(req), already_processed=True,
        )
        out = compute_invite_outcome(parsed, _completion(), req)
        assert out.already_processed is False
        assert out.csv_status == "Skipped"

    def test_empty_requested_unchanged(self) -> None:
        # recheck-only batch (no invites) — nothing to optimistically advance.
        out = compute_invite_outcome(self._parsed_skipped(set()), _completion(), set())
        assert out.csv_status == "Skipped"


class TestInviteCapRestrictionGuard:
    """Cap/restriction signals (recurring weekly-invite limit etc.) must block
    the optimistic advance — invites are silently dropped, so advancing would
    falsely mark un-sent prospects CONNECTION_SENT (Phase 0 waits forever)."""

    def _parsed_skipped(self, requested: set[str]) -> SendOutcome:
        return SendOutcome(
            container_id="c_123", csv_status="Skipped", sent_count=0,
            requested_count=len(requested), drift_skipped_reason="x",
            next_day_drift_key="k", sent_urls=frozenset(),
            skipped_urls=frozenset(requested), already_processed=False,
        )

    def test_weekly_limit_blocks_advance(self) -> None:
        log = "Adding...\n⚠️ You've reached the weekly invitation limit. Withdraw some of your pending invitations."
        assert invite_launch_advanceable(_completion(log=log)) is False

    def test_account_restriction_blocks_advance(self) -> None:
        assert invite_launch_advanceable(_completion(log="Your account is restricted")) is False

    def test_clean_launch_is_advanceable(self) -> None:
        assert invite_launch_advanceable(_completion(log="Adding X... CSV saved... finished")) is True

    def test_verify_manually_warning_is_advanceable(self) -> None:
        # benign — invites still go out
        assert invite_launch_advanceable(_completion(log="verify that the request was correctly sent")) is True

    def test_cap_log_does_not_override_outcome(self) -> None:
        req = {"https://www.linkedin.com/in/a"}
        out = compute_invite_outcome(
            self._parsed_skipped(req),
            _completion(log="reached the weekly invitation limit"),
            req,
        )
        assert out.csv_status == "Skipped"
        assert should_advance_batch(_launch(), out) is False

    def test_unconfirmed_launch_stamps_shortfall(self) -> None:
        req = {"https://www.linkedin.com/in/a"}
        out = compute_invite_outcome(self._parsed_skipped(req), _completion(log="Adding... finished"), req)
        assert out.csv_status == "Skipped"
        assert out.drift_skipped_reason == "invite_log_confirmed_0_of_1"
