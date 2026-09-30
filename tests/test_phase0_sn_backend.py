"""Tests for the Phase 0 (detect_accepted_connections) Sales Nav backend branch.

Mirrors workflows.pre_invite_check._run_sales_nav_scraper's launch pattern:
when PRE_INVITE_DEGREE_CHECK_BACKEND=sales_nav, the launch must fetch the
saved phantom argument, inject the fresh SN session into identities[0],
and POST the full arg shape to the Sales Nav scraper id (not the legacy
profile_scraper_id).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest


def test_phase0_sales_nav_backend_launches_with_identities_injection(monkeypatch):
    """When backend=sales_nav, Phase 0 must:

    1. Fetch the saved SN phantom argument via pb.get_agent(sn_id).
    2. Inject session creds into identities[0] (NOT top-level).
    3. Launch the SN scraper id (not the regular id).
    4. Preserve other saved fields (numberOfProfilesPerLaunch overridden,
       saveImg untouched).
    """
    from models.pipeline import PipelineStage
    from workflows.daily_check import detect_accepted_connections

    # Backend + env wiring. Use sentinel cookie/UA so we can assert
    # identities[0] contains them.
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-scraper-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")

    # One stale CONNECTION_SENT entry — forces the script down the PB launch
    # path (not the fresh-cache short-circuit).
    conn_sent_attrs = {
        "entry_id": "entry-1",
        "record_id": "rec-1",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
        "experiment_id": None,
        "linkedin_url": "https://www.linkedin.com/in/foo/",
    }

    mock_attio = MagicMock()
    mock_pb = MagicMock()

    # pb.get_agent returns the saved SN phantom config (JSON-stringified arg).
    mock_pb.get_agent.return_value = {
        "argument": json.dumps(
            {
                "numberOfProfilesPerLaunch": 10,
                "saveImg": False,
                "takeScreenshot": False,
                "identities": [
                    {
                        "identityId": "saved-identity-id",
                        "sessionCookie": "STALE-COOKIE-TO-BE-REPLACED",
                        "userAgent": "STALE-UA-TO-BE-REPLACED",
                    }
                ],
                "spreadsheetUrl": "old-placeholder-url",
            }
        ),
    }

    # Launch + wait + download_result_csv stubbed so the workflow can complete.
    mock_launch = MagicMock()
    mock_launch.container_id = "ct-123"
    mock_pb.launch_agent.return_value = mock_launch
    mock_pb.download_result_csv.return_value = ""  # empty CSV → early return

    mock_cache = MagicMock()
    mock_cache.get.return_value = ("Person", None, conn_sent_attrs["linkedin_url"], None, None)

    with (
        patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[conn_sent_attrs],
        ),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            return_value="https://sheet-url.example",
        ),
        patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
    ):
        # Force the URL into the stale bucket so we go to PB launch.
        mock_rc.partition.return_value = (
            {},  # fresh_cache empty
            [conn_sent_attrs["linkedin_url"]],  # stale_urls
        )
        mock_rc.RECHECK_TTL_DAYS = 7

        detect_accepted_connections(
            mock_attio,
            mock_pb,
            profile_scraper_id="legacy-scraper-id",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-scraper-id",
        )

    # Assertion 1: launch hit the SN scraper id, not the legacy one.
    assert mock_pb.launch_agent.called, "launch_agent must be called on the stale-URL path"
    launch_args_call = mock_pb.launch_agent.call_args
    assert launch_args_call.args[0] == "sn-scraper-id", (
        f"Expected SN scraper id; got {launch_args_call.args[0]!r}"
    )

    # Assertion 2: launch payload uses identities[0] injection, not top-level.
    payload = launch_args_call.args[1]
    assert "identities" in payload, "SN launch must include identities[0]"
    assert "sessionCookie" not in payload, (
        "Top-level sessionCookie indicates the legacy launch shape leaked through"
    )

    # Assertion 3: identities[0] carries the fresh session creds.
    ident = payload["identities"][0]
    assert ident.get("sessionCookie") == "fake-sn-cookie", (
        f"identities[0].sessionCookie must be the fresh SN cookie; got {ident.get('sessionCookie')!r}"
    )
    assert ident.get("userAgent") == "fake-ua", (
        f"identities[0].userAgent must be the fresh UA; got {ident.get('userAgent')!r}"
    )
    # Assertion 4: identityId preserved from saved arg (not clobbered by session injection).
    assert ident.get("identityId") == "saved-identity-id", (
        "identities[0].identityId must be preserved from the saved phantom config"
    )

    # Assertion 5: spreadsheetUrl overridden to the GSheet we wrote.
    assert payload["spreadsheetUrl"] == "https://sheet-url.example"

    # Assertion 6: numberOfProfilesPerLaunch overridden to the actual batch
    # size. The scraper sheet contains URLs only.
    assert payload["numberOfProfilesPerLaunch"] == 1, (
        "numberOfProfilesPerLaunch must match the one URL in the scraper sheet"
    )

    # Assertion 7: other saved fields preserved (saveImg, takeScreenshot).
    assert payload.get("saveImg") is False
    assert payload.get("takeScreenshot") is False


def test_phase0_regular_backend_launches_with_top_level_session(monkeypatch):
    """When backend=regular (default), Phase 0 uses the legacy top-level
    sessionCookie shape against the regular profile_scraper_id.
    """
    from models.pipeline import PipelineStage
    from workflows.daily_check import detect_accepted_connections

    # Explicitly regular (also covered by default-empty env).
    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "regular")
    monkeypatch.setenv("PB_LI_SESSION_COOKIE", "regular-cookie")
    monkeypatch.setenv("PB_LI_USER_AGENT", "regular-ua")

    conn_sent_attrs = {
        "entry_id": "entry-1",
        "record_id": "rec-1",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
        "experiment_id": None,
        "linkedin_url": "https://www.linkedin.com/in/foo/",
    }

    mock_attio = MagicMock()
    mock_pb = MagicMock()
    mock_launch = MagicMock()
    mock_launch.container_id = "ct-123"
    mock_pb.launch_agent.return_value = mock_launch
    mock_pb.download_result_csv.return_value = ""

    mock_cache = MagicMock()
    mock_cache.get.return_value = ("Person", None, conn_sent_attrs["linkedin_url"], None, None)

    with (
        patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[conn_sent_attrs],
        ),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            return_value="https://sheet-url.example",
        ),
        patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
    ):
        mock_rc.partition.return_value = (
            {},
            [conn_sent_attrs["linkedin_url"]],
        )
        mock_rc.RECHECK_TTL_DAYS = 7

        detect_accepted_connections(
            mock_attio,
            mock_pb,
            profile_scraper_id="legacy-scraper-id",
            cache=mock_cache,
            sales_nav_profile_scraper_id=None,
        )

    # Regular backend uses the legacy scraper id + top-level cookie.
    assert mock_pb.launch_agent.called
    args, _ = mock_pb.launch_agent.call_args
    assert args[0] == "legacy-scraper-id"
    payload = args[1]
    assert payload.get("sessionCookie") == "regular-cookie", (
        "Regular backend must put sessionCookie at top level"
    )
    assert "identities" not in payload, (
        "Regular backend must NOT use the identities[0] shape"
    )

    # The regular path calls pb.get_agent exactly once — the documented-dead
    # preflight (the legacy agent was deleted from the PB workspace; a dead id
    # must fail before launch). It must NOT feed the launch args: no saved-arg
    # fetch on this path (asserted via the top-level-cookie shape above).
    mock_pb.get_agent.assert_called_once_with("legacy-scraper-id")


def test_phase0_pb_timeout_degrades_gracefully(monkeypatch):
    """A PBRunTimeout during the Phase 0 scrape must NOT crash the daily run.

    Pre-fix, the timeout propagated out of detect_accepted_connections and
    aborted cli.py daily before Parts A/B. Now it is caught, logged, and the
    partial result dict (cache-hit accepts + counts) is returned with
    error='pb_timeout' so the caller proceeds to invites + DMs.
    """
    from clients.pb_envelope import PBRunTimeout
    from models.pipeline import PipelineStage
    from workflows.daily_check import detect_accepted_connections

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-scraper-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")

    conn_sent_attrs = {
        "entry_id": "entry-1",
        "record_id": "rec-1",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
        "experiment_id": None,
        "linkedin_url": "https://www.linkedin.com/in/foo/",
    }

    mock_attio = MagicMock()
    mock_pb = MagicMock()
    mock_pb.get_agent.return_value = {
        "argument": json.dumps({"identities": [{"identityId": "x"}]}),
    }
    mock_launch = MagicMock()
    mock_launch.container_id = "ct-timeout"
    mock_pb.launch_agent.return_value = mock_launch
    # The scrape never completes within the window.
    mock_pb.wait_for_completion.side_effect = PBRunTimeout(
        container_id="ct-timeout",
        agent_id="sn-scraper-id",
        elapsed_seconds=750,
        last_observed_status="none",
    )

    mock_cache = MagicMock()
    mock_cache.get.return_value = ("Person", None, conn_sent_attrs["linkedin_url"], None, None)

    with (
        patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[conn_sent_attrs],
        ),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            return_value="https://sheet-url.example",
        ),
        patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
    ):
        mock_rc.partition.return_value = ({}, [conn_sent_attrs["linkedin_url"]])
        mock_rc.RECHECK_TTL_DAYS = 7

        # Must NOT raise — returns the partial dict instead.
        result = detect_accepted_connections(
            mock_attio,
            mock_pb,
            profile_scraper_id="legacy-scraper-id",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-scraper-id",
        )

    assert result.get("error") == "pb_timeout", (
        f"timeout must surface error='pb_timeout'; got {result!r}"
    )
    # CSV download must NOT have been attempted after the timeout.
    mock_pb.download_result_csv.assert_not_called()
    # The wait was attempted (i.e. we got past launch).
    mock_pb.wait_for_completion.assert_called_once()


# ── Phase 0 degrade observability (#21) ──────────────────────────────────────


def _run_phase0_under_timeout(monkeypatch, *, escalate_side_effect=None, wait_exc=None):
    """Drive detect_accepted_connections through the PB-wait degrade branch
    (PBRunTimeout by default; pass ``wait_exc`` to exercise PBRunFailed) with
    escalate() patched. Returns (result, mock_escalate, mock_attio)."""
    from clients.pb_envelope import PBRunTimeout
    from models.pipeline import PipelineStage
    from workflows.daily_check import detect_accepted_connections

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-scraper-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")

    conn_sent_attrs = {
        "entry_id": "entry-1",
        "record_id": "rec-1",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),  # recent enough to pass the 14-day cutoff
        "experiment_id": None,
        "linkedin_url": "https://www.linkedin.com/in/foo/",
    }

    mock_attio = MagicMock()
    mock_pb = MagicMock()
    mock_pb.get_agent.return_value = {
        "argument": json.dumps({"identities": [{"identityId": "x"}]}),
    }
    mock_launch = MagicMock()
    mock_launch.container_id = "ct-timeout"
    mock_pb.launch_agent.return_value = mock_launch
    mock_pb.wait_for_completion.side_effect = wait_exc or PBRunTimeout(
        container_id="ct-timeout",
        agent_id="sn-scraper-id",
        elapsed_seconds=750,
        last_observed_status="none",
    )

    mock_cache = MagicMock()
    mock_cache.get.return_value = ("Person", None, conn_sent_attrs["linkedin_url"], None, None)

    with (
        patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[conn_sent_attrs],
        ),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            return_value="https://sheet-url.example",
        ),
        patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
        patch("workflows.daily_check.escalate", side_effect=escalate_side_effect) as mock_escalate,
    ):
        mock_rc.partition.return_value = ({}, [conn_sent_attrs["linkedin_url"]])
        mock_rc.RECHECK_TTL_DAYS = 7
        result = detect_accepted_connections(
            mock_attio,
            mock_pb,
            profile_scraper_id="legacy-scraper-id",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-scraper-id",
        )
    return result, mock_escalate, mock_attio


def test_phase0_scrape_timeout_is_registered_escalation_type():
    from workflows.escalation_schemas import ESCALATION_TYPES_SET

    assert "phase0_scrape_timeout" in ESCALATION_TYPES_SET


def test_phase0_pb_timeout_opens_escalation(monkeypatch):
    """The degrade path opens a phase0_scrape_timeout operator-review row so a
    silently-degraded run is visible Attio-side, not only in stderr."""
    result, mock_escalate, mock_attio = _run_phase0_under_timeout(monkeypatch)
    assert result.get("error") == "pb_timeout"
    mock_escalate.assert_called_once()
    kwargs = mock_escalate.call_args.kwargs
    assert kwargs["type"] == "phase0_scrape_timeout"
    assert kwargs["idempotency_key"].startswith("phase0-timeout|")
    assert kwargs["attio"] is mock_attio


def test_phase0_pb_timeout_escalation_failure_does_not_crash(monkeypatch):
    """Observability is best-effort: if escalate() itself fails (Attio down, or
    the select option not yet deployed) the degrade path must still return the
    pb_timeout dict rather than introducing a fresh crash."""
    result, mock_escalate, _ = _run_phase0_under_timeout(
        monkeypatch, escalate_side_effect=RuntimeError("attio down"),
    )
    assert result.get("error") == "pb_timeout"
    mock_escalate.assert_called_once()


def test_phase0_scrape_timeout_payload_validates_against_schema():
    """The registered Phase0ScrapeTimeoutPayload must accept the exact payload
    the degrade branch emits and reject a key typo — so a payload-shape drift
    fails in CI rather than slipping silently into Attio (the schema is what
    backs the degrade-branch broad-except being safe to keep)."""
    from workflows.escalation import (
        EscalationSchemaError,
        _validate_payload_against_typeddict,
    )

    good = {
        "run_date": "2026-05-28",
        "backend": "sales_nav",
        "profiles_pending": 54,
        "profiles_deferred": 0,
        "wait_max_seconds": 750,
        "error": "PB run X did not finish in 750s",
    }
    # The exact keys/types daily_check.detect_accepted_connections sends.
    _validate_payload_against_typeddict("phase0_scrape_timeout", good)

    typo = {k: v for k, v in good.items() if k != "wait_max_seconds"}
    typo["max_wait_seconds"] = 750  # renamed key → required field now missing
    with pytest.raises(EscalationSchemaError):
        _validate_payload_against_typeddict("phase0_scrape_timeout", typo)


# ── Phase 0 PBRunFailed degrade (mirror of the PBRunTimeout battery) ─────────


def _pb_run_failed():
    from clients.pb_envelope import PBRunFailed
    return PBRunFailed(
        container_id="ct-failed",
        agent_id="sn-scraper-id",
        log_tail="(error) something broke inside the container",
    )


def test_phase0_pb_failed_degrades_gracefully(monkeypatch):
    """A PBRunFailed (PB reports status="error") during the Phase 0 scrape
    must NOT crash the daily run. Returns partial dict with error='pb_failed'
    so the caller proceeds to invites + DMs. (PR #180.)"""
    from models.pipeline import PipelineStage
    from workflows.daily_check import detect_accepted_connections

    monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
    monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-scraper-id")
    monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "fake-sn-cookie")
    monkeypatch.setenv("PB_LI_USER_AGENT", "fake-ua")

    conn_sent_attrs = {
        "entry_id": "entry-1",
        "record_id": "rec-1",
        "stage": PipelineStage.CONNECTION_SENT.value,
        "last_contact_date": (date.today() - timedelta(days=2)).isoformat(),
        "experiment_id": None,
        "linkedin_url": "https://www.linkedin.com/in/foo/",
    }

    mock_attio = MagicMock()
    mock_pb = MagicMock()
    mock_pb.get_agent.return_value = {
        "argument": json.dumps({"identities": [{"identityId": "x"}]}),
    }
    mock_launch = MagicMock()
    mock_launch.container_id = "ct-failed"
    mock_pb.launch_agent.return_value = mock_launch
    mock_pb.wait_for_completion.side_effect = _pb_run_failed()

    mock_cache = MagicMock()
    mock_cache.get.return_value = ("Person", None, conn_sent_attrs["linkedin_url"], None, None)

    with (
        patch(
            "workflows.daily_check._get_all_entries_parsed",
            return_value=[conn_sent_attrs],
        ),
        patch("workflows.daily_check.recheck_cache") as mock_rc,
        patch(
            "workflows.daily_check.write_prospects_to_sheet",
            return_value="https://sheet-url.example",
        ),
        patch("workflows.daily_check._normalize_linkedin_url", side_effect=lambda u: u),
    ):
        mock_rc.partition.return_value = ({}, [conn_sent_attrs["linkedin_url"]])
        mock_rc.RECHECK_TTL_DAYS = 7

        result = detect_accepted_connections(
            mock_attio,
            mock_pb,
            profile_scraper_id="legacy-scraper-id",
            cache=mock_cache,
            sales_nav_profile_scraper_id="sn-scraper-id",
        )

    assert result.get("error") == "pb_failed", (
        f"PB failure must surface error='pb_failed'; got {result!r}"
    )
    mock_pb.download_result_csv.assert_not_called()
    mock_pb.wait_for_completion.assert_called_once()


def test_phase0_scrape_failed_is_registered_escalation_type():
    from workflows.escalation_schemas import ESCALATION_TYPES_SET
    assert "phase0_scrape_failed" in ESCALATION_TYPES_SET


def test_phase0_pb_failed_opens_escalation(monkeypatch):
    """The PBRunFailed degrade path opens a phase0_scrape_failed operator-review
    row so a silently-degraded run is visible Attio-side. (PR #180.)"""
    result, mock_escalate, mock_attio = _run_phase0_under_timeout(
        monkeypatch, wait_exc=_pb_run_failed(),
    )
    assert result.get("error") == "pb_failed"
    mock_escalate.assert_called_once()
    kwargs = mock_escalate.call_args.kwargs
    assert kwargs["type"] == "phase0_scrape_failed"
    assert kwargs["idempotency_key"].startswith("phase0-failed|")
    assert kwargs["attio"] is mock_attio


def test_phase0_pb_failed_escalation_failure_does_not_crash(monkeypatch):
    """Best-effort: if escalate() itself fails the degrade path still returns
    pb_failed dict rather than introducing a fresh crash. (PR #180.)"""
    result, mock_escalate, _ = _run_phase0_under_timeout(
        monkeypatch,
        escalate_side_effect=RuntimeError("attio down"),
        wait_exc=_pb_run_failed(),
    )
    assert result.get("error") == "pb_failed"
    mock_escalate.assert_called_once()


def test_phase0_scrape_failed_payload_validates_against_schema():
    """The registered Phase0ScrapeFailedPayload must accept the exact payload
    the degrade branch emits and reject a key typo. (PR #180.)"""
    from workflows.escalation import (
        EscalationSchemaError,
        _validate_payload_against_typeddict,
    )

    good = {
        "run_date": "2026-06-09",
        "backend": "sales_nav",
        "profiles_pending": 54,
        "profiles_deferred": 0,
        "container_id": "ct-failed",
        "error": "PB run ct-failed (agent=sn-scraper-id) reported status=error.",
    }
    _validate_payload_against_typeddict("phase0_scrape_failed", good)

    typo = {k: v for k, v in good.items() if k != "container_id"}
    typo["containerId"] = "ct-failed"  # renamed key → required field now missing
    with pytest.raises(EscalationSchemaError):
        _validate_payload_against_typeddict("phase0_scrape_failed", typo)
