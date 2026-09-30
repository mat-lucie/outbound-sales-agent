"""Shared test fakes for required collaborator parameters.

``run_dm_sequencing`` (PR-17) and ``run_connection_requests`` (the #182 port)
both require a ``daily_run`` argument. Tests that don't assert on cap-charging
behaviour use this minimal stand-in.
"""
from unittest.mock import MagicMock

from workflows.daily_run import DailyRun


def fake_daily_run(remaining: int = 25) -> MagicMock:
    """A DailyRun stand-in for call sites that require the parameter.

    ``remaining()`` returns an int so ``min()`` arithmetic in the invite
    target-trim works; ``reserve_send`` returns a stable token;
    ``confirm_lease``/``release_lease`` are recorded no-ops queryable via
    MagicMock assertions.
    """
    mock = MagicMock(spec=DailyRun)
    mock.remaining.return_value = remaining
    mock.reserve_send.return_value = "fake-lease-token"
    return mock


def make_wave2_person(record_id, first_name, last_name, email, stage, country="US"):
    """Attio person record in the shape ``run_wave2_blast``'s batch builder
    reads (name, email_addresses, email_campaign_stage, primary_location,
    company). Shared by the wave-2 test files so the fixture can't drift from
    the shape one of them asserts against.
    """
    return {
        "id": {"record_id": record_id},
        "values": {
            "name": [{"first_name": first_name, "last_name": last_name}],
            "email_addresses": [{"email_address": email}],
            "email_campaign_stage": [{"value": stage}],
            "primary_location": [{"country_code": country}],
            "company": [],
        },
    }



def email_guard_pass_response(stage):
    """Person-record payload for ``attio._request`` so
    ``verify_email_send_preconditions``'s re-read sees ``stage`` and allows
    the send (the email-channel sibling of ``stub_guard_reread``)."""
    return {"data": {"values": {"email_campaign_stage": [{"value": stage}]}}}



def stub_guard_reread(attio: MagicMock, entries: list[dict], *, owner: str = "") -> None:
    """Configure ``attio._request`` to serve Phase-3 send-guard re-reads.

    The guard calls ``attio._request("GET", "/lists/{lid}/entries/{entry_id}")``
    immediately before each send.  Without this stub a bare ``MagicMock()``
    returns another ``MagicMock`` whose ``entry_values`` cannot be parsed by
    ``_extract_stage_from_raw``, causing the guard to see stage=``""`` (mismatch
    → send blocked) in every test that doesn't explicitly configure the response.

    Call this after creating ``attio = MagicMock()`` in any test whose entries
    pass through the invite or DM send path (i.e. any test where the guard's
    Attio re-read must *not* block the send):

        attio = MagicMock()
        attio.query_list_entries.return_value = [entry]
        stub_guard_reread(attio, [entry])

    Args:
        attio:   The ``MagicMock`` that stands in for ``AttioClient``.
        entries: The same list you pass to ``attio.query_list_entries.return_value``
                 (raw Attio shape with ``entry_id`` + either a top-level ``stage``
                 key or ``entry_values["stage"]``), OR already-parsed flat dicts
                 with ``entry_id`` and ``stage``.
        owner:   Owner string to embed in the response (default ``""`` = unassigned /
                 single-operator mode; the guard skips the owner check when
                 ``expected_owner`` is ``None``).
    """
    # Build entry_id → raw stage payload map from either raw or parsed entry shapes.
    # We preserve the original value shape so real-API-shaped fixtures keep their
    # shape through the stub (instead of always normalising to {"value": stage}).
    stage_payload_map: dict[str, object] = {}
    for e in entries:
        entry_id = e.get("entry_id", "")
        if not entry_id:
            continue
        # Parsed (flat) shape: stage is a plain string — normalise to plain-value shape.
        stage = e.get("stage")
        if isinstance(stage, str):
            stage_payload_map[entry_id] = [{"value": stage}]
            continue
        # Raw Attio shape: entry_values["stage"] is already a list or scalar —
        # pass the original value through unchanged so real-API fixtures keep their shape.
        ev = e.get("entry_values") or {}
        raw = ev.get("stage")
        if raw is not None:
            stage_payload_map[entry_id] = raw

    owner_payload = [{"value": owner}] if owner else []

    def _request_side_effect(method: str, path: str, **kwargs):
        if (method == "POST" and path == "/objects/operator_review_queue/records/query"
                and "type" in kwargs.get("json", {}).get("filter", {})):
            return {"data": []}  # clean queue for delivery tests using this fixture
        if method == "GET" and "/entries/" in path:
            entry_id = path.split("/entries/")[-1]
            stage_raw = stage_payload_map.get(entry_id, [{"value": ""}])
            return {
                "data": {
                    "entry_values": {
                        "stage": stage_raw,
                        "owner": owner_payload,
                    }
                }
            }
        # Non-guard calls (claim re-reads, etc.): return a plain MagicMock so
        # existing behaviour for other _request consumers is preserved.
        return MagicMock()

    attio._request.side_effect = _request_side_effect
