"""Fresh pipeline-stage checks for the public single-operator engine."""
from __future__ import annotations

import os

from clients.attio import AttioClient
from clients.crm.mapping import load_crm_mapping
from models.pipeline import is_send_eligible
from workflows.email_send_guard import GuardResult


def verify_send_preconditions(
    attio: AttioClient, entry_id: str, expected_stage: str, *,
    list_id: str | None = None,
) -> GuardResult:
    """Fail closed on missing, changed, suppressed or unreadable entries."""
    try:
        lid = list_id or os.environ["ATTIO_LIST_ID"]
        response = attio._request("GET", f"/lists/{lid}/entries/{entry_id}")
        if not isinstance(response, dict):
            raise ValueError("Entry response is not an object")
        entry = response.get("data", response)
        if not isinstance(entry, dict) or not isinstance(entry.get("entry_values"), dict):
            raise ValueError("Entry response has no entry values")
        attrs = AttioClient.parse_entry(entry)
        stage_data = entry["entry_values"].get("stage", [])
        item = stage_data[0] if isinstance(stage_data, list) and stage_data else stage_data
        vendor_stage = ""
        if isinstance(item, str):
            vendor_stage = item
        elif isinstance(item, dict):
            for field in ("status", "option"):
                option = item.get(field)
                if isinstance(option, dict) and isinstance(option.get("title"), str):
                    vendor_stage = option["title"]
                    break
            if not vendor_stage and isinstance(item.get("value"), str):
                vendor_stage = item["value"]
        stage = load_crm_mapping().to_canonical_stage(vendor_stage)
        if stage != expected_stage:
            return GuardResult(False, "stage_moved", stage)
        attrs["stage"] = stage
        if not is_send_eligible(attrs):
            return GuardResult(False, "suppressed", stage)
        return GuardResult(True, actual_stage=stage)
    except Exception:
        return GuardResult(False, "reread_failed")
