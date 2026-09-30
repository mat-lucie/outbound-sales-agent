"""Regression coverage for live Attio record-reference responses (LUC-388)."""

from clients.attio import AttioClient


def test_merged_into_returns_target_id_from_live_reference_shape():
    entry = {
        "entry_values": {
            "merged_into": [{
                "attribute_type": "record-reference",
                "target_object": "people",
                "target_record_id": "winner-id",
                "active_from": "2026-09-28T13:55:46Z",
                "active_until": None,
            }],
        },
    }
    assert AttioClient.parse_entry(entry)["merged_into"] == "winner-id"


def test_missing_reference_remains_none():
    assert AttioClient.parse_entry({"entry_values": {"merged_into": []}})["merged_into"] is None


def test_incomplete_reference_remains_truthy_for_duplicate_suppression():
    reference = {"attribute_type": "record-reference", "target_object": "people"}
    assert AttioClient._extract_value({"merged_into": [reference]}, "merged_into") == reference


def test_scalar_and_select_values_keep_existing_shapes():
    assert AttioClient._extract_value({"dm_step": [{"value": 3}]}, "dm_step") == 3
    assert AttioClient._extract_value(
        {"stage": [{"attribute_type": "select", "option": {"title": "Accepted"}}]}, "stage"
    ) == "Accepted"
