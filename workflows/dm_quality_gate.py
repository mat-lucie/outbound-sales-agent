"""Delivery-time enforcement of the existing three-type P0 policy."""
from models.data_quality_report import assert_no_open_p0_alarms


class DataQualityUnavailable(RuntimeError):
    """The queue cannot prove that delivery is safe."""


def require_clear_dm_quality_queue(attio) -> None:
    """Read fresh before every live batch; no cached or partial-success fallback."""
    def count_open(slug):
        try:
            response = attio._request(
                "POST", "/objects/operator_review_queue/records/query",
                json={"filter": {"type": {"$eq": slug}, "status": {"$eq": "open"}}, "limit": 1},
            )
            if not isinstance(response, dict) or not isinstance(response.get("data"), list):
                raise ValueError("queue response must contain a data list")
            # Existence is sufficient. limit=1 cannot hide a matching alarm.
            return len(response["data"])
        except Exception as exc:
            raise DataQualityUnavailable(
                f"P0 data-quality check unavailable ({slug}); DM delivery halted. "
                "Restore queue access and rerun the check."
            ) from exc

    assert_no_open_p0_alarms(count_open)
