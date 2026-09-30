from unittest.mock import patch

from clients.attio import AttioClient, AttioResultTruncated
from workflows.metrics import DailyRunMetrics


def records(ids):
    return [{"id": {"record_id": rid}, "values": {"name": [{"value": rid}]}} for rid in ids]


def test_large_pipeline_preserves_all_identities_without_individual_reads():
    ids = {f"person-{i}" for i in range(2640)}
    metrics = DailyRunMetrics()
    with (
        AttioClient(api_key="test") as client,
        patch.object(client, "_query_paginated", return_value=records(ids)) as query,
        patch.object(client, "get_person", side_effect=AssertionError("N+1 read")),
    ):
        result = client.bulk_fetch_persons_by_record_ids(ids, metrics=metrics)
    assert set(result) == ids
    assert metrics.bulk_fetch_records_returned == len(ids)
    query.assert_called_once_with("/objects/people/records/query", None, 50_000, fail_if_truncated=True)


def test_missing_record_gets_targeted_fresh_read():
    ids = {str(i) for i in range(100)}
    with (
        AttioClient(api_key="test") as client,
        patch.object(client, "_query_paginated", return_value=records(ids - {"7"})),
        patch.object(client, "get_person", return_value=records(["7"])[0]) as get,
    ):
        result = client.bulk_fetch_persons_by_record_ids(ids)
    assert set(result) == ids
    get.assert_called_once_with("7", retry_500=False)


def test_truncated_query_never_becomes_complete_index():
    ids = {str(i) for i in range(100)}
    metrics = DailyRunMetrics()
    with (
        AttioClient(api_key="test") as client,
        patch.object(client, "_query_paginated", side_effect=AttioResultTruncated("limit")),
        patch.object(client, "get_person", side_effect=lambda rid, **_: records([rid])[0]) as get,
    ):
        result = client.bulk_fetch_persons_by_record_ids(ids, metrics=metrics)
    assert set(result) == ids and get.call_count == 100
    assert len(metrics.runtime_warnings) == 1


def test_company_bulk_primes_display_fields_without_priming_mutable_country_gate():
    ids = {f"company-{i}" for i in range(1854)}
    with AttioClient(api_key="test") as client:
        with patch.object(client, "_query_paginated", return_value=records(ids)), \
                patch.object(client, "get_company", side_effect=AssertionError("N+1 read")):
            assert client.bulk_prime_company_caches(ids) == len(ids)
        assert set(client._company_cache) == ids
        assert client._company_hq_country_cache == {}


def test_malformed_company_query_clears_partial_cache_and_repairs_individually():
    ids = {str(i) for i in range(100)}
    queried = records(ids)
    bad = next(record for record in queried if record["id"]["record_id"] == "7")
    bad["values"]["domains"] = [{"domain": None}]
    metrics = DailyRunMetrics()
    with AttioClient(api_key="test") as client:
        with patch.object(client, "_query_paginated", return_value=queried), \
                patch.object(client, "get_company", return_value=records(["7"])[0]) as get:
            assert client.bulk_prime_company_caches(ids, metrics=metrics) == 100
        get.assert_called_once_with("7", retry_500=False)
        assert client._company_cache["7"] == "7"
        assert "7" in client._industry_cache
        assert client._company_corruption_cache["7"] is False
    assert metrics.bulk_fetch_companies_returned == 100
    assert any("parse failed for 7" in warning for warning in metrics.runtime_warnings)
