from unittest.mock import MagicMock

import pytest

from workflows.run_evidence import observed, record_run


def test_timing_records_failure_and_never_serializes_arguments():
    audit = MagicMock()

    @observed("test", "api")
    def operation(secret):
        raise ValueError(secret)

    with record_run(audit, {"sha": "known"}), pytest.raises(ValueError):
        operation("secret-do-not-record")
    assert "secret-do-not-record" not in str(audit.mock_calls)
    finished = audit.event.call_args.kwargs
    assert finished["status"] == "failed"
    assert finished["error_type"] == "ValueError"
    assert finished["wall_seconds"] >= 0


def test_exact_container_and_result_hash_are_recorded_without_body():
    audit = MagicMock()
    launch = MagicMock(container_id="exact-container")

    @observed("pb.result_csv", "api")
    def result(client, launch):
        return "private-prospect-body"

    with record_run(audit, {}):
        result(None, launch)
    fields = audit.event.call_args.kwargs
    assert fields["container_id"] == "exact-container"
    assert len(fields["sha256"]) == 64
    assert "private-prospect-body" not in str(audit.mock_calls)
