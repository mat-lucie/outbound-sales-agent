"""Tests for clients.attio.request_with_retry — the bounded jittered
retry wrapper around AttioClient._request used by escalation queue-row
writes.

Read/query retries and pre-connect retries are preserved. Ambiguous creates
recover only from a positive reconciliation; misses and failed probes halt.
"""

from unittest.mock import MagicMock, patch

import httpx
import pytest

import clients.attio as attio_mod
from clients.attio import AmbiguousAttioWrite, AttioClient, request_with_retry


def _http_error(status: int, url: str = "https://api.attio.com/v2/x") -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"{status}", request=request, response=response)


@pytest.fixture
def attio() -> AttioClient:
    return AttioClient(api_key="test-key")


@pytest.fixture
def no_sleep(monkeypatch) -> MagicMock:
    """Neutralize backoff sleeps; returns the mock so tests can assert waits."""
    sleeper = MagicMock()
    monkeypatch.setattr(attio_mod.time, "sleep", sleeper)
    return sleeper


class TestRequestWithRetry:
    def test_success_first_try_no_sleep(self, attio, no_sleep) -> None:
        with patch.object(attio, "_request", return_value={"data": []}) as mock_req:
            result = request_with_retry(attio, "POST", "/objects/x/records/query", json={})

        assert result == {"data": []}
        mock_req.assert_called_once_with("POST", "/objects/x/records/query", json={})
        no_sleep.assert_not_called()

    def test_transient_500_then_success(self, attio, no_sleep) -> None:
        """A safe query survives two transient 500 responses."""
        with patch.object(
            attio, "_request",
            side_effect=[_http_error(500), _http_error(500), {"data": {"id": "rec_1"}}],
        ) as mock_req:
            result = request_with_retry(attio, "POST", "/objects/x/records/query", json={})

        assert result == {"data": {"id": "rec_1"}}
        assert mock_req.call_count == 3
        assert no_sleep.call_count == 2

    def test_connection_error_then_success(self, attio, no_sleep) -> None:
        with patch.object(
            attio, "_request",
            side_effect=[httpx.ConnectError("boom"), {"data": []}],
        ) as mock_req:
            result = request_with_retry(attio, "GET", "/x", json={})

        assert result == {"data": []}
        assert mock_req.call_count == 2

    def test_4xx_raises_immediately_no_retry(self, attio, no_sleep) -> None:
        """4xx is a caller bug (bad payload, auth) — retrying can't fix it."""
        with patch.object(attio, "_request", side_effect=_http_error(400)) as mock_req, \
                pytest.raises(httpx.HTTPStatusError):
            request_with_retry(attio, "GET", "/x", json={})

        mock_req.assert_called_once()
        no_sleep.assert_not_called()

    def test_404_raises_immediately(self, attio, no_sleep) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(404)) as mock_req, \
                pytest.raises(httpx.HTTPStatusError):
            request_with_retry(attio, "GET", "/x", json={})

        mock_req.assert_called_once()

    def test_persistent_500_exhausts_attempts_and_raises(self, attio, no_sleep) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(500)) as mock_req, \
                pytest.raises(httpx.HTTPStatusError):
            request_with_retry(attio, "GET", "/x", json={})

        assert mock_req.call_count == 5  # default attempts
        assert no_sleep.call_count == 4  # no sleep after the final failure

    def test_persistent_connection_error_exhausts_and_raises(self, attio, no_sleep) -> None:
        with patch.object(attio, "_request", side_effect=httpx.ReadTimeout("t")) as mock_req, \
                pytest.raises(httpx.ReadTimeout):
            request_with_retry(attio, "GET", "/x", json={})

        assert mock_req.call_count == 5

    def test_non_http_errors_propagate_immediately(self, attio, no_sleep) -> None:
        """Bugs (TypeError etc.) must not be masked by retries."""
        with patch.object(attio, "_request", side_effect=TypeError("bug")) as mock_req, \
                pytest.raises(TypeError):
            request_with_retry(attio, "GET", "/x", json={})

        mock_req.assert_called_once()

    def test_attempts_override(self, attio, no_sleep) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(503)) as mock_req, \
                pytest.raises(httpx.HTTPStatusError):
            request_with_retry(attio, "GET", "/x", attempts=2, json={})

        assert mock_req.call_count == 2

    def test_backoff_is_exponential_jittered_and_capped(self, attio, no_sleep) -> None:
        """Waits grow ~2^attempt with jitter in [0.5x, 1.5x], capped at max_wait."""
        with patch.object(attio, "_request", side_effect=_http_error(500)), \
                pytest.raises(httpx.HTTPStatusError):
            request_with_retry(attio, "GET", "/x", json={})

        waits = [c.args[0] for c in no_sleep.call_args_list]
        assert len(waits) == 4
        for i, wait in enumerate(waits):
            base = min(2.0 ** i, 30.0)
            assert 0.5 * base <= wait <= 1.5 * base

    def test_attempts_below_one_rejected(self, attio, no_sleep) -> None:
        with pytest.raises(ValueError, match="attempts must be >= 1"):
            request_with_retry(attio, "GET", "/x", attempts=0, json={})


class TestRecheck:
    """`recheck` — the non-idempotent-write safety valve: probe whether a
    lost-response write landed without re-issuing it."""

    def test_recheck_not_called_on_first_attempt(self, attio, no_sleep) -> None:
        recheck = MagicMock(return_value=None)
        with patch.object(attio, "_request", return_value={"data": {}}):
            request_with_retry(attio, "POST", "/x", recheck=recheck, json={})

        recheck.assert_not_called()

    def test_recheck_hit_short_circuits_retry(self, attio, no_sleep) -> None:
        """The landed-write case: first POST 'fails' (response lost), the
        probe finds the committed row — no second POST is issued."""
        landed = {"data": {"id": {"record_id": "rec_landed"}}}
        recheck = MagicMock(return_value=landed)
        with patch.object(attio, "_request", side_effect=_http_error(500)) as mock_req:
            result = request_with_retry(attio, "POST", "/x", recheck=recheck, json={})

        assert result is landed
        mock_req.assert_called_once()  # no re-POST
        recheck.assert_called_once()

    def test_recheck_miss_halts_without_replay(self, attio, no_sleep) -> None:
        recheck = MagicMock(return_value=None)
        with patch.object(attio, '_request', side_effect=_http_error(500)) as req, pytest.raises(AmbiguousAttioWrite):
            request_with_retry(attio, "POST", "/x", recheck=recheck)
        req.assert_called_once()
        recheck.assert_called_once()
        no_sleep.assert_not_called()

    def test_recheck_failure_preserves_ambiguity(self, attio, no_sleep) -> None:
        with patch.object(attio, '_request', side_effect=_http_error(500)) as req, pytest.raises(AmbiguousAttioWrite):
            request_with_retry(attio, "POST", "/x", recheck=MagicMock(side_effect=ValueError("bad probe")))
        req.assert_called_once()
