"""Tests for AttioClient delete helpers used by the dedup script."""

from unittest.mock import patch

import httpx
import pytest

from clients.attio import AmbiguousAttioWrite, AttioClient


def _http_error(status: int, url: str = "https://api.attio.com/v2/x") -> httpx.HTTPStatusError:
    request = httpx.Request("DELETE", url)
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"{status}", request=request, response=response)


@pytest.fixture
def attio() -> AttioClient:
    return AttioClient(api_key="test-key")


class TestDeletePerson:
    def test_returns_true_on_success(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", return_value={}) as mock_req:
            result = attio.delete_person("abc-123")

        assert result is True
        mock_req.assert_called_once_with("DELETE", "/objects/people/records/abc-123")

    def test_returns_false_on_404(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(404)):
            assert attio.delete_person("missing") is False

    def test_raises_on_other_errors(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(500)), pytest.raises(httpx.HTTPStatusError):
            attio.delete_person("abc-123")


class TestDeleteCompany:
    def test_returns_true_on_success(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", return_value={}) as mock_req:
            result = attio.delete_company("co-123")

        assert result is True
        mock_req.assert_called_once_with("DELETE", "/objects/companies/records/co-123")

    def test_returns_false_on_404(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(404)):
            assert attio.delete_company("missing") is False

    def test_raises_on_other_errors(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(403)), pytest.raises(httpx.HTTPStatusError):
            attio.delete_company("co-123")


class TestDeleteListEntry:
    def test_uses_explicit_list_id(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", return_value={}) as mock_req:
            result = attio.delete_list_entry("entry-9", list_id="list-xyz")

        assert result is True
        mock_req.assert_called_once_with("DELETE", "/lists/list-xyz/entries/entry-9")

    def test_falls_back_to_env_list_id(self, attio: AttioClient, monkeypatch) -> None:
        monkeypatch.setenv("ATTIO_LIST_ID", "env-list")
        with patch.object(attio, "_request", return_value={}) as mock_req:
            attio.delete_list_entry("entry-9")

        mock_req.assert_called_once_with("DELETE", "/lists/env-list/entries/entry-9")

    def test_returns_false_on_404(self, attio: AttioClient) -> None:
        with patch.object(attio, "_request", side_effect=_http_error(404)):
            assert attio.delete_list_entry("missing", list_id="list-xyz") is False


def _response(status: int, json_body: dict | None = None) -> httpx.Response:
    request = httpx.Request("POST", "https://api.attio.com/v2/x")
    return httpx.Response(
        status, request=request,
        json=json_body if json_body is not None else {"data": []},
    )


class TestRequest500Retry:
    """Opt-in 500 retry (2026-07-18 weekly-finalize crash loop).

    500 stays fatal by default: on a non-idempotent POST (note/record
    create) the write may have committed server-side, so a blanket retry
    risks double-writes. Idempotent call sites opt in via retry_500=True.
    """

    def test_500_fatal_by_default(self, attio: AttioClient) -> None:
        with patch.object(attio._client, 'request', return_value=_response(500)) as mock_req, pytest.raises(AmbiguousAttioWrite):
            attio._request("POST", "/objects/people/records", json={})
        mock_req.assert_called_once()

    def test_500_retried_when_opted_in(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": [{"id": "r1"}]})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep") as mock_sleep:
            data = attio._request("POST", "/objects/people/records/query", json={}, retry_500=True)
        assert data == {"data": [{"id": "r1"}]}
        assert mock_req.call_count == 2
        mock_sleep.assert_called_once()

    def test_500_raises_after_retries_exhausted(self, attio: AttioClient) -> None:
        with patch.object(attio._client, "request", return_value=_response(500)) as mock_req, \
                patch("clients.attio.time.sleep"), pytest.raises(httpx.HTTPStatusError):
            attio._request("GET", "/x", retries=3, retry_500=True)
        assert mock_req.call_count == 3

    def test_502_still_retried_by_default(self, attio: AttioClient) -> None:
        responses = [_response(502), _response(200, {"data": []})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep"):
            data = attio._request("GET", "/x")
        assert data == {"data": []}
        assert mock_req.call_count == 2

    def test_query_paginated_survives_transient_500(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": [{"id": "r1"}]})]
        with patch.object(attio._client, "request", side_effect=responses), \
                patch("clients.attio.time.sleep"):
            records = attio._query_paginated("/objects/people/records/query", None, limit=10)
        assert records == [{"id": "r1"}]

    def test_create_person_500_stays_fatal(self, attio: AttioClient) -> None:
        with patch.object(attio._client, 'request', return_value=_response(500)) as mock_req, pytest.raises(AmbiguousAttioWrite):
            attio.create_person({"name": "X"})
        mock_req.assert_called_once()

    def test_no_sleep_after_final_attempt(self, attio: AttioClient) -> None:
        with patch.object(attio._client, "request", return_value=_response(500)), \
                patch("clients.attio.time.sleep") as mock_sleep, pytest.raises(httpx.HTTPStatusError):
            attio._request("GET", "/x", retries=3, retry_500=True)
        assert mock_sleep.call_count == 2  # sleeps between attempts, not before the raise

    # Pin the per-method opt-ins: a future edit dropping retry_500=True from
    # one of these silently regresses the 2026-07-18 crash-loop fix while the
    # suite stays green.
    def test_get_person_retries_500_by_default(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": {"id": "p1"}})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep"):
            assert attio.get_person("p1") == {"id": "p1"}
        assert mock_req.call_count == 2

    def test_get_person_opt_out_fails_fast(self, attio: AttioClient) -> None:
        with patch.object(attio._client, 'request', return_value=_response(500)) as mock_req, pytest.raises(httpx.HTTPStatusError):
            attio.get_person("p1", retry_500=False)
        mock_req.assert_called_once()

    def test_bulk_fetch_fails_fast_on_systemic_500(self, attio: AttioClient) -> None:
        with patch.object(attio._client, "request", return_value=_response(500)) as mock_req, \
                patch("clients.attio.time.sleep") as mock_sleep:
            result = attio.bulk_fetch_persons_by_record_ids({"p1", "p2"}, max_workers=2)
        assert result == {}  # fail-open: records dropped, not raised
        assert mock_req.call_count == 2  # one attempt per record, no retries
        mock_sleep.assert_not_called()

    def test_get_company_retries_500(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": {"id": "c1"}})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep"):
            assert attio.get_company("c1") == {"id": "c1"}
        assert mock_req.call_count == 2

    def test_update_person_retries_500(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": {"id": "p1"}})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep"):
            assert attio.update_person("p1", {"name": "X"}) == {"id": "p1"}
        assert mock_req.call_count == 2

    def test_native_put_upsert_retries_500(self, attio: AttioClient) -> None:
        responses = [_response(500), _response(200, {"data": {"id": "p1"}})]
        with patch.object(attio._client, "request", side_effect=responses) as mock_req, \
                patch("clients.attio.time.sleep"):
            assert attio.upsert_person("email_addresses", {"email_addresses": "x@y.z"}) == {"id": "p1"}
        assert mock_req.call_count == 2


class TestLinkedInUrlCanonicalization:
    """URL-encoded, www, and trailing-slash variants all collapse to one form.

    Attio's text-field filter is exact-string. Without canonicalization the
    same LinkedIn profile creates a fresh duplicate each time PB returns a
    subtly different URL encoding — the root cause of the recurring
    duplicate-Attio-records bug.
    """

    def test_canonical_strips_www_slash_and_encoding(self) -> None:
        from clients.attio import _canonical_linkedin_url

        canonical = "https://linkedin.com/in/iñigo-marchal-b8901186"
        variants = [
            "https://www.linkedin.com/in/iñigo-marchal-b8901186",
            "https://linkedin.com/in/iñigo-marchal-b8901186",
            "https://www.linkedin.com/in/iñigo-marchal-b8901186/",
            "https://www.linkedin.com/in/i%C3%B1igo-marchal-b8901186",
            "HTTPS://www.linkedin.com/in/iñigo-marchal-b8901186",
        ]
        for v in variants:
            assert _canonical_linkedin_url(v) == canonical, v

    def test_variants_list_includes_www_and_slash_forms(self) -> None:
        from clients.attio import _linkedin_url_variants

        variants = _linkedin_url_variants("https://www.linkedin.com/in/foo")
        assert "https://linkedin.com/in/foo" in variants
        assert "https://www.linkedin.com/in/foo" in variants
        assert "https://www.linkedin.com/in/foo/" in variants
        # De-duplicated
        assert len(variants) == len(set(variants))

    def test_search_tries_canonical_first_then_variants(self, attio: AttioClient) -> None:
        """When Attio stored the www-prefixed form, a no-www search must still find it."""
        stored = {"id": {"record_id": "rec-1"}, "values": {"linkedin": [{"value": "https://www.linkedin.com/in/foo"}]}}
        calls: list[dict] = []

        def fake_request(method, path, json=None, **_):
            calls.append({"method": method, "path": path, "filter": json.get("filter") if json else None})
            f = (json or {}).get("filter", {})
            if f.get("linkedin") == "https://www.linkedin.com/in/foo":
                return {"data": [stored]}
            return {"data": []}

        with patch.object(attio, "_request", side_effect=fake_request):
            # Input is URL-encoded + no-www — neither matches Attio directly.
            result = attio.search_person_by_linkedin("https://linkedin.com/in/foo")
        assert result is stored
        # First variant tried is canonical (no-www), then with-www
        filters = [c["filter"]["linkedin"] for c in calls if c["filter"]]
        assert filters[0] == "https://linkedin.com/in/foo"
        assert "https://www.linkedin.com/in/foo" in filters


class TestUpsertPersonLinkedInDedup:
    """upsert_person must NOT create a duplicate when URL differs only in encoding."""

    def test_upsert_finds_existing_despite_www_mismatch(self, attio: AttioClient) -> None:
        stored = {"id": {"record_id": "rec-existing"}, "values": {"linkedin": [{"value": "https://www.linkedin.com/in/bar"}]}}

        def fake_request(method, path, json=None, params=None, **_):
            if method == "POST" and path == "/objects/people/records/query":
                f = (json or {}).get("filter", {})
                target = f.get("linkedin", "")
                return {"data": [stored] if target in ("https://www.linkedin.com/in/bar", "https://linkedin.com/in/bar") else []}
            if method == "PATCH":
                return {"data": stored}
            raise AssertionError(f"Unexpected {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.upsert_person(
                matching_attribute="linkedin",
                attributes={"linkedin": "https://www.linkedin.com/in/bar/", "name": [{"full_name": "Bar"}]},
            )
        assert result is stored

    def test_add_list_entry_upserts_when_record_already_has_entry(self, attio: AttioClient) -> None:
        """If the record already has a list entry, add_list_entry PATCHes
        instead of POSTing a fresh duplicate. Prevents the 243-record
        list-entry-accumulation bug we hit on 2026-04-21."""
        existing_entry = {
            "id": {"entry_id": "entry-existing", "list_id": "list-X"},
            "parent_record_id": "rec-1",
            "entry_values": {"stage": [{"status": {"title": "Prospect"}}]},
            "created_at": "2026-04-01T00:00:00Z",
        }

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [existing_entry]}
            if method == "PATCH" and path == "/lists/list-X/entries/entry-existing":
                return {"data": {"id": {"entry_id": "entry-existing"}, "values": json}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.add_list_entry(
                record_id="rec-1",
                stage_name="Connection Sent",
                entry_attributes={"persona": "operations_leaders"},
                list_id="list-X",
            )
        assert result["id"]["entry_id"] == "entry-existing"

    def test_add_list_entry_creates_new_when_record_has_no_entry(self, attio: AttioClient) -> None:
        """No existing entry for this record → POST a fresh entry."""
        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": []}  # record has no entry in this list yet
            if method == "POST" and path == "/lists/list-X/entries":
                return {"data": {"id": {"entry_id": "entry-new"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.add_list_entry(
                record_id="rec-brand-new",
                stage_name="Prospect",
                list_id="list-X",
            )
        assert result["id"]["entry_id"] == "entry-new"

    def test_add_list_entry_picks_most_advanced_when_multiple_exist(self, attio: AttioClient) -> None:
        """Legacy records with multiple list entries: upsert patches the
        most-advanced one (highest stage rank)."""
        prospect = {
            "id": {"entry_id": "entry-prospect", "list_id": "list-X"},
            "parent_record_id": "rec-1",
            "entry_values": {"stage": [{"status": {"title": "Prospect"}}]},
            "created_at": "2026-04-01T00:00:00Z",
        }
        dm1 = {
            "id": {"entry_id": "entry-dm1", "list_id": "list-X"},
            "parent_record_id": "rec-1",
            "entry_values": {"stage": [{"status": {"title": "DM1 Sent"}}]},
            "created_at": "2026-04-05T00:00:00Z",
        }
        patched: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [prospect, dm1]}
            if method == "PATCH":
                patched["path"] = path
                return {"data": {"id": {"entry_id": path.rsplit("/", 1)[-1]}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.add_list_entry(
                record_id="rec-1",
                stage_name="DM2 Sent",
                list_id="list-X",
            )
        assert patched["path"].endswith("/entries/entry-dm1")
        assert result["id"]["entry_id"] == "entry-dm1"

    def test_add_list_entry_skips_query_when_existing_entries_supplied(self, attio: AttioClient) -> None:
        """When the caller passes pre-loaded `existing_entries`, add_list_entry
        must NOT trigger a fresh `query_list_entries(limit=50000)` scan. Without
        this kwarg, batch flows like weekly_prospect.py issue one full-list
        scan per qualified prospect — guaranteed Attio 429 throttling at scale.
        """
        existing_entry = {
            "id": {"entry_id": "entry-pre", "list_id": "list-X"},
            "parent_record_id": "rec-1",
            "entry_values": {"stage": [{"status": {"title": "Prospect"}}]},
            "created_at": "2026-04-01T00:00:00Z",
        }

        def fake_request(method, path, json=None, **_):
            # The /entries/query endpoint must NOT be hit — that's the whole point.
            if method == "POST" and path.endswith("/entries/query"):
                raise AssertionError("query_list_entries was called despite existing_entries kwarg")
            if method == "PATCH" and path == "/lists/list-X/entries/entry-pre":
                return {"data": {"id": {"entry_id": "entry-pre"}, "values": json}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.add_list_entry(
                record_id="rec-1",
                stage_name="Connection Sent",
                list_id="list-X",
                existing_entries=[existing_entry],
            )
        assert result["id"]["entry_id"] == "entry-pre"

    def test_add_list_entry_existing_entries_filters_by_record_id(self, attio: AttioClient) -> None:
        """Pre-loaded entries can include OTHER records' entries — add_list_entry
        must filter to the target record_id and POST a fresh entry if none match.
        """
        unrelated = {
            "id": {"entry_id": "entry-other", "list_id": "list-X"},
            "parent_record_id": "rec-OTHER",
            "entry_values": {"stage": [{"status": {"title": "Prospect"}}]},
            "created_at": "2026-04-01T00:00:00Z",
        }

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                raise AssertionError("query_list_entries should not be called")
            if method == "POST" and path == "/lists/list-X/entries":
                return {"data": {"id": {"entry_id": "entry-fresh"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            result = attio.add_list_entry(
                record_id="rec-1",
                stage_name="Prospect",
                list_id="list-X",
                existing_entries=[unrelated],
            )
        assert result["id"]["entry_id"] == "entry-fresh"

    # ── Fix 3: defense-in-depth stage-regression backstop ──────────────────
    # The weekly re-stamp class PATCHed an existing entry with a lower stage
    # (Accepted/Prospect over DM3 Sent), wiping cadence depth. add_list_entry
    # now drops a regressing `stage` (and `dm_step`) key on the existing-entry
    # PATCH while letting the other attrs through, and logs loudly.

    @staticmethod
    def _existing_entry(stage_title: str) -> dict:
        return {
            "id": {"entry_id": "entry-existing", "list_id": "list-X"},
            "parent_record_id": "rec-1",
            "entry_values": {"stage": [{"status": {"title": stage_title}}]},
            "created_at": "2026-04-01T00:00:00Z",
        }

    def test_add_list_entry_drops_regressing_stage_keeps_other_attrs(
        self, attio: AttioClient, caplog
    ) -> None:
        existing = self._existing_entry("DM3 Sent")
        patched: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [existing]}
            if method == "PATCH" and path == "/lists/list-X/entries/entry-existing":
                patched["values"] = json["data"]["entry_values"]
                return {"data": {"id": {"entry_id": "entry-existing"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request), \
                caplog.at_level("WARNING"):
            attio.add_list_entry(
                record_id="rec-1",
                stage_name="Accepted",  # rank 2 < DM3 Sent rank 5 → regression
                entry_attributes={"dm_step": 0, "persona": "operations_leaders"},
                list_id="list-X",
            )

        # stage AND dm_step stripped together (the cadence-depth pair) so the
        # backstop can't manufacture the stage=DM3/dm_step=0 corruption this
        # guard fixes; non-cadence attrs preserved.
        assert "stage" not in patched["values"]
        assert "dm_step" not in patched["values"]
        assert patched["values"]["persona"] == "operations_leaders"
        # The neutralized regression is observable in aggregate, not just logs.
        assert attio.stage_regressions_blocked == 1
        # Loud, structured warning with the diagnostic fields.
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "entry-existing" in joined
        assert "DM3 Sent" in joined
        assert "Accepted" in joined

    def test_add_list_entry_allows_forward_stage_transition(
        self, attio: AttioClient
    ) -> None:
        existing = self._existing_entry("Accepted")  # rank 2
        patched: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [existing]}
            if method == "PATCH" and path == "/lists/list-X/entries/entry-existing":
                patched["values"] = json["data"]["entry_values"]
                return {"data": {"id": {"entry_id": "entry-existing"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            attio.add_list_entry(
                record_id="rec-1",
                stage_name="DM1 Sent",  # rank 3 > Accepted rank 2 → forward
                list_id="list-X",
            )
        assert patched["values"]["stage"] == "DM1 Sent"

    def test_add_list_entry_keeps_equal_rank_stage(
        self, attio: AttioClient
    ) -> None:
        existing = self._existing_entry("DM1 Sent")
        patched: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [existing]}
            if method == "PATCH" and path == "/lists/list-X/entries/entry-existing":
                patched["values"] = json["data"]["entry_values"]
                return {"data": {"id": {"entry_id": "entry-existing"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            attio.add_list_entry(
                record_id="rec-1",
                stage_name="DM1 Sent",  # equal rank → not a regression, kept
                list_id="list-X",
            )
        assert patched["values"]["stage"] == "DM1 Sent"

    def test_add_list_entry_no_stage_attr_unaffected(
        self, attio: AttioClient
    ) -> None:
        """A PATCH with no stage key (attrs-only) is untouched by the guard."""
        existing = self._existing_entry("DM3 Sent")
        patched: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path.endswith("/entries/query"):
                return {"data": [existing]}
            if method == "PATCH" and path == "/lists/list-X/entries/entry-existing":
                patched["values"] = json["data"]["entry_values"]
                return {"data": {"id": {"entry_id": "entry-existing"}}}
            raise AssertionError(f"Unexpected call: {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            attio.add_list_entry(
                record_id="rec-1",
                stage_name="",  # no stage written into attrs
                entry_attributes={"last_contact_date": "2026-06-24"},
                list_id="list-X",
            )
        assert patched["values"]["last_contact_date"] == "2026-06-24"
        assert "stage" not in patched["values"]

    def test_upsert_canonicalizes_linkedin_before_storing(self, attio: AttioClient) -> None:
        """New records get the canonical URL written to Attio, not the raw input."""
        captured: dict = {}

        def fake_request(method, path, json=None, **_):
            if method == "POST" and path == "/objects/people/records/query":
                return {"data": []}  # no existing record
            if method == "POST" and path == "/objects/people/records":
                captured["body"] = json
                return {"data": {"id": {"record_id": "rec-new"}}}
            raise AssertionError(f"Unexpected {method} {path}")

        with patch.object(attio, "_request", side_effect=fake_request):
            attio.upsert_person(
                matching_attribute="linkedin",
                attributes={"linkedin": "https://www.linkedin.com/in/baz/"},
            )
        stored_url = captured["body"]["data"]["values"]["linkedin"]
        assert stored_url == "https://linkedin.com/in/baz"


class TestBulkFetchPersonsByRecordIds:
    """Bulk preload calls get_person in parallel for the exact record set we
    need, vs. scanning the full workspace via search_people (which scales with
    total people in Attio and routinely takes minutes once a workspace exceeds
    ~10k records). Each call still flows through `_request` so 429 retry +
    backoff applies per-record."""

    def test_returns_only_requested_record_ids(self, attio: AttioClient) -> None:
        people_by_id = {
            "rec-1": {"id": {"record_id": "rec-1"}, "values": {"name": [{"full_name": "A"}]}},
            "rec-2": {"id": {"record_id": "rec-2"}, "values": {"name": [{"full_name": "B"}]}},
            "rec-3": {"id": {"record_id": "rec-3"}, "values": {"name": [{"full_name": "C"}]}},
        }
        with patch.object(attio, "get_person", side_effect=lambda rid, **kw: people_by_id.get(rid)) as mock_get:
            result = attio.bulk_fetch_persons_by_record_ids({"rec-1", "rec-3"})

        # Each requested id triggers one get_person call.
        assert sorted(c.args[0] for c in mock_get.call_args_list) == ["rec-1", "rec-3"]
        assert set(result.keys()) == {"rec-1", "rec-3"}
        assert result["rec-1"]["values"]["name"][0]["full_name"] == "A"
        assert result["rec-3"]["values"]["name"][0]["full_name"] == "C"

    def test_empty_record_ids_returns_empty_without_calling_attio(self, attio: AttioClient) -> None:
        """No records to fetch → no API call."""
        with patch.object(attio, "get_person") as mock_get:
            result = attio.bulk_fetch_persons_by_record_ids(set())

        assert result == {}
        mock_get.assert_not_called()

    def test_missing_record_in_attio_is_silently_dropped(self, attio: AttioClient) -> None:
        """If get_person returns None for a record (404 / deleted), the dict
        simply omits it. Phase code falls back to per-record GETs for cache
        misses, so a missing record self-heals."""
        def fake_get(rid: str, **kw):
            return {"id": {"record_id": "rec-1"}, "values": {}} if rid == "rec-1" else None

        with patch.object(attio, "get_person", side_effect=fake_get):
            result = attio.bulk_fetch_persons_by_record_ids({"rec-1", "rec-missing"})

        assert "rec-1" in result
        assert "rec-missing" not in result


class TestSearchLimitTruncationGuard:
    """PR-234: full-sweep callers pass ``fail_if_truncated=True`` so a query
    that hits its scan limit with records remaining raises
    ``AttioResultTruncated`` instead of silently dropping the tail."""

    @staticmethod
    def _page(n: int) -> dict:
        return {"data": [{"id": {"record_id": f"r{i}"}} for i in range(n)]}

    def test_search_people_raises_when_truncated(self, attio: AttioClient) -> None:
        from clients.attio import AttioResultTruncated

        # limit=2, page_size=2: first page full (2) → loop stops at limit; the
        # boundary probe returns a non-empty page → truncated.
        with patch.object(
            attio, "_request",
            side_effect=[self._page(2), self._page(1)],  # main page, probe
        ), pytest.raises(AttioResultTruncated):
            attio.search_people(limit=2, fail_if_truncated=True)

    def test_search_people_no_raise_when_exhausted(self, attio: AttioClient) -> None:
        # Short final page → exhausted, never raises even under the guard.
        with patch.object(attio, "_request", return_value=self._page(1)):
            result = attio.search_people(limit=50, fail_if_truncated=True)
        assert len(result) == 1

    def test_search_people_no_raise_at_exact_limit(self, attio: AttioClient) -> None:
        # Exactly `limit` records, probe returns empty → complete, no raise.
        with patch.object(
            attio, "_request",
            side_effect=[self._page(2), {"data": []}],  # main page, empty probe
        ):
            result = attio.search_people(limit=2, fail_if_truncated=True)
        assert len(result) == 2

    def test_default_never_raises(self, attio: AttioClient) -> None:
        # Without the opt-in flag the pre-PR-234 silent-truncation behavior
        # is preserved (limit caps the result, no exception).
        with patch.object(attio, "_request", return_value=self._page(2)):
            result = attio.search_people(limit=2)
        assert len(result) == 2

    def test_query_list_entries_raises_when_truncated(self, attio: AttioClient) -> None:
        from clients.attio import AttioResultTruncated

        with patch.object(
            attio, "_request",
            side_effect=[self._page(2), self._page(1)],
        ), pytest.raises(AttioResultTruncated):
            attio.query_list_entries(list_id="l1", limit=2, fail_if_truncated=True)


class TestRequestWithRetry:
    """`request_with_retry` — bounded jittered retry around `_request` for
    transient 5xx / connection errors (PR-259). 4xx propagate immediately."""

    def test_returns_first_success(self, attio: AttioClient) -> None:
        from clients.attio import request_with_retry
        with patch.object(attio, "_request", return_value={"data": "ok"}) as mock_req:
            assert request_with_retry(attio, "POST", "/objects/people/records/query", json={}) == {"data": "ok"}
        mock_req.assert_called_once()

    def test_retries_5xx_then_succeeds(self, attio: AttioClient) -> None:
        from clients.attio import request_with_retry
        req = httpx.Request("POST", "https://api.attio.com/v2/x")
        err = httpx.HTTPStatusError("500", request=req, response=httpx.Response(500, request=req))
        with (
            patch.object(attio, "_request", side_effect=[err, {"data": "ok"}]) as mock_req,
            patch("clients.attio.time.sleep") as mock_sleep,
        ):
            assert request_with_retry(attio, "POST", "/objects/people/records/query") == {"data": "ok"}
        assert mock_req.call_count == 2
        mock_sleep.assert_called_once()

    def test_4xx_propagates_without_retry(self, attio: AttioClient) -> None:
        from clients.attio import request_with_retry
        req = httpx.Request("POST", "https://api.attio.com/v2/x")
        err = httpx.HTTPStatusError("400", request=req, response=httpx.Response(400, request=req))
        with (
            patch.object(attio, "_request", side_effect=err) as mock_req,
            pytest.raises(httpx.HTTPStatusError),
        ):
            request_with_retry(attio, "POST", "/objects/people/records/query")
        mock_req.assert_called_once()

    def test_recheck_short_circuits_before_retry(self, attio: AttioClient) -> None:
        from clients.attio import request_with_retry
        req = httpx.Request("POST", "https://api.attio.com/v2/x")
        err = httpx.HTTPStatusError("500", request=req, response=httpx.Response(500, request=req))
        # First attempt 500s; the recheck finds the landed row → no re-POST.
        with (
            patch.object(attio, "_request", side_effect=err) as mock_req,
            patch("clients.attio.time.sleep"),
        ):
            result = request_with_retry(
                attio, "POST", "/objects/people/records/query", recheck=lambda: {"data": "landed"},
            )
        assert result == {"data": "landed"}
        mock_req.assert_called_once()  # never re-issued the write

    def test_transport_error_retried(self, attio: AttioClient) -> None:
        from clients.attio import request_with_retry
        err = httpx.ConnectError("boom")
        with (
            patch.object(attio, "_request", side_effect=[err, {"data": "ok"}]),
            patch("clients.attio.time.sleep"),
        ):
            assert request_with_retry(attio, "POST", "/objects/people/records/query") == {"data": "ok"}
