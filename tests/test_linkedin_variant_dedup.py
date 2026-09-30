"""Slug-variant cadence-leak regression: LinkedIn identity-key dedup.

A weekly ingest re-prospected a DM3-complete person under the shortened slug
`dana-q-70481235` while their canonical entry already sat at DM3 Sent under
`dana-quiroga-ramos-mba-70481235`. Both slugs share the numeric profile-id
suffix `70481235` — LinkedIn slug variants keep it — but every dedup surface
compared exact URL strings, so a duplicate person + entry were created and a
duplicate DM1 shipped to a DM3-complete prospect.

(The names/ids here are synthetic stand-ins for the upstream incident pair;
only the slug SHAPE the fix keys on is reproduced.)

Covers the three fixes:
  1. `linkedin_profile_id` / `linkedin_identity_key` (clients.attio) wired
     into the weekly-ingest URL gate, the 14-day recent-outreach guard, and
     the daily send-path dedup (`_dedupe_by_linkedin_url`, stage-rank index).
  2. The Pattern-A quarantine escalation payload carries
     `prior_cadence_entries` — pipeline entries sharing the profile-id — so
     the operator sees the prior cadence before judging "real accept".
  3. `manual_reply_suppressed_self_echo` idempotency keys on the thread
     signature (entry|template|count), not the date, so a resolved row no
     longer re-opens every day.
"""

import csv
import io
import json
import os
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

from clients.attio import (
    _canonical_linkedin_url,
    linkedin_identity_key,
    linkedin_identity_map,
    linkedin_profile_id,
    resolve_identity_match,
)
from clients.crm.base import CRMProvider, Entry, Stage
from clients.pb_envelope import (
    PBCompletion,
    PBLaunch,
    hash_arguments,
    parse_send_outcome,
    should_advance_batch,
)
from models.pipeline import PipelineStage
from tests.test_weekly_prospect import _crm
from workflows.daily_check_helpers import (
    _dedupe_by_linkedin_url,
    _normalize_linkedin_url,
)
from workflows.weekly_prospect import _process_prospects

_LONG = "https://linkedin.com/in/dana-quiroga-ramos-mba-70481235"
_SHORT = "https://www.linkedin.com/in/dana-q-70481235"


def _entry(entry_id: str, record_id: str, **attributes) -> Entry:
    return Entry(
        entry_id=entry_id,
        record_id=record_id,
        stage=Stage(name=str(attributes.get("stage") or "")),
        attributes=dict(attributes),
    )


# ---------------------------------------------------------------------------
# 1a. The identity-key helpers
# ---------------------------------------------------------------------------

class TestProfileIdHelpers:
    def test_incident_slugs_share_profile_id(self):
        assert linkedin_profile_id(_LONG) == "70481235"
        assert linkedin_profile_id(_SHORT) == "70481235"

    def test_identity_key_bridges_slug_variants(self):
        assert linkedin_identity_key(_LONG) == "li-id:70481235"
        assert linkedin_identity_key(_LONG) == linkedin_identity_key(_SHORT)

    def test_short_numeric_tail_is_not_a_profile_id(self):
        # A user-chosen tail like `-2` must not conflate distinct people.
        url = "https://linkedin.com/in/john-smith-2"
        assert linkedin_profile_id(url) == ""
        assert linkedin_identity_key(url) == _canonical_linkedin_url(url)

    def test_no_numeric_suffix_falls_back_to_canonical(self):
        url = "https://linkedin.com/in/dana-quiroga-ramos"
        assert linkedin_profile_id(url) == ""
        assert linkedin_identity_key(url) == _canonical_linkedin_url(url)

    def test_non_profile_url_falls_back_to_canonical(self):
        url = "https://linkedin.com/company/acme-70481235"
        assert linkedin_profile_id(url) == ""
        assert linkedin_identity_key(url) == _canonical_linkedin_url(url)

    def test_empty_url(self):
        assert linkedin_profile_id("") == ""
        assert linkedin_identity_key("") == ""

    def test_www_trailing_slash_and_query_variants_share_key(self):
        variants = [
            _LONG,
            _LONG + "/",
            "https://www.linkedin.com/in/dana-quiroga-ramos-mba-70481235/",
            _LONG + "?utm_source=x",
            # Suffixes that survive `_vanity_url_slug`: slash-before-query,
            # fragment, and /in/<slug>/sub-path forms.
            _LONG + "/?utm_source=x",
            _LONG + "#section",
            _LONG + "/recent-activity",
        ]
        assert {linkedin_identity_key(v) for v in variants} == {"li-id:70481235"}

    def test_percent_encoded_accent_variants_share_key(self):
        # `%C3%A9` vs a literal `é` in the name portion: the canonical form
        # already decodes, and the profile-id key is identical either way.
        encoded = "https://linkedin.com/in/ren%C3%A9-salas-88112233"
        literal = "https://linkedin.com/in/rené-salas-88112233"
        assert linkedin_identity_key(encoded) == linkedin_identity_key(literal)
        assert linkedin_identity_key(encoded) == "li-id:88112233"

    def test_distinct_ids_stay_distinct(self):
        a = "https://linkedin.com/in/ana-perez-11111111"
        b = "https://linkedin.com/in/ana-perez-22222222"
        assert linkedin_identity_key(a) != linkedin_identity_key(b)


# ---------------------------------------------------------------------------
# 1b. Weekly-ingest URL gate
# ---------------------------------------------------------------------------

class TestWeeklyIngestVariantGate:
    def test_in_list_set_carries_profile_id_keys(self):
        from workflows import weekly_prospect as wp

        crm = MagicMock(spec=CRMProvider)
        urls = wp._load_in_list_canonical_urls(
            crm, [_entry("e1", "r1", canonical_linkedin_url=_LONG)]
        )
        assert _canonical_linkedin_url(_LONG) in urls
        assert "li-id:70481235" in urls

    def _summary(self):
        return {
            "exported": 0, "scored": 0, "qualified": 0, "duplicates": 0,
            "rejected": 0, "added": 0, "borderline_staged": 0,
            "reprospect_review": 0, "rejected_by_path": {},
        }

    def _candidate(self, url):
        return {
            "fullName": "Dana Quiroga Ramos",
            "title": "Director of Operations",
            "companyName": "Acme Foods",
            "location": "Mexico City, Mexico",
            "defaultProfileUrl": url,
        }

    def test_variant_slug_skipped_before_live_search(self):
        # The regression: the pipeline holds the LONG slug; the scraper
        # re-emits the person under the SHORT slug. Exact-URL dedup missed
        # this; the profile-id key must catch it before the live search.
        attio = MagicMock()
        summary = self._summary()
        in_list = {_canonical_linkedin_url(_LONG), linkedin_identity_key(_LONG)}

        _process_prospects(
            [self._candidate(_SHORT)], _crm(attio), list_id="l",
            today="2026-08-14", dry_run=False, summary=summary,
            seen_urls=set(), in_list_record_ids=set(),
            persona_config={"key": "operations_leaders", "enterprise_mode": True,
                            "search_size_credit": 30},
            borderline_stage=[], reprospect_review=[],
            in_list_canonical_urls=in_list,
        )

        attio.search_person_by_linkedin.assert_not_called()
        attio.upsert_person.assert_not_called()
        attio.add_list_entry.assert_not_called()
        assert summary["duplicates"] == 1
        assert summary["added"] == 0

    def test_within_run_variant_dedups_via_seen_urls(self):
        # Both slug variants arrive in the SAME scrape batch (e.g. two search
        # lanes): the second must dedup against the first via the identity
        # key, not commit a second person.
        attio = MagicMock()
        attio.search_person_by_linkedin.return_value = None
        attio.upsert_person.return_value = {"id": {"record_id": "new-1"}}
        attio.add_list_entry.return_value = None
        attio.search_company_by_domain.return_value = None
        attio.search_companies.return_value = []
        attio.create_company.return_value = {"id": {"record_id": "c1"}}
        summary = self._summary()

        _process_prospects(
            [self._candidate(_LONG), self._candidate(_SHORT)],
            _crm(attio), list_id="l", today="2026-08-14",
            dry_run=False, summary=summary, seen_urls=set(),
            in_list_record_ids=set(),
            persona_config={"key": "operations_leaders", "enterprise_mode": True,
                            "search_size_credit": 30},
            borderline_stage=[], reprospect_review=[],
            in_list_canonical_urls=set(),
        )

        assert summary["added"] == 1
        assert summary["duplicates"] == 1
        attio.upsert_person.assert_called_once()

    def test_different_profile_id_still_commits(self):
        attio = MagicMock()
        attio.search_person_by_linkedin.return_value = None
        attio.upsert_person.return_value = {"id": {"record_id": "new-1"}}
        attio.add_list_entry.return_value = None
        attio.search_company_by_domain.return_value = None
        attio.search_companies.return_value = []
        attio.create_company.return_value = {"id": {"record_id": "c1"}}
        summary = self._summary()
        in_list = {_canonical_linkedin_url(_LONG), linkedin_identity_key(_LONG)}

        _process_prospects(
            [self._candidate("https://linkedin.com/in/other-person-55555555")],
            _crm(attio), list_id="l", today="2026-08-14",
            dry_run=False, summary=summary, seen_urls=set(),
            in_list_record_ids=set(),
            persona_config={"key": "operations_leaders", "enterprise_mode": True,
                            "search_size_credit": 30},
            borderline_stage=[], reprospect_review=[],
            in_list_canonical_urls=in_list,
        )

        assert summary["duplicates"] == 0
        assert summary["added"] == 1


class TestPersonaUpgradeAcrossVariantSlugs:
    """The midmarket persona upgrade must find the staged borderline entry
    even when the second search returns the person under a slug VARIANT —
    the identity-key dedup collapses them, so the upgrade lookup must too."""

    def _raw(self, url):
        # Borderline in enterprise mode: Plant Manager → staged, not written.
        return {
            "fullName": "Dana Quiroga Ramos",
            "title": "Plant Manager",
            "companyName": "Subsidiary",
            "location": "Mexico City, Mexico",
            "defaultProfileUrl": url,
        }

    def test_variant_match_still_upgrades_borderline_entry(self):
        from tests.test_weekly_prospect import (
            ENTERPRISE_PERSONA,
            MIDMARKET_PERSONA,
            _make_attio_mock,
        )

        attio = _make_attio_mock()
        crm = _crm(attio)
        summary = {
            "exported": 0, "scored": 0, "qualified": 0, "duplicates": 0,
            "rejected": 0, "added": 0, "borderline_staged": 0,
        }
        seen_urls: set[str] = set()
        seen_urls_midmarket: set[str] = set()
        borderline_stage: list[dict] = []

        _process_prospects(
            [self._raw(_LONG)], crm, list_id="l", today="2026-08-14",
            dry_run=False, summary=summary, seen_urls=seen_urls,
            in_list_record_ids=set(), persona_config=ENTERPRISE_PERSONA,
            borderline_stage=borderline_stage,
            seen_urls_midmarket=seen_urls_midmarket,
        )
        assert summary["borderline_staged"] == 1

        # Midmarket search returns the SAME person under the SHORT variant.
        _process_prospects(
            [self._raw(_SHORT)], crm, list_id="l", today="2026-08-14",
            dry_run=False, summary=summary, seen_urls=seen_urls,
            in_list_record_ids=set(), persona_config=MIDMARKET_PERSONA,
            borderline_stage=borderline_stage,
            seen_urls_midmarket=seen_urls_midmarket,
        )

        assert len(borderline_stage) == 1
        assert borderline_stage[0]["persona"] == "mx_midmarket_manufacturing"
        assert summary.get("persona_upgraded_to_midmarket") == 1
        assert summary["duplicates"] == 1


# ---------------------------------------------------------------------------
# 1c. 14-day recent-outreach guard
# ---------------------------------------------------------------------------

class TestRecentOutreachMapVariant:
    def test_map_carries_profile_id_keys(self):
        from workflows import weekly_prospect as wp

        crm = MagicMock(spec=CRMProvider)
        crm.query_list_entries.return_value = [
            _entry("e1", "r1", canonical_linkedin_url=_LONG,
                   last_contact_date="2026-08-10"),
        ]

        out = wp._load_recent_outreach_map(crm, "list-1", date(2026, 8, 1))

        assert out[_canonical_linkedin_url(_LONG)] == date(2026, 8, 10)
        # The identity key rides along, so a slug VARIANT of the same person
        # is caught by the guard too.
        assert out["li-id:70481235"] == date(2026, 8, 10)
        assert linkedin_identity_key(_SHORT) in out


# ---------------------------------------------------------------------------
# 1d. Daily send-path batch dedup
# ---------------------------------------------------------------------------

class TestDedupeByLinkedinUrlVariant:
    def test_slug_variants_collapse_to_one_send(self):
        rows = [
            {"linkedInUrl": _LONG, "entry_id": "e-canonical"},
            {"linkedInUrl": _SHORT, "entry_id": "e-duplicate"},
        ]
        deduped, dropped = _dedupe_by_linkedin_url(rows)
        assert len(deduped) == 1
        # Both entry_ids ride on the kept row so the stage-advance loop flips
        # the duplicate in lock-step (no re-queue tomorrow).
        assert deduped[0]["entry_ids"] == ["e-canonical", "e-duplicate"]
        assert dropped == [_SHORT]

    def test_distinct_people_not_collapsed(self):
        rows = [
            {"linkedInUrl": "https://linkedin.com/in/ana-11111111", "entry_id": "a"},
            {"linkedInUrl": "https://linkedin.com/in/ben-22222222", "entry_id": "b"},
        ]
        deduped, dropped = _dedupe_by_linkedin_url(rows)
        assert len(deduped) == 2
        assert dropped == []


# ---------------------------------------------------------------------------
# 2. Pattern-A quarantine payload: prior-cadence siblings
# ---------------------------------------------------------------------------

class TestPriorCadenceLookup:
    def _attio_with_entries(self, monkeypatch, parsed_entries):
        from workflows import pre_invite_check as pic

        raws = [{"i": i} for i in range(len(parsed_entries))]
        monkeypatch.setattr(
            pic.AttioClient, "parse_entry",
            staticmethod(lambda raw: parsed_entries[raw["i"]]),
        )
        attio = MagicMock()
        attio.query_list_entries.return_value = raws
        return attio

    def _sibling(self):
        return {
            "entry_id": "e-old", "record_id": "r-old",
            "canonical_linkedin_url": _LONG,
            "stage": "DM3 Sent", "dm_step": 3,
            "dm1_sent_at": "2026-05-20", "dm2_sent_at": "2026-05-26",
            "dm3_sent_at": "2026-06-01", "last_contact_date": "2026-06-01",
        }

    def test_returns_profile_id_siblings_excluding_own_entry(self, monkeypatch):
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        attio = self._attio_with_entries(monkeypatch, [
            self._sibling(),
            {"entry_id": "e-other", "record_id": "r-other",
             "canonical_linkedin_url": "https://linkedin.com/in/otra-persona-77777777",
             "stage": "Prospect", "dm_step": None, "dm1_sent_at": None,
             "dm2_sent_at": None, "dm3_sent_at": None, "last_contact_date": None},
        ])
        cache: dict = {}
        matches, err = _prior_cadence_entries_for_url(
            attio, "list-1", _SHORT, "e-dup", cache,
        )
        assert err == ""
        assert [m["entry_id"] for m in matches] == ["e-old"]
        assert matches[0]["stage"] == "DM3 Sent"
        assert matches[0]["dm3_sent_at"] == "2026-06-01"

    def test_own_entry_is_excluded(self, monkeypatch):
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        attio = self._attio_with_entries(monkeypatch, [self._sibling()])
        matches, err = _prior_cadence_entries_for_url(
            attio, "list-1", _SHORT, "e-old", {},
        )
        assert err == ""
        assert matches == []

    def test_merged_away_entries_excluded(self, monkeypatch):
        # §3.11 soft-deleted losers must not appear as siblings — their
        # winner inherited the cadence.
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        loser = dict(self._sibling(), merged_into="r-winner")
        attio = self._attio_with_entries(monkeypatch, [loser])
        matches, err = _prior_cadence_entries_for_url(
            attio, "list-1", _SHORT, "e-dup", {},
        )
        assert err == ""
        assert matches == []

    def test_missing_canonical_falls_back_to_person_record(self, monkeypatch):
        # PROSPECT-era entries often lack canonical_linkedin_url; the sibling
        # must still surface via the parent person record's LinkedIn URL.
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        sibling = dict(self._sibling(), canonical_linkedin_url=None)
        attio = self._attio_with_entries(monkeypatch, [sibling])
        attio.bulk_fetch_persons_by_record_ids.return_value = {
            "r-old": {"values": {"linkedin": [{"value": _LONG}]}},
        }
        matches, err = _prior_cadence_entries_for_url(
            attio, "list-1", _SHORT, "e-dup", {},
        )
        assert err == ""
        assert [m["entry_id"] for m in matches] == ["e-old"]
        assert matches[0]["linkedin_url"] == _LONG
        attio.bulk_fetch_persons_by_record_ids.assert_called_once_with({"r-old"})

    def test_fetch_happens_once_per_cache(self, monkeypatch):
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        attio = self._attio_with_entries(monkeypatch, [self._sibling()])
        cache: dict = {}
        _prior_cadence_entries_for_url(attio, "list-1", _SHORT, "e-dup", cache)
        _prior_cadence_entries_for_url(attio, "list-1", _LONG, "e-dup", cache)
        assert attio.query_list_entries.call_count == 1

    def test_fetch_failure_degrades_to_empty_with_error(self):
        from workflows.pre_invite_check import _prior_cadence_entries_for_url

        attio = MagicMock()
        attio.query_list_entries.side_effect = RuntimeError("crm down")
        cache: dict = {}
        matches, err = _prior_cadence_entries_for_url(
            attio, "list-1", _SHORT, "e-dup", cache,
        )
        assert matches == []
        assert "crm down" in err
        # The failure is memoized too — no retry storm within a run.
        _prior_cadence_entries_for_url(attio, "list-1", _SHORT, "e-dup", cache)
        assert attio.query_list_entries.call_count == 1


@patch("workflows.daily_check.write_prospects_to_sheet",
       return_value="https://sheet.example/foo")
@patch("workflows.daily_check._pb_session_args", return_value={})
class TestQuarantinePayloadCarriesPriorCadence:
    """End-to-end through `_pre_invite_degree_check`: the escalation payload
    for a quarantined URL-variant duplicate must show the prior cadence."""

    def _row(self):
        return {
            "linkedInUrl": _SHORT,
            "message": "hi",
            "entry_id": "e-dup",
            "record_id": "r-dup",
            "current_stage": "Prospect",
            "experiment_id": "exp-x",
            "experiment_id_frozen_at": "prospect",
            "name": "Dana Quiroga Ramos",
            "company": "Acme Foods",
            "prospect_committed_at": (
                date(2026, 8, 18) - timedelta(days=4)
            ).isoformat(),
        }

    def test_escalation_payload_includes_dm3_sibling(self, _pb_args, _sheet,
                                                     monkeypatch):
        from workflows import pre_invite_check as pic
        from workflows.pre_invite_check import _pre_invite_degree_check

        sibling = {
            "entry_id": "e-old", "record_id": "r-old",
            "canonical_linkedin_url": _LONG,
            "stage": "DM3 Sent", "dm_step": 3,
            "dm1_sent_at": "2026-05-20", "dm2_sent_at": "2026-05-26",
            "dm3_sent_at": "2026-06-01", "last_contact_date": "2026-06-01",
        }
        monkeypatch.setattr(
            pic.AttioClient, "parse_entry", staticmethod(lambda raw: sibling),
        )
        attio = MagicMock()
        attio.query_list_entries.return_value = [{"raw": 1}]
        pb = MagicMock()
        pb.download_result_csv.return_value = (
            "linkedinProfileUrl,connectionDegree\n" + _SHORT + ",1st"
        )
        with patch("workflows.pre_invite_check.escalate") as mock_escalate:
            still, already = _pre_invite_degree_check(
                [self._row()], pb, "scraper-id", attio, "list-id",
                today=date(2026, 8, 18),
            )

        assert still == [] and already == []
        assert mock_escalate.called
        kwargs = mock_escalate.call_args.kwargs
        assert kwargs["type"] == "pattern_a_suspected_duplicate"
        prior = kwargs["payload"]["prior_cadence_entries"]
        assert [p["entry_id"] for p in prior] == ["e-old"]
        assert prior[0]["stage"] == "DM3 Sent"
        assert prior[0]["dm3_sent_at"] == "2026-06-01"
        assert "prior_cadence_lookup_error" not in kwargs["payload"]

    def test_lookup_failure_still_escalates_with_error_marker(
            self, _pb_args, _sheet):
        from workflows.pre_invite_check import _pre_invite_degree_check

        attio = MagicMock()
        attio.query_list_entries.side_effect = RuntimeError("crm down")
        pb = MagicMock()
        pb.download_result_csv.return_value = (
            "linkedinProfileUrl,connectionDegree\n" + _SHORT + ",1st"
        )
        with patch("workflows.pre_invite_check.escalate") as mock_escalate:
            _pre_invite_degree_check(
                [self._row()], pb, "scraper-id", attio, "list-id",
                today=date(2026, 8, 18),
            )

        kwargs = mock_escalate.call_args.kwargs
        assert kwargs["payload"]["prior_cadence_entries"] == []
        assert "crm down" in kwargs["payload"]["prior_cadence_lookup_error"]


class TestQuarantinePayloadSchema:
    def test_payload_validates_against_typeddict(self):
        from workflows.escalation import _validate_payload_against_typeddict

        _validate_payload_against_typeddict("pattern_a_suspected_duplicate", {
            "record_id": "r", "entry_id": "e", "linkedin_url": _SHORT,
            "name": "n", "company": "c", "prospect_committed_at": "2026-08-14",
            "degree": "1st", "prior_cadence_entries": [],
        })

    def test_missing_prior_cadence_entries_rejected(self):
        import pytest

        from workflows.escalation import _validate_payload_against_typeddict
        from workflows.escalation_schemas import EscalationSchemaError

        with pytest.raises(EscalationSchemaError):
            _validate_payload_against_typeddict("pattern_a_suspected_duplicate", {
                "record_id": "r", "entry_id": "e", "linkedin_url": _SHORT,
                "name": "n", "company": "c",
                "prospect_committed_at": "2026-08-14", "degree": "1st",
            })


# ---------------------------------------------------------------------------
# 3. Self-echo idempotency: signature-keyed, not date-keyed
# ---------------------------------------------------------------------------

class TestSelfEchoSignatureIdempotency:
    """A resolved `manual_reply_suppressed_self_echo` row must not re-open on
    subsequent days: the idempotency key is the thread signature
    (entry|template|count), which only changes when the thread changes."""

    def _make_entry(self):
        return {
            "entry_id": "e-dana", "record_id": "r-dana",
            "stage": PipelineStage.DM1_SENT.value,
            "persona": "operations_leaders", "language": "es",
            "dm_step": 1, "quality_score": 75,
            "last_contact_date": "2026-06-20", "experiment_id": None,
        }

    def _make_sn_csv(self, **row) -> str:
        fieldnames = [
            "participantProfileUrl", "participantFullName",
            "isLastMessageFromMe", "lastMessageBody", "lastMessageDate",
            "totalMessageCount",
        ]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        w.writerow({k: row.get(k, "") for k in fieldnames})
        return buf.getvalue()

    def test_idempotency_key_is_thread_signature(self, monkeypatch):
        from models.campaign import load_messages
        from workflows.detect_responses import (
            _looks_like_self_echo,
            detect_responses,
        )

        monkeypatch.setenv("ATTIO_LIST_ID", "test-list-id")
        template = load_messages()["operations_leaders"]["dm1"]["es"]
        echoed = template.replace("[Name]", "Dana").replace(
            "[Company]", "Acme Foods"
        )
        expected_template_id = _looks_like_self_echo(echoed)
        assert expected_template_id is not None

        attio = MagicMock()
        pb = MagicMock()
        attio.query_list_entries.return_value = [self._make_entry()]
        pb.download_result_csv.return_value = self._make_sn_csv(
            participantProfileUrl=_LONG,
            participantFullName="Dana Quiroga Ramos",
            isLastMessageFromMe="true",
            lastMessageBody=echoed,
            lastMessageDate="2026-08-18T12:00:00Z",
            totalMessageCount="3",
        )
        with patch("workflows.detect_responses.AttioClient.parse_entry",
                   side_effect=lambda e: e), \
             patch("workflows.detect_responses._pb_session_args",
                   return_value={}), \
             patch("workflows.detect_responses.RecordCache") as MockCache, \
             patch("workflows.detect_responses.escalate") as mock_escalate:
            mock_cache = MagicMock()
            mock_cache.get.return_value = (
                "Dana Quiroga Ramos", "Acme Foods", _LONG, "", "",
            )
            MockCache.return_value = mock_cache
            detect_responses(attio, pb, inbox_scraper_id="scraper-123")

        echo_calls = [
            c for c in mock_escalate.call_args_list
            if c.kwargs.get("type") == "manual_reply_suppressed_self_echo"
        ]
        assert len(echo_calls) == 1
        key = echo_calls[0].kwargs["idempotency_key"]
        # Signature-keyed: same thread state → same key on every future run
        # (escalate() then no-ops against the existing row, even resolved).
        assert key == f"e-dana|{expected_template_id}|3"
        # No date component — the old daily re-open vector.
        assert date.today().isoformat() not in key


# ---------------------------------------------------------------------------
# 4. READ-side scraper CSV match-back: variant-slug echo bridging
#
# The WRITE-side fixes above stop a variant slug from creating a duplicate
# cadence. The READ-side surfaces (send-outcome advance gate, degree/
# acceptance match-back) used to key on exact normalized-URL equality, so a
# CSV echoing the profile's CURRENT slug (linkedinProfileUrl) missed:
#   - advance gate miss → delivered DM classed unreported → stage not
#     advanced → the SAME DM re-sends the next day (§3.1 violation);
#   - degree match-back miss → accepted connection never detected / row
#     re-queues and escalates forever.
# These pin the identity-key bridge: exact match stays primary, the numeric
# profile-id bridges a variant echo back to OUR url form. Vanity slugs
# without a profile-id must NOT bridge (a false positive would advance a
# stage for an unsent DM — as bad as the re-send).
# ---------------------------------------------------------------------------

_LONG_NORM = _normalize_linkedin_url(_LONG)


class TestLinkedinIdentityMap:
    def test_maps_profile_id_to_input_url(self):
        assert linkedin_identity_map({_LONG_NORM}) == {
            "li-id:70481235": _LONG_NORM
        }

    def test_vanity_urls_without_id_excluded(self):
        assert linkedin_identity_map({"https://linkedin.com/in/dana-qr"}) == {}

    def test_ambiguous_profile_id_dropped(self):
        # Two DIFFERENT input forms sharing one profile-id: bridging could
        # pick the wrong sibling, so the id must drop out of the map.
        other = "https://linkedin.com/in/dana-q-r-70481235"
        assert linkedin_identity_map({_LONG_NORM, other}) == {}

    def test_identical_inputs_not_ambiguous(self):
        mapping = linkedin_identity_map([_LONG_NORM, _LONG_NORM])
        assert mapping == {"li-id:70481235": _LONG_NORM}


class TestResolveIdentityMatch:
    """The shared match-back helper used by every READ surface: exact match
    stays primary, profile-id bridges a variant echo, no match returns ''."""

    def _by_id(self):
        return linkedin_identity_map({_LONG_NORM})

    def test_exact_match_returned_unchanged(self):
        assert resolve_identity_match(
            _LONG_NORM, {_LONG_NORM}, self._by_id()
        ) == _LONG_NORM

    def test_variant_echo_bridges_to_our_form(self):
        short_norm = _normalize_linkedin_url(f"{_SHORT}/")
        assert resolve_identity_match(
            short_norm, {_LONG_NORM}, self._by_id()
        ) == _LONG_NORM

    def test_no_match_returns_empty(self):
        stranger = _normalize_linkedin_url(
            "https://linkedin.com/in/stranger-55555555"
        )
        assert resolve_identity_match(stranger, {_LONG_NORM}, self._by_id()) == ""

    def test_vanity_slug_without_id_does_not_bridge(self):
        # No profile-id on either side → only an exact match can hit.
        exact = {"https://linkedin.com/in/dana-qr"}
        assert resolve_identity_match(
            "https://linkedin.com/in/dana-quiroga", exact,
            linkedin_identity_map(exact),
        ) == ""


def _pb_launch(container_id: str = "c_var") -> PBLaunch:
    return PBLaunch(
        container_id=container_id,
        agent_id="ag_msg",
        launched_at=datetime(2026, 8, 20, 12, 0, tzinfo=UTC),
        arguments_sha256=hash_arguments(None),
    )


def _pb_completion(container_id: str = "c_var") -> PBCompletion:
    return PBCompletion(
        container_id=container_id,
        status="finished",
        log_output="ok",
        raw_output={},
    )


class TestParseSendOutcomeVariantBridge:
    """Advance-gate surface: `parse_send_outcome` re-keys a variant-slug CSV
    echo to the REQUESTED url form, so callers' membership checks
    (`key in outcome.sent_urls`) and the lease charge
    (`requested_urls & outcome.sent_urls`) hit without any caller change."""

    # The sender omits the `query` column and reports the profile's current
    # slug — the incident shape.
    def _csv(self, url: str, status: str = "Message sent") -> str:
        return f"linkedinProfileUrl,status\n{url},{status}\n"

    def test_variant_echo_sent_row_keys_to_requested_url(self):
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv(f"{_SHORT}/"),
            requested_urls={_LONG_NORM},
        )
        assert outcome.csv_status == "Message sent"
        assert outcome.sent_urls == frozenset({_LONG_NORM})
        assert should_advance_batch(_pb_launch(), outcome)
        # The per-row advance + lease-charge intersection both hit now.
        assert {_LONG_NORM} & outcome.sent_urls == {_LONG_NORM}

    def test_variant_echo_skipped_row_keys_to_requested_url(self):
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv(f"{_SHORT}/", status="Error - InMail required"),
            requested_urls={_LONG_NORM},
        )
        assert outcome.csv_status == "Skipped"
        assert outcome.skipped_urls == frozenset({_LONG_NORM})

    def test_exact_match_stays_primary(self):
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv(_LONG),
            requested_urls={_LONG_NORM},
        )
        assert outcome.sent_urls == frozenset({_LONG_NORM})

    def test_unrequested_row_not_rekeyed(self):
        # A stale row from a prior launch (agent-scoped CSV fallback) whose
        # id matches nothing requested keeps its CSV form and stays excluded
        # from the requested∩sent charge.
        stranger = "https://linkedin.com/in/someone-else-55555555"
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv(stranger),
            requested_urls={_LONG_NORM},
        )
        assert outcome.sent_urls == frozenset({stranger})
        assert {_LONG_NORM} & outcome.sent_urls == set()

    def test_vanity_slug_variant_does_not_bridge(self):
        # No numeric profile-id → no safe identity → row stays unreported
        # for the requested url (conservative: a re-send beats advancing a
        # stage for a DM that never went to OUR prospect).
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv("https://linkedin.com/in/dana-quiroga"),
            requested_urls={"https://linkedin.com/in/dana-qr"},
        )
        assert "https://linkedin.com/in/dana-qr" not in outcome.sent_urls

    def test_ambiguous_profile_id_does_not_bridge(self):
        other = "https://linkedin.com/in/dana-q-r-70481235"
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=self._csv(f"{_SHORT}/"),
            requested_urls={_LONG_NORM, other},
        )
        # Bridge refused: the echo keeps its CSV form, neither requested row
        # is falsely marked sent.
        assert _LONG_NORM not in outcome.sent_urls
        assert other not in outcome.sent_urls

    def test_duplicate_exact_and_variant_rows_collapse_to_one(self):
        csv_text = (
            "linkedinProfileUrl,status\n"
            f"{_LONG},Message sent\n"
            f"{_SHORT}/,Message sent\n"
        )
        outcome = parse_send_outcome(
            launch=_pb_launch(),
            completion=_pb_completion(),
            csv_text=csv_text,
            requested_urls={_LONG_NORM},
        )
        assert outcome.sent_urls == frozenset({_LONG_NORM})
        assert outcome.sent_count == 1


class TestDmAdvanceGateVariantEcho:
    """End-to-end advance gate: the sender delivers the DM but echoes the
    profile's current slug. Pre-fix the row was classed unreported (stage
    held → same DM re-sent tomorrow); now it advances like an exact echo."""

    @patch.dict(os.environ, {
        "ATTIO_LIST_ID": "list-001", "ATTIO_API_KEY": "fake",
        "PHANTOMBUSTER_API_KEY": "fake", "GSHEET_AUTOCONNECT_ID": "fake-sheet-id",
        "PB_LI_SESSION_COOKIE": "fake-cookie", "PB_LI_USER_AGENT": "TestAgent/1.0",
        "STRICT_PRE_INVITE_DEGREE_CHECK": "false",
    })
    def test_dm_advances_when_sender_echoes_variant_slug(self):
        from tests.test_integration import (
            _attio_with_full_schema,
            _corruption_fake_daily_run,
            _make_attio_entry,
            _make_sn_csv,
            _typed_pb_mock,
        )
        from workflows.daily_check import run_dm_sequencing

        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        entry = _make_attio_entry(
            entry_id="entry-var-001",
            record_id="rec-var-001",
            stage="Accepted",
            last_contact_date=two_days_ago,
        )
        attio = _attio_with_full_schema()
        from tests.fakes import stub_guard_reread
        stub_guard_reread(attio, [entry])
        # The sender reports the send under the CURRENT (short) slug; the
        # `query` column is absent — the incident shape.
        pb = _typed_pb_mock(
            csv_text=_make_sn_csv([{
                "linkedinProfileUrl": f"{_SHORT}/",
                "status": "Message sent",
            }]),
        )
        attio.query_list_entries.return_value = [entry]
        daily_run = _corruption_fake_daily_run()

        with patch("workflows.daily_check.RecordCache.get") as mock_cache_get, \
             patch("workflows.daily_check.can_send_messages", return_value=True), \
             patch("workflows.daily_check.write_prospects_to_sheet",
                   return_value="https://docs.google.com/spreadsheets/d/fake"):
            mock_cache_get.return_value = (
                "Dana Quiroga Ramos", "Acme Foods", _LONG, "", "",
            )
            result = run_dm_sequencing(
                attio=attio,
                pb=pb,
                message_sender_id="agent-msg-var",
                daily_run=daily_run,
                auto_confirm=True,
            )

        attio.update_list_entry.assert_called_once()
        update_call = attio.update_list_entry.call_args
        attrs = update_call[1].get(
            "entry_attributes",
            update_call[0][1] if len(update_call[0]) > 1 else {},
        )
        assert attrs["stage"] == "DM1 Sent"
        assert result.get("dm1", 0) == 1
        # The lease charge intersects requested∩sent — the bridged key must
        # count as this launch's send, not zero.
        daily_run.confirm_lease.assert_called_once_with(
            "token-int", confirmed_count=1
        )


class TestPhase0DegreeVariantEcho:
    """Phase-0 acceptance match-back: the scraper reports the profile under
    its current slug. Pre-fix zero rows matched → the run halted as
    `stale_scrape` (or the acceptance went undetected on a partial match);
    now the degree bridges back to OUR url and the row flips to ACCEPTED."""

    _CSV_HEADER = (
        "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"
    )

    def _mock_pb(self, csv_text: str) -> MagicMock:
        pb = MagicMock()
        pb.get_agent.return_value = {
            "argument": json.dumps(
                {"numberOfProfilesPerLaunch": 10, "saveImg": False,
                 "identities": [{"identityId": "i"}], "spreadsheetUrl": "old"}
            )
        }
        pb.launch_agent.return_value = MagicMock(container_id="ct-var")
        pb.wait_for_completion.return_value = MagicMock(
            log_output="Scraped 1 profile."
        )
        pb.download_result_csv.return_value = csv_text
        return pb

    def test_variant_echo_still_flips_accepted(self, monkeypatch):
        from workflows.daily_check import detect_accepted_connections

        monkeypatch.setenv("PRE_INVITE_DEGREE_CHECK_BACKEND", "sales_nav")
        monkeypatch.setenv("PB_SALES_NAV_PROFILE_SCRAPER_ID", "sn-id")
        monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "ck")
        monkeypatch.setenv("PB_LI_USER_AGENT", "ua")

        recent = (date.today() - timedelta(days=1)).isoformat()
        attrs = {
            "entry_id": "entry-var",
            "record_id": "rec-var",
            "stage": PipelineStage.CONNECTION_SENT.value,
            "last_contact_date": recent,
            "experiment_id": None,
            "linkedin_url": _LONG,
        }
        # The scraper echoes the CURRENT (short) slug, degree 1st.
        pb = self._mock_pb(
            self._CSV_HEADER + f"{_SHORT}/,1st,false,,Dana Quiroga Ramos\n"
        )
        mock_cache = MagicMock()
        mock_cache.get.return_value = (
            "Dana Quiroga Ramos", None, _LONG, None, None,
        )

        with (
            patch("workflows.daily_check._get_all_entries_parsed",
                  return_value=[attrs]),
            patch("workflows.daily_check.recheck_cache") as mock_rc,
            patch("workflows.daily_check.write_prospects_to_sheet",
                  return_value="https://s"),
            patch("workflows.daily_check.escalate") as mock_esc,
            patch("workflows.daily_check._attio_advance_with_escalation",
                  return_value=True) as mock_advance,
        ):
            mock_rc.partition.return_value = ({}, [_LONG])
            mock_rc.RECHECK_TTL_DAYS = 3
            result = detect_accepted_connections(
                MagicMock(), pb, profile_scraper_id="legacy",
                cache=mock_cache, sales_nav_profile_scraper_id="sn-id",
            )

        # Pre-fix: zero matched rows → error == "stale_scrape", no flip.
        assert "error" not in result
        assert result["accepted"] == 1
        assert mock_advance.called
        mock_esc.assert_not_called()
        # Recheck cache stamped under OUR url form so the TTL skip works.
        mock_rc.record_many.assert_called_once()
        cache_updates = mock_rc.record_many.call_args[0][0]
        assert cache_updates == {_LONG: "1st"}


class TestSalesNavScrapeVariantRekey:
    """`_launch_sales_nav_scrape` (pre-invite degree check + pending-invite
    reconciliation): a variant-slug CSV row must be re-keyed to OUR
    normalized url so callers' lookups and the missing-row re-scrape both
    behave as if the scraper had echoed the queried slug."""

    def _mock_pb(self, csv_text: str) -> MagicMock:
        pb = MagicMock()
        pb.get_agent.return_value = {
            "argument": json.dumps({
                "numberOfProfilesPerLaunch": 10, "saveImg": False,
                "identities": [{"identityId": "id-1"}],
                "spreadsheetUrl": "old-url",
            })
        }
        pb.launch_agent.return_value = MagicMock(container_id="ct-sn")
        pb.wait_for_completion.return_value = MagicMock(log_output="")
        pb.download_result_csv.return_value = csv_text
        return pb

    def test_variant_row_keys_to_our_normalized_url(self, monkeypatch):
        from workflows.pre_invite_check import _launch_sales_nav_scrape

        monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "ck")
        monkeypatch.setenv("PB_LI_USER_AGENT", "ua")
        pb = self._mock_pb(
            "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"
            f"{_SHORT}/,2nd,true,,Dana Quiroga Ramos\n"
        )
        _cid, degree_lookup, extras = _launch_sales_nav_scrape(
            pb, "sn-id", [_LONG], retry_on_timeout=False,
        )
        assert degree_lookup == {_LONG_NORM: "2nd"}
        assert extras[_LONG_NORM]["hasPendingInvitation"] == "true"

    def test_unrelated_row_still_dropped(self, monkeypatch):
        from workflows.pre_invite_check import _launch_sales_nav_scrape

        monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "ck")
        monkeypatch.setenv("PB_LI_USER_AGENT", "ua")
        pb = self._mock_pb(
            "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"
            "https://linkedin.com/in/stranger-55555555/,1st,false,,Someone\n"
        )
        _cid, degree_lookup, extras = _launch_sales_nav_scrape(
            pb, "sn-id", [_LONG], retry_on_timeout=False,
        )
        assert degree_lookup == {}
        assert extras == {}


class TestPendingInviteReconciliationVariantEcho:
    """Phase-C reconciliation, end-to-end through the REAL
    `_launch_sales_nav_scrape`: a variant-slug echo carrying
    hasPendingInvitation=true must still flip PROSPECT→CONNECTION_SENT. Pre-
    fix the scrape row missed our exact URL, `extras` was empty, and the row
    was left at PROSPECT forever."""

    def _mock_pb(self, csv_text: str) -> MagicMock:
        pb = MagicMock()
        pb.get_agent.return_value = {
            "argument": json.dumps({
                "numberOfProfilesPerLaunch": 10, "saveImg": False,
                "identities": [{"identityId": "id-1"}],
                "spreadsheetUrl": "old-url",
            })
        }
        pb.launch_agent.return_value = MagicMock(container_id="ct-rec")
        pb.wait_for_completion.return_value = MagicMock(log_output="")
        pb.download_result_csv.return_value = csv_text
        return pb

    def test_variant_echo_flips_prospect(self, monkeypatch):
        from workflows.pending_invite_reconciliation import (
            run_pending_invite_reconciliation,
        )

        monkeypatch.setenv("PB_LI_SALES_NAV_SESSION_COOKIE", "ck")
        monkeypatch.setenv("PB_LI_USER_AGENT", "ua")

        entry = {
            "entry_id": "ent-var",
            "record_id": "rec-var",
            "stage": PipelineStage.PROSPECT.value,
            "experiment_id": None,
            "experiment_id_frozen_at": None,
            "canonical_linkedin_url": _LONG,
        }
        # Scraper echoes the CURRENT (short) slug, invite still pending.
        pb = self._mock_pb(
            "linkedinProfileUrl,connectionDegree,hasPendingInvitation,query,fullName\n"
            f"{_SHORT}/,2nd,true,,Dana Quiroga Ramos\n"
        )
        spy = MagicMock(return_value=True)
        MOD = "workflows.pending_invite_reconciliation"
        attio = MagicMock()
        attio.bulk_fetch_persons_by_record_ids.return_value = {}
        with (
            patch(f"{MOD}._get_all_entries_parsed", return_value=[entry]),
            patch(f"{MOD}.recheck_cache.partition",
                  side_effect=lambda urls: ({}, list(urls))),
            patch(f"{MOD}.recheck_cache.record_many"),
            patch("workflows.daily_check._attio_advance_with_escalation", spy),
        ):
            summary = run_pending_invite_reconciliation(
                attio=attio, pb=pb, sales_nav_profile_scraper_id="sn-1",
                list_id="list-1", dry_run=False,
            )

        assert summary["flipped"] == 1
        assert summary["left_at_prospect"] == 0
        assert {c.kwargs["entry_id"] for c in spy.call_args_list} == {"ent-var"}
