"""Mutation-free daily inventory; deliberately separate from execution paths."""
import os
import re
from collections import Counter

import click

from clients.attio import AttioClient
from clients.crm.factory import get_crm_provider
from models.business_calendar import is_send_day, operator_today
from models.pipeline import invite_slice_reason
from workflows.daily_check import dm_due_step
from workflows.daily_check_helpers import SEND_CHANNEL_BOTDOG, _resolve_send_channel


class ReadOnlyPipelineClient(AttioClient):
    """Allow only the configured pipeline query, including its pagination.

    Attio query reads use POST. A generic 'allow POST' is not read-only.
    No mutation method, alternate list, arbitrary URL or GET is needed here.
    """

    def __init__(self, list_id: str, **kwargs):
        if not re.fullmatch(r"[A-Za-z0-9_-]+", list_id):
            raise ValueError("ATTIO_LIST_ID must be a non-empty list ID or slug")
        self._preview_query_path = f"/lists/{list_id}/entries/query"
        super().__init__(**kwargs)

    def _request(self, method, path, *args, **kwargs):
        if method.upper() != "POST" or path != self._preview_query_path:
            raise RuntimeError(f"Daily preview refused non-query Attio operation: {method}")
        response = super()._request(method, path, *args, **kwargs)
        if not isinstance(response, dict) or not isinstance(response.get("data"), list):
            raise ValueError("Daily preview Attio query returned no data list")
        return response


def preview_daily(*, skip_dms=False, force_weekend=False):
    """Read pipeline rows once; never claim, scrape, dispatch, or write state."""
    today = operator_today()
    list_id = os.environ.get("ATTIO_LIST_ID", "")
    with get_crm_provider(client_factory=lambda **kw: ReadOnlyPipelineClient(list_id, **kw)) as bundle:
        entries = bundle.provider.query_list_entries(list_id=list_id, fail_if_truncated=True)
    invites: Counter[str] = Counter()
    dms: Counter[str] = Counter()
    stages: Counter[str] = Counter()
    for entry in entries:
        attrs = entry.attributes
        stages[attrs.get("stage") or "unknown"] += 1
        if _resolve_send_channel(attrs) == SEND_CHANNEL_BOTDOG:
            invites["retired_botdog_channel"] += 1
            dms["retired_botdog_channel"] += 1
            continue
        reason = invite_slice_reason(attrs, today, strict=False)
        invites[reason.value if reason else "candidate_before_live_checks"] += 1
        verdict = dm_due_step(attrs, today, strict=False)
        if verdict.step is not None:
            dms[verdict.step.value] += 1
        else:
            assert verdict.reason is not None
            dms[verdict.reason.value] += 1
    click.echo("=== Daily read-only preview ===")
    click.echo(f"Pipeline entries: {len(entries)}; date: {today}")
    for title, counts in (("Stages", stages), ("Invite inventory", invites), ("DM cadence inventory", dms)):
        click.echo(f"{title}: {dict(sorted(counts.items()))}")
    if skip_dms or (not force_weekend and not is_send_day(today)):
        click.echo("DM execution disabled by command/calendar; cadence inventory shown only.")
    click.echo("Counts are inventory, not approved send batches. Batch/daily caps are not applied.")
    click.echo("Unchecked: live degree/cookies, new replies, sibling/company filters, language, "
               "remaining daily capacity, schema and send readiness. Run live preflight before sending.")
    click.echo("No PB jobs, Sheets writes, CRM updates/escalations, drafts, LLM reservations, "
               "audit/cache writes or run locks performed.")
