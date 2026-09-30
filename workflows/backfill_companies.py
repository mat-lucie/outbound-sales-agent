"""Backfill company associations on existing Attio pipeline records."""

from __future__ import annotations

import csv
import os
import time
from datetime import date
from typing import TYPE_CHECKING

import click

from workflows.company_matcher import extract_real_domain, match_or_create_company

if TYPE_CHECKING:
    from clients.attio import AttioClient
    from clients.crm.base import CRMProvider
    from clients.phantombuster import PhantomBusterClient

# Per-launch scrape cap for the repair pipeline. PB validates
# numberOfProfilesPerLaunch against the phantom's argument schema and rejects
# the ENTIRE launch above the max ("numberOfProfilesPerLaunch => is more than
# maximum") — the failure mode of the 2026-06-11 Phase 0 incident. Schema
# maxes verified 2026-06-11 via the PB API (agents/fetch → scripts/fetch →
# argumentSchema): Sales Navigator Profile Scraper (script 11108) max=150;
# the legacy Profile Scraper agent this pipeline launches no longer exists in
# the PB org ("Agent not found"), so its max is unverifiable. 50 sits under
# any plausible scraper max AND at the per-run profile-visit safety volume
# Phase 0 enforces (PHASE0_MAX_PROFILES_PER_LAUNCH) — repair scrapes burn
# visits on the same LinkedIn account. The deferred tail converges across
# runs: repaired rows drop out of the next detect-bad-companies CSV.
REPAIR_MAX_PROFILES_PER_LAUNCH = 50


def backfill_export(crm: CRMProvider) -> str:
    """Export LinkedIn URLs of pipeline records that have no company association.

    Writes a CSV suitable for feeding into PhantomBuster's LinkedIn Profile
    Scraper so that company data can be retrieved and later imported with
    backfill_import().

    Returns the path to the written CSV file.

    P1c migration note: this slice reads the CRM exclusively through the
    vendor-neutral ``CRMProvider`` contract (``query_list_entries`` →
    ``list[Entry]``, ``get_person`` → ``Record``, ``extract_person_info`` →
    ``RecordInfo``). The structured ``company`` record-reference check has no
    contract field, so it reads the untouched payload via ``Record.raw`` (the
    contract's documented escape hatch). ``extract_person_info`` returns a
    ``RecordInfo`` dataclass; we unpack the two fields this slice uses at the
    call boundary to keep the rest of the function byte-identical. Sibling
    write-path functions in this module still take a raw ``AttioClient`` — they
    migrate in a later increment.
    """
    entries = crm.query_list_entries(limit=50_000, fail_if_truncated=True)

    total = len(entries)
    already_linked = 0
    no_linkedin = 0
    rows: list[dict] = []

    for entry in entries:
        record_id = entry.record_id
        if not record_id:
            continue

        person = crm.get_person(record_id)
        time.sleep(0.2)

        if person is None:
            continue

        # Skip records that already have a company record-reference linked.
        # The company reference is a structured multi-value slug the contract
        # does not model as a flat attribute, so read it off the untouched
        # vendor payload via Record.raw (the contract's escape hatch).
        company_data = person.raw.get("values", {}).get("company", [])
        if company_data and isinstance(company_data, list) and company_data[0].get("target_record_id"):
            already_linked += 1
            continue

        info = crm.extract_person_info(person)
        name, linkedin = info.name, info.linkedin_url
        if not linkedin:
            no_linkedin += 1
            continue

        rows.append({"linkedin_url": linkedin, "attio_record_id": record_id, "name": name})

    os.makedirs("exports", exist_ok=True)
    file_path = f"exports/backfill_companies_{date.today().isoformat()}.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["linkedin_url", "attio_record_id", "name"])
        writer.writeheader()
        writer.writerows(rows)

    click.echo("\nBackfill export complete:")
    click.echo(f"  Total entries:    {total}")
    click.echo(f"  Already linked:   {already_linked} (skipped)")
    click.echo(f"  No LinkedIn URL:  {no_linkedin} (skipped)")
    click.echo(f"  Exported:         {len(rows)}")
    click.echo(f"  File:             {file_path}")

    return file_path


def backfill_import(attio: AttioClient, pb_csv_path: str, export_csv_path: str) -> dict:
    """Read PhantomBuster Profile Scraper output and link each person to a company.

    pb_csv_path      -- path to the PhantomBuster CSV output
    export_csv_path  -- path to the CSV produced by backfill_export()

    Returns a summary dict: {processed, linked, created_new, failed, skipped}.
    """
    # Build lookup: normalized_linkedin_url -> attio_record_id
    def _normalize_url(url: str) -> str:
        url = url.strip().rstrip("/").lower()
        url = url.replace("://linkedin.com/", "://www.linkedin.com/")
        return url

    lookup: dict[str, str] = {}
    with open(export_csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = _normalize_url(row.get("linkedin_url", ""))
            rid = row.get("attio_record_id", "").strip()
            if url and rid:
                lookup[url] = rid

    counters = {"processed": 0, "linked": 0, "failed": 0, "skipped": 0}

    with open(pb_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for i, row in enumerate(rows):
        counters["processed"] += 1

        # Resolve LinkedIn URL from whichever column PB uses
        raw_url = row.get("linkedinProfileUrl", row.get("linkedInUrl", row.get("profileUrl", "")))
        norm_url = _normalize_url(raw_url)

        record_id = lookup.get(norm_url)
        if not record_id:
            counters["skipped"] += 1
            time.sleep(0.2)
            continue

        company_name = row.get("companyName", row.get("company", row.get("currentCompanyName", ""))).strip()
        if not company_name:
            counters["failed"] += 1
            time.sleep(0.2)
            continue

        domain = extract_real_domain(row)
        company_rid = match_or_create_company(attio, company_name, domain=domain or None)

        if company_rid:
            attio.update_person(
                record_id,
                {"company": [{"target_object": "companies", "target_record_id": company_rid}]},
            )
            counters["linked"] += 1
        else:
            counters["failed"] += 1

        time.sleep(0.2)

        if (i + 1) % 25 == 0:
            click.echo(
                f"  Progress: {i + 1}/{len(rows)} — "
                f"linked {counters['linked']}, failed {counters['failed']}, "
                f"skipped {counters['skipped']}"
            )

    click.echo("\nBackfill import complete:")
    click.echo(f"  Processed:    {counters['processed']}")
    click.echo(f"  Linked:       {counters['linked']}")
    click.echo(f"  Failed:       {counters['failed']}")
    click.echo(f"  Skipped:      {counters['skipped']}")

    return counters


def detect_bad_company_links(crm: CRMProvider) -> str:
    """Scan pipeline entries for people linked to companies with linkedin.com domain.

    The LinkedIn-domain bug caused match_or_create_company to save
    ``linkedin.com`` as the domain on the first company it created.  Every
    subsequent prospect then matched that company via domain search.

    Returns the path to an exported CSV listing the affected records.

    P1c migration note: this slice reads the CRM exclusively through the
    vendor-neutral ``CRMProvider`` contract (``query_list_entries`` →
    ``list[Entry]``, ``get_person`` → ``Record``, ``get_company`` → ``Record``,
    ``extract_person_info`` → ``RecordInfo``). Structured company/domain fields
    have no contract attribute, so they are read via ``Record.raw`` (the
    contract's documented escape hatch). The write sibling ``repair_bad_companies``
    stays on ``AttioClient`` — it migrates in a later increment.
    """
    entries = crm.query_list_entries(limit=50_000, fail_if_truncated=True)

    total = len(entries)
    bad_rows: list[dict] = []
    # Cache: company_record_id → (is_bad, company_name)
    company_cache: dict[str, tuple[bool, str]] = {}

    click.echo(f"Scanning {total} pipeline entries...")

    for i, entry in enumerate(entries):
        record_id = entry.record_id
        if not record_id:
            continue

        person = crm.get_person(record_id)
        time.sleep(0.15)

        if person is None:
            continue

        # Check for linked company.  The company reference is a structured
        # multi-value slug the contract does not model as a flat attribute, so
        # read it off the untouched vendor payload via Record.raw (the
        # contract's escape hatch).
        company_data = person.raw.get("values", {}).get("company", [])
        if not company_data or not isinstance(company_data, list):
            continue
        company_rid = company_data[0].get("target_record_id", "")
        if not company_rid:
            continue

        # Check if this company has linkedin.com domain (cached)
        if company_rid not in company_cache:
            company_record = crm.get_company(company_rid)
            time.sleep(0.15)
            is_bad = False
            company_name = "Unknown"
            if company_record:
                name_data = company_record.raw.get("values", {}).get("name", [])
                if name_data:
                    company_name = name_data[0].get("value", "Unknown")
                domains = company_record.raw.get("values", {}).get("domains", [])
                for d in domains:
                    domain_val = d.get("domain", "") if isinstance(d, dict) else str(d)
                    if "linkedin.com" in domain_val.lower():
                        is_bad = True
                        break
            company_cache[company_rid] = (is_bad, company_name)

        is_bad, company_name = company_cache[company_rid]
        if not is_bad:
            continue

        # Extract person info
        info = crm.extract_person_info(person)
        bad_rows.append({
            "linkedin_url": info.linkedin_url if info.linkedin_url is not None else "",
            "attio_record_id": record_id,
            "name": info.name,
            "current_wrong_company": company_name,
        })

        if (i + 1) % 50 == 0:
            click.echo(f"  Progress: {i + 1}/{total} entries scanned, {len(bad_rows)} bad links found")

    os.makedirs("exports", exist_ok=True)
    file_path = f"exports/bad_company_links_{date.today().isoformat()}.csv"

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["linkedin_url", "attio_record_id", "name", "current_wrong_company"])
        writer.writeheader()
        writer.writerows(bad_rows)

    click.echo("\nDetection complete:")
    click.echo(f"  Total entries:   {total}")
    click.echo(f"  Bad links found: {len(bad_rows)}")
    click.echo(f"  File:            {file_path}")

    return file_path


def repair_bad_companies(
    attio: AttioClient,
    pb: PhantomBusterClient,
    sales_nav_profile_scraper_id: str,
    detect_csv_path: str,
) -> dict:
    """Repair pipeline records with bad company links via PB re-scraping.

    1. Clean linkedin.com domain from the poisoned company record(s)
    2. Launch the Sales Nav Profile Scraper on affected LinkedIn URLs
    3. Re-link via backfill_import (now using extract_real_domain)

    Migrated to the Sales Navigator Profile Scraper — the legacy LinkedIn
    Profile Scraper agent this pipeline used to launch was deleted from the
    PB workspace. backfill_import already speaks the SN CSV contract: it
    prefers ``linkedinProfileUrl`` for URL matching and falls back to
    ``currentCompanyName`` for the company name. The SN CSV carries no
    real-website column (``companyUrl``/``companyWebsite``), so
    extract_real_domain returns "" and match_or_create_company falls back to
    name-only matching — the behavior that helper documents.

    Returns the backfill_import summary dict.
    """
    from clients.google_sheets import write_prospects_to_sheet
    from workflows.daily_check_helpers import (
        _fresh_csv_name,
        build_sales_nav_launch_args,
    )

    # Step 0: Clean poisoned company domains. Loop because there may be
    # multiple companies poisoned with linkedin.com (different runs of the
    # weekly pipeline created different "default" companies).
    click.echo("--- Step 0: Cleaning linkedin.com domains from company records ---")
    cleaned_count = 0
    seen_rids: set[str] = set()
    while True:
        bad_company = attio.search_company_by_domain("linkedin.com")
        if not bad_company:
            break
        rid = bad_company["id"]["record_id"]
        if rid in seen_rids:
            # Safety: avoid infinite loop if update hasn't propagated yet
            break
        seen_rids.add(rid)
        name_data = bad_company.get("values", {}).get("name", [])
        name = name_data[0].get("value", "?") if name_data else "?"
        attio.update_company(rid, {"domains": []})
        click.echo(f"  Cleared linkedin.com domain from '{name}' ({rid})")
        cleaned_count += 1
    if cleaned_count == 0:
        click.echo("  No company with linkedin.com domain found (already clean)")

    # Step 1: Read detection CSV
    with open(detect_csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        click.echo("No records to repair.")
        return {"processed": 0, "linked": 0, "failed": 0, "skipped": 0, "deferred": 0}

    deferred = 0
    if len(rows) > REPAIR_MAX_PROFILES_PER_LAUNCH:
        deferred = len(rows) - REPAIR_MAX_PROFILES_PER_LAUNCH
        rows = rows[:REPAIR_MAX_PROFILES_PER_LAUNCH]
        click.echo(
            f"  ⚠ Capping scrape at {REPAIR_MAX_PROFILES_PER_LAUNCH} profiles "
            f"per launch ({deferred} deferred). After this run, re-run "
            f"detect-bad-companies (repaired rows drop out of the new CSV) and "
            f"repair-companies again for the remainder."
        )

    click.echo(f"\n--- Step 1: Scraping {len(rows)} profiles ---")

    # Step 2: Write to Google Sheet and launch the Sales Nav Profile Scraper
    sheet_rows = [{"profileUrl": row["linkedin_url"]} for row in rows]
    # columns must match the row keys — the default ["linkedInUrl", "message"]
    # shape would write an empty sheet and the scraper would no-op silently.
    sheet_url = write_prospects_to_sheet(
        sheet_rows, columns=["profileUrl"], include_header=False
    )
    click.echo(f"  Wrote {len(sheet_rows)} URLs to Google Sheet")

    # Saved-args + identities-inject contract lives in the shared helper
    # (raises SalesNavConfigError if the SN cookie env var is missing).
    # launch_count: ``profiles_per_launch`` adds the sheet header line PB
    # counts as a processable row (else the LAST repair row is silently
    # dropped — see clients.google_sheets.profiles_per_launch). API
    # ``arguments`` REPLACE the phantom's saved console args wholesale, so the
    # per-launch count must be explicit or the phantom truncates at its
    # built-in default (10). After the cap above, ``len(sheet_rows)`` is
    # bounded by REPAIR_MAX_PROFILES_PER_LAUNCH, so batch + header stays under
    # the phantom schema max and PB never rejects the whole launch.
    csv_name = _fresh_csv_name("repair")
    launch_args: dict = {
        **build_sales_nav_launch_args(
            pb,
            sales_nav_profile_scraper_id,
            spreadsheet_url=sheet_url,
            launch_count=len(sheet_rows),
        ),
        # Fresh result-file name per launch: PB keys the phantom's
        # processed-inputs dedup DB on the file name, and a repair re-scrape
        # MUST re-visit profiles a prior daily-run scrape already processed.
        "csvName": csv_name,
    }

    click.echo("  Launching Sales Nav Profile Scraper...")
    launch = pb.launch_agent(sales_nav_profile_scraper_id, launch_args)

    # Step 3: Wait and download (F-PR-5: typed launch, container-keyed CSV).
    # 900s ceiling: the SN scraper queues + executes slower than the deleted
    # legacy scraper (daily_check Phase 0 uses 750s) and repair batches run up
    # to the full 50-profile cap, so give it the old 600s plus headroom.
    click.echo("  Waiting for completion (up to 15 min)...")
    pb.wait_for_completion(launch, poll_interval=15, max_wait=900)

    result_csv = pb.download_result_csv(launch, csv_name=csv_name)
    if not result_csv:
        # "error" key distinguishes this failure from a clean run over an
        # empty detect CSV (which returns the same zero counters) — the
        # caller must surface it as a FAILURE, not "Repair Complete". Same
        # pattern as Phase 0's graceful-degrade return in daily_check.
        click.echo("  ERROR: Sales Nav Profile Scraper returned no CSV", err=True)
        return {
            "processed": 0, "linked": 0, "failed": 0, "skipped": 0,
            "deferred": deferred, "error": "no_csv",
        }

    pb_output_path = f"exports/repair_companies_pb_{date.today().isoformat()}.csv"
    with open(pb_output_path, "w", encoding="utf-8") as f:
        f.write(result_csv)
    click.echo(f"  Saved PB output to {pb_output_path}")

    # Step 4: Re-link via backfill_import
    click.echo("\n--- Step 2: Re-linking companies ---")
    summary = backfill_import(attio, pb_output_path, detect_csv_path)
    summary["deferred"] = deferred
    return summary
