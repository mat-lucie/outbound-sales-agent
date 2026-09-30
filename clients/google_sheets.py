"""Google Sheets client for writing prospect data."""

import contextlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Callable
from typing import TypeVar

import gspread

_T = TypeVar("_T")

# gspread APIError statuses worth retrying: transient Google backend / rate-limit
# responses (429 rate-limit, 500/502/503 backend). A crash on ws.clear() from a
# one-off 503 took down a whole `daily` run mid-Phase-0 (PR-239 incident), so
# the mutating Sheets calls now ride a small backoff instead of propagating.
_RETRYABLE_SHEETS_STATUS = frozenset({429, 500, 502, 503})
_SHEETS_RETRY_BACKOFF = (5, 10, 20)  # seconds between attempts; mirrors clients/attio.py


def _with_retry(fn: Callable[[], _T], *, attempts: int = 3) -> _T:
    """Run ``fn`` with retry on transient gspread ``APIError`` statuses.

    Retries only when the APIError's response status is in
    ``_RETRYABLE_SHEETS_STATUS`` (429/500/502/503), sleeping 5/10/20s between
    attempts (mirrors the cadence of ``clients/attio.py``'s ``_request``; that
    helper is httpx-specific and not reusable here). A non-retryable status
    (e.g. 403 permission) re-raises immediately; the final attempt re-raises
    whatever it hit. ``fn`` takes no args — close over the call at the site.
    """
    for attempt in range(attempts):
        try:
            return fn()
        except gspread.exceptions.APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in _RETRYABLE_SHEETS_STATUS or attempt == attempts - 1:
                raise
            time.sleep(_SHEETS_RETRY_BACKOFF[min(attempt, len(_SHEETS_RETRY_BACKOFF) - 1)])
    raise AssertionError("unreachable")  # pragma: no cover

CREDENTIALS_DIR = os.path.join(os.path.dirname(__file__), "..", "credentials")
OAUTH_CREDENTIALS = os.path.join(CREDENTIALS_DIR, "google-oauth.json")
AUTHORIZED_USER = os.path.join(CREDENTIALS_DIR, "google-authorized-user.json")
AUTHORIZED_USER_BACKUP = AUTHORIZED_USER + ".bak"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _check_oauth_credentials() -> None:
    """Raise a clear RuntimeError if google-oauth.json is missing or 0-byte.

    gspread.oauth() crashes opaquely (KeyError / JSONDecodeError) when the
    credentials file is empty. Catching this early gives the operator an
    actionable error message instead of a cryptic stack trace deep inside
    google-auth. (T1.5c from operator-safety audit.)
    """
    if not os.path.exists(OAUTH_CREDENTIALS):
        raise RuntimeError(
            f"Missing OAuth credentials file: {OAUTH_CREDENTIALS}\n"
            "Fix: create a Google OAuth2 client secret JSON and save it to "
            "credentials/google-oauth.json. "
            "See docs at: https://docs.gspread.org/en/latest/oauth2.html"
        )
    if os.path.getsize(OAUTH_CREDENTIALS) == 0:
        raise RuntimeError(
            f"OAuth credentials file is 0 bytes: {OAUTH_CREDENTIALS}\n"
            "Fix: the file exists but is empty — re-download the OAuth2 client "
            "secret JSON from Google Cloud Console and save it to "
            "credentials/google-oauth.json."
        )


def get_client() -> gspread.Client:
    """Get an authenticated gspread client using OAuth2 credentials.

    gspread (via google-auth) writes the authorized-user token file
    non-atomically when refreshing access tokens. A network drop mid-write
    can truncate it to 0 bytes, after which gspread.oauth() crashes with
    JSONDecodeError on next start. We heal that here by restoring a known-
    good backup, or removing the broken file so a fresh interactive flow
    can run. After successful auth we refresh the backup atomically.
    """
    _heal_corrupt_token()
    # The client-secret preflight only applies when an INTERACTIVE flow
    # would run: gspread.oauth() never reads google-oauth.json while a
    # usable authorized-user token exists. Enforcing it unconditionally
    # would block sends when the client-secret file is 0-byte but the
    # authorized-user token is valid. (7bd45b3 fix.)
    if not _is_valid_token(AUTHORIZED_USER):
        _check_oauth_credentials()
    client = gspread.oauth(
        credentials_filename=OAUTH_CREDENTIALS,
        authorized_user_filename=AUTHORIZED_USER,
        scopes=SCOPES,
    )
    _backup_token()
    return client


def _is_valid_token(path: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return False
    try:
        with open(path) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return False
    return bool(data.get("refresh_token"))


def _heal_corrupt_token() -> None:
    if not os.path.exists(AUTHORIZED_USER) or _is_valid_token(AUTHORIZED_USER):
        return
    if _is_valid_token(AUTHORIZED_USER_BACKUP):
        shutil.copy2(AUTHORIZED_USER_BACKUP, AUTHORIZED_USER)
        print(f"  Restored corrupt OAuth token from {AUTHORIZED_USER_BACKUP}")
        return
    os.remove(AUTHORIZED_USER)
    print("  Removed corrupt OAuth token; gspread will trigger re-auth flow.")


def _backup_token() -> None:
    if not _is_valid_token(AUTHORIZED_USER):
        return
    tmp = f"{AUTHORIZED_USER_BACKUP}.tmp.{os.getpid()}"
    try:
        shutil.copy2(AUTHORIZED_USER, tmp)
        os.replace(tmp, AUTHORIZED_USER_BACKUP)
    except OSError:
        if os.path.exists(tmp):
            with contextlib.suppress(OSError):
                os.remove(tmp)


# Header rows prepended by write_prospects_to_sheet. PB phantoms count the
# header as a processable CSV line (see profiles_per_launch below), so launch
# counts derived from a batch written through that helper must add this.
SHEET_HEADER_LINES = 1


def profiles_per_launch(batch_size: int) -> int:
    """``numberOfProfilesPerLaunch`` for a batch written via
    :func:`write_prospects_to_sheet`.

    The sheet writer always prepends a header row, and PB phantoms count
    that header as a processable line: a launch with
    ``numberOfProfilesPerLaunch=N`` against an N-row sheet processes the
    header plus only N-1 data rows, silently dropping the LAST row of
    every batch (verified live 2026-06-12, twice: a 1-profile Phase 0
    batch logged "Got 2 lines from csv → Processing 1 profile" then
    errored on the literal header → BLIND run, container 1117150263943401;
    a 4-profile batch scraped only 3). Passing batch+header is safe
    because the phantom treats the argument as a tight cap and never
    processes more lines than the input contains.

    Callers whose batches are bounded by a per-launch cap (e.g.
    PHASE0_MAX_PROFILES_PER_LAUNCH, REPAIR_MAX_PROFILES_PER_LAUNCH) must
    keep cap + SHEET_HEADER_LINES at or under the phantom's argument
    schema maximum (150 for the SN Profile Scraper, verified 2026-06-11)
    — PB rejects the ENTIRE launch above it.

    Does NOT apply to launches that pass a bare profile URL as
    ``spreadsheetUrl`` (no sheet, no header) — pass the raw batch size
    there.
    """
    return batch_size + SHEET_HEADER_LINES


def write_prospects_to_sheet(
    rows: list[dict],
    spreadsheet_id: str | None = None,
    worksheet_name: str = "Sheet1",
    columns: list[str] | None = None,
    include_header: bool = True,
) -> str:
    """Write prospect rows to a Google Sheet, replacing all existing data.

    Columns default to ``["linkedInUrl", "message"]`` (the Network Booster /
    Message Sender input shape). Pass ``columns`` to override — e.g. Phase-0
    Profile Scraper inputs should pass ``columns=["profileUrl"]`` and
    ``include_header=False``: the phantom processes a header as a profile.

    Row keys must match the requested column names exactly (case-sensitive).
    Returns the spreadsheet URL for use as a PB phantom input.
    """
    if columns is None:
        columns = ["linkedInUrl", "message"]
    sid = spreadsheet_id or os.environ["GSHEET_AUTOCONNECT_ID"]
    gc = get_client()
    sh = gc.open_by_key(sid)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=worksheet_name, rows=len(rows) + int(include_header), cols=len(columns)
        )

    _with_retry(ws.clear)
    data = ([columns] if include_header else []) + [
        [row.get(col, "") for col in columns] for row in rows
    ]
    _with_retry(lambda: ws.update(data, "A1"))

    return f"https://docs.google.com/spreadsheets/d/{sid}"


def write_identity_batch(urls: list[str]) -> str:
    """Write an isolated, headerless input tab without replacing another run's input.

    The tab inherits the configured input spreadsheet's access policy. Retain
    it after timeout: the provider may still be reading it. No sharing changes.
    """
    if not urls or len(urls) > 50:
        raise ValueError("Identity batch must contain between 1 and 50 URLs")
    sid = os.environ["GSHEET_AUTOCONNECT_ID"]
    sh = get_client().open_by_key(sid)
    # Do not retry creation after an ambiguous response: another tab may exist.
    ws = sh.add_worksheet(title=f"inbox-identity-{uuid.uuid4().hex}", rows=len(urls), cols=1)
    data = [[url] for url in urls]
    from gspread.utils import ValueInputOption
    _with_retry(lambda: ws.update(data, "A1", value_input_option=ValueInputOption.raw))
    if ws.get_all_values() != data:
        raise ValueError("Identity input tab readback differs from requested URLs")
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit#gid={ws.id}"
