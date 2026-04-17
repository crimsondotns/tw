import time
import re
import random
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict
import common

# ==========================================
# CONSTANTS
# ==========================================
ENGAGEMENT_RANGE     = "B6:G"
ENGAGEMENT_START_ROW = 6
ENGAGEMENT_COL_COUNT = 6            # B..G = 6 columns
IDENTIFIER_COL      = 4            # Column F (0-indexed from B)
LINK_COL             = 5            # Column G (0-indexed from B)
MAX_CONSECUTIVE_ERRORS = 5
ANTI_BAN_DELAY       = (5.0, 10.0)
NITTER_INSTANCES     = ["nitter.net"]
DEFAULT_DAYS         = 30
CHUNK_SIZE           = 500

# ==========================================
# HTTP STATUS LOGGING
# ==========================================
_STATUS_MAP = {
    200: ("log_success", "\033[92m200\033[0m", "Successfully fetched posts"),
    403: ("log_error",   "\033[91m403\033[0m", "Forbidden access"),
    404: ("log_error",   "\033[91m404\033[0m", "User not found"),
    429: ("log_warn",    "\033[93m429\033[0m", "Rate limit exceeded"),
}

def log_http_status(status: int, context: str = ""):
    fn_name, code_fmt, msg = _STATUS_MAP.get(
        status, ("log_error", f"\033[91m{status}\033[0m", "Error fetching posts")
    )
    getattr(common, fn_name)(f"HTTP {code_fmt} | {msg}", context=context)

# ==========================================
# LOAD ENGAGEMENT DATA
# ==========================================
def load_engagement_rows() -> List[Tuple[int, List[str]]]:
    """Read engagement sheet and return list of (row_idx, padded_row_data)."""
    raw = common.sheet_engagement.get(ENGAGEMENT_RANGE)
    return [
        (row_idx, row + [""] * (ENGAGEMENT_COL_COUNT - len(row)))
        for row_idx, row in enumerate(raw, start=ENGAGEMENT_START_ROW)
    ]

def resolve_identifier(row_data: List[str]) -> str:
    """Extract username from row data (Column F first, fallback to Column G link)."""
    username = row_data[IDENTIFIER_COL].strip().lstrip("@")
    if not username:
        ident = common.extract_identifier_from_link(row_data[LINK_COL].strip())
        if ident:
            username = ident.lstrip("@")
    return username

# ==========================================
# NITTER RSS FETCHER
# ==========================================
def fetch_recent_tweets_nitter(username: str, days: int = DEFAULT_DAYS) -> Tuple[int, List[Tuple[str, str]]]:
    """Fetch recent tweets via Nitter RSS feed.

    Returns:
        (http_status, [(date_str, content), ...])
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    tweets: List[Tuple[str, str]] = []

    instances = list(NITTER_INSTANCES)
    random.shuffle(instances)

    for instance in instances:
        url = f"https://{instance}/{username}/rss"
        try:
            resp = common.session.get(url, timeout=15)
            if resp.status_code != 200:
                if resp.status_code in (403, 404):
                    return resp.status_code, []
                continue

            root = ET.fromstring(resp.content)
            channel = root.find("channel")
            if channel is None:
                continue

            for item in channel.findall("item"):
                title    = item.find("title")
                pub_date = item.find("pubDate")
                link     = item.find("link")
                if title is None or pub_date is None:
                    continue

                content = title.text or ""
                if content == "Image":
                    desc = item.find("description")
                    if desc is not None and desc.text:
                        m = re.search(r'src="([^"]+)"', desc.text)
                        if m:
                            content = m.group(1).replace("&amp;", "&")

                source_url = link.text.strip().split("#")[0] if link is not None and link.text else ""

                try:
                    dt = datetime.strptime(
                        pub_date.text, "%a, %d %b %Y %H:%M:%S %Z"
                    ).replace(tzinfo=timezone.utc)
                    if dt >= cutoff:
                        source = f"Source: {source_url}" if source_url else ""
                        parts = [content, source, pub_date.text]
                        tweets.append((dt.strftime("%Y-%m-%d"), "\n\n".join(p for p in parts if p)))
                except Exception:
                    continue

            tweets.sort(key=lambda x: x[0], reverse=True)
            return 200, tweets

        except Exception:
            continue

    return 500, []

# ==========================================
# X API EXISTENCE CHECK (fallback when Nitter fails)
# ==========================================
def check_user_exists_on_x(username: str, row_idx: int) -> int:
    """Quick check via UserByScreenName to distinguish 404 vs rate-limit.

    Returns:
        Effective status code: 429 (user exists, Nitter issue) or 404 / other.
    """
    variables    = {"screen_name": username, "withGrokTranslatedBio": False}
    features     = {"hidden_profile_subscriptions_enabled": True}
    field_toggles = {"withAuxiliaryUserLabels": True}

    url = (
        "https://api.x.com/graphql/ck5KkZ8t5cOmoLssopN99Q/UserByScreenName?"
        f"variables={requests.utils.quote(json.dumps(variables))}"
        f"&features={requests.utils.quote(json.dumps(features))}"
        f"&fieldToggles={requests.utils.quote(json.dumps(field_toggles))}"
    )

    old_tag = common.ENDPOINT_TAG
    common.ENDPOINT_TAG = username
    try:
        resp = common.call_x_with_backoff(url, row_idx=row_idx)
        if resp.status_code != 200:
            return resp.status_code
        data = resp.json()
        has_legacy = data.get("data", {}).get("user", {}).get("result", {}).get("legacy")
        return 429 if has_legacy else 404
    except Exception:
        return 500
    finally:
        common.ENDPOINT_TAG = old_tag

# ==========================================
# SYNC RESULTS TO GOOGLE SHEETS
# ==========================================
def sync_results_to_sheet(session_results: List[Tuple[str, List[str], List[str]]]):
    """Overwrite SHEET_NAME_MIGRATION with fetched posts."""
    all_rows: List[List[str]]  = []
    max_tweets = 0
    total_posts = 0

    for _, row_data, texts in session_results:
        if not texts:
            continue
        all_rows.append(row_data + texts)
        max_tweets = max(max_tweets, len(texts))
        total_posts += len(texts)

    if not all_rows:
        return 0

    full_width = ENGAGEMENT_COL_COUNT + max_tweets

    common.log_info("[SYNC] Preparing to overwrite data")
    try:
        common.sheet_migration.batch_clear(["A2:ZZ500000"])

        total_chunks = (len(all_rows) + CHUNK_SIZE - 1) // CHUNK_SIZE
        for i in range(0, len(all_rows), CHUNK_SIZE):
            chunk     = all_rows[i : i + CHUNK_SIZE]
            start_row = i + 2
            end_row   = start_row + len(chunk) - 1
            last_col  = common.gspread.utils.rowcol_to_a1(1, full_width).rstrip("1")
            range_name = f"A{start_row}:{last_col}{end_row}"

            common.sheet_migration.update(
                values=chunk, range_name=range_name, value_input_option="RAW"
            )
            common.log_info(
                f"[SYNC] Written chunk {i // CHUNK_SIZE + 1}/{total_chunks} "
                f"({len(chunk)} users) to '{common.SHEET_NAME_MIGRATION}'!{range_name}"
            )

        common.log_info(f"[SYNC] Overwrote {len(all_rows)} rows successfully")
    except Exception as e:
        common.log_error(f"[SYNC] Sheet write error: {e!s}")
        raise

    # Write completion timestamp
    final_ts = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
    try:
        common.sheet_migration.update(values=[[final_ts]], range_name="G1", value_input_option="RAW")
        common.log_info(f"[SYNC] Final timestamp written: {final_ts}")
    except Exception as e:
        common.log_error(f"[SYNC] Failed to write timestamp: {e!s}")

    return total_posts

def sync_error_log(daily_errors: Dict[str, dict]):
    """Merge daily errors into the persistent error.log sheet."""
    import gspread as _gspread

    try:
        sh = common.client.open_by_key(common.SPREADSHEET_ID)
        try:
            error_sheet = sh.worksheet("error.log")
        except _gspread.exceptions.WorksheetNotFound:
            error_sheet = sh.add_worksheet(title="error.log", rows=1000, cols=4)

        header = ["Timestamp", "Username", "Instance", "Error Message"]
        merged = {
            r[1]: {"ts": r[0], "instance": r[2], "msg": r[3]}
            for r in error_sheet.get_all_values()[1:]
            if len(r) >= 4
        }

        for ident, err in daily_errors.items():
            if err is None:
                merged.pop(ident, None)
            else:
                merged[ident] = err

        error_sheet.clear()
        if merged:
            rows = [header] + [[e["ts"], u, e["instance"], e["msg"]] for u, e in merged.items()]
            error_sheet.update(values=rows, range_name="A1", value_input_option="RAW")
            common.log_info(f"[SYNC] Updated error.log with {len(merged)} unique errors.")
    except Exception as e:
        common.log_error(f"[SYNC] Failed to update error.log sheet: {e!s}")

# ==========================================
# MAIN ORCHESTRATOR
# ==========================================
def get_twitter_user_recent_posts(days: int = DEFAULT_DAYS):
    overall_start = time.perf_counter()
    daily_errors: Dict[str, dict] = {}
    session_results: List[Tuple[str, List[str], List[str]]] = []
    consecutive_errors = 0
    total_tweets = 0

    engagement_rows = load_engagement_rows()
    total_accounts  = len(engagement_rows)
    common.log_info(f"[APP] Initializing get_twitter_user_recent_posts (Target: {total_accounts} accounts)")
    common.log_info(f"[NET] Nitter instances active: {len(NITTER_INSTANCES)} servers")

    for idx, row_data in engagement_rows:
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            common.log_error(f"[APP] Emergency stop: {MAX_CONSECUTIVE_ERRORS} consecutive errors.")
            break

        username = resolve_identifier(row_data)
        if not username:
            common.log_warn(f"[BYPASS] Row {idx} skipped: Missing or invalid identifier")
            continue

        if common.is_rest_id(username):
            common.log_info(f"[BYPASS] Row {idx} skipped: Community {username}")
            continue

        ctx = f"Row {idx} | @{username}"
        try:
            common.log_info("Fetching posts...", context=ctx)
            status, tweets = fetch_recent_tweets_nitter(username, days=days)

            # Nitter failed → check if account actually exists on X
            if status != 200:
                common.log_warn(f"Nitter failed ({status}), checking X API existence...", context=ctx)
                status = check_user_exists_on_x(username, row_idx=idx)

            log_http_status(status, context=ctx)

            # Track errors
            if status == 200:
                consecutive_errors = 0
                daily_errors[username] = None
            else:
                if status not in (403, 404):
                    consecutive_errors += 1
                ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
                readable_url = f"https://x.com/{username}"
                daily_errors[username] = {
                    "ts": ts_err,
                    "instance": "Nitter/X-API",
                    "msg": f"HTTP {status} - {readable_url}",
                }

            texts = [t[1] for t in tweets]
            total_tweets += len(texts)
            session_results.append((username, row_data, texts))
            time.sleep(random.uniform(*ANTI_BAN_DELAY))

        except Exception as e:
            common.log_error(f"Exception at row {idx}: {e!s}", context=ctx)
            log_http_status(500, context=ctx)
            ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
            daily_errors[username] = {"ts": ts_err, "instance": "Local", "msg": f"Exception: {e!s}"}
            consecutive_errors += 1

    # --- Sync ---
    if session_results:
        total_tweets += sync_results_to_sheet(session_results)
    sync_error_log(daily_errors)

    total_min = (time.perf_counter() - overall_start) / 60.0
    processed = len(session_results)
    common.log_info(
        f"[APP] Execution Summary: Processed {processed}/{total_accounts}"
        f" | Sheet: '{common.SHEET_NAME_MIGRATION}'"
        f" | Posts: {total_tweets} | Duration: {total_min:.2f}m"
    )

if __name__ == "__main__":
    get_twitter_user_recent_posts()
