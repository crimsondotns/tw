import time
import requests
import json
import random
from datetime import datetime
from typing import List, Dict, Tuple
import common

# ==========================================
# CONSTANTS
# ==========================================
ENGAGEMENT_RANGE = "B6:G"
ENGAGEMENT_START_ROW = 6
ENGAGEMENT_COL_COUNT = 6        # B..G = 6 columns
IDENTIFIER_COL = 4              # Column F (0-indexed from B)
LINK_COL = 5                    # Column G (0-indexed from B)
MAX_CONSECUTIVE_ERRORS = 5
ANTI_BAN_DELAY = (5.0, 10.0)

USER_BY_SCREEN_NAME_FEATURES = {
    "hidden_profile_subscriptions_enabled": True,
    "payments_enabled": False,
    "rweb_xchat_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "verified_phone_label_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": True,
    "subscriptions_feature_can_gift_premium": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
}

# ==========================================
# HTTP STATUS LOGGING
# ==========================================
_STATUS_MAP = {
    200: ("log_success", "\033[92m200\033[0m", "Successfully fetched profile"),
    403: ("log_error",   "\033[91m403\033[0m", "Forbidden access"),
    404: ("log_error",   "\033[91m404\033[0m", "User not found"),
    429: ("log_warn",    "\033[93m429\033[0m", "Rate limit exceeded"),
}

def log_http_status(status: int, context: str = ""):
    fn_name, code_fmt, msg = _STATUS_MAP.get(
        status, ("log_error", f"\033[91m{status}\033[0m", "Error fetching profile")
    )
    getattr(common, fn_name)(f"HTTP {code_fmt} | {msg}", context=context)

# ==========================================
# FETCH USER PROFILE (UserByScreenName)
# ==========================================
def fetch_user_profile(screen_name: str, row_idx: int) -> Tuple[int, str, str]:
    """Fetch posts_count and followers_count for a user via X GraphQL.

    Returns:
        (http_status, posts_val, followers_val)
    """
    variables = {"screen_name": screen_name, "withGrokTranslatedBio": False}
    field_toggles = {"withAuxiliaryUserLabels": True}

    url = (
        "https://api.x.com/graphql/ck5KkZ8t5cOmoLssopN99Q/UserByScreenName?"
        f"variables={requests.utils.quote(json.dumps(variables))}"
        f"&features={requests.utils.quote(json.dumps(USER_BY_SCREEN_NAME_FEATURES))}"
        f"&fieldToggles={requests.utils.quote(json.dumps(field_toggles))}"
    )

    old_tag = common.ENDPOINT_TAG
    common.ENDPOINT_TAG = screen_name
    try:
        resp = common.call_x_with_backoff(url, row_idx=row_idx)
        status = resp.status_code

        if status != 200:
            return status, "", f"status={status}"

        data = resp.json()
        legacy = data.get("data", {}).get("user", {}).get("result", {}).get("legacy")
        if legacy:
            return 200, str(legacy.get("statuses_count", "")), str(legacy.get("followers_count", ""))
        else:
            return 200, "Account suspended", "Account suspended"
    finally:
        common.ENDPOINT_TAG = old_tag

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
    """Extract username or community rest_id from row data."""
    username = row_data[IDENTIFIER_COL].strip().lstrip("@")
    if not username:
        ident = common.extract_identifier_from_link(row_data[LINK_COL].strip())
        if ident:
            username = ident.lstrip("@")
    return username

# ==========================================
# SYNC RESULTS TO GOOGLE SHEETS
# ==========================================
def sync_results_to_sheet(session_results: List[Tuple[str, List[str], List[str]]]):
    """Overwrite SHEET_NAME_USER_ON_X with fetched stats."""
    all_rows = [row_data + stats for _, row_data, stats in session_results if stats]
    if not all_rows:
        return

    common.log_info("[SYNC] Preparing to overwrite data")
    try:
        common.sheet_user_on_x.batch_clear(["A2:H1000"])
        common.sheet_user_on_x.update(
            values=all_rows,
            range_name=f"A2:H{1 + len(all_rows)}",
            value_input_option="RAW",
        )
        common.log_info(f"[SYNC] Overwrote {len(all_rows)} rows successfully")
    except Exception as e:
        common.log_error(f"[SYNC] Sheet write error: {e!s}")
        raise

def sync_error_log(daily_errors: Dict[str, dict]):
    """Merge daily errors into the persistent error.log sheet."""
    import gspread

    try:
        sh = common.client.open_by_key(common.SPREADSHEET_ID)
        try:
            error_sheet = sh.worksheet("error.log")
        except gspread.exceptions.WorksheetNotFound:
            error_sheet = sh.add_worksheet(title="error.log", rows=1000, cols=4)

        # Merge existing + new errors
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
def get_twitter_user_stats():
    overall_start = time.perf_counter()
    daily_errors: Dict[str, dict] = {}
    session_results: List[Tuple[str, List[str], List[str]]] = []
    consecutive_errors = 0

    engagement_rows = load_engagement_rows()
    total_accounts = len(engagement_rows)
    common.log_info(f"[APP] Initializing get_twitter_user_stats (Target: {total_accounts} accounts)")

    for idx, row_data in engagement_rows:
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            common.log_error(f"[APP] Emergency stop: {MAX_CONSECUTIVE_ERRORS} consecutive errors.")
            break

        ident = resolve_identifier(row_data)
        if not ident:
            common.log_warn(f"[BYPASS] Row {idx} skipped: Missing or invalid identifier")
            continue

        is_community = common.is_rest_id(ident)
        ctx = f"Row {idx} | Community {ident}" if is_community else f"Row {idx} | @{ident}"

        try:
            common.log_info("Fetching profile...", context=ctx)

            if is_community:
                status, member_count = common.fetch_community_member_count(ident, row_idx=idx)
                posts_val = ""
                followers_val = str(member_count) if (status == 200 and member_count >= 0) else (
                    "rate_limited" if status == 429 else f"status={status}"
                )
            else:
                status, posts_val, followers_val = fetch_user_profile(ident, row_idx=idx)

            log_http_status(status, context=ctx)

            # Track errors
            if status == 200:
                consecutive_errors = 0
                daily_errors[ident] = None
            else:
                if status not in (403, 404):
                    consecutive_errors += 1
                ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
                daily_errors[ident] = {
                    "ts": ts_err,
                    "instance": "X API",
                    "msg": f"HTTP {status} - https://x.com/{ident}",
                }

            session_results.append((ident, row_data, [posts_val, followers_val]))
            time.sleep(random.uniform(*ANTI_BAN_DELAY))

        except Exception as e:
            common.log_error(f"Exception at row {idx}: {e!s}", context=ctx)
            log_http_status(500, context=ctx)
            ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
            daily_errors[ident] = {"ts": ts_err, "instance": "Local", "msg": f"Exception: {e!s}"}
            consecutive_errors += 1

    # --- Sync ---
    if session_results:
        sync_results_to_sheet(session_results)
    sync_error_log(daily_errors)

    total_min = (time.perf_counter() - overall_start) / 60.0
    processed = len(session_results)
    common.log_info(
        f"[APP] Execution Summary: Processed {processed}/{total_accounts}"
        f" | Sheet: '{common.SHEET_NAME_USER_ON_X}' | Duration: {total_min:.2f}m"
    )

if __name__ == "__main__":
    get_twitter_user_stats()
