import time
import requests
import json
import os
import random
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict
import common

def deep_find_member_count(obj):
    if isinstance(obj, dict):
        if "member_count" in obj and isinstance(obj["member_count"], int):
            return obj["member_count"]
        for v in obj.values():
            found = deep_find_member_count(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = deep_find_member_count(v)
            if found is not None:
                return found
    return None

def fetch_community_member_count(rest_id: str, row_idx: Optional[int] = None) -> Tuple[int, int]:
    variables = {"communityId": rest_id}
    features = {
        "c9s_list_members_action_api_enabled": False,
        "c9s_superc9s_indication_enabled": False
    }
    url = (
        "https://x.com/i/api/graphql/2W09l7nD7ZbxGQHXvfB22w/CommunityQuery?"
        f"variables={requests.utils.quote(json.dumps(variables))}"
        f"&features={requests.utils.quote(json.dumps(features))}"
    )

    old_tag = common.ENDPOINT_TAG
    common.ENDPOINT_TAG = "CommunityQuery"
    try:
        if common.PREFER_USER_AUTH_FOR_COMMUNITY and common.have_user_auth():
            common.enable_user_auth_on_session()
        else:
            if "x-guest-token" not in common.session.headers:
                try:
                    common.refresh_guest_token()
                except Exception as e:
                    common.log_info(f"[ERROR] [AUTH] Initial guest token error: {e!s}", row_idx=row_idx)

        resp = common.call_x_with_backoff(url, row_idx=row_idx)
        status = resp.status_code

        if status == 403 and common.have_user_auth():
            if common.enable_user_auth_on_session():
                resp = common.session.get(url, timeout=20)
                status = resp.status_code
                common.log_info(f"[WARN]  [NET] [retry user] status={status}", row_idx=row_idx)

        if status == 200:
            data = resp.json()
            member_count = (data.get("data", {}).get("community", {}).get("member_count"))
            if not isinstance(member_count, int):
                member_count = deep_find_member_count(data) or -1
            return status, member_count
        else:
            return status, -1
    finally:
        common.ENDPOINT_TAG = old_tag


def log_http_status(status: int, label: str, context: str = ""):
    if status == 200:
        common.log_success(f"HTTP 200 | Successfully fetched profile", context=context)
    elif status == 403:
        common.log_error(f"HTTP 403 | Forbidden access", context=context)
    elif status == 404:
        common.log_error(f"HTTP 404 | User not found", context=context)
    elif status == 429:
        common.log_warn(f"HTTP 429 | Rate limit exceeded", context=context)
    else:
        common.log_error(f"HTTP {status} | Error fetching profile", context=context)

def get_twitter_user_stats():
    overall_start = time.perf_counter()

    daily_errors: Dict[str, dict] = {}
    
    raw_engagement_data = common.sheet_engagement.get("B4:G")
    engagement_rows = []
    
    for f_idx, row in enumerate(raw_engagement_data, start=4):
        padded_row = row + [""] * (6 - len(row))
        engagement_rows.append((f_idx, padded_row))

    total_accounts = len(engagement_rows)
    common.log_info(f"[INFO]  [APP] Initializing get_twitter_user_stats (Target: {total_accounts} accounts)")

    session_results: List[Tuple[str, List[str], List[str]]] = []

    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    for idx, row_data in engagement_rows:
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            common.log_info(f"[ERROR] [APP] Emergency stop triggered: {MAX_CONSECUTIVE_ERRORS} consecutive errors.")
            break

        username_raw = row_data[4].strip()
        link_raw = row_data[5].strip()
        
        username = username_raw.lstrip("@")
        if not username:
            ident_raw = common.extract_identifier_from_link(link_raw)
            if ident_raw:
                username = ident_raw.lstrip("@")
                
        if not username:
            common.log_info(f"[WARN]  [BYPASS] Row {idx} skipped: Missing or invalid identifier")
            continue

        ident = username
            
        posts_val = ""
        followers_val = ""
        status = 200

        ctx = f"Row {idx} | @{username}" if not common.is_rest_id(ident) else f"Row {idx} | Community {ident}"
        try:
            posts_val = ""
            followers_val = ""
            status = 200
            
            common.log_info(f"Fetching profile...", context=ctx)
            
            if common.is_rest_id(ident):
                status, member_count = fetch_community_member_count(ident, row_idx=idx)
                if status == 200 and member_count >= 0:
                    followers_val = str(member_count)
                    consecutive_errors = 0
                elif status == 429:
                    followers_val = "rate_limited"
                    consecutive_errors += 1
                else:
                    followers_val = f"status={status}"
                    if status not in [403, 404]: consecutive_errors += 1
            else:
                screen_name = ident
                variables = {"screen_name": screen_name, "withGrokTranslatedBio": False}
                features = {
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
                    "responsive_web_graphql_timeline_navigation_enabled": True
                }
                fieldToggles = {"withAuxiliaryUserLabels": True}

                url = (
                    "https://api.x.com/graphql/ck5KkZ8t5cOmoLssopN99Q/UserByScreenName?"
                    f"variables={requests.utils.quote(json.dumps(variables))}"
                    f"&features={requests.utils.quote(json.dumps(features))}"
                    f"&fieldToggles={requests.utils.quote(json.dumps(fieldToggles))}"
                )

                old_tag = common.ENDPOINT_TAG
                common.ENDPOINT_TAG = screen_name
                try:
                    resp = common.call_x_with_backoff(url, row_idx=idx)
                    status = resp.status_code
                    if status == 200:
                        data = resp.json()
                        legacy = (data.get("data", {}).get("user", {}).get("result", {}).get("legacy"))
                        if legacy:
                            posts_val = str(legacy.get("statuses_count", ""))
                            followers_val = str(legacy.get("followers_count", ""))
                        else:
                            posts_val = "Account suspended"
                            followers_val = "Account suspended"
                        consecutive_errors = 0
                    elif status == 429:
                        followers_val = "rate_limited"
                        consecutive_errors += 1
                    else:
                        followers_val = f"status={status}"
                        if status not in [403, 404]: consecutive_errors += 1
                finally:
                    common.ENDPOINT_TAG = old_tag

            log_http_status(status, ident, context=ctx)
            
            if status == 200:
                consecutive_errors = 0
                daily_errors[ident] = None
            else:
                ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
                readable_url = f"https://x.com/{ident}"
                err_msg = f"HTTP {status} - {readable_url}"
                daily_errors[ident] = {"ts": ts_err, "instance": "X API", "msg": err_msg}
                if status not in [403, 404]:
                    consecutive_errors += 1

            stats_output = [posts_val, followers_val]
            session_results.append((ident, row_data, stats_output))
            
            # Anti-ban delay between accounts
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            common.log_error(f"Exception at row {idx}: {e!s}", context=ctx)
            log_http_status(500, ident, context=ctx)
            ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
            daily_errors[ident] = {"ts": ts_err, "instance": "Local", "msg": f"Exception: {e!s}"}
            consecutive_errors += 1
            continue

    processed_count = len(session_results)
    if processed_count > 0:
        common.log_info(f"[INFO]  [SYNC] Preparing to overwrite data")
        
        all_rows = []
        for ident, cache_row_data, stats in session_results:
            if stats:
                all_rows.append(cache_row_data + stats)
        
        if all_rows:
            try:
                common.sheet_user_on_x.batch_clear(["A2:H1000"])
                common.sheet_user_on_x.update(values=all_rows, range_name=f"A2:H{1+len(all_rows)}", value_input_option="RAW")
                common.log_info(f"[INFO]  [SYNC] Overwrote {len(all_rows)} rows successfully")
            except Exception as e:
                common.log_info(f"[ERROR] [SYNC] Sheet write error: {e!s}")
                raise
                
    try:
        import gspread
        sh = common.client.open_by_key(common.SPREADSHEET_ID)
        try:
            error_sheet = sh.worksheet("error.log")
        except gspread.exceptions.WorksheetNotFound:
            error_sheet = sh.add_worksheet(title="error.log", rows=1000, cols=4)
        
        existing_rows = error_sheet.get_all_values()
        header = ["Timestamp", "Username", "Instance", "Error Message"]
        merged_dict = {}
        for r in existing_rows[1:]:
            if len(r) >= 4:
                merged_dict[r[1]] = {"ts": r[0], "instance": r[2], "msg": r[3]}
                
        for u, err in daily_errors.items():
            if err is None:
                merged_dict.pop(u, None)
            else:
                merged_dict[u] = err
            
        error_sheet.clear()
        if merged_dict:
            error_rows = [header]
            for u, err in merged_dict.items():
                error_rows.append([err["ts"], u, err["instance"], err["msg"]])
            
            error_sheet.update(values=error_rows, range_name="A1", value_input_option="RAW")
            common.log_info(f"[INFO]  [SYNC] Updated error.log with {len(merged_dict)} unique errors.")
    except Exception as e:
        common.log_info(f"[ERROR] [SYNC] Failed to update error.log sheet: {e!s}")

    total_min = (time.perf_counter() - overall_start) / 60.0
    common.log_info(f"[INFO]  [APP] Execution Summary: Processed {processed_count}/{total_accounts} | Sheet: '{common.SHEET_NAME_USER_ON_X}' | Duration: {total_min:.2f}m")

if __name__ == "__main__":
    get_twitter_user_stats()
