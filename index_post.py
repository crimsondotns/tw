import time
import requests
import json
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict
import xml.etree.ElementTree as ET
import common

def fetch_dynamic_nitter_instances(timeout=10) -> List[str]:
    url = "https://status.d420.de/api/v1/instances"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        working = []
        for inst in data.get("hosts", []):
            if inst.get("healthy") and inst.get("rss"):
                working.append(inst["url"])
        if working:
            return working
    except Exception as e:
        common.log_info(f"[ERROR] [NET] Failed to fetch dynamic nitter instances: {e!s}")
    return []

def get_recent_tweets_nitter(username: str, days: int = 30) -> Tuple[int, List[Tuple[str, str]]]:
    nitter_instances = fetch_dynamic_nitter_instances()
    if not nitter_instances:
        nitter_instances = [
            "https://nitter.net"
        ]
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    tweets = []
    
    random.shuffle(nitter_instances)
    success = False
    status_code = 500
    
    for instance in nitter_instances:
        url = f"{instance}/{username}/rss"
        try:
            resp = requests.get(url, timeout=12)
            status_code = resp.status_code
            if status_code == 200:
                root = ET.fromstring(resp.text)
                for item in root.findall(".//item"):
                    pubDate_str = item.findtext("pubDate")
                    desc = item.findtext("description")
                    if pubDate_str and desc:
                        try:
                            tweet_date = datetime.strptime(pubDate_str, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                        except ValueError:
                            tweet_date = datetime.now(timezone.utc)
                            
                        if tweet_date >= cutoff_date:
                            tweets.append((tweet_date.strftime("%Y-%m-%d"), desc))
                success = True
                break
        except Exception:
            continue
            
    if not success:
        return status_code, []
        
    return 200, tweets

def print_custom_log(status: int, username: str):
    if status == 200:
        common.log_info(f"[INFO]  Successfully fetched recent posts for @{username}")
    elif status == 403:
        common.log_info(f"[ERROR] [403] Forbidden access tracking @{username}")
    elif status == 404:
        common.log_info(f"[ERROR] [404] Verification failed tracking @{username}")
    elif status == 429:
        common.log_info(f"[ERROR] [429] Rate limit exceeded tracking @{username}")
    elif 500 <= status < 600:
        common.log_info(f"[ERROR] [{status}] Internal server error tracking @{username}")
    else:
        common.log_info(f"[ERROR] [{status}] Verification failed tracking @{username}")

def save_progress_state(state_file: str, date_str: str, processed_users: List[str], daily_errors: dict):
    try:
        new_state = {
            "date": date_str,
            "processed_users": processed_users,
            "errors": daily_errors
        }
        with open(state_file, "w") as f:
            json.dump(new_state, f)
        common.log_info(f"[INFO]  [STATE] Saved state to {state_file} ({len(processed_users)} users completed today)")
    except Exception as e:
        common.log_info(f"[ERROR] [STATE] Error saving state: {e!s}")

def get_twitter_user_recent_posts(days: int = 30):
    overall_start = time.perf_counter()

    STATE_FILE = "nitter_progress.json"
    state = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    processed_today: List[str] = []
    daily_errors: Dict[str, dict] = {}
    
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if state.get("date") == today_str:
                processed_today = list(state.get("processed_users", []))
                daily_errors = dict(state.get("errors", {}))
                common.log_info(f"[INFO]  [APP] Resume run detected for {today_str}. Skipping {len(processed_today)} users already processed.")
            else:
                common.log_info(f"[INFO]  [APP] New day detected ({today_str}). Starting fresh!")
        except Exception as e:
            common.log_info(f"[ERROR] [APP] Error loading state: {e!s}")

    raw_engagement_data = common.sheet_engagement.get("B4:G")
    engagement_rows = []
    for f_idx, row in enumerate(raw_engagement_data, start=4):
        padded_row = row + [""] * (6 - len(row))
        engagement_rows.append((f_idx, padded_row))

    total_accounts = len(engagement_rows)
    common.log_info(f"[INFO]  [APP] Initializing get_twitter_user_recent_posts (Target: {total_accounts} accounts)")

    working_instances = fetch_dynamic_nitter_instances()
    common.log_info(f"[INFO]  [NET] Nitter instances active: {len(working_instances)} servers")

    session_results: List[Tuple[str, List[str], List[str]]] = []

    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    total_tweets_fetched_this_run = 0

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

        if username.lower() in [u.lower() for u in processed_today]:
            common.log_info(f"[INFO]  [BYPASS] @{username} skipped (Already processed today)")
            continue

        try:
            old_tag = common.ENDPOINT_TAG
            common.ENDPOINT_TAG = "nitter_rss"
            
            status_nitter, tweets = get_recent_tweets_nitter(username, days=days)
            
            if status_nitter != 200:
                common.ENDPOINT_TAG = username
                variables = {"screen_name": username, "withGrokTranslatedBio": False}
                features = {"hidden_profile_subscriptions_enabled": True}
                fieldToggles = {"withAuxiliaryUserLabels": True}

                url = (
                    "https://api.x.com/graphql/ck5KkZ8t5cOmoLssopN99Q/UserByScreenName?"
                    f"variables={requests.utils.quote(json.dumps(variables))}"
                    f"&features={requests.utils.quote(json.dumps(features))}"
                    f"&fieldToggles={requests.utils.quote(json.dumps(fieldToggles))}"
                )
                try:
                    resp_x = common.call_x_with_backoff(url, row_idx=idx)
                    status_x = resp_x.status_code
                    if status_x == 200:
                        data = resp_x.json()
                        if data.get("data", {}).get("user", {}).get("result", {}).get("legacy"):
                            status_nitter = 429 
                        else:
                            status_nitter = 404
                    else:
                        status_nitter = status_x
                except Exception:
                    status_nitter = 500

            print_custom_log(status_nitter, username)
            
            if status_nitter == 200:
                consecutive_errors = 0
            else:
                if status_nitter not in [403, 404]:
                    consecutive_errors += 1
                
                ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
                readable_url = f"https://x.com/{username}"
                err_msg = f"HTTP {status_nitter} - {readable_url}"
                if status_nitter == 403: err_msg = f"403 Forbidden - {readable_url}"
                elif status_nitter == 404: err_msg = f"404 Not Found - {readable_url}"
                elif status_nitter == 429: err_msg = f"429 Rate Limit - {readable_url}"
                
                daily_errors[username] = {"ts": ts_err, "instance": "Nitter/X-API", "msg": err_msg}

            common.ENDPOINT_TAG = old_tag
            texts = [t[1] for t in tweets]
            total_tweets_fetched_this_run += len(texts)
            
            session_results.append((username, row_data, texts))

        except Exception as e:
            common.log_info(f"[ERROR] [APP] Exception at row {idx}: {e!s}")
            print_custom_log(500, username)
            
            ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
            daily_errors[username] = {"ts": ts_err, "instance": "Local", "msg": f"Exception: {e!s}"}
            consecutive_errors += 1
            continue

    processed_count = len(session_results)
    if processed_count > 0:
        common.log_info("[INFO]  [SYNC] Preparing to synchronize data to Migration sheet...")
        
        try:
            target_usernames = common.sheet_migration.col_values(5)
        except Exception:
            target_usernames = []
            
        target_map = {}
        max_row = 1
        for row_idx_0, u_raw in enumerate(target_usernames):
            if u_raw.strip():
                u = u_raw.strip().lower()
                target_map[u] = row_idx_0 + 1
                max_row = max(max_row, row_idx_0 + 1)
        
        next_empty_row = max_row + 1

        clear_ranges = []
        batch_updates = []
        successful_users_this_run = []
        
        for username, cache_row_data, texts in session_results:
            if not texts:
                common.log_info(f"[INFO]  [SYNC] Excluded @{username} (zero new activity)")
                successful_users_this_run.append(username)
                continue
                
            row_idx = target_map.get(username.lower())
            if not row_idx:
                row_idx = next_empty_row
                next_empty_row += 1
                
            clear_ranges.append(f"A{row_idx}:ZZ{row_idx}")
            row_out = cache_row_data + texts
            
            batch_updates.append({
                "range": f"A{row_idx}:ZZ{row_idx}",
                "values": [row_out]
            })
            successful_users_this_run.append(username)
        
        if clear_ranges:
            try:
                common.sheet_migration.batch_clear(clear_ranges)
                common.log_info(f"[INFO]  [SYNC] Cleared existing content for {len(clear_ranges)} rows.")
            except Exception as e:
                common.log_info(f"[ERROR] [SYNC] Sheet clear error: {e!s}")
        
        if batch_updates:
            try:
                common.sheet_migration.batch_update(batch_updates, value_input_option="RAW")
                common.log_info(f"[INFO]  [SYNC] Batch update committed: {len(batch_updates)} rows affected.")
            except Exception as e:
                common.log_info(f"[ERROR] [SYNC] Sheet write error: {e!s}")

    processed_today.extend(successful_users_this_run)
    save_progress_state(STATE_FILE, today_str, processed_today, daily_errors)
    
    try:
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
    common.log_info(f"[INFO]  [APP] Execution Summary: Processed {processed_count}/{total_accounts} | Posts: {total_tweets_fetched_this_run} | Duration: {total_min:.2f}m")

if __name__ == "__main__":
    get_twitter_user_recent_posts()
