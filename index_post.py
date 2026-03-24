import time
import requests
import json
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict
import xml.etree.ElementTree as ET
import re
import common

def get_recent_tweets_nitter(username: str, nitter_instances: List[str], days: int = 30) -> Tuple[int, List[Tuple[str, str]]]:
    if not nitter_instances:
        nitter_instances = ["nitter.net"]
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    tweets = []
    
    random.shuffle(nitter_instances)
    success = False
    status_code = 500
    
    for instance in nitter_instances:
        url = f"https://{instance}/{username}/rss"
        try:
            resp = common.session.get(url, timeout=15)
            status_code = resp.status_code
            if status_code == 200:
                root = ET.fromstring(resp.content)
                channel = root.find("channel")
                if channel is None:
                    continue
                    
                for item in channel.findall("item"):
                    title = item.find("title")
                    pub_date = item.find("pubDate")
                    
                    if title is not None and pub_date is not None:
                        content = title.text or ""
                        if content == "Image":
                            desc = item.find("description")
                            if desc is not None and desc.text:
                                m = re.search(r'src="([^"]+)"', desc.text)
                                if m:
                                    content = m.group(1).replace("&amp;", "&")
                        
                        try:
                            dt = datetime.strptime(pub_date.text, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                            if dt >= cutoff_date:
                                full_content = f"{content}\n\n{pub_date.text}"
                                tweets.append((dt.strftime("%Y-%m-%d"), full_content))
                        except Exception:
                            continue
                
                tweets.sort(key=lambda x: x[0], reverse=True)
                success = True
                break
            elif status_code in [403, 404]:
                break
        except Exception:
            continue
            
    if not success:
        return status_code, []
        
    return 200, tweets

# Redirect old print_custom_log to new logger if needed, but better to call directly
def log_http_status(status: int, username: str, context: str = ""):
    if status == 200:
        common.log_success(f"HTTP 200 | Successfully fetched posts", context=context)
    elif status == 403:
        common.log_error(f"HTTP 403 | Forbidden access", context=context)
    elif status == 404:
        common.log_error(f"HTTP 404 | User not found", context=context)
    elif status == 429:
        common.log_warn(f"HTTP 429 | Rate limit exceeded", context=context)
    else:
        common.log_error(f"HTTP {status} | Error fetching posts", context=context)

def get_twitter_user_recent_posts(days: int = 30):
    overall_start = time.perf_counter()

    daily_errors: Dict[str, dict] = {}

    raw_engagement_data = common.sheet_engagement.get("B4:G")
    engagement_rows = []
    for f_idx, row in enumerate(raw_engagement_data, start=4):
        padded_row = row + [""] * (6 - len(row))
        engagement_rows.append((f_idx, padded_row))

    total_accounts = len(engagement_rows)
    common.log_info(f"[INFO]  [APP] Initializing get_twitter_user_recent_posts (Target: {total_accounts} accounts)")

    working_instances = ["nitter.net"]
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

        ctx = f"Row {idx} | @{username}"
        try:
            old_tag = common.ENDPOINT_TAG
            common.ENDPOINT_TAG = "nitter_rss"
            
            common.log_info(f"Fetching posts...", context=ctx)
            status_nitter, tweets = get_recent_tweets_nitter(username, working_instances, days=days)
            
            if status_nitter != 200:
                common.ENDPOINT_TAG = username
                common.log_warn(f"Nitter failed ({status_nitter}), checking X API existence...", context=ctx)
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

            log_http_status(status_nitter, username, context=ctx)
            
            if status_nitter == 200:
                consecutive_errors = 0
                daily_errors[username] = None 
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
            
            # Anti-ban delay between accounts
            time.sleep(random.uniform(1.5, 3.0))

        except Exception as e:
            common.log_error(f"Exception at row {idx}: {e!s}", context=ctx)
            log_http_status(500, username, context=ctx)
            
            ts_err = datetime.now(common.SGT).strftime("%Y-%m-%d %H:%M:%S")
            daily_errors[username] = {"ts": ts_err, "instance": "Local", "msg": f"Exception: {e!s}"}
            consecutive_errors += 1
            continue

    processed_count = len(session_results)
    if processed_count > 0:
        common.log_info(f"[INFO]  [SYNC] Preparing to overwrite data")
        
        all_rows = []
        max_tweets = 0
        for username, cache_row_data, texts in session_results:
            if not texts:
                # Based on user feedback: if no recent posts, skip writing this account.
                continue
                
            # Metadata (6 columns) + Tweets (starting at G)
            row_out = cache_row_data + texts
            all_rows.append(row_out)
            max_tweets = max(max_tweets, len(texts))
            total_tweets_fetched_this_run += len(texts)
        
        # Normalize row widths for gspread update
        full_width = 6 + max_tweets
        normalized_rows = []
        for row in all_rows:
            padding_needed = full_width - len(row)
            normalized_rows.append(row + [""] * padding_needed)

        if normalized_rows:
            try:
                # Clear columns A to ZZ to ensure a fresh dense output
                common.sheet_migration.batch_clear(["A2:ZZ500000"])
                
                chunk_size = 500
                total_chunks = (len(normalized_rows) + chunk_size - 1) // chunk_size
                
                for i in range(0, len(normalized_rows), chunk_size):
                    chunk = normalized_rows[i:i+chunk_size]
                    start_row = i + 2
                    end_row = start_row + len(chunk) - 1
                    # A to last column based on full_width
                    last_col_char = common.gspread.utils.rowcol_to_a1(1, full_width).rstrip("1")
                    range_name = f"A{start_row}:{last_col_char}{end_row}"
                    common.sheet_migration.update(values=chunk, range_name=range_name, value_input_option="RAW")
                    common.log_info(f"[INFO]  [SYNC] Written chunk {i//chunk_size + 1}/{total_chunks} ({len(chunk)} users) to '{common.SHEET_NAME_MIGRATION}'!{range_name}")
                
                common.log_info(f"[INFO]  [SYNC] Overwrote {len(normalized_rows)} rows successfully")
            except Exception as e:
                common.log_info(f"[ERROR] [SYNC] Sheet write error: {e!s}")
                raise

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
    common.log_info(f"[INFO]  [APP] Execution Summary: Processed {processed_count}/{total_accounts} | Sheet: '{common.SHEET_NAME_MIGRATION}' | Posts: {total_tweets_fetched_this_run} | Duration: {total_min:.2f}m")

if __name__ == "__main__":
    get_twitter_user_recent_posts()
