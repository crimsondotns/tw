import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import re
import os
import json
import random
import common

NITTER_INSTANCES = [
    "nitter.net"
]

def get_nitter_instances() -> List[str]:
    try:
        resp = common.session.get("https://status.d420.de/api/v1/instances", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            valid = []
            for inst in data:
                if inst.get("rss") and inst.get("is_up"):
                    url = inst.get("url", "")
                    domain = url.replace("https://", "").replace("http://", "").rstrip("/")
                    if domain:
                        valid.append(domain)
            if len(valid) >= 3:
                return valid
    except Exception as e:
        common.log_info(f"Failed to fetch dynamic nitter instances: {e!s}")
    return NITTER_INSTANCES

def fetch_nitter_rss_posts(username: str, days: int = 7, row_idx: Optional[int] = None, instance: str = "nitter.net") -> Tuple[int, List[Tuple[datetime, str]]]:
    """
    ดึงโพสต์จาก Nitter RSS
    """
    url = f"https://{instance}/{username}/rss"
    path = f"/{instance}/{username}/rss"
    
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    
    try:
        resp = common.session.get(url, timeout=30)
        status = resp.status_code
        
        # Colorize status code
        if status == 200:
            colored_status = f"\033[92m{status}\033[0m"
        elif status == 429:
            colored_status = f"\033[93m{status}\033[0m"
        else:
            colored_status = f"\033[91m{status}\033[0m"
        
        common.log_info("", row_idx=row_idx, status_code=colored_status, method="GET", path=path)
        
        if status != 200:
            readable_url = f"https://x.com/{username}"
            if status == 403:
                common.send_telegram_notification(f"<b>403 Forbidden</b> (Nitter RSS)\nURL: {readable_url}\nRow: {row_idx}\nInstance: {instance}")
            elif status == 404:
                common.send_telegram_notification(f"<b>404 Not Found</b> (Nitter RSS)\nURL: {readable_url}\nRow: {row_idx}\nInstance: {instance}")
            return status, []
            
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return status, []
            
        posts: List[Tuple[datetime, str]] = []
        for item in channel.findall("item"):
            title = item.find("title")
            pub_date = item.find("pubDate")
            
            if title is not None and pub_date is not None:
                content = title.text or ""
                if content == "Image":
                    desc = item.find("description")
                    if desc is not None:
                        desc_text = desc.text
                        if desc_text:
                            m = re.search(r'src="([^"]+)"', desc_text)
                            if m:
                                content = m.group(1).replace("&amp;", "&")
                
                try:
                    dt = datetime.strptime(pub_date.text, "%a, %d %b %Y %H:%M:%S %Z")
                    dt = dt.replace(tzinfo=timezone.utc)
                    
                    if dt >= cutoff_dt:
                        full_content = f"{content}\n\n{pub_date.text}"
                        posts.append((dt, full_content))
                except Exception as e:
                    common.log_info(f"Date parse error: {e!s}", row_idx=row_idx)
                    
        posts.sort(key=lambda x: x[0], reverse=True)
        return status, posts
        
    except Exception as e:
        common.log_info(f"Nitter RSS error: {e!s} ❌ ({instance})", row_idx=row_idx)
        return 500, []

def save_progress_state(state_file: str, date_str: str, last_row: int, finished: bool):
    try:
        new_state = {
            "date": date_str,
            "last_row_processed": last_row,
            "finished": finished
        }
        with open(state_file, "w") as f:
            json.dump(new_state, f)
        common.log_info(f"Saved state to {state_file}: {new_state} ✅")
    except Exception as e:
        common.log_info(f"Error saving state: {e!s}")

def get_twitter_user_recent_posts(days: int = 7):
    overall_start = time.perf_counter()

    STATE_FILE = "nitter_progress.json"
    state = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_index = 2 # Sheet row 2
    
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            if state.get("date") == today_str and not state.get("finished", False):
                start_index = int(state.get("last_row_processed", 1)) + 1
                common.log_info(f"Resume run detected for {today_str}. Starting from sheet row {start_index} 🚀")
        except Exception as e:
            common.log_info(f"Error loading state: {e!s}")

    links = common.sheet_migration.col_values(1)
    total_accounts = len(links) - 1 # exclude header

    common.log_info(f"เริ่มรัน get_twitter_user_recent_posts(days={days}): total_rows_in_sheet={total_accounts}")

    instances = get_nitter_instances()
    common.log_info(f"Nitter instances ready: {len(instances)} instances.")

    all_rows: List[List[str]] = []
    max_tweets = 0

    accounts_empty_link = 0
    accounts_tweets_api_err = 0
    total_tweets_nd = 0

    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5

    # If fresh start, clear sheet
    if start_index == 2:
        end_row = len(links) + 1 # Clear up to the length of items
        clear_range = f"E2:ZZ{end_row}"
        try:
            common.sheet_migration.batch_clear([clear_range])
            common.log_info(f"cleared range {clear_range} ✅ (Fresh Start)")
        except Exception as e:
            common.log_info(f"Error clearing range: {e!s}")
    else:
        # Prevent starting beyond the end
        if start_index > len(links):
            common.log_info("All rows already processed. Exiting.")
            return

    # Process chunks from start_index
    for idx_zero_based in range(start_index - 1, len(links)):
        idx = idx_zero_based + 1 # sheet row
        link = links[idx_zero_based]
        
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            common.log_info(f"หยุดการทำงานฉุกเฉิน เพราะ Error/Timeout ติดต่อกัน {MAX_CONSECUTIVE_ERRORS} ครั้ง 🛑")
            break

        ident_raw = common.extract_identifier_from_link(link or "")
        if not ident_raw:
            accounts_empty_link += 1
            common.log_info(f"row={idx} link ว่าง / parse ไม่ได้ → ข้าม", row_idx=idx)
            all_rows.append([""])
            continue

        username = ident_raw.lstrip("@")
        if common.is_rest_id(username):
            common.log_info(f"skipping rest_id ident '{username}' — RSS needs screen_name", row_idx=idx)
            all_rows.append(["RSS_REQUIRES_SCREEN_NAME"])
            continue

        try:
            old_tag = common.ENDPOINT_TAG
            tweets = []
            success = False
            
            # Try up to 3 different instances
            attempts = 3
            for attempt in range(1, attempts + 1):
                instance = random.choice(instances)
                status_nitter, tweets = fetch_nitter_rss_posts(username, days=days, row_idx=idx, instance=instance)
                
                if status_nitter == 200:
                    success = True
                    consecutive_errors = 0
                    break
                elif status_nitter in [429, 500, 502, 503, 504]:
                    if attempt < attempts:
                        common.log_info(f"Instance {instance} failed ({status_nitter}), retrying... (Attempt {attempt}/{attempts})", row_idx=idx)
                        time.sleep(2)
                else:
                    # 404 or 403, unlikely to be solved by rotation
                    consecutive_errors = 0
                    break
            
            if not success and status_nitter not in [200, 404, 403]:
                consecutive_errors += 1
            
            common.ENDPOINT_TAG = old_tag
            
            texts = [t[1] for t in tweets]
            tweet_count = len(texts)
            total_tweets_nd += tweet_count

            row = texts if texts else [""]
            all_rows.append(row)
            max_tweets = max(max_tweets, len(row))

            time.sleep(1.5) # Anti-rate limit delay

        except Exception as e:
            accounts_tweets_api_err += 1
            common.log_info(f"Error (NitterRSS): {e!s} ❌", row_idx=idx)
            all_rows.append(["ERROR_NITTER_RSS"])
            consecutive_errors += 1
            continue

    # Writing Phase (Only for exactly what was processed in this run)
    processed_count = len(all_rows)
    if processed_count > 0:
        target_len = max_tweets if max_tweets > 0 else 1
        normalized_rows: List[List[str]] = []
        for row in all_rows:
            padded = row + [""] * (target_len - len(row))
            normalized_rows.append(padded)

        write_range = f"E{start_index}"
        try:
            common.sheet_migration.update(
                values=normalized_rows,
                range_name=write_range,
                value_input_option="RAW"
            )
            common.log_info(f"wrote {processed_count} rows starting at {write_range} ✅")
        except Exception as e:
            common.log_info(f"sheet write error: {e!s} ❌")
            raise

        last_processed = start_index + processed_count - 1
        # It's finished if we processed the very last item in the list and didn't crash
        # If we exited due to MAX_CONSECUTIVE_ERRORS, consecutive_errors will be >= max
        is_finished = (last_processed >= len(links)) and (consecutive_errors < MAX_CONSECUTIVE_ERRORS)
        
        save_progress_state(STATE_FILE, today_str, last_processed, is_finished)
    else:
        common.log_info("No rows processed in this run.")

    total_min = (time.perf_counter() - overall_start) / 60.0
    common.log_info("===== SUMMARY (RECENT POSTS) ===")
    common.log_info(f"accounts_total={total_accounts}")
    common.log_info(f"processed_this_run={processed_count}")
    common.log_info(f"total_tweets_fetched_this_run={total_tweets_nd}")
    common.log_info(f"time_spent={total_min:.2f} นาที")
    common.log_info("===== END SUMMARY ===")

if __name__ == "__main__":
    get_twitter_user_recent_posts(30)
