import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import re
import common

def fetch_nitter_rss_posts(username: str, days: int = 7, row_idx: Optional[int] = None) -> Tuple[int, List[Tuple[datetime, str]]]:
    """
    ดึงโพสต์จาก Nitter RSS (nitter.net/{username}/rss)
    """
    url = f"https://nitter.net/{username}/rss"
    path = f"/nitter.net/{username}/rss"
    
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
                common.send_telegram_notification(f"<b>403 Forbidden</b> (Nitter RSS)\nURL: {readable_url}\nRow: {row_idx}")
            elif status == 404:
                common.send_telegram_notification(f"<b>404 Not Found</b> (Nitter RSS)\nURL: {readable_url}\nRow: {row_idx}")
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
                    if desc is not None and desc.text:
                        m = re.search(r'src="([^"]+)"', desc.text)
                        if m:
                            content = m.group(1).replace("&amp;", "&")
                
                # Nitter RSS pubDate format: "Sat, 08 Mar 2025 14:14:48 GMT"
                try:
                    dt = datetime.strptime(pub_date.text, "%a, %d %b %Y %H:%M:%S %Z")
                    dt = dt.replace(tzinfo=timezone.utc)
                    
                    if dt >= cutoff_dt:
                        full_content = f"{content}\n\n{pub_date.text}"
                        posts.append((dt, full_content))
                except Exception as e:
                    common.log_info(f"Date parse error: {e!s}", row_idx=row_idx)
                    
        # Sort by date descending
        posts.sort(key=lambda x: x[0], reverse=True)
        return status, posts
        
    except Exception as e:
        common.log_info(f"Nitter RSS error: {e!s} ❌", row_idx=row_idx)
        return 500, []

def get_twitter_user_recent_posts(days: int = 7):
    overall_start = time.perf_counter()

    links = common.sheet_migration.col_values(1)[1:]
    total_accounts = len(links)

    common.log_info(f"เริ่มรัน get_twitter_user_recent_posts(days={days}): total_rows={total_accounts}")

    all_rows: List[List[str]] = []
    max_tweets = 0

    accounts_empty_link = 0
    accounts_no_user_id = 0
    accounts_user_lookup_err = 0
    accounts_tweets_api_err = 0
    accounts_zero_tweets_nd = 0
    accounts_with_tweets_nd = 0
    total_tweets_nd = 0

    for idx, link in enumerate(links, start=2):
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
            for attempt in range(1, 8):
                status_nitter, tweets = fetch_nitter_rss_posts(username, days=days, row_idx=idx)
                if status_nitter == 200:
                    break
                if status_nitter == 429 and attempt < 7:
                    common.log_info(f"Nitter RSS 429 (Rate Limit), retrying in 10s... (Attempt {attempt}/7)", row_idx=idx)
                    time.sleep(10)
                else:
                    break
            
            common.ENDPOINT_TAG = old_tag
            
            texts = [t[1] for t in tweets]
            tweet_count = len(texts)
            total_tweets_nd += tweet_count

            if tweet_count == 0:
                accounts_zero_tweets_nd += 1
            else:
                accounts_with_tweets_nd += 1

            row = texts if texts else [""]
            all_rows.append(row)
            max_tweets = max(max_tweets, len(row))

        except Exception as e:
            accounts_tweets_api_err += 1
            common.log_info(f"Error (NitterRSS): {e!s} ❌", row_idx=idx)
            all_rows.append(["ERROR_NITTER_RSS"])
            continue

    target_len = max_tweets if max_tweets > 0 else 1
    normalized_rows: List[List[str]] = []
    for row in all_rows:
        padded = row + [""] * (target_len - len(row))
        normalized_rows.append(padded)

    end_row = 1 + len(links)
    clear_range = f"E2:ZZ{end_row}"

    try:
        common.sheet_migration.batch_clear([clear_range])
        common.log_info(f"cleared range {clear_range} ✅")

        common.sheet_migration.update(
            values=normalized_rows,
            range_name="E2",
            value_input_option="RAW"
        )
        common.log_info(f"wrote {len(normalized_rows)} rows starting at E2 ✅")
    except Exception as e:
        common.log_info(f"sheet write error: {e!s} ❌")
        raise

    total_min = (time.perf_counter() - overall_start) / 60.0

    common.log_info("===== SUMMARY (RECENT POSTS) ===")
    common.log_info(f"accounts_total={total_accounts}")
    common.log_info(f"accounts_empty_link={accounts_empty_link}")
    common.log_info(f"accounts_no_user_id={accounts_no_user_id}")
    common.log_info(f"accounts_user_lookup_err={accounts_user_lookup_err}")
    common.log_info(f"accounts_tweets_api_err={accounts_tweets_api_err}")
    common.log_info(f"accounts_with_tweets_{days}d={accounts_with_tweets_nd}")
    common.log_info(f"accounts_zero_tweets_{days}d={accounts_zero_tweets_nd}")
    common.log_info(f"total_tweets_{days}d={total_tweets_nd}")
    common.log_info(f"ใช้เวลารวม {total_min:.2f} นาที")
    common.log_info("===== END SUMMARY ===")

    common.log_info("✅ เสร็จสิ้นการรันสคริปต์ (recent posts) ✅")

if __name__ == "__main__":
    get_twitter_user_recent_posts(30)
