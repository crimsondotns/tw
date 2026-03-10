import os
import re
import gspread
from google.oauth2.service_account import Credentials
import requests
import json
import time
import math
import random
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Dict, List, Any, Set
import xml.etree.ElementTree as ET

# ===============================
# SERVICE ACCOUNT JSON
# ===============================
def load_credentials():
    # Attempt to load from service-account.json first
    if os.path.exists("service-account.json"):
        with open("service-account.json", "r") as f:
            return json.load(f)
    # Fallback to environment variable
    env_creds = os.getenv("SERVICE_ACCOUNT")
    if env_creds:
        return json.loads(env_creds)
    raise ValueError("Missing credentials: 'service-account.json' not found and 'SERVICE_ACCOUNT' env var is empty.")

SERVICE_ACCOUNT_INFO = load_credentials()

# ===============================
# GOOGLE SHEETS AUTH
# ===============================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1xKU6PB6PaPBmq6wHkW6cEM3PDDu6LWzZjlnHJp0Mvqo"
SHEET_NAME_STATUS = "Twitter(X) User Stat"
SHEET_NAME_MIGRATION = "Migration"

sheet_status = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_STATUS)
sheet_migration = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_MIGRATION)

# ===============================
# X API CONFIG
# ===============================
BEARER_TOKEN = os.getenv("X_BEARER", "")

ENDPOINT_TAG = "UserByScreenName"

# Optional user cookie sources
X_COOKIE_STRING = os.getenv("X_COOKIE_STRING", "")
X_AUTH_TOKEN = os.getenv("X_AUTH_TOKEN", "")
X_CT0 = os.getenv("X_CT0", "")

PREFER_USER_AUTH_FOR_COMMUNITY = True

# ===============================
# TELEGRAM NOTIFICATION
# ===============================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram_notification(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or "YOUR_" in TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            log_info(f"Telegram notification failed: status={resp.status_code}")
    except Exception as e:
        log_info(f"Telegram notification error: {e!s}")

# ===============================
# LOGGING UTIL
# ===============================
SGT = timezone(timedelta(hours=7))


def now_sgt_str():
    # Example format: [10/Mar/2026:07:24:03 +0000]
    t = datetime.now(timezone.utc)
    return t.strftime("[%d/%b/%Y:%H:%M:%S +0000]")


def emoji_for_status(status_code):
    if status_code == 200:
        return "✅"
    if status_code == 429:
        return "⚠️"
    if 200 <= status_code < 300:
        return "✅"
    return "❌"


def log_info(msg, row_idx=None, status_code=None, method=None, path=None):
    ts = now_sgt_str()
    if method and path:
        row_str = f" ROW{row_idx}" if row_idx is not None else ""
        st = f" {status_code}" if status_code else ""
        print(f"{ts} \"{method}{row_str} {path}\"{st} {msg}", flush=True)
    else:
        row_str = f"Row {row_idx} " if row_idx is not None else ""
        print(f"{ts} {row_str}{msg}", flush=True)


log_info("Script starting... 🚀")


# ===============================
# SESSION + AUTH HELPERS
# ===============================
session = requests.Session()
session.headers.update({
    "Authorization": BEARER_TOKEN,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9,th;q=0.8",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "x-twitter-client-language": "en"
})


def parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    jar: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            jar[k.strip()] = v.strip()
    return jar


def enable_user_auth_on_session() -> bool:
    cookie_map: Dict[str, str] = {}

    if X_COOKIE_STRING:
        cookie_map.update(parse_cookie_string(X_COOKIE_STRING))
    if X_AUTH_TOKEN:
        cookie_map["auth_token"] = X_AUTH_TOKEN
    if X_CT0:
        cookie_map["ct0"] = X_CT0

    if not cookie_map.get("auth_token") or not cookie_map.get("ct0"):
        return False

    for k, v in cookie_map.items():
        session.cookies.set(k, v, domain=".x.com")

    session.headers["x-csrf-token"] = cookie_map["ct0"]
    session.headers["x-twitter-auth-type"] = "OAuth2Session"
    session.headers["x-twitter-active-user"] = "yes"
    log_info("enabled user auth headers ✅")
    return True


def have_user_auth() -> bool:
    return ("auth_token" in session.cookies and "ct0" in session.cookies) or \
           (X_AUTH_TOKEN and X_CT0) or bool(X_COOKIE_STRING)


def refresh_guest_token() -> str:
    url = "https://api.x.com/1.1/guest/activate.json"
    hdrs = {
        "Authorization": BEARER_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0")
    }
    resp = requests.post(url, headers=hdrs, timeout=20)
    if resp.status_code == 200:
        token = resp.json().get("guest_token")
        if token:
            session.headers["x-guest-token"] = token
            log_info("acquired x-guest-token ✅")
            return token
    log_info(f"guest token activation failed: status={resp.status_code} ❌")
    resp.raise_for_status()
    return ""


# ===============================
# BACKOFF WRAPPER
# ===============================
def call_x_with_backoff(url, row_idx=None, max_retries=8, base_sleep=2.0, timeout=20):
    attempt = 0
    guest_refreshed = False
    while True:
        attempt += 1
        start = time.perf_counter()
        try:
            resp = session.get(url, timeout=timeout)
            dur_ms = int((time.perf_counter() - start) * 1000)
            status = resp.status_code
            log_info(f"{dur_ms}ms, status={status} {emoji_for_status(status)}", row_idx=row_idx)

            if status == 200:
                return resp

            if status == 403 and have_user_auth():
                if "x-csrf-token" not in session.headers:
                    if enable_user_auth_on_session():
                        continue
                send_telegram_notification(f"<b>403 Forbidden</b> (X API)\nURL: {url}\nRow: {row_idx}")
                return resp

            if status == 403 and not have_user_auth() and not guest_refreshed:
                try:
                    refresh_guest_token()
                    guest_refreshed = True
                    continue
                except Exception as e:
                    log_info(f"guest token refresh error: {e!s} ❌", row_idx=row_idx)
                    return resp

            if status == 429:
                retry_after = resp.headers.get("Retry-After")
                reset_epoch = resp.headers.get("x-rate-limit-reset")
                sleep_s: Optional[float] = None
                if reset_epoch:
                    try:
                        reset_ts = int(reset_epoch)
                        now_utc = datetime.now(timezone.utc).timestamp()
                        sleep_s = max(0, math.ceil(reset_ts - now_utc) + 1)
                    except Exception:
                        pass
                if sleep_s is None and retry_after:
                    try:
                        sleep_s = max(1, int(float(retry_after)))
                    except Exception:
                        pass
                if sleep_s is None:
                    sleep_s = min(60 * 5, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
                log_info(f"waiting {sleep_s:.0f}s for rate limit window…", row_idx=row_idx)
                time.sleep(sleep_s)
                if attempt > max_retries:
                    raise RuntimeError("Too many rate limit retries.")
                continue

            if attempt <= max_retries and (500 <= status < 600 or status in (408, 409, 425, 502, 503, 504)):
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                log_info(f"transient error {status}, retrying in {sleep_s:.1f}s…", row_idx=row_idx)
                time.sleep(sleep_s)
                continue

            return resp

        except requests.RequestException as e:
            dur_ms = int((time.perf_counter() - start) * 1000)
            log_info(f"{dur_ms}ms, network error: {e!s} ❌", row_idx=row_idx)
            if attempt <= max_retries:
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                log_info(f"retrying in {sleep_s:.1f}s…", row_idx=row_idx)
                time.sleep(sleep_s)
                continue
            raise


# ===============================
# URL → identifier
# ===============================
RE_COMMUNITY_ID_AND_TAIL = re.compile(
    r"https?://(?:x\.com|twitter\.com)/i/communities/\d+/([^/?#]+)",
    re.IGNORECASE
)
RE_COMMUNITY_ID_ONLY = re.compile(
    r"https?://(?:x\.com|twitter\.com)/i/communities/(\d+)(?:/|$)",
    re.IGNORECASE
)
RE_USERNAME_ROOT = re.compile(
    r"https?://(?:x\.com|twitter\.com)/([^/?#]+)",
    re.IGNORECASE
)


def extract_identifier_from_link(link: str) -> str:
    """
    จากลิงก์ X:
      - community link → ดึง tail / id
      - user profile link → ดึง username
      - ถ้าใส่เป็น @username ตรง ๆ ก็ให้ใช้ไปเลย
    """
    if not link:
        return ""
    link = link.strip()

    if link.startswith("@") and " " not in link:
        return link.lstrip("@")

    m = RE_COMMUNITY_ID_AND_TAIL.search(link)
    if m:
        return m.group(1)

    m = RE_COMMUNITY_ID_ONLY.search(link)
    if m:
        return m.group(1)

    m = RE_USERNAME_ROOT.search(link)
    if m:
        return m.group(1)

    return link


def is_rest_id(s: str) -> bool:
    """
    เช็คว่าเป็นเลขยาว ๆ แบบ userId / rest_id (16–20 หลัก) หรือไม่
    """
    s = (s or "").strip()
    return s.isdigit() and 16 <= len(s) <= 20


# ===============================
# Community helpers (member_count)
# ===============================
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

    global ENDPOINT_TAG
    old_tag = ENDPOINT_TAG
    ENDPOINT_TAG = "CommunityQuery"
    try:
        if PREFER_USER_AUTH_FOR_COMMUNITY and have_user_auth():
            enable_user_auth_on_session()
        else:
            if "x-guest-token" not in session.headers:
                try:
                    refresh_guest_token()
                except Exception as e:
                    log_info(f"initial guest token error: {e!s} ❌", row_idx=row_idx)

        resp = call_x_with_backoff(url, row_idx=row_idx)
        status = resp.status_code

        if status == 403 and have_user_auth():
            if enable_user_auth_on_session():
                resp = session.get(url, timeout=20)
                status = resp.status_code
                log_info(f"[retry user] status={status} {emoji_for_status(status)}", row_idx=row_idx)

        if status == 200:
            data = resp.json()
            member_count = (data.get("data", {}).get("community", {}).get("member_count"))
            if not isinstance(member_count, int):
                member_count = deep_find_member_count(data) or -1
            return status, member_count
        else:
            return status, -1
    finally:
        ENDPOINT_TAG = old_tag


# ===============================
# Nitter RSS Helper
# ===============================
def fetch_nitter_rss_posts(username: str, days: int = 7, row_idx: Optional[int] = None) -> List[Tuple[datetime, str]]:
    """
    ดึงโพสต์จาก Nitter RSS (nitter.net/{username}/rss)
    """
    url = f"https://nitter.net/{username}/rss"
    path = f"/nitter.net/{username}/rss"
    
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    
    try:
        resp = session.get(url, timeout=30)
        status = resp.status_code
        
        # Colorize status code
        if status == 200:
            colored_status = f"\033[92m{status}\033[0m"
        elif status == 429:
            colored_status = f"\033[93m{status}\033[0m"
        else:
            colored_status = f"\033[91m{status}\033[0m"
        
        log_info("", row_idx=row_idx, status_code=colored_status, method="GET", path=path)
        
        if status != 200:
            if status == 403:
                send_telegram_notification(f"<b>403 Forbidden</b> (Nitter RSS)\nURL: {url}\nRow: {row_idx}")
            return []
            
        root = ET.fromstring(resp.content)
        channel = root.find("channel")
        if channel is None:
            return []
            
        posts: List[Tuple[datetime, str]] = []
        for item in channel.findall("item"):
            title = item.find("title")
            pub_date = item.find("pubDate")
            
            if title is not None and pub_date is not None:
                content = title.text or ""
                # Nitter RSS pubDate format: "Sat, 08 Mar 2025 14:14:48 GMT"
                try:
                    dt = datetime.strptime(pub_date.text, "%a, %d %b %Y %H:%M:%S %Z")
                    dt = dt.replace(tzinfo=timezone.utc)
                    
                    if dt >= cutoff_dt:
                        posts.append((dt, content))
                except Exception as e:
                    log_info(f"Date parse error: {e!s}", row_idx=row_idx)
                    
        # Sort by date descending
        posts.sort(key=lambda x: x[0], reverse=True)
        return posts
        
    except Exception as e:
        log_info(f"Nitter RSS error: {e!s} ❌", row_idx=row_idx)
        return []


# ===============================
# UserTweets helpers (Removed legacy GraphQL functions)
# ===============================


# ===============================
# MAIN 1: ดึง stats → Migration (B:C:D)
# ===============================
def get_twitter_user_stats():
    overall_start = time.perf_counter()

    log_info("Fetching account links from spreadsheet...")
    links = sheet_status.col_values(1)[1:]
    log_info(f"Found {len(links)} links. Starting processing...")
    results: List[List[str]] = []

    for idx, link in enumerate(links, start=2):
        ident = extract_identifier_from_link(link or "")
        if not ident:
            results.append(["", "", ""])
            continue

        # ถ้าเป็นเลขยาว → communityId
        if is_rest_id(ident):
            try:
                status, member_count = fetch_community_member_count(ident, row_idx=idx)
                if status == 200 and member_count >= 0:
                    # B = communityId, C = "", D = member_count
                    results.append([ident, "", str(member_count)])
                elif status == 429:
                    results.append([ident, "", "rate_limited"])
                else:
                    results.append([ident, "", f"status={status}"])
            except Exception as e:
                log_info(f"Error (Community) at row {idx}: {e!s}", row_idx=idx)
                results.append([ident, "", "ERROR"])
            continue

        # ไม่ใช่เลขยาว → screen_name
        screen_name = ident.lstrip("@")
        variables = {
            "screen_name": screen_name,
            "withGrokTranslatedBio": False
        }
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

        global ENDPOINT_TAG
        old_tag = ENDPOINT_TAG
        ENDPOINT_TAG = screen_name
        try:
            resp = call_x_with_backoff(url, row_idx=idx)
            status = resp.status_code
            if status == 200:
                data = resp.json()
                legacy = (data.get("data", {}).get("user", {}).get("result", {}).get("legacy"))
                if legacy:
                    # B = screen_name, C = statuses_count, D = followers_count
                    results.append([
                        screen_name,
                        str(legacy.get("statuses_count", "")),
                        str(legacy.get("followers_count", ""))
                    ])
                else:
                    results.append([screen_name, "Account suspended", "Account suspended"])
            elif status == 429:
                results.append([screen_name, "", "rate_limited"])
            else:
                results.append([screen_name, f"status={status}", f"status={status}"])
        except Exception as e:
            log_info(f"Error (UserByScreenName) at row {idx}: {e!s}", row_idx=idx)
            results.append([screen_name, "ERROR", "ERROR"])
        finally:
            ENDPOINT_TAG = old_tag

    end_row = 1 + len(links)
    range_bcd = f"B2:D{end_row}"

    try:
        sheet_status.batch_clear([range_bcd])
        log_info(f"cleared range {range_bcd} ✅")
        sheet_status.update(values=results, range_name=range_bcd, value_input_option="RAW")
        log_info(f"wrote {len(results)} rows to {range_bcd} ✅")
    except Exception as e:
        log_info(f"sheet write error: {e!s} ❌")
        raise

    total_min = (time.perf_counter() - overall_start) / 60.0
    log_info(f"✅ เสร็จสิ้นการรันสคริปต์ (stats) ใช้เวลา {total_min:.1f} นาที ✅")


# ===============================
# MAIN 2: ดึงโพสต์ย้อนหลัง N วัน → Migration (E:...)
# ===============================
def get_twitter_user_recent_posts(days: int = 7):
    overall_start = time.perf_counter()

    links = sheet_migration.col_values(1)[1:]
    total_accounts = len(links)

    log_info(f"เริ่มรัน get_twitter_user_recent_posts(days={days}): total_rows={total_accounts}")

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
        ident_raw = extract_identifier_from_link(link or "")
        if not ident_raw:
            accounts_empty_link += 1
            log_info(f"row={idx} link ว่าง / parse ไม่ได้ → ข้าม", row_idx=idx)
            all_rows.append([""])
            continue

        username = ident_raw.lstrip("@")
        if is_rest_id(username):
            # Nitter doesn't support userId (rest_id) in RSS as easily as screen_name
            # For now, we skip or attempt to use it as is
            log_info(f"skipping rest_id ident '{username}' — RSS needs screen_name", row_idx=idx)
            all_rows.append(["RSS_REQUIRES_SCREEN_NAME"])
            continue

        try:
            global ENDPOINT_TAG
            old_tag = ENDPOINT_TAG
            ENDPOINT_TAG = f"NitterRSS:{username}"
            
            tweets = fetch_nitter_rss_posts(username, days=days, row_idx=idx)
            ENDPOINT_TAG = old_tag
            
            texts = [t[1] for t in tweets]
            tweet_count = len(texts)
            total_tweets_nd += tweet_count

            if tweet_count == 0:
                accounts_zero_tweets_nd += 1
            else:
                accounts_with_tweets_nd += 1
                # newest_dt = tweets[0][0].astimezone(SGT)
                # oldest_dt = tweets[-1][0].astimezone(SGT)

            row = texts if texts else [""]
            all_rows.append(row)
            max_tweets = max(max_tweets, len(row))

        except Exception as e:
            accounts_tweets_api_err += 1
            log_info(f"Error (NitterRSS): {e!s} ❌", row_idx=idx)
            all_rows.append(["ERROR_NITTER_RSS"])
            continue

    target_len = max_tweets if max_tweets > 0 else 1
    normalized_rows: List[List[str]] = []
    for row in all_rows:
        padded = row + [""] * (target_len - len(row))
        normalized_rows.append(padded)

    end_row = 1 + len(links)
    clear_range = f"E2:ZZ{end_row}"   # เคลียร์เฉพาะคอลัมน์ E เป็นต้นไป ไม่ทับ B:C:D

    try:
        sheet_migration.batch_clear([clear_range])
        log_info(f"cleared range {clear_range} ✅")

        sheet_migration.update(
            values=normalized_rows,
            range_name="E2",          # เริ่มเขียนที่คอลัมน์ E
            value_input_option="RAW"
        )
        log_info(f"wrote {len(normalized_rows)} rows starting at E2 ✅")
    except Exception as e:
        log_info(f"sheet write error: {e!s} ❌")
        raise

    total_min = (time.perf_counter() - overall_start) / 60.0

    log_info("===== SUMMARY (RECENT POSTS) ===")
    log_info(f"accounts_total={total_accounts}")
    log_info(f"accounts_empty_link={accounts_empty_link}")
    log_info(f"accounts_no_user_id={accounts_no_user_id}")
    log_info(f"accounts_user_lookup_err={accounts_user_lookup_err}")
    log_info(f"accounts_tweets_api_err={accounts_tweets_api_err}")
    log_info(f"accounts_with_tweets_{days}d={accounts_with_tweets_nd}")
    log_info(f"accounts_zero_tweets_{days}d={accounts_zero_tweets_nd}")
    log_info(f"total_tweets_{days}d={total_tweets_nd}")
    log_info(f"ใช้เวลารวม {total_min:.2f} นาที")
    log_info("===== END SUMMARY ===")

    log_info("✅ เสร็จสิ้นการรันสคริปต์ (recent posts) ✅")


# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    # เขียน stats ลง Migration (B:C:D)
    get_twitter_user_stats()

    # เขียนโพสต์ย้อนหลัง 30 วันลง Migration (E:...)
    # get_twitter_user_recent_posts(30)
