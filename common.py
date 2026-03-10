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
from typing import Optional, Tuple, Dict, List
import xml.etree.ElementTree as ET

# ===============================
# LOAD .ENV MANUALLY
# ===============================
def load_dotenv_manually():
    if os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    os.environ[key] = value

load_dotenv_manually()

# ===============================
# SERVICE ACCOUNT JSON
# ===============================
def load_credentials():
    if os.path.exists("service-account.json"):
        with open("service-account.json", "r") as f:
            return json.load(f)
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
SHEET_NAME_MIGRATION = "Copy of Migration"

sheet_status = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_STATUS)
sheet_migration = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_MIGRATION)

# ===============================
# X API CONFIG
# ===============================
BEARER_TOKEN = os.getenv("X_BEARER", "")
ENDPOINT_TAG = "UserByScreenName"
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
        row_str = f" Row: {row_idx}" if row_idx is not None else ""
        st = f" {status_code}" if status_code else ""
        print(f"{ts} \"{method}{row_str} {path}\"{st} {msg}", flush=True)
    else:
        row_str = f"Row {row_idx} " if row_idx is not None else ""
        print(f"{ts} {row_str}{msg}", flush=True)

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

    if cookie_map:
        for k, v in cookie_map.items():
            session.cookies.set(k, v, domain=".twitter.com")
            session.cookies.set(k, v, domain=".x.com")
        if "ct0" in cookie_map:
            session.headers["x-csrf-token"] = cookie_map["ct0"]
        return True
    return False

def setup_guest_token(token: str):
    session.headers["x-guest-token"] = token
    # Remove user auth features
    if "x-csrf-token" in session.headers:
        del session.headers["x-csrf-token"]
    session.cookies.clear()

def have_user_auth() -> bool:
    return bool(X_AUTH_TOKEN or X_CT0 or X_COOKIE_STRING)

def refresh_guest_token():
    log_info("Refreshing guest token from X API...")
    url = "https://api.twitter.com/1.1/guest/activate.json"
    headers = {"Authorization": BEARER_TOKEN}
    resp = requests.post(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    new_token = data.get("guest_token")
    if not new_token:
        raise ValueError("No guest_token returned from X API.")
    os.environ["X_GUEST_TOKEN"] = new_token
    setup_guest_token(new_token)
    log_info(f"New guest token: {new_token}")

X_GUEST_TOKEN = os.getenv("X_GUEST_TOKEN", "")
if have_user_auth():
    enable_user_auth_on_session()
elif X_GUEST_TOKEN:
    setup_guest_token(X_GUEST_TOKEN)
else:
    refresh_guest_token()

# ===============================
# X API HELPER (call_x_with_backoff)
# ===============================
def call_x_with_backoff(url: str, params=None, max_retries=5, base_sleep=2.0, row_idx=None):
    for attempt in range(1, max_retries + 2):
        try:
            resp = session.get(url, params=params, timeout=10)
            status = resp.status_code

            method = "GET"
            path = ENDPOINT_TAG
            emj = emoji_for_status(status)
            log_info(f"{resp.elapsed.total_seconds()*1000:.0f}ms, status={status} {emj}", row_idx=row_idx, status_code=status, method=method, path=path)

            if status == 200:
                return resp

            if status == 400:
                return resp

            if status == 401 or status == 403:
                if status == 403 and have_user_auth():
                    readable_url = get_readable_url(url)
                    send_telegram_notification(f"<b>403 Forbidden</b> (X API)\nURL: {readable_url}\nRow: {row_idx}\nAttempt: {attempt}/{max_retries}")
                log_info(f"Auth error {status}, attempt {attempt}/{max_retries}")
                if attempt > max_retries:
                    raise RuntimeError(f"Persistent {status} on {url}. (Cookies banned?)")
                if not have_user_auth():
                    log_info("Attempting guest token refresh...", row_idx=row_idx)
                    refresh_guest_token()
                time.sleep(2)
                continue

            if status == 404:
                readable_url = get_readable_url(url)
                send_telegram_notification(f"<b>404 Not Found</b> (X API)\nURL: {readable_url}\nRow: {row_idx}")
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
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
                log_info(f"Server error {status}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})", row_idx=row_idx)
                time.sleep(sleep_s)
            else:
                resp.raise_for_status()

        except requests.exceptions.Timeout:
            if attempt > max_retries:
                raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_info(f"Timeout, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})", row_idx=row_idx)
            time.sleep(sleep_s)

        except KeyboardInterrupt:
            raise
        except Exception as e:
            if attempt > max_retries:
                raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_info(f"Error {e!s}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})", row_idx=row_idx)
            time.sleep(sleep_s)
    raise RuntimeError("Unreachable")

# ===============================
# COMMON HELPERS
# ===============================
def extract_identifier_from_link(link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip().lower()
    if not link:
        return None
    m = re.search(r'(?:twitter\.com|x\.com)(?:/i/community)?/([^/?#]+)', link)
    if m:
        return m.group(1).strip()
    return None

def is_rest_id(ident: str) -> bool:
    return bool(re.match(r'^\d+$', ident))

def get_readable_url(api_url: str) -> str:
    """แปลง URL ของ API (GraphQL) ให้เป็น URL ที่อ่านได้ง่ายสำหรับส่งเข้า Telegram"""
    if "CommunityQuery" in api_url:
        m = re.search(r'communityId%22%3A%20%22(\d+)%22', api_url)
        if m:
            return f"https://x.com/i/communities/{m.group(1)}"
    elif "UserByScreenName" in api_url:
        m = re.search(r'screen_name%22%3A%20%22([^%]+)%22', api_url)
        if m:
            return f"https://x.com/{m.group(1)}"
    return api_url

