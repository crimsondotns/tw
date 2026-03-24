import os
import re
import json
import time
import math
import random
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials

if os.name == 'nt':
    os.system("")

# ==========================================
# 1. ENVIRONMENT & CONFIGURATION
# ==========================================
def load_dotenv_manually():
    if os.path.exists(".env"):
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")

load_dotenv_manually()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BEARER_TOKEN = os.getenv("X_BEARER", "")
X_COOKIE_STRING = os.getenv("X_COOKIE_STRING", "")
X_AUTH_TOKEN = os.getenv("X_AUTH_TOKEN", "")
X_CT0 = os.getenv("X_CT0", "")
PREFER_USER_AUTH_FOR_COMMUNITY = True
ENDPOINT_TAG = "UserByScreenName"

SGT = timezone(timedelta(hours=7))

# ==========================================
# 2. GOOGLE SHEETS SETUP
# ==========================================
def load_credentials():
    if os.path.exists("service-account.json"):
        with open("service-account.json", "r") as f:
            return json.load(f)
    env_creds = os.getenv("SERVICE_ACCOUNT")
    if env_creds:
        return json.loads(env_creds)
    raise ValueError("Missing credentials: 'service-account.json' not found and 'SERVICE_ACCOUNT' env var is empty.")

SERVICE_ACCOUNT_INFO = load_credentials()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1xKU6PB6PaPBmq6wHkW6cEM3PDDu6LWzZjlnHJp0Mvqo"
SHEET_NAME_MIGRATION = "Copy of Migration"
SHEET_NAME_ENGAGEMENT = "Copy of Engagement"
SHEET_NAME_USER_ON_X = "User_on_X"

sheet_migration = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_MIGRATION)
sheet_engagement = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_ENGAGEMENT)
sheet_user_on_x = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME_USER_ON_X)

# ==========================================
# 3. UTILITIES & LOGGING
# ==========================================
def log_info(msg: str, **kwargs):
    ts = datetime.now(SGT).strftime("%Y-%m-%d %H:%M:%S")
    formatted_ts = f"\033[90m{ts}\033[0m"

    # Only colorize errors, warnings, and bypasses to reduce visual noise
    msg = msg.replace("[WARN]", "\033[93m[WARN]\033[0m")
    msg = msg.replace("[ERROR]", "\033[91m[ERROR]\033[0m")
    
    msg = msg.replace("[AUTH]", "\033[33m[AUTH]\033[0m")
    msg = msg.replace("[BYPASS]", "\033[90m[BYPASS]\033[0m")
    
    msg = re.sub(r'\[(2\d{2})\]', "\033[92m[\\1]\033[0m", msg)
    msg = re.sub(r'\[(4\d{2})\]', "\033[93m[\\1]\033[0m", msg)
    msg = re.sub(r'\[(5\d{2})\]', "\033[91m[\\1]\033[0m", msg)
    
    msg = re.sub(r'Status (2\d{2})', "Status \033[92m\\1\033[0m", msg)
    msg = re.sub(r'Status (4\d{2})', "Status \033[93m\\1\033[0m", msg)
    msg = re.sub(r'Status (5\d{2})', "Status \033[91m\\1\033[0m", msg)
    
    print(f"{formatted_ts} {msg}", flush=True)

def send_telegram_notification(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or "YOUR_" in TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            log_info(f"[WARN]  [NOTIFY] Telegram notification failed: status={resp.status_code}")
    except Exception as e:
        log_info(f"[ERROR] [NOTIFY] Telegram notification error: {e!s}")

def extract_identifier_from_link(link: str) -> Optional[str]:
    if not link: return None
    link = link.strip().lower()
    if not link: return None
    m = re.search(r'(?:twitter\.com|x\.com)(?:/i/community)?/([^/?#]+)', link)
    return m.group(1).strip() if m else None

def is_rest_id(ident: str) -> bool:
    return bool(re.match(r'^\d+$', ident))

def get_readable_url(api_url: str) -> str:
    if "CommunityQuery" in api_url:
        m = re.search(r'communityId%22%3A%20%22(\d+)%22', api_url)
        if m: return f"https://x.com/i/communities/{m.group(1)}"
    elif "UserByScreenName" in api_url:
        m = re.search(r'screen_name%22%3A%20%22([^%]+)%22', api_url)
        if m: return f"https://x.com/{m.group(1)}"
    return api_url

# ==========================================
# 4. X API SESSION & AUTHENTICATION
# ==========================================
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
    if X_COOKIE_STRING: cookie_map.update(parse_cookie_string(X_COOKIE_STRING))
    if X_AUTH_TOKEN: cookie_map["auth_token"] = X_AUTH_TOKEN
    if X_CT0: cookie_map["ct0"] = X_CT0

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
    if "x-csrf-token" in session.headers:
        del session.headers["x-csrf-token"]
    session.cookies.clear()

def have_user_auth() -> bool:
    return bool(X_AUTH_TOKEN or X_CT0 or X_COOKIE_STRING)

def refresh_guest_token():
    log_info("[INFO]  [AUTH] Refreshing guest token from X API...")
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
    log_info(f"[INFO]  [AUTH] New guest token: {new_token}")

X_GUEST_TOKEN = os.getenv("X_GUEST_TOKEN", "")
if have_user_auth():
    enable_user_auth_on_session()
elif X_GUEST_TOKEN:
    setup_guest_token(X_GUEST_TOKEN)
else:
    refresh_guest_token()

# ==========================================
# 5. X API NETWORK REQUEST ENGINE
# ==========================================
def call_x_with_backoff(url: str, params=None, max_retries=5, base_sleep=2.0, row_idx=None):
    for attempt in range(1, max_retries + 2):
        try:
            resp = session.get(url, params=params, timeout=10)
            status = resp.status_code
            
            log_info(f"[INFO]  [NET] GET {ENDPOINT_TAG} | Row {row_idx} | Status {status} | {resp.elapsed.total_seconds()*1000:.0f}ms")

            if status in (200, 400):
                return resp

            if status in (401, 403):
                if status == 403 and have_user_auth():
                    readable_url = get_readable_url(url)
                    send_telegram_notification(f"<b>403 Forbidden</b> (X API)\nURL: {readable_url}\nRow: {row_idx}\nAttempt: {attempt}/{max_retries}")
                log_info(f"[WARN]  [NET] Auth error {status}, attempt {attempt}/{max_retries}")
                
                if attempt > max_retries:
                    raise RuntimeError(f"Persistent {status} on {url}. (Cookies banned?)")
                if not have_user_auth():
                    log_info("[INFO]  [AUTH] Attempting guest token refresh...")
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
                    sleep_s = min(300, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
                    
                log_info(f"[WARN]  [NET] Waiting {sleep_s:.0f}s for rate limit window...")
                time.sleep(sleep_s)
                if attempt > max_retries:
                    raise RuntimeError("Too many rate limit retries.")
                continue

            if attempt <= max_retries and (500 <= status < 600 or status in (408, 409, 425)):
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
                log_info(f"[ERROR] [NET] Server error {status}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
                time.sleep(sleep_s)
            else:
                resp.raise_for_status()

        except requests.exceptions.Timeout:
            if attempt > max_retries: raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_info(f"[ERROR] [NET] Timeout, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
            time.sleep(sleep_s)

        except KeyboardInterrupt:
            raise
        except Exception as e:
            if attempt > max_retries: raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_info(f"[ERROR] [NET] Error {e!s}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
            time.sleep(sleep_s)
            
    raise RuntimeError("Unreachable")
