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

# Enable ANSI escape codes on Windows
if os.name == "nt":
    os.system("")

# ==========================================
# 1. ENVIRONMENT & CONFIGURATION
# ==========================================
def _load_dotenv():
    """Load .env file into os.environ (simple key=value parser)."""
    if not os.path.exists(".env"):
        return
    with open(".env", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip().strip('"').strip("'")

_load_dotenv()

BEARER_TOKEN      = os.getenv("X_BEARER", "")
X_COOKIE_STRING   = os.getenv("X_COOKIE_STRING", "")
X_AUTH_TOKEN      = os.getenv("X_AUTH_TOKEN", "")
X_CT0             = os.getenv("X_CT0", "")
ENDPOINT_TAG      = "UserByScreenName"

SGT = timezone(timedelta(hours=7))

# ==========================================
# 2. GOOGLE SHEETS SETUP
# ==========================================
def _load_credentials() -> dict:
    """Load Google service-account credentials from file or env."""
    if os.path.exists("service-account.json"):
        with open("service-account.json", "r") as f:
            return json.load(f)
    env_creds = os.getenv("SERVICE_ACCOUNT")
    if env_creds:
        return json.loads(env_creds)
    raise ValueError(
        "Missing credentials: 'service-account.json' not found "
        "and 'SERVICE_ACCOUNT' env var is empty."
    )

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_creds  = Credentials.from_service_account_info(_load_credentials(), scopes=_SCOPES)
client  = gspread.authorize(_creds)

SPREADSHEET_ID       = "1xKU6PB6PaPBmq6wHkW6cEM3PDDu6LWzZjlnHJp0Mvqo"
SHEET_NAME_MIGRATION = "Copy of Migration"
SHEET_NAME_ENGAGEMENT = "Copy of Engagement"
SHEET_NAME_USER_ON_X = "Copy of User_on_X"

_spreadsheet     = client.open_by_key(SPREADSHEET_ID)
sheet_migration  = _spreadsheet.worksheet(SHEET_NAME_MIGRATION)
sheet_engagement = _spreadsheet.worksheet(SHEET_NAME_ENGAGEMENT)
sheet_user_on_x  = _spreadsheet.worksheet(SHEET_NAME_USER_ON_X)

# ==========================================
# 3. LOGGING
# ==========================================
class Logger:
    _COLORS = {
        "INFO":    "\033[36m",   # Cyan
        "SUCCESS": "\033[32m",   # Green
        "WARN":    "\033[33m",   # Yellow
        "ERROR":   "\033[31m",   # Red
    }
    _RESET = "\033[0m"
    _GREY  = "\033[90m"

    @staticmethod
    def log(level: str, message: str, context: str = ""):
        now = datetime.now(SGT)
        ts  = f"{now.strftime('%Y-%m-%d %H:%M:%S')},{now.microsecond // 1000:03d}"
        color = Logger._COLORS.get(level, "")
        ctx   = f"{Logger._GREY}{context}{Logger._RESET} " if context else ""
        print(f"{ts} | {color}{level:<8}{Logger._RESET} | {ctx}{message}", flush=True)

def log_info(msg, context=""):    Logger.log("INFO", msg, context)
def log_success(msg, context=""): Logger.log("SUCCESS", msg, context)
def log_warn(msg, context=""):   Logger.log("WARN", msg, context)
def log_error(msg, context=""):  Logger.log("ERROR", msg, context)

# ==========================================
# 4. UTILITIES
# ==========================================
def extract_identifier_from_link(link: str) -> Optional[str]:
    """Extract username or community ID from a twitter.com / x.com URL."""
    if not link:
        return None
    link = link.strip().lower()
    if not link:
        return None
    m = re.search(r'(?:twitter\.com|x\.com)(?:/i/community)?/([^/?#]+)', link)
    return m.group(1).strip() if m else None

def is_rest_id(ident: str) -> bool:
    """Check whether *ident* looks like a numeric REST ID (community ID)."""
    return bool(re.match(r'^\d+$', ident))

# ==========================================
# 5. X API SESSION & AUTHENTICATION
# ==========================================
session = requests.Session()
session.headers.update({
    "Authorization":            BEARER_TOKEN,
    "Content-Type":             "application/json",
    "User-Agent":               ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) "
                                 "Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"),
    "Accept":                   "*/*",
    "Accept-Language":          "en-US,en;q=0.9,th;q=0.8",
    "Accept-Encoding":          "gzip, deflate, br, zstd",
    "x-twitter-client-language": "en",
    "x-twitter-active-user":    "yes",
})

def _parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    jar: Dict[str, str] = {}
    for part in cookie_str.split(";"):
        if "=" in part:
            k, v = part.strip().split("=", 1)
            jar[k.strip()] = v.strip()
    return jar

def enable_user_auth_on_session() -> bool:
    """Inject user cookies (auth_token / ct0) into the shared session."""
    cookie_map: Dict[str, str] = {}
    if X_COOKIE_STRING:
        cookie_map.update(_parse_cookie_string(X_COOKIE_STRING))
    if X_AUTH_TOKEN:
        cookie_map["auth_token"] = X_AUTH_TOKEN
    if X_CT0:
        cookie_map["ct0"] = X_CT0

    if not cookie_map:
        return False

    for k, v in cookie_map.items():
        session.cookies.set(k, v, domain=".twitter.com")
        session.cookies.set(k, v, domain=".x.com")
    if "ct0" in cookie_map:
        session.headers["x-csrf-token"] = cookie_map["ct0"]
    return True

def setup_guest_token(token: str):
    """Switch the session to guest-token mode."""
    session.headers["x-guest-token"] = token
    session.headers.pop("x-csrf-token", None)
    session.cookies.clear()

def have_user_auth() -> bool:
    return bool(X_AUTH_TOKEN or X_CT0 or X_COOKIE_STRING)

def refresh_guest_token():
    """Activate a new guest token via the X API."""
    log_info("[AUTH] Refreshing guest token from X API...")
    resp = requests.post(
        "https://api.x.com/1.1/guest/activate.json",
        headers={"Authorization": BEARER_TOKEN},
        timeout=10,
    )
    resp.raise_for_status()
    new_token = resp.json().get("guest_token")
    if not new_token:
        raise ValueError("No guest_token returned from X API.")
    os.environ["X_GUEST_TOKEN"] = new_token
    setup_guest_token(new_token)
    log_info(f"[AUTH] New guest token: {new_token}")

# --- Bootstrap auth on import ---
_X_GUEST_TOKEN = os.getenv("X_GUEST_TOKEN", "")
if have_user_auth():
    enable_user_auth_on_session()
elif _X_GUEST_TOKEN:
    setup_guest_token(_X_GUEST_TOKEN)
else:
    refresh_guest_token()

# ==========================================
# 6. X API NETWORK REQUEST ENGINE
# ==========================================
_RETRYABLE_SERVER_CODES = frozenset(range(500, 600)) | {408, 409, 425}

def _compute_rate_limit_sleep(resp: requests.Response, attempt: int,
                               base_sleep: float) -> float:
    """Determine how long to sleep on a 429 response."""
    reset_epoch = resp.headers.get("x-rate-limit-reset")
    if reset_epoch:
        try:
            return max(0, math.ceil(int(reset_epoch) - datetime.now(timezone.utc).timestamp()) + 1)
        except Exception:
            pass
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return max(1, int(float(retry_after)))
        except Exception:
            pass
    return min(300, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)

def call_x_with_backoff(url: str, *, params=None, max_retries=5,
                         base_sleep=2.0, row_idx=None) -> requests.Response:
    """GET *url* with exponential back-off, auth-refresh, and rate-limit handling."""
    for attempt in range(1, max_retries + 2):
        try:
            resp   = session.get(url, params=params, timeout=10)
            status = resp.status_code
            ms     = resp.elapsed.total_seconds() * 1000

            log_info(
                f"[NET] GET {ENDPOINT_TAG} | "
                f"Row \033[33m{row_idx}\033[0m "
                f"(Compiled in : \033[90m{ms:.0f}ms\033[0m)"
            )

            # --- Success / client error ---
            if status in (200, 400):
                return resp

            # --- Auth errors ---
            if status in (401, 403):
                log_warn(f"[NET] Auth error {status}, attempt {attempt}/{max_retries}")
                if attempt > max_retries:
                    raise RuntimeError(f"Persistent {status} on {url}. (Cookies banned?)")
                if not have_user_auth():
                    log_info("[AUTH] Attempting guest token refresh...")
                    refresh_guest_token()
                time.sleep(2)
                continue

            # --- Not found ---
            if status == 404:
                return resp

            # --- Rate limit ---
            if status == 429:
                sleep_s = _compute_rate_limit_sleep(resp, attempt, base_sleep)
                log_warn(f"[NET] Waiting {sleep_s:.0f}s for rate limit window...")
                time.sleep(sleep_s)
                if attempt > max_retries:
                    raise RuntimeError("Too many rate limit retries.")
                continue

            # --- Server errors (retryable) ---
            if attempt <= max_retries and status in _RETRYABLE_SERVER_CODES:
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
                log_error(f"[NET] Server error {status}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
                time.sleep(sleep_s)
            else:
                resp.raise_for_status()

        except requests.exceptions.Timeout:
            if attempt > max_retries:
                raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_error(f"[NET] Timeout, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
            time.sleep(sleep_s)

        except KeyboardInterrupt:
            raise

        except Exception as e:
            if attempt > max_retries:
                raise
            sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 1.0)
            log_error(f"[NET] Error {e!s}, retrying in {sleep_s:.1f}s... ({attempt}/{max_retries})")
            time.sleep(sleep_s)

    raise RuntimeError("Unreachable")

# ==========================================
# 7. COMMUNITY QUERY API
# ==========================================
def _deep_find_member_count(obj):
    """Recursively search a JSON tree for the first ``member_count`` int."""
    if isinstance(obj, dict):
        if "member_count" in obj and isinstance(obj["member_count"], int):
            return obj["member_count"]
        for v in obj.values():
            found = _deep_find_member_count(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _deep_find_member_count(v)
            if found is not None:
                return found
    return None

def fetch_community_member_count(rest_id: str, row_idx: Optional[int] = None) -> Tuple[int, int]:
    """Fetch member_count for a community via X GraphQL CommunityQuery.

    Args:
        rest_id:  The numeric community ID.
        row_idx:  Optional sheet row index for logging.

    Returns:
        ``(http_status, member_count)``.  *member_count* is ``-1`` on failure.
    """
    variables = {"communityId": rest_id}
    features  = {
        "c9s_list_members_action_api_enabled": False,
        "c9s_superc9s_indication_enabled": False,
    }
    url = (
        "https://api.x.com/graphql/uBpODvS60xZ1q2L88d-W2A/CommunityQuery?"
        f"variables={requests.utils.quote(json.dumps(variables))}"
        f"&features={requests.utils.quote(json.dumps(features))}"
    )

    global ENDPOINT_TAG
    old_tag = ENDPOINT_TAG
    ENDPOINT_TAG = "CommunityQuery"
    try:
        # CommunityQuery works with the current session on api.x.com
        try:
            refresh_guest_token()
        except Exception as e:
            log_warn(f"[AUTH] Guest token refresh failed: {e!s}, trying with existing session...")

        try:
            resp   = call_x_with_backoff(url, row_idx=row_idx, max_retries=3)
            status = resp.status_code
        except RuntimeError as e:
            log_error(f"[NET] CommunityQuery failed for community={rest_id}: {e!s}")
            return 403, -1

        if status == 200:
            data = resp.json()
            member_count = data.get("data", {}).get("community", {}).get("member_count")
            if not isinstance(member_count, int):
                member_count = _deep_find_member_count(data) or -1
            return 200, member_count

        return status, -1
    finally:
        ENDPOINT_TAG = old_tag
