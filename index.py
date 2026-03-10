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
SHEET_NAME_STATUS = "Migration"
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
# LOGGING UTIL
# ===============================
SGT = ZoneInfo("Asia/Bangkok")


def now_sgt_str():
    t = datetime.now(SGT)
    s = t.strftime("%b %d, %Y, %I:%M:%S %p")
    return s.replace(" AM", "\u202FAM").replace(" PM", "\u202FPM")


def emoji_for_status(status_code):
    if status_code == 200:
        return "✅"
    if status_code == 429:
        return "⚠️"
    if 200 <= status_code < 300:
        return "✅"
    return "❌"


def log_info(msg):
    print(f"{now_sgt_str()}\tInfo\t{msg}")


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
def call_x_with_backoff(url, max_retries=8, base_sleep=2.0, timeout=20):
    attempt = 0
    guest_refreshed = False
    while True:
        attempt += 1
        start = time.perf_counter()
        try:
            resp = session.get(url, timeout=timeout)
            dur_ms = int((time.perf_counter() - start) * 1000)
            status = resp.status_code
            log_info(f"[{ENDPOINT_TAG}] {dur_ms}ms, status={status} {emoji_for_status(status)}")

            if status == 200:
                return resp

            if status == 403 and have_user_auth():
                if "x-csrf-token" not in session.headers:
                    if enable_user_auth_on_session():
                        continue
                return resp

            if status == 403 and not have_user_auth() and not guest_refreshed:
                try:
                    refresh_guest_token()
                    guest_refreshed = True
                    continue
                except Exception as e:
                    log_info(f"guest token refresh error: {e!s} ❌")
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
                log_info(f"waiting {sleep_s:.0f}s for rate limit window…")
                time.sleep(sleep_s)
                if attempt > max_retries:
                    raise RuntimeError("Too many rate limit retries.")
                continue

            if attempt <= max_retries and (500 <= status < 600 or status in (408, 409, 425, 502, 503, 504)):
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                log_info(f"transient error {status}, retrying in {sleep_s:.1f}s…")
                time.sleep(sleep_s)
                continue

            return resp

        except requests.RequestException as e:
            dur_ms = int((time.perf_counter() - start) * 1000)
            log_info(f"[{ENDPOINT_TAG}] {dur_ms}ms, network error: {e!s} ❌")
            if attempt <= max_retries:
                sleep_s = min(60, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                log_info(f"retrying in {sleep_s:.1f}s…")
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


def fetch_community_member_count(rest_id: str) -> Tuple[int, int]:
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
                    log_info(f"initial guest token error: {e!s} ❌")

        resp = call_x_with_backoff(url)
        status = resp.status_code

        if status == 403 and have_user_auth():
            if enable_user_auth_on_session():
                resp = session.get(url, timeout=20)
                status = resp.status_code
                log_info(f"[CommunityQuery retry user] status={status} {emoji_for_status(status)}")

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
# UserTweets helpers (scan JSON ทั้งก้อน)
# ===============================
def extract_tweets_from_usertweets(
    data: dict,
    cutoff_dt: Optional[datetime]
) -> List[Tuple[datetime, str]]:
    """
    ดึงทวีตจาก response ของ UserTweets แบบ scan ทั้ง JSON
    """
    tweets: List[Tuple[datetime, str]] = []
    seen: Set[Tuple[str, str]] = set()

    def walk(obj: Any):
        if isinstance(obj, dict):
            legacy = obj.get("legacy")
            if isinstance(legacy, dict):
                text = legacy.get("full_text") or legacy.get("text")
                created_at = legacy.get("created_at")
                if text and created_at:
                    key = (created_at, text)
                    if key not in seen:
                        seen.add(key)
                        dt: Optional[datetime] = None
                        try:
                            dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                        except ValueError:
                            try:
                                dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %Y")
                                dt = dt.replace(tzinfo=timezone.utc)
                            except Exception:
                                dt = None

                        if dt is not None:
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            else:
                                dt = dt.astimezone(timezone.utc)
                            tweets.append((dt, text))

            for v in obj.values():
                walk(v)

        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(data)
    tweets.sort(key=lambda x: x[0], reverse=True)

    if tweets:
        log_info(f"UserTweets raw (scan-all) count={len(tweets)}")
        for i, (dt, text) in enumerate(tweets[:3], start=1):
            snippet = text.replace("\n", " ")
            if len(snippet) > 60:
                snippet = snippet[:57] + "..."
            log_info(f"UserTweets raw[{i}] dt_utc={dt.isoformat()} snippet={snippet}")
    else:
        log_info("UserTweets raw (scan-all) count=0 — ไม่เจอ legacy.full_text เลย")

    if cutoff_dt is not None:
        if cutoff_dt.tzinfo is None:
            cutoff_dt = cutoff_dt.replace(tzinfo=timezone.utc)
        else:
            cutoff_dt = cutoff_dt.astimezone(timezone.utc)

        kept = [(dt, text) for (dt, text) in tweets if dt >= cutoff_dt]
        log_info(
            f"UserTweets filter by cutoff={cutoff_dt.isoformat()} → kept={len(kept)}"
        )
        return kept

    return tweets


def fetch_user_tweets_last_days(
    user_id: str,
    days: int = 7,
    max_count: int = 100
) -> Tuple[int, List[Tuple[datetime, str]]]:
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)
    log_info(f"fetch_user_tweets_last_days: user_id={user_id}, days={days}, cutoff_utc={cutoff_dt.isoformat()}")

    variables = {
        "userId": user_id,
        "count": max_count,
        "includePromotedContent": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withVoice": True,
        "withV2Timeline": True,
    }

    features = {
        "rweb_video_screen_enabled": False,
        "payments_enabled": False,
        "profile_label_improvements_pcf_label_in_post_enabled": True,
        "responsive_web_profile_redirect_enabled": False,
        "rweb_tipjar_consumption_enabled": True,
        "verified_phone_label_enabled": False,
        "creator_subscriptions_tweet_preview_api_enabled": True,
        "responsive_web_graphql_timeline_navigation_enabled": True,
        "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
        "premium_content_api_read_enabled": False,
        "communities_web_enable_tweet_community_results_fetch": True,
        "c9s_tweet_anatomy_moderator_badge_enabled": True,
        "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
        "responsive_web_grok_analyze_post_followups_enabled": True,
        "responsive_web_jetfuel_frame": True,
        "responsive_web_grok_share_attachment_enabled": True,
        "articles_preview_enabled": True,
        "responsive_web_edit_tweet_api_enabled": True,
        "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
        "view_counts_everywhere_api_enabled": True,
        "longform_notetweets_consumption_enabled": True,
        "responsive_web_twitter_article_tweet_consumption_enabled": True,
        "tweet_awards_web_tipping_enabled": False,
        "responsive_web_grok_show_grok_translated_post": False,
        "responsive_web_grok_analysis_button_from_backend": True,
        "creator_subscriptions_quote_tweet_preview_enabled": False,
        "freedom_of_speech_not_reach_fetch_enabled": True,
        "standardized_nudges_misinfo": True,
        "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
        "longform_notetweets_rich_text_read_enabled": True,
        "longform_notetweets_inline_media_enabled": True,
        "responsive_web_grok_image_annotation_enabled": True,
        "responsive_web_grok_imagine_annotation_enabled": True,
        "responsive_web_grok_community_note_auto_translation_is_enabled": False,
        "responsive_web_enhance_cards_enabled": False
    }

    fieldToggles = {"withArticlePlainText": False}

    url = (
        "https://x.com/i/api/graphql/oRJs8SLCRNRbQzuZG93_oA/UserTweets?"
        f"variables={requests.utils.quote(json.dumps(variables))}"
        f"&features={requests.utils.quote(json.dumps(features))}"
        f"&fieldToggles={requests.utils.quote(json.dumps(fieldToggles))}"
    )

    global ENDPOINT_TAG
    old_tag = ENDPOINT_TAG
    ENDPOINT_TAG = "UserTweets"
    try:
        if have_user_auth():
            enable_user_auth_on_session()
        else:
            if "x-guest-token" not in session.headers:
                try:
                    refresh_guest_token()
                except Exception as e:
                    log_info(f"initial guest token error: {e!s} ❌")

        resp = call_x_with_backoff(url)
        status = resp.status_code
        if status == 200:
            data = resp.json()
            tweets = extract_tweets_from_usertweets(data, cutoff_dt)
            return status, tweets
        else:
            return status, []
    finally:
        ENDPOINT_TAG = old_tag


# ===============================
# MAIN 1: ดึง stats → Migration (B:C:D)
# ===============================
def get_twitter_user_stats():
    overall_start = time.perf_counter()

    links = sheet_status.col_values(1)[1:]
    results: List[List[str]] = []

    for idx, link in enumerate(links, start=2):
        ident = extract_identifier_from_link(link or "")
        if not ident:
            results.append(["", "", ""])
            continue

        # ถ้าเป็นเลขยาว → communityId
        if is_rest_id(ident):
            try:
                status, member_count = fetch_community_member_count(ident)
                if status == 200 and member_count >= 0:
                    # B = communityId, C = "", D = member_count
                    results.append([ident, "", str(member_count)])
                elif status == 429:
                    results.append([ident, "", "rate_limited"])
                else:
                    results.append([ident, "", f"status={status}"])
            except Exception as e:
                log_info(f"Error (Community) at row {idx}: {e!s}")
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
        ENDPOINT_TAG = "UserByScreenName"
        try:
            resp = call_x_with_backoff(url)
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
            log_info(f"Error (UserByScreenName) at row {idx}: {e!s}")
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
            log_info(f"row={idx} link ว่าง / parse ไม่ได้ → ข้าม")
            all_rows.append([""])
            continue

        if is_rest_id(ident_raw):
            rest_id = ident_raw
            screen_name_for_sheet = ident_raw  # ชื่อแถวมีอยู่แล้วในคอลัมน์ B จาก stats
            log_info(f"row={idx} ใช้ค่าจากลิงก์เป็น userId โดยตรง: userId={rest_id}")
        else:
            screen_name = ident_raw.lstrip("@")
            screen_name_for_sheet = screen_name
            log_info(f"row={idx} เริ่ม lookup UserByScreenName สำหรับ @{screen_name}")

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
            ENDPOINT_TAG = "UserByScreenName"

            try:
                resp = call_x_with_backoff(url)
                status = resp.status_code
                if status != 200:
                    accounts_user_lookup_err += 1
                    log_info(f"row={idx} UserByScreenName status={status} ❌")
                    all_rows.append([f"status={status}"])
                    ENDPOINT_TAG = old_tag
                    continue

                data = resp.json()
                user_result = (data.get("data", {}) or {}).get("user", {}) or {}
                user_result = user_result.get("result", {}) or {}
                rest_id = user_result.get("rest_id") or (user_result.get("result", {}) or {}).get("rest_id")
                if not rest_id:
                    accounts_no_user_id += 1
                    log_info(f"row={idx} ไม่พบ rest_id ใน UserByScreenName result ❌")
                    all_rows.append(["NO_USER_ID"])
                    ENDPOINT_TAG = old_tag
                    continue

                log_info(f"row={idx} ได้ userId={rest_id} สำหรับ @{screen_name}")

            except Exception as e:
                accounts_user_lookup_err += 1
                log_info(f"row={idx} Error (UserByScreenName): {e!s} ❌")
                all_rows.append(["ERROR_USER_LOOKUP"])
                ENDPOINT_TAG = old_tag
                continue
            finally:
                ENDPOINT_TAG = "UserByScreenName"

        try:
            log_info(f"row={idx} เรียก UserTweets สำหรับ userId={rest_id}")
            status2, tweets = fetch_user_tweets_last_days(rest_id, days=days, max_count=100)
            if status2 != 200:
                accounts_tweets_api_err += 1
                log_info(f"row={idx} UserTweets status={status2} ❌")
                all_rows.append([f"tweets_status={status2}"])
                continue

            texts = [t[1] for t in tweets]
            tweet_count = len(texts)
            total_tweets_nd += tweet_count

            if tweet_count == 0:
                accounts_zero_tweets_nd += 1
                log_info(f"row={idx} user={screen_name_for_sheet}, tweets_{days}d=0 (ไม่มีโพสต์ในช่วง {days} วัน)")
            else:
                accounts_with_tweets_nd += 1
                newest_dt = tweets[0][0].astimezone(SGT)
                oldest_dt = tweets[-1][0].astimezone(SGT)
                log_info(
                    f"row={idx} user={screen_name_for_sheet}, tweets_{days}d={tweet_count}, "
                    f"newest_SGT={newest_dt.isoformat()}, oldest_SGT={oldest_dt.isoformat()}"
                )
                snippet = texts[0].replace("\n", " ")
                if len(snippet) > 80:
                    snippet = snippet[:77] + "..."
                log_info(f"row={idx} ตัวอย่างโพสต์ล่าสุด: {snippet}")

            # เขียนเฉพาะโพสต์ลงคอลัมน์ E,F,G,... (ไม่ต้องเขียน username ซ้ำ)
            row = texts if texts else [""]
            all_rows.append(row)
            max_tweets = max(max_tweets, len(row))

        except Exception as e:
            accounts_tweets_api_err += 1
            log_info(f"row={idx} Error (UserTweets): {e!s} ❌")
            all_rows.append(["ERROR_TWEETS"])
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

    log_info("===== SUMMARY (RECENT POSTS) ===telek")
    log_info(f"accounts_total={total_accounts}")
    log_info(f"accounts_empty_link={accounts_empty_link}")
    log_info(f"accounts_no_user_id={accounts_no_user_id}")
    log_info(f"accounts_user_lookup_err={accounts_user_lookup_err}")
    log_info(f"accounts_tweets_api_err={accounts_tweets_api_err}")
    log_info(f"accounts_with_tweets_{days}d={accounts_with_tweets_nd}")
    log_info(f"accounts_zero_tweets_{days}d={accounts_zero_tweets_nd}")
    log_info(f"total_tweets_{days}d={total_tweets_nd}")
    log_info(f"ใช้เวลารวม {total_min:.2f} นาที")
    log_info("===== END SUMMARY ===telek")

    log_info("✅ เสร็จสิ้นการรันสคริปต์ (recent posts) ✅")


# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    # เขียน stats ลง Migration (B:C:D)
     get_twitter_user_stats()

    # เขียนโพสต์ย้อนหลัง 30 วันลง Migration (E:...)
     get_twitter_user_recent_posts(30)
