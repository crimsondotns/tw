import time
import requests
import json
from typing import Optional, Tuple, List
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
                    common.log_info(f"initial guest token error: {e!s} ❌", row_idx=row_idx)

        resp = common.call_x_with_backoff(url, row_idx=row_idx)
        status = resp.status_code

        if status == 403 and common.have_user_auth():
            if common.enable_user_auth_on_session():
                resp = common.session.get(url, timeout=20)
                status = resp.status_code
                common.log_info(f"[retry user] status={status} {common.emoji_for_status(status)}", row_idx=row_idx)

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

def get_twitter_user_stats():
    overall_start = time.perf_counter()

    common.log_info("Fetching account links from spreadsheet...")
    links = common.sheet_status.col_values(1)[1:]
    common.log_info(f"Found {len(links)} links. Starting processing...")
    results: List[List[str]] = []

    for idx, link in enumerate(links, start=2):
        ident = common.extract_identifier_from_link(link or "")
        if not ident:
            results.append(["", "", ""])
            continue

        if common.is_rest_id(ident):
            try:
                status, member_count = fetch_community_member_count(ident, row_idx=idx)
                if status == 200 and member_count >= 0:
                    results.append([ident, "", str(member_count)])
                elif status == 429:
                    results.append([ident, "", "rate_limited"])
                else:
                    results.append([ident, "", f"status={status}"])
            except Exception as e:
                common.log_info(f"Error (Community) at row {idx}: {e!s}", row_idx=idx)
                results.append([ident, "", "ERROR"])
            continue

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

        old_tag = common.ENDPOINT_TAG
        common.ENDPOINT_TAG = screen_name
        try:
            resp = common.call_x_with_backoff(url, row_idx=idx)
            status = resp.status_code
            if status == 200:
                data = resp.json()
                legacy = (data.get("data", {}).get("user", {}).get("result", {}).get("legacy"))
                if legacy:
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
            common.log_info(f"Error (UserByScreenName) at row {idx}: {e!s}", row_idx=idx)
            results.append([screen_name, "ERROR", "ERROR"])
        finally:
            common.ENDPOINT_TAG = old_tag

    end_row = 1 + len(links)
    range_bcd = f"B2:D{end_row}"

    try:
        common.sheet_status.batch_clear([range_bcd])
        common.log_info(f"cleared range {range_bcd} ✅")
        common.sheet_status.update(values=results, range_name=range_bcd, value_input_option="RAW")
        common.log_info(f"wrote {len(results)} rows to {range_bcd} ✅")
    except Exception as e:
        common.log_info(f"sheet write error: {e!s} ❌")
        raise

    total_min = (time.perf_counter() - overall_start) / 60.0
    common.log_info(f"✅ เสร็จสิ้นการรันสคริปต์ (stats) ใช้เวลา {total_min:.1f} นาที ✅")

if __name__ == "__main__":
    get_twitter_user_stats()
