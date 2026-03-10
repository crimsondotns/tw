import os

def process_file(file_name, extracts, is_profile):
    with open(f"d:\\Product\\Web2\\Twitter\\{file_name}", "r", encoding="utf-8") as f:
        lines = f.readlines()

    def extract(start_ln, end_ln):
        return "".join(lines[start_ln-1:end_ln])

    blocks = ""
    for start, end in extracts:
        blocks += extract(start, end)
        blocks += "\n"

    new_content = ""
    if is_profile:
        new_content = f"""import time
import requests
import json
from typing import Optional, Tuple, List
import common

{blocks}

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    get_twitter_user_stats()
"""
        new_content = new_content.replace("have_user_auth()", "common.have_user_auth()")
        new_content = new_content.replace("PREFER_USER_AUTH_FOR_COMMUNITY", "common.PREFER_USER_AUTH_FOR_COMMUNITY")
        new_content = new_content.replace("enable_user_auth_on_session()", "common.enable_user_auth_on_session()")
        new_content = new_content.replace("session.headers", "common.session.headers")
        new_content = new_content.replace("session.get", "common.session.get")
        new_content = new_content.replace("refresh_guest_token()", "common.refresh_guest_token()")
        new_content = new_content.replace("log_info", "common.log_info")
        new_content = new_content.replace("call_x_with_backoff(", "common.call_x_with_backoff(")
        new_content = new_content.replace("emoji_for_status(", "common.emoji_for_status(")
        new_content = new_content.replace("global ENDPOINT_TAG\n", "")
        new_content = new_content.replace("old_tag = ENDPOINT_TAG", "old_tag = common.ENDPOINT_TAG")
        new_content = new_content.replace("ENDPOINT_TAG = ", "common.ENDPOINT_TAG = ")
        new_content = new_content.replace("extract_identifier_from_link(", "common.extract_identifier_from_link(")
        new_content = new_content.replace("is_rest_id(", "common.is_rest_id(")
        new_content = new_content.replace("sheet_status", "common.sheet_status")
    else:
        new_content = f"""import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
import common

{blocks}

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    get_twitter_user_recent_posts(30)
"""
        new_content = new_content.replace("send_telegram_notification(", "common.send_telegram_notification(")
        new_content = new_content.replace("session.get", "common.session.get")
        new_content = new_content.replace("log_info", "common.log_info")
        new_content = new_content.replace("sheet_migration", "common.sheet_migration")
        new_content = new_content.replace("extract_identifier_from_link(", "common.extract_identifier_from_link(")
        new_content = new_content.replace("is_rest_id(", "common.is_rest_id(")
        new_content = new_content.replace("SGT", "common.SGT")
        new_content = new_content.replace("global ENDPOINT_TAG\n", "")
        new_content = new_content.replace("old_tag = ENDPOINT_TAG", "old_tag = common.ENDPOINT_TAG")
        new_content = new_content.replace("ENDPOINT_TAG = ", "common.ENDPOINT_TAG = ")

    with open(f"d:\\Product\\Web2\\Twitter\\{file_name}", "w", encoding="utf-8") as f:
        f.write(new_content)

# line numbers as seen in previous view_file (1-indexed, inclusive bounds)
# index_profile:
# 337 to 401 is deep_find_member_count and fetch_community_member_count
# 469 to 576 is get_twitter_user_stats
process_file("index_profile.py", [(337, 401), (469, 576)], True)

# index_post:
# 402 to 462 is fetch_nitter_rss_posts
# 577 to 692 is get_twitter_user_recent_posts
process_file("index_post.py", [(402, 462), (577, 692)], False)

print("success")
