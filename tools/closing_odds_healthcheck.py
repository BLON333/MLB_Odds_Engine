import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

dotenv_file = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_file, override=True)
loaded_hooks = [
    v.strip()
    for v in [
        os.getenv("DISCORD_ALERT_WEBHOOK_URL"),
        os.getenv("DISCORD_ALERT_WEBHOOK_URL_2"),
    ]
    if v and v.strip()
]
print(f"🔧 Loaded {len(loaded_hooks)} Discord webhook(s) from {dotenv_file}")

# Allow healthcheck alerts to be sent to multiple Discord channels. Define
# `DISCORD_ALERT_WEBHOOK_URL` and optionally `DISCORD_ALERT_WEBHOOK_URL_2`.
DISCORD_ALERT_WEBHOOK_URLS = loaded_hooks
CLOSING_ODDS_DIR = "data/closing_odds"


def send_discord_alert(message):
    if not DISCORD_ALERT_WEBHOOK_URLS:
        print("❌ No Discord webhook configured for alerts.")
        return
    for url in DISCORD_ALERT_WEBHOOK_URLS:
        try:
            resp = requests.post(url, json={"content": message}, timeout=10)
            if resp.status_code in (200, 204):
                print(f"✅ Alert sent to Discord webhook: {url}")
            else:
                print(
                    f"❌ Discord webhook {url} returned {resp.status_code}: {resp.text}"
                )
        except Exception as e:
            print(f"❌ Failed to send alert to {url}: {e}")
def check_closing_odds():
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    # We want to check yesterday's games
    date_str = yesterday.strftime("%Y-%m-%d")
    file_path = os.path.join(CLOSING_ODDS_DIR, f"{date_str}.json")

    if not os.path.exists(file_path):
        msg = f"⚠️ Closing odds file for {date_str} is **missing**! (`{file_path}`)"
        print(msg)
        send_discord_alert(msg)
        return

    with open(file_path, "r") as f:
        data = json.load(f)

    if not data or len(data.keys()) < 3:  # If very few games, might be suspicious
        msg = f"⚠️ Closing odds file for {date_str} exists but is **empty or incomplete**. ({len(data.keys())} games)"
        print(msg)
        send_discord_alert(msg)
    else:
        print(
            f"✅ Closing odds file for {date_str} looks good: {len(data.keys())} games recorded."
        )


if __name__ == "__main__":
    check_closing_odds()
