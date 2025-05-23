import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DISCORD_ALERT_WEBHOOK_URL = os.getenv("DISCORD_ALERT_WEBHOOK_URL")  # ⚠️ Add to .env
CLOSING_ODDS_DIR = "data/closing_odds"

def send_discord_alert(message):
    if not DISCORD_ALERT_WEBHOOK_URL:
        print("❌ No Discord webhook configured for alerts.")
        return
    try:
        requests.post(DISCORD_ALERT_WEBHOOK_URL, json={"content": message})
        print("✅ Alert sent to Discord.")
    except Exception as e:
        print(f"❌ Failed to send alert: {e}")

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
        print(f"✅ Closing odds file for {date_str} looks good: {len(data.keys())} games recorded.")

if __name__ == "__main__":
    check_closing_odds()
