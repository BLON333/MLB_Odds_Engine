import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import csv
import json
import time
import requests
from datetime import datetime
from utils import now_eastern, to_eastern
from dotenv import load_dotenv

from core.odds_fetcher import fetch_consensus_for_single_game
from core.market_pricer import decimal_odds, to_american_odds
from utils import TEAM_NAME_TO_ABBR, TEAM_ABBR_TO_NAME, TEAM_ABBR

from dotenv import load_dotenv
from pathlib import Path
dotenv_file = Path(__file__).resolve().parent.parent / ".env"
        os.getenv("DISCORD_ALERT_WEBHOOK_URL"),
        os.getenv("DISCORD_ALERT_WEBHOOK_URL_2"),
    ]
    if v and v.strip()
]
print(f"🔧 Loaded {len(loaded_hooks)} Discord webhook(s) from {dotenv_file}")

# Support sending CLV alerts to multiple Discord channels. Users can define
# `DISCORD_ALERT_WEBHOOK_URL` and optionally `DISCORD_ALERT_WEBHOOK_URL_2` in
# their .env file. Any non-empty URLs will receive the same alert message.
DISCORD_ALERT_WEBHOOK_URLS = loaded_hooks
closing_odds_path = "data/closing_odds"
os.makedirs(closing_odds_path, exist_ok=True)

fetched_games = set()
debug_mode = True  # ✅ easy toggle for debug

def send_discord_alert(message):
    if not DISCORD_ALERT_WEBHOOK_URLS:
        print("❌ No Discord webhook configured for alerts.")
        return
    for url in DISCORD_ALERT_WEBHOOK_URLS:
        try:

            resp = requests.post(url, json={"content": message}, timeout=10)
            if resp.status_code in (200, 204):
                print(f"✅ CLV alert sent to Discord webhook: {url}")
            else:
                print(
                    f"❌ Discord webhook {url} returned {resp.status_code}: {resp.text}"
                )
        except Exception as e:
            print(f"❌ Failed to send Discord alert to {url}: {e}")


def load_tracked_games(csv_path="logs/market_evals.csv"):
    bets = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bets.append(row)
    return bets


from utils import (
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
)  # Make sure this is imported at the top


def fuzzy_match_side(side, market_data):
    def clean(s):
        return (
            s.replace(" ", "")
            .replace("+", "")
            .replace("-", "")
            .replace(".", "")
            .lower()
        )

    side_clean = clean(side)

    # ✅ Try exact normalized match
    for key in market_data.keys():
        if clean(key) == side_clean:
            print(f"🧠 Fuzzy match: Exact normalized match for '{side}' → '{key}'")
            return key

    # ✅ Try partial substring or reverse match
    for key in market_data.keys():
        if side_clean in clean(key) or clean(key) in side_clean:
            print(f"🧠 Fuzzy match: Partial match for '{side}' → '{key}'")
            return key

    # ✅ Try Full Team Name → Abbreviation (e.g., "San Diego Padres" → "SD")
    if side in TEAM_NAME_TO_ABBR:
        abbr = TEAM_NAME_TO_ABBR[side]
        for key in market_data.keys():
            if clean(key) == clean(abbr):
                print(f"🧠 Fuzzy match: Abbreviation '{abbr}' for '{side}' → '{key}'")
                return key

    # ✅ NEW: Try Abbreviation → Full Team Name (e.g., "SD" → "San Diego Padres")
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.lower() == full_name.lower():
            for key in market_data:
                if clean(key) == clean(abbr):
                    print(
                        f"🧠 Fuzzy match: Full name '{full_name}' → abbr '{abbr}' → key '{key}'"
                    )
                    return key

    # ✅ Handle compact Over/Under formatting (e.g., Under8.0 vs Under8)
    if side.lower().startswith("over") or side.lower().startswith("under"):
        prefix = side.split()[0]
        try:
            number = side.split()[1]
        except IndexError:
            return None
        side_compact = f"{prefix}{number}".lower()
        for key in market_data.keys():
            if clean(key) == clean(side_compact):
                print(f"🧠 Fuzzy match: Over/Under compact '{side}' → '{key}'")
                return key

    # ❌ Nothing worked
    print(
        f"⚠️ Fuzzy match failed for '{side}' — tried keys: {list(market_data.keys())[:5]}"
    )
    return None


def get_market_data_with_alternates(consensus_odds, market_key):
    """
    Try to get market odds from main market or alternate fallback (e.g., totals → alternate_totals)
    """
    return consensus_odds.get(market_key) or consensus_odds.get(
        f"alternate_{market_key}"
    )


def monitor_loop(poll_interval=600, target_date=None):
    """Continuously fetch closing odds for games on ``target_date``.

    ``target_date`` defaults to today's date when the monitor is started and
    remains constant for the entire runtime. This prevents late-night runs from
    switching to the next calendar day mid-loop and inadvertently skipping the
    remaining games of the original date.
    """

    if target_date is None:
        target_date = now_eastern().strftime("%Y-%m-%d")

    while True:
        now_est = now_eastern()
        today = target_date  # Use a fixed date for the entire run

        loaded_bets = load_tracked_games()
        bets = [b for b in loaded_bets if b["game_id"].startswith(today)]
        tracked_games = set(b["game_id"] for b in bets)

        print(f"🔁 Checking games as of {now_est.strftime('%Y-%m-%d %H:%M:%S EST')}...")
        print(f"🔎 Monitoring {len(tracked_games)} games with bets placed...")

        file_path = os.path.join(closing_odds_path, f"{today}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path, "r") as f:
                    existing = json.load(f)
            except:
                print(
                    f"⚠️ Warning: Corrupt closing odds file for {today}. Starting fresh."
                )
                existing = {}
        else:
            existing = {}

        try:
            resp = requests.get(
                "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
                params={"apiKey": os.getenv("ODDS_API_KEY")},
            )
            if resp.status_code != 200:
                print("❌ Error fetching events:", resp.text)
                time.sleep(poll_interval)
                continue
            events = resp.json()
        except Exception as e:
            print(f"❌ Failed to fetch events: {e}")
            time.sleep(poll_interval)
            continue

        for event in events:
            start_time = event.get("commence_time", "")
            if not start_time:
                continue

            try:
                game_time_utc = datetime.fromisoformat(
                    start_time.replace("Z", "+00:00")
                )
                game_time = to_eastern(game_time_utc)

                game_date = game_time.strftime("%Y-%m-%d")
                if game_date != today:
                    if debug_mode:
                        print(
                            f"⏩ Skipping {event['away_team']}@{event['home_team']} because game date {game_date} != today {today}"
                        )
                    continue

                away_team_full = event["away_team"]
                home_team_full = event["home_team"]

                away_abbr = TEAM_ABBR.get(away_team_full, away_team_full.split()[-1])
                home_abbr = TEAM_ABBR.get(home_team_full, home_team_full.split()[-1])
                gid = f"{game_date}-{away_abbr}@{home_abbr}"

                time_to_game = (game_time - now_est).total_seconds()
                if debug_mode:
                    print(f"DEBUG: {gid} | time_to_game={time_to_game:.2f}s")

                if gid not in tracked_games:
                    continue
                if gid in fetched_games:
                    continue
                if gid in existing:
                    print(f"🛑 {gid} already captured. Skipping re-fetch.")
                    fetched_games.add(gid)
                    continue

                if 0 <= time_to_game <= 1000000:
                    print(f"📡 Fetching consensus odds for {gid}...")

                    consensus_odds = None
                    for attempt in range(2):
                        consensus_odds = fetch_consensus_for_single_game(gid)

                        if debug_mode:
                            print(
                                f"📡 [DEBUG] Attempt {attempt+1}: consensus odds fetched: {bool(consensus_odds)} for {gid}"
                            )

                        if consensus_odds:
                            break

                        if attempt == 0:
                            print(f"⚠️ No consensus odds found for {gid} — retrying...")
                            time.sleep(10)

                    if not consensus_odds:
                        print(f"⚠️ No consensus odds found for {gid} after retry.")
                        continue

                    existing[gid] = consensus_odds
                    with open(file_path, "w") as f:
                        json.dump(existing, f, indent=2)
                    print(f"✅ Saved closing odds snapshot for {gid}")

                    matching_bets = [b for b in bets if b["game_id"] == gid]
                    if not matching_bets:
                        print(f"ℹ️ No matching bets for {gid}.")
                        fetched_games.add(gid)
                        continue

                    print(f"✅ Found {len(matching_bets)} matching bets for {gid}")
                    alert_lines = []

                    for bet in matching_bets:
                        market = bet["market"]
                        side = bet["side"]
                        bet_odds = float(bet["market_odds"])

                        market_data = get_market_data_with_alternates(
                            consensus_odds, market
                        )
                        if not market_data:
                            print(
                                f"⚠️ Market '{market}' not found in consensus odds for {gid}. Available markets: {list(consensus_odds.keys())}"
                            )
                            continue

                        if debug_mode:
                            print(
                                f"   Available sides for market '{market}': {list(market_data.keys())}"
                            )
                            print(f"   Attempting to match bet side: '{side}'")

                        closing_data = market_data.get(side)
                        if not closing_data:
                            print(f"🔍 Attempting fuzzy match for: '{side}'")
                            fuzzy_key = fuzzy_match_side(side, market_data)
                            if fuzzy_key:
                                closing_data = market_data[fuzzy_key]

                        if not closing_data:
                            print(
                                f"⚠️ No match found for bet side '{side}' in market '{market}'"
                            )
                            continue

                        closing_prob = closing_data.get("consensus_prob")
                        if closing_prob is None:
                            continue

                        closing_american = to_american_odds(closing_prob)
                        bet_dec = decimal_odds(bet_odds)
                        closing_dec = decimal_odds(closing_american)
                        clv = ((bet_dec / closing_dec) - 1) * 100
                        emoji = "🟢" if clv > 0 else "🔴"

                        line = (
                            f"- **{side} ({market})**\n"
                            f"  • Bet Line: `{bet_odds:+}`\n"
                            f"  • Closing Line: `{closing_american:+}`\n"
                            f"  • CLV: `{clv:+.2f}%` {emoji}"
                        )
                        alert_lines.append(line)

                    if alert_lines:
                        print(
                            f"📣 Will send Discord alert with {len(alert_lines)} line(s):\n"
                            + "\n".join(alert_lines)
                        )
                        message = f"📊 **CLV Check - {gid}**\n" + "\n".join(alert_lines)
                        send_discord_alert(message)

                    fetched_games.add(gid)

            except Exception as e:
                print(f"⚠️ Error processing event: {e}")

        print(f"⏱ Sleeping for {poll_interval // 60} minutes...\n")
        time.sleep(poll_interval)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Monitor and capture MLB closing odds")
    parser.add_argument(
        "--date",
        dest="date",
        help="YYYY-MM-DD date to monitor (defaults to today's Eastern date)",
    )
    args = parser.parse_args()

    monitor_loop(target_date=args.date)
