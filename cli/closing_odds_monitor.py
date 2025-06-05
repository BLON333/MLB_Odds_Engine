import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.logger import get_logger
logger = get_logger(__name__)

import csv
import json
import time
import requests
from datetime import datetime
from utils import now_eastern, to_eastern, safe_load_json
from dotenv import load_dotenv

from core.odds_fetcher import fetch_consensus_for_single_game
from core.market_pricer import decimal_odds, to_american_odds
from utils import TEAM_NAME_TO_ABBR, TEAM_ABBR_TO_NAME, TEAM_ABBR

from dotenv import load_dotenv
from pathlib import Path

dotenv_file = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_file)
loaded_hooks = [
    v
    for v in [
        os.getenv("DISCORD_ALERT_WEBHOOK_URL"),
        os.getenv("DISCORD_ALERT_WEBHOOK_URL_2"),
    ]
    if v and v.strip()
]
logger.info("🔧 Loaded %s Discord webhook(s) from %s", len(loaded_hooks), dotenv_file)

# Support sending CLV alerts to multiple Discord channels. Users can define
# `DISCORD_ALERT_WEBHOOK_URL` and optionally `DISCORD_ALERT_WEBHOOK_URL_2` in
# their .env file. Any non-empty URLs will receive the same alert message.
DISCORD_ALERT_WEBHOOK_URLS = loaded_hooks
closing_odds_path = "data/closing_odds"
os.makedirs(closing_odds_path, exist_ok=True)

fetched_games = set()
debug_mode = False  # ✅ easy toggle for debug

def send_discord_alert(message):
    if not DISCORD_ALERT_WEBHOOK_URLS:
        logger.error("❌ No Discord webhook configured for alerts.")
        return
    for url in DISCORD_ALERT_WEBHOOK_URLS:
        try:

            resp = requests.post(url, json={"content": message}, timeout=10)
            if resp.status_code in (200, 204):
                logger.info("✅ CLV alert sent to Discord webhook: %s", url)
            else:
                logger.error(
                    "❌ Discord webhook %s returned %s: %s", url, resp.status_code, resp.text
                )
        except Exception as e:
            logger.error("❌ Failed to send Discord alert to %s: %s", url, e)


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
            logger.debug("🧠 Fuzzy match: Exact normalized match for '%s' → '%s'", side, key)
            return key

    # ✅ Try partial substring or reverse match
    for key in market_data.keys():
        if side_clean in clean(key) or clean(key) in side_clean:
            logger.debug("🧠 Fuzzy match: Partial match for '%s' → '%s'", side, key)
            return key

    # ✅ Detect abbreviation prefix (e.g., "HOU -1.5") and expand to full name
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.upper().startswith(abbr):
            rest = side[len(abbr):].strip()
            reconstructed = f"{full_name} {rest}".strip()
            for key in market_data.keys():
                if clean(key) == clean(reconstructed):
                    logger.debug(
                        "🧠 Fuzzy match: Abbrev prefix '%s' → '%s' → '%s'",
                        side,
                        reconstructed,
                        key,
                    )
                    return key

    # ✅ Try Full Team Name → Abbreviation (e.g., "San Diego Padres" → "SD")
    if side in TEAM_NAME_TO_ABBR:
        abbr = TEAM_NAME_TO_ABBR[side]
        for key in market_data.keys():
            if clean(key) == clean(abbr):
                logger.debug("🧠 Fuzzy match: Abbreviation '%s' for '%s' → '%s'", abbr, side, key)
                return key

    # ✅ NEW: Try Abbreviation → Full Team Name (e.g., "SD" → "San Diego Padres")
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.lower() == full_name.lower():
            for key in market_data:
                if clean(key) == clean(abbr):
                    logger.debug(
                        "🧠 Fuzzy match: Full name '%s' → abbr '%s' → key '%s'",
                        full_name,
                        abbr,
                        key,
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
                logger.debug("🧠 Fuzzy match: Over/Under compact '%s' → '%s'", side, key)
                return key

    # ❌ Nothing worked
    logger.debug(
        "⚠️ Fuzzy match failed for '%s' — tried keys: %s",
        side,
        list(market_data.keys())[:5],
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

        logger.info(
            "🔁 Checking games as of %s...",
            now_est.strftime("%Y-%m-%d %H:%M:%S EST"),
        )
        logger.info("🔎 Monitoring %s games with bets placed...", len(tracked_games))

        file_path = os.path.join(closing_odds_path, f"{today}.json")
        if os.path.exists(file_path):
            existing = safe_load_json(file_path)
            if not isinstance(existing, dict):
                logger.warning(
                    "⚠️ Warning: Corrupt closing odds file for %s. Starting fresh.",
                    today,
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
                logger.error("❌ Error fetching events: %s", resp.text)
                time.sleep(poll_interval)
                continue
            events = resp.json()
        except Exception as e:
            logger.error("❌ Failed to fetch events: %s", e)
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
                        logger.debug(
                            "⏩ Skipping %s@%s because game date %s != today %s",
                            event['away_team'],
                            event['home_team'],
                            game_date,
                            today,
                        )
                    continue

                away_team_full = event["away_team"]
                home_team_full = event["home_team"]

                away_abbr = TEAM_ABBR.get(away_team_full, away_team_full.split()[-1])
                home_abbr = TEAM_ABBR.get(home_team_full, home_team_full.split()[-1])
                gid = f"{game_date}-{away_abbr}@{home_abbr}"

                time_to_game = (game_time - now_est).total_seconds()
                if debug_mode:
                    logger.debug("DEBUG: %s | time_to_game=%.2fs", gid, time_to_game)

                if gid not in tracked_games:
                    continue
                if gid in fetched_games:
                    continue
                if gid in existing:
                    logger.info("🛑 %s already captured. Skipping re-fetch.", gid)
                    fetched_games.add(gid)
                    continue

                if 0 <= time_to_game <= 1000000:
                    logger.info("📡 Fetching consensus odds for %s...", gid)

                    consensus_odds = None
                    for attempt in range(2):
                        consensus_odds = fetch_consensus_for_single_game(gid)

                        if debug_mode:
                            logger.debug(
                                "📡 [DEBUG] Attempt %s: consensus odds fetched: %s for %s",
                                attempt + 1,
                                bool(consensus_odds),
                                gid,
                            )

                        if consensus_odds:
                            break

                        if attempt == 0:
                            logger.warning("⚠️ No consensus odds found for %s — retrying...", gid)
                            time.sleep(10)

                    if not consensus_odds:
                        logger.warning("⚠️ No consensus odds found for %s after retry.", gid)
                        continue

                    existing[gid] = consensus_odds
                    with open(file_path, "w") as f:
                        json.dump(existing, f, indent=2)
                    logger.info("✅ Saved closing odds snapshot for %s", gid)

                    matching_bets = [b for b in bets if b["game_id"] == gid]
                    if not matching_bets:
                        logger.info("ℹ️ No matching bets for %s.", gid)
                        fetched_games.add(gid)
                        continue

                    logger.info("✅ Found %s matching bets for %s", len(matching_bets), gid)
                    alert_lines = []

                    for bet in matching_bets:
                        market = bet["market"]
                        side = bet["side"]
                        bet_odds = float(bet["market_odds"])

                        market_data = get_market_data_with_alternates(
                            consensus_odds, market
                        )
                        if not market_data:
                            logger.warning(
                                "⚠️ Market '%s' not found in consensus odds for %s. Available markets: %s",
                                market,
                                gid,
                                list(consensus_odds.keys()),
                            )
                            continue

                        if debug_mode:
                            logger.debug(
                                "   Available sides for market '%s': %s",
                                market,
                                list(market_data.keys()),
                            )
                            logger.debug("   Attempting to match bet side: '%s'", side)

                        closing_data = market_data.get(side)
                        if not closing_data:
                            logger.debug("🔍 Attempting fuzzy match for: '%s'", side)
                            fuzzy_key = fuzzy_match_side(side, market_data)
                            if fuzzy_key:
                                closing_data = market_data[fuzzy_key]

                        if not closing_data:
                            logger.warning(
                                "⚠️ No match found for bet side '%s' in market '%s'",
                                side,
                                market,
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
                        logger.info(
                            "📣 Will send Discord alert with %s line(s):\n%s",
                            len(alert_lines),
                            "\n".join(alert_lines),
                        )
                        message = f"📊 **CLV Check - {gid}**\n" + "\n".join(alert_lines)
                        send_discord_alert(message)

                    fetched_games.add(gid)

            except Exception as e:
                logger.error("⚠️ Error processing event: %s", e)

        logger.info("⏱ Sleeping for %s minutes...\n", poll_interval // 60)
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
