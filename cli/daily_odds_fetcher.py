import sys
import os
from core.bootstrap import *  # noqa
from core.config import DEBUG_MODE, VERBOSE_MODE

from core.logger import get_logger
logger = get_logger(__name__)

import json
from datetime import datetime

from core.odds_fetcher import fetch_market_odds_from_api, save_market_odds_to_file


# === Market Health Check Helper ===
def print_market_health(odds_data):
    print("\n🩺 Market Health Check:\n")
    for game_id, markets in odds_data.items():
        if not markets:
            print(f"⚠️ {game_id}: No markets available.")
            continue

        print(f"🎯 {game_id}:")
        for market_key, entries in markets.items():
            if market_key.endswith("_source") or market_key == "start_time":
                continue
            if isinstance(entries, dict):
                count = len(entries)
                print(f"   ➤ {market_key}: {count} odds lines available")
            else:
                print(f"   ➤ {market_key}: [unexpected type {type(entries).__name__}]")

# === Run Daily Odds Fetching + Health Check ===
def run_daily_odds_pipeline():
    # Default to today
    today_tag = datetime.today().strftime("%Y-%m-%d")
    sim_folder = f"backtest/sims/{today_tag}"

    if not os.path.exists(sim_folder):
        print(f"⚠️ Sim folder does not exist: {sim_folder}")
        return

    from pathlib import Path

    sim_dir = Path(sim_folder)
    game_ids = [f.stem for f in sim_dir.glob("*.json") if "-T" in f.stem]
    print(f"\n📅 Running daily odds fetch for {len(game_ids)} games on {today_tag}\n")

    odds_data = fetch_market_odds_from_api(game_ids)
    if odds_data is None:
        print("❌ Failed to fetch odds data.")
        return

    out_path = save_market_odds_to_file(odds_data, today_tag)
    if out_path:
        print_market_health(odds_data)

if __name__ == "__main__":
    run_daily_odds_pipeline()