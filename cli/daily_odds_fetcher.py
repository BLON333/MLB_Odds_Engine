import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

    game_ids = [f.replace(".json", "") for f in os.listdir(sim_folder) if f.endswith(".json")]
    print(f"\n📅 Running daily odds fetch for {len(game_ids)} games on {today_tag}\n")

    odds_data = fetch_market_odds_from_api(game_ids)
    if not odds_data:
        print("❌ No odds data fetched.")
        return

    save_market_odds_to_file(odds_data, today_tag)
    print_market_health(odds_data)

if __name__ == "__main__":
    run_daily_odds_pipeline()
