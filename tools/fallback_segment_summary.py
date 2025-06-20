# fallback_segment_summary.py

import os
import json
from core.utils import normalize_label, classify_market_segment, find_sim_entry

# === Settings ===
MARKET_ODDS_FOLDER = "data/market_odds"
SIM_FOLDER = "backtest/sims"
TARGET_MARKETS = ["spreads", "totals", "h2h"]

fallback_log = []

print("\n📊 Fallback Segment Match Summary Report")

for fname in sorted(os.listdir(MARKET_ODDS_FOLDER)):
    if not fname.endswith(".json"):
        continue

    date_tag = fname.replace(".json", "")
    odds_path = os.path.join(MARKET_ODDS_FOLDER, fname)
    sim_dir = os.path.join(SIM_FOLDER, date_tag)

    if not os.path.exists(sim_dir):
        continue

    with open(odds_path) as f:
        odds_data = json.load(f)

    for game_id in odds_data:
        sim_path = os.path.join(sim_dir, f"{game_id}.json")
        if not os.path.exists(sim_path):
            continue

        with open(sim_path) as sf:
            sim_data = json.load(sf)

        sim_lines = sim_data.get("markets", [])

        for market_key in TARGET_MARKETS:
            market = odds_data[game_id].get(market_key, {})
            for label in market:
                entry = find_sim_entry(sim_lines, market_key, label, allow_fallback=False)
                if entry is None:
                    fallback_log.append((game_id, market_key, label))

print("\n📋 Summary of Sim Segment Mismatches:")
if fallback_log:
    for game_id, market_key, label in fallback_log:
        print(f"❌ {game_id} | {market_key} | {label}")
else:
    print("✅ No mismatches found — all market odds match correct sim segments!")

print(f"\n🧮 Total mismatches detected: {len(fallback_log)}")