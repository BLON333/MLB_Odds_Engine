# consensus_audit_report.py

import os
import json
from utils import normalize_label, classify_market_segment
from core.market_pricer import implied_prob

# === Settings ===
MARKET_ODDS_FOLDER = "data/market_odds"
SIM_FOLDER = "backtest/sims"

print("\n📊 Running Consensus Matching Audit...")

# === Loop over all market odds files ===
for fname in sorted(os.listdir(MARKET_ODDS_FOLDER)):
    if not fname.endswith(".json"):
        continue

    date_tag = fname.replace(".json", "")
    odds_path = os.path.join(MARKET_ODDS_FOLDER, fname)
    sim_dir = os.path.join(SIM_FOLDER, date_tag)

    if not os.path.exists(sim_dir):
        print(f"❌ Missing sims for {date_tag} — skipping")
        continue

    with open(odds_path) as f:
        odds_data = json.load(f)

    for game_id in odds_data:
        sim_path = os.path.join(sim_dir, f"{game_id}.json")
        if not os.path.exists(sim_path):
            print(f"   ❌ No sim for {game_id}")
            continue

        with open(sim_path) as sf:
            sim_data = json.load(sf)

        print(f"\n——————————————\n{game_id}")

        # === Pull all sim entries
        sim_lines = sim_data.get("markets", [])
        sim_lookup = {
            (normalize_label(e["side"]), e["market"]): e
            for e in sim_lines
        }

        for market_key in ["spreads", "totals", "h2h"]:
            full_market = odds_data[game_id].get(market_key, {})
            for label in full_market:
                norm_label = normalize_label(label)

                matched = None
                for (lbl, mkt) in sim_lookup:
                    if lbl == norm_label and classify_market_segment(mkt) == classify_market_segment(market_key):
                        matched = sim_lookup[(lbl, mkt)]
                        break

                if matched:
                    print(f"✅ {market_key:8} | {label:15} → Sim P: {matched['sim_prob']:.4f} | Segment: {classify_market_segment(mkt)}")
                else:
                    print(f"❌ {market_key:8} | {label:15} → NO MATCH IN SIM — fallback likely")
