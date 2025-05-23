import os, sys, json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import os
import numpy as np
from core.market_pricer import implied_prob

ODDS_PATH = "data/market_odds/2025-05-20.json"
BACKUP_PATH = ODDS_PATH.replace(".json", ".original.json")
THRESHOLD = 0.005  # Only rewrite if delta > 0.5%

def devig_pair(prob1, prob2):
    total = prob1 + prob2
    if total == 0:
        return None, None
    return prob1 / total, prob2 / total

def fix_game_h2h(game_id, game_data):
    updated = False
    for market_key in game_data:
        if not market_key.startswith("h2h") or "_source" in market_key:
            continue

        market = game_data.get(market_key, {})
        source = game_data.get(f"{market_key}_source", {})
        if not market or not source:
            continue

        teams = list(market.keys())
        if len(teams) != 2:
            continue
        team1, team2 = teams

        team1_probs, team2_probs = [], []

        for book in source.get(team1, {}):
            if book not in source.get(team2, {}):
                continue
            try:
                p1 = implied_prob(source[team1][book])
                p2 = implied_prob(source[team2][book])
                nv1, nv2 = devig_pair(p1, p2)
                if nv1 is not None and nv2 is not None:
                    team1_probs.append(nv1)
                    team2_probs.append(nv2)
            except:
                continue

        if not team1_probs or not team2_probs:
            continue

        avg1 = round(np.mean(team1_probs), 6)
        avg2 = round(np.mean(team2_probs), 6)

        stored1 = round(market[team1].get("consensus_prob", -1), 6)
        stored2 = round(market[team2].get("consensus_prob", -1), 6)

        delta1 = abs(avg1 - stored1)
        delta2 = abs(avg2 - stored2)

        if delta1 > THRESHOLD or delta2 > THRESHOLD:
            game_data[market_key][team1]["consensus_prob"] = avg1
            game_data[market_key][team1]["consensus_odds"] = round(100 / avg1 - 100, 2) if avg1 >= 0.5 else round(-100 / (1 / avg1 - 1), 2)

            game_data[market_key][team2]["consensus_prob"] = avg2
            game_data[market_key][team2]["consensus_odds"] = round(100 / avg2 - 100, 2) if avg2 >= 0.5 else round(-100 / (1 / avg2 - 1), 2)

            print(f"✅ Fixed {game_id} | {market_key}")
            print(f"   {team1}: {stored1} → {avg1}")
            print(f"   {team2}: {stored2} → {avg2}\n")
            updated = True

    return updated


def main():
    print(f"📂 Loading odds file: {ODDS_PATH}")
    with open(ODDS_PATH, "r") as f:
        data = json.load(f)

    print(f"📦 Backing up original to: {BACKUP_PATH}")
    with open(BACKUP_PATH, "w") as f:
        json.dump(data, f, indent=2)

    fixes = 0
    for gid, game_data in data.items():
        if fix_game_h2h(gid, game_data):
            fixes += 1

    if fixes:
        with open(ODDS_PATH, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n💾 Saved fixed file with {fixes} games updated.")
    else:
        print("✅ No changes needed — all consensus probs look clean.")

if __name__ == "__main__":
    main()
