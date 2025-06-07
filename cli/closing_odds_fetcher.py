import os
import csv
import json
import argparse
from dotenv import load_dotenv

from core.odds_fetcher import (
    fetch_consensus_for_single_game,
    american_to_prob,
)
from core.market_pricer import to_american_odds
from utils import normalize_line_label

load_dotenv()
from core.logger import get_logger
logger = get_logger(__name__)



def load_game_ids_from_csv(csv_path):
    game_ids = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_ids.add(row["game_id"])
    return sorted(game_ids)


def attach_consensus_probs(consensus_odds):
    """Compute devigged probabilities and fair odds for each side."""
    for mkey, market in consensus_odds.items():
        if not isinstance(market, dict):
            continue

        groups = {}
        for label, info in market.items():
            if not isinstance(info, dict):
                continue

            price = info.get("price")
            prefix, point = normalize_line_label(label)

            if "team_totals" in mkey:
                group_key = (prefix.upper(), point)
            elif "totals" in mkey:
                group_key = point
            elif mkey.startswith("spreads") or mkey.startswith("alternate_spreads"):
                group_key = abs(point) if point is not None else None
            else:
                group_key = None

            groups.setdefault(group_key, []).append((label, price))

        for entries in groups.values():
            if len(entries) != 2:
                continue

            (l1, p1), (l2, p2) = entries
            try:
                imp1 = american_to_prob(p1)
                imp2 = american_to_prob(p2)
                total = imp1 + imp2
                if total <= 0:
                    continue
                prob1 = round(imp1 / total, 6)
                prob2 = round(imp2 / total, 6)
            except Exception:
                continue

            market[l1]["consensus_prob"] = prob1
            market[l1]["consensus_odds"] = round(to_american_odds(prob1), 2)
            market[l2]["consensus_prob"] = prob2
            market[l2]["consensus_odds"] = round(to_american_odds(prob2), 2)


def attach_implied_probs(consensus_odds):
    """Attach implied probability for each side based on its price."""
    for market in consensus_odds.values():
        if not isinstance(market, dict):
            continue
        for info in market.values():
            if not isinstance(info, dict):
                continue
            price = info.get("price")
            if price is None:
                continue
            info["implied_prob"] = american_to_prob(price)




def save_output_json(data, date_str):
    path = f"data/closing_odds/{date_str}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved closing odds to: {path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-path", default="logs/market_evals.csv", help="Path to your market_evals.csv file")
    args = parser.parse_args()

    game_ids = load_game_ids_from_csv(args.log_path)

    results = {}
    for game_id in game_ids:
        consensus_odds = fetch_consensus_for_single_game(game_id)
        if not consensus_odds:
            print(f"❌ No odds found for {game_id}")
            continue

        attach_consensus_probs(consensus_odds)
        attach_implied_probs(consensus_odds)
        results[game_id] = consensus_odds
        print(f"✅ Logged closing odds for {game_id}")

    if results:
        date = list(results.keys())[0][:10]
        save_output_json(results, date)
