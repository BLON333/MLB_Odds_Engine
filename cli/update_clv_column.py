import csv
import json
import argparse
from datetime import datetime, timedelta
import os
import sys

# 🔁 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.logger import get_logger
logger = get_logger(__name__)

from utils import format_market_key, TEAM_ABBR
from core.odds_fetcher import american_to_prob  # ✅ Corrected path

def normalize_team_name(abbr):
    return TEAM_ABBR.get(abbr.strip(), abbr)

def classify_clv(clv_percent):
    try:
        clv = float(clv_percent)
        if clv > 2:
            return "positive"
        elif clv < -2:
            return "negative"
        else:
            return "neutral"
    except:
        return ""

def update_clv(csv_path, odds_json_path, target_date):
    with open(csv_path, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    with open(odds_json_path, "r") as f:
        closing_odds = json.load(f)

    updated_rows = []
    for row in rows:
        gid = row.get("game_id")
        row_date = gid[:10] if gid else ""

        # ⛔ Skip if not from the target date
        if row_date != target_date:
            updated_rows.append(row)
            continue

        if not gid or gid not in closing_odds:
            row["closing_odds"] = ""
            row["clv_percent"] = ""
            row["model_clv_percent"] = ""
            row["clv_class"] = ""
            updated_rows.append(row)
            continue

        market_key, side_key = format_market_key(row)
        market_data = closing_odds[gid].get(market_key, {})
        line_info = market_data.get(side_key)

        if isinstance(line_info, dict):
            closing_line = line_info.get("price")
            consensus_fv = line_info.get("consensus_odds")
        else:
            closing_line = line_info
            consensus_fv = None

        row["closing_odds"] = closing_line if closing_line is not None else ""

        bet_prob = american_to_prob(row.get("market_odds"))
        closing_prob = american_to_prob(closing_line) if closing_line is not None else None

        if bet_prob is not None and closing_prob is not None:
            clv = round((closing_prob - bet_prob) * 100, 2)
            row["clv_percent"] = clv
            row["clv_class"] = classify_clv(clv)
        else:
            row["clv_percent"] = ""
            row["clv_class"] = ""

        try:
            model_fv = float(row.get("fair_odds"))
            if model_fv and consensus_fv:
                model_clv = round((model_fv / consensus_fv - 1) * 100, 2)
                row["model_clv_percent"] = model_clv
            else:
                row["model_clv_percent"] = ""
        except:
            row["model_clv_percent"] = ""

        updated_rows.append(row)

    fieldnames = list(updated_rows[0].keys())
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    print(f"✅ Updated {csv_path} with closing_odds, clv_percent, model_clv_percent, and clv_class")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update CLV and closing odds in market_evals.csv")
    parser.add_argument("--date", help="Target date in YYYY-MM-DD format (default: yesterday)")
    parser.add_argument("--csv", default="logs/market_evals.csv", help="Path to market_evals.csv")
    args = parser.parse_args()

    if args.date:
        target_date = args.date
    else:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    closing_odds_file = os.path.join("data", "closing_odds", f"{target_date}.json")
    update_clv(args.csv, closing_odds_file, target_date)
