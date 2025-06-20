import csv
import os
import sys
from core.bootstrap import *  # noqa
from core.config import DEBUG_MODE, VERBOSE_MODE
import argparse
from datetime import datetime, timedelta

from core.logger import get_logger
logger = get_logger(__name__)

from core.utils import (
    format_market_key,
    TEAM_ABBR,
    safe_load_json,
    normalize_line_label,
    normalize_to_abbreviation,
    canonical_game_id,
)
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


def find_closing_label(side, market_key, market_data, threshold=1.0):
    """Return best matching label and signed line shift."""
    lookup = normalize_to_abbreviation(side)

    if lookup in market_data:
        return lookup, 0.0

    prefix, val = normalize_line_label(lookup)
    if val is None:
        return None, None

    best_key = None
    best_signed = None
    best_abs = None

    for label in market_data.keys():
        p2, v2 = normalize_line_label(label)
        if p2 != prefix or v2 is None:
            continue
        if market_key.startswith("spreads") and ((val >= 0) != (v2 >= 0)):
            continue
        diff = v2 - val
        adiff = abs(diff)
        if best_abs is None or adiff < best_abs:
            best_abs = adiff
            best_signed = diff
            best_key = label

    if best_abs is not None and best_abs <= threshold:
        return best_key, best_signed

    return None, None

def update_clv(csv_path, odds_json_path, target_date):
    with open(csv_path, "r", newline="") as f:
        rows = list(csv.DictReader(f))

    closing_odds = safe_load_json(odds_json_path)
    if not isinstance(closing_odds, dict):
        logger.warning(
            "⚠️ Failed to load closing odds from %s. CLV columns will be blank.",
            odds_json_path,
        )
        closing_odds = {}

    json_has_time = any("-T" in k for k in closing_odds)
    csv_has_time = any("-T" in row.get("game_id", "") for row in rows)

    if json_has_time and not csv_has_time:
        logger.warning(
            "⚠️ Detected possible mismatch: JSON has -T time in game_ids, CSV does not. CLV matching may fail."
        )

    updated_rows = []
    for row in rows:
        gid = canonical_game_id(row.get("game_id", ""))
        row_date = gid[:10] if gid else ""

        # ⛔ Skip if not from the target date
        if row_date != target_date:
            updated_rows.append(row)
            continue

        # 🔁 Use direct closing odds match, otherwise fallback to base ID
        if gid in closing_odds:
            game_data = closing_odds[gid]
        else:
            base_id = gid.split("-T")[0]
            matches = [k for k in closing_odds if k.startswith(base_id)]
            game_data = closing_odds[matches[0]] if len(matches) == 1 else None

        if not gid or not game_data:
            row["closing_odds"] = ""
            row["clv_percent"] = ""
            row["model_clv_percent"] = ""
            row["clv_class"] = ""
            updated_rows.append(row)
            continue

        market_key, side_key = format_market_key(row)
        market_data = game_data.get(market_key, {})

        match_key, shift = find_closing_label(side_key, market_key, market_data)
        line_info = market_data.get(match_key) if match_key else None

        if isinstance(line_info, dict):
            closing_line = line_info.get("price")
            consensus_fv = line_info.get("consensus_odds")
        else:
            closing_line = line_info
            consensus_fv = None

        row["line_shift"] = round(shift, 1) if shift not in (None, "") else ""

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

    print(
        f"✅ Updated {csv_path} with closing_odds, clv_percent, model_clv_percent, clv_class, and line_shift"
    )

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