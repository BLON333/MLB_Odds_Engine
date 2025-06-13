import argparse
import csv
import json
import os
from datetime import datetime

from utils import canonical_game_id, parse_game_id

FIELDNAMES = [
    "Date",
    "Time",
    "Start Time (ISO)",
    "Matchup",
    "game_id",
    "market",
    "market_class",
    "side",
    "lookup_side",
    "sim_prob",
    "fair_odds",
    "market_prob",
    "market_fv",
    "consensus_prob",
    "pricing_method",
    "books_used",
    "model_edge",
    "market_odds",
    "ev_percent",
    "blended_prob",
    "blended_fv",
    "hours_to_game",
    "stake",
    "entry_type",
    "segment",
    "segment_label",
    "best_book",
    "date_simulated",
    "result",
]

def load_pending(path):
    if not os.path.exists(path):
        print(f"No pending bets file found at {path}")
        return []
    with open(path, "r") as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"Failed to parse {path}: {e}")
            return []

    if isinstance(data, dict):
        return list(data.values())

    if isinstance(data, list):
        return data

    print(f"Warning: unexpected format in {path}; expected list or dict")
    return []

def load_existing_keys(csv_path):
    keys = set()
    if not os.path.exists(csv_path):
        return keys
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = canonical_game_id(row.get("game_id", ""))
            key = (gid, row.get("market"), row.get("side"))
            keys.add(key)
    return keys

def append_rows(rows, csv_path):
    if not rows:
        return 0
    is_new = not os.path.exists(csv_path)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in FIELDNAMES})
    return len(rows)

def main():
    parser = argparse.ArgumentParser(description="Replay pending bets and log them")
    parser.add_argument(
        "--json",
        default="logs/pending_bets.json",
        help="Path to pending_bets.json",
    )
    parser.add_argument("--csv", default="logs/market_evals.csv", help="Path to market_evals.csv")
    args = parser.parse_args()

    pending = load_pending(args.json)
    if not pending:
        print("No pending bets to replay.")
        return

    existing = load_existing_keys(args.csv)
    new_rows = []
    for bet in pending:
        gid = canonical_game_id(bet.get("game_id", ""))
        key = (gid, bet.get("market"), bet.get("side"))
        if key in existing:
            continue
        bet["game_id"] = gid

        parsed = parse_game_id(gid)
        bet["Date"] = parsed.get("date", "")
        bet["Matchup"] = f"{parsed['away']} @ {parsed['home']}"
        time_raw = parsed.get("time", "")
        if time_raw.startswith("T"):
            bet["Time"] = datetime.strptime(time_raw[1:], "%H%M").strftime("%-I:%M %p")
        else:
            bet["Time"] = ""

        new_rows.append(bet)
        existing.add(key)

    count = append_rows(new_rows, args.csv)
    if count:
        print(f"✅ Recovered {count} pending bets → {args.csv}")
    else:
        print("No new pending bets to append.")

if __name__ == "__main__":
    main()
