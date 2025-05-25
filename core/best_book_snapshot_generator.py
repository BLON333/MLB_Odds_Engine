#!/usr/bin/env python
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

import json
import argparse
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

import odds_fetcher
from odds_fetcher import fetch_market_odds_from_api
from log_betting_evals import expand_snapshot_rows_with_kelly
from live_snapshot_generator import (
    load_simulations,
    build_snapshot_rows,
    compare_and_flag_new_rows,
    format_for_display,
    format_table_with_highlights,
    export_market_snapshots,
    send_bet_snapshot_to_discord,
)

load_dotenv()

# Sportsbooks considered popular for best-book selection
POPULAR_BOOKS = [
    "fanduel",
    "draftkings",
    "betmgm",
    "williamhill_us",
    "espnbet",
    "hardrockbet",
    "fliff",
    "mybookieag",
    "lowvig",
    "betonlineag",
    "betrivers",
    "fanatics",
    "pinnacle",
]

SNAPSHOT_DIR = "backtest"


def make_snapshot_path(date_key: str) -> str:
    safe = date_key.replace(",", "_")
    return os.path.join(SNAPSHOT_DIR, f"best_book_snapshot_{safe}.json")


def make_market_snapshot_paths(date_key: str) -> dict:
    safe = date_key.replace(",", "_")
    return {"main": os.path.join(SNAPSHOT_DIR, f"last_best_book_snapshot_{safe}.json")}


# Utility --------------------------------------------------------------------
from typing import List, Dict
from core.market_pricer import decimal_odds


def select_best_book_rows(rows: List[dict], preferred_books: List[str] | None = None) -> List[dict]:
    """Return best-priced row per (game_id, market, side)."""
    groups: Dict[tuple, dict] = {}
    fallbacks: Dict[tuple, dict] = {}

    for r in rows:
        key = (r.get("game_id"), r.get("market"), r.get("side"))
        odds = r.get("market_odds")
        try:
            dec = decimal_odds(float(odds))
        except Exception:
            dec = -1.0

        if preferred_books and r.get("best_book") not in preferred_books:
            current = fallbacks.get(key)
            if not current or dec > decimal_odds(float(current.get("market_odds", 0))):
                fallbacks[key] = r
            continue

        current = groups.get(key)
        if not current or dec > decimal_odds(float(current.get("market_odds", 0))):
            groups[key] = r

    for key, fb in fallbacks.items():
        groups.setdefault(key, fb)

    return list(groups.values())


# Main -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate best-book market snapshot")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"), help="Comma-separated list of dates")
    parser.add_argument("--min-ev", type=float, default=0.05)
    parser.add_argument("--max-ev", type=float, default=0.20)
    parser.add_argument("--output-discord", dest="output_discord", action="store_true")
    parser.add_argument("--no-output-discord", dest="output_discord", action="store_false")
    parser.add_argument("--diff-highlight", action="store_true", help="Highlight new rows and odds movements")
    parser.add_argument("--reset-snapshot", action="store_true", help="Clear stored snapshot before running")
    parser.set_defaults(output_discord=False)
    args = parser.parse_args()

    snapshot_path = make_snapshot_path(args.date)
    market_snapshot_paths = make_market_snapshot_paths(args.date)

    if args.reset_snapshot and os.path.exists(snapshot_path):
        os.remove(snapshot_path)

    date_list = [d.strip() for d in str(args.date).split(',') if d.strip()]
    all_rows = []
    for date_str in date_list:
        sim_dir = os.path.join("backtest", "sims", date_str)
        sims = load_simulations(sim_dir)
        if not sims:
            print(f"❌ No simulation files found for {date_str}.")
            continue

        odds = fetch_market_odds_from_api(list(sims.keys()))
        if not odds:
            print(f"❌ Failed to fetch market odds for {date_str}.")
            continue

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, []))

    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )

    rows = select_best_book_rows(rows, POPULAR_BOOKS)

    if not rows:
        print("⚠️ No qualifying bets found.")
        return

    if args.diff_highlight:
        rows, snapshot_next = compare_and_flag_new_rows(rows, snapshot_path)
    else:
        snapshot_next = {}
        for r in rows:
            fair_odds = r.get("blended_fv")
            market_odds = r.get("market_odds")
            ev_pct = r.get("ev_percent")
            if fair_odds is None or ev_pct is None or market_odds is None:
                continue
            game_id = r.get("game_id", "")
            book = r.get("best_book", "")
            key = f"{game_id}:{r['market']}:{r['side']}:{book}"
            snapshot_next[key] = {
                "fair_odds": fair_odds,
                "market_odds": market_odds,
                "ev_percent": ev_pct,
            }

    df = format_for_display(rows, include_movement=args.diff_highlight)

    df_export = df.drop(columns=[c for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"] if c in df.columns])
    export_market_snapshots(df_export, market_snapshot_paths)

    if args.output_discord:
        send_bet_snapshot_to_discord(df, "Best Book", os.getenv("DISCORD_BEST_BOOK_WEBHOOK_URL", ""))
    else:
        if args.diff_highlight:
            print(format_table_with_highlights(rows))
        else:
            print(df.to_string(index=False))

    os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
    with open(snapshot_path, "w") as f:
        json.dump(snapshot_next, f, indent=2)


if __name__ == "__main__":
    main()
