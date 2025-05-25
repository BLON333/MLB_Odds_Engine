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
)
# send_bet_snapshot_to_discord was previously provided by a separate
# module (discord_snapshots.py) which has since been removed. The
# function now lives in live_snapshot_generator.py, so import it from
# there to restore Discord image functionality.
from live_snapshot_generator import send_bet_snapshot_to_discord

load_dotenv()

CUSTOM_BOOKMAKERS = ["pinnacle", "betonlineag", "fanduel", "bovada"]
odds_fetcher.BOOKMAKERS = CUSTOM_BOOKMAKERS
print("📊 Using custom bookmakers:", odds_fetcher.BOOKMAKERS)

SNAPSHOT_DIR = "backtest"


def make_snapshot_path(date_key: str) -> str:
    safe = date_key.replace(",", "_")
    return os.path.join(SNAPSHOT_DIR, f"personal_snapshot_{safe}.json")


def make_market_snapshot_paths(date_key: str) -> dict:
    safe = date_key.replace(",", "_")
    return {"all": os.path.join(SNAPSHOT_DIR, f"last_personal_snapshot_{safe}.json")}

PERSONAL_WEBHOOK_URL = "https://discord.com/api/webhooks/1368408687559053332/2uhUud0fgdonV0xdIDorXX02HGQ1AWsEO_lQHMDqWLh-4THpMEe3mXb7u88JSvssSRtM"

DEBUG_LOG = []

def main():
    parser = argparse.ArgumentParser(description="Generate personal live market snapshot")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"), help="Comma-separated list of dates")
    parser.add_argument("--min-ev", type=float, default=0.05)
    parser.add_argument("--max-ev", type=float, default=0.20)
    parser.add_argument("--output-discord", dest="output_discord", action="store_true")
    parser.add_argument("--no-output-discord", dest="output_discord", action="store_false")
    parser.add_argument("--diff-highlight", action="store_true", help="Highlight new rows and odds movements")
    parser.add_argument("--reset-snapshot", action="store_true", help="Clear stored snapshot before running")
    parser.set_defaults(output_discord=True)
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

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, DEBUG_LOG))

    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )

    # Filter rows within EV bounds and sort by EV descending
    rows = [
        r for r in rows
        if args.min_ev * 100 <= r.get("ev_percent", 0) <= args.max_ev * 100
    ]
    rows.sort(key=lambda r: r.get("ev_percent", 0), reverse=True)

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
        send_bet_snapshot_to_discord(df, "MLB Markets", PERSONAL_WEBHOOK_URL)
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
