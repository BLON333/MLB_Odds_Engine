#!/usr/bin/env python
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

import json
import argparse
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from odds_fetcher import fetch_market_odds_from_api
from log_betting_evals import expand_snapshot_rows_with_kelly
from utils import (
    convert_full_team_spread_to_odds_key,
    normalize_to_abbreviation,
    get_market_entry_with_alternate_fallback,
)

from snapshot_core import (
    build_argument_parser,
    send_bet_snapshot_to_discord,
    load_simulations,
    build_snapshot_rows,
    compare_and_flag_new_rows,
    build_display_block,
    format_for_display,
    format_table_with_highlights,
    export_market_snapshots,
)

DEBUG_LOG = []

SNAPSHOT_DIR = "backtest"


def make_snapshot_path(date_key: str) -> str:
    """Return the per-date snapshot path."""
    safe = date_key.replace(",", "_")
    return os.path.join(SNAPSHOT_DIR, f"last_live_snapshot_{safe}.json")


def make_market_snapshot_paths(date_key: str) -> dict:
    """Return per-date market snapshot paths."""
    safe = date_key.replace(",", "_")
    return {
        "spreads": os.path.join(SNAPSHOT_DIR, f"last_spreads_snapshot_{safe}.json"),
        "h2h": os.path.join(SNAPSHOT_DIR, f"last_h2h_snapshot_{safe}.json"),
        "totals": os.path.join(SNAPSHOT_DIR, f"last_totals_snapshot_{safe}.json"),
    }

load_dotenv()

WEBHOOKS = {
    "h2h": os.getenv("DISCORD_H2H_WEBHOOK_URL"),
    "spreads": os.getenv("DISCORD_SPREADS_WEBHOOK_URL"),
    "totals": os.getenv("DISCORD_TOTALS_WEBHOOK_URL"),
}



def main():
    parser = build_argument_parser(
        "Generate live market snapshots from latest sims",
        output_discord_default=True,
        include_stake_mode=True,
        include_debug_json=True,
    )
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

    # Expand rows and apply EV/stake filtering
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

    rows, snapshot_next = compare_and_flag_new_rows(rows, snapshot_path)

    df = format_for_display(rows, include_movement=args.diff_highlight)

    df_export = df.drop(columns=[c for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"] if c in df.columns])
    export_market_snapshots(df_export, market_snapshot_paths)

    if args.output_discord:
        print(df.head())
        for mkt, webhook in WEBHOOKS.items():
            if not webhook:
                continue
            subset = df[df["Market"].str.lower().str.startswith(mkt.lower(), na=False)]
            # Ensure main and alternate lines stay together.  We never split on
            # the "Market Class" column for live snapshots.
            print(f"📡 Evaluating snapshot for: {mkt} → {subset.shape[0]} rows")
            if subset.empty:
                print(f"⚠️ No bets for {mkt}")
                continue
            send_bet_snapshot_to_discord(subset, mkt, webhook)
    else:
        if args.diff_highlight:
            print(format_table_with_highlights(rows))
        else:
            print(df.to_string(index=False))

    # Save snapshot for next run
    os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
    with open(snapshot_path, "w") as f:
        json.dump(snapshot_next, f, indent=2)

    if args.debug_json:
        try:
            with open(args.debug_json, "w") as fh:
                json.dump(DEBUG_LOG, fh, indent=2)
            print(f"📝 Debug info written to {args.debug_json}")
        except Exception as e:
            print(f"❌ Failed to write debug file: {e}")


if __name__ == "__main__":
    main()
