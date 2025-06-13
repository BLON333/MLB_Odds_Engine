#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys

import sys
if sys.version_info >= (3, 7):
    import os
    os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

import json
import argparse
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from core.odds_fetcher import fetch_market_odds_from_api
from cli.log_betting_evals import expand_snapshot_rows_with_kelly
from utils import (
    convert_full_team_spread_to_odds_key,
    normalize_to_abbreviation,
    get_market_entry_with_alternate_fallback,
)

from core.snapshot_core import (
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
from core.logger import get_logger
logger = get_logger(__name__)

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
    parser.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    args = parser.parse_args()

    snapshot_path = make_snapshot_path(args.date)
    market_snapshot_paths = make_market_snapshot_paths(args.date)

    if args.reset_snapshot and os.path.exists(snapshot_path):
        os.remove(snapshot_path)

    date_list = [d.strip() for d in str(args.date).split(',') if d.strip()]
    all_rows = []

    odds_cache = None
    if args.odds_path:
        try:
            with open(args.odds_path) as fh:
                odds_cache = json.load(fh)
            logger.info("📥 Loaded odds from %s", args.odds_path)
        except Exception as e:
            logger.error("❌ Failed to load odds file %s: %s", args.odds_path, e)

    for date_str in date_list:
        sim_dir = os.path.join("backtest", "sims", date_str)
        sims = load_simulations(sim_dir)
        if not sims:
            logger.warning("❌ No simulation files found for %s.", date_str)
            continue

        if odds_cache is not None:
            odds = {gid: odds_cache.get(gid) for gid in sims.keys()}
        else:
            odds = fetch_market_odds_from_api(list(sims.keys()))
        if not odds:
            logger.warning("❌ Failed to fetch market odds for %s.", date_str)
            continue

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, DEBUG_LOG))

    # Expand rows and apply EV/stake filtering
    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )

    # Reload market eval tracker to compare against latest evaluations
    from core.market_eval_tracker import load_tracker
    from core.snapshot_core import MARKET_EVAL_TRACKER

    MARKET_EVAL_TRACKER.clear()
    MARKET_EVAL_TRACKER.update(load_tracker())

    prior_snapshot_data = {}
    for p in market_snapshot_paths.values():
        try:
            with open(p) as fh:
                prior_snapshot_data.update(json.load(fh))
        except Exception:
            pass

    flagged_rows, snapshot_next = compare_and_flag_new_rows(
        rows,
        snapshot_path,
        prior_snapshot=prior_snapshot_data,
    )

    # Filter rows within EV bounds and sort by EV descending
    rows = [
        r for r in flagged_rows
        if args.min_ev * 100 <= r.get("ev_percent", 0) <= args.max_ev * 100
    ]
    rows.sort(key=lambda r: r.get("ev_percent", 0), reverse=True)

    if not rows:
        logger.warning("⚠️ No qualifying bets found.")
        return

    df = format_for_display(rows, include_movement=True)

    df_all_export = format_for_display(flagged_rows, include_movement=False)
    df_export = df_all_export.drop(
        columns=[
            c
            for c in [
                "odds_movement",
                "fv_movement",
                "ev_movement",
                "stake_movement",
                "sim_movement",
                "mkt_movement",
                "is_new",
            ]
            if c in df_all_export.columns
        ]
    )
    export_market_snapshots(df_export, market_snapshot_paths)

    if args.output_discord:
        logger.debug(df.head())
        for mkt, webhook in WEBHOOKS.items():
            if not webhook:
                continue
            subset = df[df["Market"].str.lower().str.startswith(mkt.lower(), na=False)]
            # Ensure main and alternate lines stay together.  We never split on
            # the "Market Class" column for live snapshots.
            logger.info("📡 Evaluating snapshot for: %s → %s rows", mkt, subset.shape[0])
            if subset.empty:
                logger.warning("⚠️ No bets for %s", mkt)
                continue
            send_bet_snapshot_to_discord(subset, mkt, webhook)
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
            logger.info("📝 Debug info written to %s", args.debug_json)
        except Exception as e:
            logger.error("❌ Failed to write debug file: %s", e)


if __name__ == "__main__":
    main()
