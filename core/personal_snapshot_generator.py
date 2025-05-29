#!/usr/bin/env python
import os
import sys

import sys
if sys.version_info >= (3, 7):
    import os
    os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

import json
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from core import odds_fetcher
from core.odds_fetcher import fetch_market_odds_from_api
from cli.log_betting_evals import expand_snapshot_rows_with_kelly
from core.snapshot_core import  (
    build_argument_parser,
    load_simulations,
    build_snapshot_rows,
    compare_and_flag_new_rows,
    build_display_block,
    format_for_display,
    format_table_with_highlights,
    export_market_snapshots,
    send_bet_snapshot_to_discord,
)

load_dotenv()
from core.logger import get_logger
logger = get_logger(__name__)

CUSTOM_BOOKMAKERS = ["betonlineag", "bovada"]
odds_fetcher.BOOKMAKERS = CUSTOM_BOOKMAKERS
logger.info("📊 Using custom bookmakers: %s", odds_fetcher.BOOKMAKERS)

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
    parser = build_argument_parser(
        "Generate personal live market snapshot",
        output_discord_default=True,
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
            odds = fetch_market_odds_from_api(
                list(sims.keys()), filter_bookmakers=odds_fetcher.BOOKMAKERS
            )
        if not odds:
            logger.warning("❌ Failed to fetch market odds for %s.", date_str)
            continue

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, DEBUG_LOG))

    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )

    # Reload market eval tracker to ensure latest bet logs are included
    from core.market_eval_tracker import load_tracker
    from core.snapshot_core import MARKET_EVAL_TRACKER, MARKET_EVAL_TRACKER_BEFORE_UPDATE
    import copy

    MARKET_EVAL_TRACKER.clear()
    MARKET_EVAL_TRACKER.update(load_tracker())
    MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()
    MARKET_EVAL_TRACKER_BEFORE_UPDATE.update(copy.deepcopy(MARKET_EVAL_TRACKER))

    flagged_rows, snapshot_next = compare_and_flag_new_rows(
        rows,
        snapshot_path,
        prior_snapshot=market_snapshot_paths.get("all"),
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

    df = format_for_display(rows, include_movement=args.diff_highlight)

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
