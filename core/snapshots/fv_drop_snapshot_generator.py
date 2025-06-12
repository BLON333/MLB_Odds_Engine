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
import requests

import pandas as pd

from core import odds_fetcher
from core.odds_fetcher import fetch_market_odds_from_api
from cli.log_betting_evals import expand_snapshot_rows_with_kelly
from core.snapshot_core import (
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

WEBHOOK_URL = os.getenv("DISCORD_FV_DROP_WEBHOOK_URL")

SNAPSHOT_DIR = "backtest"


def make_snapshot_path(date_key: str) -> str:
    safe = date_key.replace(",", "_")
    return os.path.join(SNAPSHOT_DIR, f"fv_drop_snapshot_{safe}.json")


def make_market_snapshot_paths(date_key: str) -> dict:
    safe = date_key.replace(",", "_")
    return {"all": os.path.join(SNAPSHOT_DIR, f"last_fv_drop_snapshot_{safe}.json")}


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


def main():
    parser = build_argument_parser(
        "Generate snapshot of bets with decreased fair value",
        output_discord_default=False,
    )
    parser.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    args = parser.parse_args()

    snapshot_path = make_snapshot_path(args.date)
    market_snapshot_paths = make_market_snapshot_paths(args.date)
    logger.debug("Using snapshot file: %s", snapshot_path)

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

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, []))

    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )
    logger.debug("Rows after expansion: %s", len(rows))

    rows = select_best_book_rows(rows, POPULAR_BOOKS)
    logger.debug("Rows after best-book selection: %s", len(rows))

    # Ensure tracker reflects latest logged evaluations
    from core.market_eval_tracker import load_tracker
    from core.snapshot_core import MARKET_EVAL_TRACKER

    MARKET_EVAL_TRACKER.clear()
    MARKET_EVAL_TRACKER.update(load_tracker())

    flagged_rows, snapshot_next = compare_and_flag_new_rows(
        rows,
        snapshot_path,
        prior_snapshot=market_snapshot_paths.get("all"),
    )
    from collections import Counter
    mv_counts = Counter(r.get("fv_movement") for r in flagged_rows)
    logger.debug("FV movement counts: %s", dict(mv_counts))

    rows = [
        r
        for r in flagged_rows
        if r.get("fv_movement") == "worse" and r.get("ev_movement") == "better"
    ]
    logger.debug("Rows with confirmed FV drop: %s", len(rows))

    # Filter rows within EV bounds and sort descending by EV percentage
    rows = [
        r for r in rows
        if args.min_ev * 100 <= r.get("ev_percent", 0) <= args.max_ev * 100
    ]
    logger.debug("Rows after EV filter: %s", len(rows))
    rows.sort(key=lambda r: r.get("ev_percent", 0), reverse=True)

    if not rows:
        logger.warning("⚠️ No bets with decreased FV found.")
        if args.output_discord:
            if WEBHOOK_URL:
                msg = (
                    f"\U0001F4C9 No bets qualified for FV drop snapshot today "
                    f"(min EV {args.min_ev*100:.1f}%)."
                )
                try:
                    requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
                    logger.info("✅ Sent no-drop message to Discord.")
                except Exception as e:
                    logger.error("❌ Failed to send no-drop message: %s", e)
            else:
                logger.warning("⚠️ Discord webhook is not configured for FV drop snapshots.")
        return

    df = format_for_display(rows, include_movement=True)

    df_all_export = format_for_display(flagged_rows, include_movement=False)
    df_all_export = df_all_export.drop(
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
    export_market_snapshots(df_all_export, market_snapshot_paths)

    if args.output_discord and WEBHOOK_URL:
        send_bet_snapshot_to_discord(df, "FV Drop", WEBHOOK_URL)
    else:
        print(df.to_string(index=False))
        if args.output_discord and not WEBHOOK_URL:
            logger.warning("⚠️ Discord webhook is not configured for FV drop snapshots.")

    os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
    with open(snapshot_path, "w") as f:
        json.dump(snapshot_next, f, indent=2)


if __name__ == "__main__":
    main()