#!/usr/bin/env python
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""Dispatch personal-book snapshot from unified snapshot JSON."""

import json
from utils import safe_load_json
import argparse
from typing import List
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_PERSONAL_WEBHOOK_URL"))

PERSONAL_WEBHOOK_URL = os.getenv(
    "DISCORD_PERSONAL_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1368408687559053332/2uhUud0fgdonV0xdIDorXX02HGQ1AWsEO_lQHMDqWLh-4THpMEe3mXb7u88JSvssSRtM",
)


def latest_snapshot_path(folder="backtest") -> str | None:
    files = sorted(
        [f for f in os.listdir(folder) if f.startswith("market_snapshot_") and f.endswith(".json")],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None


def load_rows(path: str) -> list:
    rows = safe_load_json(path)
    if rows is None:
        logger.error("❌ Failed to load snapshot %s", path)
        return []
    return rows


def filter_by_date(rows: list, date_str: str | None) -> list:
    if not date_str:
        return rows
    return [r for r in rows if str(r.get("snapshot_for_date")) == date_str]


def filter_by_books(df: pd.DataFrame, books: List[str] | None) -> pd.DataFrame:
    """Return DataFrame filtered to the specified sportsbook keys."""
    if not books or "Book" not in df.columns:
        return df
    clean_books = [b.strip() for b in books if b.strip()]
    if not clean_books:
        return df
    return df[df["Book"].isin(clean_books)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch personal-book snapshot")
    parser.add_argument("--snapshot-path", default=None, help="Path to unified snapshot JSON")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument("--diff-highlight", action="store_true")
    parser.add_argument(
        "--min-ev",
        type=float,
        default=5.0,
        help="Minimum EV% required to dispatch",
    )
    parser.add_argument(
        "--max-ev",
        type=float,
        default=20.0,
        help="Maximum EV% allowed to dispatch",
    )
    args = parser.parse_args()

    # Clamp EV range to 5%-20%
    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        return

    rows = load_rows(path)
    for r in rows:
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]
    rows = [r for r in rows if "personal" in r.get("snapshot_roles", [])]
    rows = filter_by_date(rows, args.date)

    rows = [
        r
        for r in rows
        if args.min_ev <= r.get("ev_percent", 0) <= args.max_ev
    ]
    logger.info(
        "🧪 Dispatch filter: %d rows with %.1f ≤ EV%% ≤ %.1f",
        len(rows),
        args.min_ev,
        args.max_ev,
    )

    df = format_for_display(rows, include_movement=args.diff_highlight)
    allowed_books = ["pinnacle", "fanduel", "bovada", "betonlineag"]
    df = filter_by_books(df, allowed_books)
    if "sim_prob_display" in df.columns:
        df["Sim %"] = df["sim_prob_display"]
    if "mkt_prob_display" in df.columns:
        df["Mkt %"] = df["mkt_prob_display"]
    if "odds_display" in df.columns:
        df["Odds"] = df["odds_display"]
    if "fv_display" in df.columns:
        df["FV"] = df["fv_display"]
    if df.empty:
        logger.warning("⚠️ Snapshot DataFrame is empty — nothing to dispatch.")
        return

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"]

    if "Market Class" not in df.columns:
        logger.warning("⚠️ 'Market Class' column missing — cannot dispatch personal main/alt splits.")
        return

    if args.output_discord:
        webhook = PERSONAL_WEBHOOK_URL

        main_df = df[df["Market Class"] == "Main"]
        alt_df = df[df["Market Class"] == "Alt"]

        if not main_df.empty:
            logger.info("📡 Dispatching Personal Snapshot → Main Markets (%s rows)", main_df.shape[0])
            send_bet_snapshot_to_discord(main_df, "Personal (Main)", webhook)
        else:
            logger.info("⚠️ No personal bets found for Main markets")

        if not alt_df.empty:
            logger.info("📡 Dispatching Personal Snapshot → Alt Markets (%s rows)", alt_df.shape[0])
            send_bet_snapshot_to_discord(alt_df, "Personal (Alt)", webhook)
        else:
            logger.info("⚠️ No personal bets found for Alt markets")
    else:
        print(df.to_string(index=False))



if __name__ == "__main__":
    main()
