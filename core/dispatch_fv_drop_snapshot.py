#!/usr/bin/env python
from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
from core.bootstrap import *  # noqa

"""Dispatch FV drop snapshot (market probability increases) from unified snapshot JSON."""

import json
from core.utils import safe_load_json
import argparse
from typing import List
import re
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the project root .env file
load_dotenv()

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger
from core.should_log_bet import MAX_POSITIVE_ODDS, MIN_NEGATIVE_ODDS
from utils.book_helpers import parse_american_odds, filter_by_odds, ensure_side
from core.book_whitelist import ALLOWED_BOOKS

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_FV_DROP_WEBHOOK_URL"))


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
        sys.exit(1)
    for r in rows:
        ensure_side(r)
    return rows


def filter_by_date(rows: list, date_str: str | None) -> list:
    if not date_str:
        return rows
    return [r for r in rows if str(r.get("snapshot_for_date")) == date_str]


def filter_by_books(df: pd.DataFrame, books: List[str] | None) -> pd.DataFrame:
    """Return df filtered to the given book keys."""
    if not books or "Book" not in df.columns:
        return df
    clean_books = [b.strip() for b in books if b.strip()]
    if not clean_books:
        return df
    return df[df["Book"].isin(clean_books)]




def filter_main_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Return df filtered to only main market lines."""
    if "Market Class" in df.columns:
        return df[df["Market Class"] == "Main"]
    return df


def is_market_prob_increasing(val: str) -> bool:
    """Return True if val contains an upward market probability shift."""
    if not isinstance(val, str) or "→" not in val:
        return False
    try:
        left, right = val.split("→")
        left = float(left.strip().replace("%", ""))
        right = float(right.strip().replace("%", ""))
        return right > left
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch FV drop snapshot (market probability increases)")
    parser.add_argument("--snapshot-path", default=None, help="Path to unified snapshot JSON")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument(
        "--books",
        default=os.getenv("FV_DROP_BOOKS"),
        help="Comma-separated book keys to include",
    )
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
        sys.exit(1)

    rows = load_rows(path)
    for r in rows:
        if "book" not in r and "best_book" in r:
            r["book"] = r["best_book"]


    # ✅ No role/movement filter — allow full snapshot set
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





    df = format_for_display(rows, include_movement=True)
    if "sim_prob_display" in df.columns:
        df["Sim %"] = df["sim_prob_display"]
    if "mkt_prob_display" in df.columns:
        df["Mkt %"] = df["mkt_prob_display"]
    if "odds_display" in df.columns:
        df["Odds"] = df["odds_display"]
    if "fv_display" in df.columns:
        df["FV"] = df["fv_display"]

    # ✅ Filter to only show rows where market probability increased
    if "Mkt %" in df.columns:
        df = df[df["Mkt %"].apply(is_market_prob_increasing)]

    # The all-books snapshot is disabled by default to avoid exposing
    # disallowed books in production. Set ENABLE_ALL_BOOKS_OUTPUT=true to
    # enable this debug output.
    enable_all_books = os.getenv("ENABLE_ALL_BOOKS_OUTPUT", "false").lower() == "true"
    df_all_books = df.copy() if enable_all_books else pd.DataFrame()

    # ✅ Hardcoded sportsbook filter for FV Drop (aligned with POPULAR_BOOKS)
    allowed_books = list(ALLOWED_BOOKS)

    df_allowed = filter_by_books(df, allowed_books)
    df_allowed = filter_main_lines(df_allowed)
    df_allowed = filter_by_odds(
        df_allowed,
        MIN_NEGATIVE_ODDS,
        MAX_POSITIVE_ODDS,
    )

    if df_allowed.empty and (not enable_all_books or df_all_books.empty):
        logger.info("⚠️ No qualifying FV Drop rows with market movement to display.")
        return

    if args.output_discord:
        webhook_allowed = os.getenv("DISCORD_FV_DROP_WEBHOOK_URL")
        webhook_all = os.getenv("DISCORD_FV_DROP_ALL_WEBHOOK_URL")

        if webhook_allowed and not df_allowed.empty:
            send_bet_snapshot_to_discord(df_allowed, "FV Drop", webhook_allowed)
        elif webhook_allowed:
            logger.warning("⚠️ No FV Drop rows for allowed books")
        else:
            logger.error("❌ DISCORD_FV_DROP_WEBHOOK_URL not configured")

        if enable_all_books:
            if webhook_all and not df_all_books.empty:
                send_bet_snapshot_to_discord(df_all_books, "FV Drop (All Books)", webhook_all)
            elif webhook_all:
                logger.warning("⚠️ No FV Drop rows for all books")
    else:
        print(df_allowed.to_string(index=False))


if __name__ == "__main__":
    main()
