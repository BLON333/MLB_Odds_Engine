#!/usr/bin/env python
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""Dispatch FV drop snapshot (market probability increases) from unified snapshot JSON."""

import json
from utils import safe_load_json
import argparse
from typing import List
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger
from core.should_log_bet import should_log_bet
from core.market_eval_tracker import load_tracker
from cli.log_betting_evals import (
    load_existing_stakes,
    load_existing_theme_stakes,
    load_persisted_theme_stakes,
    write_to_csv,
    send_discord_notification,
)
from collections import defaultdict
import copy

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_FV_DROP_WEBHOOK_URL"))

MARKET_EVAL_TRACKER = load_tracker()
MARKET_EVAL_TRACKER_BEFORE_UPDATE = copy.deepcopy(MARKET_EVAL_TRACKER)


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
    """Return df filtered to the given book keys."""
    if not books or "Book" not in df.columns:
        return df
    clean_books = [b.strip() for b in books if b.strip()]
    if not clean_books:
        return df
    return df[df["Book"].isin(clean_books)]


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


def log_and_notify_if_qualified(row, existing, session_exposure, existing_theme_stakes):
    """Run staking, logging, and notification for a snapshot row."""
    staked_row = should_log_bet(
        row,
        existing_theme_stakes,
        eval_tracker=MARKET_EVAL_TRACKER,
        reference_tracker=MARKET_EVAL_TRACKER_BEFORE_UPDATE,
    )
    if staked_row:
        result = write_to_csv(
            staked_row,
            "logs/market_evals.csv",
            existing,
            session_exposure,
            existing_theme_stakes,
        )
        if result:
            send_discord_notification(result)
            return result
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch FV drop snapshot (market probability increases)")
    parser.add_argument("--snapshot-path", default=None, help="Path to unified snapshot JSON")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument("--diff-highlight", action="store_true")
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
        return

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





    df = format_for_display(rows, include_movement=args.diff_highlight)
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

    df_all_books = df.copy()

    # ✅ Hardcoded sportsbook filter for FV Drop
    allowed_books = ["betonlineag", "bovada"]

    df_allowed = filter_by_books(df, allowed_books)

    # === Load existing exposure + trackers ===
    existing = load_existing_stakes("logs/market_evals.csv")
    existing_theme_stakes = load_existing_theme_stakes("logs/market_evals.csv")
    persisted = load_persisted_theme_stakes()
    if persisted:
        existing_theme_stakes.update(persisted)
    session_exposure = defaultdict(set)

    logged_indices = []
    for idx in df_all_books.index:
        row = rows[idx]
        result = log_and_notify_if_qualified(
            row,
            existing,
            session_exposure,
            existing_theme_stakes,
        )
        if result:
            logged_indices.append(idx)

    df_logged = df_all_books.loc[logged_indices] if logged_indices else pd.DataFrame()

    if df_allowed.empty and df_logged.empty:
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

        if webhook_all and not df_logged.empty:
            send_bet_snapshot_to_discord(df_logged, "FV Drop (All Books)", webhook_all)
        elif webhook_all:
            logger.warning("⚠️ No FV Drop rows for all books")
    else:
        print(df_allowed.to_string(index=False))


if __name__ == "__main__":
    main()
