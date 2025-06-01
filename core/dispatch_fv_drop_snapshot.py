#!/usr/bin/env python
"""Dispatch FV drop snapshot (market probability increases) from unified snapshot JSON."""

import os
import sys
import json
import argparse
from typing import List
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger

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
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception as e:
        logger.error("❌ Failed to load snapshot %s: %s", path, e)
        return []


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
        default=2.0,
        help="Minimum EV% required to dispatch",
    )
    args = parser.parse_args()

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        return

    rows = load_rows(path)

    # ✅ No role/movement filter — allow full snapshot set
    rows = filter_by_date(rows, args.date)

    rows = [r for r in rows if r.get("ev_percent", 0) >= args.min_ev]
    logger.info(
        "🧪 Dispatch filter: %d rows passed EV%% ≥ %.1f", len(rows), args.min_ev
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

    # ✅ Hardcoded sportsbook filter for FV Drop
    allowed_books = ["betonlineag", "bovada"]

    df = filter_by_books(df, allowed_books)

    if df.empty:
        logger.info("⚠️ No qualifying FV Drop rows with market movement to display.")
        return

    if args.output_discord:
        webhook = os.getenv("DISCORD_FV_DROP_WEBHOOK_URL")
        if webhook:
            send_bet_snapshot_to_discord(df, "FV Drop", webhook)
        else:
            logger.error("❌ DISCORD_FV_DROP_WEBHOOK_URL not configured")
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()