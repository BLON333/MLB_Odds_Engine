#!/usr/bin/env python
"""Dispatch best-book snapshot from unified snapshot JSON."""

import os
import sys
import json
import glob
import argparse
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug(
    "✅ Loaded webhook: %s", os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL")
)


def latest_snapshot_path() -> str | None:
    files = sorted(glob.glob(os.path.join("backtest", "market_snapshot_*.json")))
    return files[-1] if files else None


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
    return [r for r in rows if str(r.get("game_id", "")).startswith(date_str)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch best-book snapshot")
    parser.add_argument(
        "--snapshot-path", default=None, help="Path to unified snapshot JSON"
    )
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument("--diff-highlight", action="store_true")
    args = parser.parse_args()

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        return

    rows = load_rows(path)
    rows = [r for r in rows if "best_book" in r.get("snapshot_roles", [])]
    rows = filter_by_date(rows, args.date)

    df = format_for_display(rows, include_movement=args.diff_highlight)
    if df.empty:
        logger.warning("⚠️ Snapshot DataFrame is empty — nothing to dispatch.")
        return

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"]

    if "Market" not in df.columns:
        logger.warning("⚠️ 'Market' column missing — cannot apply fallback filters.")
        return

    if args.output_discord:
        webhook_main = os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL")
        webhook_alt = os.getenv("DISCORD_BEST_BOOK_ALT_WEBHOOK_URL")
        if webhook_main or webhook_alt:
            if webhook_main:
                if "Market Class" in df.columns:
                    subset = df[df["Market Class"] == "Main"]
                else:
                    logger.warning(
                        "⚠️ 'Market Class' column missing — using fallback"
                    )
                    subset = df[
                        df["Market"]
                        .str.lower()
                        .str.startswith(("h2h", "spreads", "totals"), na=False)
                    ]
                if subset.empty:
                    subset = df[
                        df["Market"]
                        .str.lower()
                        .str.startswith(("h2h", "spreads", "totals"), na=False)
                    ]
                logger.info(
                    "📡 Evaluating snapshot for: main → %s rows", subset.shape[0]
                )
                if not subset.empty:
                    send_bet_snapshot_to_discord(
                        subset, "Best Book (Main)", webhook_main
                    )
                else:
                    logger.warning("⚠️ No bets for main")
            if webhook_alt:
                if "Market Class" in df.columns:
                    subset = df[df["Market Class"] == "Alt"]
                else:
                    subset = df[
                        ~df["Market"]
                        .str.lower()
                        .str.startswith(("h2h", "spreads", "totals"), na=False)
                    ]
                if subset.empty:
                    subset = df[
                        ~df["Market"]
                        .str.lower()
                        .str.startswith(("h2h", "spreads", "totals"), na=False)
                    ]
                logger.info(
                    "📡 Evaluating snapshot for: alternate → %s rows", subset.shape[0]
                )
                if not subset.empty:
                    send_bet_snapshot_to_discord(subset, "Best Book (Alt)", webhook_alt)
                else:
                    logger.warning("⚠️ No bets for alternate")
        else:
            logger.warning("❌ No Discord webhook configured for best-book snapshots.")
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
