#!/usr/bin/env python
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""Dispatch best-book snapshot from unified snapshot JSON."""

import json
from utils import safe_load_json
import argparse
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug(
    "✅ Loaded webhook: %s", os.getenv("DISCORD_BEST_BOOK_MAIN_WEBHOOK_URL")
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch best-book snapshot")
    parser.add_argument(
        "--snapshot-path", default=None, help="Path to unified snapshot JSON"
    )
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
    rows = [r for r in rows if "best_book" in r.get("snapshot_roles", [])]
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
