#!/usr/bin/env python
"""Dispatch live snapshot from unified snapshot JSON."""

import os
import sys
import json
import argparse
from dotenv import load_dotenv

load_dotenv(dotenv_path="C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/.env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.snapshot_core import format_for_display, send_bet_snapshot_to_discord
from core.logger import get_logger

logger = get_logger(__name__)

# Optional debug log to verify environment variables are loaded
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_SPREADS_WEBHOOK_URL"))


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
    return [r for r in rows if str(r.get("game_id", "")).startswith(date_str)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch live snapshot")
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
    rows = [r for r in rows if "live" in r.get("snapshot_roles", [])]
    rows = filter_by_date(rows, args.date)

    df = format_for_display(rows, include_movement=args.diff_highlight)

    if df.empty:
        logger.warning("⚠️ Snapshot DataFrame is empty — nothing to dispatch.")
        return

    if "market" in df.columns and "Market" not in df.columns:
        df["Market"] = df["market"]

    if "Market" not in df.columns:
        logger.warning("⚠️ 'Market' column missing — skipping live snapshot dispatch.")
        return

    if args.output_discord:
        webhook_map = {
            "h2h": os.getenv("DISCORD_H2H_WEBHOOK_URL"),
            "spreads": os.getenv("DISCORD_SPREADS_WEBHOOK_URL"),
            "totals": os.getenv("DISCORD_TOTALS_WEBHOOK_URL"),
        }
        for label in ["h2h", "spreads", "totals"]:
            subset = df[df["Market"].str.lower().str.startswith(label, na=False)]
            webhook = webhook_map.get(label)
            logger.info(
                "📡 Evaluating snapshot for: %s → %s rows", label, subset.shape[0]
            )
            if subset.empty:
                logger.warning("⚠️ No bets for %s", label)
                continue
            if webhook:
                send_bet_snapshot_to_discord(subset, label, webhook)
            else:
                logger.warning("❌ Discord webhook not configured for %s", label)
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
