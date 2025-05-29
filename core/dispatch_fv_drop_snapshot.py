#!/usr/bin/env python
"""Dispatch FV drop snapshot from unified snapshot JSON."""

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
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_FV_DROP_WEBHOOK_URL"))


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
    parser = argparse.ArgumentParser(description="Dispatch FV drop snapshot")
    parser.add_argument("--snapshot-path", default=None, help="Path to unified snapshot JSON")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument("--diff-highlight", action="store_true")
    args = parser.parse_args()

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        return

    rows = load_rows(path)
    rows = [r for r in rows if "fv_drop" in r.get("snapshot_roles", [])]
    rows = filter_by_date(rows, args.date)

    df = format_for_display(rows, include_movement=args.diff_highlight)

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
