#!/usr/bin/env python
"""Unified snapshot generator.

This script combines the logic of the various snapshot generators into
one builder that outputs a timestamped JSON file.  Each row is
annotated with ``snapshot_roles`` describing which downstream snapshot
categories it qualifies for.
"""

import os
import sys
import json
import argparse
import shutil
from datetime import timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

from utils import now_eastern, safe_load_json
from core.logger import get_logger
from core.odds_fetcher import fetch_market_odds_from_api
from core.snapshot_core import (
    load_simulations,
    build_snapshot_rows,
)
from core.snapshot_core import expand_snapshot_rows_with_kelly

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def latest_odds_file(folder="data/market_odds") -> str | None:
    files = sorted(
        [
            f
            for f in os.listdir(folder)
            if f.startswith("market_odds_") and f.endswith(".json")
        ],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None


# ---------------------------------------------------------------------------
# Snapshot role helpers
# ---------------------------------------------------------------------------
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


def is_best_book_row(row: dict) -> bool:
    """Return True if row uses a popular sportsbook."""
    return row.get("book") in POPULAR_BOOKS


def is_live_snapshot_row(row: dict) -> bool:
    """Return True if row qualifies for the live snapshot."""
    return row.get("ev_percent", 0) >= 5.0


def is_personal_book_row(row: dict) -> bool:
    """Return True if row is from a personal sportsbook."""
    return row.get("book") in [
        "pinnacle",
        "fanduel",
        "bovada",
        "betonlineag",
    ]


# ---------------------------------------------------------------------------
# Snapshot generation
# ---------------------------------------------------------------------------


def build_snapshot_for_date(
    date_str: str,
    odds_data: dict | None,
    ev_range: tuple[float, float] = (5.0, 20.0),
) -> list:
    """Return expanded snapshot rows for a single date."""
    sim_dir = os.path.join("backtest", "sims", date_str)
    sims = load_simulations(sim_dir)
    if not sims:
        logger.warning("❌ No simulation files found for %s", date_str)
        return []

    # Fetch or slice market odds
    if odds_data is None:
        odds = fetch_market_odds_from_api(list(sims.keys()))
    else:
        odds = {gid: odds_data.get(gid) for gid in sims.keys()}
    if not odds:
        logger.warning("❌ No market odds found for %s", date_str)
        return []

    # Build base rows and expand per-book variants
    raw_rows = build_snapshot_rows(sims, odds, min_ev=0.01)
    logger.info("\U0001F9EA Raw bets from build_snapshot_rows(): %d", len(raw_rows))
    expanded_rows = expand_snapshot_rows_with_kelly(raw_rows)
    logger.info("\U0001F9E0 Expanded per-book rows: %d", len(expanded_rows))

    rows = expanded_rows

    # 🎯 Retain all rows (EV% filter removed)
    min_ev, max_ev = ev_range  # kept for compatibility
    logger.info(
        "📊 Snapshot generation: %d rows evaluated (no EV%% filtering applied)",
        len(rows),
    )

    # 📦 Assign snapshot roles (include all rows)
    snapshot_rows = []
    best_book_tracker: dict[tuple[str, str, str], dict] = {}

    for row in rows:
        row["snapshot_roles"] = []

        if is_live_snapshot_row(row):
            row["snapshot_roles"].append("live")
        if is_personal_book_row(row):
            row["snapshot_roles"].append("personal")

        if is_best_book_row(row):
            key = (row.get("game_id"), row.get("market"), row.get("side"))
            best_row = best_book_tracker.get(key)
            if not best_row:
                best_book_tracker[key] = row
            else:
                ev = row.get("ev_percent", 0)
                best_ev = best_row.get("ev_percent", 0)
                if ev > best_ev or (
                    ev == best_ev and row.get("stake", 0) > best_row.get("stake", 0)
                ):
                    best_book_tracker[key] = row

        snapshot_rows.append(row)

    for best_row in best_book_tracker.values():
        best_row.setdefault("snapshot_roles", []).append("best_book")

    final_rows = snapshot_rows

    logger.info("\u2705 Final snapshot rows to write: %d", len(final_rows))

    num_with_roles = sum(1 for r in final_rows if r.get("snapshot_roles"))
    num_stake_half = sum(1 for r in final_rows if r.get("stake", 0) >= 0.5)
    num_stake_one = sum(1 for r in final_rows if r.get("stake", 0) >= 1.0)
    logger.info(
        "\U0001F4CA Of those: %d rows have roles, %d have stake \u2265 0.5u, %d have stake \u2265 1.0u",
        num_with_roles,
        num_stake_half,
        num_stake_one,
    )

    return final_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate unified market snapshot")
    parser.add_argument("--date", default=None)
    parser.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    parser.add_argument(
        "--ev-range",
        default="5.0,20.0",
        help="EV%% range to include as 'min,max'",
    )
    args = parser.parse_args()

    if args.date:
        date_list = [d.strip() for d in str(args.date).split(",") if d.strip()]
    else:
        today = now_eastern().strftime("%Y-%m-%d")
        tomorrow = (now_eastern() + timedelta(days=1)).strftime("%Y-%m-%d")
        date_list = [today, tomorrow]

    try:
        min_ev, max_ev = map(float, args.ev_range.split(","))
    except Exception:
        logger.error("❌ Invalid --ev-range format, expected 'min,max'")
        return

    odds_cache = None
    if args.odds_path:
        odds_cache = safe_load_json(args.odds_path)
        if odds_cache is not None:
            logger.info("📥 Loaded odds from %s", args.odds_path)
        else:
            logger.error("❌ Failed to load odds file %s", args.odds_path)
    else:
        auto_path = latest_odds_file()
        if auto_path:
            odds_cache = safe_load_json(auto_path)
            if odds_cache is not None:
                logger.info("📥 Auto-loaded latest odds: %s", auto_path)
        if odds_cache is None:
            logger.error("❌ No market_odds_*.json files found or failed to load.")
            sys.exit(1)
    if not odds_cache:
        logger.error("❌ No valid market odds loaded")
        sys.exit(1)

    all_rows: list = []
    for date_str in date_list:
        rows_for_date = build_snapshot_for_date(date_str, odds_cache, (min_ev, max_ev))
        for row in rows_for_date:
            row["snapshot_for_date"] = date_str
        all_rows.extend(rows_for_date)

    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    out_path = os.path.join("backtest", f"market_snapshot_{timestamp}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_rows, f, indent=2)

    # Validate written JSON
    try:
        with open(out_path) as f:
            json.load(f)
    except Exception:
        logger.exception("❌ Snapshot JSON validation failed for %s", out_path)
        bad_path = out_path + ".bad.json"
        try:
            shutil.move(out_path, bad_path)
            logger.error("🚨 Corrupted snapshot moved to %s", bad_path)
        except Exception as mv_err:
            logger.error("❌ Failed to move corrupt snapshot: %s", mv_err)
        return

    logger.info("✅ Wrote %s rows to %s", len(all_rows), out_path)


if __name__ == "__main__":
    main()