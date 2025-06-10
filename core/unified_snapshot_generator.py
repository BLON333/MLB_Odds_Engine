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

from utils import now_eastern, safe_load_json, lookup_fallback_odds
from core.logger import get_logger
from core.odds_fetcher import fetch_market_odds_from_api
from core.snapshot_core import (
    load_simulations,
    build_snapshot_rows as _core_build_snapshot_rows,
    MARKET_EVAL_TRACKER,
    MARKET_EVAL_TRACKER_BEFORE_UPDATE,
)
from core.snapshot_core import expand_snapshot_rows_with_kelly
from core.market_eval_tracker import load_tracker, save_tracker

logger = get_logger(__name__)

# Debug/verbose toggles
VERBOSE = False
DEBUG = False

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


def build_snapshot_rows(sim_data: dict, odds_json: dict, min_ev: float = 0.01):
    """Wrapper around snapshot_core.build_snapshot_rows with debug logging."""
    if VERBOSE or DEBUG:
        for game_id in sim_data.keys():
            print(f"\U0001F50D Evaluating {game_id}")
            if lookup_fallback_odds(game_id, odds_json):
                print(f"\u2705 Matched odds for {game_id}")
            else:
                print(f"\u274C No odds found for {game_id}")
    return _core_build_snapshot_rows(sim_data, odds_json, min_ev=min_ev)


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
        odds = {gid: lookup_fallback_odds(gid, odds_data) for gid in sims.keys()}

    for gid in sims.keys():
        if gid not in odds or odds.get(gid) is None:
            logger.warning(
                "\u26A0\uFE0F No odds found for %s \u2014 check if sim or odds file used wrong ID format",
                gid,
            )

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
    try:
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
                return
    
        # Refresh tracker baseline before snapshot generation
        MARKET_EVAL_TRACKER.clear()
        MARKET_EVAL_TRACKER.update(load_tracker())
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()
        MARKET_EVAL_TRACKER_BEFORE_UPDATE.update(MARKET_EVAL_TRACKER)

        all_rows: list = []
        for date_str in date_list:
            rows_for_date = build_snapshot_for_date(date_str, odds_cache, (min_ev, max_ev))
            for row in rows_for_date:
                row["snapshot_for_date"] = date_str
            all_rows.extend(rows_for_date)

        # Save tracker after snapshot generation
        save_tracker(MARKET_EVAL_TRACKER)
        print(f"\U0001F4BE Saved market_eval_tracker with {len(MARKET_EVAL_TRACKER)} entries.")
    
        timestamp = now_eastern().strftime("%Y%m%dT%H%M")
        out_dir = "backtest"
        final_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.json")
        tmp_path = os.path.join(out_dir, f"market_snapshot_{timestamp}.tmp")

        os.makedirs(out_dir, exist_ok=True)
        with open(tmp_path, "w") as f:
            json.dump(all_rows, f, indent=2)

        # Validate written JSON before renaming
        try:
            with open(tmp_path) as f:
                json.load(f)
        except Exception:
            logger.exception("❌ Snapshot JSON validation failed for %s", tmp_path)
            bad_path = final_path + ".bad.json"
            try:
                shutil.move(tmp_path, bad_path)
                logger.error("🚨 Corrupted snapshot moved to %s", bad_path)
            except Exception as mv_err:
                logger.error("❌ Failed to move corrupt snapshot: %s", mv_err)
            return

        try:
            os.rename(tmp_path, final_path)
        except Exception:
            logger.exception(
                "❌ Failed to finalize snapshot rename from %s to %s",
                tmp_path,
                final_path,
            )
            return

        if len(all_rows) == 0:
            logger.warning(
                "⚠️ Snapshot %s written with 0 rows — no matched games.",
                final_path,
            )
        else:
            logger.info("✅ Snapshot written: %s with %d rows", final_path, len(all_rows))
    except Exception:
        logger.exception("Snapshot generation failed:")
        sys.exit(1)


if __name__ == "__main__":
    main()
