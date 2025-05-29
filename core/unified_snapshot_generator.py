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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

from utils import now_eastern
from core.logger import get_logger
from core.odds_fetcher import fetch_market_odds_from_api
from core.snapshot_core import load_simulations, build_snapshot_rows
from core.market_eval_tracker import load_tracker, save_tracker
from core.market_movement_tracker import track_and_update_market_movement
import copy
from cli.log_betting_evals import expand_snapshot_rows_with_kelly

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_odds_file(folder="data/market_odds") -> str | None:
    files = sorted(
        [f for f in os.listdir(folder) if f.startswith("market_odds_") and f.endswith(".json")],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None

# ---------------------------------------------------------------------------
# Snapshot role helpers
# ---------------------------------------------------------------------------
POPULAR_BOOKS = [
    "fanduel", "draftkings", "betmgm", "williamhill_us", "espnbet",
    "hardrockbet", "fliff", "mybookieag", "lowvig", "betonlineag",
    "betrivers", "fanatics", "pinnacle",
]


def is_best_book_row(row: dict) -> bool:
    """Return True if row uses a popular sportsbook."""
    return row.get("best_book") in POPULAR_BOOKS


def is_live_snapshot_row(row: dict) -> bool:
    """Return True if row qualifies for the live snapshot."""
    return row.get("ev_percent", 0) >= 5.0


def is_fv_drop_row(row: dict, prior_snapshot: dict | None = None) -> bool:
    """Return True if market probability increased while EV improved."""
    return row.get("ev_movement") == "better" and row.get("mkt_movement") == "better"


def is_personal_book_row(row: dict) -> bool:
    """Return True if row is from a personal sportsbook."""
    return row.get("best_book") in ["bovada", "betonlineag"]


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

    # Build base rows and expand per-book variants
    rows = build_snapshot_rows(sims, odds, min_ev=0.01)
    rows = expand_snapshot_rows_with_kelly(rows, min_ev=1.0, min_stake=0.5)

    # 🧠 Track line movement
    tracker = load_tracker()
    reference_tracker = copy.deepcopy(tracker)
    for row in rows:
        movement = track_and_update_market_movement(
            row,
            tracker,
            reference_tracker,
        )
        row.update(movement)
    save_tracker(tracker)

    # 🎯 Filter by EV% (optional)
    min_ev, max_ev = ev_range
    rows = [r for r in rows if min_ev <= r.get("ev_percent", 0) <= max_ev]
    logger.info(
        "✅ Filtered rows to EV%% between %.2f and %.2f → %d rows remain",
        min_ev,
        max_ev,
        len(rows),
    )

    # 📦 Assign snapshot roles (but include all rows)
    snapshot_rows = []
    for row in rows:
        row["snapshot_roles"] = []
        if is_best_book_row(row):
            row["snapshot_roles"].append("best_book")
        if is_live_snapshot_row(row):
            row["snapshot_roles"].append("live")
        if is_fv_drop_row(row, None):
            row["snapshot_roles"].append("fv_drop")
        if is_personal_book_row(row):
            row["snapshot_roles"].append("personal")
        snapshot_rows.append(row)

    print(f"✅ Snapshot contains {len(snapshot_rows)} evaluated bets.")
    return snapshot_rows



def main() -> None:
    parser = argparse.ArgumentParser(description="Generate unified market snapshot")
    parser.add_argument("--date", default=now_eastern().strftime("%Y-%m-%d"))
    parser.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    parser.add_argument(
        "--ev-range",
        default="5.0,20.0",
        help="EV%% range to include as 'min,max'",
    )
    args = parser.parse_args()

    date_list = [d.strip() for d in str(args.date).split(",") if d.strip()]

    try:
        min_ev, max_ev = map(float, args.ev_range.split(","))
    except Exception:
        logger.error("❌ Invalid --ev-range format, expected 'min,max'")
        return

    odds_cache = None
    if args.odds_path:
        try:
            with open(args.odds_path) as fh:
                odds_cache = json.load(fh)
            logger.info("📥 Loaded odds from %s", args.odds_path)
        except Exception as e:
            logger.error("❌ Failed to load odds file %s: %s", args.odds_path, e)
    else:
        auto_path = latest_odds_file()
        if auto_path:
            with open(auto_path) as fh:
                odds_cache = json.load(fh)
            logger.info("📥 Auto-loaded latest odds: %s", auto_path)
        else:
            logger.error("❌ No market_odds_*.json files found.")
            return

    all_rows: list = []
    for date_str in date_list:
        all_rows.extend(
            build_snapshot_for_date(date_str, odds_cache, (min_ev, max_ev))
        )

    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    out_path = os.path.join("backtest", f"market_snapshot_{timestamp}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_rows, f, indent=2)

    logger.info("✅ Wrote %s rows to %s", len(all_rows), out_path)


if __name__ == "__main__":
    main()