#!/usr/bin/env python3
"""Rebuild theme_exposure.json from market_evals.csv."""

import csv
import os
from datetime import datetime
from typing import Dict, Tuple

from cli.log_betting_evals import get_exposure_key
from core.theme_exposure_tracker import TRACKER_PATH, load_tracker, save_tracker

CSV_PATH = os.path.join("logs", "market_evals.csv")


def backup_tracker(path: str) -> str:
    """Backup the existing tracker file with a timestamped copy."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(path)[0]
    backup_path = f"{base}.backup.{timestamp}.json"
    if os.path.exists(path):
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with open(path, "r", encoding="utf-8") as src, open(
            backup_path, "w", encoding="utf-8"
        ) as dst:
            dst.write(src.read())
        print(f"🛟 Backup written to {backup_path}")
    return backup_path


def compute_csv_totals(csv_path: str) -> Dict[Tuple[str, str, str], float]:
    """Return exposure totals keyed by ``(game_id, theme_key, segment)``."""
    totals: Dict[Tuple[str, str, str], float] = {}
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return totals

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stake_val = row.get("stake")
            if not stake_val:
                continue
            try:
                stake = float(stake_val)
            except ValueError:
                continue
            key = get_exposure_key(row)
            totals[key] = totals.get(key, 0.0) + stake
    return totals


def reconcile(csv_path: str = CSV_PATH, tracker_path: str = TRACKER_PATH) -> None:
    """Rebuild ``theme_exposure.json`` from the CSV log."""
    csv_totals = compute_csv_totals(csv_path)
    if not csv_totals:
        print("⚠️ No exposure totals found in CSV")
        return

    old_tracker = load_tracker(tracker_path)
    added_keys = set(csv_totals) - set(old_tracker)
    removed_keys = set(old_tracker) - set(csv_totals)

    backup_tracker(tracker_path)
    save_tracker(csv_totals, tracker_path)

    print("✅ Reconciliation Complete")
    print(f"➕ Entries added: {len(added_keys)}")
    print(f"➖ Entries removed: {len(removed_keys)}")
    print(f"📊 Final tracker size: {len(csv_totals)}")


if __name__ == "__main__":
    reconcile()