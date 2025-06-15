#!/usr/bin/env python3
"""Remove phantom entries from market_eval_tracker.json based on market_evals.csv."""

import csv
import json
import os
from datetime import datetime

from core.market_eval_tracker import TRACKER_PATH

CSV_PATH = os.path.join("logs", "market_evals.csv")


def parse_tracker_key(key: str):
    """Return ``(game_id, market, side)`` from a tracker key."""
    parts = key.split(":")
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    return None, None, None


def load_csv_keys(csv_path: str) -> set[tuple[str, str, str]]:
    """Load ``market_evals.csv`` and return a set of triples."""
    entries = set()
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return entries

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = row.get("game_id")
            market = row.get("market")
            side = row.get("side")
            if gid and market and side:
                entries.add((gid.strip(), market.strip(), side.strip()))
    return entries


def load_tracker(path: str) -> dict:
    if not os.path.exists(path):
        print(f"❌ Tracker not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if isinstance(data, dict):
            return data
    print(f"❌ Unexpected tracker format in {path}")
    return {}


def save_tracker(tracker: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(tracker.items())), f, indent=2)


def backup_tracker(path: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(path)[0]
    backup_path = f"{base}.backup.{timestamp}.json"
    if os.path.exists(path):
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        with open(path, "r", encoding="utf-8") as src, open(backup_path, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        print(f"🛟 Backup written to {backup_path}")
    return backup_path


def reconcile(csv_path: str = CSV_PATH, tracker_path: str = TRACKER_PATH) -> None:
    csv_keys = load_csv_keys(csv_path)
    tracker = load_tracker(tracker_path)
    if not tracker:
        return

    original_count = len(tracker)
    removed = 0
    for key in list(tracker.keys()):
        parsed = parse_tracker_key(key)
        if parsed[0] is None:
            continue
        if parsed not in csv_keys:
            del tracker[key]
            removed += 1

    if removed:
        backup_tracker(tracker_path)
        save_tracker(tracker, tracker_path)
        print(f"✅ Removed {removed} phantom entries (now {len(tracker)} total)")
    else:
        print("✅ No phantom entries found")

    # Confirm remaining keys exist in CSV
    missing = [k for k in tracker if parse_tracker_key(k) not in csv_keys]
    if missing:
        print(f"⚠️ {len(missing)} remaining keys not found in CSV")
    else:
        print("✅ All remaining keys verified against CSV")


if __name__ == "__main__":
    reconcile()
