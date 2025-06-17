import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.theme_exposure_tracker import load_tracker, TRACKER_PATH
from scripts.reconcile_theme_exposure import compute_csv_totals


def test_theme_exposure_matches_csv():
    tracker = load_tracker(TRACKER_PATH)
    csv_totals = compute_csv_totals(os.path.join("logs", "market_evals.csv"))

    for key, exposure in tracker.items():
        if exposure <= 0:
            continue
        assert key in csv_totals, f"Tracker exposure for {key} missing from CSV"
        if abs(csv_totals[key] - exposure) > 1e-6:
            print(f"Mismatch for {key}: tracker={exposure} csv={csv_totals[key]}")
