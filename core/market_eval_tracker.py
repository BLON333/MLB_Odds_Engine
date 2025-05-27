import os
import json
from typing import Dict

TRACKER_PATH = os.path.join('backtest', 'market_eval_tracker.json')


def load_tracker(path: str = TRACKER_PATH) -> Dict[str, dict]:
    """Load the market evaluation tracker dictionary."""
    try:
        with open(path, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            if isinstance(data, list):
                converted = {}
                for entry in data:
                    key = entry.get('key')
                    if not key:
                        continue
                    converted[key] = {
                        k: v for k, v in entry.items() if k != 'key'
                    }
                return converted
    except Exception:
        pass
    return {}


def save_tracker(tracker: Dict[str, dict], path: str = TRACKER_PATH) -> None:
    """Save tracker data atomically."""
    tmp = f"{path}.tmp"
    try:
        if not tracker:
            print("⚠️ Tracker is empty, saving 0 entries")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, 'w') as f:
            sorted_data = dict(sorted(tracker.items()))
            json.dump(sorted_data, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save market eval tracker: {e}")