import os
import json
from typing import Dict

TRACKER_PATH = os.path.join(os.path.dirname(__file__), '..', 'logs', 'market_eval_tracker.json')


def load_tracker(path: str = TRACKER_PATH) -> Dict[str, dict]:
    """Load the market evaluation tracker dictionary."""
    try:
        with open(path, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_tracker(tracker: Dict[str, dict], path: str = TRACKER_PATH) -> None:
    """Save tracker data atomically."""
    tmp = f"{path}.tmp"
    try:
        with open(tmp, 'w') as f:
            json.dump(tracker, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save market eval tracker: {e}")
