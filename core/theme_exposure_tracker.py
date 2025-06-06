import os
import json
import ast
from typing import Dict, Tuple

TRACKER_PATH = os.path.join('backtest', 'existing_theme_stakes.json')


def load_tracker(path: str = TRACKER_PATH) -> Dict[Tuple[str, str, str], float]:
    """Load theme exposure tracker from ``path``."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        stakes: Dict[Tuple[str, str, str], float] = {}
        for k, v in data.items():
            try:
                key = ast.literal_eval(k)
                if isinstance(key, (list, tuple)) and len(key) == 3:
                    stakes[tuple(key)] = float(v)
            except Exception:
                continue
        return stakes
    except Exception:
        return {}


def save_tracker(stakes: Dict[Tuple[str, str, str], float], path: str = TRACKER_PATH) -> None:
    """Atomically persist theme exposure tracker to ``path``."""
    serializable = {str(k): v for k, v in stakes.items()}
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(tmp, 'w') as f:
            json.dump(serializable, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save theme exposure tracker: {e}")

