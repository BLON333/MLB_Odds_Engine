import os
import json
import ast
from typing import Dict

from .theme_key_utils import make_theme_key, parse_theme_key

from core.file_utils import with_locked_file

# Default location for persistent theme exposure tracking
TRACKER_PATH = os.path.join("logs", "theme_exposure.json")


def load_tracker(path: str = TRACKER_PATH) -> Dict[str, float]:
    """Load theme exposure tracker from ``path``.

    Handles both legacy tuple-string keys and the new "``game::theme::segment``"
    format.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        stakes: Dict[str, float] = {}
        for k, v in data.items():
            if isinstance(k, str):
                if "::" in k and k.count("::") == 2:
                    stakes[k] = float(v)
                    continue
                try:
                    key = ast.literal_eval(k)
                except Exception:
                    key = None
                if isinstance(key, (list, tuple)) and len(key) == 3:
                    stakes[make_theme_key(str(key[0]), str(key[1]), str(key[2]))] = float(v)
        return stakes
    except Exception:
        return {}


def save_tracker(stakes: Dict[str, float], path: str = TRACKER_PATH) -> None:
    """Atomically persist theme exposure tracker to ``path``."""
    serializable = {k: v for k, v in stakes.items()}
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with with_locked_file(lock):
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2)
            os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save theme exposure tracker: {e}")

