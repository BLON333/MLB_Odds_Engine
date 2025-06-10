import os
import json
import time
import math
from typing import Dict

from utils import canonical_game_id, parse_game_id
from core.file_utils import is_file_older_than

TRACKER_PATH = os.path.join('backtest', 'market_eval_tracker.json')


def build_tracker_key(game_id: str, market: str, side: str) -> str:
    """Return a normalized key for the tracker."""
    parts = parse_game_id(game_id)
    if parts.get("away") and parts.get("home"):
        gid = canonical_game_id(game_id)
    else:
        gid = game_id
    return f"{gid}:{str(market).strip()}:{str(side).strip()}"


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
    """Save tracker data atomically with a simple file lock."""
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"

    # Detect and clean up stale lock files before attempting to acquire the lock
    if is_file_older_than(lock, 10):
        print("⚠️ Stale tracker lock detected; removing old lock file")
        try:
            os.remove(lock)
        except Exception:
            pass

    lock_acquired = False
    try:
        # Acquire an exclusive lock file with exponential backoff to avoid contention
        for attempt in range(50):
            try:
                fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.close(fd)
                lock_acquired = True
                break
            except FileExistsError:
                if is_file_older_than(lock, 10):
                    os.remove(lock)
                    continue
                time.sleep(min(0.1 * math.pow(2, attempt), 2.0))
        if not lock_acquired:
            print("⚠️ Failed to acquire tracker lock; aborting save")
            return

        # Merge with latest tracker on disk to avoid overwriting concurrent updates
        existing = load_tracker(path)
        if existing:
            existing.update(tracker)
            tracker.clear()
            tracker.update(existing)
        else:
            existing = tracker

        if not existing:
            print("⚠️ Tracker is empty, saving 0 entries")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(dict(sorted(existing.items())), f, indent=2)

        # Retry replace in case another process still has the file open
        for _ in range(5):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:
                time.sleep(0.1)
        else:
            raise
    except Exception as e:
        print(f"⚠️ Failed to save market eval tracker: {e}")
    finally:
        if lock_acquired:
            try:
                os.remove(lock)
            except Exception:
                pass
