from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import json
import time
import math
from typing import Dict

from core.logger import get_logger

from core.utils import canonical_game_id, parse_game_id
from core.file_utils import is_file_older_than, with_locked_file

# Default location for persistent market evaluation tracking
TRACKER_PATH = os.path.join('data', 'trackers', 'market_eval_tracker.json')

logger = get_logger(__name__)

FAILURE_LOG_PATH = os.path.join(os.path.dirname(TRACKER_PATH), "save_failures.log")
RECOVERY_PATH = os.path.join(os.path.dirname(TRACKER_PATH), "market_eval_tracker.recovery.json")


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
    data: Dict[str, dict] = {}
    try:
        with open(path, "r") as f:
            raw = json.load(f)
            if isinstance(raw, dict):
                data = raw
            elif isinstance(raw, list):
                converted: Dict[str, dict] = {}
                for entry in raw:
                    key = entry.get("key")
                    if not key:
                        continue
                    converted[key] = {k: v for k, v in entry.items() if k != "key"}
                data = converted
    except Exception:
        pass

    recovery_path = os.path.join(os.path.dirname(path), "market_eval_tracker.recovery.json")
    if os.path.exists(recovery_path):
        try:
            with open(recovery_path, "r") as f:
                recovery = json.load(f)
            if isinstance(recovery, dict):
                data.update(recovery)
            os.remove(recovery_path)
            print(f"\U0001F4E4 Merged recovery tracker with {len(recovery)} entries")
        except Exception as e:
            logger.error("❌ Failed to merge recovery tracker: %s", e)
    return data


def save_tracker(tracker: Dict[str, dict], path: str = TRACKER_PATH) -> None:
    """Save tracker data atomically using a simple lock file."""
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"

    try:
        with with_locked_file(lock):
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
    except TimeoutError:
        logger.error("❌ Tracker lock failed after multiple retries — skipping save")
        try:
            os.makedirs(os.path.dirname(FAILURE_LOG_PATH), exist_ok=True)
            with open(FAILURE_LOG_PATH, "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} tracker lock timeout\n")
        except Exception:
            pass
        try:
            os.makedirs(os.path.dirname(RECOVERY_PATH), exist_ok=True)
            with open(RECOVERY_PATH, "w") as f:
                json.dump(dict(sorted(tracker.items())), f, indent=2)
            print(f"🆘 Wrote recovery tracker to {RECOVERY_PATH}")
        except Exception as rec_e:
            print(f"❌ Failed to write recovery tracker: {rec_e}")
        return
    except Exception as e:
        print(f"⚠️ Failed to save market eval tracker: {e}")
        try:
            os.makedirs(os.path.dirname(RECOVERY_PATH), exist_ok=True)
            with open(RECOVERY_PATH, "w") as f:
                json.dump(dict(sorted(tracker.items())), f, indent=2)
            print(f"🆘 Wrote recovery tracker to {RECOVERY_PATH}")
        except Exception as rec_e:
            print(f"❌ Failed to write recovery tracker: {rec_e}")
