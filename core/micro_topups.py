import os
import json
import time
from datetime import datetime

from core.utils import safe_load_json
from core.lock_utils import with_locked_file

MICRO_TOPUPS_PATH = os.path.join('logs', 'micro_topups_pending.json')


def load_micro_topups(path: str = MICRO_TOPUPS_PATH) -> dict:
    """Return dict of pending micro top-ups."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    return {}


def save_micro_topups(pending: dict, path: str = MICRO_TOPUPS_PATH) -> None:
    """Persist ``pending`` to disk atomically."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    try:
        with with_locked_file(lock):
            with open(tmp, 'w') as f:
                json.dump(pending, f, indent=2)
            # Skip replace if unchanged
            skip_replace = False
            if os.path.exists(path):
                try:
                    with open(path, 'r') as cur, open(tmp, 'r') as new:
                        if cur.read() == new.read():
                            skip_replace = True
                except Exception:
                    pass
            if not skip_replace:
                for _ in range(5):
                    try:
                        os.replace(tmp, path)
                        break
                    except PermissionError as e:
                        last_err = e
                        time.sleep(0.1)
                else:
                    print(f"⚠️ Failed to save micro top-ups: {last_err}")
            else:
                os.remove(tmp)
    except Exception as e:
        print(f"⚠️ Failed to save micro top-ups: {e}")


def queue_micro_topup(key: tuple[str, str, str], bet: dict, delta: float, path: str = MICRO_TOPUPS_PATH) -> None:
    """Add or update a pending micro top-up."""
    pending = load_micro_topups(path)
    key_str = "|".join(key)
    bet_copy = {k: v for k, v in bet.items() if not k.startswith('_')}
    bet_copy['delta'] = float(delta)
    bet_copy['queued_ts'] = datetime.now().isoformat()
    pending[key_str] = bet_copy
    save_micro_topups(pending, path)


def remove_micro_topup(key: tuple[str, str, str], path: str = MICRO_TOPUPS_PATH) -> None:
    """Remove ``key`` from the pending queue if present."""
    pending = load_micro_topups(path)
    key_str = "|".join(key)
    if key_str in pending:
        pending.pop(key_str, None)
        save_micro_topups(pending, path)
