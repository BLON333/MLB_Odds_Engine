import os
import json
import time
from datetime import datetime

from utils import safe_load_json, now_eastern
from core.lock_utils import with_locked_file

PENDING_BETS_PATH = os.path.join('logs', 'pending_bets.json')


def load_pending_bets(path: str = PENDING_BETS_PATH) -> dict:
    """Return dictionary of pending bets keyed by tracker key."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    return {}


def save_pending_bets(pending: dict, path: str = PENDING_BETS_PATH) -> None:
    """Persist ``pending`` to ``path`` atomically using a lock."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    try:
        with with_locked_file(lock):
            with open(tmp, 'w') as f:
                json.dump(pending, f, indent=2)

            # Skip replace if contents are unchanged
            skip_replace = False
            if os.path.exists(path):
                try:
                    with open(path, 'r') as cur, open(tmp, 'r') as new:
                        if cur.read() == new.read():
                            skip_replace = True
                except Exception:
                    pass

            if not skip_replace:
                try:
                    os.replace(tmp, path)
                except PermissionError as e:
                    print(f"⚠️ Failed to save pending bets: {e} — retrying")
                    time.sleep(0.5)
                    try:
                        os.replace(tmp, path)
                    except Exception as retry_err:
                        print(f"❌ Retry failed — could not save pending bets: {retry_err}")
                except Exception as e:
                    print(f"⚠️ Failed to save pending bets: {e}")
            else:
                os.remove(tmp)
    except Exception as e:
        print(f"⚠️ Failed to save pending bets: {e}")


def queue_pending_bet(bet: dict, path: str = PENDING_BETS_PATH) -> None:
    """Append or update ``bet`` in ``pending_bets.json``."""
    pending = load_pending_bets(path)
    key = f"{bet['game_id']}:{bet['market']}:{bet['side']}"
    bet_copy = {k: v for k, v in bet.items() if not k.startswith('_')}
    bet_copy['queued_ts'] = datetime.now().isoformat()
    pending[key] = bet_copy
    save_pending_bets(pending, path)