import os
import sys
import time

# Add repository root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.market_eval_tracker import save_tracker, TRACKER_PATH


def main() -> None:
    """Verify that stale tracker lock files are detected and cleaned."""
    lock_path = f"{TRACKER_PATH}.lock"
    os.makedirs(os.path.dirname(TRACKER_PATH), exist_ok=True)

    # Create dummy lock file
    with open(lock_path, "w") as f:
        f.write("dummy")

    # Set mtime 30 seconds in the past to simulate staleness
    past = time.time() - 30
    os.utime(lock_path, (past, past))

    # Save dummy tracker data
    dummy_tracker = {"dummy:key": {"market_odds": 100}}
    save_tracker(dummy_tracker)

    if os.path.exists(lock_path):
        raise AssertionError("Lock file was not removed")
    print("✅ Stale lock file removed successfully")


if __name__ == "__main__":
    main()
