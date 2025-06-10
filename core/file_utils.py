import os
import time


def is_file_older_than(path: str, seconds: int) -> bool:
    """Return True if ``path`` exists and is older than ``seconds``."""
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) > seconds
