from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import time


def is_file_older_than(path: str, seconds: int) -> bool:
    """Return True if ``path`` exists and is older than ``seconds``."""
    if not os.path.exists(path):
        return False
    return (time.time() - os.path.getmtime(path)) > seconds


from contextlib import contextmanager
import math


@contextmanager
def with_locked_file(lock_path: str, *, stale_after: int = 10, retries: int = 50):
    """Context manager that acquires ``lock_path`` and releases it on exit.

    A simple lock file is used for mutual exclusion. If the existing lock file
    is older than ``stale_after`` seconds it will be removed before retrying.
    Acquisition is attempted ``retries`` times with exponential backoff and a
    ``TimeoutError`` is raised if the lock cannot be obtained.
    """

    lock_acquired = False

    # Remove stale lock before attempting acquisition
    if is_file_older_than(lock_path, stale_after):
        print("⚠️ Stale tracker lock detected; removing old lock file")
        try:
            os.remove(lock_path)
        except Exception:
            pass

    try:
        for attempt in range(retries):
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.close(fd)
                lock_acquired = True
                break
            except FileExistsError:
                if is_file_older_than(lock_path, stale_after):
                    print("⚠️ Stale tracker lock detected; removing old lock file")
                    try:
                        os.remove(lock_path)
                    except Exception:
                        pass
                    continue
                time.sleep(min(0.1 * math.pow(2, attempt), 2.0))

        if not lock_acquired:
            raise TimeoutError("Lock acquisition failed")

        yield
    finally:
        if lock_acquired and os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except Exception:
                pass