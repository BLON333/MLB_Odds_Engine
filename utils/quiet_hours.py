from datetime import time
from core.utils import now_eastern, to_eastern


def is_within_quiet_hours(now=None):
    """Return ``True`` if ``now`` falls within quiet hours (10 PM - 8 AM ET)."""
    dt = to_eastern(now) if now else now_eastern()
    return dt.time() >= time(22, 0) or dt.time() < time(8, 0)

