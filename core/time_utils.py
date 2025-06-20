"""Utility helpers for core modules."""

from datetime import datetime
from typing import Optional

from core.utils import to_eastern, now_eastern


def compute_hours_to_game(game_start: datetime, now: Optional[datetime] = None) -> float:
    """Return hours until ``game_start`` relative to ``now``.

    Both ``game_start`` and ``now`` may be naive or timezone-aware. They are
    normalized to US/Eastern before the difference is calculated.  If ``now``
    is ``None`` the current Eastern time is used.
    """
    start_et = to_eastern(game_start)
    now_et = to_eastern(now or now_eastern())
    diff = (start_et - now_et).total_seconds() / 3600
    return diff if diff >= 0 else 0.0