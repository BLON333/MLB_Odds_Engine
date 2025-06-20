import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.utils import logging_allowed_now, EASTERN_TZ


def test_logging_allowed_daytime():
    dt = datetime(2024, 1, 1, 12, 0, tzinfo=EASTERN_TZ)
    assert logging_allowed_now(dt)


def test_logging_blocked_at_night():
    late = datetime(2024, 1, 1, 22, 30, tzinfo=EASTERN_TZ)
    early = datetime(2024, 1, 1, 7, 59, tzinfo=EASTERN_TZ)
    assert not logging_allowed_now(late)
    assert not logging_allowed_now(early)
