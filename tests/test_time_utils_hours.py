import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.time_utils import compute_hours_to_game
from core.utils import EASTERN_TZ


def test_compute_hours_to_game_tz_normalization():
    aware_now = datetime(2025, 6, 9, 12, 0, tzinfo=EASTERN_TZ)
    aware_start = datetime(2025, 6, 9, 15, 0, tzinfo=EASTERN_TZ)
    hrs = compute_hours_to_game(aware_start, aware_now)
    assert abs(hrs - 3.0) < 1e-6

    # Equivalent times expressed as naive UTC datetimes
    naive_now = datetime(2025, 6, 9, 16, 0)
    naive_start = datetime(2025, 6, 9, 19, 0)
    hrs_naive = compute_hours_to_game(naive_start, naive_now)
    assert abs(hrs_naive - 3.0) < 1e-6
