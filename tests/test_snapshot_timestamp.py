import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import parse_snapshot_timestamp, EASTERN_TZ


def test_parse_snapshot_timestamp_basic():
    dt = parse_snapshot_timestamp("20250616T1530")
    assert dt.tzinfo == EASTERN_TZ
    assert dt.hour == 15 and dt.minute == 30
