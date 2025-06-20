import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.dispatch_clv_snapshot import parse_start_time
from core.utils import EASTERN_TZ

def test_parse_start_time_from_game_id():
    gid = "2025-06-16-COL@WSH-T1845"
    dt = parse_start_time(gid, None)
    assert dt.tzinfo == EASTERN_TZ
    assert dt.hour == 18 and dt.minute == 45

def test_parse_start_time_fallback_iso():
    gid = "2025-06-16-COL@WSH"
    odds = {"start_time": "2025-06-16T18:45:00Z"}
    dt = parse_start_time(gid, odds)
    assert dt.tzinfo == EASTERN_TZ
    assert dt.hour == 14 and dt.minute == 45


def test_parse_start_time_handles_eastern_token():
    """Game ID time tags should already be in Eastern time."""
    gid = "2025-07-04-NYM@ATL-T1905"
    dt = parse_start_time(gid, None)
    assert dt.tzinfo == EASTERN_TZ
    assert dt.hour == 19 and dt.minute == 5


def test_parse_start_time_returns_eastern():
    """Game ID times should parse directly to Eastern timezone."""
    gid = "2025-06-16-COL@WSH-T1905"
    dt = parse_start_time(gid, None)
    zone_name = getattr(dt.tzinfo, "zone", getattr(dt.tzinfo, "key", None))
    assert zone_name == "US/Eastern"
    assert dt.hour == 19 and dt.minute == 5
