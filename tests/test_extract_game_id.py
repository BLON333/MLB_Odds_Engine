import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.utils as utils


def test_extract_game_id_from_event_datetime():
    start = datetime(2025, 6, 9, 17, 5, tzinfo=ZoneInfo("UTC"))
    gid = utils.extract_game_id_from_event("Milwaukee Brewers", "Cincinnati Reds", start)
    assert gid == "2025-06-09-MIL@CIN-T1305"


def test_extract_game_id_from_event_string():
    start = "2025-06-09T17:05:00Z"
    gid = utils.extract_game_id_from_event("Milwaukee Brewers", "Cincinnati Reds", start)
    assert gid == "2025-06-09-MIL@CIN-T1305"

