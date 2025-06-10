import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_id_utils import build_game_id, normalize_game_id, fuzzy_match_game_id


def test_build_game_id_converts_to_eastern():
    start = datetime(2025, 6, 9, 17, 5, tzinfo=ZoneInfo("UTC"))
    gid = build_game_id("MIL", "CIN", start)
    assert gid == "2025-06-09-MIL@CIN-T1305"


def test_normalize_game_id_strips_suffix():
    assert normalize_game_id("2025-06-09-MIL@CIN-T1305") == "2025-06-09-MIL@CIN"
    assert normalize_game_id("2025-06-09-MIL@CIN") == "2025-06-09-MIL@CIN"


def test_fuzzy_match_game_id_window():
    target = "2025-06-09-MIL@CIN-T1307"
    cands = ["2025-06-09-MIL@CIN-T1305", "2025-06-09-MIL@CIN-T1500"]
    assert fuzzy_match_game_id(target, cands, window=5) == "2025-06-09-MIL@CIN-T1305"
    assert fuzzy_match_game_id(target, cands, window=1) is None
