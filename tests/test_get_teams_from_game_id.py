import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import utils


def test_get_teams_from_valid_game_id():
    away, home = utils.get_teams_from_game_id("2025-05-05-CHW@KC-T1305-DH1")
    assert away == "CHW"
    assert home == "KC"


def test_get_teams_from_invalid_game_id():
    away, home = utils.get_teams_from_game_id("2025-05-05")
    assert away == ""
    assert home == ""
