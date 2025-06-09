import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import utils


def test_canonical_game_id_preserves_doubleheader_suffix():
    raw = "2025-05-05-CHW@KC-T1305-DH1"
    assert utils.canonical_game_id(raw) == "2025-05-05-CWS@KC-T1305-DH1"


def test_canonical_game_id_simple_doubleheader():
    raw = "2025-05-05-CHW@KC-DH2"
    assert utils.canonical_game_id(raw) == "2025-05-05-CWS@KC-DH2"


def test_parse_game_id_handles_extra_parts():
    parts = utils.parse_game_id("2025-05-05-CHW@KC-T1305-DH1")
    assert parts["time"] == "T1305-DH1"
    assert parts["away"] == "CHW"
    assert parts["home"] == "KC"

