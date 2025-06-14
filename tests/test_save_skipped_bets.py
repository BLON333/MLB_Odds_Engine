import json
import os
from cli.log_betting_evals import save_skipped_bets


def test_save_skipped_bets(tmp_path):
    bets = [{
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "ev_percent": 5.5,
        "stake": 1.0,
        "skip_reason": "low_ev",
    }]
    path = save_skipped_bets(bets, base_dir=str(tmp_path))
    assert os.path.exists(path)
    assert not os.path.exists(path + ".tmp")
    with open(path) as f:
        data = json.load(f)
    assert data == bets
