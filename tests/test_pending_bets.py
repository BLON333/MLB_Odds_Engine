import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.should_log_bet import should_log_bet
from core import pending_bets


def test_queue_pending_bet(monkeypatch, tmp_path):
    path = tmp_path / "pending.json"

    orig = pending_bets.queue_pending_bet

    def queue(bet):
        orig(bet, str(path))

    monkeypatch.setattr(pending_bets, "queue_pending_bet", queue)

    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.0,
        "ev_percent": 6.0,
        "market_prob": 0.55,
        "hours_to_game": 14,
    }
    tracker_key = f"{bet['game_id']}:{bet['market']}:Over 8.5"
    reference = {tracker_key: {"market_prob": bet["market_prob"]}}

    res = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert res["skip"] is True
    data = pending_bets.load_pending_bets(str(path))
    key = f"{bet['game_id']}:{bet['market']}:{bet['side']}"
    assert key in data
    queued = data[key]
    assert queued["full_stake"] == bet["full_stake"]
    assert queued["baseline_consensus_prob"] == bet["market_prob"]
