import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.bet_helpers import evaluate_late_confirmed_bet
from core.confirmation_utils import required_market_move


def _base_bet():
    return {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "market_odds": 110,
        "blended_prob": 0.55,
        "full_stake": 3.0,
        "hours_to_game": 5.0,
        "baseline_consensus_prob": 0.52,
    }


def test_top_up_triggered_when_movement_meets_threshold():
    bet = _base_bet()
    threshold = required_market_move(bet["hours_to_game"])
    new_prob = bet["baseline_consensus_prob"] + threshold + 0.002

    res = evaluate_late_confirmed_bet(bet, new_prob, existing_stake=1.0)
    assert res is not None
    assert res["entry_type"] == "top-up"
    assert res["stake"] == 2.0
    assert res["full_stake"] <= bet["full_stake"]


def test_no_top_up_when_movement_insufficient():
    bet = _base_bet()
    threshold = required_market_move(bet["hours_to_game"])
    new_prob = bet["baseline_consensus_prob"] + threshold - 0.001

    res = evaluate_late_confirmed_bet(bet, new_prob, existing_stake=1.0)
    assert res is None
