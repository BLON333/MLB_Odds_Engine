import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.should_log_bet import should_log_bet, get_theme, get_theme_key, get_segment_group


def _exposure_key(bet):
    base_market = bet["market"].replace("alternate_", "")
    segment = get_segment_group(bet["market"])
    theme = get_theme({"side": bet["side"], "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    return (bet["game_id"], theme_key, segment)


def test_top_up_accepted():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 2.0,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.4}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.4}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, eval_tracker=tracker)
    assert result is not None
    assert result["entry_type"] == "top-up"
    assert result["stake"] == 0.6


def test_top_up_rejected_for_small_delta():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.9,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.7}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.7}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, eval_tracker=tracker)
    assert result is None
    assert bet["entry_type"] == "none"
