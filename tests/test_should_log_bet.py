import os
import sys
import csv
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.should_log_bet import (
    should_log_bet,
    get_theme,
    get_theme_key,
    normalize_segment,
)
from core.confirmation_utils import required_market_move
from core.skip_reasons import SkipReason


def _exposure_key(bet):
    base_market = bet["market"].replace("alternate_", "")
    segment = normalize_segment(bet["market"])
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
    assert result["log"] is True
    assert result["entry_type"] == "top-up"
    assert result["stake"] == 0.6


def test_top_up_accepted_for_1p6_full_stake():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.6,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.0}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.0}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, eval_tracker=tracker)
    assert result["log"] is True
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
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == SkipReason.LOW_TOPUP.value


def test_top_up_rejected_for_delta_point_three():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.3,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.0}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.0}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, eval_tracker=tracker)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"


def test_top_up_delta_rounded_before_threshold():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.7,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing = 0.4 * 3  # 1.2000000000000002 introduces floating point imprecision
    existing_theme_stakes = {exposure_key: existing}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": existing}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, eval_tracker=tracker)
    assert result["log"] is True
    assert result["entry_type"] == "top-up"
    assert result["stake"] == 0.5


def test_first_bet_logged_even_if_odds_worse():
    bet = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_odds": 105,
    }

    tracker_key = f"{bet['game_id']}:{bet['market']}:TeamA"
    reference = {tracker_key: {"market_odds": 110, "ev_percent": 7.0}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["log"] is True
    assert result["entry_type"] == "first"


def test_top_up_rejected_if_odds_worse():
    bet = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "full_stake": 2.0,
        "ev_percent": 6.0,
        "market_odds": 105,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.0}
    tracker_key = f"{bet['game_id']}:{bet['market']}:TeamA"
    reference = {tracker_key: {"market_odds": 110, "ev_percent": 7.0}}

    result = should_log_bet(bet, existing_theme_stakes, verbose=False, reference_tracker=reference)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == SkipReason.ODDS_WORSENED.value


def test_first_bet_logged_if_odds_improve():
    bet = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "full_stake": 1.2,
        "ev_percent": 7.0,
        "market_odds": 115,
    }

    tracker_key = f"{bet['game_id']}:{bet['market']}:TeamA"
    reference = {tracker_key: {"market_odds": 110, "ev_percent": 6.0}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["log"] is True
    assert result["entry_type"] == "first"


def test_team_total_classified_as_over():
    bet = {"side": "ATL Over 4.5", "market": "team_totals"}
    theme = get_theme(bet)
    assert theme == "Over"
    theme_key = get_theme_key(bet["market"], theme)
    assert theme_key == "Over_total"


def test_team_total_classified_as_under():
    bet = {"side": "NYY Under 3.5", "market": "team_totals"}
    theme = get_theme(bet)
    assert theme == "Under"
    theme_key = get_theme_key(bet["market"], theme)
    assert theme_key == "Under_total"


def test_normalized_market_keys():
    # totals vs alternate_totals
    bet_main = {"side": "Over 8.5", "market": "totals"}
    bet_alt = {"side": "Over 8.5", "market": "alternate_totals"}
    theme = get_theme(bet_main)
    assert get_theme_key(bet_main["market"], theme) == get_theme_key(bet_alt["market"], theme)

    # spreads vs alternate_spreads
    bet_main = {"side": "ATL +1.5", "market": "spreads"}
    bet_alt = {"side": "ATL +1.5", "market": "alternate_spreads"}
    theme = get_theme(bet_main)
    assert get_theme_key(bet_main["market"], theme) == get_theme_key(bet_alt["market"], theme)

    # h2h vs moneyline
    bet_main = {"side": "ATL", "market": "h2h"}
    bet_alt = {"side": "ATL", "market": "moneyline"}
    theme = get_theme(bet_main)
    assert get_theme_key(bet_main["market"], theme) == get_theme_key(bet_alt["market"], theme)


def test_rejected_for_low_ev():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 3.0,
    }

    result = should_log_bet(bet, {}, verbose=False, min_ev=0.05)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == "low_ev"


def test_rejected_for_low_stake():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 0.5,
        "ev_percent": 6.0,
    }

    result = should_log_bet(bet, {}, verbose=False)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == SkipReason.LOW_INITIAL.value


def test_rejected_for_odds_too_high():
    bet = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_odds": 205,
    }

    result = should_log_bet(bet, {}, verbose=False)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == "bad_odds"


def test_rejected_for_odds_too_negative():
    bet = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_odds": -155,
    }

    result = should_log_bet(bet, {}, verbose=False)
    assert result["skip"] is True
    assert bet["entry_type"] == "none"
    assert result["reason"] == "bad_odds"


def test_suppressed_early_unconfirmed(monkeypatch):
    hours = 14
    threshold = required_market_move(hours)
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_prob": 0.55,
        "hours_to_game": hours,
    }
    tracker_key = f"{bet['game_id']}:{bet['market']}:Over 8.5"
    reference = {tracker_key: {"market_prob": bet["market_prob"]}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["skip"] is True
    assert result["reason"] == SkipReason.SUPPRESSED_EARLY.value


def test_early_bet_allowed_with_confirmation(monkeypatch):
    hours = 14
    move = required_market_move(hours) + 0.005
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_prob": 0.55 + move,
        "hours_to_game": hours,
    }
    tracker_key = f"{bet['game_id']}:{bet['market']}:Over 8.5"
    reference = {tracker_key: {"market_prob": 0.55}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["log"] is True
    assert result["entry_type"] == "first"


def test_early_bet_rejected_for_low_book_agreement():
    hours = 13
    move = required_market_move(hours) + 0.005
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_prob": 0.55 + move,
        "hours_to_game": hours,
        "book_agreement_score": 0.25,
    }
    tracker_key = f"{bet['game_id']}:{bet['market']}:Over 8.5"
    reference = {tracker_key: {"market_prob": 0.55}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["skip"] is True
    assert result["reason"] == "suppressed_low_agreement"


def test_early_bet_allowed_with_book_agreement():
    hours = 13
    move = required_market_move(hours) + 0.005
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
        "market_prob": 0.55 + move,
        "hours_to_game": hours,
        "book_agreement_score": 0.4,
    }
    tracker_key = f"{bet['game_id']}:{bet['market']}:Over 8.5"
    reference = {tracker_key: {"market_prob": 0.55}}

    result = should_log_bet(bet, {}, verbose=False, reference_tracker=reference)
    assert result["log"] is True
    assert result["entry_type"] == "first"


def test_stale_theme_exposure_reset():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.2}
    result = should_log_bet(
        bet,
        existing_theme_stakes,
        verbose=False,
        existing_csv_stakes={},
    )
    assert result["log"] is True
    assert result["entry_type"] == "first"
    assert existing_theme_stakes[exposure_key] == 0.0


def test_theme_exposure_kept_when_csv_matches():
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.2,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.2}
    csv_stakes = {(bet["game_id"], bet["market"], bet["side"]): 1.2}
    result = should_log_bet(
        bet,
        existing_theme_stakes,
        verbose=False,
        existing_csv_stakes=csv_stakes,
    )
    assert result["skip"] is True
    assert existing_theme_stakes[exposure_key] == 1.2


def test_top_up_downgraded_without_csv(tmp_path):
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.6,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.0}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.0}}

    csv_path = tmp_path / "market_evals.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["game_id", "market", "side", "stake"])
        writer.writeheader()
        writer.writerow({"game_id": "other", "market": "totals", "side": "Over 7.5", "stake": 1.0})

    result = should_log_bet(
        bet,
        existing_theme_stakes,
        verbose=False,
        eval_tracker=tracker,
        existing_csv_stakes={},
        csv_path=str(csv_path),
    )
    assert result["log"] is True
    assert result["entry_type"] == "first"
    assert result["stake"] == pytest.approx(1.6)


def test_top_up_respected_when_csv_exists(tmp_path):
    bet = {
        "game_id": "gid",
        "market": "totals",
        "side": "Over 8.5",
        "full_stake": 1.6,
        "ev_percent": 6.0,
    }
    exposure_key = _exposure_key(bet)
    existing_theme_stakes = {exposure_key: 1.0}
    tracker = {f"{bet['game_id']}:{bet['market']}:Over 8.5": {"stake": 1.0}}

    csv_path = tmp_path / "market_evals.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["game_id", "market", "side", "stake"])
        writer.writeheader()
        writer.writerow({"game_id": bet["game_id"], "market": bet["market"], "side": bet["side"], "stake": 1.0})

    result = should_log_bet(
        bet,
        existing_theme_stakes,
        verbose=False,
        eval_tracker=tracker,
        existing_csv_stakes={},
        csv_path=str(csv_path),
    )
    assert result["log"] is True
    assert result["entry_type"] == "top-up"
    assert result["stake"] == pytest.approx(0.6)
