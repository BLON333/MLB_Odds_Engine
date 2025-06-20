import os, sys, pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import snapshot_core as sc
from core.market_pricer import kelly_fraction
from core.confirmation_utils import confirmation_strength


def test_adjusted_kelly_scaling(monkeypatch):
    monkeypatch.setattr(sc, "save_tracker", lambda tracker: None)
    sc.MARKET_EVAL_TRACKER.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()

    from core.utils import canonical_game_id
    gid = canonical_game_id("2025-06-09-MIL@CIN-T1305")
    key = f"{gid}:h2h:TeamA"
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE[key] = {"market_prob": 0.5}

    row = {
        "game_id": gid,
        "market": "h2h",
        "side": "TeamA",
        "blended_prob": 0.55,
        "market_prob": 0.51,
        "sim_prob": 0.55,
        "market_odds": 115,
        "ev_percent": 6.0,
        "hours_to_game": 10,
        "_raw_sportsbook": {"B1": 115},
        "best_book": "B1",
        "book": "B1",
    }

    result = sc.expand_snapshot_rows_with_kelly([row])[0]
    raw = kelly_fraction(0.55, 115, fraction=0.25)
    strength = confirmation_strength(0.01, 10)
    expected = round(raw * (strength ** 1.5), 4)

    assert result["raw_kelly"] == pytest.approx(raw)
    assert result["adjusted_kelly"] == pytest.approx(expected)
    assert result["stake"] == pytest.approx(expected)
