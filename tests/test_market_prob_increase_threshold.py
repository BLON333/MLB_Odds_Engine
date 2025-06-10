import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import market_prob_increase_threshold
from core import market_movement_tracker as mmt


@pytest.mark.parametrize(
    "hours,market,expected",
    [
        (50, "f5 totals", 0.004),
        (50, "h2h", 0.005),
        (6, "1st inning", 0.001),
        (6, "h2h", 0.002),
    ],
)
def test_derivative_vs_mainline_thresholds(hours, market, expected):
    assert market_prob_increase_threshold(hours, market) == pytest.approx(expected)


def test_mid_range_thresholds():
    hours = 24
    non_derivative = market_prob_increase_threshold(hours, "h2h")
    derivative = market_prob_increase_threshold(hours, "f5 h2h")
    base = 0.002 + 0.003 * (hours - 6) / 42
    assert non_derivative == pytest.approx(base)
    assert derivative == pytest.approx(base - 0.001)


def test_detect_movement_derivative_vs_mainline():
    prior = {"market_prob": 0.5}
    derivative = {
        "market_prob": 0.5045,
        "market": "f5 totals",
        "hours_to_game": 50,
    }
    mainline = {
        "market_prob": 0.5045,
        "market": "h2h",
        "hours_to_game": 50,
    }
    assert mmt.detect_market_movement(derivative, prior)["mkt_movement"] == "up"
    assert mmt.detect_market_movement(mainline, prior)["mkt_movement"] == "same"


def test_detect_movement_hours_to_game():
    prior = {"market_prob": 0.5}
    far = {
        "market_prob": 0.502,
        "market": "f5 totals",
        "hours_to_game": 50,
    }
    close = {
        "market_prob": 0.502,
        "market": "f5 totals",
        "hours_to_game": 6,
    }
    assert mmt.detect_market_movement(far, prior)["mkt_movement"] == "same"
    assert mmt.detect_market_movement(close, prior)["mkt_movement"] == "up"


def test_detect_movement_missing_hours(monkeypatch):
    """Missing hours_to_game should use conservative threshold."""
    prior = {"market_prob": 0.5}
    row = {"market_prob": 0.515, "market": "h2h"}

    # Use a large threshold to prove fallback is not using this value
    from cli import log_betting_evals as lbe

    monkeypatch.setattr(
        lbe, "market_prob_increase_threshold", lambda h, m: 0.02
    )

    assert mmt.detect_market_movement(row, prior)["mkt_movement"] == "up"
