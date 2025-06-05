import os
import sys
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import market_prob_increase_threshold


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
