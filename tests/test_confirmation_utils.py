import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.confirmation_utils import required_market_move, confirmation_strength


def test_required_market_move_endpoints():
    assert required_market_move(0) == pytest.approx(0.006)
    assert required_market_move(24) == pytest.approx(0.018)
    # Hours beyond 24h should cap at 24h threshold
    assert required_market_move(36) == pytest.approx(0.018)


def test_required_market_move_midpoint():
    expected = (1.0 + 2.0 * 12 / 24.0) * 0.006
    assert required_market_move(12) == pytest.approx(expected)


def test_confirmation_strength_example():
    hours = 12
    required = required_market_move(hours)
    strength = confirmation_strength(0.009, hours)
    assert required == pytest.approx(0.012)
    assert strength == pytest.approx(0.009 / required)
