import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.confirmation_utils import (
    required_market_move,
    confirmation_strength,
    print_threshold_table,
)


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


def test_print_threshold_table_output(capsys):
    print_threshold_table()
    out_lines = [line.strip() for line in capsys.readouterr().out.splitlines()]
    expected_header = "[Hours to Game] | [Required Move (%)] | [Movement Units]"
    assert out_lines[0] == expected_header
    key_hours = [24, 18, 12, 6, 3, 1, 0]
    for idx, hours in enumerate(key_hours, start=1):
        threshold = required_market_move(hours)
        percent = threshold * 100.0
        units = threshold / 0.006
        expected = f"{hours:>3}h | {percent:>6.3f}% | {units:>5.2f}".strip()
        assert out_lines[idx] == expected
