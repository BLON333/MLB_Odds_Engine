import os
import sys
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.confirmation_utils import (
    required_market_move,
    confirmation_strength,
    print_threshold_table,
    book_agreement_score,
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


def test_confirmation_strength_clamped():
    hours = 10
    # Negative observed move should yield minimum strength of 0.0
    assert confirmation_strength(-0.005, hours) == 0.0
    # Observed move well above threshold should clamp to 1.0
    large_move = required_market_move(hours) * 2
    assert confirmation_strength(large_move, hours) == 1.0


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


def test_book_agreement_score_positive():
    data = {
        "pinnacle": 0.01,
        "betonlineag": 0.006,
        "fanduel": 0.007,
        "betmgm": -0.002,
        "draftkings": 0.004,
        "williamhill": 0.008,
        "mybookieag": 0.001,
    }
    score = book_agreement_score(data)
    assert score == pytest.approx(4 / 7, rel=1e-2)


def test_book_agreement_score_negative():
    data = {
        "pinnacle": -0.01,
        "betonlineag": -0.006,
        "fanduel": 0.004,
        "betmgm": -0.003,
        "draftkings": 0.0,
        "williamhill": -0.007,
        "mybookieag": 0.002,
    }
    score = book_agreement_score(data)
    assert score == pytest.approx(3 / 7, rel=1e-2)
