import os
import sys
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.pa_simulator import simulate_pa


def test_strikeout_path():
    batter = {"k_rate": 1.0, "bb_rate": 0.0}
    pitcher = {"k_rate": 1.0, "bb_rate": 0.0, "hr_pa": {"hr_pa_projected": 0.0}}
    result = simulate_pa(batter, pitcher, use_noise=False, rng=np.random.default_rng(1))
    assert result == "K"


def test_walk_path():
    batter = {"k_rate": 0.0, "bb_rate": 1.0}
    pitcher = {"k_rate": 0.0, "bb_rate": 1.0, "hr_pa": {"hr_pa_projected": 0.0}}
    result = simulate_pa(batter, pitcher, use_noise=False, rng=np.random.default_rng(2))
    assert result == "BB"


def test_home_run_path():
    batter = {"k_rate": 0.0, "bb_rate": 0.0}
    pitcher = {"k_rate": 0.0, "bb_rate": 0.0, "hr_pa": {"hr_pa_projected": 1.0}}
    result = simulate_pa(batter, pitcher, use_noise=False, rng=np.random.default_rng(3))
    assert result == "HR"


def test_contact_outcome_type():
    batter = {"k_rate": 0.0, "bb_rate": 0.0}
    pitcher = {"k_rate": 0.0, "bb_rate": 0.0, "hr_pa": {"hr_pa_projected": 0.0}}
    result = simulate_pa(batter, pitcher, use_noise=False, rng=np.random.default_rng(4))
    assert result in {"1B", "2B", "3B", "OUT"}
