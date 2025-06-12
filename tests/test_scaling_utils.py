import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.scaling_utils import base_model_weight_for_market


def test_base_model_weights():
    cases = {
        "spreads": 0.6,
        "totals": 0.6,
        "h2h": 0.6,
        "spreads_1st_5_innings": 0.8,
        "totals_1st_5_innings": 0.9,
        "spreads_1st_3_innings": 0.9,
        "totals_1st_3_innings": 0.9,
        "totals_1st_1_innings": 0.95,
        "spreads_1st_7_innings": 0.75,
        "totals_1st_7_innings": 0.75,
        "team_totals": 0.7,
    }
    for market, expected in cases.items():
        assert base_model_weight_for_market(market) == expected
