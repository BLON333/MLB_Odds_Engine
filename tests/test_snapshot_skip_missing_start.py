import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.snapshot_core as sc


def test_skip_when_start_time_missing():
    sc.MARKET_EVAL_TRACKER.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()

    sims = {
        "2030-06-10-ARI@COL": {
            "markets": [{"market": "h2h", "side": "ARI", "sim_prob": 0.6}]
        }
    }

    odds = {}

    rows = sc.build_snapshot_rows(sims, odds, min_ev=0.0)

    assert rows == []
