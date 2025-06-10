import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.snapshot_core as sc


def test_fuzzy_odds_match(monkeypatch):
    monkeypatch.setattr(sc, "save_tracker", lambda tracker: None)
    monkeypatch.setattr(sc, "compute_hours_to_game", lambda dt, now=None: 8.0)

    sc.MARKET_EVAL_TRACKER.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()

    sim_gid = "2025-06-09-MIL@CIN-T1307"
    alt_gid = "2025-06-09-MIL@CIN-T1305"

    sims = {
        sim_gid: {
            "markets": [{"market": "h2h", "side": "MIL", "sim_prob": 0.55}]
        }
    }
    odds = {
        alt_gid: {
            "start_time": "2025-06-09T17:05:00Z",
            "h2h": {
                "MIL": {"price": 110},
                "CIN": {"price": -120},
            },
        }
    }

    rows = sc.build_snapshot_rows(sims, odds, min_ev=0.0)

    assert len(rows) == 1
    assert rows[0]["game_id"] == sim_gid
    assert rows[0]["market_odds"] == 110
