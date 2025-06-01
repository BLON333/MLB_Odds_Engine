import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.snapshot_core as sc


def test_tracker_updates_after_expand(monkeypatch):
    # Avoid writing to disk
    monkeypatch.setattr(sc, "save_tracker", lambda tracker: None)

    sc.MARKET_EVAL_TRACKER.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()

    key = "gid:h2h:TeamA"
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE[key] = {
        "market_odds": 120,
        "ev_percent": 5.0,
        "blended_fv": -110,
        "stake": 1.0,
        "market_prob": 0.5,
        "sim_prob": 0.55,
    }
    sc.MARKET_EVAL_TRACKER.update(sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE)

    row = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "blended_prob": 0.55,
        "market_prob": 0.5,
        "sim_prob": 0.55,
        "blended_fv": -110,
        "market_odds": 115,
        "ev_percent": 6.0,
        "stake": 1.1,
        "full_stake": 1.1,
        "_raw_sportsbook": {"B1": 115},
        "best_book": "B1",
    }

    first = sc.expand_snapshot_rows_with_kelly([row])
    assert first[0]["odds_display"].endswith("115")
    assert sc.MARKET_EVAL_TRACKER[key]["market_odds"] == 115

    # Simulate loading tracker for the next snapshot
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.update(sc.MARKET_EVAL_TRACKER)

    second = sc.expand_snapshot_rows_with_kelly([row])
    assert second[0]["odds_display"].strip() == "+115"


def test_frozen_tracker_used_for_each_expanded_row(monkeypatch):
    monkeypatch.setattr(sc, "save_tracker", lambda tracker: None)

    sc.MARKET_EVAL_TRACKER.clear()
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE.clear()

    key = "gid:h2h:TeamA"
    sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE[key] = {
        "market_odds": 120,
        "market_prob": 0.5,
        "sim_prob": 0.55,
        "blended_fv": -110,
    }
    sc.MARKET_EVAL_TRACKER.update(sc.MARKET_EVAL_TRACKER_BEFORE_UPDATE)

    row = {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "blended_prob": 0.55,
        "market_prob": 0.5,
        "sim_prob": 0.55,
        "blended_fv": -110,
        "market_odds": 115,
        "ev_percent": 6.0,
        "stake": 1.1,
        "full_stake": 1.1,
        "_raw_sportsbook": {"B1": 115, "B2": 110},
        "best_book": "B1",
    }

    expanded = sc.expand_snapshot_rows_with_kelly([row])
    assert len(expanded) == 2
    for r in expanded:
        assert r["prev_market_odds"] == 120
        assert r["prev_market_prob"] == 0.5
        assert r["prev_sim_prob"] == 0.55
        assert r["prev_blended_fv"] == -110
