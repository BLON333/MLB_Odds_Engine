import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.unified_snapshot_generator as usg


def test_build_snapshot_uses_full_game_id(monkeypatch):
    gid = "2025-06-09-ATL@MIL-T1941"
    sims = {gid: {}}
    monkeypatch.setattr(usg, "load_simulations", lambda *_: sims)

    captured = {}

    def fake_build_snapshot_rows(sim_data, odds, min_ev=0.01):
        captured["sim_keys"] = list(sim_data.keys())
        captured["odds_keys"] = list(odds.keys())
        return []

    monkeypatch.setattr(usg, "build_snapshot_rows", fake_build_snapshot_rows)
    monkeypatch.setattr(usg, "expand_snapshot_rows_with_kelly", lambda rows: rows)

    odds_data = {gid: {"h2h": {}}}
    rows = usg.build_snapshot_for_date("2025-06-09", odds_data)

    assert captured["sim_keys"] == [gid]
    assert captured["odds_keys"] == [gid]
    assert rows == []
