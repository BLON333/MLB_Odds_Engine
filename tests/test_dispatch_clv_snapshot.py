import os
import sys
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.dispatch_clv_snapshot import build_snapshot_rows


def test_skip_row_when_consensus_missing(caplog):
    rows = [
        {
            "game_id": "2030-06-09-MIL@CIN-T1305",
            "market": "h2h",
            "side": "MIL",
            "market_odds": "110",
        }
    ]
    odds = {}  # no odds data for game
    with caplog.at_level(logging.WARNING, logger="core.dispatch_clv_snapshot"):
        result = build_snapshot_rows(rows, odds)
    assert result == []
    assert any("consensus price" in rec.message for rec in caplog.records)
