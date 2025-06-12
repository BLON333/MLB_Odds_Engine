import os
import sys
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.dispatch_clv_snapshot import build_snapshot_rows


def _row(game_id, market, side, odds="110"):
    return {"game_id": game_id, "market": market, "side": side, "market_odds": odds}


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
    assert any("Skipped 1 bets" in rec.message for rec in caplog.records)


def test_lookup_with_normalized_labels():
    gid = "2030-06-09-WAS@NYM-T1905"

    rows = [_row(gid, "totals", "Over 8.5")]
    odds = {
        gid: {
            "alternate_totals": {
                "Over 8.5": {"consensus_prob": 0.55}
            }
        }
    }

    result = build_snapshot_rows(rows, odds)
    assert len(result) == 1


def test_lookup_team_name_vs_abbr():
    gid = "2030-06-09-WAS@NYM-T1905"
    rows = [_row(gid, "spreads", "Washington Nationals +1.5")]
    odds = {
        gid: {
            "spreads": {
                "WSH +1.5": {"consensus_prob": 0.5}
            }
        }
    }

    result = build_snapshot_rows(rows, odds)
    assert len(result) == 1
