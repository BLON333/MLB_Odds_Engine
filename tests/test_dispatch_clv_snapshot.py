import os
import sys
import logging
from datetime import datetime, timedelta

from utils import EASTERN_TZ
import pandas as pd

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


def test_send_empty_notice_called(monkeypatch):
    import core.dispatch_clv_snapshot as dcs

    monkeypatch.setattr(dcs, "WEBHOOK_URL", "http://example.com")
    called = {}

    def fake_load_logged_bets(path):
        return [{}]

    def fake_latest_odds_file(folder="data/market_odds"):
        return "odds.json"

    def fake_load_odds(path):
        return {}

    def fake_build_rows(csv_rows, odds_data, verbose=False, return_counts=False):
        return ([], {"open": 0, "matched": 0, "skipped": 0})

    def fake_send_empty(url, counts=None):
        called["url"] = url

    monkeypatch.setattr(dcs, "load_logged_bets", fake_load_logged_bets)
    monkeypatch.setattr(dcs, "latest_odds_file", fake_latest_odds_file)
    monkeypatch.setattr(dcs.os.path, "exists", lambda p: True)
    monkeypatch.setattr(dcs, "load_odds", fake_load_odds)
    monkeypatch.setattr(dcs, "build_snapshot_rows", fake_build_rows)
    monkeypatch.setattr(dcs, "send_empty_clv_notice", fake_send_empty)

    sys.argv = ["dispatch_clv_snapshot", "--output-discord"]
    dcs.main()

    assert called.get("url") == "http://example.com"


def test_send_snapshot_empty(monkeypatch):
    import core.dispatch_clv_snapshot as dcs
    df = pd.DataFrame()
    sent = {}

    def fake_post(url, json=None, timeout=None, **kwargs):
        sent["content"] = json.get("content")

    monkeypatch.setattr(dcs, "post_with_retries", fake_post)
    dcs.send_snapshot(df, "http://example.com", {})

    assert "No qualifying open bets" in sent.get("content", "")


def test_fuzzy_game_id_match():
    target = "2030-06-16-COL@WSH-T1845"
    alt = "2030-06-16-COL@WSH-T1846"
    rows = [_row(target, "h2h", "COL")]
    odds = {alt: {"h2h": {"COL": {"consensus_prob": 0.55}}}}

    result = build_snapshot_rows(rows, odds)

    assert len(result) == 1


def test_open_bet_retained_until_start(monkeypatch):
    import core.dispatch_clv_snapshot as dcs

    gid = "2031-07-01-NYM@ATL-T1930"
    rows = [_row(gid, "h2h", "NYM")]
    odds = {gid: {"h2h": {"NYM": {"consensus_prob": 0.5}}}}

    start_dt = datetime(2031, 7, 1, 19, 30, tzinfo=EASTERN_TZ)

    monkeypatch.setattr(dcs, "parse_start_time", lambda *_: start_dt)
    monkeypatch.setattr(dcs, "now_eastern", lambda: start_dt - timedelta(minutes=30))

    result = build_snapshot_rows(rows, odds)

    assert len(result) == 1


def test_started_game_is_skipped(monkeypatch):
    import core.dispatch_clv_snapshot as dcs

    gid = "2031-07-01-NYM@ATL-T1930"
    rows = [_row(gid, "h2h", "NYM")]
    odds = {gid: {"h2h": {"NYM": {"consensus_prob": 0.5}}}}

    start_dt = datetime(2031, 7, 1, 19, 30, tzinfo=EASTERN_TZ)

    monkeypatch.setattr(dcs, "parse_start_time", lambda *_: start_dt)
    monkeypatch.setattr(dcs, "now_eastern", lambda: start_dt + timedelta(minutes=1))

    result = build_snapshot_rows(rows, odds)

    assert result == []


def test_missing_start_time_skips_row(monkeypatch):
    import core.dispatch_clv_snapshot as dcs

    gid = "2031-07-02-NYM@ATL"
    rows = [_row(gid, "h2h", "NYM")]
    odds = {}

    monkeypatch.setattr(dcs, "parse_start_time", lambda *_: None)

    result = build_snapshot_rows(rows, odds)

    assert result == []
