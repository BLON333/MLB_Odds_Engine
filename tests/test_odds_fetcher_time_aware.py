import os
import sys
import io

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import core.odds_fetcher as of
import core.consensus_pricer as cp

class DummyResp:
    def __init__(self, data, status=200):
        self._data = data
        self.status_code = status
        self.text = "OK"
    def json(self):
        return self._data


def _patch_common(monkeypatch):
    # avoid file writes
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    monkeypatch.setattr(sys.modules['builtins'], "open", lambda *a, **k: io.StringIO(), raising=False)
    monkeypatch.setattr(of, "normalize_odds", lambda gid, offers: {"h2h": {"A": {"price": 100}}, "start_time": "2025-06-09T13:05:00-04:00"})
    monkeypatch.setattr(of, "extract_per_book_odds", lambda *a, **k: {})
    monkeypatch.setattr(cp, "calculate_consensus_prob", lambda *a, **k: ({}, None))
    monkeypatch.setattr(of, "ODDS_API_KEY", "TESTKEY")


def test_fetch_market_odds_time_aware(monkeypatch):
    events = [
        {
            "id": "e1",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T17:05:00Z",
        },
        {
            "id": "e2",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T21:05:00Z",
        },
    ]
    odds_calls = []

    def fake_get(url, params=None):
        if url == of.EVENTS_URL:
            return DummyResp(events)
        if url == of.EVENT_ODDS_URL.format(event_id="e1"):
            odds_calls.append("e1")
            return DummyResp({
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "a", "price": 100}
                                ],
                            }
                        ],
                    }
                ]
            })
        if url == of.EVENT_ODDS_URL.format(event_id="e2"):
            odds_calls.append("e2")
            return DummyResp({"bookmakers": []})
        raise AssertionError(f"Unexpected URL {url}")

    _patch_common(monkeypatch)
    monkeypatch.setattr(of.requests, "get", fake_get)

    gid = "2025-06-09-MIL@CIN-T1305"
    result = of.fetch_market_odds_from_api([gid])
    assert gid in result
    assert odds_calls == ["e1"]


def test_fetch_consensus_single_game_time_aware(monkeypatch):
    events = [
        {
            "id": "e1",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T17:05:00Z",
        },
        {
            "id": "e2",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T21:05:00Z",
        },
    ]
    odds_calls = []

    def fake_get(url, params=None):
        if url == of.EVENTS_URL:
            return DummyResp(events)
        if url == of.EVENT_ODDS_URL.format(event_id="e1"):
            odds_calls.append("e1")
            return DummyResp({
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "a", "price": 100}
                                ],
                            }
                        ],
                    }
                ]
            })
        if url == of.EVENT_ODDS_URL.format(event_id="e2"):
            odds_calls.append("e2")
            return DummyResp({"bookmakers": []})
        raise AssertionError(f"Unexpected URL {url}")

    _patch_common(monkeypatch)
    monkeypatch.setattr(of.requests, "get", fake_get)

    gid = "2025-06-09-MIL@CIN-T1305"
    result = of.fetch_consensus_for_single_game(gid)
    assert result is not None
    assert odds_calls == ["e1"]


def test_fetch_consensus_single_game_no_time(monkeypatch):
    events = [
        {
            "id": "e1",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T17:05:00Z",
        },
        {
            "id": "e2",
            "home_team": "Cincinnati Reds",
            "away_team": "Milwaukee Brewers",
            "commence_time": "2025-06-09T21:05:00Z",
        },
    ]
    odds_calls = []

    def fake_get(url, params=None):
        if url == of.EVENTS_URL:
            return DummyResp(events)
        if url == of.EVENT_ODDS_URL.format(event_id="e1"):
            odds_calls.append("e1")
            return DummyResp({
                "bookmakers": [
                    {
                        "key": "fanduel",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "a", "price": 100}
                                ],
                            }
                        ],
                    }
                ]
            })
        if url == of.EVENT_ODDS_URL.format(event_id="e2"):
            odds_calls.append("e2")
            return DummyResp({"bookmakers": []})
        raise AssertionError(f"Unexpected URL {url}")

    _patch_common(monkeypatch)
    monkeypatch.setattr(of.requests, "get", fake_get)

    gid = "2025-06-09-MIL@CIN"
    result = of.fetch_consensus_for_single_game(gid)
    assert result is not None
    assert odds_calls == ["e1"]
