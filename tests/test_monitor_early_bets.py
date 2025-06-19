import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import cli.monitor_early_bets as meb


def _pending_bet():
    return {
        "game_id": "2025-06-15-AAA@BBB-T1905",
        "market": "h2h",
        "side": "TeamA",
        "market_prob": 0.55,
        "consensus_prob": 0.5,
        "baseline_consensus_prob": 0.5,
        "full_stake": 1.0,
        "hours_to_game": 5.0,
    }


def test_tracker_not_updated_when_write_skipped(monkeypatch):
    bet = _pending_bet()
    pending = {"k": bet}

    monkeypatch.setattr(meb, "load_pending_bets", lambda *_: pending)
    monkeypatch.setattr(meb, "save_pending_bets", lambda *a, **k: None)
    monkeypatch.setattr(meb, "load_existing_stakes", lambda *_: {})

    theme = {}
    monkeypatch.setattr(meb, "load_theme_stakes", lambda: theme)
    monkeypatch.setattr(meb, "save_theme_stakes", lambda *_: theme)
    monkeypatch.setattr(meb, "load_eval_tracker", lambda: {})
    monkeypatch.setattr(meb, "compute_hours_to_game", lambda *a, **k: 5.0)
    monkeypatch.setattr(
        meb,
        "fetch_consensus_for_single_game",
        lambda gid: {bet["market"]: {bet["side"]: {"price": -120}}},
    )
    monkeypatch.setattr(meb, "should_log_bet", lambda row, *a, **k: {**row, "skip_reason": "bad_odds"})

    calls = []
    monkeypatch.setattr(meb, "write_to_csv", lambda row, *a, **k: row)
    monkeypatch.setattr(meb, "record_successful_log", lambda *a, **k: calls.append(True))

    meb.recheck_pending_bets("dummy.json")

    assert not calls
    assert theme == {}


def test_tracker_updated_on_success(monkeypatch):
    bet = _pending_bet()
    pending = {"k": bet}

    monkeypatch.setattr(meb, "load_pending_bets", lambda *_: pending)
    monkeypatch.setattr(meb, "save_pending_bets", lambda *a, **k: None)
    monkeypatch.setattr(meb, "load_existing_stakes", lambda *_: {})

    theme = {}
    monkeypatch.setattr(meb, "load_theme_stakes", lambda: theme)
    monkeypatch.setattr(meb, "save_theme_stakes", lambda *_: theme)
    monkeypatch.setattr(meb, "load_eval_tracker", lambda: {})
    monkeypatch.setattr(meb, "compute_hours_to_game", lambda *a, **k: 5.0)
    monkeypatch.setattr(
        meb,
        "fetch_consensus_for_single_game",
        lambda gid: {bet["market"]: {bet["side"]: {"price": -120}}},
    )
    monkeypatch.setattr(
        meb,
        "should_log_bet",
        lambda row, *a, **k: {**row, "stake": 1.0, "entry_type": "first"},
    )

    def fake_write(row, *_a, **_k):
        return row

    called = []

    def fake_record(row, exist, stakes):
        called.append(row)
        stakes[(row["game_id"], row["market"], row["side"])] = row["stake"]

    monkeypatch.setattr(meb, "write_to_csv", fake_write)
    monkeypatch.setattr(meb, "record_successful_log", fake_record)

    meb.recheck_pending_bets("dummy.json")

    assert called
    key = (bet["game_id"], bet["market"], bet["side"])
    assert theme[key] == 1.0

