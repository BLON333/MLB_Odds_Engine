import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import write_to_csv, send_discord_notification


def _base_row():
    return {
        "game_id": "gid",
        "market": "h2h",
        "side": "TeamA",
        "consensus_prob": 0.55,
        "market_prob": 0.6,
        "full_stake": 1.2,
        "entry_type": "first",
        "market_odds": 110,
        "ev_percent": 6.0,
        "blended_prob": 0.6,
        "blended_fv": -105,
        "stake": 1.2,
    }


def test_skip_reason_quiet_hours(monkeypatch):
    row = _base_row()
    monkeypatch.setattr("utils.logging_allowed_now", lambda now=None: False)
    result = write_to_csv(row, "dummy.csv", {}, {}, {}, dry_run=True, force_log=False)
    assert result is None
    assert row["skip_reason"] == "quiet_hours"


def test_skip_reason_no_consensus(monkeypatch):
    row = _base_row()
    row["consensus_prob"] = None
    monkeypatch.setattr("utils.logging_allowed_now", lambda now=None: True)
    result = write_to_csv(row, "dummy.csv", {}, {}, {}, dry_run=True, force_log=False)
    assert result is None
    assert row["skip_reason"] == "no_consensus"


def test_skip_reason_market_not_moved(monkeypatch, tmp_path):
    row = _base_row()
    row["market_prob"] = 0.55
    row["_prior_snapshot"] = {"market_prob": 0.6}
    monkeypatch.setattr("utils.logging_allowed_now", lambda now=None: True)
    result = write_to_csv(row, tmp_path / "t.csv", {}, {}, {}, dry_run=False, force_log=False)
    assert result is None
    assert row["skip_reason"] == "market_not_moved"


def test_send_discord_notification_no_webhook(monkeypatch):
    row = _base_row()
    monkeypatch.setattr("cli.log_betting_evals.get_discord_webhook_for_market", lambda m: "")
    skipped = []
    send_discord_notification(row, skipped)
    assert row["skip_reason"] == "no_webhook"
    assert skipped and skipped[0] is row
