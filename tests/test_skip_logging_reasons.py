import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import write_to_csv, send_discord_notification
from core.skip_reasons import SkipReason


def _base_row():
    return {
        "game_id": "gid",
        "market": "h2h",
        "market_class": "main",
        "side": "TeamA",
        "lookup_side": "TeamA",
        "sim_prob": 0.6,
        "fair_odds": -110,
        "market_prob": 0.6,
        "market_fv": -110,
        "consensus_prob": 0.55,
        "pricing_method": "consensus",
        "books_used": {},
        "model_edge": 0.05,
        "market_odds": 110,
        "ev_percent": 6.0,
        "blended_prob": 0.6,
        "blended_fv": -105,
        "hours_to_game": 8.0,
        "stake": 1.2,
        "entry_type": "first",
        "segment": "full_game",
        "segment_label": "mainline",
        "best_book": "draftkings",
        "Start Time (ISO)": "2025-01-01T13:05:00",
        "date_simulated": "2025-01-01",
        "result": "",
        "full_stake": 1.2,
    }


def test_skip_reason_quiet_hours(monkeypatch):
    row = _base_row()
    monkeypatch.setattr(
        "core.utils.logging_allowed_now", lambda now=None, **_: False
    )
    result = write_to_csv(row, "dummy.csv", {}, {}, {}, dry_run=True, force_log=False)
    assert result is None
    assert row["skip_reason"] == SkipReason.QUIET_HOURS.value


def test_skip_reason_no_consensus(monkeypatch):
    row = _base_row()
    row["consensus_prob"] = None
    monkeypatch.setattr(
        "core.utils.logging_allowed_now", lambda now=None, **_: True
    )
    result = write_to_csv(row, "dummy.csv", {}, {}, {}, dry_run=True, force_log=False)
    assert result is None
    assert row["skip_reason"] == SkipReason.NO_CONSENSUS.value


def test_skip_reason_market_not_moved(monkeypatch, tmp_path):
    row = _base_row()
    row["market_prob"] = 0.55
    row["_prior_snapshot"] = {"market_prob": 0.6}
    monkeypatch.setattr(
        "core.utils.logging_allowed_now", lambda now=None, **_: True
    )
    result = write_to_csv(row, tmp_path / "t.csv", {}, {}, {}, dry_run=False, force_log=False)
    assert result is None
    assert row["skip_reason"] == SkipReason.MARKET_NOT_MOVED.value


def test_top_up_skips_movement_check(monkeypatch, tmp_path):
    row = _base_row()
    row["entry_type"] = "top-up"
    row["market_prob"] = 0.55
    row["_prior_snapshot"] = {"market_prob": 0.6}
    row["best_book"] = "draftkings"
    monkeypatch.setattr(
        "core.utils.logging_allowed_now", lambda now=None, **_: True
    )
    result = write_to_csv(row, tmp_path / "t.csv", {}, {}, {}, dry_run=False, force_log=False)
    assert result is not None
    assert "skip_reason" not in row


def test_send_discord_notification_no_webhook(monkeypatch):
    row = _base_row()
    monkeypatch.setattr("cli.log_betting_evals.OFFICIAL_PLAYS_WEBHOOK_URL", "")
    monkeypatch.setattr("cli.log_betting_evals.DISCORD_WEBHOOK_URL", "")
    skipped = []
    send_discord_notification(row, skipped)
    assert row["skip_reason"] == SkipReason.NO_WEBHOOK.value
    assert skipped and skipped[0] is row
