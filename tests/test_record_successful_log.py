import csv
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import (
    write_to_csv,
    record_successful_log,
    get_exposure_key,
)


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


def test_tracker_updates_only_after_success(monkeypatch, tmp_path):
    row = _base_row()
    row["entry_type"] = "top-up"
    row["_prior_snapshot"] = {"market_prob": 0.6}
    existing = {}
    theme = {}
    monkeypatch.setattr("core.utils.logging_allowed_now", lambda now=None, **_: True)
    path = tmp_path / "t.csv"
    result = write_to_csv(row, path, existing, {}, theme, dry_run=False, force_log=False)
    assert result is not None
    assert not existing
    record_successful_log(result, existing, theme)
    key = (row["game_id"], row["market"], row["side"])
    exposure = get_exposure_key(row)
    assert existing[key] == row["stake"]
    assert theme[exposure] == row["stake"]
    with open(path) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1


def test_skip_does_not_update_tracker(monkeypatch, tmp_path):
    row = _base_row()
    row["consensus_prob"] = None
    existing = {}
    theme = {}
    monkeypatch.setattr("core.utils.logging_allowed_now", lambda now=None, **_: True)
    path = tmp_path / "t.csv"
    result = write_to_csv(row, path, existing, {}, theme, dry_run=False, force_log=False)
    assert result is None
    assert not existing and not theme
