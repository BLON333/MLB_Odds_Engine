import os, sys, csv
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import write_to_csv, get_exposure_key
from core.should_log_bet import should_log_bet


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


def test_top_up_written_even_without_market_move(monkeypatch, tmp_path):
    row = _base_row()
    row["entry_type"] = "top-up"
    row["full_stake"] = 1.6
    row["market_prob"] = 0.515
    row["_prior_snapshot"] = {"market_prob": 0.520}
    row["best_book"] = "draftkings"

    existing = {(row["game_id"], row["market"], row["side"]): 1.0}
    theme_key = get_exposure_key(row)
    theme_stakes = {theme_key: 1.0}

    tracker_key = f"{row['game_id']}:{row['market']}:{row['side']}"
    reference = {tracker_key: {"market_prob": 0.520}}
    result = should_log_bet(
        row,
        theme_stakes,
        verbose=False,
        reference_tracker=reference,
        existing_csv_stakes=existing,
    )
    assert result["log"] is True
    evaluated = result

    monkeypatch.setattr(
        "core.utils.logging_allowed_now", lambda now=None, **_: True
    )
    monkeypatch.setattr("cli.log_betting_evals.LOGGER_CONFIG", "test", raising=False)

    path = tmp_path / "log.csv"
    result_csv = write_to_csv(evaluated, path, existing, theme_stakes, {}, dry_run=False, force_log=False)

    assert result_csv is not None
    assert evaluated["stake"] == 0.6
    assert evaluated["cumulative_stake"] == pytest.approx(1.6)

    with open(path) as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert float(rows[0]["stake"]) == 0.6
    assert float(rows[0]["cumulative_stake"]) == pytest.approx(1.6)


def test_theme_total_ge_stake_without_csv_record(monkeypatch, tmp_path):
    row = _base_row()
    row["entry_type"] = "top-up"
    row["full_stake"] = 1.2
    row["market_prob"] = 0.515
    row["_prior_snapshot"] = {"market_prob": 0.520}
    row["best_book"] = "draftkings"

    existing = {}
    theme_key = get_exposure_key(row)
    theme_stakes = {theme_key: 1.5}

    tracker_key = f"{row['game_id']}:{row['market']}:{row['side']}"
    reference = {tracker_key: {"market_prob": 0.520}}
    result = should_log_bet(
        row,
        theme_stakes,
        verbose=False,
        reference_tracker=reference,
        existing_csv_stakes=existing,
    )
    assert result["log"] is True
    evaluated = result
    assert evaluated["stake"] == pytest.approx(1.2)

    monkeypatch.setattr("core.utils.logging_allowed_now", lambda now=None, **_: True)
    monkeypatch.setattr("cli.log_betting_evals.LOGGER_CONFIG", "test", raising=False)

    path = tmp_path / "log.csv"
    result_csv = write_to_csv(evaluated, path, existing, theme_stakes, {}, dry_run=False, force_log=False)

    assert result_csv is None


def test_missing_side_skips_write(monkeypatch, tmp_path):
    row = _base_row()
    row.pop("side")

    existing = {}
    theme_stakes = {}

    monkeypatch.setattr("core.utils.logging_allowed_now", lambda now=None, **_: True)
    monkeypatch.setattr("cli.log_betting_evals.LOGGER_CONFIG", "test", raising=False)

    path = tmp_path / "log.csv"
    result_csv = write_to_csv(row, path, existing, theme_stakes, {}, dry_run=False, force_log=False)

    assert result_csv is None
    assert not existing
    assert not theme_stakes
