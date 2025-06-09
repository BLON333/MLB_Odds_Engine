import os, sys, csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli.log_betting_evals import write_to_csv


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
    }


def test_top_up_written_even_without_market_move(monkeypatch, tmp_path):
    row = _base_row()
    row["entry_type"] = "top-up"
    row["full_stake"] = 1.6
    row["market_prob"] = 0.515
    row["_prior_snapshot"] = {"market_prob": 0.520}
    row["sportsbook"] = "B1"

    existing = {(row["game_id"], row["market"], row["side"]): 1.0}

    monkeypatch.setattr("utils.logging_allowed_now", lambda now=None: True)

    path = tmp_path / "log.csv"
    result = write_to_csv(row, path, existing, {}, {}, dry_run=False, force_log=False)

    assert result is not None
    assert row["stake"] == 0.6

    with open(path) as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 1
    assert float(rows[0]["stake"]) == 0.6
