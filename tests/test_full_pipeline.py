import os
import sys
import json
import pandas as pd
from types import SimpleNamespace

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import cli.log_betting_evals as lbe
import core.unified_snapshot_generator as usg
import core.dispatch_live_snapshot as dls


def test_full_pipeline(tmp_path, monkeypatch):
    game_id = "2025-06-15-WSH@ATL-T1905"
    sim = {
        "start_time_iso": "2025-06-15T23:05:00Z",
        "markets": [
            {"market": "h2h", "side": "ATL", "sim_prob": 0.65, "fair_odds": -200},
            {"market": "h2h", "side": "WSH", "sim_prob": 0.35, "fair_odds": 200},
        ],
    }
    odds = {
        game_id: {
            "start_time": "2025-06-15T23:05:00Z",
            "h2h": {
                "ATL": {
                    "price": -110,
                    "consensus_prob": 0.55,
                    "per_book": {"b1": -110, "b2": -105},
                },
                "WSH": {
                    "price": 100,
                    "consensus_prob": 0.45,
                    "per_book": {"b1": 100, "b2": 105},
                },
            },
        }
    }

    sim_dir = tmp_path / "backtest" / "sims" / "2025-06-15"
    sim_dir.mkdir(parents=True)
    with open(sim_dir / f"{game_id}.json", "w") as f:
        json.dump(sim, f)

    monkeypatch.setattr(lbe, "send_discord_notification", lambda *a, **k: None)
    monkeypatch.setattr(lbe, "upload_summary_image_to_discord", lambda *a, **k: None)
    monkeypatch.setattr(lbe, "save_tracker", lambda *a, **k: None)
    monkeypatch.setattr(lbe, "save_theme_stakes", lambda *a, **k: None)
    monkeypatch.setattr(lbe, "save_market_conf_tracker", lambda *a, **k: None)
    monkeypatch.setattr(lbe, "confirmation_strength", lambda *a, **k: 1.0)
    monkeypatch.setattr(lbe, "should_log_bet", lambda bet, *a, **k: {**bet, "entry_type": "first", "stake": bet.get("full_stake", bet.get("stake", 0))})

    def fake_write(row, path, *_args, **_kw):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df = pd.DataFrame([row])
        if os.path.exists(path):
            df_existing = pd.read_csv(path)
            df = pd.concat([df_existing, df], ignore_index=True)
        df.to_csv(path, index=False)
        return row

    monkeypatch.setattr(lbe, "write_to_csv", fake_write)
    lbe.args = SimpleNamespace(min_ev=0.01)

    cwd = os.getcwd()
    os.chdir(tmp_path)
    run_batch = lbe.run_batch_logging
    run_batch(
        eval_folder=str(sim_dir),
        market_odds=odds,
        min_ev=0.01,
        dry_run=False,
        force_log=True,
    )
    os.chdir(cwd)

    log_path = tmp_path / "logs" / "market_evals.csv"
    # Bets exactly at the start time should be skipped
    assert not log_path.exists()
    logged = pd.read_csv(log_path) if log_path.exists() else pd.DataFrame()
    assert logged.empty

    monkeypatch.setattr(usg, "save_tracker", lambda *a, **k: None)
    os.chdir(tmp_path)
    rows = usg.build_snapshot_for_date("2025-06-15", odds)
    os.chdir(cwd)
    assert not any(r["game_id"] == game_id for r in rows)

    snap_path = tmp_path / "snap.json"
    with open(snap_path, "w") as f:
        json.dump(rows, f)

    captured = {}
    monkeypatch.setattr(dls, "send_bet_snapshot_to_discord", lambda df, label, url, debug_counts=None: captured.setdefault("rows", len(df)))
    monkeypatch.setenv("DISCORD_H2H_WEBHOOK_URL", "http://example.com")
    sys.argv = ["dispatch_live_snapshot", "--snapshot-path", str(snap_path), "--output-discord", "--min-ev", "0"]
    dls.main()
    assert captured.get("rows", 0) == 0
