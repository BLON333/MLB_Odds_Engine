# 📊 Data Flow & Pipeline Guide

This document outlines how the automation pieces connect together and what files are generated along the way.

## End-to-End Pipeline

```
Simulation -> Logging -> Snapshot -> Notification
```

1. **Simulation** – Monte Carlo processes simulate upcoming games and write results under `backtest/sims/`.
2. **Logging** – Bet evaluations and market odds are appended to CSV logs in `logs/`.
3. **Snapshot** – Periodic scripts capture market snapshots (`market_snapshot_*.json`) in `backtest/` for later analysis.
4. **Notification** – Discord webhooks deliver alerts or summaries once snapshots are ready.

A simplified flow diagram:

```
+-----------+     +-------+     +----------+     +-------------+
| simulate  | --> | logs  | --> | snapshot | --> | notification|
+-----------+     +-------+     +----------+     +-------------+
```

## Expected Folders & Files

- `logs/` – rolling CSV logs like `market_evals.csv` and `bet_history.csv`.
- `backtest/sims/` – saved simulation results and `market_snapshot_*.json` files.
- `data/trackers/` – JSON trackers such as `market_conf_tracker.json` for stateful processes.

Ensure these directories exist before running automation scripts.

## Environment Variables (.env)

Configuration is loaded from a `.env` file in the project root. Important keys include:

- `DISCORD_SIM_ONLY_MAIN_WEBHOOK_URL` – webhook for simulation-only snapshots.
- `DISCORD_ALERT_WEBHOOK_URL` and `DISCORD_ALERT_WEBHOOK_URL_2` – channels for closing line value alerts.
- `DISCORD_WEBHOOK_URL`, `DISCORD_TOTALS_WEBHOOK_URL`, `DISCORD_H2H_WEBHOOK_URL`, `DISCORD_SPREADS_WEBHOOK_URL`, `OFFICIAL_PLAYS_WEBHOOK_URL` – destinations for betting logs.
- `ODDS_API_KEY` – Odds API key used when fetching market lines.
- `QUIET_HOURS_START` and `QUIET_HOURS_END` – hour window (ET) to suppress routine Discord messages.
- `SIM_INTERVAL` and `LOG_INTERVAL` – intervals used by `auto_sim_and_log_loop.py`; can be overridden for custom schedules.

Set these variables in `.env` before running any scripts so the notification and odds fetching steps work correctly.
