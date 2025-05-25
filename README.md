# ⚾ MLB Monte Carlo Simulation Engine

This project is a modular, data-driven Monte Carlo simulation engine for modeling MLB games and pricing betting markets. It supports game-level, inning-level, and plate appearance-level simulations using player projections, park/weather factors, bullpen fatigue, and more.

---

## 🔧 Features

- Simulates full MLB games with detailed inning logs
- Weather and park factor adjustments (NOAA-integrated)
- Pitcher fatigue + TTO effects
- Bullpen usage modeling with reliever chaining
- Moneyline and total pricing (with American odds)
- Full slate simulation and PMF summaries
- Modular architecture for plug-and-play enhancements

---

## 📁 Core Modules

| File | Description |
|------|-------------|
| `simulate_game_and_price_market.py` | Main driver for simulating and pricing a game |
| `game_simulator.py` | Simulates full games inning-by-inning |
| `half_inning_simulator.py` | Handles per-half-inning simulation |
| `pa_simulator.py` | Plate appearance outcome engine |
| `env_builder.py` | Constructs park/weather/environment context |
| `bullpen_builder.py` | Dynamically builds bullpens from data |
| `bullpen_utils.py` | Reliever selection logic, fatigue filters, roles |
| `market_pricer.py` | Converts sim results to fair moneyline/total odds |
| `stats_loader.py` | Loads and enriches player projections |
| `summary_formatter.py` | Generates human-readable betting summaries |
| `cli/run_distribution_simulator.py` | PMF + distribution simulation for totals |
| `cli/run_full_slate_entry.py` | Simulates all games on a slate/date |
| `noaa_weather.py` | Alternative NOAA wind/temperature fetcher |
| `lineup_scraper_selenium.py` | Scrapes FantasyData lineups using Selenium |
| `probable_pitchers.py` | Pulls MLB probable starters from StatsAPI |
| `fatigue_modeling.py` | Applies TTO and pitch count adjustments |
| `test_weighted_reliever_selection.py` | Test harness for reliever chain logic |

---

## 🚀 Setup Instructions

1. **Install dependencies:**

```bash
pip install pandas numpy matplotlib beautifulsoup4 selenium


Download required data:

Batters.csv

Pitchers.csv

Stuff+_Location+.csv

xSLG.csv

reliever_depth_chart_YYYY-MM-DD.json

Set up Selenium ChromeDriver path in lineup_scraper_selenium.py

🧪 Usage Examples

Simulate and price a single game:
python simulate_game_and_price_market.py 2025-04-04-TEX@HOU moneyline


Simulate full distribution (PMF) of total runs:
python cli/run_distribution_simulator.py 2025-04-04-TEX@HOU

Simulate and price entire slate:
python cli/run_full_slate_entry.py 2025-04-04 --csv

Track closing line value:
python cli/closing_odds_monitor.py

Run the full automation loop (which also launches the monitor):
python cli/auto_sim_and_log_loop.py

Windows users can start just the monitor via `launch_closing_odds_monitor.bat`.

## Closing Odds & CLV Tracking

These tools help measure closing line value (CLV) for your logged bets.

* **closing_odds_monitor.py** – runs continuously and checks the Odds API for
  games starting within 15 minutes. When closing odds are found it saves a
  snapshot under `data/closing_odds/` and posts a Discord alert summarizing the
  expected value of any logged bets for that game. Set `DISCORD_ALERT_WEBHOOK_URL`
  in your `.env` to enable alerts.
* **closing_odds_fetcher.py** – fetches closing odds for every game ID in your
  `logs/market_evals.csv` and writes a JSON file for the day. Useful after games
  finish if the monitor was not running.
* **update_clv_column.py** – merges the saved closing odds back into
  `market_evals.csv`, calculating `clv_percent`, `model_clv_percent` and a
  `clv_class` label for quick filtering.


📈 Output Fields
Moneyline Pricing Example:
{
  "home": {
    "prob": 0.562,
    "fair_odds": -128.57
  },
  "away": {
    "prob": 0.438,
    "fair_odds": +128.31
  }
}


Total Market Output:
{
  "line": 9.5,
  "total_score": 10,
  "over": true,
  "under": false
}


✅ To Do / Roadmap
Add bullpen fatigue log across days

Integrate custom umpire bias profiles

Add player-level regression/tuning interface

Store sim results to DB or JSON for tracking

Web dashboard or CLI enhancements

📬 Feedback & Contributions
This engine is modular and open to extensions. If you have ideas for improvement or encounter any issues, feel free to open a discussion or reach out.


---

Let me know if you'd like me to retry generating a downloadable `.md` file or push it to a GitHub-compatible format!


