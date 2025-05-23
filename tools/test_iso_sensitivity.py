import sys
import os
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG

# === Pitcher baseline (fixed suppressor) ===
def make_pitcher():
    return {
        "name": "avg_pitcher",
        "throws": "R",
        "stuff_plus": 100,
        "command_plus": 100,
        "location_plus": 100,
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "hr_fb_rate": 0.115,
        "iso_allowed": 0.140,
        "exit_velocity_avg": None,
        "launch_angle_avg": None,
        "barrel_batted_rate": None,
        "sweet_spot_percent": None,
        "xiso": None,
        "xwobacon": None
    }

# === ISO-based batter generator ===
def make_batter(iso):
    return {
        "name": f"iso_{iso}",
        "handedness": "R",
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "iso": iso,
        "avg": 0.245,
        "woba": 0.320
    }

env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0}
}

NUM_SIMS = 3000
iso_values = [0.08, 0.12, 0.16, 0.20, 0.25, 0.30]

print("\n🔬 ISO Power Sensitivity Test")

for iso in iso_values:
    pitcher = make_pitcher()
    batter = make_batter(iso)
    lineup = [batter.copy() for _ in range(9)]

    for team in PA_RECAP_LOG:
        for k in PA_RECAP_LOG[team]:
            PA_RECAP_LOG[team][k] = 0

    for _ in range(NUM_SIMS):
        simulate_game(lineup, lineup, pitcher, pitcher, env)

    events = {k: PA_RECAP_LOG['HOME'][k] + PA_RECAP_LOG['AWAY'][k] for k in PA_RECAP_LOG['HOME']}
    total_pa = sum(events.values())
    hr_pct = 100 * events['HR'] / total_pa if total_pa else 0
    xbh_pct = 100 * (events['HR'] + events['2B'] + events['3B']) / total_pa if total_pa else 0
    print(f"  ISO {iso:<5}: HR% = {hr_pct:5.2f}% | XBH% = {xbh_pct:5.2f}%")
