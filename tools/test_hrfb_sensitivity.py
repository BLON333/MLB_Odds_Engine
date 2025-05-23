import sys
import os
import numpy as np
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG

# === Pitcher baseline (hr_fb_rate will vary, Statcast removed) ===
def make_pitcher(hrfb):
    return {
        "name": f"hrfb_{hrfb}",
        "throws": "R",
        "stuff_plus": 100,
        "command_plus": 100,
        "location_plus": 100,
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "hr_fb_rate": hrfb,
        "iso_allowed": 0.140,
        "barrel_batted_rate": None,
        "exit_velocity_avg": None,
        "launch_angle_avg": None,
        "xiso": None,
        "xwobacon": None
    }

# === Batter baseline ===
def make_batter():
    return {
        "name": "avg_batter",
        "handedness": "R",
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "iso": 0.145,
        "avg": 0.245,
        "woba": 0.320
    }

# === Neutral environment ===
env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0}
}

NUM_SIMS = 3000

# === HR/FB rate sensitivity test ===
hrfb_rates = [0.05, 0.09, 0.115, 0.14, 0.18]
print("\n🔬 HR/FB Rate Sensitivity Test (Fallback Path)")
for hrfb in hrfb_rates:
    pitcher = make_pitcher(hrfb)
    batter = make_batter()
    lineup = [batter.copy() for _ in range(9)]

    for team in PA_RECAP_LOG:
        for k in PA_RECAP_LOG[team]:
            PA_RECAP_LOG[team][k] = 0

    for _ in range(NUM_SIMS):
        simulate_game(lineup, lineup, pitcher, pitcher, env)

    events = {k: PA_RECAP_LOG['HOME'][k] + PA_RECAP_LOG['AWAY'][k] for k in PA_RECAP_LOG['HOME']}
    total_pa = sum(events.values())
    hr_pct = 100 * events['HR'] / total_pa if total_pa else 0
    print(f"  HR/FB {hrfb:<5}: HR% = {hr_pct:5.2f}%")
