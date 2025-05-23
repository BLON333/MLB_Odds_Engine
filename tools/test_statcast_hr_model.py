import sys
import os
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG

# === Controlled pitcher with statcast inputs ===
def make_pitcher(ev, la, barrel_rate):
    return {
        "name": f"ev{ev}_la{la}_barrel{barrel_rate}",
        "throws": "R",
        "stuff_plus": 100,
        "command_plus": 100,
        "location_plus": 100,
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "hr_fb_rate": 0.115,  # not used, but present
        "iso_allowed": 0.140,
        "exit_velocity_avg": ev,
        "launch_angle_avg": la,
        "barrel_batted_rate": barrel_rate * 100,  # as percentage
        "sweet_spot_percent": 33.0,  # safe default
        "xiso": 0.145,
        "xwobacon": 0.360
    }

# === Average batter ===
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

env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0}
}

# === Test range of EV & Barrel Rates ===
ev_values = [86, 88, 90, 92, 94]
barrel_rates = [0.05, 0.10, 0.15, 0.20]
NUM_SIMS = 3000

print("\n🔬 Statcast HR Model Sensitivity Test")

for barrel in barrel_rates:
    print(f"\nBarrel Rate: {barrel:.2f}")
    for ev in ev_values:
        pitcher = make_pitcher(ev=ev, la=20, barrel_rate=barrel)
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
        print(f"  EV {ev:<2}: HR% = {hr_pct:.2f}%")
