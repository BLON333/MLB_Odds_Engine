import sys
import os
import numpy as np
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG

# === League-average batter ===
avg_batter = {
    "name": "avg_batter",
    "handedness": "R",
    "k_rate": 0.225,
    "bb_rate": 0.082,
    "iso": 0.145,
    "avg": 0.245,
    "woba": 0.320
}

# === League-average pitcher template (command_plus will vary) ===
def make_pitcher(command):
    return {
        "name": f"command_{command}",
        "throws": "R",
        "stuff_plus": 100,
        "command_plus": command,
        "location_plus": 100,
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "hr_fb_rate": 0.115,
        "iso_allowed": 0.140
    }

# === Neutral environment ===
env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0}
}

# === Test range of command_plus values ===
command_range = [70, 85, 100, 115, 130]
NUM_SIMS = 3000

print("\n🔬 Command+ Sensitivity Test ({} sims each)".format(NUM_SIMS))

for command in command_range:
    home_lineup = [avg_batter.copy() for _ in range(9)]
    away_lineup = [avg_batter.copy() for _ in range(9)]
    pitcher = make_pitcher(command)

    # Reset logs
    for team in PA_RECAP_LOG:
        for key in PA_RECAP_LOG[team]:
            PA_RECAP_LOG[team][key] = 0

    for _ in range(NUM_SIMS):
        simulate_game(
            home_lineup=home_lineup,
            away_lineup=away_lineup,
            home_pitcher=pitcher,
            away_pitcher=pitcher,
            env=env,
            debug=False
        )

    all_events = {k: PA_RECAP_LOG["HOME"][k] + PA_RECAP_LOG["AWAY"][k] for k in PA_RECAP_LOG["HOME"]}
    total_pa = sum(all_events.values())

    k_pct = 100 * all_events["K"] / total_pa if total_pa else 0
    bb_pct = 100 * all_events["BB"] / total_pa if total_pa else 0
    hr_pct = 100 * all_events["HR"] / total_pa if total_pa else 0
    out_pct = 100 * all_events["OUT"] / total_pa if total_pa else 0

    print(f"\nCommand+: {command}")
    print(f"  K%  : {k_pct:5.2f}%")
    print(f"  BB% : {bb_pct:5.2f}%")
    print(f"  HR% : {hr_pct:5.2f}%")
    print(f"  OUT%: {out_pct:5.2f}%")