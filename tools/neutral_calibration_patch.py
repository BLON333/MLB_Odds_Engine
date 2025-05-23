# Neutral Calibration Patch — Updated for 2022–2024 Benchmarks

import numpy as np
import core.fatigue_modeling
import core.project_hr_pa
import core.pa_simulator
import core.game_simulator

# === 1. Disable Fatigue ===
def no_fatigue(pitcher_stats, pitcher_state):
    return pitcher_stats.copy()
core.fatigue_modeling.apply_fatigue_modifiers = no_fatigue

# === 2. Adjust HR/PA Model to Remove 1.10 Uplift ===
def patched_project_hr_pa(pitcher_data):
    from core.project_hr_pa import math, dynamic_shrink_n, infer_league_avg_hr_pa, safe_float

    HR = pitcher_data.get("HR", 0)
    TBF = pitcher_data.get("TBF", 1)
    IP = pitcher_data.get("IP", 1)
    empirical_hr_pa = HR / TBF

    pitcher_data["barrel_batted_rate"] = safe_float(pitcher_data.get("barrel_batted_rate"), 0.075)
    pitcher_data["exit_velocity"] = safe_float(pitcher_data.get("exit_velocity"), 88.7)
    pitcher_data["launch_angle"] = safe_float(pitcher_data.get("launch_angle"), 12.0)

    barrel_rate = pitcher_data["barrel_batted_rate"]
    sweet_spot_pct = pitcher_data.get("sweet_spot_pct", 0.32)
    xSLG = pitcher_data.get("xSLG", 0.0)
    xwOBAcon = pitcher_data.get("xwOBAcon", 0.0)
    xSLG_diff = pitcher_data.get("xSLG_diff", 0.0)
    wOBAdiff = pitcher_data.get("wOBAdiff", 0.0)

    normalized_ev = (pitcher_data["exit_velocity"] - 85) / 1000
    normalized_la = (pitcher_data["launch_angle"] - 12) / 100

    K_pct = pitcher_data.get("K_pct", 0.0)
    BB_pct = pitcher_data.get("BB_pct", 0.0)

    Stuff_plus = pitcher_data.get("Stuff+", 100.0)
    Location_plus = pitcher_data.get("Location+", 100.0) / 100.0

    norm_stuff = Stuff_plus / 100.0
    FIP = pitcher_data.get("FIP", 4.0)
    norm_FIP = 1 - FIP / 10.0

    role = pitcher_data.get("role", "SP")
    league_avg_hr_pa = pitcher_data.get("league_avg_hr_pa", infer_league_avg_hr_pa(role, Stuff_plus))

    base = 0.0011
    model_estimate_hr_pa = (
        base
        + 0.020 * barrel_rate
        + 0.018 * normalized_ev
        + 0.018 * normalized_la
        + 0.010 * xSLG
        + 0.006 * xwOBAcon
        + 0.010 * sweet_spot_pct
        + 0.005 * xSLG_diff
        + 0.005 * wOBAdiff
        + 0.008 * (1 - K_pct)
        + 0.005 * (1 - BB_pct)
        + 0.004 * norm_stuff
        + 0.010 * Location_plus
        + 0.005 * norm_FIP
    )

    # No global HR boost here
    if TBF >= 900:
        model_estimate_hr_pa *= 0.87
    elif TBF >= 700:
        model_estimate_hr_pa *= 0.93
    elif TBF >= 500:
        model_estimate_hr_pa *= 0.96

    model_estimate_hr_pa = min(max(model_estimate_hr_pa, 0.005), 0.07)

    shrink_n = dynamic_shrink_n(TBF)
    smoothed_hr_pa = (TBF * model_estimate_hr_pa + shrink_n * league_avg_hr_pa) / (TBF + shrink_n)
    hr_per_9 = smoothed_hr_pa * (TBF / IP) * 9

    return {
        "pitcher_id": pitcher_data.get("pitcher_id", None),
        "role": role,
        "hr_pa_projected": smoothed_hr_pa,
        "empirical_hr_pa": empirical_hr_pa,
        "model_estimate_hr_pa": model_estimate_hr_pa,
        "hr_per_9": hr_per_9,
        "league_avg_hr_pa": league_avg_hr_pa
    }


# === 3. Override BIP Distribution with Realistic MLB Split ===
def override_bip_distribution():
    orig_fn = core.pa_simulator.simulate_pa
    def wrapped_pa(*args, **kwargs):
        core.pa_simulator.bip_distribution = [0.44, 0.21, 0.31, 0.04]
        return orig_fn(*args, **kwargs)
    wrapped_pa.__wrapped__ = orig_fn
    core.pa_simulator.simulate_pa = wrapped_pa
override_bip_distribution()

# === 4. Disable Run Compression + Volatility in Game Sim ===
def patched_simulate_game(*args, **kwargs):
    result = core.game_simulator.simulate_game.__wrapped__(*args, **kwargs) if hasattr(core.game_simulator.simulate_game, '__wrapped__') else core.game_simulator.simulate_game(*args, **kwargs)
    result['game_type'] = 'Neutral Calibration'
    for inn in result['innings']:
        inn['home_runs'] = round(inn['home_runs'], 2)
        inn['away_runs'] = round(inn['away_runs'], 2)
    return result

def override_simulate_game():
    orig_fn = core.game_simulator.simulate_game
    def wrapped_game(*args, **kwargs):
        return patched_simulate_game(*args, **kwargs)
    wrapped_game.__wrapped__ = orig_fn
    core.game_simulator.simulate_game = wrapped_game
override_simulate_game()

print("✅ Neutral calibration patch applied (2022–2024 MLB baseline)")
