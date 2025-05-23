import sys
import json
import os
import csv
import numpy as np
import statistics
from collections import Counter
from scipy.special import logit, expit
from sklearn.linear_model import LinearRegression

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG
from core.project_hr_pa import project_hr_pa

DEBUG = "--debug" in sys.argv

avg_batter = {
    "name": "avg_batter",
    "handedness": "R",
    "k_rate": 0.225,
    "bb_rate": 0.082,
    "iso": 0.125,
    "avg": 0.230,
    "woba": 0.305
}

avg_pitcher = {
    "name": "avg_pitcher",
    "throws": "R",
    "stuff_plus": 100,
    "command_plus": 100,
    "location_plus": 100,
    "k_rate": 0.225,
    "bb_rate": 0.082,
    "hr_fb_rate": 0.115,
    "iso_allowed": 0.140,
    "HR": 20,
    "TBF": 650,
    "IP": 180,
    "barrel_batted_rate": 0.065,
    "exit_velocity": 88.5,
    "launch_angle": 13,
    "role": "SP"
}
avg_pitcher["hr_pa"] = project_hr_pa(avg_pitcher)

env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0}
}

mlb_benchmarks = {
    "K": 22.5,
    "BB": 8.2,
    "1B": 21.5,
    "2B": 5.2,
    "3B": 0.5,
    "HR": 3.0,
    "OUT": 39.1
}

home_lineup = [avg_batter.copy() for _ in range(9)]
away_lineup = [avg_batter.copy() for _ in range(9)]

NUM_SIMS = 100000	
results = []
home_wins = 0

for team in PA_RECAP_LOG:
    for outcome in PA_RECAP_LOG[team]:
        PA_RECAP_LOG[team][outcome] = 0

sim_outcome_counts = {k: [] for k in mlb_benchmarks}

for _ in range(NUM_SIMS):
    result = simulate_game(
        home_lineup=home_lineup,
        away_lineup=away_lineup,
        home_pitcher=avg_pitcher,
        away_pitcher=avg_pitcher,
        env=env,
        debug=False
    )
    results.append(result)
    if result["home_score"] > result["away_score"]:
        home_wins += 1
    if "recap" in result:
        for k in sim_outcome_counts:
            sim_outcome_counts[k].append(result["recap"].get(k, 0))

home_scores = [r["home_score"] for r in results]
away_scores = [r["away_score"] for r in results]
total_scores = [h + a for h, a in zip(home_scores, away_scores)]
run_diffs = [h - a for h, a in zip(home_scores, away_scores)]

# ----------------------------
# Segment Raw Totals/Differentials
# ----------------------------
segment_raw = {}
for cap, key in [(1, "f1"), (3, "f3"), (5, "f5"), (7, "f7")]:
    home_seg = [sum(inn["home_runs"] for inn in r["innings"] if inn["inning"] <= cap) for r in results]
    away_seg = [sum(inn["away_runs"] for inn in r["innings"] if inn["inning"] <= cap) for r in results]
    totals_seg = [h + a for h, a in zip(home_seg, away_seg)]
    diffs_seg = [h - a for h, a in zip(home_seg, away_seg)]
    segment_raw[key] = {"total": totals_seg, "diff": diffs_seg}

TARGET_MEAN = 8.85
TARGET_TOTAL_RUN_SD = 4.65
TARGET_RUN_DIFF_SD = 3.0
TARGET_HOME_MEAN = 4.62
TARGET_AWAY_MEAN = 4.35
TARGET_HOME_SD = 3.35
TARGET_AWAY_SD = 3.20

actual_mean = round(np.mean(total_scores), 3)
actual_std = round(np.std(total_scores), 3)
std_scaling_factor = round(TARGET_TOTAL_RUN_SD / actual_std, 4)

mean_diff = np.mean(run_diffs)
std_diff = np.std(run_diffs)
diff_scaling_factor = round(TARGET_RUN_DIFF_SD / std_diff, 4)

home_mean_factor = round(TARGET_HOME_MEAN / np.mean(home_scores), 4)
away_mean_factor = round(TARGET_AWAY_MEAN / np.mean(away_scores), 4)
home_std_factor = round(TARGET_HOME_SD / np.std(home_scores), 4)
away_std_factor = round(TARGET_AWAY_SD / np.std(away_scores), 4)

scaled_total_scores = [(r - actual_mean) * std_scaling_factor + actual_mean for r in total_scores]
scaled_run_diffs = [(d - mean_diff) * diff_scaling_factor + mean_diff for d in run_diffs]

# Logit win% calibration (mock fitting set, real version would use empirical fit)
sim_win_pct = np.array([0.52, 0.60, 0.67, 0.75])
obs_win_pct = np.array([0.50, 0.56, 0.62, 0.70])
logit_sim = logit(sim_win_pct)
logit_obs = logit(obs_win_pct)
model = LinearRegression().fit(logit_sim.reshape(-1, 1), logit_obs)
logit_a = float(model.intercept_)
logit_b = float(model.coef_[0])

sim_home_win_pct = home_wins / NUM_SIMS
calibrated_home_win_pct = float(expit(logit_a + logit_b * logit(sim_home_win_pct)))

def mode_safe(values):
    try:
        return statistics.mode(values)
    except statistics.StatisticsError:
        return Counter(values).most_common(1)[0][0]

def label_volatility(std_dev):
    if std_dev < 4.2:
        return "\U0001f7e2 LOW"
    elif std_dev < 5.0:
        return "\U0001f7e1 MEDIUM"
    else:
        return "\U0001f534 HIGH"

print("\n\U0001f9ee Run Total & Spread Distribution")
print("-----------------------------------")
print(f"Total Runs → Mean: {np.mean(total_scores):.2f} | Mode: {mode_safe(total_scores)} | SD: {np.std(total_scores):.2f} {label_volatility(np.std(total_scores))}")
print(f"Run Line   → Mean: {np.mean(run_diffs):+.2f} | Mode: {mode_safe(run_diffs):+} | SD: {np.std(run_diffs):.2f}")

print("\n\U0001f52e Calibration Check (Neutral Game)")
print(f"Avg Home Score:  {np.mean(home_scores):.2f}")
print(f"Avg Away Score:  {np.mean(away_scores):.2f}")
print(f"Avg Total Runs:  {np.mean(total_scores):.2f}")

print("\n📈 Logit-Calibrated Win Probability:")
print(f"  - Raw Sim Win % (Home):     {sim_home_win_pct:.4f}")
print(f"  - Calibrated Win % (Home):  {calibrated_home_win_pct:.4f}")

print("\n\U0001f3cb Simulated Outcome Rates vs MLB Benchmarks:")
all_events = {k: PA_RECAP_LOG["HOME"][k] + PA_RECAP_LOG["AWAY"][k] for k in PA_RECAP_LOG["HOME"]}
total_pa = sum(all_events.values())

for outcome, count in all_events.items():
    pct = 100 * count / total_pa if total_pa else 0.0
    bench = mlb_benchmarks.get(outcome, None)
    benchmark_str = f" (MLB: {bench:.1f}%)" if bench else ""
    print(f"  - {outcome:<3}: {count:>6} ({pct:5.2f}%)" + benchmark_str)

print(f"\n\U0001f504 Total Plate Appearances Simulated: {total_pa:,}")

print("\n\U0001f4ca Per-Game Outcome Totals (Mean ± SD):")
for outcome, count in all_events.items():
    per_game = count / NUM_SIMS
    values = sim_outcome_counts.get(outcome)
    std_dev = statistics.stdev(values) if values and len(values) > 1 else 0.0
    print(f"  - {outcome:<3}: {per_game:6.2f} ± {std_dev:.2f} per game")

if DEBUG:
    print("\n\U0001f9e0 HR/PA Projection Used:")
    print(f"  - Projected HR/PA: {avg_pitcher['hr_pa']['hr_pa_projected']:.4f}")
    print(f"  - Empirical HR/PA: {avg_pitcher['hr_pa']['empirical_hr_pa']:.4f}")
    print(f"  - Model Estimate : {avg_pitcher['hr_pa']['model_estimate_hr_pa']:.4f}")
    print(f"  - HR/9 Estimate  : {avg_pitcher['hr_pa']['hr_per_9']:.2f}")

    total_hits = all_events["1B"] + all_events["2B"] + all_events["3B"] + all_events["HR"]
    contact_events = total_pa - all_events["K"] - all_events["BB"]
    contact_rate = 100 * contact_events / total_pa if total_pa else 0.0
    hit_rate = 100 * total_hits / total_pa if total_pa else 0.0
    hit_contact = 100 * total_hits / contact_events if contact_events else 0.0

    print("\n\U0001f4ca Contact Profile:")
    print(f"  - Contact Rate:    {contact_rate:.2f}%")
    print(f"  - Hit Rate:        {hit_rate:.2f}%")
    print(f"  - Hit / Contact:   {hit_contact:.2f}%")

    print("\n\U0001f522 Outcome Rate Breakdown (per 100 PA):")
    for k in mlb_benchmarks:
        rate = 100 * all_events[k] / total_pa
        print(f"  {k:<3}: {rate:>5.2f}")

os.makedirs("logs", exist_ok=True)
with open("logs/calibration_outcomes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Outcome", "Count", "Percent", "Per Game", "MLB Benchmark"])
    for outcome, count in all_events.items():
        pct = 100 * count / total_pa if total_pa else 0.0
        per_game = count / NUM_SIMS
        benchmark = mlb_benchmarks.get(outcome, "")
        writer.writerow([outcome, count, f"{pct:.2f}%", f"{per_game:.2f}", benchmark])

print("\n\U0001f4c4 Exported outcome summary to: logs/calibration_outcomes.csv")

# ----------------------------
# Segment Calibration Factors
# ----------------------------
segment_targets = {
    "f1": {"run_mean": 1.05, "run_sd": 1.55, "diff_sd": 1.55},
    "f3": {"run_mean": 3.30, "run_sd": 2.70, "diff_sd": 2.70},
    "f5": {"run_mean": 5.40, "run_sd": 3.35, "diff_sd": 3.35},
    "f7": {"run_mean": 7.25, "run_sd": 3.85, "diff_sd": 3.85},
}

segment_scaling = {}
for key, raw in segment_raw.items():
    tgt = segment_targets[key]
    actual_mean = np.mean(raw["total"])
    actual_sd = np.std(raw["total"])
    actual_diff_sd = np.std(raw["diff"])

    run_sd_factor = round(tgt["run_sd"] / actual_sd, 4) if actual_sd > 0 else 1.0
    run_mean_shift = round(tgt["run_mean"] - actual_mean, 4)
    diff_sd_factor = round(tgt["diff_sd"] / actual_diff_sd, 4) if actual_diff_sd > 0 else 1.0

    segment_scaling[key] = {
        "run_mean": round(tgt["run_mean"], 2),
        "run_sd": round(tgt["run_sd"], 2),
        "diff_sd": round(tgt["diff_sd"], 2),
        "run_sd_scaling_factor": run_sd_factor,
        "run_mean_shift": run_mean_shift,
        "diff_sd_scaling_factor": diff_sd_factor,
    }

    print(f"\n📊 Segment {key.upper()} Calibration:")
    print(f"  - Total Mean: {actual_mean:.2f} → Target: {tgt['run_mean']} | Δ: {run_mean_shift:+.2f}")
    print(f"  - Total SD:   {actual_sd:.2f} → Target: {tgt['run_sd']} | Scale: x{run_sd_factor:.4f}")
    print(f"  - Diff SD:    {actual_diff_sd:.2f} → Target: {tgt['diff_sd']} | Scale: x{diff_sd_factor:.4f}")

adjustment_factor = round(TARGET_MEAN / actual_mean, 4)

calib_path = "logs/calibration_offset.json"
calib_data = {
    "run_scaling_factor": adjustment_factor,
    "stddev_scaling_factor": std_scaling_factor,
    "run_diff_scaling_factor": diff_scaling_factor,
    "team_total_scaling": {
        "home_mean_factor": home_mean_factor,
        "home_std_factor": home_std_factor,
        "away_mean_factor": away_mean_factor,
        "away_std_factor": away_std_factor,
    },
    "logit_win_pct_calibration": {
        "a": round(logit_a, 4),
        "b": round(logit_b, 4)
    },
    "segment_scaling": {
        k: {
            "run_mean": v["run_mean"],
            "run_sd": v["run_sd"],
            "diff_sd": v["diff_sd"],
        }
        for k, v in segment_scaling.items()
    }
}

try:
    os.makedirs(os.path.dirname(calib_path), exist_ok=True)
    with open(calib_path, "w") as f:
        json.dump(calib_data, f, indent=2)
    print(f"\n\U0001f4be Saved calibration factors → {calib_path}")
    print(f"  - Run Mean Scaling:     x{adjustment_factor:.4f}")
    print(f"  - Total Runs SD Scale:  x{std_scaling_factor:.4f}")
    print(f"  - Run Diff SD Scale:    x{diff_scaling_factor:.4f}")
    print(
        f"  - Team Totals Home → mean x{home_mean_factor:.4f}, sd x{home_std_factor:.4f}"
    )
    print(
        f"  - Team Totals Away → mean x{away_mean_factor:.4f}, sd x{away_std_factor:.4f}"
    )
    print(f"  - Logit Calibration:    logit(p) = {logit_a:.4f} + {logit_b:.4f} * logit(p_sim)")
except Exception as e:
    print(f"⚠️ Failed to write calibration offset: {e}")
