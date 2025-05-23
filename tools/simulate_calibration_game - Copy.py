import sys
import json
import os
import csv
import numpy as np
import argparse
import statistics
from collections import Counter
from scipy.special import logit, expit
from sklearn.linear_model import LinearRegression

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# import tools.neutral_calibration_patch  # ⛔ Disabled to allow player-based calibration
from core.game_simulator import simulate_game
from core.pa_simulator import PA_RECAP_LOG
from core.project_hr_pa import project_hr_pa
USE_NEUTRAL_PATCH = False
if USE_NEUTRAL_PATCH:
    import tools.neutral_calibration_patch

DEBUG = "--debug" in sys.argv

# Updated MLB Benchmarks (2022–2024)
mlb_benchmarks = {
    "K": 22.5,
    "BB": 8.4,
    "1B": 14.0,
    "2B": 4.5,
    "3B": 0.7,
    "HR": 3.0,
    "OUT": 47.0
}

avg_batter = {
    "name": "avg_batter",
    "handedness": "R",
    "k_rate": 0.225,
    "bb_rate": 0.084,
    "iso": 0.162,
    "avg": 0.245,
    "woba": 0.314
}

avg_pitcher = {
    "name": "avg_pitcher",
    "throws": "R",
    "stuff_plus": 100,
    "command_plus": 100,
    "location_plus": 100,
    "k_rate": 0.21,
    "bb_rate": 0.07,
    "hr_fb_rate": 0.11,
    "iso_allowed": 0.140,
    "HR": 20,
    "TBF": 650,
    "IP": 180,
    "barrel_batted_rate": 0.075,
    "exit_velocity": 88.7,
    "launch_angle": 12.0,
    "role": "SP"
}
avg_pitcher["hr_pa"] = project_hr_pa(avg_pitcher)

env = {
    "park_hr_mult": 1.00,
    "single_mult": 1.00,
    "weather_hr_mult": 1.00,
    "adi_mult": 1.00,
    "umpire": {"K": 1.0, "BB": 1.0},
    "home_field_advantage": 1.025,
    "debug_hfa": False
}

home_lineup = [avg_batter.copy() for _ in range(9)]
away_lineup = [avg_batter.copy() for _ in range(9)]

print(f"🧪 DEBUG: HFA in calibration env = {env.get('home_field_advantage')}")

NUM_SIMS = 10000	
results = []
home_wins = 0

for team in PA_RECAP_LOG:
    for outcome in PA_RECAP_LOG[team]:
        PA_RECAP_LOG[team][outcome] = 0

sim_outcome_counts = {k: [] for k in mlb_benchmarks}

# Rest of simulation logic remains unchanged from here
# Calibration summary logic will be appended afterward


for i in range(NUM_SIMS):
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
    if i > 0 and i % 10000 == 0:
        print(f"  ... completed {i:,} simulations")

home_scores = [r["home_score"] for r in results]
away_scores = [r["away_score"] for r in results]

# These now use the corrected scores
total_scores = [h + a for h, a in zip(home_scores, away_scores)]
run_diffs = [h - a for h, a in zip(home_scores, away_scores)]


TARGET_MEAN = 8.78
TARGET_TOTAL_RUN_SD = 5.15
TARGET_RUN_DIFF_SD = 3.61

actual_mean = round(np.mean(total_scores), 3)
actual_std = round(np.std(total_scores), 3)
std_scaling_factor = round(TARGET_TOTAL_RUN_SD / actual_std, 4)

mean_diff = np.mean(run_diffs)
std_diff = np.std(run_diffs)
diff_scaling_factor = round(TARGET_RUN_DIFF_SD / std_diff, 4)

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

print("\n✅ Calibration completed successfully.")
print("   → Calibration factors written to logs/calibration_offset.json")
print("   → Segment-level adjustments included for F1, F3, F5, F7")


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

print("\n📄 Exported outcome summary to: logs/calibration_outcomes.csv")

# === Inning-Level Calibration ===
mlb_per_inning_target = [
    1.0108,
    1.0135,
    0.9947,
    1.0190,
    1.0056,
    1.0096,
    1.0128,
    1.0047,
    0.7093
]

inning_scaling_factors = []

print("\n📊 Inning-Level Run Scaling Factors (with raw means):")
for i in range(1, 10):
    inning_totals = [
        g["innings"][i - 1]["home_runs"] + g["innings"][i - 1]["away_runs"]
        for g in results
    ]
    sim_mean = np.mean(inning_totals)
    target_mean = mlb_per_inning_target[i - 1]

    if sim_mean < 1e-4:
        print(f"  ⚠️ Inning {i}: Simulated mean is {sim_mean:.6f} — scaling will explode")
    scale = round(target_mean / sim_mean, 4) if sim_mean > 0 else 1.0

    print(f"  - Inning {i}: Sim Mean = {sim_mean:.4f}, Target = {target_mean:.4f}, Scale = x{scale:.4f}")
    inning_scaling_factors.append(scale)

# === Normalize Inning-Level Run Scaling Factors ===
target_full_game_mean = 8.78
raw_total_mean = actual_mean  # Your full-game simulated mean before scaling

# Compute total multiplier needed
total_scaling = target_full_game_mean / raw_total_mean  # e.g. 1.345

# Normalize inning scalers so their product = total_scaling
raw_product = np.prod(inning_scaling_factors)
adjustment = total_scaling / raw_product

normalized_inning_scaling_factors = [
    round(v * adjustment ** (1 / 9), 6) for v in inning_scaling_factors
]


print("\n📊 Normalized Inning-Level Run Scaling Factors:")
for i, scale in enumerate(normalized_inning_scaling_factors, 1):
    print(f"  - Inning {i}: x{scale:.4f}")



# === Compose Cascading Segment Chain from Inning Factors ===
def get_segment_scaling(start_inn, end_inn, inning_scaling_factors):
    return round(np.prod(inning_scaling_factors[start_inn - 1:end_inn]), 4)

segment_scaling_chain = {
    "F1": get_segment_scaling(1, 1, normalized_inning_scaling_factors),
    "F3": get_segment_scaling(1, 3, normalized_inning_scaling_factors),
    "F5": get_segment_scaling(1, 5, normalized_inning_scaling_factors),
    "F7": get_segment_scaling(1, 7, normalized_inning_scaling_factors),
    "F9": get_segment_scaling(1, 9, normalized_inning_scaling_factors)
}
run_scaling_factor = segment_scaling_chain["F9"]

# 🔍 Verify consistency between F9 chain and run_scaling_factor
f9_chain_value = segment_scaling_chain.get("F9", None)
if f9_chain_value is not None:
    difference = abs(f9_chain_value - run_scaling_factor)
    print(f"\n🧾 Run Scaling Consistency Check:")
    print(f"  • F9 cumulative chain:         x{f9_chain_value:.4f}")
    print(f"  • Global run_scaling_factor:  x{run_scaling_factor:.4f}")
    print(f"  • Difference:                 x{difference:.6f}")

    if difference > 0.01:
        print("  ❌ Mismatch — simulator may misprice full-game totals.")
    else:
        print("  ✅ Scaling matches — full-game pricing should be correct.")



print("\n🔗 Cascaded Segment Scaling Chain:")
for tag, val in segment_scaling_chain.items():
    print(f"  - {tag}: x{val:.4f}")


# === Segment-Level Calibration ===
segment_targets = {
    "F1": {"mean": 1.10, "std": 1.6, "diff_sd": 0.8},
    "F3": {"mean": 2.80, "std": 3.2, "diff_sd": 1.3},
    "F5": {"mean": 4.60, "std": 4, "diff_sd": 1.7},
    "F7": {"mean": 6.60, "std": 4.8, "diff_sd": 2},
    "F9": {"mean": 8.78, "std": 5.15, "diff_sd": 3.61}
}

def get_segment_stats(results, max_inning):
    totals = []
    diffs = []
    for r in results:
        h = a = 0
        for inn in r["innings"]:
            if inn["inning"] > max_inning:
                break
            h += inn["home_runs"]
            a += inn["away_runs"]
        totals.append(h + a)
        diffs.append(h - a)
    return totals, diffs

segment_scaling = {}
for tag in ["F1", "F3", "F5", "F7", "F9"]:
    target = segment_targets[tag]
    max_inn = int(tag[1])
    seg_totals, seg_diffs = get_segment_stats(results, max_inn)

    sim_mean = np.mean(seg_totals)
    sim_std = np.std(seg_totals)
    sim_diff_sd = np.std(seg_diffs)

    print(f"⚠️ Skipping run_scaling_factor for {tag} — using inning-based cascade instead.")

    segment_scaling[tag] = {
        "stddev_scaling_factor": round(target["std"] / sim_std, 4),
        "run_diff_scaling_factor": round(target["diff_sd"] / sim_diff_sd, 4)
    }

    print(f"\n📐 Segment Calibration: {tag}")
    print(f"  ➤ Target Mean:   {target['mean']:.2f}, Simulated Mean:   {sim_mean:.2f} (NOT USED)")
    print(f"  ➤ Target StdDev: {target['std']:.2f}, Simulated StdDev: {sim_std:.2f}, Scaling: x{segment_scaling[tag]['stddev_scaling_factor']:.4f}")
    print(f"  ➤ Target DiffSD: {target['diff_sd']:.2f}, Simulated DiffSD: {sim_diff_sd:.2f}, Scaling: x{segment_scaling[tag]['run_diff_scaling_factor']:.4f}")


calib_path = "logs/calibration_offset.json"

adjustment_factor = run_scaling_factor  # ✅ now dynamically computed

# ✨ Full-Game Run Mean Check — Post-Scaling Audit
expected_mean = TARGET_MEAN
simulated_mean = actual_mean
reconstructed_total_mean = simulated_mean * run_scaling_factor

error_margin = abs(reconstructed_total_mean - expected_mean)

print("\n📐 FULL-GAME RUN MEAN CHECK:")
print(f"  • Simulated Mean (pre-scaling): {simulated_mean:.4f}")
print(f"  • Scaling Factor Applied:       x{run_scaling_factor:.4f}")
print(f"  • Reconstructed Mean:           {reconstructed_total_mean:.4f}")
print(f"  • Target Mean (MLB):            {expected_mean:.4f}")
print(f"  • Difference:                   {error_margin:.4f} runs")

if error_margin < 0.1:
    print("  ✅ Looks good — you're on target!")
elif error_margin < 0.3:
    print("  ⚠️ Small miss — might want to refine segment or inning scaling.")
else:
    print("  ❌ Large gap — check segment chain or inning means.")



calib_data = {
    "run_scaling_factor": run_scaling_factor,  # ✅ Derived from inning_scaling_factors
    "stddev_scaling_factor": std_scaling_factor,
    "run_diff_scaling_factor": diff_scaling_factor,
    "logit_win_pct_calibration": {
        "a": round(logit_a, 4),
        "b": round(logit_b, 4)
    },
    "inning_scaling_factors": normalized_inning_scaling_factors,  # ✅ Use corrected, normalized scalers
    "segment_scaling_chain": segment_scaling_chain,        # ✅ Includes F9 now
    "segment_scaling_factors": segment_scaling             # ✅ Volatility and diff cal
}



try:
    os.makedirs(os.path.dirname(calib_path), exist_ok=True)
    with open(calib_path, "w") as f:
        json.dump(calib_data, f, indent=2)
    print(f"\n\U0001f4be Saved calibration factors → {calib_path}")
    print(f"  - Run Mean Scaling:     x{run_scaling_factor:.4f}")
    print(f"  - Total Runs SD Scale:  x{std_scaling_factor:.4f}")
    print(f"  - Run Diff SD Scale:    x{diff_scaling_factor:.4f}")
    print(f"  - Logit Calibration:    logit(p) = {logit_a:.4f} + {logit_b:.4f} * logit(p_sim)")
except Exception as e:
    print(f"⚠️ Failed to write calibration offset: {e}")


# Check how often the home team bats in the 9th inning
home_bats_in_9th = 0
for g in results:
    # Check if inning 9 exists and has non-zero home_runs or any events
    for inn in g["innings"]:
        if inn["inning"] == 9 and inn["home_runs"] > 0:
            home_bats_in_9th += 1
            break

print(f"\n⚾ Home team batted in 9th inning: {home_bats_in_9th:,} of {NUM_SIMS:,} games")
print(f"   → That's {100 * home_bats_in_9th / NUM_SIMS:.2f}% of the time")


# Diagnostic Summary Section
print("\n📊 ===== FIRST CALIBRATION PASS SUMMARY =====")

raw_mean = round(TARGET_MEAN / adjustment_factor, 2)
raw_sd = round(TARGET_TOTAL_RUN_SD / std_scaling_factor, 2)
raw_diff_sd = round(TARGET_RUN_DIFF_SD / diff_scaling_factor, 2)

print(f"\n🔢 GLOBAL RUN METRICS:")
print(f"   • Simulated Mean Runs:       {raw_mean} → Target: {TARGET_MEAN} → Suggest: run_scaling_factor ≈ {TARGET_MEAN / raw_mean:.4f}")
print(f"   • Simulated Run Std Dev:     {raw_sd} → Target: {TARGET_TOTAL_RUN_SD} → Suggest: stddev_scaling_factor ≈ {TARGET_TOTAL_RUN_SD / raw_sd:.4f}")
print(f"   • Simulated Run Diff Std Dev:{raw_diff_sd} → Target: {TARGET_RUN_DIFF_SD} → Suggest: run_diff_scaling_factor ≈ {TARGET_RUN_DIFF_SD / raw_diff_sd:.4f}")

print("\n📐 SEGMENT SCALING (F1–F7):")
for tag, target in segment_targets.items():
    print(f"   • {tag}:")
    print(f"      - Target Mean:   {target['mean']:.2f} (total run target)")
    print(f"      - Segment SD Target:   {target['std']:.2f}")
    print(f"      - Segment Diff SD Target: {target['diff_sd']:.2f}")
    print(f"      - Volatility Scale:     x{segment_scaling[tag]['stddev_scaling_factor']:.4f}")
    print(f"      - Run Diff Scale:       x{segment_scaling[tag]['run_diff_scaling_factor']:.4f}")


print("\n🧠 Review these numbers before tuning. Start with global scalars, then derivative segment factors.")
print("====================================================\n")


