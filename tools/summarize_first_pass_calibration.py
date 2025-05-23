import numpy as np
import json

# These should match your real MLB targets
MLB_TARGETS = {
    "mean_total_runs": 8.78,
    "total_runs_sd": 5.15,
    "run_diff_sd": 3.61,
    "segment_targets": {
        "F1": 1.10,
        "F3": 2.80,
        "F5": 4.60,
        "F7": 6.60
    }
}

# Load your most recent calibration results
with open("logs/calibration_offset.json") as f:
    calib = json.load(f)

# Print global calibration insights
print("\n🎯 First Calibration Summary:")

sim_total_mean = round(MLB_TARGETS["mean_total_runs"] / calib["run_scaling_factor"], 2)
sim_total_sd = round(MLB_TARGETS["total_runs_sd"] / calib["stddev_scaling_factor"], 2)
sim_run_diff_sd = round(MLB_TARGETS["run_diff_sd"] / calib["run_diff_scaling_factor"], 2)

print(f"• Simulated Mean Total Runs:    {sim_total_mean} → Target: {MLB_TARGETS['mean_total_runs']}")
print(f"• Simulated Total Run SD:       {sim_total_sd} → Target: {MLB_TARGETS['total_runs_sd']}")
print(f"• Simulated Run Differential SD:{sim_run_diff_sd} → Target: {MLB_TARGETS['run_diff_sd']}")

# Suggest which global scalars to tune
print("\n🛠️ Scalar Tuning Recommendations:")
if abs(sim_total_mean - MLB_TARGETS["mean_total_runs"]) > 0.2:
    print(f"➤ Adjust `run_scaling_factor` closer to: {MLB_TARGETS['mean_total_runs'] / sim_total_mean:.4f}")
if abs(sim_total_sd - MLB_TARGETS["total_runs_sd"]) > 0.2:
    print(f"➤ Adjust `stddev_scaling_factor` closer to: {MLB_TARGETS['total_runs_sd'] / sim_total_sd:.4f}")
if abs(sim_run_diff_sd - MLB_TARGETS["run_diff_sd"]) > 0.2:
    print(f"➤ Adjust `run_diff_scaling_factor` closer to: {MLB_TARGETS['run_diff_sd'] / sim_run_diff_sd:.4f}")

# Segment checks
print("\n📐 Segment-Level Check:")
segment_scaling = calib.get("segment_scaling_factors", {})
for tag, target in MLB_TARGETS["segment_targets"].items():
    sim_value = round(target / segment_scaling[tag]["run_scaling_factor"], 2)
    print(f"  • {tag}: Simulated = {sim_value} → Target: {target}")
    if abs(sim_value - target) > 0.2:
        suggestion = target / sim_value
        print(f"    ➤ Tune `{tag}` run_scaling_factor toward: {suggestion:.4f}")

print("\n✅ Use these as your starting points before modifying other tuning areas.")
