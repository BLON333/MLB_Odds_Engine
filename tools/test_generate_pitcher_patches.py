import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from assets.stats_loader import load_pitcher_stats

print("🔁 Running test: loading and estimating HR/FB...")
pitcher_stats = load_pitcher_stats(
    "data/Pitchers.csv",
    "data/Stuff+_Location+.csv",
    "data/statcast.csv",
    patch_hrfb=True
)

print(f"✅ Loaded {len(pitcher_stats)} pitchers with HR/FB values.")
print("📄 Check:")
print("- data/estimated_hrfb_patch.csv → patched HR/FB values")
print("- data/unmatched_names_log.csv → unmatched pitcher names (if any)")
