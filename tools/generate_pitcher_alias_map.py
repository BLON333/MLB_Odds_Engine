import pandas as pd
import difflib
import json
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from assets.stats_loader import normalize_name

PITCHER_PATH = "data/Pitchers.csv"
STATCAST_PATH = "data/statcast.csv"
OUTPUT_PATH = "data/pitcher_alias_map.json"
FUZZY_THRESHOLD = 0.8

def generate_alias_map():
    print("🔍 Generating alias map for pitcher name mismatches...\n")

    # === Load CSVs ===
    p_df = pd.read_csv(PITCHER_PATH)
    s_df = pd.read_csv(STATCAST_PATH)

    # === Normalize pitcher names ===
    p_df["norm_name"] = p_df["Name"].apply(normalize_name)

    # === Smart fallback for Statcast name column ===
    for col in ["Name", "player_name", "last_name, first_name"]:
        if col in s_df.columns:
            s_df["norm_name"] = s_df[col].apply(normalize_name)
            break
    else:
        raise KeyError("[❌] No valid name column found in Statcast file.")

    pitcher_names = set(p_df["norm_name"])
    statcast_names = set(s_df["norm_name"])

    unmatched = sorted(pitcher_names - statcast_names)
    alias_map = {}

    for name in unmatched:
        best_match = difflib.get_close_matches(name, statcast_names, n=1, cutoff=FUZZY_THRESHOLD)
        if best_match:
            alias_map[name] = best_match[0]

    with open(OUTPUT_PATH, "w") as f:
        json.dump(alias_map, f, indent=2)

    print(f"✅ Alias map created with {len(alias_map)} entries.")
    print(f"📄 Saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_alias_map()
