import os
import sys
import csv
import json
from collections import defaultdict

# Ensure root path for relative imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from assets.stats_loader import normalize_name

BATTERS_CSV = "data/Batters.csv"
ALIAS_OUT_PATH = "missing_batter_alias_suggestions.json"
LINEUP_JSONS = ["data/lineup_data.json", "data/last_scraped_lineup.json"]  # 🔧 Adjust to your actual paths

def load_batter_name_map():
    name_map = {}
    try:
        with open(BATTERS_CSV, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                name = (
                    row.get("Name") or
                    row.get("name") or
                    row.get("player_name", "")
                ).strip().lower()
                if name:
                    tokens = name.split()
                    if len(tokens) >= 2:
                        alias_key = f"{tokens[0][0]} {tokens[-1]}"  # e.g. "w contreras"
                        norm_key = normalize_name(alias_key)
                        name_map[norm_key] = name
    except Exception as e:
        print(f"❌ Failed to read Batters.csv: {e}")
    return name_map

def extract_names_from_lineup_file(path):
    try:
        with open(path) as f:
            data = json.load(f)
        names = set()
        for team, batters in data.items():
            for b in batters:
                if isinstance(b, dict):
                    raw_name = b.get("name", "").strip().lower()
                    if raw_name:
                        names.add(normalize_name(raw_name))
        print(f"📂 Loaded {len(names)} names from {path}")
        return names
    except Exception as e:
        print(f"⚠️ Could not read {path}: {e}")
        return set()

def main():
    batter_name_map = load_batter_name_map()
    print(f"✅ Loaded {len(batter_name_map)} batter alias keys from Batters.csv")

    all_lineup_names = set()
    for path in LINEUP_JSONS:
        all_lineup_names.update(extract_names_from_lineup_file(path))

    proposed_aliases = {}
    for name in sorted(all_lineup_names):
        if name not in batter_name_map:
            tokens = name.split()
            if len(tokens) >= 2 and len(tokens[0]) == 1:
                guessed_key = normalize_name(name)
                match = batter_name_map.get(guessed_key)
                if match:
                    proposed_aliases[name] = match

    if proposed_aliases:
        with open(ALIAS_OUT_PATH, "w") as f:
            json.dump(proposed_aliases, f, indent=2)
        print(f"\n💡 Proposed {len(proposed_aliases)} new alias entries.")
        print(f"📄 Saved to: {ALIAS_OUT_PATH}")
        for k, v in proposed_aliases.items():
            print(f"   → '{k}' → '{v}'")
    else:
        print("✅ No missing aliases found.")

if __name__ == "__main__":
    main()
