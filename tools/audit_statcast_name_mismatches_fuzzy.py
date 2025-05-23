import pandas as pd
import difflib
from assets.stats_loader import normalize_name

PITCHER_PATH = "data/Pitchers.csv"
STATCAST_PATH = "data/statcast.csv"
OUTPUT_PATH = "logs/statcast_name_mismatches_fuzzy.csv"

FUZZY_MATCH_THRESHOLD = 0.8  # Match ratio threshold for close_matches

def audit_statcast_mismatches_fuzzy():
    print("🔍 Auditing Statcast name mismatches with fuzzy suggestions...\n")

    # === Load files ===
    p_df = pd.read_csv(PITCHER_PATH)
    s_df = pd.read_csv(STATCAST_PATH)

    # === Normalize names ===
    p_df["norm_name"] = p_df["Name"].apply(normalize_name)
    s_df["norm_name"] = s_df["last_name, first_name"].apply(normalize_name)

    pitcher_names = set(p_df["norm_name"])
    statcast_names = set(s_df["norm_name"])

    unmatched_in_statcast = sorted(pitcher_names - statcast_names)
    unmatched_in_pitchers = sorted(statcast_names - pitcher_names)

    print(f"📉 Pitchers with no Statcast match: {len(unmatched_in_statcast)}")
    print(f"📉 Statcast entries not matched in Pitchers.csv: {len(unmatched_in_pitchers)}\n")

    # === Suggest close fuzzy matches ===
    fuzzy_suggestions = []
    for name in unmatched_in_statcast:
        matches = difflib.get_close_matches(name, statcast_names, n=1, cutoff=FUZZY_MATCH_THRESHOLD)
        fuzzy_match = matches[0] if matches else ""
        fuzzy_suggestions.append((name, fuzzy_match))

    # === Save to CSV ===
    result_df = pd.DataFrame({
        "Pitcher_not_in_Statcast": [x[0] for x in fuzzy_suggestions],
        "Suggested_Match_in_Statcast": [x[1] for x in fuzzy_suggestions],
        "Statcast_not_in_Pitchers": unmatched_in_pitchers + [""] * max(0, len(fuzzy_suggestions) - len(unmatched_in_pitchers))
    })

    result_df.to_csv(OUTPUT_PATH, index=False)
    print(f"📄 Fuzzy mismatch report saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    audit_statcast_mismatches_fuzzy()
