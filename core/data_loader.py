from core.config import DEBUG_MODE, VERBOSE_MODE
from assets.stats_loader import load_pitcher_stats, load_batter_stats
from core.project_hr_pa import project_hr_pa  # ✅ Added import
import pandas as pd
import numpy as np
import os

def load_all_stats(patch_hrfb=False, verbose=False):
    """
    Loads batter and pitcher statistics from CSV files.
    Optionally applies HR/FB patch estimation to pitchers.

    Returns:
        (dict, dict): batter_stats, pitcher_stats
    """
    try:
        batter_stats = load_batter_stats("data/Batters.csv")
    except Exception as e:
        print(f"[WARNING] Failed to load Batters.csv: {e}")
        batter_stats = {}

    try:
        pitcher_stats = load_pitcher_stats(
            "data/Pitchers.csv",
            "data/Stuff+_Location+.csv",
            "data/statcast.csv",
            patch_hrfb=patch_hrfb,
            verbose=verbose
        )
    except Exception as e:
        print(f"[WARNING] Failed to load pitcher stats: {e}")
        pitcher_stats = {}

    # ✅ Apply HR/PA projection to each pitcher
    for name, stats in pitcher_stats.items():
        try:
            stats["hr_pa"] = project_hr_pa(stats)
        except Exception as e:
            print(f"[ERROR] Failed to project HR/PA for {name}: {e}")

    # Bulletproof debug print of enriched pitchers
    enriched_sample = {
        k: v for k, v in pitcher_stats.items()
        if any(x in v and v[x] is not None and not pd.isna(v[x]) for x in [
            "exit_velocity_avg", "barrel_batted_rate", "xiso"
        ])
    }

    if verbose:
        print(f"\n🧪 Enrichment Summary: {len(enriched_sample)} pitchers with advanced metrics")
        for name, stats in list(enriched_sample.items())[:3]:
            print(f"\n[📊] Enriched pitcher: {name.title()}")
            for k in ["exit_velocity_avg", "launch_angle_avg", "barrel_batted_rate", "xiso", "xwobacon"]:
                val = stats.get(k)
                if isinstance(val, (int, float)) and not pd.isna(val):
                    print(f"  - {k}: {val:.3f}")
                else:
                    print(f"  - {k}: N/A")

        # Optional: log missing enrichment fields
        missing_fields = [
            (name, [k for k in ["stuff_plus", "hr_fb_rate", "xiso", "exit_velocity_avg"] if pd.isna(stats.get(k)) or stats.get(k) is None])
            for name, stats in pitcher_stats.items()
            if any(stats.get(k) is None or pd.isna(stats.get(k)) for k in ["stuff_plus", "hr_fb_rate", "xiso"])
        ]

        print(f"\n[⚠️] {len(missing_fields)} pitchers missing key fields:")
        for name, fields in missing_fields[:5]:
            print(f"  - {name.title()}: Missing {', '.join(fields)}")

    return batter_stats, pitcher_stats
