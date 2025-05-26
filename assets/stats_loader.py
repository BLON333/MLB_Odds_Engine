import pandas as pd
import numpy as np
import json
from utils import normalize_name
from core.project_hr_pa import project_hr_pa

# === Safe float fallback ===
def safe_float(val, fallback=0.0):
    try:
        if isinstance(val, list):
            print(f"⚠️ safe_float received list: {val} → using first item")
            val = val[0] if val else fallback
        result = float(val) if pd.notna(val) else fallback
    except Exception:
        result = fallback

    # clamp derived hardhit fallback to prevent extreme values
    if fallback == 0.35:
        result = max(0.3, min(result, 0.5))

    return result


# === Pitcher Stats Loader ===
def load_pitcher_stats(pitcher_file, stuff_file, statcast_file, patch_hrfb=False, verbose=False):
    pitcher_stats = {}

    p_df = pd.read_csv(pitcher_file)
    s_df = pd.read_csv(stuff_file)
    x_df = pd.read_csv(statcast_file)

    for df in (p_df, s_df, x_df):
        df.columns = df.columns.str.strip()

    # Normalize Statcast percentages
    if "barrel_batted_rate" in x_df.columns:
        x_df["barrel_batted_rate"] = x_df["barrel_batted_rate"].apply(lambda x: x / 100.0 if pd.notna(x) and x > 1.0 else x)
        x_df["barrel_batted_rate"] = x_df["barrel_batted_rate"].apply(lambda x: max(x, 0.005) if pd.notna(x) else x)
    if "fb_rate" in x_df.columns:
        x_df["fb_rate"] = x_df["fb_rate"].apply(lambda x: x / 100.0 if pd.notna(x) and x > 1.0 else x)
    if "hardhit_percent" in x_df.columns:
        x_df["hardhit_percent"] = x_df["hardhit_percent"].apply(lambda x: x / 100.0 if pd.notna(x) and x > 1.0 else x)

    for col in ["Name", "player_name", "last_name, first_name"]:
        if col in p_df.columns:
            p_df["norm_name"] = p_df[col].apply(normalize_name)
            break
    else:
        raise KeyError("[❌] No valid name column in Pitchers.csv")

    for col in ["Name", "player_name", "last_name, first_name"]:
        if col in s_df.columns:
            s_df["norm_name"] = s_df[col].apply(normalize_name)
            break
    s_df = s_df.rename(columns={"Stuff+": "stuff_plus", "Location+": "location_plus"})

    if "first_name" in x_df.columns and "last_name" in x_df.columns:
        x_df["norm_name"] = (x_df["first_name"].astype(str) + " " + x_df["last_name"].astype(str)).apply(normalize_name)
    else:
        for col in ["Name", "player_name", "last_name, first_name"]:
            if col in x_df.columns:
                x_df["norm_name"] = x_df[col].apply(normalize_name)
                break

    try:
        with open("pitcher_alias_map.json") as f:
            alias_map_raw = json.load(f)
            alias_map = {normalize_name(k): normalize_name(v) for k, v in alias_map_raw.items()}
        x_df["norm_name"] = x_df["norm_name"].apply(lambda name: alias_map.get(name, name))
    except FileNotFoundError:
        if verbose:
            print("[⚠️] No pitcher alias map found — skipping alias correction.")

    p_df = p_df.rename(columns={"K%": "K_pct", "BB%": "BB_pct", "HR/FB": "HR_FB_pct", "GB%": "GB_pct", "FB%": "FB_pct"})

    if "ISO" not in x_df.columns and "xiso" in x_df.columns:
        x_df["ISO"] = x_df["xiso"]
        if verbose:
            print("[🧠] Using xISO as fallback for ISO")
    if "hardhit_percent" not in x_df.columns and "exit_velocity_avg" in x_df.columns:
        x_df["hardhit_percent"] = x_df["exit_velocity_avg"].apply(
            lambda ev: (35.0 + ((ev - 88) * 1.25)) / 100.0 if pd.notna(ev) else 0.35
        )

    # Optional debug output
    if verbose:
        print("\n[🧪] Available columns in Statcast:")
        print(x_df.columns.tolist())

    valid_names = set(p_df["norm_name"])
    s_df = s_df[s_df["norm_name"].isin(valid_names)]
    x_df = x_df[x_df["norm_name"].isin(valid_names)]

    x_cols_to_merge = [
        "norm_name", "ISO", "barrel_batted_rate", "hardhit_percent",
        "exit_velocity_avg", "launch_angle_avg", "xwobacon",
        "xslg", "xslgdiff", "wobadiff", "sweet_spot_percent"
    ]
    x_df = x_df[[col for col in x_cols_to_merge if col in x_df.columns]]



    merged = p_df.merge(
        s_df[["norm_name", "stuff_plus", "location_plus"]],
        on="norm_name", how="left"
    ).merge(
        x_df,
        on="norm_name", how="left"
    )

    merged = merged.dropna(subset=["K_pct", "BB_pct"])

    for _, row in merged.iterrows():
        n = row["norm_name"]
        hr_fb_val = safe_float(row.get("HR_FB_pct"), 0.115)
        if hr_fb_val is not None and hr_fb_val > 0.5:
            print(f"[⚠️] Suspicious HR/FB rate for {n}: {hr_fb_val:.2%}")

        pitcher_stats[n] = {
            "k_rate": safe_float(row.get("K_pct"), 0.225),
            "bb_rate": safe_float(row.get("BB_pct"), 0.082),
            "hr_fb_rate": hr_fb_val,
            "gb_rate": safe_float(row.get("GB_pct"), 40.0),
            "fb_rate": safe_float(row.get("FB_pct"), 40.0),
            "iso_allowed": safe_float(row.get("ISO"), 0.14),
            "stuff_plus": safe_float(row.get("stuff_plus"), 100),
            "location_plus": safe_float(row.get("location_plus"), 100),
            "command_plus": safe_float(row.get("location_plus"), 100),
            "barrel_batted_rate": safe_float(row.get("barrel_batted_rate"), 0.06),
            "hardhit_percent": safe_float(row.get("hardhit_percent"), 0.35),
            "exit_velocity_avg": safe_float(row.get("exit_velocity_avg"), 88.0),
            "launch_angle_avg": safe_float(row.get("launch_angle_avg"), 13.0),
            "xiso": safe_float(row.get("ISO"), 0.14),
            "xwobacon": safe_float(row.get("xwobacon"), 0.320),
            "xSLG": safe_float(row.get("xslg"), 0.0),
            "xSLG_diff": safe_float(row.get("xslgdiff"), 0.0),
            "xwOBAcon": safe_float(row.get("xwobacon"), 0.0),
            "wOBAdiff": safe_float(row.get("wobadiff"), 0.0),
            "sweet_spot_pct": safe_float(row.get("sweet_spot_percent"), 0.32),
            "HR": safe_float(row.get("HR"), 0),
            "TBF": max(safe_float(row.get("TBF"), 1), 1),
            "IP": safe_float(row.get("IP"), 1),
            "FIP": safe_float(row.get("FIP"), 4.00),  # add if available
            "name": n
        }

    for name, stats in pitcher_stats.items():
        try:
            proj = project_hr_pa(stats)
            stats["hr_pa"] = proj
            stats["enriched"] = True
        except Exception as e:
            stats["hr_pa"] = {}
            stats["enriched"] = False
            if verbose:
                missing = [k for k in ("HR", "TBF", "IP", "exit_velocity_avg", "launch_angle_avg") if stats.get(k) is None or pd.isna(stats.get(k))]
                print(f"[⚠️] {name} missing: {missing} → enriched=False")
                print(f"[❌] HR/PA projection failed for {name} → check input completeness")

    # ✅ MOVE THIS INSIDE FUNCTION
    if verbose:
        print("\n[DEBUG] Columns in p_df:", p_df.columns.tolist())
        print("[DEBUG] Columns in s_df:", s_df.columns.tolist())
        print("[DEBUG] Columns in x_df:", x_df.columns.tolist())
        print("[DEBUG] Sample norm_names in p_df:", p_df['norm_name'].head(5).tolist())
        print("[DEBUG] Sample norm_names in s_df:", s_df.get("norm_name", pd.Series()).head(5).tolist())
        print("[DEBUG] Sample norm_names in x_df:", x_df.get("norm_name", pd.Series()).head(5).tolist())

    return pitcher_stats

# === Batter Stats Loader ===
def load_batter_stats(batter_file, verbose=False):
    df = pd.read_csv(batter_file)
    df.columns = df.columns.str.strip()
    df = df.rename(columns={"K%": "K_pct", "BB%": "BB_pct", "ISO": "ISO", "AVG": "AVG", "wOBA": "wOBA"})

    for col in ["K_pct", "BB_pct", "ISO", "AVG", "wOBA"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["Name", "K_pct", "BB_pct"])

    try:
        with open("batter_alias_map.json") as f:
            raw = json.load(f)
            alias_map = {normalize_name(k): normalize_name(v) for k, v in raw.items()}
    except FileNotFoundError:
        alias_map = {}
        if verbose:
            print("[⚠️] No batter alias map found — proceeding without alias correction.")

    fallback = {"b": "brayan"}
    batter_stats = {}
    for _, row in df.iterrows():
        raw = row["Name"]
        norm = normalize_name(raw)
        if norm.split()[0] in fallback:
            norm = fallback[norm.split()[0]] + " " + " ".join(norm.split()[1:])
        final = alias_map.get(norm, norm)
        name = normalize_name(final)
        batter_stats[name] = {
            "k_rate": row["K_pct"],
            "bb_rate": row["BB_pct"],
            "iso":     row.get("ISO", 0.15),
            "avg":     row.get("AVG", 0.25),
            "woba":    row.get("wOBA", 0.32)
        }

    if verbose:
        print(f"\n[🧠] Loaded batter stats: {len(batter_stats)} names post-alias.")

    return batter_stats
