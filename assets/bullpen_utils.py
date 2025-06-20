import random
import json
import pandas as pd
from core.utils import normalize_name, normalize_team_abbr_to_name
from core.project_hr_pa import project_hr_pa
from assets.probable_pitchers import fetch_probable_pitchers


def safe_float(val, fallback=0.0):
    try:
        if isinstance(val, list):
            print(f"⚠️ safe_float received list: {val} → using first item")
            val = val[0] if val else fallback
        return float(val)
    except Exception:
        return fallback


def build_bullpen_for_team(team_abbr, pitcher_stats, reliever_depth_chart=None, max_relievers=6):
    """
    Constructs a bullpen for a team from a depth chart (if available) or raw pitcher stats.
    Excludes today's starters and relievers with missing Stuff+/HR-FB data.
    Prioritizes relievers based on scoring formula.
    """
    bullpen = []
    used_names = set()
    
    # Exclude today's starters
    matchups = fetch_probable_pitchers()
    for game in matchups.values():
        for side in ["home", "away"]:
            starter = game.get(side, {}).get("name")
            if starter:
                used_names.add(normalize_name(starter))

    mapped_team = normalize_team_abbr_to_name(team_abbr)

    using_chart = reliever_depth_chart and mapped_team in reliever_depth_chart
    if using_chart:
        reliever_entries = reliever_depth_chart[mapped_team]
    else:
        # Use all pitchers for this team if depth chart is unavailable
        reliever_entries = [
            {"name": stats.get("name", norm_name.title())}
            for norm_name, stats in pitcher_stats.items()
            if stats.get("team_abbr", "").upper() == team_abbr.upper()
        ]

    for entry in reliever_entries:
        reliever_name = entry["name"]
        norm_name = normalize_name(reliever_name)
        stats = pitcher_stats.get(norm_name)
        if using_chart and entry.get("role") in (None, "SP"):
            continue
        if norm_name in used_names:
            continue

        if isinstance(stats, list):
            print(f"❌ Reliever '{reliever_name}' has list-based stats: {stats}")
            continue
        if not isinstance(stats, dict):
            print(f"❌ Invalid pitcher stats for '{reliever_name}': {type(stats)}")
            continue
        if not stats:
            continue

        hr_fb = stats.get("hr_fb_rate", 9.5)
        if hr_fb is not None and hr_fb > 1:
            hr_fb = hr_fb / 100.0

        stuff_plus = stats.get("stuff_plus")
        if stuff_plus is None or pd.isna(stuff_plus):
            continue
        if hr_fb is None or pd.isna(hr_fb):
            continue

        reliever = {
            "name": reliever_name,
            "throws": stats.get("throws", "R"),
            "k_rate": safe_float(stats.get("k_rate"), 0.22),
            "bb_rate": safe_float(stats.get("bb_rate"), 0.08),
            "hr_fb_rate": safe_float(hr_fb, 0.10),
            "stuff_plus": safe_float(stuff_plus, 100),
            "command_plus": safe_float(stats.get("command_plus"), 100),
            "location_plus": safe_float(stats.get("location_plus"), 100),
            "barrel_batted_rate": safe_float(stats.get("barrel_batted_rate"), 0.06),
            "exit_velocity_avg": safe_float(stats.get("exit_velocity_avg"), 88.0),
            "launch_angle_avg": safe_float(stats.get("launch_angle_avg"), 13.0),
            "HR": safe_float(stats.get("HR"), 0),
            "TBF": max(safe_float(stats.get("TBF"), 1), 1),
            "role": "RP"
        }


        reliever["hr_pa"] = project_hr_pa(reliever)

        # Simple leverage scoring
        score = reliever["stuff_plus"] + reliever["k_rate"] * 100 - reliever["bb_rate"] * 100
        reliever["score"] = score

        bullpen.append(reliever)

    bullpen.sort(key=lambda x: x["score"], reverse=True)
    return bullpen[:max_relievers]

RELIEVER_USAGE_COUNTS = {"home": {}, "away": {}}  # optional: track usage across sims

def simulate_reliever_chain(bullpen, num_needed=1, side="home", sim_index=None, debug=False, max_uses_per_reliever=3):
    """
    Selects relievers using IP-weighted probability with optional fatigue suppression.
    Logs reliever weights and picks if debug is enabled.
    Relievers already used `max_uses_per_reliever` times in this sim are skipped.
    """
    if not bullpen or num_needed <= 0:
        return []

    selected = []
    available = bullpen.copy()

    for slot in range(num_needed):
        weights = []
        names = []

        usable = [r for r in available if RELIEVER_USAGE_COUNTS[side].get(r.get("name", "Unknown"), 0) < max_uses_per_reliever]

        for rp in usable:
            ip = rp.get("IP", 1)
            name = rp.get("name", "Unknown")
            usage_count = RELIEVER_USAGE_COUNTS[side].get(name, 0)
            fatigue_penalty = max(0.25, 1 - 0.005 * usage_count)  # 0.5 penalty after ~100 uses
            weight = ip * fatigue_penalty

            weights.append(weight)
            names.append(name)

        if debug and sim_index is not None and slot == 0:
            print(f"\n🎯 Sim {sim_index+1} — {side.title()} Bullpen Draw:")
            for n, w in zip(names, weights):
                print(f"    - {n:20} | Weight (IP x fatigue): {w:.2f}")

        if not usable:
            pick = random.choice(available)
        elif sum(weights) == 0:
            pick = random.choice(usable)
        else:
            pick = random.choices(usable, weights=weights, k=1)[0]

        selected.append(pick)
        available = [r for r in available if r["name"] != pick["name"]]

        # Track usage for fatigue suppression
        name = pick.get("name", "Unknown")
        RELIEVER_USAGE_COUNTS[side][name] = RELIEVER_USAGE_COUNTS[side].get(name, 0) + 1

    return selected
