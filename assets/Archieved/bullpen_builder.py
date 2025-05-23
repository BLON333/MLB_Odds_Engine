import csv
from assets.stats_loader import normalize_name, load_pitcher_stats
from assets.probable_pitchers import fetch_probable_pitchers
import pandas as pd

def build_bullpen_for_team(team_abbr, pitcher_stats, max_relievers=5):
    """
    Dynamically builds a bullpen list for a given team using Pitchers.csv data.
    Returns a list of reliever dictionaries with k_rate, bb_rate, stuff_plus, etc.
    Skips relievers missing Stuff+ or HR/FB rate inputs.
    """
    bullpen = []
    used_names = set()

    # Grab all probable starters today to exclude
    matchups = fetch_probable_pitchers()
    for game in matchups.values():
        if "home" in game and "name" in game["home"]:
            used_names.add(normalize_name(game["home"]["name"]))
        if "away" in game and "name" in game["away"]:
            used_names.add(normalize_name(game["away"]["name"]))

    for norm_name, stats in pitcher_stats.items():
        # Skip starters in today's matchups
        if norm_name in used_names:
            continue

        # Skip pitchers from other teams
        if stats.get("team_abbr", "").upper() != team_abbr.upper():
            continue

        # Skip pitchers with missing or invalid Stuff+ or HR/FB
        stuff_plus = stats.get("stuff_plus")
        hr_fb = stats.get("hr_fb_rate")
        if stuff_plus is None or pd.isna(stuff_plus):
            print(f"[❌] Skipping {norm_name.title()} — missing Stuff+")
            continue
        if hr_fb is None or pd.isna(hr_fb):
            print(f"[❌] Skipping {norm_name.title()} — missing HR/FB rate")
            continue

        reliever = {
            "name": stats.get("name", norm_name.title()),
            "k_rate": stats.get("k_rate", 0.20),
            "bb_rate": stats.get("bb_rate", 0.08),
            "stuff_plus": stuff_plus,
            "command_plus": stats.get("command_plus", 100),
            "location_plus": stats.get("location_plus", 100),
            "hr_fb_rate": hr_fb,
            "ip": stats.get("ip", 0.0),
        }

        # Score = Stuff+ + K%*100 – BB%*100
        score = reliever["stuff_plus"] + reliever["k_rate"] * 100 - reliever["bb_rate"] * 100
        reliever["score"] = score
        bullpen.append(reliever)

    # Sort by score and return top N
    bullpen.sort(key=lambda x: x["score"], reverse=True)
    return bullpen[:max_relievers]