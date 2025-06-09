import csv
from collections import defaultdict
import json
from datetime import datetime
from assets.probable_pitchers import fetch_probable_pitchers
from assets.lineup_scraper_selenium import fetch_lineups_selenium
from assets.stats_loader import load_batter_stats, load_pitcher_stats, normalize_name
from utils import parse_game_id
from assets.bullpen_utils import build_bullpen_for_team
from core.project_hr_pa import project_hr_pa
import numpy as np

TEAM_ABBR_FIXES = {
    "CHW": "CWS",
    "WSN": "WSH",
    "TBD": "TB",
    "KCR": "KC",
    "ATH": "OAK"
}

def load_projected_lineups_from_csv(path="data/Batters.csv", key_metric="woba", top_n=9):
    from utils import TEAM_ABBR_FIXES  # this must be present

    from collections import defaultdict
    team_lineups = defaultdict(list)

    try:
        with open(path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                raw_team = row.get("team", "").strip().upper()
                normalized_team = TEAM_ABBR_FIXES.get(raw_team, raw_team)

                name = row.get("name") or row.get("player_name") or ""
                if not normalized_team or not name:
                    continue

                batter = {
                    "name": name.strip(),
                    "handedness": row.get("handedness", "R").upper()
                }

                try:
                    batter[key_metric] = float(row.get(key_metric, 0.320))
                except:
                    batter[key_metric] = 0.320

                team_lineups[normalized_team].append(batter)

        # Trim to top N by metric
        for team in team_lineups:
            team_lineups[team] = sorted(
                team_lineups[team],
                key=lambda x: x.get(key_metric, 0.0),
                reverse=True
            )[:top_n]

        print("✅ Projected lineups loaded for teams:", list(team_lineups.keys()))
        return dict(team_lineups)

    except Exception as e:
        print(f"❌ Error loading projected lineups from {path}: {e}")
        return {}


def build_game_assets(game_id, batter_stats, pitcher_stats, patch_hrfb=False):
    try:
        projected_lineups = load_projected_lineups_from_csv()

        matchups = fetch_probable_pitchers()
        if game_id not in matchups:
            raise ValueError(f"Game ID '{game_id}' not found.")

        matchup = matchups[game_id]
        game_date = "-".join(game_id.split("-")[:3])
        lineup_data = fetch_lineups_selenium(for_date=game_date)

        # 🧼 Normalize scraped team keys using TEAM_ABBR_FIXES
        lineup_data = {
            TEAM_ABBR_FIXES.get(team, team): batters
            for team, batters in lineup_data.items()
        }
        suggest_missing_aliases_from_lineup(lineup_data, batter_stats)


        # 🔒 Defensive check — make sure it's a dict
        if not isinstance(lineup_data, dict):
            print(f"❌ lineup_data is not a dict! Got type: {type(lineup_data)} — contents: {str(lineup_data)[:200]}")
            return None

        # 🔍 Helpful keys print for tracing
        print(f"✅ Lineup data loaded: keys = {list(lineup_data.keys())}")

        parsed_id = parse_game_id(game_id)
        away_abbr_raw, home_abbr_raw = parsed_id["away"], parsed_id["home"]
        away_abbr = TEAM_ABBR_FIXES.get(away_abbr_raw.strip().upper(), away_abbr_raw.strip().upper())
        home_abbr = TEAM_ABBR_FIXES.get(home_abbr_raw.strip().upper(), home_abbr_raw.strip().upper())


        try:
            with open("batter_alias_map.json") as f:
                alias_map_raw = json.load(f)
                batter_alias_map = {normalize_name(k): normalize_name(v) for k, v in alias_map_raw.items()}
        except FileNotFoundError:
            batter_alias_map = {}

        fallback_expansions = {"b": "brayan"}

        # ✅ Guard inside structure_batter
        def structure_batter(batter):
            if not isinstance(batter, dict):
                raise TypeError(f"❌ structure_batter expected dict, got {type(batter)} — value: {batter}")

            raw_name = batter.get("name", "").strip()
            norm_name = normalize_name(raw_name)
            tokens = norm_name.split()
            if tokens and len(tokens[0]) == 1:
                expanded_first = fallback_expansions.get(tokens[0], tokens[0])
                norm_name = expanded_first + " " + " ".join(tokens[1:])
            final_name = normalize_name(batter_alias_map.get(norm_name, norm_name))
            proj = batter_stats.get(final_name)
            if proj is None:
                # Try fuzzy fallback by checking lowercase, normalized name keys
                alt_name = final_name.replace(".", "").lower()
                proj = next((v for k, v in batter_stats.items() if k.replace(".", "").lower() == alt_name), None)

            if proj is None:
                print(f"⚠️ No batter stats found for: '{final_name}' (raw: '{raw_name}')")

            def fallback(val, default):
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return default

            return {
                "name": final_name,
                "handedness": batter.get("handedness", "R"),
                "k_rate": fallback(proj.get("k_rate"), 0.22) if proj else 0.22,
                "bb_rate": fallback(proj.get("bb_rate"), 0.08) if proj else 0.08,
                "iso": fallback(proj.get("iso"), 0.150) if proj else 0.150,
                "avg": fallback(proj.get("avg"), 0.250) if proj else 0.250,
                "woba": fallback(proj.get("woba"), 0.320) if proj else 0.320
            }

        def fallback_lineup(team_abbr, current_lineup, side):
            print(f"\n🔎 fallback_lineup called for {side.upper()} ({team_abbr})")
            print(f"   → Initial batter count: {len(current_lineup)}")
            for i, b in enumerate(current_lineup):
                if not isinstance(b, dict):
                    print(f"❌ [{side}] Batter {i+1} is not a dict: {type(b)} — {b}")
                else:
                    print(f"✅ [BATTER {i+1}] {b.get('name', 'UNKNOWN')}")

            if len(current_lineup) < 9:
                if team_abbr in projected_lineups:
                    print(f"↩️  Using fallback projected lineup from Batters.csv for {team_abbr}")
                    current_lineup = [structure_batter(b) for b in projected_lineups[team_abbr]]

            if len(current_lineup) < 9:
                print(f"[🛠️] Still short — padding with top hitters")
                missing = 9 - len(current_lineup)
                fillers = sorted(batter_stats.values(), key=lambda b: b.get("woba", 0.320), reverse=True)[:missing]
                for b in fillers:
                    print(f"➕ Adding fallback batter: {b.get('name', 'UNKNOWN')}")
                    current_lineup.append({**b, "name": f"auto_{side}_{len(current_lineup)+1}"})

            final_names = [b.get('name', 'UNKNOWN') for b in current_lineup]
            print(f"✅ Final {side.upper()} lineup: {final_names}\n")

            return current_lineup[:9]

        # Lineup extraction and flattening
        home_lineup_raw = lineup_data.get(home_abbr, [])
        away_lineup_raw = lineup_data.get(away_abbr, [])

        print(f"🧪 Raw lineup for {away_abbr}:")
        for i, b in enumerate(away_lineup_raw):
            print(f"   {i+1}. {b} ({type(b)})")


        if isinstance(home_lineup_raw, list) and any(isinstance(x, list) for x in home_lineup_raw):
            print(f"❌ Nested list detected in lineup_data[{home_abbr}] — flattening.")
            home_lineup_raw = [item for sublist in home_lineup_raw for item in (sublist if isinstance(sublist, list) else [sublist])]

        if isinstance(away_lineup_raw, list) and any(isinstance(x, list) for x in away_lineup_raw):
            print(f"❌ Nested list detected in lineup_data[{away_abbr}] — flattening.")
            away_lineup_raw = [item for sublist in away_lineup_raw for item in (sublist if isinstance(sublist, list) else [sublist])]

        # Lineup building with structure validation
        home_lineup = fallback_lineup(
            home_abbr,
            [structure_batter(b if isinstance(b, dict) else {"name": str(b)}) for b in home_lineup_raw],
            "home"
        )

        away_lineup = fallback_lineup(
            away_abbr,
            [structure_batter(b if isinstance(b, dict) else {"name": str(b)}) for b in away_lineup_raw],
            "away"
        )




        def structure_pitcher(name):
            norm_name = normalize_name(name)
            stats = pitcher_stats.get(norm_name)

            if isinstance(stats, list):
                print(f"❌ structure_pitcher → pitcher '{name}' has list-valued stats: {stats}")
                stats = None

            hr_pa_projection = project_hr_pa(stats) if stats else {"hr_pa_projected": None}
            hr_fb = stats.get("hr_fb_rate") if stats else None
            if hr_fb is not None and hr_fb > 1:
                hr_fb = hr_fb / 100.0
            if hr_fb is None or (isinstance(hr_fb, float) and np.isnan(hr_fb)):
                hr_fb_display = "N/A"
            else:
                hr_fb_display = f"{hr_fb:.1%}"
            return {
                "name": name,
                "throws": "R",
                "stuff_plus": stats.get("stuff_plus", 100) if stats else 100,
                "command_plus": stats.get("command_plus", 100) if stats else 100,
                "location_plus": stats.get("location_plus", 100) if stats else 100,
                "k_rate": stats.get("k_rate", 0.22) if stats else 0.22,
                "bb_rate": stats.get("bb_rate", 0.08) if stats else 0.08,
                "hr_fb_rate": hr_fb_display,
                "hr_pa": hr_pa_projection,
                "iso_allowed": stats.get("iso_allowed", 0.140) if stats else 0.140,
                "exit_velocity_avg": stats.get("exit_velocity_avg", "N/A") if stats else "N/A",
                "launch_angle_avg": stats.get("launch_angle_avg", "N/A") if stats else "N/A",
                "barrel_batted_rate": stats.get("barrel_batted_rate", "N/A") if stats else "N/A",
                "HR": stats.get("HR", "N/A") if stats else "N/A",
                "TBF": stats.get("TBF", "N/A") if stats else "N/A",
                "IP": stats.get("IP", "N/A") if stats else "N/A",
                "enriched": stats.get("enriched", False) if stats else False
            }


        pitcher_data = {
            "home": structure_pitcher(matchup["home"]["name"]),
            "away": structure_pitcher(matchup["away"]["name"])
        }

        with open("data/reliever_depth_chart_2025-04-03.json") as f:
            reliever_depth_chart = json.load(f)

        home_bullpen = build_bullpen_for_team(home_abbr, pitcher_stats, reliever_depth_chart)
        away_bullpen = build_bullpen_for_team(away_abbr, pitcher_stats, reliever_depth_chart)


        for rp in home_bullpen:
            rp["role"] = "RP"
            rp["hr_pa"] = project_hr_pa(rp)

        for rp in away_bullpen:
            rp["role"] = "RP"
            rp["hr_pa"] = project_hr_pa(rp)

        return {
            "lineups": {"home": home_lineup, "away": away_lineup},
            "pitchers": pitcher_data,
            "bullpens": {"home": home_bullpen, "away": away_bullpen}
        }

    except Exception as e:
        print(f"❌ Error in build_game_assets for {game_id}: {e}")
        return None

    lineup_data = fetch_lineups_selenium(for_date=game_date)

    # 🔒 Defensive check — make sure it's a dict
    if not isinstance(lineup_data, dict):
        print(f"❌ lineup_data is not a dict! Got type: {type(lineup_data)} — contents: {str(lineup_data)[:200]}")
        return None

    # ✅ Per-team fix
    for team_key, lineup in lineup_data.items():
        if not isinstance(lineup, list):
            print(f"⚠️ lineup_data[{team_key}] is not a list — got {type(lineup)}. Fixing to empty list.")
            lineup_data[team_key] = []

    # 🔍 Helpful keys print for tracing
    print(f"✅ Lineup data loaded: keys = {list(lineup_data.keys())}")




def suggest_missing_aliases_from_lineup(lineup_data, batter_stats):
    import os
    from assets.stats_loader import normalize_name

    alias_map_path = "C:/Users/jason/OneDrive/Documents/Projects/odds-gpt/mlb_odds_engine_V1.1/batter_alias_map.json"
    alias_backup_path = "batter_alias_map_backup.json"
    suggestions_path = "missing_batter_alias_suggestions.json"

    try:
        with open(alias_map_path) as f:
            alias_map = json.load(f)
    except FileNotFoundError:
        alias_map = {}

    existing_keys = {normalize_name(k) for k in batter_stats.keys()}
    current_aliases = {normalize_name(k): normalize_name(v) for k, v in alias_map.items()}

    alias_suggestions = {}
    for team, batters in lineup_data.items():
        for batter in batters:
            raw = batter.get("name", "").strip()
            norm = normalize_name(raw)
            tokens = norm.split()
            if len(tokens) >= 2 and len(tokens[0]) == 1:
                guessed_full = next((
                    full for full in existing_keys
                    if full.endswith(tokens[1]) and full.startswith(tokens[0])
                ), None)
                if guessed_full and norm not in current_aliases and guessed_full not in current_aliases.values():
                    alias_suggestions[norm] = guessed_full

    if alias_suggestions:
        with open(suggestions_path, "w") as f:
            json.dump(alias_suggestions, f, indent=2)
        print(f"\n💡 Auto-suggested {len(alias_suggestions)} new aliases. Saved to {suggestions_path}")

        if os.path.exists(alias_map_path):
                if os.path.exists(alias_backup_path):
                        os.remove(alias_backup_path)
                os.rename(alias_map_path, alias_backup_path)
                print(f"📦 Backed up existing alias map → {alias_backup_path}")

        updated = {**alias_map, **alias_suggestions}
        with open(alias_map_path, "w") as f:
            json.dump(updated, f, indent=2)
        print(f"✅ Merged {len(alias_suggestions)} new aliases into {alias_map_path}")
    else:
        print("✅ No missing aliases found from current lineups.")


