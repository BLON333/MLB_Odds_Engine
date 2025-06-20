from datetime import datetime, timedelta
from core.utils import (
    canonical_game_id,
    disambiguate_game_id,
    to_eastern,
)
import pytz
import requests

def abbreviate_team(name):
    team_map = {
        "Arizona Diamondbacks": "ARI",
        "Atlanta Braves": "ATL",
        "Baltimore Orioles": "BAL",
        "Boston Red Sox": "BOS",
        "Chicago White Sox": "CHW",
        "Chicago Cubs": "CHC",
        "Cincinnati Reds": "CIN",
        "Cleveland Guardians": "CLE",
        "Colorado Rockies": "COL",
        "Detroit Tigers": "DET",
        "Houston Astros": "HOU",
        "Kansas City Royals": "KC",
        "Los Angeles Angels": "LAA",
        "Los Angeles Dodgers": "LAD",
        "Miami Marlins": "MIA",
        "Milwaukee Brewers": "MIL",
        "Minnesota Twins": "MIN",
        "New York Mets": "NYM",
        "New York Yankees": "NYY",
        "Oakland Athletics": "OAK",
        "Philadelphia Phillies": "PHI",
        "Pittsburgh Pirates": "PIT",
        "San Diego Padres": "SD",
        "San Francisco Giants": "SF",
        "Seattle Mariners": "SEA",
        "St. Louis Cardinals": "STL",
        "Tampa Bay Rays": "TB",
        "Texas Rangers": "TEX",
        "Toronto Blue Jays": "TOR",
        "Washington Nationals": "WSH"
    }
    return team_map.get(name, name[:3].upper())

def to_eastern_date(utc_str):
    try:
        utc_dt = datetime.strptime(utc_str, "%Y-%m-%dT%H:%M:%SZ")
        utc_dt = utc_dt.replace(tzinfo=pytz.utc)
        est_dt = utc_dt.astimezone(pytz.timezone("US/Eastern"))
        return est_dt.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR] Failed ET conversion for {utc_str}: {e}")
        return utc_str.split("T")[0]  # fallback

def fetch_probable_pitchers(days_ahead=1):
    dates = [
        (datetime.today() + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days_ahead + 1)
    ]
    matchups = {}

    for date_str in dates:
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule"
            f"?sportId=1&date={date_str}&hydrate=probablePitcher(note)"
        )

        response = requests.get(url)
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch schedule for {date_str}: {response.status_code}")
            continue

        data = response.json()
        games = data.get("dates", [{}])[0].get("games", [])
        print(f"Found {len(games)} games for {date_str}")

        for game in games:
            try:
                utc_ts = game["gameDate"]  # e.g. "2025-04-18T01:05:00Z"
                start_dt = datetime.strptime(utc_ts, "%Y-%m-%dT%H:%M:%SZ")
                start_dt = start_dt.replace(tzinfo=pytz.utc)
                start_et = to_eastern(start_dt)
                corrected_date = start_et.strftime("%Y-%m-%d")

                away_team_name = game["teams"]["away"]["team"]["name"]
                home_team_name = game["teams"]["home"]["team"]["name"]

                away_team = abbreviate_team(away_team_name)
                home_team = abbreviate_team(home_team_name)

                raw_game_id = disambiguate_game_id(
                    corrected_date, away_team, home_team, start_et
                )
                # canonical_game_id preserves the time suffix while normalizing team codes
                game_id = canonical_game_id(raw_game_id)

                away_prob = game["teams"]["away"].get("probablePitcher")
                home_prob = game["teams"]["home"].get("probablePitcher")

                if not away_prob or not home_prob:
                    continue

                away_name = away_prob.get("fullName", "TBD")
                home_name = home_prob.get("fullName", "TBD")
                away_hand = away_prob.get("pitchHand", {}).get("code", "R")
                home_hand = home_prob.get("pitchHand", {}).get("code", "R")

                matchups[game_id] = {
                    "home": {"name": home_name, "throws": home_hand},
                    "away": {"name": away_name, "throws": away_hand}
                }

            except Exception as e:
                print(f"[ERROR] Problem parsing game on {date_str}: {e}")

    return matchups
