import json
import requests

def get_park_name(game_id):
    park_by_home_team = {
        "LAA": "Angel Stadium",
        "STL": "Busch Stadium",
        "BAL": "Camden Yards",
        "ARI": "Chase Field",
        "PHI": "Citizens Bank Park",
        "NYM": "Citi Field",
        "DET": "Comerica Park",
        "COL": "Coors Field",
        "LAD": "Dodger Stadium",
        "BOS": "Fenway Park",
        "TEX": "Globe Life Field",
        "CIN": "Great American Ball Park",
        "CWS": "Guaranteed Rate Field",
        "KC": "Kauffman Stadium",
        "MIA": "loanDepot Park",
        "HOU": "Minute Maid Park",
        "WSH": "Nationals Park",
        "OAK": "Oakland Coliseum",
        "SF": "Oracle Park",
        "SD": "Petco Park",
        "PIT": "PNC Park",
        "CLE": "Progressive Field",
        "TOR": "Rogers Centre",
        "SEA": "T-Mobile Park",
        "MIN": "Target Field",
        "TB": "Tropicana Field",
        "ATL": "Truist Park",
        "CHC": "Wrigley Field",
        "NYY": "Yankee Stadium"
    }
    try:
        home_abbr = game_id.split('@')[1].upper()
        return park_by_home_team.get(home_abbr, "League Average")
    except Exception as e:
        print(f"[WARNING] Could not extract park from game_id '{game_id}': {e}")
        return "League Average"

def get_park_factors(park_name):
    try:
        with open("data/park_factors.json") as f:
            park_data = json.load(f)
        return park_data.get(park_name, park_data["League Average"])
    except Exception as e:
        print(f"[ERROR] Failed to load park factors for '{park_name}': {e}")
        return {"hr_mult": 1.0, "single_mult": 1.0}

def get_weather_hr_mult(weather_profile):
    direction = weather_profile.get("wind_direction", "").lower()
    speed = weather_profile.get("wind_speed", 0)

    if direction == "out":
        # allow a bit more juice for extreme out-blowing winds
        return 1.0 + min(speed * 0.01, 0.25)  # before: cap at 0.20
    elif direction == "in":
        return max(1.0 - speed * 0.01, 0.80)
    else:
        return 1.0

def get_noaa_weather(park_name):
    domes = {
        "Rogers Centre",        # TOR
        "Tropicana Field",      # TB
        "Chase Field",          # ARI
        "Globe Life Field",     # TEX (retractable roof)
        "loanDepot Park",       # MIA
        "Minute Maid Park",     # HOU
        "American Family Field" # MIL
    }

    if park_name in domes:
        print(f"[🌐] Skipping NOAA fetch for dome stadium: {park_name}")
        return {
            "wind_direction": "none",
            "wind_speed": 0,
            "temperature": 72,
            "humidity": 50
        }

    try:
        with open("data/stadium_locations.json") as f:
            stadiums = json.load(f)

        location = stadiums.get(park_name, stadiums["League Average"])
        lat, lon = location["lat"], location["lon"]

        metadata_url = f"https://api.weather.gov/points/{lat},{lon}"
        meta_response = requests.get(metadata_url, timeout=5)
        meta_response.raise_for_status()
        grid_info = meta_response.json()["properties"]
        forecast_url = grid_info["forecastHourly"]

        forecast_response = requests.get(forecast_url, timeout=5)
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()["properties"]["periods"][0]

        wind_dir = forecast_data.get("windDirection", "none")
        wind_speed = int(forecast_data.get("windSpeed", "0 mph").split()[0])
        temperature = int(forecast_data.get("temperature", 70))

        humidity = 50  # fallback since NOAA doesn't expose it

        return {
            "wind_direction": wind_dir.lower(),
            "wind_speed": wind_speed,
            "temperature": temperature,
            "humidity": humidity
        }

    except Exception as e:
        print(f"[ERROR] NOAA weather fetch failed for {park_name}: {e}")
        return {
            "wind_direction": "none",
            "wind_speed": 0,
            "temperature": 70,
            "humidity": 50
        }

def compute_weather_multipliers(weather, hitter_side="R", park_orientation="center"):
    temp = weather.get("temperature", 70)
    humidity = weather.get("humidity", 50)
    wind_dir = weather.get("wind_direction", "none").lower()
    wind_speed = weather.get("wind_speed", 0)

    temp_mult = 1.0 + 0.003 * (temp - 70)
    humidity_mult = 1.0 - 0.0015 * (humidity - 50)

    if park_orientation == "center":
        wind_angle_mult = 1.0 + (0.01 * wind_speed if wind_dir == "out" else -0.01 * wind_speed)
    elif park_orientation == "lf" and hitter_side == "R":
        wind_angle_mult = 1.0 + (0.015 * wind_speed if wind_dir == "out" else -0.015 * wind_speed)
    elif park_orientation == "rf" and hitter_side == "L":
        wind_angle_mult = 1.0 + (0.015 * wind_speed if wind_dir == "out" else -0.015 * wind_speed)
    else:
        wind_angle_mult = 1.0

    adi_mult = temp_mult * humidity_mult * wind_angle_mult
    # widen allowable range slightly for extreme conditions
    adi_mult = max(0.85, min(adi_mult, 1.25))  # before: capped at 1.20

    return {
        "temp_mult": round(temp_mult, 4),
        "humidity_mult": round(humidity_mult, 4),
        "wind_angle_mult": round(wind_angle_mult, 4),
        "adi_mult": round(adi_mult, 4)
    }
