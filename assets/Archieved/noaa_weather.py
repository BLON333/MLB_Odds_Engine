import requests
import json

# === STEP 1: Stadium Coordinates ===
# These are static lat/lon for each MLB park. Extend as needed.
STADIUM_COORDS = {
    "Camden Yards": {"lat": 39.2839, "lon": -76.6218},
    "Coors Field": {"lat": 39.7559, "lon": -104.9942},
    "Fenway Park": {"lat": 42.3467, "lon": -71.0972},
    "Petco Park": {"lat": 32.7073, "lon": -117.1566},
    "Wrigley Field": {"lat": 41.9484, "lon": -87.6553},
    "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262}
    # Add more parks here...
}

# === STEP 2: NOAA Grid Lookup ===
def get_gridpoint(lat, lon):
    url = f"https://api.weather.gov/points/{lat},{lon}"
    r = requests.get(url, headers={"User-Agent": "MLB-Odds-Engine"})
    if r.status_code != 200:
        raise ValueError(f"Failed to get NOAA gridpoint: {r.text}")
    data = r.json()
    grid = data["properties"]
    return grid["gridId"], grid["gridX"], grid["gridY"]

# === STEP 3: Get Forecast from Grid ===
def get_hourly_forecast(grid_id, x, y):
    url = f"https://api.weather.gov/gridpoints/{grid_id}/{x},{y}/forecast/hourly"
    r = requests.get(url, headers={"User-Agent": "MLB-Odds-Engine"})
    if r.status_code != 200:
        raise ValueError(f"Failed to get NOAA forecast: {r.text}")
    return r.json()

# === STEP 4: Extract Wind & Temp for Target Time ===
def extract_game_weather(forecast_json, game_hour):
    for period in forecast_json["properties"]["periods"]:
        if period["startTime"].endswith(f"T{game_hour:02}:00:00-04:00"):
            return {
                "wind_speed": int(period["windSpeed"].split()[0]),
                "wind_direction": period["windDirection"],
                "temperature": period["temperature"]
            }
    raise ValueError("Could not find weather for game hour.")

# === TEST EXAMPLE ===
if __name__ == "__main__":
    park = "Camden Yards"
    hour = 19  # 7:00 PM ET

    coords = STADIUM_COORDS[park]
    grid_id, x, y = get_gridpoint(coords["lat"], coords["lon"])
    forecast_json = get_hourly_forecast(grid_id, x, y)
    game_weather = extract_game_weather(forecast_json, game_hour=hour)

    print(f"\n🌦️ NOAA Forecast for {park} at {hour}:00:")
    print(json.dumps(game_weather, indent=2))
