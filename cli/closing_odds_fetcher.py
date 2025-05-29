import os
import csv
import json
import requests
import argparse
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
from core.logger import get_logger
logger = get_logger(__name__)

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
SPORT = "baseball_mlb"
EVENTS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events"
EVENT_ODDS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{{event_id}}/odds"

MARKET_KEYS = ",".join([
    "h2h", "spreads", "totals",
    "alternate_spreads", "alternate_totals",
    "h2h_1st_1_innings", "h2h_1st_3_innings", "h2h_1st_5_innings", "h2h_1st_7_innings",
    "spreads_1st_1_innings", "spreads_1st_3_innings", "spreads_1st_5_innings", "spreads_1st_7_innings",
    "alternate_spreads_1st_1_innings", "alternate_spreads_1st_3_innings", "alternate_spreads_1st_5_innings", "alternate_spreads_1st_7_innings",
    "totals_1st_1_innings", "totals_1st_3_innings", "totals_1st_5_innings", "totals_1st_7_innings",
    "alternate_totals_1st_1_innings", "alternate_totals_1st_3_innings", "alternate_totals_1st_5_innings", "alternate_totals_1st_7_innings",
    "team_totals", "alternate_team_totals"
])

TEAM_ABBR_TO_NAME = {
    "ATL": "Atlanta Braves", "BOS": "Boston Red Sox", "TB": "Tampa Bay Rays", "TOR": "Toronto Blue Jays",
    "NYY": "New York Yankees", "CLE": "Cleveland Guardians", "HOU": "Houston Astros", "OAK": "Oakland Athletics",
    "SEA": "Seattle Mariners", "CWS": "Chicago White Sox", "LAA": "Los Angeles Angels", "DET": "Detroit Tigers",
    "KC": "Kansas City Royals", "MIN": "Minnesota Twins", "BAL": "Baltimore Orioles", "TEX": "Texas Rangers",
    "CHC": "Chicago Cubs", "MIL": "Milwaukee Brewers", "PIT": "Pittsburgh Pirates", "CIN": "Cincinnati Reds",
    "STL": "St. Louis Cardinals", "PHI": "Philadelphia Phillies", "NYM": "New York Mets", "WSH": "Washington Nationals",
    "MIA": "Miami Marlins", "ARI": "Arizona Diamondbacks", "COL": "Colorado Rockies", "SD": "San Diego Padres",
    "LAD": "Los Angeles Dodgers", "SF": "San Francisco Giants"
}

def parse_game_id(game_id):
    parts = game_id.split("-")
    date = "-".join(parts[0:3])
    matchup = parts[3]
    away_abbr, home_abbr = matchup.split("@")
    return {
        "date": date,
        "away_abbr": away_abbr,
        "home_abbr": home_abbr,
        "away_name": TEAM_ABBR_TO_NAME.get(away_abbr),
        "home_name": TEAM_ABBR_TO_NAME.get(home_abbr)
    }

def load_game_ids_from_csv(csv_path):
    game_ids = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            game_ids.add(row["game_id"])
    return sorted(game_ids)

def fetch_all_events():
    params = {"apiKey": ODDS_API_KEY}
    resp = requests.get(EVENTS_URL, params=params)
    if resp.status_code != 200:
        print("❌ Failed to fetch events:", resp.text)
        return []
    return resp.json()

def match_event_id(game_id, events):
    parsed = parse_game_id(game_id)
    for event in events:
        if event["home_team"] == parsed["home_name"] and event["away_team"] == parsed["away_name"]:
            return event["id"]
    return None

def fetch_event_odds(event_id, bookmaker="fanduel"):
    url = EVENT_ODDS_URL.format(event_id=event_id)
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": MARKET_KEYS,
        "bookmakers": bookmaker,
        "oddsFormat": "american"
    }
    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        print(f"❌ Failed to fetch odds for event {event_id}:", resp.text)
        return None
    return resp.json()

def normalize_event_odds(game_id, event_data):
    output = {"start_time": event_data.get("commence_time", "")}

    for book in event_data.get("bookmakers", []):
        if book["key"] != "fanduel":
            continue

        for market in book["markets"]:
            market_key = market["key"]
            outcomes = market.get("outcomes", [])

            if len(outcomes) != 2:
                continue  # skip markets without both sides

            o1, o2 = outcomes
            name1 = o1["name"].strip()
            name2 = o2["name"].strip()

            price1 = o1["price"]
            price2 = o2["price"]

            # Calculate implied probabilities
            p1 = 100 / (abs(price1) + 100) if price1 < 0 else price1 / (price1 + 100)
            p2 = 100 / (abs(price2) + 100) if price2 < 0 else price2 / (price2 + 100)

            # De-vig using simple normalization
            total = p1 + p2
            prob1 = round(p1 / total, 4)
            prob2 = round(p2 / total, 4)

            # Fair odds (de-vigged)
            fair_odds1 = round((100 / prob1) - 100 if prob1 >= 0.5 else -100 / (1 - prob1), 2)
            fair_odds2 = round((100 / prob2) - 100 if prob2 >= 0.5 else -100 / (1 - prob2), 2)

            if market_key not in output:
                output[market_key] = {}

            label1 = name1
            label2 = name2
            if "point" in o1:
                label1 += f" {o1['point']}"
            if "point" in o2:
                label2 += f" {o2['point']}"

            output[market_key][label1] = {
                "price": price1,
                "consensus_prob": prob1,
                "consensus_odds": fair_odds1
            }

            output[market_key][label2] = {
                "price": price2,
                "consensus_prob": prob2,
                "consensus_odds": fair_odds2
            }

    return game_id, output


def save_output_json(data, date_str):
    path = f"data/closing_odds/{date_str}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved closing odds to: {path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-path", default="logs/market_evals.csv", help="Path to your market_evals.csv file")
    parser.add_argument("--bookmaker", default="fanduel", help="Bookmaker to fetch odds from")
    args = parser.parse_args()

    game_ids = load_game_ids_from_csv(args.log_path)
    events = fetch_all_events()

    results = {}
    for game_id in game_ids:
        event_id = match_event_id(game_id, events)
        if not event_id:
            print(f"❌ Could not match {game_id}")
            continue

        event_data = fetch_event_odds(event_id, args.bookmaker)
        if event_data:
            gid, odds_data = normalize_event_odds(game_id, event_data)
            results[gid] = odds_data
            print(f"✅ Logged closing odds for {game_id}")

    if results:
        date = list(results.keys())[0][:10]
        save_output_json(results, date)