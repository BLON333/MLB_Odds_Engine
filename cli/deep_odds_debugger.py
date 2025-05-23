import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from utils import normalize_label, TEAM_ABBR_TO_NAME
from core.market_pricer import implied_prob

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
SPORT = "baseball_mlb"
EVENTS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events"
EVENT_ODDS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{{event_id}}/odds"

BOOKMAKERS = [
    "betonlineag", "betmgm", "betrivers", "betus", "bovada", "williamhill_us",
    "draftkings", "fanatics", "fanduel", "lowvig", "mybookieag",
    "ballybet", "betanysports", "betparx", "espnbet", "fliff", "hardrockbet", "windcreek",
    "onexbet", "sport888", "betclic", "betfair_ex_eu", "betsson",
    "betvictor", "coolbet", "everygame", "gtbets", "marathonbet", "matchbook",
    "nordicbet", "pinnacle", "suprabets", "tipico_de", "unibet_eu", "williamhill",
    "winamax_de", "winamax_fr"
]

CONSENSUS_BOOKS = [
    "pinnacle", "betonlineag", "fanduel", "betmgm", "draftkings", "caesars", "mybookieag"
]

MARKET_KEYS = [
    "h2h", "spreads", "totals",
    "h2h_1st_5_innings", "spreads_1st_5_innings", "totals_1st_5_innings",
    "h2h_1st_7_innings", "spreads_1st_7_innings", "totals_1st_7_innings"
]

def fetch_today_events():
    params = {"apiKey": ODDS_API_KEY}
    resp = requests.get(EVENTS_URL, params=params)
    if resp.status_code != 200:
        print(f"❌ Failed to fetch events: {resp.text}")
        return []
    return resp.json()

def flatten_outcomes(event_id):
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": ",".join(MARKET_KEYS),
        "bookmakers": ",".join(BOOKMAKERS),
        "oddsFormat": "american"
    }
    resp = requests.get(EVENT_ODDS_URL.format(event_id=event_id), params=params)
    if resp.status_code != 200:
        print(f"❌ Failed to fetch odds: {resp.text}")
        return

    data = resp.json()
    for bm in data.get("bookmakers", []):
        book_key = bm.get("key")
        print(f"\n🏦 Bookmaker: {bm.get('title')} ({book_key})")
        for market in bm.get("markets", []):
            key = market.get("key")
            print(f"   ➤ Market: {key}")
            for outcome in market.get("outcomes", []):
                label = outcome.get("name")
                price = outcome.get("price")
                point = outcome.get("point")
                team = outcome.get("description", "")

                if label in TEAM_ABBR_TO_NAME:
                    normalized = TEAM_ABBR_TO_NAME[label]
                else:
                    normalized = normalize_label(label)

                point_str = f"{point:.1f}" if isinstance(point, (int, float)) else ""

                if key.startswith("totals"):
                    full_label = f"{normalized} {point_str}"
                elif "spreads" in key:
                    full_label = f"{normalized} {'+' if point > 0 else ''}{point_str}"
                elif "team_totals" in key and team:
                    full_label = f"{team} {normalized} {point_str}"
                else:
                    full_label = normalized

                # Determine if this entry would be included in final odds
                try:
                    prob = implied_prob(price)
                    include = book_key in CONSENSUS_BOOKS
                except:
                    include = False

                flag = "✅ INCLUDED" if include else "❌ SKIPPED"
                print(f"       • {full_label} | Price: {price} | Book: {book_key} {flag}")


def deep_dive_debug():
    events = fetch_today_events()
    if not events:
        print("❌ No events returned.")
        return

    for event in events:
        home = event.get("home_team")
        away = event.get("away_team")
        start_time = event.get("commence_time")
        eid = event.get("id")
        print(f"\n🧪 Event: {away} @ {home} | Start: {start_time} | ID: {eid}")
        flatten_outcomes(eid)


if __name__ == "__main__":
    deep_dive_debug()