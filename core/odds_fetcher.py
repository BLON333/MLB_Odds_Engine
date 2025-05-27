import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import requests
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta  # ✅ ADD THIS LINE
from collections import defaultdict

from core.market_pricer import implied_prob, to_american_odds, best_price
from core.bookmakers import get_us_bookmakers
from utils import (
    normalize_label,
    build_full_label,
    fallback_source,
    print_market_debug,
    TEAM_ABBR,
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
    extract_game_id_from_event,
    merge_book_sources_for
)

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
SPORT = "baseball_mlb"

BOOKMAKERS = [
    "betonlineag", "betmgm", "betrivers", "betus", "bovada", "williamhill_us",
    "draftkings", "fanatics", "fanduel", "lowvig", "mybookieag",
    "ballybet", "betanysports", "betparx", "espnbet", "fliff", "hardrockbet", "windcreek",
    "onexbet", "sport888", "betclic", "betfair_ex_eu", "betsson",
    "betvictor", "coolbet", "everygame", "gtbets", "marathonbet", "matchbook",
    "nordicbet", "pinnacle", "suprabets", "tipico_de", "unibet_eu", "williamhill",
    "winamax_de", "winamax_fr"
]
from .logger import get_logger

logger = get_logger(__name__)

logger.info("📊 Using bookmakers: %s", BOOKMAKERS)

MARKET_KEYS = [
    "h2h", "spreads", "totals",
    "alternate_spreads", "alternate_totals",
    "h2h_1st_1_innings", "h2h_1st_3_innings", "h2h_1st_5_innings", "h2h_1st_7_innings",
    "spreads_1st_1_innings", "spreads_1st_3_innings", "spreads_1st_5_innings", "spreads_1st_7_innings",
    "alternate_spreads_1st_1_innings", "alternate_spreads_1st_3_innings", "alternate_spreads_1st_5_innings", "alternate_spreads_1st_7_innings",
    "totals_1st_1_innings", "totals_1st_3_innings", "totals_1st_5_innings", "totals_1st_7_innings",
    "alternate_totals_1st_1_innings", "alternate_totals_1st_3_innings", "alternate_totals_1st_5_innings", "alternate_totals_1st_7_innings",
    "team_totals", "alternate_team_totals"
]

EVENTS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events"
EVENT_ODDS_URL = f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{{event_id}}/odds"


TEAM_ABBR = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH"
}

def remove_vig(probs_dict):
    avg_probs = {k: np.mean(v) for k, v in probs_dict.items()}
    total = sum(avg_probs.values())
    return {
        k: round(v / total, 6) if total > 0 else 0.0
        for k, v in avg_probs.items()
    }

def american_to_prob(odds):
    try:
        odds = float(odds)
        return round(100 / (odds + 100), 6) if odds > 0 else round(abs(odds) / (abs(odds) + 100), 6)
    except:
        return None

def prob_to_american(prob):
    try:
        if prob >= 0.5:
            return round(-(prob / (1 - prob)) * 100, 2)
        else:
            return round(((1 - prob) / prob) * 100, 2)
    except:
        return None

def fetch_consensus_for_single_game(game_id):
    """
    Pulls odds from API for a single game and returns de-vigged consensus odds.
    """
    logger.debug(f"🔎 Fetching consensus odds for {game_id}")

    # Step 1: Pull events
    resp = requests.get(EVENTS_URL, params={"apiKey": ODDS_API_KEY})
    if resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch events.")
        return None

    events = resp.json()

    # Step 2: Find event_id matching game_id

    date_tag, matchup = game_id.split("-", 3)[0:3], game_id.split("-")[3]
    away_abbr, home_abbr = matchup.split("@")
    away_name = TEAM_ABBR_TO_NAME.get(away_abbr, away_abbr)
    home_name = TEAM_ABBR_TO_NAME.get(home_abbr, home_abbr)

    event_id = None
    for event in events:
        if event["home_team"] == home_name and event["away_team"] == away_name:
            event_id = event["id"]
            break

    if not event_id:
        logger.debug(f"⚠️ No event found for {game_id}")
        return None

    # Step 3: Fetch odds for event
    odds_resp = requests.get(
        EVENT_ODDS_URL.format(event_id=event_id),
        params={
            "apiKey": ODDS_API_KEY,
            "regions": "us",
            "markets": ",".join(MARKET_KEYS),
            "bookmakers": ",".join(BOOKMAKERS),
            "oddsFormat": "american"
        }
    )
    if odds_resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch odds for {event_id}")
        return None

    event_data = odds_resp.json()
    if not event_data:
        logger.debug(f"⚠️ No odds data found for {game_id}")
        return None

    # Step 4: Normalize using your already existing `normalize_odds`
    offers = {}
    for bm in event_data.get("bookmakers", []):
        book_key = bm.get("key", "unknown")
        markets = bm.get("markets", [])
        if not isinstance(markets, list):
            continue

        for market in markets:
            market_key = market.get("key")
            outcomes = market.get("outcomes", [])
            for outcome in outcomes:
                label = outcome.get("name")
                price = outcome.get("price")
                point = outcome.get("point")
                if label and price is not None:
                    offers.setdefault(market_key, {}).setdefault(book_key, {})[label] = {
                        "price": price,
                        "point": point
                    }

    normalized = normalize_odds(game_id, offers)
    return normalized


def fetch_market_odds_from_api(game_ids):
    logger.debug(f"🎯 Incoming game_ids from sim folder: {sorted(game_ids)}")
    logger.debug(f"[DEBUG] Using ODDS_API_KEY prefix: {ODDS_API_KEY[:4]}*****")

    resp = requests.get(EVENTS_URL, params={"apiKey": ODDS_API_KEY})
    if resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch events: {resp.text}")
        return {}

    events = resp.json()
    logger.debug(f"[DEBUG] Received {len(events)} events from Odds API")

    odds_data = {}

    for event in events:
        try:
            home_team = event["home_team"]
            away_team = event["away_team"]
            start_time = datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00"))

            game_id = extract_game_id_from_event(away_team, home_team, start_time)
            if game_id not in game_ids:
                continue

            logger.debug(f"\n🧪 Scanned event: {away_team} @ {home_team} → {game_id} | Start: {start_time.isoformat()}")

            event_id = event["id"]
            odds_resp = requests.get(
                EVENT_ODDS_URL.format(event_id=event_id),
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": "us",
                    "markets": ",".join(MARKET_KEYS),
                    "bookmakers": ",".join(BOOKMAKERS),
                    "oddsFormat": "american"
                }
            )

            if odds_resp.status_code != 200:
                logger.debug(f"⚠️ Failed to fetch odds for {game_id}: {odds_resp.text}")
                continue

            offers_raw = odds_resp.json()
            debug_path = f"debug_odds_raw/{game_id}.json"
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)
            with open(debug_path, "w") as f:
                json.dump(offers_raw, f, indent=2)
            logger.debug(f"📄 Saved raw odds snapshot to {debug_path}")

            if not offers_raw or not isinstance(offers_raw, dict):
                logger.debug(f"⚠️ Odds API returned unexpected format for {game_id}: {type(offers_raw)}")
                continue

            bookmakers_data = offers_raw.get("bookmakers", [])
            if not bookmakers_data or not isinstance(bookmakers_data, list):
                logger.debug(f"⚠️ No bookmakers array in odds data for {game_id}")
                continue

            logger.debug(f"📦 Odds markets received from {len(bookmakers_data)} bookmakers for {game_id}")

            offers = {}

            for bm in bookmakers_data:
                book_key = bm.get("key", "unknown")
                markets = bm.get("markets", [])
                if not isinstance(markets, list):
                    continue

                for market in markets:
                    market_type = market.get("key")
                    outcomes = market.get("outcomes", [])

                    logger.debug(f"   ➤ {market_type} | {len(outcomes)} outcomes")

                    if not market_type or not outcomes:
                        continue

                    for outcome in outcomes:
                        label = outcome.get("name")
                        price = outcome.get("price")
                        point = outcome.get("point")
                        team = outcome.get("description")  # For team_totals

                        if label is None or price is None:
                            continue

                        # Normalize label and build unified full label
                        norm_label = normalize_label(label)

                        if "team_totals" in market_type and team:
                            team_abbr = TEAM_NAME_TO_ABBR.get(team.strip(), team.strip())
                            base_label = f"{team_abbr} {norm_label}".strip()
                        else:
                            base_label = TEAM_NAME_TO_ABBR.get(norm_label, norm_label)

                        full_label = build_full_label(base_label, market_type, point)

                        offers.setdefault(market_type, {}).setdefault(book_key, {})[full_label] = {
                            "price": price,
                            "point": point
                        }

            logger.debug(f"🔎 Offers collected for {game_id}: {list(offers.keys())}")

            if not offers:
                logger.debug(f"❌ No valid odds found for {game_id} — skipping normalization.")
                odds_data[game_id] = None
                continue

            normalized = normalize_odds(game_id, offers)

            if normalized is not None:
                normalized["start_time"] = start_time.isoformat()

            # Add per_book odds (used later for true consensus devigging)
            per_book_odds = extract_per_book_odds(bookmakers_data, debug=True)
            for mkt_key, labels in per_book_odds.items():
                for label, book_prices in labels.items():
                    if label in normalized.get(mkt_key, {}):
                        normalized[mkt_key][label]["per_book"] = book_prices

            # Calculate consensus probabilities using unified logic
            from core.consensus_pricer import calculate_consensus_prob
            for mkt_key, market in normalized.items():
                if not isinstance(market, dict) or mkt_key.endswith("_source") or mkt_key == "start_time":
                    continue
                for label in market:
                    result, _ = calculate_consensus_prob(
                        game_id=game_id,
                        market_odds={game_id: normalized},
                        market_key=mkt_key,
                        label=label,
                    )
                    normalized[mkt_key][label].update(result)

            odds_data[game_id] = normalized

            if normalized:
                logger.debug(f"📱 ✅ Normalized odds for {game_id} — {len(normalized)} markets stored")
            else:
                logger.debug(f"📭 Normalized odds for {game_id} is empty — possible filtering or no valid odds.")

        except Exception as e:
            logger.debug(f"💥 Exception while processing {game_id if 'game_id' in locals() else 'event'}: {e}")

    return odds_data


def normalize_odds(game_id: str, offers: dict) -> dict:
    """Normalize odds into a per-book structure."""
    from core.market_pricer import best_price
    from utils import normalize_label, build_full_label, fallback_source

    logger.debug(f"\n🔍 Normalizing odds for: {game_id}")

    consensus = {}
    sources = {}

    for market_key, market in offers.items():
        if not isinstance(market, dict):
            continue

        label_prices = {}
        for book, book_data in market.items():
            if not isinstance(book_data, dict):
                continue

            for label, data in book_data.items():
                price = data.get("price")
                point = data.get("point")
                if price is None:
                    continue


                norm = normalize_label(label)
                full_label = build_full_label(norm, market_key, point)

                label_prices.setdefault(full_label, []).append(price)
                sources.setdefault(f"{market_key}_source", {}).setdefault(full_label, {})[book] = price

        for label, prices in label_prices.items():
            canonical = normalize_label(label).strip()
            price = best_price(prices, label)
            if not sources.get(f"{market_key}_source", {}).get(canonical):
                sources[f"{market_key}_source"][canonical] = fallback_source(canonical, price)
            consensus.setdefault(market_key, {})[canonical] = {"price": price}

    merged = {**consensus, **sources}
    return merged


def extract_per_book_odds(bookmakers_list, target_market_key=None, debug=False):
    result = defaultdict(lambda: defaultdict(dict))

    for bm in bookmakers_list:
        book = bm.get("key")
        markets = bm.get("markets", [])
        for market in markets:
            mkey = market.get("key")
            if target_market_key and mkey != target_market_key:
                continue
            for outcome in market.get("outcomes", []):
                name = outcome.get("name")
                point = outcome.get("point")
                price = outcome.get("price")
                team_desc = outcome.get("description")
                # H2H markets do not have a point value, so allow point to be None
                if name and price is not None:
                    base = normalize_label(name)
                    if "team_totals" in mkey and team_desc:
                        team_abbr = TEAM_NAME_TO_ABBR.get(team_desc.strip(), team_desc.strip())
                        base = f"{team_abbr} {base}".strip()
                    label = build_full_label(base, mkey, point)
                    result[mkey][label][book] = price
                    if debug:
                        logger.debug(f"✅ Stored {book}: {mkey} → {label} @ {price}")
    return result




def save_market_odds_to_file(odds_data, date_tag):
    path = f"data/market_odds/{date_tag}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(odds_data, f, indent=2)
    logger.debug(f"✅ Saved market odds to {path}")
    return path
