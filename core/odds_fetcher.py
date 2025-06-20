from core.config import DEBUG_MODE, VERBOSE_MODE
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import shutil
import requests
import numpy as np
from dotenv import load_dotenv
from datetime import datetime, timedelta  # ✅ ADD THIS LINE
from zoneinfo import ZoneInfo
from collections import defaultdict

from core.market_pricer import implied_prob, to_american_odds, best_price
from core.book_whitelist import ALLOWED_BOOKS
from core.utils import (
    normalize_label,
    normalize_label_for_odds,
    fallback_source,
    print_market_debug,
    TEAM_ABBR,
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
    extract_game_id_from_event,
    merge_book_sources_for,
    canonical_game_id,
    parse_game_id,
    to_eastern,
)

load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
SPORT = "baseball_mlb"

BOOKMAKERS = sorted(ALLOWED_BOOKS)
from core.logger import get_logger

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

def fetch_consensus_for_single_game(game_id, lookahead_days=2):
    """Return de-vigged consensus odds for a single game.

    Parameters
    ----------
    game_id : str
        Canonical game identifier.
    lookahead_days : int, default 2
        How many days of events to request from the Odds API.
    """
    game_id = canonical_game_id(game_id)
    logger.debug(f"🔎 Fetching consensus odds for {game_id}")

    # Step 1: Pull events
    try:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
            timeout=10,
        )
    except TypeError:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
        )
    except requests.exceptions.RequestException as e:
        logger.error("❌ Error fetching events: %s", e)
        return {}
    if resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch events.")
        return None

    events = resp.json()

    # Step 2: Find event_id matching game_id (including time)

    input_parts = parse_game_id(game_id)
    input_base = f"{input_parts['date']}-{input_parts['away']}@{input_parts['home']}"

    event_id = None
    matched_event = None
    for event in events:
        away_team = event["away_team"]
        home_team = event["home_team"]
        start_time_utc = datetime.fromisoformat(
            event["commence_time"].replace("Z", "+00:00")
        ).replace(tzinfo=ZoneInfo("UTC"))
        start_time = to_eastern(start_time_utc)

        event_gid = canonical_game_id(
            extract_game_id_from_event(away_team, home_team, start_time)
        )

        if event_gid == game_id:
            matched_event = event
            game_id = event_gid
            break

        if "-T" not in game_id and event_gid.startswith(input_base):
            matched_event = event
            game_id = event_gid
            break

    if matched_event:
        event_id = matched_event["id"]

    if not event_id:
        logger.debug(f"⚠️ No event found for {game_id}")
        return None

    # Step 3: Fetch odds for event
    try:
        odds_resp = requests.get(
            EVENT_ODDS_URL.format(event_id=event_id),
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": ",".join(MARKET_KEYS),
                "bookmakers": ",".join(BOOKMAKERS),
                "oddsFormat": "american",
            },
            timeout=10,
        )
    except TypeError:
        odds_resp = requests.get(
            EVENT_ODDS_URL.format(event_id=event_id),
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "us",
                "markets": ",".join(MARKET_KEYS),
                "bookmakers": ",".join(BOOKMAKERS),
                "oddsFormat": "american",
            },
        )
    except requests.exceptions.RequestException as e:
        logger.error("❌ Error fetching odds for %s: %s", event_id, e)
        return {}
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


def fetch_market_odds_from_api(game_ids, filter_bookmakers=None, lookahead_days=2):
    """Fetch market odds for the provided game IDs.

    Parameters
    ----------
    game_ids : list[str]
        Canonical game IDs to pull odds for.
    filter_bookmakers : list[str] | None
        Optional subset of bookmakers to include.
    lookahead_days : int, default 2
        Number of days ahead to request from the Odds API. The default of ``2``
        ensures today's and tomorrow's games are returned.
    """

    input_game_ids = [canonical_game_id(gid) for gid in game_ids]
    logger.debug(f"🎯 Incoming game_ids from sim folder: {sorted(input_game_ids)}")
    logger.debug(f"[DEBUG] Using ODDS_API_KEY prefix: {ODDS_API_KEY[:4]}*****")

    try:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
            timeout=10,
        )
    except TypeError:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
        )
    except requests.exceptions.RequestException as e:
        logger.error("❌ Error fetching events: %s", e)
        return {}
    if resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch events: {resp.text}")
        return None

    events = resp.json()
    logger.debug(f"[DEBUG] Received {len(events)} events from Odds API")

    odds_data = {}

    for event in events:
        try:
            home_team = event["home_team"]
            away_team = event["away_team"]
            start_time_utc = datetime.fromisoformat(
                event["commence_time"].replace("Z", "+00:00")
            ).replace(tzinfo=ZoneInfo("UTC"))
            start_time = to_eastern(start_time_utc)

            game_id = canonical_game_id(
                extract_game_id_from_event(away_team, home_team, start_time)
            )

            # 🕒 Debug the start times and game_id
            logger.debug(
                "📅 Game %s starts at %s ET (UTC: %s)",
                game_id,
                start_time.isoformat(),
                start_time_utc.isoformat(),
            )

            # 🔍 DEBUG comparison with your sim game_ids
            logger.debug("🔍 Incoming game_ids (expected): %s", sorted(input_game_ids))
            logger.debug(
                "🧱 Built from API: %s → Home: %s, Away: %s",
                game_id,
                home_team,
                away_team,
            )

            if game_id not in input_game_ids:
                logger.warning(
                    "❌ No odds for %s — possible ID mismatch or time suffix drift",
                    game_id,
                )
                os.makedirs("logs", exist_ok=True)
                with open("logs/missed_game_ids.txt", "a") as f:
                    f.write(f"{game_id} — API: {away_team} @ {home_team}\n")
                continue

            logger.debug(f"\n✅ Matched event: {away_team} @ {home_team} → {game_id} | Start: {start_time.isoformat()}")


            event_id = event["id"]
            try:
                odds_resp = requests.get(
                    EVENT_ODDS_URL.format(event_id=event_id),
                    params={
                        "apiKey": ODDS_API_KEY,
                        "regions": "us",
                        "markets": ",".join(MARKET_KEYS),
                        "bookmakers": ",".join(BOOKMAKERS),
                        "oddsFormat": "american",
                    },
                    timeout=10,
                )
            except TypeError:
                odds_resp = requests.get(
                    EVENT_ODDS_URL.format(event_id=event_id),
                    params={
                        "apiKey": ODDS_API_KEY,
                        "regions": "us",
                        "markets": ",".join(MARKET_KEYS),
                        "bookmakers": ",".join(BOOKMAKERS),
                        "oddsFormat": "american",
                    },
                )
            except requests.exceptions.RequestException as e:
                logger.error("❌ Error fetching odds for %s: %s", game_id, e)
                continue

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

            if filter_bookmakers:
                before = len(bookmakers_data)
                bookmakers_data = [bm for bm in bookmakers_data if bm.get("key") in filter_bookmakers]
                logger.debug(
                    f"📦 Odds markets received from {before} bookmakers, filtered to {len(bookmakers_data)} for {game_id}"
                )
            else:
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

                        # Build unified full label
                        if "team_totals" in market_type and team:
                            team_abbr = TEAM_NAME_TO_ABBR.get(team.strip(), team.strip())
                            label_input = f"{team_abbr} {label}".strip()
                        else:
                            label_input = label

                        full_label = normalize_label_for_odds(label_input, market_type, point)

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

            if not normalized:
                logger.debug(
                    f"📭 Normalized odds for {game_id} is empty — possible filtering or no valid odds. Skipping."
                )
                odds_data[game_id] = None
                continue

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

            # Ensure all expected market keys exist for downstream tools
            for key in MARKET_KEYS:
                normalized.setdefault(key, {})

            odds_data[game_id] = normalized

            logger.debug(
                f"📱 ✅ Normalized odds for {game_id} — {len(normalized)} markets stored"
            )

        except Exception as e:
            logger.debug(f"💥 Exception while processing {game_id if 'game_id' in locals() else 'event'}: {e}")

    has_markets = False
    for data in odds_data.values():
        if not data:
            continue
        for key in MARKET_KEYS:
            if data.get(key):
                has_markets = True
                break
        if has_markets:
            break

    if not has_markets:
        logger.error("❌ Odds API returned no games with market entries")
        return None

    return odds_data


def fetch_all_market_odds(lookahead_days=2):
    """Fetch market odds for all games returned by the Odds API."""

    logger.debug(f"🌐 Fetching all market odds for daysFrom={lookahead_days}")

    try:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
            timeout=10,
        )
    except TypeError:
        resp = requests.get(
            EVENTS_URL,
            params={"apiKey": ODDS_API_KEY, "daysFrom": lookahead_days},
        )
    except requests.exceptions.RequestException as e:
        logger.error("❌ Error fetching events: %s", e)
        return {}
    if resp.status_code != 200:
        logger.debug(f"❌ Failed to fetch events: {resp.text}")
        return None

    events = resp.json()
    logger.debug(f"[DEBUG] Received {len(events)} events from Odds API")

    odds_data = {}

    for event in events:
        try:
            home_team = event["home_team"]
            away_team = event["away_team"]
            start_time_utc = datetime.fromisoformat(
                event["commence_time"].replace("Z", "+00:00")
            ).replace(tzinfo=ZoneInfo("UTC"))
            start_time = to_eastern(start_time_utc)

            game_id = canonical_game_id(
                extract_game_id_from_event(away_team, home_team, start_time)
            )

            # 🕒 Debug the start times and game_id
            logger.debug(
                "📅 Game %s starts at %s ET (UTC: %s)",
                game_id,
                start_time.isoformat(),
                start_time_utc.isoformat(),
            )

            logger.debug(
                f"\n🌐 Processing event: {away_team} @ {home_team} → {game_id} | Start: {start_time.isoformat()}"
            )

            event_id = event["id"]
            try:
                odds_resp = requests.get(
                    EVENT_ODDS_URL.format(event_id=event_id),
                    params={
                        "apiKey": ODDS_API_KEY,
                        "regions": "us",
                        "markets": ",".join(MARKET_KEYS),
                        "bookmakers": ",".join(BOOKMAKERS),
                        "oddsFormat": "american",
                    },
                    timeout=10,
                )
            except TypeError:
                odds_resp = requests.get(
                    EVENT_ODDS_URL.format(event_id=event_id),
                    params={
                        "apiKey": ODDS_API_KEY,
                        "regions": "us",
                        "markets": ",".join(MARKET_KEYS),
                        "bookmakers": ",".join(BOOKMAKERS),
                        "oddsFormat": "american",
                    },
                )
            except requests.exceptions.RequestException as e:
                logger.error("❌ Error fetching odds for %s: %s", game_id, e)
                continue

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
                logger.debug(
                    f"⚠️ Odds API returned unexpected format for {game_id}: {type(offers_raw)}"
                )
                continue

            bookmakers_data = offers_raw.get("bookmakers", [])
            if not bookmakers_data or not isinstance(bookmakers_data, list):
                logger.debug(f"⚠️ No bookmakers array in odds data for {game_id}")
                continue

            offers = {}

            for bm in bookmakers_data:
                book_key = bm.get("key", "unknown")
                markets = bm.get("markets", [])
                if not isinstance(markets, list):
                    continue

                for market in markets:
                    market_type = market.get("key")
                    outcomes = market.get("outcomes", [])

                    if not market_type or not outcomes:
                        continue

                    for outcome in outcomes:
                        label = outcome.get("name")
                        price = outcome.get("price")
                        point = outcome.get("point")
                        team = outcome.get("description")

                        if label is None or price is None:
                            continue

                        if "team_totals" in market_type and team:
                            team_abbr = TEAM_NAME_TO_ABBR.get(team.strip(), team.strip())
                            label_input = f"{team_abbr} {label}".strip()
                        else:
                            label_input = label

                        full_label = normalize_label_for_odds(label_input, market_type, point)

                        offers.setdefault(market_type, {}).setdefault(book_key, {})[
                            full_label
                        ] = {
                            "price": price,
                            "point": point,
                        }

            if not offers:
                logger.debug(f"❌ No valid odds found for {game_id} — skipping normalization.")
                odds_data[game_id] = None
                continue

            normalized = normalize_odds(game_id, offers)

            if not normalized:
                logger.debug(
                    f"📭 Normalized odds for {game_id} is empty — possible filtering or no valid odds. Skipping."
                )
                odds_data[game_id] = None
                continue

            normalized["start_time"] = start_time.isoformat()

            per_book_odds = extract_per_book_odds(bookmakers_data, debug=True)
            for mkt_key, labels in per_book_odds.items():
                for label, book_prices in labels.items():
                    if label in normalized.get(mkt_key, {}):
                        normalized[mkt_key][label]["per_book"] = book_prices

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

            for key in MARKET_KEYS:
                normalized.setdefault(key, {})

            odds_data[game_id] = normalized

            logger.debug(
                f"📱 ✅ Normalized odds for {game_id} — {len(normalized)} markets stored"
            )

        except Exception as e:
            logger.debug(
                f"💥 Exception while processing {game_id if 'game_id' in locals() else 'event'}: {e}"
            )

    has_markets = False
    for data in odds_data.values():
        if not data:
            continue
        for key in MARKET_KEYS:
            if data.get(key):
                has_markets = True
                break
        if has_markets:
            break

    if not has_markets:
        logger.error("❌ Odds API returned no games with market entries")
        return None

    return odds_data


def normalize_odds(game_id: str, offers: dict) -> dict:
    """Normalize odds into a per-book structure."""
    from core.market_pricer import best_price
    from core.utils import normalize_label, normalize_label_for_odds, fallback_source

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


                full_label = normalize_label_for_odds(label, market_key, point)

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
                    if "team_totals" in mkey and team_desc:
                        team_abbr = TEAM_NAME_TO_ABBR.get(team_desc.strip(), team_desc.strip())
                        label_input = f"{team_abbr} {name}".strip()
                    else:
                        label_input = name
                    label = normalize_label_for_odds(label_input, mkey, point)
                    result[mkey][label][book] = price
                    if debug:
                        logger.debug(f"✅ Stored {book}: {mkey} → {label} @ {price}")
    return result




def save_market_odds_to_file(odds_data, date_tag):
    if odds_data is None:
        logger.warning("⚠️ No odds data provided for %s, skipping write", date_tag)
        return None

    path = f"data/market_odds/{date_tag}.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(odds_data, f, indent=2)
    try:
        with open(path) as f:
            json.load(f)
    except Exception:
        logger.exception("❌ Market odds JSON validation failed for %s", path)
        bad_path = path + ".bad.json"
        try:
            shutil.move(path, bad_path)
            logger.error("🚨 Corrupted odds file moved to %s", bad_path)
        except Exception as mv_err:
            logger.error("❌ Failed to move corrupt odds file: %s", mv_err)
        return bad_path

    logger.debug(f"✅ Saved market odds to {path}")
    return path