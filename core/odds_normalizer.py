from datetime import datetime
from zoneinfo import ZoneInfo

from core.market_pricer import best_price
from core.consensus_pricer import calculate_consensus_prob
from core.utils import (
    normalize_label,
    normalize_label_for_odds,
    merge_offers_with_alternates,
    TEAM_ABBR,
    TEAM_NAME_TO_ABBR,
    canonical_game_id,
    clean_book_prices,
    to_eastern,
)


def normalize_market_odds(odds: dict) -> dict:
    """Return normalized market odds for a single game.

    Parameters
    ----------
    odds : dict
        Raw Odds API event JSON containing ``bookmakers`` and team info.

    Returns
    -------
    dict
        Dictionary keyed by canonical market name with enriched odds fields.
    """
    if not isinstance(odds, dict):
        return {}

    home_team = odds.get("home_team")
    away_team = odds.get("away_team")
    commence = odds.get("commence_time")
    start_ts = None
    game_id = None
    try:
        if commence:
            start_ts = datetime.fromisoformat(commence.replace("Z", "+00:00"))
            start_ts = to_eastern(start_ts)
        if home_team and away_team and start_ts:
            date_str = start_ts.strftime("%Y-%m-%d")
            away_abbr = TEAM_ABBR.get(away_team, away_team)
            home_abbr = TEAM_ABBR.get(home_team, home_team)
            raw_id = disambiguate_game_id(date_str, away_abbr, home_abbr, start_ts)
            game_id = canonical_game_id(raw_id)
    except Exception:
        pass

    offers = {}
    for bm in odds.get("bookmakers", []):
        book = bm.get("key", "unknown")
        for market in bm.get("markets", []):
            mkey = market.get("key")
            for outcome in market.get("outcomes", []):
                label = outcome.get("name")
                price = outcome.get("price")
                point = outcome.get("point")
                team_desc = outcome.get("description")
                if label is None or price is None:
                    continue
                if "team_totals" in mkey and team_desc:
                    team_abbr = TEAM_NAME_TO_ABBR.get(team_desc.strip(), team_desc.strip())
                    base_label = f"{team_abbr} {label}".strip()
                else:
                    base_label = label
                full_label = normalize_label_for_odds(base_label, mkey, point)
                offers.setdefault(mkey, {}).setdefault(book, {})[full_label] = price

    offers = merge_offers_with_alternates(offers)
    normalized = {}

    for market_key, books in offers.items():
        for book, labels in books.items():
            for label, price in labels.items():
                key = normalize_label(label).strip()
                normalized.setdefault(market_key, {}).setdefault(key, {"per_book": {}})
                normalized[market_key][key]["per_book"][book] = price

    for market_key, market in normalized.items():
        for label, entry in market.items():
            prices = clean_book_prices(entry.get("per_book", {}))
            entry["per_book"] = prices
            entry["price"] = best_price(list(prices.values()), label)

    if game_id:
        for mkt_key, market in normalized.items():
            for label in list(market.keys()):
                result, _ = calculate_consensus_prob(
                    game_id=game_id,
                    market_odds={game_id: normalized},
                    market_key=mkt_key,
                    label=label,
                )
                market[label].update(result)

    if start_ts:
        normalized["start_time"] = start_ts.isoformat()

    for key in ["h2h", "spreads", "totals"]:
        normalized.setdefault(key, {})

    return normalized