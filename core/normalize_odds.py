from core.config import DEBUG_MODE, VERBOSE_MODE
from core.market_pricer import implied_prob, to_american_odds, best_price
from core.book_whitelist import ALLOWED_BOOKS
from core.utils import (
    normalize_label,
    merge_offers_with_alternates,
    get_teams_from_game_id,
)
import numpy as np

# Books we prefer to use when creating consensus prices
DEFAULT_CONSENSUS_BOOKS = [
    "pinnacle",
    "betonlineag",
    "fanduel",
    "betmgm",
    "draftkings",
    "williamhill_us",
    "mybookieag",
]

# Filter the default list by the global whitelist so we never use unapproved books
CONSENSUS_BOOKS = [book for book in DEFAULT_CONSENSUS_BOOKS if book in ALLOWED_BOOKS]


def normalize_odds(game_id: str, offers: dict) -> dict:
    # ✅ Merge alternate markets like alternate_totals → totals
    offers = merge_offers_with_alternates(offers)

    def get_opponent_abbr(team_abbr, game_id):
        away, home = get_teams_from_game_id(game_id)
        return home if team_abbr.upper() == away.upper() else away

    print(f"\n🔍 Normalizing odds for: {game_id}")
    consensus = {}
    sources = {}

    for market_key, market in offers.items():
        if not isinstance(market, dict):
            continue

        label_prices = {}
        paired_novig = {}

        for book, lines in market.items():
            for label, entry in lines.items():
                price = entry.get("price") if isinstance(entry, dict) else entry
                if price is None:
                    continue
                label_prices.setdefault(label, []).append(price)
                sources.setdefault(f"{market_key}_source", {}).setdefault(label, {})[book] = price

        if "totals" in market_key:
            overs = [l for l in label_prices if l.startswith("Over")]
            for over in overs:
                try:
                    point = over.split()[-1]
                    under = f"Under {point}"

                    books = [
                        b for b in CONSENSUS_BOOKS
                        if b in sources[f"{market_key}_source"].get(over, {})
                        and b in sources[f"{market_key}_source"].get(under, {})
                    ]

                    for b in books:
                        p1 = implied_prob(sources[f"{market_key}_source"][over][b])
                        p2 = implied_prob(sources[f"{market_key}_source"][under][b])
                        total = p1 + p2
                        if total == 0:
                            continue
                        paired_novig.setdefault(over, []).append(round(p1 / total, 6))
                        paired_novig.setdefault(under, []).append(round(p2 / total, 6))
                except Exception as e:
                    print(f"❌ Devig error ({over} vs {under}): {e}")
                    continue

        for label in label_prices:
            price = best_price(label_prices[label], label)
            if label in paired_novig and len(paired_novig[label]) >= 2:
                prob = round(np.mean(paired_novig[label]), 6)
                odds = to_american_odds(prob)
                consensus.setdefault(market_key, {})[label] = {
                    "price": price,
                    "consensus_prob": prob,
                    "consensus_odds": odds,
                    "pricing_method": "devig",
                    "raw_devig_probs": paired_novig[label]
                }
            else:
                consensus.setdefault(market_key, {})[label] = {
                    "price": price,
                    "consensus_prob": None,
                    "consensus_odds": None,
                    "pricing_method": "sim_only"
                }

    return consensus