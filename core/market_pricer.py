from core.config import DEBUG_MODE, VERBOSE_MODE
import numpy as np
import math
from core.logger import get_logger

logger = get_logger(__name__)

# === Core Odds Logic ===
def to_american_odds(prob):
    """
    Convert a probability into American odds (with decimal precision).
    """
    if prob >= 1.0:
        return -float("inf")
    elif prob <= 0.0:
        return float("inf")

    decimal = 1 / prob
    if decimal >= 2:
        return round((decimal - 1) * 100, 2)
    else:
        return round(-100 / (decimal - 1), 2)

from scipy.special import logit, expit

def apply_logit_calibration(p_sim, a, b):
    if p_sim <= 0.0:
        return 0.0001  # prevent math error
    elif p_sim >= 1.0:
        return 0.9999
    return float(expit(a + b * logit(p_sim)))

def calculate_ev_from_prob(prob, market_odds):
    """
    Calculate EV% from a win probability and market odds.
    Uses: EV% = (market_decimal / fair_decimal_from_prob) - 1
    """
    if prob <= 0.0 or prob >= 1.0:
        return 0.0

    fair_decimal = 1 / prob
    market_decimal = decimal_odds(market_odds)
    return round((market_decimal / fair_decimal - 1) * 100, 2)


def implied_prob(odds):
    """
    Convert American odds to implied probability.
    """
    odds = float(odds)
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    else:
        return 100 / (odds + 100)

def best_price(odds_list, side):
    """
    Return the best (most favorable) American odds from a list,
    considering payout, regardless of side.

    For both OVER and UNDER, or any side:
    - If comparing -105 vs -115, -105 is better.
    - If comparing +105 vs +120, +120 is better.
    - If comparing +120 vs -105, +120 is better (higher payout).

    We convert to decimal odds temporarily to determine the best payout,
    but return the original American odds.
    """
    def to_decimal(american_odds):
        try:
            american_odds = float(american_odds)
            if american_odds >= 0:
                return (american_odds / 100) + 1
            else:
                return (100 / abs(american_odds)) + 1
        except:
            return 0

    if not odds_list:
        return None

    return max(odds_list, key=lambda o: to_decimal(o))

def adjust_for_push(p_win, p_loss):
    p_push = max(0.0, 1.0 - (p_win + p_loss))
    denom = max(1.0 - p_push, 1e-8)
    logger.debug(f"🔧 Adjusting for push: win={p_win:.4f}, loss={p_loss:.4f}, push={p_push:.4f}, denom={denom:.4f}")
    return p_win / denom, p_loss / denom




def decimal_odds(american):
    """
    Convert American odds to decimal odds (e.g., -120 → 1.83, +150 → 2.50)
    """
    if american < 0:
        return round(100 / abs(american) + 1, 4)
    else:
        return round(american / 100 + 1, 4)


def calculate_clv_and_fv(bet_odds: float, consensus_prob: float) -> tuple[float, int]:
    """Return (clv_percent, fair_value_odds) for the given bet odds."""
    bet_implied_prob = 1 / decimal_odds(bet_odds)
    fair_value_odds = to_american_odds(consensus_prob)
    clv_percent = (consensus_prob - bet_implied_prob) * 100
    return round(clv_percent, 2), int(round(fair_value_odds))

def kelly_fraction(prob_win, american_odds, fraction=0.25):
    if prob_win <= 0 or prob_win >= 1:
        return 0

    if american_odds > 0:
        decimal_odds = (american_odds / 100) + 1
    else:
        decimal_odds = (100 / abs(american_odds)) + 1

    b = decimal_odds - 1
    q = 1 - prob_win

    kelly = (b * prob_win - q) / b
    return max(0, round(kelly * 100 * fraction, 4))  # ✅ convert directly to units


def extract_best_book(per_book: dict) -> str | None:
    """Return the sportsbook name offering the best (highest payout) price."""
    if isinstance(per_book, dict) and per_book:
        try:
            best = max(per_book, key=lambda b: decimal_odds(per_book[b]))
            logger.debug(f"✅ Best book resolved: {best}")
            return best
        except Exception:
            return None
    return None




def prob_to_moneyline(prob):
    """
    Format a probability into American odds string (rounded, readable).
    Example: 0.55 → -122
    """
    return f"{int(to_american_odds(prob)):+}"

# === Moneyline Pricing ===
def compute_moneyline(home_scores, away_scores):
    """
    Given lists of simulated scores, compute win probabilities and fair odds.
    """
    home_win = np.array(home_scores) > np.array(away_scores)
    p_home = home_win.mean()
    p_away = 1 - p_home

    return {
        "home": {
            "prob": round(p_home, 4),
            "fair_odds": to_american_odds(p_home)
        },
        "away": {
            "prob": round(p_away, 4),
            "fair_odds": to_american_odds(p_away)
        }
    }



def get_market_price(market_dict, market, side):
    """
    Retrieves the consensus market price for a given market and side.
    Ensures only matching segment (e.g., totals_1st_5_innings) is searched,
    including alternate versions. Never leaks across segments.
    """
    def extract_segment_suffix(key):
        parts = key.split("_")
        return "_".join(parts[1:]) if len(parts) > 1 else ""

    segment_suffix = extract_segment_suffix(market)
    base_prefix = market.split("_")[0]
    normalized_side = side.strip()

    # ✅ Optional: Log the segment (for debugging)
    segment = classify_market_segment(market)
    logger.debug(f"[PRICE MATCH] Looking in segment: {segment} → market_key: {market}, side: {normalized_side}")

    if "team_totals" in market:
        normalized_side = side.replace(" Over", "Over").replace(" Under", "Under")

    search_keys = []
    for key in market_dict.keys():
        if key == market or key == f"alternate_{market}":
            search_keys.append(key)
        elif key.startswith(base_prefix) and extract_segment_suffix(key) == segment_suffix:
            search_keys.append(key)

    for key in search_keys:
        market_block = market_dict.get(key, {})
        if normalized_side in market_block:
            return market_block[normalized_side]

    logger.debug(f"❌ No market price found for: {normalized_side} in segment: {segment}")
    return None





# === CLI Summary Formatter ===
def print_market_summary(
    home_prob,
    away_prob,
    home_mean,
    away_mean,
    runline_prob,
    total_prob_over,
    total_std_dev,
    total_threshold=8.5
):
    """
    Print formatted console output summarizing market simulation results.
    """
    logger.info(
        f"""
====================
🧮 Market Simulation Summary
====================

💰 Moneyline Odds:
   Home: {prob_to_moneyline(home_prob)} (Win Probability: {home_prob:.1%})
   Away: {prob_to_moneyline(away_prob)} (Win Probability: {away_prob:.1%})

📊 Team Totals:
   Home Expected Runs: {home_mean:.2f}
   Away Expected Runs: {away_mean:.2f}

🎯 Run Line & Totals (Fair Odds):
   P(Home -1.5): {runline_prob:.1%} → {prob_to_moneyline(runline_prob)}
   P(Away +1.5): {(1 - runline_prob):.1%} → {prob_to_moneyline(1 - runline_prob)}
   P(Total > {total_threshold}): {total_prob_over:.2%} → {prob_to_moneyline(total_prob_over)}
   P(Total < {total_threshold}): {(1 - total_prob_over):.2%} → {prob_to_moneyline(1 - total_prob_over)}

🧠 Model STD DEV: {total_std_dev:.2f} (Total Runs)
====================
""")