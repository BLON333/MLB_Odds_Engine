import numpy as np
import math
import os
from typing import Optional, Tuple

__all__ = [
    "scale_distribution",
    "dynamic_blend_weight",
    "base_model_weight_for_market",
    "blend_prob",
    "min_weight_override_for_market",
]


def scale_distribution(raw_vals, target_mean=None, target_sd=None):
    """Return values scaled to target mean and standard deviation."""
    arr = np.array(raw_vals, dtype=float)
    raw_mean = arr.mean()
    raw_sd = arr.std()
    scaled = arr
    if target_sd is not None and raw_sd > 0:
        scaled = (scaled - raw_mean) * (target_sd / raw_sd) + raw_mean
    if target_mean is not None:
        scaled = scaled + (target_mean - raw_mean)
    return scaled.tolist()


def min_weight_override_for_market(market_type: str) -> float:
    """Return the minimum model weight allowed for a given market type."""
    market_type = market_type.lower()
    if "team_total" in market_type:
        return 0.2
    elif "1st" in market_type or "alt" in market_type:
        return 0.4
    elif "spread" in market_type or "total" in market_type:
        return 0.3
    return 0.3


def dynamic_blend_weight(base_weight: float, hours_to_game: float, market_type: str) -> float:
    """Return the model weight accounting for time until game start.

    A gentler logistic curve (slope ``3.0``) is used to taper model influence
    as first pitch approaches while enforcing a minimum weight floor per market
    type so the model never disappears completely.
    """
    if hours_to_game is None:
        hours_to_game = 8  # Fallback assumption

    logistic_weight = 1 / (1 + math.exp((8 - hours_to_game) / 3.0))

    decay_curve_value = base_weight * logistic_weight

    min_weight = min_weight_override_for_market(market_type)
    w_model = max(min_weight, decay_curve_value)

    return w_model


def base_model_weight_for_market(market: str) -> float:
    """Return the base model weight for ``market``.

    Derivative markets like inning-specific lines or alternate spreads/totals
    receive higher weights while full-game mainlines are discounted.
    """

    # Normalize alternate market prefixes and remove ``_innings`` suffixes
    key = market.lower().replace("alternate_", "").replace("_innings", "")

    base_weights = {
        "spreads": 0.6,
        "totals": 0.6,
        "h2h": 0.6,
        "spreads_1st_5": 0.8,
        "totals_1st_5": 0.9,
        "spreads_1st_3": 0.9,
        "totals_1st_3": 0.9,
        "totals_1st_1": 0.95,
        "spreads_1st_7": 0.75,
        "totals_1st_7": 0.75,
        "team_totals": 0.7,
    }

    if key in base_weights:
        return base_weights[key]

    for mkt_key, weight in base_weights.items():
        if key.startswith(mkt_key + "_"):
            return weight

    # Default fallback for unrecognized markets
    return 0.75


def blend_prob(
    p_model: float,
    market_odds: float,
    market_type: str,
    hours_to_game: float,
    p_market: Optional[float] = None,
) -> Tuple[float, float, float, float]:
    """Return a blended probability and weights.

    The model probability weight decreases as first pitch approaches using a
    gentler logistic curve with a floor to ensure the model maintains some
    influence. The base weight also depends on the market type (mainline vs.
    derivative).

    Args:
        p_model: Probability estimated by the model.
        market_odds: Current market odds in American format.
        market_type: Market identifier used to determine the base model weight.
        hours_to_game: Hours until game start.
        p_market: Market implied probability. If ``None`` the value is derived
            from ``market_odds``.

    Returns:
        A tuple ``(p_blended, w_model, p_model, p_market)`` where ``p_blended``
        is the combined probability and ``w_model`` is the weight applied to the
        model probability.
    """
    from core.market_pricer import implied_prob

    if p_market is None:
        p_market = implied_prob(market_odds)

    base_weight = base_model_weight_for_market(market_type)
    w_model = dynamic_blend_weight(base_weight, hours_to_game, market_type)
    w_market = 1.0 - w_model

    p_blended = w_model * p_model + w_market * p_market

    if os.getenv("BLEND_PROB_DEBUG"):
        from core.logger import get_logger
        logger = get_logger(__name__)
        logger.debug(
            "[blend_prob] model_prob=%.4f market_prob=%.4f blended_prob=%.4f | "
            "w_model=%.3f w_market=%.3f base_weight=%.2f",
            p_model,
            p_market,
            p_blended,
            w_model,
            w_market,
            base_weight,
        )

    return p_blended, w_model, p_model, p_market
