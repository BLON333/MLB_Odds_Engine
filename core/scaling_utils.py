import numpy as np
import math
import os
from typing import Optional, Tuple

__all__ = [
    "scale_distribution",
    "dynamic_blend_weight",
    "base_model_weight_for_market",
    "blend_prob",
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


def dynamic_blend_weight(base_weight: float, hours_to_game: float, min_weight: float = 0.3) -> float:
    """Return the model weight accounting for time until game start.

    A gentler logistic curve (slope ``3.0``) is used to taper model influence
    as first pitch approaches while enforcing a minimum ``min_weight`` so the
    model never disappears completely.
    """
    if hours_to_game is None:
        hours_to_game = 8  # Fallback assumption

    logistic_weight = 1 / (1 + math.exp((8 - hours_to_game) / 3.0))

    w_model = base_weight * logistic_weight

    return max(w_model, min_weight)


def base_model_weight_for_market(market):
    """Return base model weight depending on market type."""
    if "1st" in market:
        return 0.9  # prioritize derivatives (1st innings)
    elif (
        market.startswith("h2h")
        or (market.startswith("spreads") and "_" not in market)
        or (market.startswith("totals") and "_" not in market)
    ):
        return 0.6  # mainlines (h2h, spreads, totals without "_")
    else:
        return 0.75  # fallback for anything else


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
    w_model = dynamic_blend_weight(base_weight, hours_to_game, min_weight=0.3)
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
