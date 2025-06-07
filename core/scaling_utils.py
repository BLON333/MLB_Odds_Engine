import numpy as np
import math


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


def logistic_decay(t_hours, t_switch=8, slope=1.5):
    """Return a weight that decays with time until game start."""
    return 1 / (1 + math.exp((t_switch - t_hours) / slope))


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


def blend_prob(p_model, market_odds, market_type, hours_to_game, p_market=None):
    """Blend model and market probabilities with time-based weighting."""
    from core.market_pricer import implied_prob

    if p_market is None:
        p_market = implied_prob(market_odds)

    base_weight = base_model_weight_for_market(market_type)
    w_time = logistic_decay(hours_to_game, t_switch=8, slope=1.5)
    w_model = min(base_weight * w_time, 1.0)
    w_market = 1 - w_model

    p_blended = w_model * p_model + w_market * p_market
    return p_blended, w_model, p_model, p_market
