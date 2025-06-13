"""Helper utilities for bet management and top-ups."""

from typing import Optional

from core.confirmation_utils import required_market_move, confirmation_strength
from core.market_pricer import kelly_fraction
from core.should_log_bet import MIN_TOPUP_STAKE


def evaluate_late_confirmed_bet(
    bet: dict,
    new_consensus_prob: float,
    existing_stake: float,
) -> Optional[dict]:
    """Return a top-up bet if late confirmation warrants additional stake."""

    try:
        hours = float(bet.get("hours_to_game"))
    except Exception:
        return None

    try:
        prev_prob = bet.get("baseline_consensus_prob")
        if prev_prob is None:
            prev_prob = bet.get("consensus_prob")
        prev_prob = float(prev_prob)
    except Exception:
        return None

    try:
        new_prob = float(new_consensus_prob)
    except Exception:
        return None

    movement = new_prob - prev_prob
    if movement < required_market_move(hours):
        return None

    prob = (
        bet.get("blended_prob")
        or bet.get("sim_prob")
        or bet.get("consensus_prob")
        or new_prob
    )
    odds = bet.get("market_odds")
    if odds is None:
        return None

    try:
        prob_val = float(prob)
        odds_val = float(odds)
    except Exception:
        return None

    fraction = 0.125 if bet.get("market_class") == "alternate" else 0.25
    raw_kelly = bet.get("raw_kelly")
    if raw_kelly is None:
        raw_kelly = kelly_fraction(prob_val, odds_val, fraction=fraction)
    try:
        raw_kelly = float(raw_kelly)
    except Exception:
        raw_kelly = 0.0

    strength = confirmation_strength(movement, hours)
    target_stake = round(raw_kelly * (strength ** 1.5), 4)

    try:
        max_full = float(bet.get("full_stake", target_stake))
    except Exception:
        max_full = target_stake

    target_stake = min(target_stake, max_full)
    delta = round(target_stake - float(existing_stake), 2)

    if delta < MIN_TOPUP_STAKE:
        return None

    updated = bet.copy()
    updated.update(
        {
            "stake": delta,
            "full_stake": target_stake,
            "entry_type": "top-up",
            "consensus_prob": new_prob,
            "market_prob": new_prob,
        }
    )
    return updated
