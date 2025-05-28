"""Utility for detecting line movement between snapshots."""

from typing import Dict, Optional

from core.market_pricer import decimal_odds


def _move_odds(curr, prev, threshold: float = 0.01) -> str:
    """Return bettor-facing movement based on decimal payout value."""

    if curr is None or prev is None:
        return "same"
    try:
        dec_curr = decimal_odds(float(curr))
        dec_prev = decimal_odds(float(prev))
    except Exception:
        return "same"

    if abs(dec_curr - dec_prev) < threshold:
        return "same"

    return "better" if dec_curr > dec_prev else "worse"


def _move_fv(curr, prev, threshold: float = 0.01) -> str:
    """Return market-confirmation movement (inverse of :func:`_move_odds`)."""

    base = _move_odds(curr, prev, threshold)
    if base == "better":
        return "worse"
    if base == "worse":
        return "better"
    return base


def detect_market_movement(
    current: Dict,
    prior: Optional[Dict],
    *,
    fv_threshold: float = 0.01,
    ev_threshold: float = 0.001,
    odds_threshold: float = 0.01,
    stake_threshold: float = 0.001,
    sim_threshold: float = 0.001,
    mkt_threshold: float = 0.001,
) -> Dict[str, object]:
    """Return movement info for FV, EV, odds and stake compared to a prior entry.

    The function now uses per-field thresholds so even small changes can be
    detected.  Thresholds can be overridden via keyword arguments if desired.
    """

    def _get(d: Dict, *keys):
        for k in keys:
            if d is None:
                continue
            v = d.get(k)
            if v is not None:
                return v
        return None

    def _move(curr, prev, threshold: float = 0.1):
        if prev is None or curr is None:
            return "same"
        try:
            if abs(float(curr) - float(prev)) < threshold:
                return "same"
        except Exception:
            pass
        if curr > prev:
            return "better"
        if curr < prev:
            return "worse"
        return "same"

    fv_curr = _get(current, "blended_fv", "fair_value", "fair_odds")
    fv_prev = _get(prior or {}, "blended_fv", "fair_value", "fair_odds")
    ev_curr = _get(current, "ev_percent")
    ev_prev = _get(prior or {}, "ev_percent")
    odds_curr = _get(current, "market_odds")
    odds_prev = _get(prior or {}, "market_odds")
    stake_curr = _get(current, "stake")
    stake_prev = _get(prior or {}, "stake")
    sim_curr = _get(current, "sim_prob")
    sim_prev = _get(prior or {}, "sim_prob")
    mkt_curr = _get(current, "market_prob")
    mkt_prev = _get(prior or {}, "market_prob")

    return {
        "is_new": prior is None,
        "fv_movement": _move_fv(fv_curr, fv_prev, fv_threshold),
        "ev_movement": _move(ev_curr, ev_prev, ev_threshold),
        "odds_movement": _move_odds(odds_curr, odds_prev, odds_threshold),
        "stake_movement": _move(stake_curr, stake_prev, stake_threshold),
        "sim_movement": _move(sim_curr, sim_prev, sim_threshold),
        "mkt_movement": _move(mkt_curr, mkt_prev, mkt_threshold),
    }
