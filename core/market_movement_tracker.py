"""Utility for detecting line movement between snapshots."""

from typing import Dict, Optional


def detect_market_movement(
    current: Dict, prior: Optional[Dict], threshold: float = 0.1
) -> Dict[str, object]:
    """Return movement info for FV, EV, odds and stake compared to a prior entry."""

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
        "fv_movement": _move(fv_curr, fv_prev, threshold),
        "ev_movement": _move(ev_curr, ev_prev, threshold),
        "odds_movement": _move(odds_curr, odds_prev, threshold),
        "stake_movement": _move(stake_curr, stake_prev, threshold),
        "sim_movement": _move(sim_curr, sim_prev, threshold),
        "mkt_movement": _move(mkt_curr, mkt_prev, threshold),
    }
