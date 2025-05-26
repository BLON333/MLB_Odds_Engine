"""Utility for detecting line movement between snapshots."""

from typing import Dict, Optional


def detect_market_movement(current: Dict, prior: Optional[Dict]) -> Dict[str, object]:
    """Return movement info for FV, EV and odds compared to a prior entry."""

    def _get(d: Dict, *keys):
        for k in keys:
            if d is None:
                continue
            v = d.get(k)
            if v is not None:
                return v
        return None

    def _move(curr, prev):
        if prev is None:
            return "same"
        if curr is None:
            return "same"
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

    return {
        "is_new": prior is None,
        "fv_movement": _move(fv_curr, fv_prev),
        "ev_movement": _move(ev_curr, ev_prev),
        "odds_movement": _move(odds_curr, odds_prev),
    }

