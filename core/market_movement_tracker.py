"""Utility for detecting line movement between snapshots."""

from typing import Dict, Optional

MOVEMENT_THRESHOLDS = {
    "ev_percent": 0.001,
    "market_prob": 0.0005,
    "blended_fv": 0.01,
    "market_odds": 0.01,
    "stake": 0.001,
    "sim_prob": 0.001,
}

from core.market_pricer import decimal_odds


def _compare_change(curr, prev, threshold):
    if curr is None or prev is None:
        return "same"
    try:
        if abs(float(curr) - float(prev)) < threshold:
            return "same"
        return "better" if float(curr) > float(prev) else "worse"
    except:
        return "same"


def _compare_odds(curr, prev, threshold):
    from core.market_pricer import decimal_odds
    try:
        dec_curr = decimal_odds(float(curr))
        dec_prev = decimal_odds(float(prev))
        if abs(dec_curr - dec_prev) < threshold:
            return "same"
        return "better" if dec_curr > dec_prev else "worse"
    except:
        return "same"


def _compare_fv(curr, prev, threshold):
    base = _compare_odds(curr, prev, threshold)
    return {"better": "worse", "worse": "better"}.get(base, base)


def detect_market_movement(current: Dict, prior: Optional[Dict]) -> Dict[str, object]:
    movement = {"is_new": prior is None}

    field_map = {
        "ev_movement": ("ev_percent", _compare_change),
        "mkt_movement": ("market_prob", _compare_change),
        "fv_movement": ("blended_fv", _compare_fv),
        "odds_movement": ("market_odds", _compare_odds),
        "stake_movement": ("stake", _compare_change),
        "sim_movement": ("sim_prob", _compare_change),
    }

    for move_key, (field, fn) in field_map.items():
        curr = current.get(field)
        prev = (prior or {}).get(field)
        threshold = MOVEMENT_THRESHOLDS.get(field, 0.001)
        movement[move_key] = fn(curr, prev, threshold)

    return movement


def track_and_update_market_movement(
    entry: Dict,
    tracker: Dict,
    reference_tracker: Optional[Dict] | None = None,
) -> Dict[str, object]:
    """Detect movement for an entry and update the tracker in one step.

    Parameters
    ----------
    entry : Dict
        Current market evaluation row.
    tracker : Dict
        Tracker to update with the new values.
    reference_tracker : Optional[Dict], optional
        Frozen snapshot used for movement comparison.  If ``None`` the
        ``tracker`` itself is used, which preserves the previous behaviour.
    """

    key = (
        f"{entry.get('game_id', '')}:{str(entry.get('market', '')).strip()}:{str(entry.get('side', '')).strip()}"
    )
    base = reference_tracker if reference_tracker is not None else tracker
    prior = base.get(key)

    movement = detect_market_movement(entry, prior)
    entry.update(movement)

    tracker[key] = {
        "ev_percent": entry.get("ev_percent"),
        "blended_fv": entry.get("blended_fv"),
        "market_odds": entry.get("market_odds"),
        "stake": entry.get("stake"),
        "sim_prob": entry.get("sim_prob"),
        "market_prob": entry.get("market_prob"),
        "date_simulated": entry.get("date_simulated"),
        "best_book": entry.get("best_book"),
    }

    return movement
