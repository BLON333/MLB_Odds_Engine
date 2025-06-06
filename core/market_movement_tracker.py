"""Utility for detecting line movement between snapshots."""

from typing import Dict, Optional

MOVEMENT_THRESHOLDS = {
    "ev_percent": 0.5,
    "market_prob": 0.00001,
    "blended_fv": 1,
    "market_odds": 1,
    "stake": 0.1,
    "sim_prob": 0.1,
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

        if field == "market_prob":
            from cli.log_betting_evals import market_prob_increase_threshold

            try:
                hours = float(current.get("hours_to_game", 8.0))
            except Exception:
                hours = 8.0
            market_type = current.get("market", "")
            threshold = market_prob_increase_threshold(hours, market_type)
        else:
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
    prior = base.get(key) or {}

    # Determine the sportsbook for this row
    book = entry.get("book") or entry.get("best_book")
    current_raw = entry.get("_raw_sportsbook", {}) or {}

    # Lookup prior odds for the same book
    prev_raw = prior.get("raw_sportsbook") or prior.get("prev_raw_sportsbook") or {}
    prev_market_odds = None
    if isinstance(prev_raw, dict):
        prev_market_odds = prev_raw.get(book)
    if prev_market_odds is None:
        print(
            f"⚠️ No prior odds found for book: {book} in {entry.get('game_id')}:{entry.get('market')}:{entry.get('side')}"
        )

    # Use prior book odds for movement detection
    if prior:
        prior_for_detect = prior.copy()
        prior_for_detect["market_odds"] = prev_market_odds
    else:
        prior_for_detect = None

    if entry.get("market_prob") is None:
        print(
            f"⚠️ Skipping {entry.get('game_id')}:{entry.get('market')}:{entry.get('side')} — missing market_prob for movement detection."
        )
        movement = {}
    else:
        movement = detect_market_movement(entry, prior_for_detect)
    entry.update(movement)
    entry["prev_market_odds"] = prev_market_odds

    tracker[key] = {
        "ev_percent": entry.get("ev_percent"),
        "blended_fv": entry.get("blended_fv"),
        "market_odds": entry.get("market_odds"),
        "stake": entry.get("stake"),
        "sim_prob": entry.get("sim_prob"),
        "market_prob": entry.get("market_prob"),
        "date_simulated": entry.get("date_simulated"),
        "best_book": entry.get("best_book"),
        "raw_sportsbook": current_raw,
        "prev_raw_sportsbook": prev_raw,
    }

    return movement