import pandas as pd
import os
import json
from typing import Optional, Dict

from pandas import to_datetime

from core.market_eval_tracker import load_tracker, save_tracker


def _log_verbose(msg: str, verbose: bool) -> None:
    if verbose:
        print(msg)


def _extract_fair_value(bet: Dict) -> float:
    for key in ("blended_fv", "fair_odds", "fair_value"):
        val = bet.get(key)
        if val is None or val == "":
            continue
        try:
            return float(val)
        except Exception:
            continue
    return 0.0


MARKET_EVAL_TRACKER = load_tracker()


def _update_eval_tracker(bet: Dict, tracker: Dict) -> None:
    """Update tracker with the newest evaluation if more recent."""
    if tracker is None:
        return
    key = f"{bet['game_id']}|{get_bet_group_key(bet)}|{bet['side']}"
    new_dt = to_datetime(bet.get('date_simulated'), errors='coerce')
    record = {
        'ev_percent': bet.get('ev_percent'),
        'fair_value': _extract_fair_value(bet),
        'date_simulated': bet.get('date_simulated'),
    }
    prev = tracker.get(key)
    if prev:
        prev_dt = to_datetime(prev.get('date_simulated'), errors='coerce')
        if new_dt and prev_dt and new_dt <= prev_dt:
            return
    tracker[key] = record

from utils import (
    normalize_to_abbreviation,
    get_normalized_lookup_side,
    classify_market_segment,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
)


def parse_team_total_side(side: str) -> tuple[str, str]:
    """Return team abbreviation and direction from a team total label."""
    tokens = side.split()

    direction = "Over" if "Over" in tokens else "Under" if "Under" in tokens else ""

    team_abbr = None
    # common formats: 'ATL Over 4.5' or 'Over 4.5 ATL'
    for token in tokens:
        if token.upper() in TEAM_ABBR_TO_NAME:
            team_abbr = token.upper()
            break
        if token.title() in TEAM_NAME_TO_ABBR:
            team_abbr = TEAM_NAME_TO_ABBR[token.title()]
            break

    if not team_abbr:
        team_abbr = tokens[0].upper()

    return team_abbr, direction


def get_bet_group_key(bet: dict) -> str:
    """Classify a bet into a group key for staking logic."""
    market = bet["market"].lower()
    if market.startswith("alternate_"):
        market = market.replace("alternate_", "", 1)
    segment = classify_market_segment(market)

    if market in {"h2h", "spreads", "runline"}:
        return "mainline_spread_h2h"
    if market.startswith(("h2h_", "spreads_", "runline_")):
        return f"derivative_spread_h2h_{segment}"
    if market.startswith("totals") and not market.startswith("team_totals"):
        return f"totals_{segment}"
    if market.startswith("team_totals"):
        team, direction = parse_team_total_side(bet["side"])
        return f"team_total_{team}_{direction}"
    return f"{market}_{segment}"


def orientation_key(bet: dict) -> str:
    """Return a simplified orientation key used to detect opposing bets."""
    market = bet["market"].lower()
    side = bet["side"]

    if market.startswith("team_totals"):
        team, direction = parse_team_total_side(side)
        return f"{team}_{direction.lower()}"
    if market.startswith("totals"):
        return "over" if "over" in side.lower() else "under"
    # spreads/h2h/runline -> use team abbreviation
    tokens = side.split()
    team = tokens[0]
    if team.title() in TEAM_NAME_TO_ABBR:
        team = TEAM_NAME_TO_ABBR[team.title()]
    return team.upper()


def should_log_bet(
    new_bet: dict,
    market_evals: pd.DataFrame,
    eval_tracker: Dict[str, dict] = None,
    verbose: bool = True,
    min_ev: float = 0.05,
    min_stake: float = 1.0,
) -> Optional[dict]:
    """Return updated bet dict if it should be logged based on staking rules."""

    if eval_tracker is None:
        eval_tracker = MARKET_EVAL_TRACKER

    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = normalize_to_abbreviation(
        get_normalized_lookup_side(new_bet["side"], market)
    )
    new_bet["side"] = side
    stake = new_bet["full_stake"]
    ev = new_bet["ev_percent"]
    fv = _extract_fair_value(new_bet)

    if ev < min_ev * 100 or stake < min_stake:
        _log_verbose(
            f"⛔ should_log_bet: Rejected due to EV/stake threshold → EV: {ev:.2f}%, Stake: {stake:.2f}u",
            verbose,
        )
        _update_eval_tracker(new_bet, eval_tracker)
        return None

    group_key = get_bet_group_key(new_bet)
    orient = orientation_key(new_bet)

    # 🧠 Market confirmation check using eval tracker
    tracker_key = f"{game_id}|{group_key}|{side}"
    prev_eval = eval_tracker.get(tracker_key)
    if prev_eval:
        prev_ev = prev_eval.get("ev_percent")
        prev_fv = prev_eval.get("fair_value")
        if prev_ev is not None and prev_fv is not None:
            if ev <= prev_ev or fv >= prev_fv:
                _log_verbose(
                    f"⛔ Rejected: No market confirmation (EV {ev:.2f}% ≤ {prev_ev:.2f}% or FV {fv:.2f} ≥ {prev_fv:.2f})",
                    verbose,
                )
                _update_eval_tracker(new_bet, eval_tracker)
                return None

    prior = market_evals[market_evals["game_id"] == game_id]
    if not prior.empty:
        prior = prior[prior.apply(lambda r: get_bet_group_key(r.to_dict()) == group_key, axis=1)]

    # ❌ Opposing orientation rejection
    if not prior.empty:
        prior_orients = prior.apply(lambda r: orientation_key(r.to_dict()), axis=1).unique()
        if any(po != orient for po in prior_orients):
            _log_verbose(
                f"❌ should_log_bet: Rejected due to theme conflict in group '{group_key}'",
                verbose,
            )
            _update_eval_tracker(new_bet, eval_tracker)
            return None

    is_alt = market.lower().startswith("alternate_")
    prior_alt = prior[prior["market"].str.startswith("alternate_") & (prior["side"] == side)]
    prior_main = prior[~prior["market"].str.startswith("alternate_") & (prior["side"] == side)]
    alt_stake = prior_alt["stake"].sum() if not prior_alt.empty else 0.0
    main_stake = prior_main["stake"].sum() if not prior_main.empty else 0.0

    if is_alt:
        if main_stake > 0:
            # 🧠 Alt-line restricted to top-up only if mainline is already logged for this group/side
            delta = stake - alt_stake
            if delta >= 0.5:
                new_bet["stake"] = round(delta, 2)
                new_bet["entry_type"] = "top-up"
                _log_verbose(
                    f"🔼 Alt-line top-up accepted → {side} | {group_key} | Delta: {delta:.2f}u",
                    verbose,
                )
                _update_eval_tracker(new_bet, eval_tracker)
                return new_bet
            _log_verbose("⛔ Alt-line top-up rejected — delta too small (< 0.5u)", verbose)
            _update_eval_tracker(new_bet, eval_tracker)
            return None
        elif alt_stake > 0:
            # subsequent alt-line top-ups when no mainline exists
            delta = stake - alt_stake
            if delta >= 0.5:
                new_bet["stake"] = round(delta, 2)
                new_bet["entry_type"] = "top-up"
                _log_verbose(
                    f"🔼 Alt-line top-up accepted → {side} | {group_key} | Delta: {delta:.2f}u",
                    verbose,
                )
                _update_eval_tracker(new_bet, eval_tracker)
                return new_bet
            _log_verbose("⛔ Alt-line top-up rejected — delta too small (< 0.5u)", verbose)
            _update_eval_tracker(new_bet, eval_tracker)
            return None
        else:
            new_bet["stake"] = stake
            new_bet["entry_type"] = "first"
            _log_verbose(
                f"✅ Alt-line first bet accepted → {side} | {group_key} | Stake: {stake:.2f}u",
                verbose,
            )
            _update_eval_tracker(new_bet, eval_tracker)
            return new_bet

    # ---- Mainline logic ----
    if main_stake > 0:
        delta = stake - main_stake
        if delta >= 0.5:
            new_bet["stake"] = round(delta, 2)
            new_bet["entry_type"] = "top-up"
            _log_verbose(
                f"🔼 should_log_bet: Top-up accepted → {side} | {group_key} | Delta: {delta:.2f}u",
                verbose,
            )
            _update_eval_tracker(new_bet, eval_tracker)
            return new_bet
        _log_verbose("⛔ should_log_bet: Rejected top-up — delta too small (< 0.5u)", verbose)
        _update_eval_tracker(new_bet, eval_tracker)
        return None

    new_bet["stake"] = stake
    new_bet["entry_type"] = "first"
    _log_verbose(
        f"✅ should_log_bet: Accepted → {side} | {group_key} | Stake: {stake:.2f}u | EV: {ev:.2f}%",
        verbose,
    )
    _update_eval_tracker(new_bet, eval_tracker)
    return new_bet