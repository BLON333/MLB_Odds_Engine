import pandas as pd
from typing import Optional

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
    verbose: bool = True,
    min_ev: float = 0.05,
    min_stake: float = 1.0,
) -> Optional[dict]:
    """Return updated bet dict if it should be logged based on staking rules."""

    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = normalize_to_abbreviation(
        get_normalized_lookup_side(new_bet["side"], market)
    )
    new_bet["side"] = side  # ensure consistent formatting
    stake = new_bet["full_stake"]
    ev = new_bet["ev_percent"]

    if ev < min_ev * 100 or stake < min_stake:
        if verbose:
            print(
                f"⛔ should_log_bet: Rejected due to EV/stake threshold → EV: {ev:.2f}%, Stake: {stake:.2f}u"
            )
        return None

    group_key = get_bet_group_key(new_bet)
    orient = orientation_key(new_bet)

    prior = market_evals[market_evals["game_id"] == game_id]
    if not prior.empty:
        prior = prior[prior.apply(lambda r: get_bet_group_key(r.to_dict()) == group_key, axis=1)]

    if not prior.empty:
        # Reject if any existing bet in group has different orientation
        prior_orients = prior.apply(lambda r: orientation_key(r.to_dict()), axis=1).unique()
        if any(po != orient for po in prior_orients):
            if verbose:
                print(
                    f"❌ should_log_bet: Rejected due to theme conflict in group '{group_key}'"
                )
            return None

        total_prev_stake = prior["stake"].sum()
        delta = stake - total_prev_stake
        if delta >= 0.5:
            new_bet["stake"] = round(delta, 2)
            new_bet["entry_type"] = "top-up"
            if verbose:
                print(
                    f"🔼 should_log_bet: Top-up accepted → {side} | {group_key} | Delta: {delta:.2f}u"
                )
            return new_bet
        if verbose:
            print(
                f"⛔ should_log_bet: Rejected top-up — delta too small (< 0.5u)"
            )
        return None

    new_bet["stake"] = stake
    new_bet["entry_type"] = "first"
    if verbose:
        print(
            f"✅ should_log_bet: Accepted → {side} | {group_key} | Stake: {stake:.2f}u | EV: {ev:.2f}%"
        )
    return new_bet
