import pandas as pd
from typing import Optional

from utils import (
    normalize_to_abbreviation,
    get_normalized_lookup_side,
    classify_market_segment,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
)


def _log_verbose(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def get_theme(bet: dict) -> str:
    """Return the exposure theme for a bet."""
    side = bet["side"].strip()
    market = bet["market"].replace("alternate_", "")

    if side.startswith("Over"):
        return "Over"
    if side.startswith("Under"):
        return "Under"

    if "h2h" in market or "spreads" in market or "runline" in market:
        for name in TEAM_NAME_TO_ABBR:
            if side.startswith(name):
                return name
    return "Other"


def get_theme_key(market: str, theme: str) -> str:
    if "spreads" in market or "h2h" in market or "runline" in market:
        return f"{theme}_spread"
    if "totals" in market:
        return f"{theme}_total"
    return f"{theme}_other"


def get_segment_group(market: str) -> str:
    base = market.replace("alternate_", "")
    seg = classify_market_segment(base)
    return "derivative" if seg != "full_game" else "full_game"


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
    existing_theme_stakes: dict,
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

    base_market = market.replace("alternate_", "")
    segment = get_segment_group(market)
    theme = get_theme({"side": side, "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    exposure_key = (game_id, theme_key, segment)
    theme_total = existing_theme_stakes.get(exposure_key, 0.0)
    is_alt_line = market.startswith("alternate_")

    if theme_total == 0:
        stake_amt = stake * 0.125 if is_alt_line else stake
        new_bet["stake"] = round(stake_amt, 2)
        new_bet["entry_type"] = "first"
        _log_verbose(
            f"✅ should_log_bet: First bet → {side} | {theme_key} [{segment}] | Stake: {stake_amt:.2f}u | EV: {ev:.2f}%",
            verbose,
        )
        return new_bet

    delta = stake - theme_total
    if delta >= 0.5:
        new_bet["stake"] = round(delta, 2)
        new_bet["entry_type"] = "top-up"
        _log_verbose(
            f"🔼 should_log_bet: Top-up accepted → {side} | {theme_key} [{segment}] | Δ {delta:.2f}u",
            verbose,
        )
        return new_bet

    _log_verbose("⛔ Rejected — top-up delta too small (< 0.5u)", verbose)
    return None
