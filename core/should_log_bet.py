from typing import Optional

from core.market_movement_tracker import track_and_update_market_movement

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
        tokens = side.split()
        if tokens:
            first = tokens[0]
            if first.upper() in TEAM_ABBR_TO_NAME:
                return first.upper()
            if first.title() in TEAM_NAME_TO_ABBR:
                return TEAM_NAME_TO_ABBR[first.title()]
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
    existing_theme_stakes: dict,
    verbose: bool = True,
    min_ev: float = 0.05,
    min_stake: float = 1.0,
    eval_tracker: dict | None = None,
    reference_tracker: dict | None = None,
) -> Optional[dict]:
    """Return updated bet dict if staking and movement criteria are met.

    The optional ``eval_tracker`` should contain previous market evaluations
    keyed by ``game_id:market:side:book`` so line movement can be enforced for
    first-time entries.
    """

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

    prior_entry = None

    if eval_tracker is not None and prior_entry is None:
        t_key = f"{game_id}:{market}:{side}"
        tracker_entry = eval_tracker.get(t_key)
        if isinstance(tracker_entry, dict):
            prior_entry = tracker_entry

    tracker_key = f"{game_id}:{market}:{side}"
    movement = track_and_update_market_movement(
        new_bet,
        eval_tracker or {},
        reference_tracker or eval_tracker,
    )
    if movement.get("is_new"):
        _log_verbose(f"🟡 First-time seen → {tracker_key}", verbose)
    else:
        try:
            _log_verbose(
                f"🧠 Prior FV: {prior_entry.get('blended_fv')} → New FV: {new_bet.get('blended_fv')}",
                verbose,
            )
        except Exception:
            pass
    if not (
        movement["mkt_movement"] == "better" and 5 <= ev <= 20
    ):
        _log_verbose(
            f"⛔ should_log_bet: Market not confirmed (Market movement: {movement['mkt_movement']}, EV: {ev:.2f}%)",
            verbose,
        )
        return None
    _log_verbose(
        f"✅ Market-confirmed bet → {tracker_key} — should be logged and posted",
        verbose,
    )

    base_market = market.replace("alternate_", "")
    segment = get_segment_group(market)
    theme = get_theme({"side": side, "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    exposure_key = (game_id, theme_key, segment)
    theme_total = existing_theme_stakes.get(exposure_key, 0.0)
    # Alternate lines (identified by market prefix or class) should use half of
    # the normal full_stake amount for the initial entry (⅛ Kelly).
    is_alt_line = market.startswith("alternate_") or new_bet.get("market_class") == "alternate"

    if theme_total == 0:
        stake_amt = stake * 0.5 if is_alt_line else stake
        new_bet["stake"] = round(stake_amt, 2)
        new_bet["entry_type"] = "first"
        if new_bet["stake"] < 1.0:
            _log_verbose(
                f"⛔ Skipping bet — scaled stake {new_bet['stake']}u is below 1.0u minimum",
                verbose,
            )
            return None
        _log_verbose(
            f"✅ should_log_bet: First bet → {side} | {theme_key} [{segment}] | Stake: {stake_amt:.2f}u | EV: {ev:.2f}%",
            verbose,
        )
        return new_bet

    delta = stake - theme_total
    if delta >= 0.5:
        new_bet["stake"] = round(delta, 2)
        new_bet["entry_type"] = "top-up"
        if new_bet["stake"] < 0.5:
            _log_verbose(
                f"⛔ Skipping top-up — delta stake {new_bet['stake']}u is below 0.5u minimum",
                verbose,
            )
            return None
        _log_verbose(
            f"🔼 should_log_bet: Top-up accepted → {side} | {theme_key} [{segment}] | Δ {delta:.2f}u",
            verbose,
        )
        return new_bet

    new_bet["entry_type"] = "none"
    _log_verbose("⛔ Rejected — top-up delta too small (< 0.5u)", verbose)
    return None