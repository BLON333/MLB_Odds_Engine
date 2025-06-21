from core.config import DEBUG_MODE, VERBOSE_MODE

# Minimum stake thresholds used across the staking pipeline
MIN_FIRST_STAKE = 1.0
MIN_TOPUP_STAKE = 0.5

# Round stakes to this precision across the pipeline
ROUND_STAKE_TO = 0.01

# Odds outside this range are ignored for logging
MAX_POSITIVE_ODDS = 200
MIN_NEGATIVE_ODDS = -150

from core.market_pricer import decimal_odds
from core.confirmation_utils import required_market_move, book_agreement_score
from core.skip_reasons import SkipReason
from core.logger import get_logger
import csv
import os


from core.utils import (
    normalize_label_for_odds,
    classify_market_segment,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
)


def round_stake(stake: float, precision: float = ROUND_STAKE_TO) -> float:
    """Return ``stake`` rounded to the nearest ``precision``."""
    return round(stake / precision) * precision


def _log_verbose(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def normalize_market_key(market: str) -> str:
    """Return a canonical key for a market name."""
    base = market.replace("alternate_", "").lower()
    if base.startswith("totals") or base.startswith("team_totals"):
        return "total"
    if base.startswith("spreads") or base.startswith("runline"):
        return "spread"
    if base in {"h2h", "moneyline"} or base.startswith("h2h") or base.startswith("moneyline"):
        return "h2h"
    return base


def get_theme(bet: dict) -> str:
    """Return the exposure theme for a bet."""
    side = bet["side"].strip()
    market = bet["market"].replace("alternate_", "")

    # 🆕 Handle team total bets like "ATL Over 4.5" or "Los Angeles Over 5.0"
    if "team_totals" in market:
        _, direction = parse_team_total_side(side)
        if direction:
            return direction

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
    """Return a theme key combining theme name with a normalized market."""
    key = normalize_market_key(market)
    if key in {"total", "spread", "h2h"}:
        return f"{theme}_{key}"
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


def build_skipped_evaluation(
    reason: str, game_id: str | None = None, bet: dict | None = None
) -> dict:
    """Return a consistent structure for skipped evaluations."""
    result = {
        "game_id": game_id,
        "log": False,
        "full_stake": 0.0,
        "skip_reason": reason,
        "skip": True,
        "reason": reason,
    }
    if bet is not None:
        result.update(bet)
    return result


def _compute_csv_theme_total(
    game_id: str,
    theme_key: str,
    segment: str,
    csv_stakes: dict,
) -> float:
    """Return cumulative stake for a theme based on CSV stake mapping."""
    total = 0.0
    for (gid, mkt, side), stake in csv_stakes.items():
        if gid != game_id:
            continue
        base = mkt.replace("alternate_", "")
        seg = get_segment_group(mkt)
        theme = get_theme({"side": side, "market": base})
        key = get_theme_key(base, theme)
        if (gid, key, seg) == (game_id, theme_key, segment):
            try:
                total += float(stake)
            except Exception:
                continue
    return total


def should_log_bet(
    new_bet: dict,
    existing_theme_stakes: dict,
    verbose: bool = True,
    min_ev: float = 0.05,
    min_stake: float = MIN_FIRST_STAKE,
    eval_tracker: dict | None = None,
    reference_tracker: dict | None = None,
    existing_csv_stakes: dict | None = None,
) -> dict:
    """Evaluate whether a bet should be logged and return a structured result.

    Parameters
    ----------
    existing_csv_stakes : dict | None, optional
        Mapping of previously logged stakes keyed by ``(game_id, market, side)``.
        When provided, the function treats missing entries as no prior stake for
        that specific market even if theme exposure exists.

    The optional ``eval_tracker`` should contain previous market evaluations
    keyed by ``game_id:market:side:book`` so line movement can be enforced for
    first-time entries.
    """

    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = normalize_label_for_odds(new_bet["side"], market)
    new_bet["side"] = side  # ensure consistent formatting
    # ``full_stake`` may be absent in legacy entries; fall back to ``stake``
    # or 0.0 to avoid KeyError.
    stake = round_stake(float(new_bet.get("full_stake", new_bet.get("stake", 0.0))))
    ev = new_bet["ev_percent"]

    if DEBUG_MODE and ev >= 10.0 and stake >= 2.0:
        logger = get_logger(__name__)
        logger.debug(f"High EV bet passed thresholds: EV={ev:.2f}%, Stake={stake:.2f}u")

    odds_value = None
    try:
        odds_value = float(new_bet.get("market_odds"))
    except Exception:
        pass

    if odds_value is not None:
        if odds_value > MAX_POSITIVE_ODDS or odds_value < MIN_NEGATIVE_ODDS:
            _log_verbose(
                "⛔ should_log_bet: Rejected due to odds out of range",
                verbose,
            )
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = "bad_odds"
            return build_skipped_evaluation("bad_odds", game_id, new_bet)

    if ev < min_ev * 100:
        if verbose:
            print(f"⛔ should_log_bet: Rejected due to EV threshold → EV: {ev:.2f}%")
        new_bet["entry_type"] = "none"
        new_bet["skip_reason"] = "low_ev"
        return build_skipped_evaluation("low_ev", game_id, new_bet)

    base_market = market.replace("alternate_", "")
    segment = get_segment_group(market)
    theme = get_theme({"side": side, "market": base_market})
    theme_key = get_theme_key(base_market, theme)
    exposure_key = (game_id, theme_key, segment)
    theme_total = existing_theme_stakes.get(exposure_key, 0.0)
    csv_stake = 0.0
    if existing_csv_stakes is not None:
        csv_stake = existing_csv_stakes.get((game_id, market, side), 0.0)

    if theme_total >= stake and existing_csv_stakes is not None:
        csv_theme_total = _compute_csv_theme_total(
            game_id, theme_key, segment, existing_csv_stakes
        )
        if csv_theme_total == 0:
            # Tracker likely stale; reset exposure
            existing_theme_stakes[exposure_key] = 0.0
            theme_total = 0.0

    delta_base = theme_total
    if theme_total >= stake and csv_stake == 0:
        print(
            f"⚠️ Theme stake exists ({theme_total}) but no CSV stake for {side}. Tracker may be stale."
        )
        delta_base = 0.0
    is_alt_line = (
        market.startswith("alternate_") or new_bet.get("market_class") == "alternate"
    )

    prior_entry = None
    t_key = f"{game_id}:{market}:{side}"

    if reference_tracker is not None:
        tracker_entry = reference_tracker.get(t_key)
        if isinstance(tracker_entry, dict):
            prior_entry = tracker_entry

    if prior_entry is None and eval_tracker is not None:
        tracker_entry = eval_tracker.get(t_key)
        if isinstance(tracker_entry, dict):
            prior_entry = tracker_entry

    # 🆕 Suppress early bets lacking market confirmation
    hours_to_game = None
    try:
        hours_to_game = float(new_bet.get("hours_to_game"))
    except Exception:
        hours_to_game = None

    if hours_to_game is not None and hours_to_game > 12:
        prev_prob = None
        if prior_entry is not None:
            prev_prob = prior_entry.get("consensus_prob")
            if prev_prob is None:
                prev_prob = prior_entry.get("market_prob")
        curr_prob = new_bet.get("consensus_prob")
        if curr_prob is None:
            curr_prob = new_bet.get("market_prob")
        movement = 0.0
        try:
            if prev_prob is not None and curr_prob is not None:
                movement = float(curr_prob) - float(prev_prob)
        except Exception:
            movement = 0.0

        books = new_bet.get("per_book")
        book_count = len(books) if isinstance(books, dict) and books else 1
        threshold = required_market_move(hours_to_game, book_count=book_count)
        if movement < threshold:
            _log_verbose(
                f"⛔ should_log_bet: Early bet suppressed — movement {movement:.3f} < {threshold:.3f} (books={book_count})",
                verbose,
            )
            try:
                from core.pending_bets import queue_pending_bet

                queue_pending_bet(
                    {
                        **new_bet,
                        "baseline_consensus_prob": prev_prob,
                    }
                )
            except Exception:
                pass
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = SkipReason.SUPPRESSED_EARLY.value
            return build_skipped_evaluation(
                SkipReason.SUPPRESSED_EARLY.value, game_id, new_bet
            )

        # Additional filter → require broad agreement across books when far from game time
        score = None
        try:
            score_val = new_bet.get("book_agreement_score")
            if score_val is not None:
                score = float(score_val)
        except Exception:
            score = None

        if hours_to_game > 12 and score is not None and score < 0.3:
            _log_verbose(
                f"⛔ should_log_bet: Low book agreement {score:.2f} < 0.30",
                verbose,
            )
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = "suppressed_low_agreement"
            return build_skipped_evaluation(
                "suppressed_low_agreement", game_id, new_bet
            )

    # 🚦 Reject bet if odds worsened versus reference snapshot
    if prior_entry is not None and theme_total > 0:
        try:
            prev_odds = prior_entry.get("market_odds")
            curr_odds = new_bet.get("market_odds")
            prev_ev = prior_entry.get("ev_percent")
            curr_ev = new_bet.get("ev_percent")
            odds_worsened = False
            reason_parts = []
            if prev_ev is not None and curr_ev is not None:
                if float(curr_ev) < float(prev_ev):
                    reason_parts.append(
                        f"EV fell from {float(prev_ev):.1f}% to {float(curr_ev):.1f}%"
                    )
                    odds_worsened = True
            if prev_odds is not None and curr_odds is not None:
                if decimal_odds(float(curr_odds)) < decimal_odds(float(prev_odds)):
                    reason_parts.append(
                        f"odds worsened from {int(prev_odds):+d} to {int(curr_odds):+d}"
                    )
                    odds_worsened = True
            if odds_worsened:
                msg = "⛔ Skipping top-up: " + ", ".join(reason_parts)
                _log_verbose(msg, verbose)
                new_bet["entry_type"] = "none"
                new_bet["skip_reason"] = SkipReason.ODDS_WORSENED.value
                return build_skipped_evaluation(
                    SkipReason.ODDS_WORSENED.value, game_id, new_bet
                )
        except Exception:
            pass

    tracker_key = f"{game_id}:{market}:{side}"

    if theme_total == 0:
        new_bet["stake"] = round_stake(stake)
        new_bet["entry_type"] = "first"
        if new_bet["stake"] < MIN_FIRST_STAKE:
            _log_verbose(
                f"⛔ Skipping bet — scaled stake {new_bet['stake']}u is below {MIN_FIRST_STAKE:.1f}u minimum",
                verbose,
            )
            new_bet["entry_type"] = "none"
            new_bet["skip_reason"] = SkipReason.LOW_INITIAL.value
            return build_skipped_evaluation(
                SkipReason.LOW_INITIAL.value, game_id, new_bet
            )
        _log_verbose(
            f"✅ should_log_bet: First bet → {side} | {theme_key} [{segment}] | Stake: {stake:.2f}u | EV: {ev:.2f}%",
            verbose,
        )
        return {
            "skip": False,
            "full_stake": stake,
            "ev": ev,
            "log": True,
            "game_id": game_id,
            "side": new_bet["side"],
            **new_bet,
        }

    # Round the delta once to avoid floating point drift across the pipeline
    delta_raw = stake - delta_base
    delta = round_stake(delta_raw)
    if delta >= MIN_TOPUP_STAKE:
        new_bet["stake"] = delta
        new_bet["entry_type"] = "top-up"
        _log_verbose(
            f"🔼 should_log_bet: Top-up accepted → {side} | {theme_key} [{segment}] | Δ {delta:.2f}u",
            verbose,
        )
        return {
            "skip": False,
            "full_stake": stake,
            "partial_stake": delta,
            "ev": ev,
            "log": True,
            "game_id": game_id,
            "side": new_bet["side"],
            **new_bet,
        }

    msg = f"⛔ Delta stake {delta:.2f}u < {MIN_TOPUP_STAKE:.1f}u minimum"
    new_bet["entry_type"] = "none"
    new_bet["skip_reason"] = SkipReason.LOW_TOPUP.value
    _log_verbose(msg, verbose)
    return build_skipped_evaluation(SkipReason.LOW_TOPUP.value, game_id, new_bet)
