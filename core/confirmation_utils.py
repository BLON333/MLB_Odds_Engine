"""Utilities for confirming market signals."""

from __future__ import annotations
from typing import Optional

from core.config import DEBUG_MODE, VERBOSE_MODE
from core.market_pricer import kelly_fraction

__all__ = [
    "required_market_move",
    "confirmation_strength",
    "print_threshold_table",
    "book_agreement_score",
    "evaluate_late_confirmed_bet",
]

# Toggle for optional debug logging
VERBOSE = False

# Minimum additional stake required to generate a top-up bet
MIN_TOPUP_STAKE = 0.5


def required_market_move(hours_to_game: float, book_count: int = 1) -> float:
    """Return required consensus probability movement for confirmation.

    Parameters
    ----------
    hours_to_game : float
        Hours until game time.
    book_count : int, optional
        Number of sportsbooks contributing to the consensus line.  If
        unknown or ``None`` this should default to ``1``.

    Returns
    -------
    float
        Minimum consensus implied probability delta.

    Notes
    -----
    A base movement unit of ``0.0045`` (approximate 15-cent move at one book)
    is scaled by two multipliers:

    1. ``time_multiplier``
        Linear decay from ``3.0`` at 24 hours before the game to ``1.0`` at
        game time.
    2. ``book_multiplier``
        Scales stricter when fewer books contribute to the consensus odds. The
        multiplier is ``1.0`` when seven or more books are present and
        increases by ``0.25`` for each missing book (capped at ``2.5`` when only
        one book is available).
    """
    BASE_MOVEMENT_UNIT = 0.0045

    movement_unit = BASE_MOVEMENT_UNIT

    hours = 0.0 if hours_to_game is None else float(hours_to_game)
    clamped_time = min(max(hours, 0.0), 24.0)
    time_multiplier = 1.0 + 2.0 * clamped_time / 24.0

    try:
        books = int(book_count)
    except Exception:
        books = 1
    clamped_books = max(min(books, 7), 1)
    book_multiplier = 1.0 + (max(7 - clamped_books, 0) * 0.25)

    return movement_unit * time_multiplier * book_multiplier


def confirmation_strength(observed_move: float, hours_to_game: float) -> float:
    """Return the market confirmation strength for a bet.

    Parameters
    ----------
    observed_move : float
        The consensus implied probability change observed in the market.
    hours_to_game : float
        Hours until game time used to determine the required threshold.

    Returns
    -------
    float
        A normalized strength value between 0 and 1 where ``1`` means the
        observed move meets or exceeds the required threshold.
    """

    threshold = required_market_move(hours_to_game)
    if threshold <= 0:
        return 1.0

    strength = max(0.0, min(1.0, float(observed_move) / threshold))
    if VERBOSE and strength <= 0:
        print(
            f"[DEBUG] Negative market confirmation: observed_move={observed_move:.4f}, "
            f"threshold={threshold:.4f} → strength=0.0"
        )
    return strength


def print_threshold_table() -> None:
    """Print required market move thresholds at key hours.

    The table shows how much consensus line movement is needed for
    confirmation at selected hours leading up to a game.
    """

    key_hours = [24, 18, 12, 6, 3, 1, 0]
    print("[Hours to Game] | [Required Move (%)] | [Movement Units]")
    for hours in key_hours:
        threshold = required_market_move(hours, book_count=7)
        percent = threshold * 100.0
        units = threshold / 0.0045
        print(f"{hours:>3}h | {percent:>6.3f}% | {units:>5.2f}")


def book_agreement_score(market_data: dict) -> float:
    """Return the fraction of sharp books agreeing on a line move.

    ``market_data`` should map sportsbook keys to the implied probability
    change observed at that book. Positive deltas indicate movement in favor
    of the bet while negative deltas reflect the opposite.
    """

    sharp_books = [
        "pinnacle",
        "betonlineag",
        "fanduel",
        "betmgm",
        "draftkings",
        "williamhill",
        "mybookieag",
    ]

    threshold = 0.005
    deltas = {}
    for book in sharp_books:
        try:
            delta = float(market_data.get(book))
        except Exception:
            continue
        if abs(delta) >= threshold:
            deltas[book] = delta

    if not deltas:
        return 0.0

    up = sum(1 for d in deltas.values() if d > 0)
    down = sum(1 for d in deltas.values() if d < 0)
    direction = 1 if up >= down else -1

    agree = sum(
        1
        for d in deltas.values()
        if (d > 0 and direction > 0) or (d < 0 and direction < 0)
    )

    return round(agree / len(sharp_books), 2)


def evaluate_late_confirmed_bet(
    bet: dict,
    new_consensus_prob: float,
    existing_stake: float,
) -> Optional[dict]:
    """Return a top-up bet if late confirmation warrants additional stake."""

    try:
        hours = float(bet.get("hours_to_game"))
    except Exception:
        return None

    try:
        prev_prob = bet.get("baseline_consensus_prob")
        if prev_prob is None:
            prev_prob = bet.get("consensus_prob")
        prev_prob = float(prev_prob)
    except Exception:
        return None

    try:
        new_prob = float(new_consensus_prob)
    except Exception:
        return None

    movement = new_prob - prev_prob
    if movement < required_market_move(hours):
        return None

    prob = (
        bet.get("blended_prob")
        or bet.get("sim_prob")
        or bet.get("consensus_prob")
        or new_prob
    )
    odds = bet.get("market_odds")
    if odds is None:
        return None

    try:
        prob_val = float(prob)
        odds_val = float(odds)
    except Exception:
        return None

    fraction = 0.125 if bet.get("market_class") == "alternate" else 0.25
    raw_kelly = bet.get("raw_kelly")
    if raw_kelly is None:
        raw_kelly = kelly_fraction(prob_val, odds_val, fraction=fraction)
    try:
        raw_kelly = float(raw_kelly)
    except Exception:
        raw_kelly = 0.0

    strength = confirmation_strength(movement, hours)
    target_stake = round(raw_kelly * (strength ** 1.5), 4)

    try:
        max_full = float(bet.get("full_stake", target_stake))
    except Exception:
        max_full = target_stake

    target_stake = min(target_stake, max_full)
    delta = round(target_stake - float(existing_stake), 2)

    if delta < MIN_TOPUP_STAKE:
        return None

    updated = bet.copy()
    updated.update(
        {
            "stake": delta,
            "full_stake": target_stake,
            "entry_type": "top-up",
            "consensus_prob": new_prob,
            "market_prob": new_prob,
        }
    )
    return updated