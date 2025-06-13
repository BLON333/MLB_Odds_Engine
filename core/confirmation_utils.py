"""Utilities for confirming market signals."""

from __future__ import annotations
from core.config import DEBUG_MODE, VERBOSE_MODE

__all__ = [
    "required_market_move",
    "confirmation_strength",
    "print_threshold_table",
    "book_agreement_score",
]

# Toggle for optional debug logging
VERBOSE = False


def required_market_move(hours_to_game: float) -> float:
    """Return required consensus probability movement for confirmation.

    Parameters
    ----------
    hours_to_game : float
        Hours until game time.

    Returns
    -------
    float
        Minimum consensus implied probability delta.

    Notes
    -----
    A base movement unit of ``0.006`` (approximate 20-cent move at one book)
    is scaled by a linear decay multiplier that ranges from ``3.0`` 24 hours
    before the game to ``1.0`` at game time.
    """
    movement_unit = 0.006
    if hours_to_game is None:
        hours = 0.0
    else:
        hours = float(hours_to_game)
    clamped = min(max(hours, 0.0), 24.0)
    multiplier = 1.0 + 2.0 * clamped / 24.0
    return multiplier * movement_unit


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
        threshold = required_market_move(hours)
        percent = threshold * 100.0
        units = threshold / 0.006
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
