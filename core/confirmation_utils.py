"""Utilities for confirming market signals."""

from __future__ import annotations

__all__ = [
    "required_market_move",
    "confirmation_strength",
    "print_threshold_table",
]


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
    return min(1.0, float(observed_move) / threshold)


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
