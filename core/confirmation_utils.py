"""Utilities for confirming market signals."""

from __future__ import annotations

__all__ = ["required_market_move"]


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
