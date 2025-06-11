"""Utility functions for formatting betting market data."""
from __future__ import annotations

from typing import Dict, Tuple


def format_market_odds_and_roles(
    best_book: str,
    consensus_books: Dict[str, float] | None,
    ev_map: Dict[str, float] | None,
    role_map: Dict[str, str] | None,
) -> Tuple[str, str]:
    """Return formatted odds markdown and role tags for Discord notifications.

    Parameters
    ----------
    best_book : str
        Name of the sportsbook offering the best price.
    consensus_books : dict
        Mapping of sportsbook names to offered odds (American format).
    ev_map : dict
        Mapping of sportsbook names to expected value percentages.
    role_map : dict
        Mapping of sportsbook names to Discord role tags.

    Returns
    -------
    tuple[str, str]
        ``(odds_block, roles_text)`` where ``odds_block`` is formatted
        markdown listing each book and price, and ``roles_text`` is a
        string of tagged roles (``"📣 <@&...>"``) or ``""`` if none.

    Notes
    -----
    * Best book will be bolded in the odds block.
    * Books are sorted by descending odds value.
    * Odds are displayed in American format with an explicit sign.
    * A book's role is included only if its EV is at least 5%% and its
      odds fall within the range -150 to +200 (inclusive).
    """

    if not consensus_books:
        return "N/A", ""

    ev_map = ev_map or {}
    role_map = role_map or {}

    def _format_odds(val: float) -> str:
        sign = "+" if val > 0 else ""
        return f"{sign}{int(val) if float(val).is_integer() else val}"

    # Normalize keys for matching
    best_key = (best_book or "").lower()

    # Sort books by numeric odds (descending)
    sorted_books = sorted(
        consensus_books.items(),
        key=lambda x: float(x[1]),
        reverse=True,
    )

    roles = set()
    lines = []
    for book, price in sorted_books:
        book_key = str(book).lower()
        price_val = float(price)
        ev_val = float(ev_map.get(book_key, 0))
        qualifies = price_val >= -150 and price_val <= 200 and ev_val >= 5

        tag = ""
        role = role_map.get(book_key)
        if role and qualifies:
            tag = f" {role}"
            roles.add(role)

        odds_str = _format_odds(price_val)
        book_line = f"{book}: {odds_str}{tag}"
        if book_key == best_key:
            book_line = f"**{book_line}**"
        lines.append(f"• {book_line}")

    odds_block = "\n".join(lines)
    roles_text = f"📣 {' '.join(sorted(roles))}" if roles else ""
    return odds_block, roles_text
