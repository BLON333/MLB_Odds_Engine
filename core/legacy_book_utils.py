# Legacy bookmaker utilities
"""Deprecated utilities for retrieving bookmaker sets and labels.

These helpers were superseded by :mod:`core.book_whitelist` and are kept
only for backward compatibility.
"""

from warnings import warn

from .bookmakers import BOOKMAKER_CATALOG

# ⚠️ Deprecated – not used in current logging/snapshot logic.
# Logic now relies on core.book_whitelist.ALLOWED_BOOKS.
def get_us_bookmakers(include_exchanges: bool = False,
                      include_dfs: bool = False,
                      include_secondary: bool = False):
    """Return US bookmakers optionally including exchanges/DFS providers."""
    warn(
        "get_us_bookmakers is deprecated; use book_whitelist.ALLOWED_BOOKS",
        DeprecationWarning,
        stacklevel=2,
    )
    keys = list(BOOKMAKER_CATALOG["us"].keys())
    if include_secondary:
        keys += BOOKMAKER_CATALOG["us2"].keys()
    if include_exchanges:
        keys += BOOKMAKER_CATALOG["us_ex"].keys()
    if include_dfs:
        keys += BOOKMAKER_CATALOG["us_dfs"].keys()
    return list(keys)

# ⚠️ Deprecated – not used in current logging/snapshot logic.
# Logic now relies on core.book_whitelist.ALLOWED_BOOKS.
def get_all_bookmaker_keys():
    """Return all bookmaker keys across regions."""
    warn(
        "get_all_bookmaker_keys is deprecated; use book_whitelist.ALLOWED_BOOKS",
        DeprecationWarning,
        stacklevel=2,
    )
    return [key for region in BOOKMAKER_CATALOG.values() for key in region.keys()]

# ⚠️ Deprecated – not used in current logging/snapshot logic.
# Logic now relies on core.book_whitelist.ALLOWED_BOOKS.
def get_all_bookmaker_display_names():
    """Return display names for all known bookmakers."""
    warn(
        "get_all_bookmaker_display_names is deprecated; use book_whitelist.ALLOWED_BOOKS",
        DeprecationWarning,
        stacklevel=2,
    )
    return [label for region in BOOKMAKER_CATALOG.values() for label in region.values()]

# ⚠️ Deprecated – not used in current logging/snapshot logic.
# Logic now relies on core.book_whitelist.ALLOWED_BOOKS.
def get_bookmaker_label(book_key: str):
    """Return a display label for ``book_key`` if known."""
    warn(
        "get_bookmaker_label is deprecated; use book_whitelist.ALLOWED_BOOKS",
        DeprecationWarning,
        stacklevel=2,
    )
    for region in BOOKMAKER_CATALOG.values():
        if book_key in region:
            return region[book_key]
    return book_key
