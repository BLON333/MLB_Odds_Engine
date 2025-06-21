"""Utility functions for theme key handling."""

from typing import Tuple


def make_theme_key(game_id: str, theme: str, segment: str) -> str:
    """Return a canonical theme key string."""
    return f"{game_id}::{theme}::{segment}"


def parse_theme_key(key: str) -> Tuple[str, str, str]:
    """Parse a theme key string into its components."""
    return tuple(key.split("::", 2))  # type: ignore[return-value]


def theme_key_equals(a: str, b: str) -> bool:
    """Return True if two theme key strings represent the same key."""
    return parse_theme_key(a) == parse_theme_key(b)
