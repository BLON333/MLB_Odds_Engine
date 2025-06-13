from __future__ import annotations

from typing import Iterable, Tuple, Dict, Any
import re


def parse_american_odds(val: str | float | int | None) -> float | None:
    """Return numeric odds from various representations."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except Exception:
            return None
    if isinstance(val, str):
        s = val.strip()
        if not s or s.upper() == "N/A":
            return None
        try:
            return float(s)
        except Exception:
            m = re.match(r"^[+-]?\d+", s)
            if m:
                try:
                    return float(m.group())
                except Exception:
                    return None
    return None


def _get_odds_value(row: Dict[str, Any]) -> float | None:
    for key in ("market_odds", "odds", "odds_display"):
        if key in row:
            val = parse_american_odds(row.get(key))
            if val is not None:
                return val
    return None


def filter_snapshot_rows(
    rows: Iterable[Dict[str, Any]],
    min_ev: float = 3,
    odds_range: Tuple[float, float] = (-150, 200),
) -> list[Dict[str, Any]]:
    """Return rows filtered by EV% and odds range."""
    min_odds, max_odds = odds_range
    result = []
    for r in rows:
        try:
            ev = float(r.get("ev_percent", 0))
        except Exception:
            ev = 0.0
        if ev < min_ev:
            continue
        odds_val = _get_odds_value(r)
        if odds_val is not None and not (min_odds <= odds_val <= max_odds):
            continue
        result.append(r)
    return result
