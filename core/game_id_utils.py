from __future__ import annotations

"""Helpers for constructing and matching ``game_id`` strings."""

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Optional

def _get_eastern_tz():
    from utils import EASTERN_TZ  # imported lazily to avoid circular deps
    return EASTERN_TZ

__all__ = ["build_game_id", "normalize_game_id", "fuzzy_match_game_id"]


def build_game_id(away: str, home: str, start_time_utc: datetime) -> str:
    """Return a time-aware ``game_id`` using ET-local date and time."""
    if start_time_utc.tzinfo is None:
        start_time_utc = start_time_utc.replace(tzinfo=ZoneInfo("UTC"))
    start_et = start_time_utc.astimezone(_get_eastern_tz())
    date_str = start_et.strftime("%Y-%m-%d")
    suffix = start_et.strftime("T%H%M")
    return f"{date_str}-{away}@{home}-{suffix}"


def normalize_game_id(game_id: str) -> str:
    """Strip ``-T`` suffix from ``game_id`` for base matchup comparison."""
    return game_id.split("-T")[0] if "-T" in game_id else game_id


def _suffix_minutes(gid: str) -> Optional[int]:
    if "-T" not in gid:
        return None
    raw = gid.split("-T", 1)[1].split("-", 1)[0]
    try:
        dt = datetime.strptime(raw, "%H%M")
    except Exception:
        return None
    return dt.hour * 60 + dt.minute


def fuzzy_match_game_id(target: str, candidates: List[str], window: int = 5) -> Optional[str]:
    """Return candidate within ``window`` minutes of ``target`` if found."""
    base = normalize_game_id(target)
    target_min = _suffix_minutes(target)
    best: Optional[str] = None
    best_delta: Optional[int] = None
    for cand in candidates:
        if normalize_game_id(cand) != base:
            continue
        cand_min = _suffix_minutes(cand)
        if target_min is None or cand_min is None:
            return cand
        delta = abs(cand_min - target_min)
        if delta <= window and (best_delta is None or delta < best_delta):
            best = cand
            best_delta = delta
    return best

