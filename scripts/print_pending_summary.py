#!/usr/bin/env python
"""Print a summary of pending bets."""

from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
from core.bootstrap import *  # noqa
import argparse

from core.utils import safe_load_json

DEFAULT_JSON = os.path.join("logs", "pending_bets.json")


def load_bets(path: str) -> list:
    """Load pending bets from ``path`` and return them as a list."""
    if not os.path.exists(path):
        print(f"\u274c No pending bets file found at: {path}")
        return []

    data = safe_load_json(path)
    if data is None:
        return []

    if isinstance(data, dict):
        return list(data.values())
    if isinstance(data, list):
        return data

    print(f"\u274c Unexpected format in {path} (expected dict or list)")
    return []


def print_summary(bets: list) -> int:
    """Print count and up to 5 sample bets. Return the bet count."""
    count = len(bets)
    print(f"Pending bets: {count}")

    for bet in bets[:5]:
        game_id = bet.get("game_id", "N/A")
        market = bet.get("market", "N/A")
        side = bet.get("side", "N/A")
        reason = bet.get("reason") or bet.get("skip_reason") or "N/A"
        print(f"- {game_id} | {market} | {side} | {reason}")

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Show summary of pending bets")
    parser.add_argument(
        "--json",
        default=DEFAULT_JSON,
        help="Path to pending_bets.json",
    )
    args = parser.parse_args()

    bets = load_bets(args.json)
    count = print_summary(bets)

    if count:
        sys.exit(1)


if __name__ == "__main__":
    main()