import os
import sys
from core.bootstrap import *  # noqa
import time
from collections import defaultdict
from datetime import datetime

from core.utils import (
    parse_game_id,
    EASTERN_TZ,
    now_eastern,
)
from core.time_utils import compute_hours_to_game
from core.odds_fetcher import fetch_consensus_for_single_game, american_to_prob
from core.logger import get_logger
from core.confirmation_utils import required_market_move
from core.pending_bets import (
    load_pending_bets,
    save_pending_bets,
    PENDING_BETS_PATH,
)
from core.theme_exposure_tracker import load_tracker as load_theme_stakes, save_tracker as save_theme_stakes
from core.market_eval_tracker import load_tracker as load_eval_tracker
from cli.log_betting_evals import (
    write_to_csv,
    load_existing_stakes,
    record_successful_log,
)
from core.should_log_bet import should_log_bet

logger = get_logger(__name__)

CHECK_INTERVAL = 30 * 60  # 30 minutes


def retry_api_call(func, max_attempts: int = 3, wait_seconds: int = 2):
    """Call ``func`` retrying on Exception."""
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            if attempt < max_attempts - 1:
                logger.warning(
                    "\u26a0\ufe0f API call failed (attempt %d/%d): %s. Retrying...",
                    attempt + 1,
                    max_attempts,
                    e,
                )
                time.sleep(wait_seconds)
            else:
                logger.error(
                    "\u274c API call failed after %d attempts: %s",
                    max_attempts,
                    e,
                )
                raise


def _start_time_from_gid(game_id: str) -> datetime | None:
    parts = parse_game_id(game_id)
    date = parts.get("date")
    time_part = parts.get("time", "")
    if not date:
        return None
    if time_part.startswith("T"):
        # Handle tokens like "T1845" or "T1845-DH1" by isolating the time digits
        raw = time_part.split("-")[0][1:]
        digits = "".join(c for c in raw if c.isdigit())[:4]
        try:
            dt = datetime.strptime(f"{date} {digits}", "%Y-%m-%d %H%M")
            return dt.replace(tzinfo=EASTERN_TZ)
        except Exception:
            return None
    try:
        return datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=EASTERN_TZ)
    except Exception:
        return None


def recheck_pending_bets(path: str = PENDING_BETS_PATH) -> None:
    pending = load_pending_bets(path)
    if not pending:
        return

    existing = load_existing_stakes("logs/market_evals.csv")
    session_exposure = defaultdict(set)
    theme_stakes = load_theme_stakes()
    eval_tracker = load_eval_tracker()

    updated = {}
    for key, bet in pending.items():
        start_dt = _start_time_from_gid(bet["game_id"])
        if not start_dt:
            continue
        hours_to_game = compute_hours_to_game(start_dt)
        if hours_to_game <= 0:
            # Game started; drop entry
            continue
        market_data = retry_api_call(
            lambda: fetch_consensus_for_single_game(bet["game_id"])
        )
        if not market_data:
            updated[key] = bet
            continue
        market = market_data.get(bet["market"], {})
        label_key = next(
            (k for k in market if k.lower() == bet["side"].lower()),
            None,
        )
        if not label_key:
            updated[key] = bet
            continue
        price = market[label_key].get("price")
        new_prob = american_to_prob(price) if price is not None else None
        if new_prob is None:
            updated[key] = bet
            continue
        prev_prob = bet.get("baseline_consensus_prob")
        if prev_prob is None:
            prev_prob = bet.get("consensus_prob")
        try:
            movement = float(new_prob) - float(prev_prob)
        except Exception:
            movement = 0.0
        if movement < required_market_move(hours_to_game):
            updated[key] = bet
            continue
        row = bet.copy()
        row["consensus_prob"] = new_prob
        row["market_prob"] = new_prob
        row["hours_to_game"] = hours_to_game
        ref = {key: {"consensus_prob": prev_prob}}
        evaluated = should_log_bet(
            row,
            theme_stakes,
            verbose=False,
            eval_tracker=eval_tracker,
            reference_tracker=ref,
            existing_csv_stakes=existing,
        )
        if evaluated:
            result = write_to_csv(
                evaluated,
                "logs/market_evals.csv",
                existing,
                session_exposure,
                theme_stakes,
            )
            if result and not result.get("skip_reason") and result.get("side"):
                record_successful_log(result, existing, theme_stakes)
                save_theme_stakes(theme_stakes)
                continue
            else:
                logger.warning(
                    "❌ Skipping tracker update: result was skipped or malformed → %s",
                    result,
                )
        updated[key] = bet

    if updated != pending:
        save_pending_bets(updated, path)


def main() -> None:
    while True:
        recheck_pending_bets()
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()