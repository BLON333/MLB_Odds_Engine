import sys
import os
from core.bootstrap import *  # noqa
from core.config import DEBUG_MODE, VERBOSE_MODE

from core.logger import get_logger
logger = get_logger(__name__)

import csv
import json
import time
import requests
from datetime import datetime
from core.utils import (
    now_eastern,
    to_eastern,
    safe_load_json,
    normalize_to_abbreviation,
    normalize_line_label,
    canonical_game_id,
    extract_game_id_from_event,
)
from dotenv import load_dotenv


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

from core.odds_fetcher import (
    fetch_consensus_for_single_game,
    fetch_market_odds_from_api,
    american_to_prob,
)
from core.consensus_pricer import get_paired_label
from core.market_pricer import to_american_odds
from core.utils import TEAM_NAME_TO_ABBR, TEAM_ABBR_TO_NAME, TEAM_ABBR

from dotenv import load_dotenv
from pathlib import Path

dotenv_file = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_file)
loaded_hooks = [
    v
    for v in [
        os.getenv("DISCORD_ALERT_WEBHOOK_URL"),
        os.getenv("DISCORD_ALERT_WEBHOOK_URL_2"),
    ]
    if v and v.strip()
]
logger.info("🔧 Loaded %s Discord webhook(s) from %s", len(loaded_hooks), dotenv_file)

# Support sending CLV alerts to multiple Discord channels. Users can define
# `DISCORD_ALERT_WEBHOOK_URL` and optionally `DISCORD_ALERT_WEBHOOK_URL_2` in
# their .env file. Any non-empty URLs will receive the same alert message.
DISCORD_ALERT_WEBHOOK_URLS = loaded_hooks
closing_odds_path = "data/closing_odds"
os.makedirs(closing_odds_path, exist_ok=True)

fetched_games = set()
debug_mode = False  # ✅ easy toggle for debug

def send_discord_alert(message):
    if not DISCORD_ALERT_WEBHOOK_URLS:
        logger.error("❌ No Discord webhook configured for alerts.")
        return
    for url in DISCORD_ALERT_WEBHOOK_URLS:
        try:

            resp = requests.post(url, json={"content": message}, timeout=10)
            if resp.status_code in (200, 204):
                logger.info("✅ CLV alert sent to Discord webhook: %s", url)
            else:
                logger.error(
                    "❌ Discord webhook %s returned %s: %s", url, resp.status_code, resp.text
                )
        except Exception as e:
            logger.error("❌ Failed to send Discord alert to %s: %s", url, e)


def load_tracked_games(csv_path="logs/market_evals.csv"):
    bets = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bets.append(row)
    return bets


def fetch_single_book_fallback(game_id, books=("pinnacle", "betonlineag")):
    """Fetch odds from a single bookmaker as a fallback."""
    for book in books:
        try:
            data = fetch_market_odds_from_api([game_id], filter_bookmakers=[book])
            odds = data.get(game_id) if data else None
            if odds:
                logger.warning(
                    "⚠️ Using %s prices as fallback for %s", book, game_id
                )
                return odds
        except Exception as e:
            logger.error(
                "❌ Fallback fetch with %s failed for %s: %s", book, game_id, e
            )
    return None


from core.utils import (
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
)  # Make sure this is imported at the top


def fuzzy_match_side(side, market_data):
    def clean(s):
        return (
            s.replace(" ", "")
            .replace("+", "")
            .replace("-", "")
            .replace(".", "")
            .lower()
        )

    side_clean = clean(side)

    # ✅ Try exact normalized match
    for key in market_data.keys():
        if clean(key) == side_clean:
            logger.debug("🧠 Fuzzy match: Exact normalized match for '%s' → '%s'", side, key)
            return key

    # ✅ Try partial substring or reverse match
    for key in market_data.keys():
        if side_clean in clean(key) or clean(key) in side_clean:
            logger.debug("🧠 Fuzzy match: Partial match for '%s' → '%s'", side, key)
            return key

    # ✅ Detect abbreviation prefix (e.g., "HOU -1.5") and expand to full name
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.upper().startswith(abbr):
            rest = side[len(abbr):].strip()
            reconstructed = f"{full_name} {rest}".strip()
            for key in market_data.keys():
                if clean(key) == clean(reconstructed):
                    logger.debug(
                        "🧠 Fuzzy match: Abbrev prefix '%s' → '%s' → '%s'",
                        side,
                        reconstructed,
                        key,
                    )
                    return key

    # ✅ Try Full Team Name → Abbreviation (e.g., "San Diego Padres" → "SD")
    if side in TEAM_NAME_TO_ABBR:
        abbr = TEAM_NAME_TO_ABBR[side]
        for key in market_data.keys():
            if clean(key) == clean(abbr):
                logger.debug("🧠 Fuzzy match: Abbreviation '%s' for '%s' → '%s'", abbr, side, key)
                return key

    # ✅ NEW: Try Abbreviation → Full Team Name (e.g., "SD" → "San Diego Padres")
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.lower() == full_name.lower():
            for key in market_data:
                if clean(key) == clean(abbr):
                    logger.debug(
                        "🧠 Fuzzy match: Full name '%s' → abbr '%s' → key '%s'",
                        full_name,
                        abbr,
                        key,
                    )
                    return key

    # ✅ Handle compact Over/Under formatting (e.g., Under8.0 vs Under8)
    if side.lower().startswith("over") or side.lower().startswith("under"):
        prefix = side.split()[0]
        try:
            number = side.split()[1]
        except IndexError:
            return None
        side_compact = f"{prefix}{number}".lower()
        for key in market_data.keys():
            if clean(key) == clean(side_compact):
                logger.debug("🧠 Fuzzy match: Over/Under compact '%s' → '%s'", side, key)
                return key

    # ❌ Nothing worked
    logger.debug(
        "⚠️ Fuzzy match failed for '%s' — tried keys: %s",
        side,
        list(market_data.keys())[:5],
    )
    return None


def find_matching_closing_odds(side, market_key, market_data, threshold=1.0):
    """Return closest matching odds key and line shift for ``side``."""
    lookup = normalize_to_abbreviation(side)

    if lookup in market_data:
        return lookup, 0.0

    fuzzy = fuzzy_match_side(lookup, market_data)
    if fuzzy:
        return fuzzy, 0.0

    prefix, val = normalize_line_label(lookup)
    if val is None:
        return None, None

    best_key = None
    best_diff = None

    for label in market_data.keys():
        p2, v2 = normalize_line_label(label)
        if p2 != prefix or v2 is None:
            continue
        if market_key.startswith("spreads") and ((val >= 0) != (v2 >= 0)):
            continue
        diff = abs(v2 - val)
        if best_diff is None or diff < best_diff:
            best_key = label
            best_diff = diff

    if best_diff is not None and best_diff <= threshold:
        return best_key, best_diff

    return None, None


def get_market_data_with_alternates(consensus_odds, market_key):
    """
    Try to get market odds from main market or alternate fallback (e.g., totals → alternate_totals)
    """
    return consensus_odds.get(market_key) or consensus_odds.get(
        f"alternate_{market_key}"
    )


def attach_consensus_probs(consensus_odds):
    """Compute devigged probabilities and fair odds for each side."""
    for mkey, market in consensus_odds.items():
        if not isinstance(market, dict):
            continue

        groups = {}
        for label, info in market.items():
            if not isinstance(info, dict):
                continue

            price = info.get("price")
            prefix, point = normalize_line_label(label)

            if "team_totals" in mkey:
                group_key = (prefix.upper(), point)
            elif "totals" in mkey:
                group_key = point
            elif mkey.startswith("spreads") or mkey.startswith("alternate_spreads"):
                group_key = abs(point) if point is not None else None
            else:  # h2h and others
                group_key = None

            groups.setdefault(group_key, []).append((label, price))

        for entries in groups.values():
            if len(entries) != 2:
                continue

            (l1, p1), (l2, p2) = entries
            try:
                imp1 = american_to_prob(p1)
                imp2 = american_to_prob(p2)
                total = imp1 + imp2
                if total <= 0:
                    continue
                prob1 = round(imp1 / total, 6)
                prob2 = round(imp2 / total, 6)
            except Exception:
                continue

            market[l1]["consensus_prob"] = prob1
            market[l1]["consensus_odds"] = round(to_american_odds(prob1), 2)
            market[l2]["consensus_prob"] = prob2
            market[l2]["consensus_odds"] = round(to_american_odds(prob2), 2)


def monitor_loop(poll_interval=600, target_date=None, force_game_id=None):
    """Continuously fetch closing odds for games on ``target_date``.

    ``target_date`` defaults to today's date when the monitor is started and
    remains constant for the entire runtime. This prevents late-night runs from
    switching to the next calendar day mid-loop and inadvertently skipping the
    remaining games of the original date.
    """

    if target_date is None:
        target_date = now_eastern().strftime("%Y-%m-%d")

    while True:
        now_est = now_eastern()
        today = target_date  # Use a fixed date for the entire run

        loaded_bets = load_tracked_games()
        today_str = target_date or now_eastern().strftime("%Y-%m-%d")
        bets = [b for b in loaded_bets if b["game_id"].startswith(today_str)]

        # Normalize game ids to canonical form while preserving any time tag
        tracked_games = {canonical_game_id(b["game_id"]) for b in bets}
        # Keep base ids (without time component) for backwards compatibility
        tracked_bases = {gid.split("-T")[0] for gid in tracked_games}

        logger.info(
            "🔁 Checking games as of %s...",
            now_est.strftime("%Y-%m-%d %H:%M:%S EST"),
        )
        logger.info("🔎 Monitoring %s games with bets placed...", len(tracked_games))

        file_path = os.path.join(closing_odds_path, f"{today}.json")
        if os.path.exists(file_path):
            existing = safe_load_json(file_path)
            if not isinstance(existing, dict):
                logger.warning(
                    "⚠️ Warning: Corrupt closing odds file for %s. Starting fresh.",
                    today,
                )
                existing = {}
        else:
            existing = {}

        try:
            resp = retry_api_call(
                lambda: requests.get(
                    "https://api.the-odds-api.com/v4/sports/baseball_mlb/events",
                    params={"apiKey": os.getenv("ODDS_API_KEY")},
                    timeout=10,
                )
            )
        except Exception as e:
            logger.error("❌ Error fetching events: %s", e)
            time.sleep(poll_interval)
            continue
        if resp.status_code != 200:
            logger.error("❌ Error fetching events: %s", resp.text)
            time.sleep(poll_interval)
            continue
        events = resp.json()

        for event in events:
            start_time = event.get("commence_time", "")
            if not start_time:
                continue

            try:
                game_time_utc = datetime.fromisoformat(
                    start_time.replace("Z", "+00:00")
                )
                game_time = to_eastern(game_time_utc)

                game_date = game_time.strftime("%Y-%m-%d")
                if game_date != today:
                    if debug_mode:
                        logger.debug(
                            "⏩ Skipping %s@%s because game date %s != today %s",
                            event['away_team'],
                            event['home_team'],
                            game_date,
                            today,
                        )
                    continue

                away_team_full = event["away_team"]
                home_team_full = event["home_team"]

                away_abbr = TEAM_ABBR.get(away_team_full, away_team_full.split()[-1])
                home_abbr = TEAM_ABBR.get(home_team_full, home_team_full.split()[-1])

                raw_id = extract_game_id_from_event(
                    away_team_full,
                    home_team_full,
                    game_time,
                )
                gid = canonical_game_id(raw_id)

                time_to_game = (game_time - now_est).total_seconds()
                if debug_mode:
                    logger.debug("DEBUG: %s | time_to_game=%.2fs", gid, time_to_game)

                # Reject games outside the closing odds window
                if time_to_game > 600 or time_to_game < 0:
                    if debug_mode:
                        logger.debug(
                            "⏩ Skipping %s - outside 10 minute window (%.2fs)",
                            gid,
                            time_to_game,
                        )
                    continue

                if gid not in tracked_games and gid.split("-T")[0] not in tracked_bases:
                    continue

                if gid in fetched_games:
                    continue

                if gid in existing:
                    if force_game_id and gid == force_game_id:
                        logger.info("🧼 Forcing re-fetch for %s (was already captured)", gid)
                        del existing[gid]
                        try:
                            with open(file_path, "r") as f:
                                data = json.load(f)
                            data.pop(gid, None)
                            with open(file_path, "w") as f:
                                json.dump(data, f, indent=2)
                            logger.info("🧼 Removed %s from saved JSON", gid)
                        except Exception as e:
                            logger.warning("⚠️ Could not remove from saved file: %s", e)
                    else:
                        logger.info("🛑 %s already captured. Skipping re-fetch.", gid)
                        fetched_games.add(gid)
                        continue


                # Only capture closing odds within ~10 minutes of first pitch
                # ``commence_time`` from the API is in UTC, so ``game_time`` is
                # already converted to Eastern above. ``time_to_game`` is
                # therefore an Eastern-based delta in seconds.
                if 0 <= time_to_game <= 600:
                    logger.info("📡 Fetching consensus odds for %s...", gid)

                    try:
                        consensus_odds = retry_api_call(
                            lambda: fetch_consensus_for_single_game(gid),
                            max_attempts=2,
                            wait_seconds=10,
                        )
                    except Exception:
                        logger.warning(
                            "⚠️ No consensus odds for %s after retry, using single-book fallback...",
                            gid,
                        )
                        consensus_odds = fetch_single_book_fallback(gid)
                        if not consensus_odds:
                            logger.warning(
                                "❌ Fallback odds also unavailable for %s",
                                gid,
                            )
                            continue

                    attach_consensus_probs(consensus_odds)

                    # Attach normalized labels for easier lookups
                    #
                    # NOTE: We build the ``_normalized`` blocks in a separate
                    # dictionary and merge them after the loop. Mutating the
                    # ``consensus_odds`` dict while iterating over it previously
                    # triggered ``RuntimeError: dictionary changed size during
                    # iteration``.
                    normalized_blocks = {}
                    for mkey, market_vals in list(consensus_odds.items()):
                        if not isinstance(market_vals, dict):
                            continue
                        normalized_block = {}
                        for label, info in market_vals.items():
                            if not isinstance(info, dict):
                                continue
                            norm = normalize_to_abbreviation(label)
                            info.setdefault("label_normalized", norm)
                            normalized_block[norm] = info
                        if normalized_block:
                            normalized_blocks[f"{mkey}_normalized"] = normalized_block

                    # Merge normalized blocks after iteration to avoid mutation
                    consensus_odds.update(normalized_blocks)

                    existing[gid] = consensus_odds
                    with open(file_path, "w") as f:
                        json.dump(existing, f, indent=2)
                    logger.info("✅ Saved closing odds snapshot for %s", gid)

                    matching_bets = [b for b in bets if b["game_id"] == gid]
                    if not matching_bets:
                        logger.info("ℹ️ No matching bets for %s.", gid)
                        fetched_games.add(gid)
                        continue

                    logger.info("✅ Found %s matching bets for %s", len(matching_bets), gid)
                    alert_lines = []

                    for bet in matching_bets:
                        market = bet["market"]
                        side = bet["side"]
                        bet_odds = float(bet["market_odds"])

                        market_data = get_market_data_with_alternates(
                            consensus_odds, market
                        )
                        if not market_data:
                            logger.warning(
                                "⚠️ Market '%s' not found in consensus odds for %s. Available markets: %s",
                                market,
                                gid,
                                list(consensus_odds.keys()),
                            )
                            continue

                        # Build normalized lookup map
                        normalized_market_data = {}
                        for label, info in market_data.items():
                            if not isinstance(info, dict):
                                continue
                            norm = info.get("label_normalized") or normalize_to_abbreviation(label)
                            normalized_market_data[norm] = info

                        if debug_mode:
                            logger.debug(
                                "   Available sides for market '%s': %s",
                                market,
                                list(market_data.keys()),
                            )
                            logger.debug("   Attempting to match bet side: '%s'", side)

                        lookup_side = normalize_to_abbreviation(side)
                        closing_data = normalized_market_data.get(lookup_side)
                        line_shift = 0.0

                        if not closing_data:
                            match_key, line_shift = find_matching_closing_odds(
                                side,
                                market,
                                normalized_market_data,
                            )
                            if match_key:
                                closing_data = normalized_market_data[match_key]
                                if debug_mode or line_shift:
                                    logger.debug(
                                        "🎯 Bet Line: %s | Closest Match: %s | Line Shift: %+0.1f",
                                        side,
                                        match_key,
                                        line_shift,
                                    )

                        if not closing_data:
                            paired = get_paired_label(side, market, gid)
                            if paired:
                                paired_norm = normalize_to_abbreviation(paired)
                                paired_data = normalized_market_data.get(paired_norm)
                                if paired_data and paired_data.get("consensus_prob") is not None:
                                    pair_prob = paired_data["consensus_prob"]
                                    closing_prob = 1 - pair_prob
                                    closing_data = {
                                        "consensus_prob": closing_prob
                                    }
                                    if debug_mode:
                                        logger.debug(
                                            "📐 Inferred %s from %s prob %.3f",
                                            side,
                                            paired,
                                            pair_prob,
                                        )
                            if not closing_data:
                                logger.warning(
                                    "⚠️ No match found for bet side '%s' in market '%s'",
                                    side,
                                    market,
                                )
                                labels_list = sorted(market_data.keys())
                                if labels_list:
                                    logger.info(
                                        "📦 Available book options:\n  • %s",
                                        "\n  • ".join(labels_list),
                                    )
                                continue

                        closing_prob = closing_data.get("consensus_prob")
                        if closing_prob is None and "price" in closing_data:
                            closing_prob = american_to_prob(closing_data["price"])
                        if closing_prob is None:
                            logger.warning(
                                "⚠️ No consensus probability for %s (%s) in %s",
                                side,
                                market,
                                gid,
                            )
                            continue

                        closing_american = to_american_odds(closing_prob)
                        bet_prob = american_to_prob(bet_odds)
                        clv = (closing_prob - bet_prob) * 100
                        emoji = "🟢" if clv > 0 else "🔴"

                        line = (
                            f"- **{side} ({market})**\n"
                            f"  • Bet Line: `{bet_odds:+}`\n"
                            f"  • Closing Line: `{closing_american:+}`\n"
                            f"  • CLV: `{clv:+.2f}%` {emoji}"
                        )
                        if line_shift:
                            line += f"\n  • Line Shift: `{line_shift:+.1f}`"
                        alert_lines.append(line)
                        logger.info("✅ Prepared alert line for %s: %s", gid, side)

                    if alert_lines:
                        logger.info(
                            "📣 Will send Discord alert with %s line(s):\n%s",
                            len(alert_lines),
                            "\n".join(alert_lines),
                        )
                        message = f"📊 **CLV Check - {gid}**\n" + "\n".join(alert_lines)
                        send_discord_alert(message)
                    else:
                        logger.info(
                            "ℹ️ Skipping %s — no matched closing odds for any bets.",
                            gid,
                        )

                    fetched_games.add(gid)

            except Exception as e:
                logger.error("⚠️ Error processing event: %s", e)

        logger.info("⏱ Sleeping for %s minutes...\n", poll_interval // 60)
        time.sleep(poll_interval)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Monitor and capture MLB closing odds")
    parser.add_argument(
        "--date",
        dest="date",
        help="YYYY-MM-DD date to monitor (defaults to today's Eastern date)",
    )
    parser.add_argument(
        "--force-game",
        dest="force_game",
        help="Force re-fetch and alert for specific game_id (e.g., 2025-06-04-NYM@LAD)",
    )
    args = parser.parse_args()

    monitor_loop(target_date=args.date, force_game_id=args.force_game)