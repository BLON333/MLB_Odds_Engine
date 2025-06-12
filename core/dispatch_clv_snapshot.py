#!/usr/bin/env python
"""Generate and dispatch a CLV snapshot for open bets."""

import os
import sys
import json
import csv
import io
import argparse
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils import (
    parse_game_id,
    canonical_game_id,
    normalize_line_label,
    normalize_to_abbreviation,
    to_eastern,
    now_eastern,
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
)
from core.logger import get_logger
from core.odds_fetcher import american_to_prob
from core.market_pricer import to_american_odds

try:
    import dataframe_image as dfi
except Exception:  # pragma: no cover - optional dependency
    dfi = None

load_dotenv()
logger = get_logger(__name__)

WEBHOOK_URL = os.getenv("DISCORD_CLV_SNAPSHOT_WEBHOOK_URL") or os.getenv(
    "DISCORD_ALERT_WEBHOOK_URL"
)


def latest_odds_file(folder: str = "data/market_odds") -> str | None:
    files = [
        f
        for f in os.listdir(folder)
        if f.startswith("market_odds_") and f.endswith(".json")
    ]
    if not files:
        files = [f for f in os.listdir(folder) if f.endswith(".json")]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join(folder, files[0])


# ---------------------------------------------------------------------------
# Odds helpers copied from closing_odds_monitor
# ---------------------------------------------------------------------------

def fuzzy_match_side(side, market_data):
    def clean(s: str) -> str:
        return (
            s.replace(" ", "")
            .replace("+", "")
            .replace("-", "")
            .replace(".", "")
            .lower()
        )

    side_clean = clean(side)

    for key in market_data.keys():
        if clean(key) == side_clean:
            logger.debug("🧠 Fuzzy match: %s → %s", side, key)
            return key
    for key in market_data.keys():
        if side_clean in clean(key) or clean(key) in side_clean:
            logger.debug("🧠 Fuzzy partial: %s → %s", side, key)
            return key
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.upper().startswith(abbr):
            rest = side[len(abbr) :].strip()
            reconstructed = f"{full_name} {rest}".strip()
            for key in market_data.keys():
                if clean(key) == clean(reconstructed):
                    return key
    if side in TEAM_NAME_TO_ABBR:
        abbr = TEAM_NAME_TO_ABBR[side]
        for key in market_data.keys():
            if clean(key) == clean(abbr):
                return key
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.lower() == full_name.lower():
            for key in market_data:
                if clean(key) == clean(abbr):
                    return key
    if side.lower().startswith("over") or side.lower().startswith("under"):
        prefix = side.split()[0]
        try:
            number = side.split()[1]
        except IndexError:
            return None
        side_compact = f"{prefix}{number}".lower()
        for key in market_data.keys():
            if clean(key) == clean(side_compact):
                return key
    return None


def find_matching_closing_odds(side, market_key, market_data, threshold=1.0):
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
    return consensus_odds.get(market_key) or consensus_odds.get(f"alternate_{market_key}")


# ---------------------------------------------------------------------------
# Snapshot helpers
# ---------------------------------------------------------------------------

def load_logged_bets(path: str) -> list:
    if not os.path.exists(path):
        logger.error("❌ Logged bets CSV not found: %s", path)
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_odds(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.error("❌ Failed to load odds file %s: %s", path, e)
        return {}


def parse_start_time(gid: str, odds_game: dict | None) -> datetime | None:
    parts = parse_game_id(gid)
    date = parts.get("date")
    time_token = parts.get("time", "")
    dt = None
    if time_token.startswith("T"):
        digits = "".join(c for c in time_token[1:] if c.isdigit())[:4]
        if len(digits) == 4:
            try:
                dt = datetime.strptime(f"{date} {digits}", "%Y-%m-%d %H%M")
            except Exception:
                dt = None
    if dt is None and odds_game:
        start_iso = odds_game.get("start_time")
        if start_iso:
            try:
                dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            except Exception:
                dt = None
    return to_eastern(dt) if dt else None


def lookup_consensus_prob(odds_game: dict, market: str, side: str) -> float | None:
    if not odds_game:
        return None
    market_data = get_market_data_with_alternates(odds_game, market)
    if not isinstance(market_data, dict):
        return None
    key = side if side in market_data else None
    if key is None:
        key, _ = find_matching_closing_odds(side, market, market_data)
    entry = market_data.get(key) if key else None
    if not isinstance(entry, dict):
        return None
    prob = entry.get("consensus_prob")
    if prob is None and entry.get("price") is not None:
        prob = american_to_prob(entry["price"])
    return prob


def build_snapshot_rows(csv_rows: list, odds_data: dict) -> list:
    results = []
    now = now_eastern()
    for row in csv_rows:
        gid = canonical_game_id(row.get("game_id", ""))
        game_odds = odds_data.get(gid) or odds_data.get(gid.split("-T")[0])
        start_dt = parse_start_time(gid, game_odds)
        if start_dt and start_dt <= now:
            continue
        consensus_prob = lookup_consensus_prob(game_odds, row.get("market", ""), row.get("side", ""))
        if consensus_prob is None:
            continue
        bet_prob = american_to_prob(row.get("market_odds"))
        if bet_prob is None:
            continue
        clv_pct = round((consensus_prob - bet_prob) * 100, 2)
        fv_odds = to_american_odds(consensus_prob)
        try:
            stake = float(row.get("stake", 0))
        except Exception:
            stake = 0.0
        expected_profit = round(stake * clv_pct / 100, 2)
        parts = parse_game_id(gid)
        date = row.get("Date") or parts.get("date", "")
        matchup = row.get("Matchup") or f"{parts.get('away','')} @ {parts.get('home','')}"
        time_val = row.get("Time", "")
        if not time_val and start_dt:
            try:
                time_val = start_dt.strftime("%-I:%M %p")
            except Exception:
                time_val = start_dt.strftime("%I:%M %p").lstrip("0")
        market_class_key = row.get("market_class", "main").lower()
        market_class = "Alt" if market_class_key.startswith("alt") else "Main"
        odds_str = row.get("market_odds")
        try:
            odds_str = f"{int(float(odds_str)):+}"
        except Exception:
            odds_str = str(odds_str)
        results.append(
            {
                "Date": date,
                "Time": time_val,
                "Matchup": matchup,
                "Market Class": market_class,
                "Market": row.get("market", ""),
                "Bet": row.get("side", ""),
                "Book": row.get("best_book", row.get("book", "")),
                "Odds": odds_str,
                "FV": f"{round(fv_odds)}" if isinstance(fv_odds, (int, float)) else "N/A",
                "CLV%": f"{clv_pct:+.2f}%",
                "Stake": f"{stake:.2f}u",
                "Expected Profit": f"{expected_profit:.2f}u",
            }
        )
    return results


# ---------------------------------------------------------------------------
# Discord helpers (styled dataframe)
# ---------------------------------------------------------------------------

def _style_plain(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    styled = (
        df.style.set_properties(
            **{"text-align": "center", "font-family": "monospace", "font-size": "10pt"}
        )
        .set_table_styles(
            [
                {
                    "selector": "th",
                    "props": [
                        ("font-weight", "bold"),
                        ("background-color", "white"),
                        ("color", "black"),
                        ("text-align", "center"),
                    ],
                }
            ]
        )
    )
    try:
        styled = styled.hide_index()
    except Exception:
        pass
    return styled


def send_snapshot(df: pd.DataFrame, webhook_url: str) -> None:
    if df.empty:
        logger.info("⚠️ No open bets to report.")
        return
    if dfi is None:
        logger.warning("⚠️ dataframe_image not available. Sending text fallback.")
        table = df.to_string(index=False)
        requests.post(webhook_url, json={"content": f"```\n{table}\n```"})
        return
    styled = _style_plain(df)
    buf = io.BytesIO()
    try:
        dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
    except Exception as e:
        logger.error("❌ dfi.export failed: %s", e)
        buf.close()
        table = df.to_string(index=False)
        requests.post(webhook_url, json={"content": f"```\n{table}\n```"})
        return
    buf.seek(0)
    caption = "📊 **CLV Snapshot**"
    files = {"file": ("snapshot.png", buf, "image/png")}
    try:
        resp = requests.post(
            webhook_url,
            data={"payload_json": json.dumps({"content": caption})},
            files=files,
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("✅ CLV snapshot sent (%d rows)", df.shape[0])
    except Exception as e:
        logger.error("❌ Failed to send snapshot: %s", e)
    finally:
        buf.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch CLV snapshot for open bets")
    parser.add_argument("--log-path", default="logs/market_evals.csv", help="Path to market_evals.csv")
    parser.add_argument("--odds-path", default=None, help="Path to odds snapshot JSON")
    parser.add_argument("--output-discord", action="store_true")
    args = parser.parse_args()

    csv_rows = load_logged_bets(args.log_path)
    if not csv_rows:
        logger.error("❌ No logged bets found")
        return

    odds_path = args.odds_path or latest_odds_file()
    if not odds_path or not os.path.exists(odds_path):
        logger.error("❌ Odds snapshot not found: %s", odds_path)
        return
    odds_data = load_odds(odds_path)

    rows = build_snapshot_rows(csv_rows, odds_data)
    if not rows:
        logger.info("⚠️ No qualifying open bets found.")
        return
    df = pd.DataFrame(rows)
    df = df.sort_values(by="CLV%", key=lambda s: s.str.replace("%", "").astype(float), ascending=False)

    if args.output_discord and WEBHOOK_URL:
        send_snapshot(df, WEBHOOK_URL)
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
