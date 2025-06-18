#!/usr/bin/env python
"""Dispatch a simulation-only snapshot for mainline markets."""

from core.config import DEBUG_MODE, VERBOSE_MODE
import os
import sys
import json
import io
from typing import List
import argparse
from core.bootstrap import *  # noqa

from dotenv import load_dotenv
import pandas as pd
import requests
from requests.exceptions import Timeout

from utils import safe_load_json, post_with_retries
from core.logger import get_logger
from core.market_pricer import (
    extract_best_book,
    calculate_ev_from_prob,
    to_american_odds,
    kelly_fraction,
)

# Load environment variables from a .env file in the working directory
load_dotenv()

logger = get_logger(__name__)
logger.debug("✅ Loaded webhook: %s", os.getenv("DISCORD_SIM_ONLY_MAIN_WEBHOOK_URL"))

try:
    import dataframe_image as dfi
except Exception:  # pragma: no cover - optional dep
    dfi = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def latest_snapshot_path(folder: str = "backtest") -> str | None:
    files = sorted(
        [f for f in os.listdir(folder) if f.startswith("market_snapshot_") and f.endswith(".json")],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None


def load_rows(path: str) -> List[dict]:
    rows = safe_load_json(path)
    if rows is None:
        logger.error("❌ Failed to load snapshot %s", path)
        sys.exit(1)
    return rows


def filter_by_date(rows: List[dict], date_str: str | None) -> List[dict]:
    if not date_str:
        return rows
    return [r for r in rows if str(r.get("snapshot_for_date")) == date_str]


# ---------------------------------------------------------------------------
# Styling & Discord Helpers
# ---------------------------------------------------------------------------

def _style_plain(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Return a simple white-background style."""
    styled = (
        df.style.set_properties(
            **{
                "text-align": "center",
                "font-family": "monospace",
                "font-size": "10pt",
            }
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
    """Render and send the DataFrame image to Discord."""
    if df.empty:
        logger.info("⚠️ No snapshot rows to send.")
        return

    if dfi is None:
        logger.warning("⚠️ dataframe_image not available. Sending text fallback.")
        message = df.to_string(index=False)
        try:
            post_with_retries(
                webhook_url,
                json={"content": f"```\n{message}\n```"},
                timeout=15,
            )
        except Timeout:
            logger.error("❌ Discord post failed due to timeout")
            sys.exit(1)
        except Exception as e:
            logger.error("❌ Failed to send snapshot: %s", e)
            sys.exit(1)
        return

    styled = _style_plain(df)
    buf = io.BytesIO()
    try:
        dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
    except Exception as e:
        logger.error("❌ dfi.export failed: %s", e)
        buf.close()
        return
    buf.seek(0)

    caption = "📊 Simulation-Only Snapshot Feed (Mainlines Only)"
    files = {"file": ("snapshot.png", buf, "image/png")}
    try:
        resp = post_with_retries(
            webhook_url,
            data={"payload_json": json.dumps({"content": caption})},
            files=files,
            timeout=15,
        )
        if resp:
            logger.info("✅ Snapshot sent (%d rows)", df.shape[0])
    except Timeout:
        logger.error("❌ Discord post failed due to timeout")
        sys.exit(1)
    except Exception as e:  # pragma: no cover - network errors
        logger.error("❌ Failed to send snapshot: %s", e)
        sys.exit(1)
    finally:
        buf.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Dispatch sim-only mainline snapshot")
    parser.add_argument("--snapshot-path", default=None, help="Path to unified snapshot JSON")
    parser.add_argument("--date", default=None, help="Filter by game date")
    parser.add_argument("--output-discord", action="store_true")
    parser.add_argument("--min-ev", type=float, default=10.0)
    parser.add_argument("--max-ev", type=float, default=20.0)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit total rows dispatched",
    )
    args = parser.parse_args()

    args.min_ev = max(10.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        sys.exit(1)

    rows = load_rows(path)
    rows = filter_by_date(rows, args.date)

    dedup: dict[tuple, dict] = {}
    for r in rows:
        if str(r.get("market_class", "main")).lower() != "main":
            continue

        per_book = r.get("_raw_sportsbook") or r.get("per_book") or {}
        best_book = r.get("best_book") or extract_best_book(per_book)
        best_odds = None
        if best_book and isinstance(per_book, dict):
            best_odds = per_book.get(best_book)
        if best_odds is None:
            best_odds = r.get("market_odds")

        try:
            odds_val = float(best_odds)
            if odds_val.is_integer():
                odds_val = int(odds_val)
        except Exception:
            continue

        sim_prob = r.get("sim_prob")
        if sim_prob is None:
            continue
        try:
            ev_sim = calculate_ev_from_prob(float(sim_prob), odds_val)
            stake_units = kelly_fraction(float(sim_prob), odds_val, fraction=0.25)
        except Exception:
            continue

        key = (r.get("game_id", ""), r.get("market", ""), r.get("side", ""))
        entry = {
            "Game": key[0],
            "Market": key[1],
            "Side": key[2],
            "Book": best_book or "",
            "Odds": odds_val,
            "Sim Prob": sim_prob,
            "Fair Odds": to_american_odds(float(sim_prob)),
            "EV_numeric": ev_sim,
            "Stake_units": stake_units,
        }

        if key not in dedup or entry["EV_numeric"] > dedup[key]["EV_numeric"]:
            dedup[key] = entry

    processed = [
        v for v in dedup.values() if args.min_ev <= v["EV_numeric"] <= args.max_ev
    ]

    if not processed:
        logger.info("⚠️ No rows after filtering.")
        return

    df = pd.DataFrame(processed)
    df["Sim Prob"] = (df["Sim Prob"] * 100).map("{:.1f}%".format)
    df["Fair Odds"] = df["Fair Odds"].apply(
        lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
    )
    df["Odds"] = df["Odds"].apply(
        lambda x: f"{int(x):+}" if isinstance(x, (int, float)) else "N/A"
    )
    df["EV%"] = df["EV_numeric"].map("{:+.1f}%".format)
    df["Stake"] = df["Stake_units"].map("{:.2f}u".format)
    df = df.drop(columns=["EV_numeric", "Stake_units"])

    df = df[[
        "Game",
        "Market",
        "Side",
        "Book",
        "Odds",
        "Sim Prob",
        "Fair Odds",
        "EV%",
        "Stake",
    ]]
    df = df.sort_values(
        by="EV%", key=lambda s: s.str.replace("%", "").astype(float), ascending=False
    )

    if args.max_rows and args.max_rows > 0:
        df = df.head(args.max_rows)

    if args.output_discord:
        webhook = os.getenv("DISCORD_SIM_ONLY_MAIN_WEBHOOK_URL")
        if not webhook:
            logger.error("❌ DISCORD_SIM_ONLY_MAIN_WEBHOOK_URL not configured")
            return
        logger.info("📤 Using Discord webhook: %s", webhook)
        for start in range(0, len(df), 25):
            send_snapshot(df.iloc[start : start + 25], webhook)
    else:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
