#!/usr/bin/env python
"""Dispatch a simulation-only snapshot for mainline markets."""

import os
import sys
import json
import io
from typing import List
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
import pandas as pd
import requests

from utils import safe_load_json
from core.logger import get_logger

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
        return []
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
        requests.post(webhook_url, json={"content": f"```\n{message}\n```"})
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
        resp = requests.post(
            webhook_url,
            data={"payload_json": json.dumps({"content": caption})},
            files=files,
            timeout=10,
        )
        resp.raise_for_status()
        logger.info("✅ Snapshot sent (%d rows)", df.shape[0])
    except Exception as e:  # pragma: no cover - network errors
        logger.error("❌ Failed to send snapshot: %s", e)
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
    parser.add_argument("--min-ev", type=float, default=5.0)
    parser.add_argument("--max-ev", type=float, default=20.0)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit total rows dispatched",
    )
    args = parser.parse_args()

    args.min_ev = max(5.0, args.min_ev)
    args.max_ev = min(20.0, args.max_ev)
    if args.min_ev > args.max_ev:
        args.max_ev = args.min_ev

    path = args.snapshot_path or latest_snapshot_path()
    if not path or not os.path.exists(path):
        logger.error("❌ Snapshot not found: %s", path)
        return

    rows = load_rows(path)
    rows = filter_by_date(rows, args.date)

    # Filter EV and mainline markets only
    rows = [
        r
        for r in rows
        if args.min_ev <= r.get("ev_percent", 0) <= args.max_ev
        and str(r.get("market_class", "main")).lower() == "main"
    ]
    logger.info(
        "🧪 Dispatch filter → %d rows with %.1f ≤ EV%% ≤ %.1f",
        len(rows),
        args.min_ev,
        args.max_ev,
    )

    if not rows:
        logger.info("⚠️ No rows after filtering.")
        return

    df = pd.DataFrame(rows)
    df["Game"] = df.get("game_id", "")
    df["Market"] = df.get("market", "")
    df["Side"] = df.get("side", "")
    df["Sim Prob"] = (df.get("sim_prob", 0) * 100).map("{:.1f}%".format)
    df["Fair Odds"] = df.get("blended_fv", df.get("fair_odds", "")).apply(
        lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
    )
    df["EV%"] = df.get("ev_percent", 0).map("{:+.1f}%".format)

    df = df[["Game", "Market", "Side", "Sim Prob", "Fair Odds", "EV%"]]
    df = df.sort_values(by="EV%", key=lambda s: s.str.replace("%", "").astype(float), ascending=False)

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
