import os
import io
from datetime import datetime
import json
import pandas as pd

try:
    import dataframe_image as dfi
except ImportError:  # pragma: no cover - dependency not installed
    dfi = None

import requests


def _style_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Return a styled DataFrame with conditional formatting."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")


    def highlight_ev(val: str):
        try:
            v = float(str(val).replace("%", ""))
        except Exception:
            return ""
        if v >= 15:
            return "background-color: #d4edda"  # light green
        return ""

    styled = (
        df.style
        .applymap(highlight_ev, subset=["EV"])
        .set_properties(**{
            "text-align": "left",
            "font-family": "monospace",
            "font-size": "10pt",
        })
        .set_caption(f"Generated: {timestamp}")
        .set_table_styles([
            {
                "selector": "th",
                "props": [
                    ("font-weight", "bold"),
                    ("background-color", "#e0f7fa"),
                    ("color", "black"),
                    ("text-align", "center"),
                ],
            }
        ])
    )

    try:
        styled = styled.hide_index()
    except AttributeError:  # pragma: no cover - pandas < 1.4
        pass

    return styled


def send_bet_snapshot_to_discord(df: pd.DataFrame, market_type: str, webhook_url: str):
    """Generate a styled image from `df` and send it to a Discord webhook."""
    if df is None or df.empty:
        return

    if dfi is None:
        # dataframe_image not available -> fallback to plain text
        _send_table_text(df, market_type, webhook_url)
        return

    df = df.sort_values(by="EV", ascending=False)
    styled = _style_dataframe(df)

    buf = io.BytesIO()
    dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
    buf.seek(0)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    caption = (
        f"📈 **Live Market Snapshot — {market_type}**\n"
        f"_Generated: {timestamp}_\n"
        f"_(Not an official bet — for informational purposes only)_"
    )

    files = {"file": ("snapshot.png", buf, "image/png")}
    try:
        resp = requests.post(
            webhook_url,
            data={"payload_json": json.dumps({"content": caption})},
            files=files,
            timeout=10,
        )
        resp.raise_for_status()
    except Exception:
        _send_table_text(df, market_type, webhook_url)
    finally:
        buf.close()


def _send_table_text(df: pd.DataFrame, market_type: str, webhook_url: str) -> None:
    """Send the DataFrame as a Markdown code block to Discord."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    caption = f"📈 **Live Market Snapshot — {market_type}** (text fallback)"\
        f"\n_Generated: {timestamp}_"

    try:
        table = df.to_markdown(index=False)
    except Exception:
        table = df.to_string(index=False)

    message = f"{caption}\n```\n{table}\n```\n_(Not an official bet — for informational purposes only)_"
    requests.post(webhook_url, json={"content": message}, timeout=10)