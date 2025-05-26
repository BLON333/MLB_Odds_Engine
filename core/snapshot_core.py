# Shared snapshot utilities for generator scripts
import os
import json
from datetime import datetime
from typing import List, Dict, Tuple
import io

import pandas as pd

import requests

try:
    import dataframe_image as dfi
except Exception:  # pragma: no cover - optional dep
    dfi = None

from utils import (
    convert_full_team_spread_to_odds_key,
    normalize_to_abbreviation,
    get_market_entry_with_alternate_fallback,
)
from market_pricer import (
    to_american_odds,
    kelly_fraction,
    blend_prob,
    calculate_ev_from_prob,
)
from core.market_movement_tracker import detect_market_movement


def build_argument_parser(
    description: str,
    output_discord_default: bool = True,
    include_stake_mode: bool = False,
    include_debug_json: bool = False,
) -> "argparse.ArgumentParser":
    """Return a parser with common snapshot CLI options."""
    import argparse

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Comma-separated list of dates",
    )
    parser.add_argument("--min-ev", type=float, default=0.05)
    parser.add_argument("--max-ev", type=float, default=0.20)
    if include_stake_mode:
        parser.add_argument("--stake-mode", default="model")
    parser.add_argument("--output-discord", dest="output_discord", action="store_true")
    parser.add_argument(
        "--no-output-discord", dest="output_discord", action="store_false"
    )
    if include_debug_json:
        parser.add_argument(
            "--debug-json", default=None, help="Path to write debug output"
        )
    parser.add_argument(
        "--diff-highlight",
        action="store_true",
        help="Highlight new rows and odds movements",
    )
    parser.add_argument(
        "--reset-snapshot",
        action="store_true",
        help="Clear stored snapshot before running",
    )
    parser.set_defaults(output_discord=output_discord_default)
    return parser


def _style_dataframe(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Return a styled DataFrame with conditional formatting."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    def _apply_movement(col: str, move_col: str):
        def inner(series):
            colors = []
            moves = df.get(move_col)
            for mv in moves if moves is not None else []:
                if mv == "better":
                    colors.append("background-color: #d4edda")
                elif mv == "worse":
                    colors.append("background-color: #f8d7da")
                else:
                    colors.append("")
            return colors

        return inner

    styled = df.style.set_caption(f"Generated: {timestamp}")
    if "odds_movement" in df.columns:
        styled = styled.apply(_apply_movement("Odds", "odds_movement"), subset=["Odds"])
    if "fv_movement" in df.columns:
        styled = styled.apply(_apply_movement("FV", "fv_movement"), subset=["FV"])
    if "ev_movement" in df.columns:
        styled = styled.apply(_apply_movement("EV", "ev_movement"), subset=["EV"])
    if "is_new" in df.columns:

        def highlight_new(row):
            return [
                "background-color: #e6ffe6" if row.get("is_new") else "" for _ in row
            ]

        styled = styled.apply(highlight_new, axis=1)

    styled = styled.set_properties(
        subset=df.columns.tolist(),
        **{
            "text-align": "left",
            "font-family": "monospace",
            "font-size": "10pt",
        },
    ).set_table_styles(
        [
            {
                "selector": "th",
                "props": [
                    ("font-weight", "bold"),
                    ("background-color", "#e0f7fa"),
                    ("color", "black"),
                    ("text-align", "center"),
                ],
            }
        ]
    )

    try:
        styled = styled.hide_index()
    except AttributeError:
        pass

    hide_cols = [
        c
        for c in [
            "odds_movement",
            "fv_movement",
            "ev_movement",
            "is_new",
            "market_class",
        ]
        if c in df.columns
    ]
    if hide_cols:
        try:
            styled = styled.hide(axis="columns", subset=hide_cols)
        except Exception:
            try:
                styled = styled.hide_columns(hide_cols)
            except Exception:
                pass

    return styled


def send_bet_snapshot_to_discord(
    df: pd.DataFrame, market_type: str, webhook_url: str
) -> None:
    """Render a styled image and send it to a Discord webhook."""
    if df is None or df.empty:
        print(f"⚠️ No snapshot rows to send for {market_type}.")
        return
    if dfi is None:
        print("⚠️ dataframe_image is not available. Skipping image send.")
        return

    if "EV" in df.columns:
        sort_tmp = df["EV"].str.replace("%", "", regex=False)
        try:
            sort_vals = sort_tmp.astype(float)
            df = (
                df.assign(_ev_sort=sort_vals)
                .sort_values("_ev_sort", ascending=False)
                .drop(columns="_ev_sort")
            )
        except Exception:
            df = df.sort_values(by="EV", ascending=False)
    else:
        df = df.sort_values(by="ev_percent", ascending=False)
    styled = _style_dataframe(df)

    buf = io.BytesIO()
    try:
        dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
    except Exception as e:
        print(f"❌ dfi.export failed: {e}")
        try:
            buf.seek(0)
            buf.truncate(0)
            dfi.export(styled, buf, table_conversion="matplotlib", max_rows=-1)
        except Exception as e2:
            print(f"⚠️ Fallback export failed: {e2}")
            buf.close()
            _send_table_text(df, market_type, webhook_url)
            return
    buf.seek(0)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    caption = (
        f"📈 **Live Market Snapshot — {market_type}**\n" f"_Generated: {timestamp}_"
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
        print(f"✅ Snapshot sent for {market_type}.")
    except Exception as e:
        print(f"❌ Failed to send snapshot for {market_type}: {e}")
    finally:
        buf.close()


def _send_table_text(df: pd.DataFrame, market_type: str, webhook_url: str) -> None:
    """Send the DataFrame as a Markdown code block to Discord."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    caption = f"📈 **Live Market Snapshot — {market_type}** (text fallback)\n_Generated: {timestamp}_"

    try:
        table = df.to_markdown(index=False)
    except Exception:
        table = df.to_string(index=False)

    message = f"{caption}\n```\n{table}\n```"
    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"❌ Failed to send text snapshot for {market_type}: {e}")


def compare_and_flag_new_rows(
    current_entries: List[dict], snapshot_path: str
) -> Tuple[List[dict], Dict[str, dict]]:
    """Return entries annotated with new-row and movement flags."""
    try:
        with open(snapshot_path) as f:
            last_snapshot = json.load(f)
    except Exception:
        last_snapshot = {}

    seen = set()
    flagged = []
    next_snapshot = {}

    for entry in current_entries:
        game_id = entry.get("game_id", "")
        book = entry.get("best_book", "")
        key = f"{game_id}:{entry.get('market')}:{entry.get('side')}:{book}"
        blended_fv = entry.get("blended_fv", entry.get("fair_odds"))
        market_odds = entry.get("market_odds")
        ev_pct = entry.get("ev_percent")

        if blended_fv is None or ev_pct is None or market_odds is None:
            continue

        next_snapshot[key] = {
            "game_id": game_id,
            "market": entry.get("market"),
            "side": entry.get("side"),
            "best_book": book,
            "blended_fv": blended_fv,
            "market_odds": market_odds,
            "ev_percent": ev_pct,
            "sim_prob": entry.get("sim_prob"),
            "market_prob": entry.get("market_prob"),
            "segment": entry.get("segment"),
            "stake": entry.get("stake"),
            "market_class": entry.get("market_class"),
            "date_simulated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "display": build_display_block(entry),
        }

        j = json.dumps(entry, sort_keys=True)
        if j in seen:
            continue
        seen.add(j)

        prev = last_snapshot.get(key)
        movement = detect_market_movement(
            {
                "blended_fv": blended_fv,
                "market_odds": market_odds,
                "ev_percent": ev_pct,
            },
            prev,
        )
        entry.update(movement)
        flagged.append(entry)

    return flagged, next_snapshot


def format_table_with_highlights(entries: List[dict]) -> str:
    """Render rows with emoji to highlight changes."""
    lines = []
    for e in entries:
        new_sym = "🟢" if e.get("is_new") else " "
        odds_sym = {"better": "🟢", "worse": "🔴", "same": ""}.get(
            e.get("odds_movement"), ""
        )
        ev_sym = {"better": "🟢", "worse": "🔴", "same": ""}.get(
            e.get("ev_movement"), ""
        )
        fair = e.get("blended_fv", e.get("fair_odds"))
        if isinstance(fair, (int, float)):
            fair_str = f"{fair:+}"
        else:
            fair_str = str(fair)
        ev = e.get("ev_percent", 0.0)
        ev_str = f"{ev:+.1f}%"
        line = f"{new_sym} {e.get('market', ''):<7} | {e.get('side', ''):<12} | {odds_sym} {fair_str:>6} | {ev_sym} {ev_str}"
        lines.append(line)
    return "\n".join(lines)


def load_simulations(sim_dir: str) -> dict:
    sims = {}
    if not os.path.isdir(sim_dir):
        print(f"⚠️ Sim directory not found: {sim_dir}")
        return sims
    for f in os.listdir(sim_dir):
        if f.endswith(".json"):
            path = os.path.join(sim_dir, f)
            try:
                with open(path) as fh:
                    sims[f.replace(".json", "")] = json.load(fh)
            except Exception as e:
                print(f"⚠️ Failed to load {path}: {e}")
    return sims


def build_snapshot_rows(
    sim_data: dict, odds_data: dict, min_ev: float, debug_log=None
) -> list:
    if debug_log is None:
        debug_log = []
    rows = []
    for game_id, sim in sim_data.items():
        markets = sim.get("markets", [])
        odds = odds_data.get(game_id)
        if not odds:
            print(f"⚠️ No odds for {game_id}")
            continue
        start_str = odds.get("start_time")
        hours_to_game = 8.0
        if start_str:
            try:
                dt = datetime.fromisoformat(start_str)
                hours_to_game = (dt - datetime.now(dt.tzinfo)).total_seconds() / 3600
            except Exception:
                pass
        if hours_to_game < 0:
            print(
                f"⏱️ Skipping {game_id} — game has already started ({hours_to_game:.2f}h ago)"
            )
            debug_log.append(
                {
                    "game_id": game_id,
                    "reason": "game_live",
                    "hours_to_game": round(hours_to_game, 2),
                }
            )
            continue
        for entry in markets:
            market = entry.get("market")
            side = entry.get("side")
            sim_prob = entry.get("sim_prob")
            if market is None or side is None or sim_prob is None:
                continue

            lookup_side = (
                normalize_to_abbreviation(side.strip()) if market == "h2h" else side
            )
            market_entry, _, matched_key, segment, price_source = (
                get_market_entry_with_alternate_fallback(odds, market, lookup_side)
            )
            if not isinstance(market_entry, dict):
                alt = convert_full_team_spread_to_odds_key(lookup_side)
                market_entry, _, matched_key, segment, price_source = (
                    get_market_entry_with_alternate_fallback(odds, market, alt)
                )
            if not isinstance(market_entry, dict):
                continue

            price = market_entry.get("price")
            if price is None:
                continue

            consensus_prob = market_entry.get("consensus_prob")
            p_blended, _, _, p_market = blend_prob(
                sim_prob, price, market, hours_to_game, consensus_prob
            )
            ev_pct = calculate_ev_from_prob(p_blended, price)
            stake = kelly_fraction(p_blended, price, fraction=0.25)
            market_clean = matched_key.replace("alternate_", "")
            market_class = "alternate" if price_source == "alternate" else "main"

            print(
                f"✓ {game_id} | {market_clean} | {side} → EV {ev_pct:.2f}% | Stake {stake:.2f}u | Source {market_entry.get('pricing_method', 'book')}"
            )

            row = {
                "game_id": game_id,
                "market": market_clean,
                "side": side,
                "sim_prob": round(sim_prob, 4),
                "market_prob": round(p_market, 4),
                "blended_prob": round(p_blended, 4),
                "blended_fv": to_american_odds(p_blended),
                "market_odds": price,
                "ev_percent": round(ev_pct, 2),
                "stake": stake,
                "full_stake": stake,
                "segment": segment,
                "market_class": market_class,
                "_raw_sportsbook": market_entry.get("per_book", {}),
            }
            rows.append(row)
    return rows


def format_for_display(rows: list, include_movement: bool = False) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Date"] = df["game_id"].apply(lambda x: "-".join(x.split("-")[:3]))
    df["Matchup"] = df["game_id"].apply(lambda x: x.split("-")[-1].replace("@", " @ "))
    if "market_class" not in df.columns:
        df["market_class"] = "main"
    df["Market Class"] = (
        df["market_class"]
        .map({"alternate": "📐 Alt Line", "main": "🏆 Main"})
        .fillna("❓")
    )
    df["Market"] = df["market"]
    df["Bet"] = df["side"]
    if "best_book" in df.columns:
        df["Book"] = df["best_book"]
    else:
        df["Book"] = ""
    df["Odds"] = df["market_odds"].apply(
        lambda x: f"{x:+}" if isinstance(x, (int, float)) else x
    )
    df["Sim %"] = (df["sim_prob"] * 100).map("{:.1f}%".format)
    df["Mkt %"] = (df["market_prob"] * 100).map("{:.1f}%".format)
    df["FV"] = df["blended_fv"].apply(
        lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
    )
    df["EV"] = df["ev_percent"].map("{:+.1f}%".format)
    df["Stake"] = df["stake"].map("{:.2f}u".format)

    required_cols = [
        "Date",
        "Matchup",
        "Market Class",
        "Market",
        "Bet",
        "Book",
        "Odds",
        "Sim %",
        "Mkt %",
        "FV",
        "EV",
        "Stake",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = "N/A"

    if include_movement:
        movement_cols = []
        for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"]:
            if c in df.columns:
                movement_cols.append(c)
        return df[required_cols + movement_cols + ["market_class"]]
    return df[required_cols + ["market_class"]]


def build_display_block(row: dict) -> Dict[str, str]:
    """Return formatted display fields for a snapshot row."""
    game_id = str(row.get("game_id", ""))
    date = "-".join(game_id.split("-")[:3]) if game_id else ""
    matchup = game_id.split("-")[-1].replace("@", " @ ") if game_id else ""

    market_class_key = row.get("market_class", "main")
    market_class = {
        "alternate": "📐 Alt Line",
        "main": "🏆 Main",
    }.get(market_class_key, "❓")

    odds = row.get("market_odds")
    if isinstance(odds, (int, float)):
        odds_str = f"{odds:+}"
    else:
        odds_str = str(odds) if odds is not None else "N/A"

    sim_prob = row.get("sim_prob")
    sim_str = f"{sim_prob * 100:.1f}%" if sim_prob is not None else "N/A"

    mkt_prob = row.get("market_prob")
    mkt_str = f"{mkt_prob * 100:.1f}%" if mkt_prob is not None else "N/A"

    fv = row.get("blended_fv", row.get("fair_odds"))
    fv_str = f"{round(fv)}" if isinstance(fv, (int, float)) else "N/A"

    ev = row.get("ev_percent")
    ev_str = f"{ev:+.1f}%" if ev is not None else "N/A"

    stake = row.get("stake")
    stake_str = f"{stake:.2f}u" if stake is not None else "N/A"

    return {
        "Date": date,
        "Matchup": matchup,
        "Market Class": market_class,
        "Market": row.get("market", ""),
        "Bet": row.get("side", ""),
        "Book": row.get("best_book", ""),
        "Odds": odds_str,
        "Sim %": sim_str,
        "Mkt %": mkt_str,
        "FV": fv_str,
        "EV": ev_str,
        "Stake": stake_str,
    }


def export_market_snapshots(df: pd.DataFrame, snapshot_paths: Dict[str, str]) -> None:
    """Write full market tables to JSON files."""
    if not snapshot_paths:
        return
    os.makedirs(os.path.dirname(list(snapshot_paths.values())[0]), exist_ok=True)
    for market, path in snapshot_paths.items():
        subset = df[df["Market"].str.lower().str.startswith(market.lower(), na=False)]
        try:
            subset.to_json(path, orient="records", indent=2)
        except Exception as e:
            print(f"❌ Failed to export {market} snapshot to {path}: {e}")
