#!/usr/bin/env python
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "cli")))

import json
import argparse
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from odds_fetcher import fetch_market_odds_from_api
from market_pricer import (
    implied_prob,
    decimal_odds,
    to_american_odds,
    kelly_fraction,
    blend_prob,
    calculate_ev_from_prob,
)

from log_betting_evals import expand_snapshot_rows_with_kelly, get_theme
from utils import (
    convert_full_team_spread_to_odds_key,
    normalize_to_abbreviation,
    get_segment_from_market,
)

DEBUG_LOG = []

SNAPSHOT_DIR = "backtest"
SNAPSHOT_PATH = os.path.join(SNAPSHOT_DIR, "last_table_snapshot.json")
# Additional JSON exports for each market type
MARKET_SNAPSHOT_PATHS = {
    "spreads": os.path.join(SNAPSHOT_DIR, "last_spreads_snapshot.json"),
    "h2h": os.path.join(SNAPSHOT_DIR, "last_h2h_snapshot.json"),
    "totals": os.path.join(SNAPSHOT_DIR, "last_totals_snapshot.json"),
}

load_dotenv()

import requests
import io

try:
    import dataframe_image as dfi
except ImportError:
    dfi = None

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
        styled = styled.apply(
            lambda row: [
                "background-color: #fff3cd" if row.get("is_new") else "" for _ in row
            ],
            axis=1,
        )

    styled = (
        styled
        .set_properties(subset=df.columns.tolist(), **{
            "text-align": "left",
            "font-family": "monospace",
            "font-size": "10pt",
        })
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
    except AttributeError:
        pass

    # Hide movement metadata columns if present
    hide_cols = [c for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"] if c in df.columns]
    if hide_cols:
        try:
            styled = styled.hide(axis="columns", subset=hide_cols)
        except Exception:
            try:
                styled = styled.hide_columns(hide_cols)
            except Exception:
                pass

    return styled

def send_bet_snapshot_to_discord(df: pd.DataFrame, market_type: str, webhook_url: str):
    """Render a styled image and send it to a Discord webhook."""
    if df is None or df.empty:
        print(f"⚠️ No snapshot rows to send for {market_type}.")
        return

    if dfi is None:
        print("⚠️ dataframe_image is not available. Skipping image send.")
        return

    df = df.sort_values(by="EV", ascending=False)
    styled = _style_dataframe(df)

    buf = io.BytesIO()
    try:
        dfi.export(styled, buf, table_conversion="chrome", max_rows=-1)
    except Exception as e:
        print(f"\u274c dfi.export failed: {e}")
        return   
    buf.seek(0)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M ET")
    caption = (
        f"📈 **Live Market Snapshot — {market_type}**\n"
        f"_Generated: {timestamp}_\n"
        f"_(Not an official bet — informational only)_"
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

WEBHOOKS = {
    "h2h": os.getenv("DISCORD_H2H_WEBHOOK_URL"),
    "spreads": os.getenv("DISCORD_SPREADS_WEBHOOK_URL"),
    "totals": os.getenv("DISCORD_TOTALS_WEBHOOK_URL"),
}


def _movement(curr, prev):
    """Return movement category for numeric values."""
    if prev is None:
        return "same"
    if curr > prev:
        return "better"
    if curr < prev:
        return "worse"
    return "same"


from typing import List, Dict, Tuple


def compare_and_flag_new_rows(
    current_entries: List[dict],
    snapshot_path: str = SNAPSHOT_PATH,
) -> Tuple[List[dict], Dict[str, dict]]:
    """Return entries annotated with new-row and movement flags.

    Parameters
    ----------
    current_entries : List[dict]
        The list of market entries for the current run.
    snapshot_path : str, optional
        File path of the previous snapshot JSON.

    Returns
    -------
    Tuple[List[dict], Dict[str, dict]]
        The flagged entries and the snapshot dictionary for the next run.
    """
    try:
        with open(snapshot_path) as f:
            last_snapshot = json.load(f)
    except Exception:
        last_snapshot = {}

    seen = set()
    flagged = []
    next_snapshot = {}

    for entry in current_entries:
        key = f"{entry.get('market')}:{entry.get('side')}"
        fair_odds = entry.get("blended_fv", entry.get("fair_odds"))
        market_odds = entry.get("market_odds")
        ev_pct = entry.get("ev_percent")

        if fair_odds is None or ev_pct is None or market_odds is None:
            continue

        next_snapshot[key] = {
            "fair_odds": fair_odds,
            "market_odds": market_odds,
            "ev_percent": ev_pct,
        }

        j = json.dumps(entry, sort_keys=True)
        if j in seen:
            continue
        seen.add(j)

        prev = last_snapshot.get(key)
        entry["is_new"] = prev is None
        entry["odds_movement"] = _movement(market_odds, prev.get("market_odds") if prev else None)
        entry["fv_movement"] = _movement(fair_odds, prev.get("fair_odds") if prev else None)
        entry["ev_movement"] = _movement(ev_pct, prev.get("ev_percent") if prev else None)
        flagged.append(entry)

    return flagged, next_snapshot


def format_table_with_highlights(entries: List[dict]) -> str:
    """Render rows with emoji to highlight changes."""
    lines = []
    for e in entries:
        new_sym = "🟢" if e.get("is_new") else " "
        odds_sym = {"better": "🟢", "worse": "🔴", "same": ""}.get(e.get("odds_movement"), "")
        ev_sym = {"better": "🟢", "worse": "🔴", "same": ""}.get(e.get("ev_movement"), "")
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


def build_snapshot_rows(sim_data: dict, odds_data: dict, min_ev: float, debug_log=None) -> list:
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
        for entry in markets:
            market = entry.get("market")
            side = entry.get("side")
            sim_prob = entry.get("sim_prob")
            if market is None or side is None or sim_prob is None:
                continue

            market_block = odds.get(market, {})
            if market == "h2h":

                side_key = normalize_to_abbreviation(side.strip())
                price_entry = market_block.get(side_key)
                if price_entry is None:
                    print(
                        f"⚠️ H2H side not found: {side} → tried '{side_key}' in {list(market_block.keys())}"
                    )
            else:
                price_entry = market_block.get(side)
            if price_entry is None:
                alt = convert_full_team_spread_to_odds_key(side)
                price_entry = market_block.get(alt)
            if price_entry is None:
                continue

            price = price_entry.get("price")
            if price is None:
                continue

            consensus_prob = price_entry.get("consensus_prob")
            p_blended, _, _, p_market = blend_prob(sim_prob, price, market, hours_to_game, consensus_prob)
            ev_pct = calculate_ev_from_prob(p_blended, price)
            stake = kelly_fraction(p_blended, price, fraction=0.25)
            segment = get_segment_from_market(market)
            market_class = "alternate" if market.startswith("alternate_") else "main"

            print(
                f"✓ {game_id} | {market} | {side} → EV {ev_pct:.2f}% | Stake {stake:.2f}u | Source {price_entry.get('pricing_method', 'book')}"
            )

            row = {
                "game_id": game_id,
                "market": market,
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
                "_raw_sportsbook": price_entry.get("per_book", {}),
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
    df["Market Class"] = df["market_class"].map({"alternate": "📐 Alt Line", "main": "🏆 Main"}).fillna("❓")
    df["Market"] = df["market"]
    df["Bet"] = df["side"]
    if "best_book" in df.columns:
        df["Book"] = df["best_book"]
    else:
        df["Book"] = ""
    df["Odds"] = df["market_odds"].apply(lambda x: f"{x:+}" if isinstance(x, (int, float)) else x)
    df["Sim %"] = (df["sim_prob"] * 100).map("{:.1f}%".format)
    df["Mkt %"] = (df["market_prob"] * 100).map("{:.1f}%".format)
    df["FV"] = df["blended_fv"].apply(lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A")
    df["EV"] = df["ev_percent"].map("{:+.1f}%".format)
    df["Stake"] = df["stake"].map("{:.2f}u".format)

    # Ensure all required columns exist to avoid styling/indexing errors
    required_cols = ["Date", "Matchup", "Market Class", "Market", "Bet", "Book", "Odds", "Sim %", "Mkt %", "FV", "EV", "Stake"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = "N/A"

    if include_movement:
        movement_cols = []
        for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"]:
            if c in df.columns:
                movement_cols.append(c)
        return df[required_cols + movement_cols]

    return df[required_cols]


def export_market_snapshots(df: pd.DataFrame, snapshot_paths: dict) -> None:
    """Write full market tables to JSON files."""
    os.makedirs(os.path.dirname(list(snapshot_paths.values())[0]), exist_ok=True)
    for market, path in snapshot_paths.items():
        subset = df[df["Market"].str.contains(market, case=False, na=False)]
        try:
            subset.to_json(path, orient="records", indent=2)
        except Exception as e:
            print(f"❌ Failed to export {market} snapshot to {path}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Generate live market snapshots from latest sims")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"), help="Comma-separated list of dates")
    parser.add_argument("--min-ev", type=float, default=0.05)
    parser.add_argument("--max-ev", type=float, default=0.20)
    parser.add_argument("--stake-mode", default="model")
    parser.add_argument("--output-discord", dest="output_discord", action="store_true")
    parser.add_argument("--no-output-discord", dest="output_discord", action="store_false")
    parser.add_argument("--debug-json", default=None, help="Path to write debug output")
    parser.add_argument("--diff-highlight", action="store_true", help="Highlight new rows and odds movements")
    parser.add_argument("--reset-snapshot", action="store_true", help="Clear stored snapshot before running")
    parser.set_defaults(output_discord=True)
    args = parser.parse_args()

    if args.reset_snapshot and os.path.exists(SNAPSHOT_PATH):
        os.remove(SNAPSHOT_PATH)

    date_list = [d.strip() for d in str(args.date).split(',') if d.strip()]
    all_rows = []
    for date_str in date_list:
        sim_dir = os.path.join("backtest", "sims", date_str)
        sims = load_simulations(sim_dir)
        if not sims:
            print(f"❌ No simulation files found for {date_str}.")
            continue

        odds = fetch_market_odds_from_api(list(sims.keys()))
        if not odds:
            print(f"❌ Failed to fetch market odds for {date_str}.")
            continue

        all_rows.extend(build_snapshot_rows(sims, odds, args.min_ev, DEBUG_LOG))

    # Filter expanded rows based on EV% and a minimum 1u Kelly stake
    rows = expand_snapshot_rows_with_kelly(
        all_rows,
        min_ev=args.min_ev * 100,
        min_stake=1.0,
    )
    rows = [
        r
        for r in rows
        if (
            args.min_ev * 100 <= r.get("ev_percent", 0) <= args.max_ev * 100
            and r.get("stake", 0) >= 1.0
        )
    ]

    if not rows:
        print("⚠️ No qualifying bets found.")
        return

    if args.diff_highlight:
        rows, snapshot_next = compare_and_flag_new_rows(rows, SNAPSHOT_PATH)
    else:
        snapshot_next = {}
        for r in rows:
            fair_odds = r.get("blended_fv")
            market_odds = r.get("market_odds")
            ev_pct = r.get("ev_percent")
            if fair_odds is None or ev_pct is None or market_odds is None:
                continue
            key = f"{r['market']}:{r['side']}"
            snapshot_next[key] = {
                "fair_odds": fair_odds,
                "market_odds": market_odds,
                "ev_percent": ev_pct,
            }

    df = format_for_display(rows, include_movement=args.diff_highlight)

    df_export = df.drop(columns=[c for c in ["odds_movement", "fv_movement", "ev_movement", "is_new"] if c in df.columns])
    export_market_snapshots(df_export, MARKET_SNAPSHOT_PATHS)

    if args.output_discord:
        for mkt, webhook in WEBHOOKS.items():
            if not webhook:
                continue
            subset = df[df["Market"].str.contains(mkt, case=False, na=False)]
            print(f"📡 Evaluating snapshot for: {mkt} → {subset.shape[0]} rows")
            if subset.empty:
                print(f"⚠️ No bets for {mkt}")
                continue
            send_bet_snapshot_to_discord(subset, mkt, webhook)
    else:
        if args.diff_highlight:
            print(format_table_with_highlights(rows))
        else:
            print(df.to_string(index=False))

    # Save snapshot for next run
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshot_next, f, indent=2)

    if args.debug_json:
        try:
            with open(args.debug_json, "w") as fh:
                json.dump(DEBUG_LOG, fh, indent=2)
            print(f"📝 Debug info written to {args.debug_json}")
        except Exception as e:
            print(f"❌ Failed to write debug file: {e}")


if __name__ == "__main__":
    main()