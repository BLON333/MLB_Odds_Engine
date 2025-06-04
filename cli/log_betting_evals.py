# === Path Setup ===
import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sys
if sys.version_info >= (3, 7):
    import os
    os.environ["PYTHONIOENCODING"] = "utf-8"

# === Core Imports ===
import json, csv, math, argparse, ast
from datetime import datetime
from collections import defaultdict

# === External Notification / Environment ===
import requests
from dotenv import load_dotenv

from core.market_eval_tracker import load_tracker, save_tracker
from utils import safe_load_json

load_dotenv()
from core.logger import get_logger
logger = get_logger(__name__)

# === Console Output Controls ===
SEGMENT_SKIP_LIMIT = 5
segment_skip_count = 0
MOVEMENT_LOG_LIMIT = 5
movement_log_count = 0
VERBOSE = False
SHOW_SKIPPED = False

def log_segment_mismatch(sim_segment: str, book_segment: str) -> None:
    """Print a segment mismatch message with truncation after a limit."""
    global segment_skip_count
    segment_skip_count += 1
    if segment_skip_count <= SEGMENT_SKIP_LIMIT:
        print(
            f"🔒 Skipping due to segment mismatch → Sim: {sim_segment} | Book: {book_segment}"
        )
    elif segment_skip_count == SEGMENT_SKIP_LIMIT + 1:
        print("🔒 ... (truncated additional segment mismatch skips)")


def should_log_movement() -> bool:
    """Return True if movement details should be printed."""
    global movement_log_count
    movement_log_count += 1
    if movement_log_count <= MOVEMENT_LOG_LIMIT:
        return True
    if movement_log_count == MOVEMENT_LOG_LIMIT + 1:
        print("🧠 ... (truncated additional movement logs)")
    return False
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DISCORD_TOTALS_WEBHOOK_URL = os.getenv("DISCORD_TOTALS_WEBHOOK_URL")
DISCORD_H2H_WEBHOOK_URL = os.getenv("DISCORD_H2H_WEBHOOK_URL")
DISCORD_SPREADS_WEBHOOK_URL = os.getenv("DISCORD_SPREADS_WEBHOOK_URL")
OFFICIAL_PLAYS_WEBHOOK_URL = os.getenv("OFFICIAL_PLAYS_WEBHOOK_URL")

# === Market Confirmation Tracker ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MARKET_CONF_TRACKER_PATH = os.path.join(
    SCRIPT_DIR, "..", "logs", "market_conf_tracker.json"
)


def load_market_conf_tracker(path: str = MARKET_CONF_TRACKER_PATH):
    """Load last seen consensus probabilities for bets."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    if os.path.exists(path):
        print(
            f"⚠️ Could not load market confirmation tracker at {path}, starting fresh."
        )


def save_market_conf_tracker(tracker: dict, path: str = MARKET_CONF_TRACKER_PATH):
    """Atomically save tracker data to disk."""
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(tracker, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save market confirmation tracker: {e}")

import copy
from datetime import datetime

def latest_snapshot_path(folder="backtest"):
    """Return the most recent snapshot file from the given folder."""
    files = sorted(
        [f for f in os.listdir(folder) if f.startswith("market_snapshot_") and f.endswith(".json")],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None

# Load tracker for updates during logging
MARKET_EVAL_TRACKER = load_tracker()

# Load most recent snapshot file for movement comparison
SNAPSHOT_PATH_USED = latest_snapshot_path("backtest")
if SNAPSHOT_PATH_USED and os.path.exists(SNAPSHOT_PATH_USED):
    print(f"📂 Using prior snapshot for market movement detection: {SNAPSHOT_PATH_USED}")
    prior_snapshot_data = safe_load_json(SNAPSHOT_PATH_USED) or []

    if isinstance(prior_snapshot_data, list):
        # Convert snapshot list to tracker-style dict
        prior_snapshot_tracker = {
            f"{r['game_id']}:{r['market']}:{r['side']}": r
            for r in prior_snapshot_data
            if "game_id" in r and "market" in r and "side" in r
        }
        print(f"🔁 Loaded {len(prior_snapshot_tracker)} entries from snapshot.")
    else:
        prior_snapshot_tracker = prior_snapshot_data  # already dict

    MARKET_EVAL_TRACKER_BEFORE_UPDATE = prior_snapshot_tracker
else:
    print("⚠️ No valid prior snapshot found — using fallback copy of tracker.")
    MARKET_EVAL_TRACKER_BEFORE_UPDATE = copy.deepcopy(MARKET_EVAL_TRACKER)

# Path used to persist theme exposure between auto loop iterations
PERSISTED_THEME_STAKES_PATH = os.path.join("backtest", "existing_theme_stakes.json")


# === Local Modules ===
from core.market_pricer import (
    implied_prob,
    decimal_odds,
    to_american_odds,
    kelly_fraction,
    blend_prob,
    calculate_ev_from_prob,
    extract_best_book,
)
from core.odds_fetcher import fetch_market_odds_from_api, save_market_odds_to_file
from utils import (
    TEAM_ABBR,
    TEAM_NAME_TO_ABBR,
    TEAM_ABBR_TO_NAME,
    get_market_entry_with_alternate_fallback,
    normalize_segment_name,
    clean_book_prices,
    get_contributing_books,
    get_segment_from_market,
    normalize_lookup_side,  # ✅ This is likely what you actually want
    get_normalized_lookup_side,
    normalize_to_abbreviation,
    convert_full_team_spread_to_odds_key,
    assert_segment_match,
    classify_market_segment,
    find_sim_entry,
    normalize_label,
    get_segment_label,
    canonical_game_id,
    now_eastern,
)


# === Staking Logic Refactor ===
from core.should_log_bet import should_log_bet
from core.market_eval_tracker import load_tracker, save_tracker
from core.market_movement_tracker import (
    track_and_update_market_movement,
    detect_market_movement,
)
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches


# === Bookmaker Key to Discord Role Mapping (using real Discord Role IDs) ===
BOOKMAKER_TO_ROLE = {
    "fanduel": "<@&1366767456470831164>",
    "draftkings": "<@&1366767510246133821>",
    "betmgm": "<@&1366767548502245457>",
    "betonlineag": "<@&1366767585906917437>",
    "bovada": "<@&1366767654966394901>",
    "betrivers": "<@&1366767707403452517>",
    "betus": "<@&1366767782049616004>",
    "williamhill_us": "<@&1366767816086392965>",  # Caesars
    "fanatics": "<@&1366767852123586661>",
    "lowvig": "<@&1366767880762294313>",
    "mybookieag": "<@&1366767916883640361>",
    "ballybet": "<@&1366767951671328888>",
    "betanysports": "<@&1366767991861018644>",
    "betparx": "<@&1366768027483504690>",
    "espnbet": "<@&1366768064200179712>",
    "fliff": "<@&1366768103811452950>",
    "hardrockbet": "<@&1366775051747065877>",
    "windcreek": "<@&1366768133804789760>",
    "pinnacle": "<@&1366768197247963170>",
}

# === Segment Label to Discord Role Mapping (placeholder IDs) ===
SEGMENT_ROLE = {
    "mainline": "<@&SEG_MAINLINE>",
    "alt_line": "<@&SEG_ALTLINE>",
    "team_total": "<@&SEG_TEAMTOTAL>",
    "derivative": "<@&SEG_DERIVATIVE>",
    "pk_equiv": "<@&SEG_PKEQUIV>",
}


# === Lookup Helpers ===
def normalize_lookup_side(side):
    """
    Normalize side label for matching odds:
    - Expand abbreviations like PIT to full team name.
    - Handle Over/Under without changes.
    """
    if side.startswith(("Over", "Under")):
        return side.strip()

    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.startswith(abbr):
            suffix = side[len(abbr) :].strip()
            return f"{full_name} {suffix}".strip()

    return side.strip()


def get_theme_key(market: str, theme: str) -> str:
    if "spreads" in market or "h2h" in market or "runline" in market:
        return f"{theme}_spread"
    elif "totals" in market:
        return f"{theme}_total"
    else:
        return f"{theme}_other"


def remap_side_key(side):
    """
    Standardize side labels:
    - Always expand abbreviations to full team names
    - Preserve Over/Under bets
    """

    # If already a full team name (e.g., 'Pittsburgh Pirates'), keep it
    if side in TEAM_NAME_TO_ABBR:
        return side

    # Check for abbreviation + number (like 'PIT+0.5' or 'MIA-1.5')
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.startswith(abbr):
            rest = side[len(abbr) :].strip()
            return f"{full_name} {rest}".strip()

    # If it's an Over/Under line like 'Over 4.5', 'Under 7.0', leave unchanged
    if side.startswith("Over") or side.startswith("Under"):
        return side

    # Fallback — if unknown, return side as-is
    return side

    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    import os


def generate_clean_summary_image(
    bets,
    output_path="logs/mlb_summary_table_model.png",
    max_rows=25,
    min_ev=5.0,
    max_ev=20.0,
    min_stake=1.0,
    stake_mode="model",
):
    import pandas as pd
    import dataframe_image as dfi
    import os

    # 🔍 Apply logic matching send_discord_notification()
    filtered = []
    for b in bets:
        ev = b.get("ev_percent", 0)
        stake = b.get("stake", 0)
        market = b.get("market", "").strip().lower()

        # 🚫 EV or stake too low
        if ev < min_ev or ev > max_ev or stake < min_stake:
            continue

        # 🚫 Skip totals_1st_5_innings and totals_1st_7_innings
        if market in {"totals_1st_5_innings", "totals_1st_7_innings"}:
            continue

        # 🚫 Skip H2H outside EV bounds
        if market.startswith("h2h") and (ev < 5.0 or ev > 20.0):
            continue

        filtered.append(b)

    print(f"🖼️ Image Summary Candidates ({len(filtered)}):")
    for b in filtered:
        print(
            f"   • {b['game_id']} | {b['market']} | {b['side']} | EV: {b['ev_percent']}% | Stake: {b['stake']}"
        )

    if not filtered:
        print("⚠️ No bets to display in styled image.")
        return

    df = (
        pd.DataFrame(filtered)
        .sort_values(by="ev_percent", ascending=False)
        .head(max_rows)
    )

    df["Sim %"] = df["sim_prob"].apply(lambda x: f"{x * 100:.1f}%")
    df["Mkt %"] = df["market_prob"].apply(lambda x: f"{x * 100:.1f}%")
    df["EV"] = df["ev_percent"].apply(lambda x: f"{x:+.1f}%")
    df["Stake"] = df["stake"].apply(lambda x: f"{x:.2f}u")
    df["Odds"] = df["market_odds"].apply(
        lambda x: f"{x:+}" if isinstance(x, (int, float)) else "N/A"
    )
    df["FV"] = df["blended_fv"].apply(
        lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
    )

    if "segment" in df.columns:
        df["Segment"] = (
            df["segment"]
            .map({"derivative": "📐 Derivative", "full_game": "🏟️ Full Game"})
            .fillna("⚠️ Unknown")
        )
    else:
        df["Segment"] = "⚠️ Unknown"

    df["Date"] = df["game_id"].apply(lambda x: "-".join(x.split("-")[:3]))
    df["Matchup"] = df["game_id"].apply(lambda x: x.split("-")[-1].replace("@", " @ "))

    if "market_class" in df.columns:
        df["Market"] = df.apply(
            lambda r: (
                f"📐 {r['market']}"
                if r.get("market_class") == "alternate"
                else r["market"]
            ),
            axis=1,
        )
    else:
        df["Market"] = df["market"]

    display_df = df[
        [
            "Date",
            "Matchup",
            "Segment",
            "Market",
            "side",
            "best_book",
            "Odds",
            "Sim %",
            "Mkt %",
            "FV",
            "EV",
            "Stake",
        ]
    ].rename(columns={"side": "Bet", "best_book": "Book"})

    styled = display_df.style.set_properties(
        **{"text-align": "left", "font-family": "monospace", "font-size": "11pt"}
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

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    dfi.export(styled, output_path, table_conversion="chrome", max_rows=-1)
    print(f"✅ Saved styled summary image to {output_path}")


def generate_clean_summary_table(
    bets,
    output_dir="logs",
    max_rows=20,
    min_ev=5.0,
    max_ev=20.0,
    min_stake=1.0,
    stake_mode="model",
):
    import pandas as pd
    from datetime import datetime
    import os

    # ✅ Apply same filters as send_discord_notification
    filtered = []
    for b in bets:
        ev = b.get("ev_percent", 0)
        stake = b.get("stake", 0)
        market = b.get("market", "").strip().lower()

        if ev < min_ev or ev > max_ev or stake < min_stake:
            continue

        if market in {"totals_1st_5_innings", "totals_1st_7_innings"}:
            continue

        if market.startswith("h2h") and (ev < 5.0 or ev > 20.0):
            continue

        filtered.append(b)

    if not filtered:
        print("⚠️ No bets to include in HTML table.")
        return

    df = (
        pd.DataFrame(filtered)
        .sort_values(by="ev_percent", ascending=False)
        .head(max_rows)
    )

    df["Sim %"] = (df["sim_prob"] * 100).map("{:.1f}%".format)
    df["Mkt %"] = (df["market_prob"] * 100).map("{:.1f}%".format)
    df["EV"] = df["ev_percent"].map("{:+.1f}%".format)
    df["Stake"] = df["stake"].map("{:.2f}u".format)
    df["Odds"] = df["market_odds"].apply(
        lambda x: f"{x:+}" if isinstance(x, (int, float)) else "N/A"
    )
    df["FV"] = df["blended_fv"].apply(
        lambda x: f"{round(x)}" if isinstance(x, (int, float)) else "N/A"
    )

    if "segment" in df.columns:
        df["Segment"] = (
            df["segment"]
            .map({"derivative": "📐 Derivative", "full_game": "🏟️ Full Game"})
            .fillna("⚠️ Unknown")
        )
    else:
        df["Segment"] = "⚠️ Unknown"

    # 🗓️ Add readable fields
    df["Date"] = df["game_id"].apply(lambda x: "-".join(x.split("-")[:3]))
    df["Matchup"] = df["game_id"].apply(lambda x: x.split("-")[-1].replace("@", " @ "))

    if "market_class" in df.columns:
        df["Market"] = df.apply(
            lambda r: (
                f"📐 {r['market']}"
                if r.get("market_class") == "alternate"
                else r["market"]
            ),
            axis=1,
        )
    else:
        df["Market"] = df["market"]

    display_df = df[
        [
            "Date",
            "Matchup",
            "Segment",
            "Market",
            "side",
            "best_book",
            "Odds",
            "Sim %",
            "Mkt %",
            "FV",
            "EV",
            "Stake",
        ]
    ].rename(columns={"side": "Bet", "best_book": "Book"})

    # 🖼️ Output file path
    date_tag = datetime.now().strftime("%Y-%m-%d")
    filename = f"{output_dir}/mlb_summary_table_{date_tag}.html"
    os.makedirs(output_dir, exist_ok=True)

    # 🧾 Style the HTML
    html = display_df.to_html(
        index=False,
        escape=False,
        classes=["table", "table-bordered", "table-sm", "table-striped"],
        border=0,
    )

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: monospace; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
            th, td {{ text-align: left; padding: 6px; }}
            tr:nth-child(even) {{ background-color: #f2f2f2; }}
            th {{
                background-color: #333;
                color: white;
                text-align: center;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <h2>MLB Model Snapshot – {date_tag}</h2>
        {html}
    </body>
    </html>
    """

    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✅ Saved HTML summary table to {filename}")


def upload_summary_image_to_discord(image_path, webhook_url):
    import requests
    import os

    if not webhook_url:
        print("❌ No Discord webhook URL provided.")
        return

    if not os.path.exists(image_path):
        print(f"❌ Image file not found: {image_path}")
        return

    with open(image_path, "rb") as img:
        files = {"file": (os.path.basename(image_path), img)}
        try:
            response = requests.post(webhook_url, files=files)
            response.raise_for_status()
            print("✅ Summary image uploaded to Discord.")
        except Exception as e:
            print(f"❌ Failed to upload summary image to Discord: {e}")


def expand_snapshot_rows_with_kelly(
    final_snapshot, min_ev=1.0, min_stake=0.5, kelly_fraction=0.25
):
    """
    Expand snapshot rows into 1 row per sportsbook, recalculating EV% and stake using Quarter-Kelly.
    """
    from core.market_pricer import calculate_ev_from_prob

    expanded_rows = []

    


    for bet in final_snapshot:
        base_fields = {
            "game_id": bet.get("game_id", "unknown"),
            "league": bet.get("league", "MLB"),
            "Date": bet.get("Date", ""),
            "Matchup": bet.get("Matchup", bet.get("game_id", "")[-7:]),
            "side": bet.get("side", ""),
            "market": bet.get("market", ""),
            "sim_prob": bet.get("sim_prob", 0),
            "market_prob": bet.get("market_prob", 0),
            "blended_prob": bet.get("blended_prob", bet.get("sim_prob", 0)),
            "blended_fv": bet.get("blended_fv", ""),
            "segment": bet.get("segment"),
            "segment_label": bet.get("segment_label"),
        }

        for field in [
            "ev_movement", "fv_movement", "odds_movement", "stake_movement",
            "sim_movement", "mkt_movement", "is_new",
        ]:
            if field in bet:
                base_fields[field] = bet[field]

        for field in [
            "ev_display", "fv_display", "odds_display", "stake_display",
            "sim_prob_display", "mkt_prob_display",
        ]:
            if field in bet:
                base_fields[field] = bet[field]

        if not isinstance(bet.get("_raw_sportsbook", None), dict):
            print(
                f"⚠️ No expansion data available — keeping existing row: {bet['side']} @ {bet['market']}"
            )
            expanded_rows.append(bet)
            continue

        raw_books = bet.get("_raw_sportsbook") or bet.get("sportsbook", {})
        if not isinstance(raw_books, dict):
            continue  # skip malformed entries

        for book, odds in raw_books.items():
            try:
                p = base_fields.get("blended_prob", base_fields.get("sim_prob", 0))
                stake = round(float(bet.get("full_stake", 0)), 4)
                ev = calculate_ev_from_prob(p, odds)

                if base_fields["side"] == "St. Louis Cardinals":
                    print(f"🔍 {book}: EV={ev:.2f}%, Odds={odds}, Stake={stake:.2f}u")

                tracker_key = f"{base_fields['game_id']}:{base_fields['market']}:{base_fields['side']}"
                prior_snapshot_row = bet.get("_prior_snapshot")

                # 🧪 Optional Debug
                if VERBOSE and not prior_snapshot_row:
                    print(f"⚠️ Missing _prior_snapshot for {tracker_key} in expanded_row")


                if VERBOSE and not prior_snapshot_row:
                    print(f"⚠️ Missing prior snapshot for: {tracker_key}")

                if ev >= min_ev and stake >= min_stake:
                    expanded_row = {
                        **base_fields,
                        "best_book": book,
                        "market_odds": odds,
                        "market_class": bet.get("market_class", "main"),
                        "segment": bet.get("segment"),
                        "segment_label": bet.get("segment_label"),
                        "ev_percent": round(ev, 2),
                        "stake": stake,
                        "full_stake": stake,
                        "_prior_snapshot": prior_snapshot_row,
                    }

                    for field in [
                        "ev_movement", "fv_movement", "odds_movement", "stake_movement",
                        "sim_movement", "mkt_movement", "is_new",
                    ]:
                        if field in base_fields:
                            expanded_row[field] = base_fields[field]

                    for disp in [
                        "ev_display", "fv_display", "odds_display", "stake_display",
                        "sim_prob_display", "mkt_prob_display",
                    ]:
                        if disp in base_fields:
                            expanded_row[disp] = base_fields[disp]

                    expanded_rows.append(expanded_row)
                else:
                    if VERBOSE:
                        if ev < min_ev:
                            print("   ⛔ Skipped: EV too low")
                        if stake < min_stake:
                            print("   ⛔ Skipped: Stake too low")

            except Exception as e:
                print(f"⚠️ Error processing {book}: {e}")
                continue

    # ✅ Deduplicate by (game_id, market, side, best_book)
    seen = set()
    deduped = []
    for row in expanded_rows:
        key = (row["game_id"], row["market"], row["side"], row["best_book"])
        if key not in seen:
            deduped.append(row)
            seen.add(key)

    return deduped



def logistic_decay(t_hours, t_switch=8, slope=1.5):
    return 1 / (1 + math.exp((t_switch - t_hours) / slope))


def base_model_weight_for_market(market):
    if "1st" in market:
        return 0.9  # prioritize derivatives (1st innings) first
    elif (
        market.startswith("h2h")
        or (market.startswith("spreads") and "_" not in market)
        or (market.startswith("totals") and "_" not in market)
    ):
        return 0.6  # mainlines (h2h, spreads, totals without "_")
    else:
        return 0.75  # fallback for anything else


def should_include_in_summary(row):
    """
    Return True if the row qualifies to appear in summary notifications.
    Currently defined as EV ≥ 5.0%.
    """
    return row.get("ev_percent", 0) >= 5.0


def blend_prob(p_model, market_odds, market_type, hours_to_game, p_market=None):
    # Use provided consensus_prob if available, otherwise derive from odds
    if p_market is None:
        p_market = implied_prob(market_odds)

    base_weight = base_model_weight_for_market(market_type)
    w_time = logistic_decay(hours_to_game, t_switch=8, slope=1.5)
    w_model = min(base_weight * w_time, 1.0)
    w_market = 1 - w_model

    p_blended = w_model * p_model + w_market * p_market
    return p_blended, w_model, p_model, p_market


def get_theme(row):
    """
    Group bets into themes for exposure control:
    - Match full team names (handles New York teams correctly)
    - Over/Under bets separately
    """
    side = remap_side_key(row["side"])  # Normalize side first
    market = row["market"]

    if "Over" in side:
        return "Over"
    if "Under" in side:
        return "Under"

    if "h2h" in market or "spreads" in market:
        for full_team_name in TEAM_NAME_TO_ABBR.keys():
            if side.startswith(full_team_name):
                return full_team_name  # ✅ Return full team name
    return "Other"


def count_theme_exposure(existing, game_id, theme):
    return sum(
        1
        for (gid, _, side) in existing.keys()
        if gid == game_id
        and (
            theme in ["Over", "Under"]
            and theme in side
            or theme not in ["Over", "Under"]
            and side.startswith(theme)
        )
    )


def standardize_derivative_label(label):
    """
    Standardize derivative market side labels:
    - Expand team abbreviations to full team names
    - Handle Over/Under bets cleanly
    """

    label = label.strip()

    if label.lower() in {"score ≥1 run", "score >0", "score at least 1"}:
        return "Over 0.5"
    if label.lower() in {"score <1", "score = 0", "score 0 runs"}:
        return "Under 0.5"

    if label.endswith(" win"):
        abbr = label.replace(" win", "").strip()
        return TEAM_ABBR_TO_NAME.get(abbr, abbr)

    if label.startswith("Run line (") and label.endswith(")"):
        inside = label[len("Run line (") : -1]
        parts = inside.split()
        if len(parts) == 2:
            abbr, spread = parts
            full_name = TEAM_ABBR_TO_NAME.get(abbr, abbr)
            return f"{full_name} {spread}".strip()
        return inside

    if label.startswith("Total >"):
        val = label.split(">")[1].strip()
        return f"Over {val}"
    if label.startswith("Total <"):
        val = label.split("<")[1].strip()
        return f"Under {val}"

    # ✅ NEW: Expand simple abbreviations like 'PIT+0.5'
    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if label.startswith(abbr):
            rest = label[len(abbr) :].strip()
            return f"{full_name} {rest}".strip()

    # Fallback
    return label


def calculate_ev(fair_odds, market_odds):
    fair_dec = (1 + abs(100 / fair_odds)) if fair_odds < 0 else (fair_odds / 100 + 1)
    mkt_dec = (
        (1 + abs(100 / market_odds)) if market_odds < 0 else (market_odds / 100 + 1)
    )
    return round((mkt_dec / fair_dec - 1) * 100, 2)


def decimal_odds(american):
    return (
        round(100 / abs(american) + 1, 4)
        if american < 0
        else round(american / 100 + 1, 4)
    )


def calculate_market_fv(sim_prob, market_odds):
    try:
        decimal = (
            100 / abs(market_odds) + 1 if market_odds < 0 else market_odds / 100 + 1
        )
        return round(sim_prob * decimal * 100, 2)
    except:
        return 0.0


def load_existing_stakes(log_path):
    """
    Reads existing market_evals.csv and returns a dict
    keyed by (game_id, market, side) → stake
    """
    existing = {}
    if not os.path.exists(log_path):
        return existing

    with open(log_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                gid = canonical_game_id(row["game_id"])
                key = (gid, row["market"], row["side"])
                stake_str = row.get("stake", "").strip()
                stake = float(stake_str) if stake_str else 0.0
                existing[key] = stake
            except Exception as e:
                print(f"⚠️ Error parsing row {row}: {e}")
    return existing


def load_existing_theme_stakes(csv_path):
    """
    Builds a dict mapping (game_id, theme_key, segment) → total_stake
    to track existing theme-level exposure from the CSV.
    """
    from collections import defaultdict

    theme_stakes = defaultdict(float)

    if not os.path.exists(csv_path):
        return theme_stakes

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                game_id = canonical_game_id(row["game_id"])
                market = row["market"]
                side = row["side"]

                # Safe fallback for malformed stake
                try:
                    stake = float(row.get("stake", 0))
                except:
                    stake = 0.0

                # Identify theme category
                theme = get_theme({"side": side, "market": market})
                if "spreads" in market or "h2h" in market or "runline" in market:
                    theme_key = f"{theme}_spread"
                elif "totals" in market:
                    theme_key = f"{theme}_total"
                else:
                    theme_key = f"{theme}_other"

                # Determine segment: mainline vs. derivative
                segment = (
                    "derivative"
                    if "1st" in market or "7_innings" in market
                    else "mainline"
                )

                # Aggregate total exposure
                theme_stakes[(game_id, theme_key, segment)] += stake

            except Exception as e:
                print(f"⚠️ Error processing theme stake row: {e}")

    return theme_stakes


def load_persisted_theme_stakes(path=PERSISTED_THEME_STAKES_PATH):
    """Load theme exposure from a JSON file."""
    if not os.path.exists(path):
        return {}
    data = safe_load_json(path)
    if not isinstance(data, dict):
        return {}
    stakes = {}
    for k, v in data.items():
        try:
            key = ast.literal_eval(k)
            if isinstance(key, (list, tuple)) and len(key) == 3:
                stakes[tuple(key)] = float(v)
        except Exception:
            continue
    return stakes


def save_persisted_theme_stakes(stakes: dict, path=PERSISTED_THEME_STAKES_PATH):
    """Atomically save theme exposure to a JSON file."""
    serializable = {str(k): v for k, v in stakes.items()}
    tmp = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(tmp, "w") as f:
            json.dump(serializable, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"⚠️ Failed to save theme stakes: {e}")


def get_discord_webhook_for_market(market: str) -> str:
    """Return the Discord webhook URL for a given market type."""
    return OFFICIAL_PLAYS_WEBHOOK_URL or DISCORD_WEBHOOK_URL


def send_discord_notification(row):

    webhook_url = get_discord_webhook_for_market(row.get("market", ""))
    if not webhook_url:
        return
    print(f"Webhook URL resolved: {webhook_url}")

    ev = row["ev_percent"]
    stake = float(row.get("stake", 0))
    entry_type = row.get("entry_type", "first")
    stake = round(stake, 2)
    full_stake = round(float(row.get("full_stake", stake)), 2)
    print(
        f"📬 Sending Discord Notification → stake: {stake}, full: {full_stake}, type: {entry_type}"
    )
    if entry_type == "top-up":
        bet_label = "🔁 Top-Up"
    elif row.get("market_class") == "alternate":
        bet_label = "🟢 First Bet (⅛ Kelly)"
    else:
        bet_label = "🟢 First Bet"

    # Treat as top-up only if full_stake > stake AND stake was previously logged
    if full_stake > stake and full_stake - stake >= 0.5:
        tag = "🔁"
        header = "**Top-Up Bet Logged**"
        topup_note = f"🔁 Top-Up: `{stake:.2f}u` added → Total: `{full_stake:.2f}u`"
    else:
        tag = "🟢" if ev >= 10 else "🟡" if ev >= 5 else "⚪"
        header = "**New Bet Logged**"
        topup_note = ""

    if row.get("test_mode"):
        header = f"[TEST] {header}"

    game_id = row["game_id"]
    side = row["side"]
    market = row["market"]

    if row.get("price_source") == "alternate":
        market_class_tag = "📐 *Alt Line*"
    elif "1st" in market or "innings" in market:
        market_class_tag = "🧩 *Derivative*"
    else:
        market_class_tag = "📊 *Mainline*"

    odds = row["market_odds"]

    from datetime import datetime, timedelta

    now = datetime.now()
    game_date_str = game_id.split("-")[0:3]
    game_date = datetime.strptime("-".join(game_date_str), "%Y-%m-%d").date()

    if game_date == now.date():
        game_day_tag = "📅 *Today*"
    elif game_date == (now.date() + timedelta(days=1)):
        game_day_tag = "📅 *Tomorrow*"
    else:
        game_day_tag = f"📅 *{game_date.strftime('%A')}*"

    from utils import TEAM_ABBR_TO_NAME

    try:
        away_abbr, home_abbr = game_id.split("-")[-1].split("@")
        away_team = TEAM_ABBR_TO_NAME.get(away_abbr, away_abbr)
        home_team = TEAM_ABBR_TO_NAME.get(home_abbr, home_abbr)
        event_label = f"{away_team} @ {home_team}"
    except Exception:
        event_label = game_id

    best_book_data = row.get("best_book", {})
    if isinstance(best_book_data, dict):
        best_book = extract_best_book(best_book_data)
    elif isinstance(best_book_data, str) and best_book_data.strip().startswith("{"):
        try:
            tmp = json.loads(best_book_data.replace("'", '"'))
            best_book = extract_best_book(tmp) or best_book_data
        except Exception:
            best_book = best_book_data
    else:
        best_book = best_book_data or row.get("sportsbook", "N/A")

    tracker_key = f"{game_id}:{market}:{side}"
    prior = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key)
    movement = row.get("_movement")
    if not movement:
        movement = detect_market_movement(row, prior)
    row.setdefault("_movement", movement)
    if movement.get("is_new"):
        print(f"🟡 First-time seen → {tracker_key}")
    else:
        try:
            print(
                f"🧠 Prior FV: {prior.get('blended_fv')} → New FV: {row.get('blended_fv')}"
            )
        except Exception:
            pass
    if movement.get("mkt_movement") != "better":
        print(
            "⛔ Discord notification aborted — Mkt % movement is not 'better'"
        )
        return
    print(f"✅ Market-confirmed bet → {tracker_key} — sending notification")

    sim_prob = row["sim_prob"]
    consensus_prob = row["market_prob"]
    blended_prob = row["blended_prob"]

    def _parse_odds_dict(val):
        """Return a clean {book: odds} dict from various input formats."""
        if isinstance(val, dict):
            # Handle nested dict serialized as the sole key
            if len(val) == 1:
                ((k, v),) = val.items()
                if (
                    isinstance(k, str)
                    and k.strip().startswith("{")
                    and k.strip().endswith("}")
                ):
                    try:
                        inner = json.loads(k.replace("'", '"'))
                        return inner
                    except Exception:
                        pass
            return val
        if isinstance(val, str):
            s = val.strip()
            if s.startswith("{") and s.endswith("}"):
                try:
                    return json.loads(s.replace("'", '"'))
                except Exception:
                    pass
            odds = {}
            for piece in s.split(","):
                if ":" not in piece:
                    continue
                book, price = piece.split(":", 1)
                try:
                    odds[book.strip()] = float(price)
                except Exception:
                    continue
            if odds:
                return odds
        return {}

    all_odds_dict = (
        _parse_odds_dict(row.get("_raw_sportsbook"))
        or _parse_odds_dict(row.get("consensus_books"))
        or _parse_odds_dict(row.get("sportsbook"))
    )

    best_book_name = best_book.lower() if isinstance(best_book, str) else ""

    # === Utility: Convert American Odds to Decimal
    def to_decimal(american_odds):
        try:
            return (
                100 / abs(american_odds) + 1
                if american_odds < 0
                else (american_odds / 100) + 1
            )
        except:
            return 0.0

    # === Sort books and format display
    sorted_books = []
    if isinstance(all_odds_dict, dict):
        sorted_books = sorted(
            all_odds_dict.items(), key=lambda x: to_decimal(x[1]), reverse=True
        )

        all_odds_str_pieces = []
        for book, odds_value in sorted_books:
            book_display = f"{book}: {odds_value}"
            if book.lower() == best_book_name:
                book_display = f"**{book_display}**"
            all_odds_str_pieces.append(f"• {book_display}")

        all_odds_str = "\n".join(all_odds_str_pieces)
    else:
        all_odds_str = "N/A"

    # === Tag Roles from Books
    # Only mention sportsbooks whose individual EV falls within the
    # notification range (5% - 20%). This prevents tagging every book
    # just because the best price qualifies.
    roles = set()
    for book, price in sorted_books:
        model_prob = sim_prob
        offered_decimal = to_decimal(price)
        ev_this_book = (model_prob * offered_decimal - 1) * 100

        if 5 <= ev_this_book <= 20:
            role_tag = BOOKMAKER_TO_ROLE.get(book.lower())
            if role_tag:
                roles.add(role_tag)

    best_role = BOOKMAKER_TO_ROLE.get(best_book_name)
    if best_role:
        roles.add(best_role)

    if len(roles) > 1:
        print(f"🔔 Multiple books tagged: {', '.join(sorted(roles))}")

    if roles:
        roles_text = "📣 " + " ".join(sorted(roles))
    else:
        roles_text = ""

    topup_note = ""
    if entry_type == "top-up" and stake < full_stake:
        topup_note = f"🔁 Top-Up: `{stake:.2f}u` added → Total: `{full_stake:.2f}u`"


    prev_market_prob = None
    if isinstance(prior, dict):
        prev_market_prob = prior.get("market_prob")

    if prev_market_prob is not None:
        market_prob_str = f"{prev_market_prob:.1%} → {consensus_prob:.1%}"
    else:
        market_prob_str = f"{consensus_prob:.1%}"

    message = None
    try:
        game_day_clean = game_day_tag.replace("**", "").replace("*", "")
        message = (
            f"{tag} {header}\n\n"
            f"{game_day_clean} | {market_class_tag} | 🏷 {row.get('segment_label','')}\n"
            f"🏟️ Game: **{event_label}**\n"
            f"🧾 Market: **{market} — {side}**\n"
            f"💰 Stake: **{stake:.2f}u @ {odds}** → {bet_label}\n"
            f"{topup_note}\n\n"
            "---\n\n"
            "📈 **Model vs. Market**\n"
            f"• Sim Win Rate: **{sim_prob:.1%}**\n"
            f"• Market Implied: **{market_prob_str}**\n"
            f"• Blended: **{blended_prob:.1%}**\n"
            f"💸 Fair Value: **{row.get('blended_fv')}**\n"
            f"📊 EV: **{ev:+.2f}%**\n\n"
            "---\n\n"
            f"🏦 **Best Book**: {best_book}\n"
            f"📉 **Market Odds**:\n{all_odds_str}\n\n"
            f"{roles_text}\n"
        )

        response = requests.post(webhook_url, json={"content": message.strip()})
        print(f"Discord response: {response.status_code} | {response.text}")
    except Exception as e:
        print(f"❌ Failed to send Discord message: {e}")
        if message:
            print(f"🔍 Message that failed: {message}")


def get_exposure_key(row):
    market = row["market"]
    game_id = row["game_id"]
    side = remap_side_key(row["side"])

    if "totals" in market:
        market_type = "total"
    elif "spreads" in market or "h2h" in market or "runline" in market:
        market_type = "spread"
    else:
        market_type = "other"

    is_derivative = "_" in market and any(
        x in market for x in ["1st", "f5", "3_innings", "5_innings", "7_innings"]
    )
    segment = "derivative" if is_derivative else "full_game"

    for team in TEAM_NAME_TO_ABBR:
        if side.startswith(team):
            theme = team
            break
    else:
        if "Over" in side:
            theme = "Over"
        elif "Under" in side:
            theme = "Under"
        else:
            theme = "Other"

    theme_key = f"{theme}_{market_type}"
    return (game_id, theme_key, segment)


def write_to_csv(
    row,
    path,
    existing,
    session_exposure,
    existing_theme_stakes,
    dry_run=False,
):
    """
    Final write function for fully approved bets only.

    This function assumes the bet has already passed all pruning:
    - Exposure rules
    - Stake thresholds (min 1u / top-up ≥ 0.5u)
    - EV caps
    - Segment tagging

    It should only be called from process_theme_logged_bets().

    Parameters
    ----------
    existing_theme_stakes : dict
        Mapping used to track current theme exposure in-memory. Updated on
        successful writes.
    """
    key = (row["game_id"], row["market"], row["side"])
    tracker_key = (
        f"{row['game_id']}:{row['market']}:{row['side']}"
    )

    new_conf = row.get("consensus_prob")
    try:
        new_conf_val = float(new_conf) if new_conf is not None else None
    except Exception:
        new_conf_val = None

    prev_conf_val = None
    # if isinstance(MARKET_CONF_TRACKER.get(tracker_key), dict):
    #     prev_conf_val = MARKET_CONF_TRACKER[tracker_key].get("consensus_prob")

    if new_conf_val is None:
        print(f"  ⛔ No valid consensus_prob for {tracker_key} — skipping")
        return None

    # if prev_conf_val is not None and new_conf_val <= prev_conf_val:
    #     print(
    #         f"  ⛔ Market confirmation not improved ({new_conf_val:.4f} ≤ {prev_conf_val:.4f}) — skipping {tracker_key}"
    #     )
    #     return 0
    full_stake = round(float(row.get("full_stake", 0)), 2)
    prev = existing.get(key, 0)
    delta = round(full_stake - prev, 2)

    if prev >= full_stake:
        print(f"  ⛔ Already logged full stake for {key}, skipping.")
        return None

    entry_type = row.get("entry_type", "first")
    stake_to_log = delta
    if entry_type == "first" and stake_to_log < 1.0:
        print(f"  ⛔ First bet stake {stake_to_log:.2f}u below 1.0u — skipping")
        return None
    if entry_type == "top-up" and stake_to_log < 0.5:
        print(f"  ⛔ Top-up stake {stake_to_log:.2f}u below 0.5u — skipping")
        return None

    row["stake"] = stake_to_log
    row["result"] = ""

    if dry_run:
        print(
            f"📝 [Dry Run] Would log: {key} | Stake: {delta:.2f}u | EV: {row['ev_percent']:.2f}%"
        )
        return None

    if VERBOSE and "_prior_snapshot" not in row:
        print(f"⚠️ _prior_snapshot not present in row for {tracker_key}")



    # ===== Market Confirmation =====

    if VERBOSE:
        if "_prior_snapshot" in row:
            print(f"📥 Using injected _prior_snapshot for movement check.")
        else:
            print(f"📥 Falling back to MARKET_EVAL_TRACKER_BEFORE_UPDATE for movement check.")

    prior_snapshot = row.get("_prior_snapshot") or MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key)

    if VERBOSE:
        print(f"📈 Prior Tracker market_prob : {MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key, {}).get('market_prob')}")
        print(f"📈 Attached Snapshot market_prob: {row.get('_prior_snapshot', {}).get('market_prob')}")
        print(f"📈 New market_prob             : {row.get('market_prob')}")

        if row.get("_prior_snapshot") != MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key):
            print(f"⚠️ Snapshot mismatch for {tracker_key}")


    movement = detect_market_movement(row, prior_snapshot)
    row["_movement"] = movement  # store for Discord/export/debug

    # 🔍 Snapshot Debug Metadata
    print(f"\n🔎 Movement Debug for {tracker_key}:")
    print(f"    • Simulated EV           : {row.get('ev_percent')}%")
    print(f"    • Market Prob (New)      : {row.get('market_prob')}")
    print(f"    • Market Prob (Prior)    : {prior_snapshot.get('market_prob') if prior_snapshot else 'None'}")
    print(f"    • Movement               : {movement.get('mkt_movement')}")

    if isinstance(MARKET_EVAL_TRACKER_BEFORE_UPDATE, dict):
        print(f"    • Tracker Source         : Snapshot-Based Tracker (Length: {len(MARKET_EVAL_TRACKER_BEFORE_UPDATE)})")
    else:
        print(f"    • Tracker Source         : Unknown")

    try:
        print(f"    • Snapshot File Used     : {SNAPSHOT_PATH_USED}")
    except NameError:
        print(f"    • Snapshot File Used     : Not available in this scope")

    if movement.get("mkt_movement") != "better":
        print(f"    ⛔ BLOCKED                : Market movement not better ({movement.get('mkt_movement')})")
        return None
    else:
        print(f"    ✅ PASSED                 : Market movement: {movement.get('mkt_movement')}")

    # Clean up non-persistent keys
    row.pop("consensus_books", None)

    fieldnames = [
        "game_id",
        "market",
        "market_class",
        "side",
        "lookup_side",
        "sim_prob",
        "fair_odds",
        "market_prob",
        "market_fv",
        "consensus_prob",
        "pricing_method",
        "books_used",
        "model_edge",
        "market_odds",
        "ev_percent",
        "blended_prob",
        "blended_fv",
        "hours_to_game",
        "blend_weight_model",
        "stake",
        "entry_type",
        "segment",
        "segment_label",
        "sportsbook",
        "best_book",
        "date_simulated",
        "result",
    ]

    is_new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()

        # ✅ Convert sportsbook dict → string before writing
        if isinstance(row.get("sportsbook"), dict):
            row["sportsbook"] = ", ".join(
                f"{book}:{odds:+}" for book, odds in row["sportsbook"].items()
            )

        row_to_write = {k: v for k, v in row.items() if k in fieldnames}
        writer.writerow(row_to_write)
        print(
            f"✅ Logged to CSV → {row['game_id']} | {row['market']} | {row['side']}"
        )

        # Update market confirmation tracker on successful log
        # MARKET_CONF_TRACKER[tracker_key] = {
        #     "consensus_prob": new_conf_val,
        #     "timestamp": datetime.now().isoformat(),
        # }
        # save_market_conf_tracker(MARKET_CONF_TRACKER)

        movement = track_and_update_market_movement(
            row,
            MARKET_EVAL_TRACKER,
            MARKET_EVAL_TRACKER_BEFORE_UPDATE,
        )
        if should_log_movement():
            print(
                f"🧠 Movement for {tracker_key}: EV {movement['ev_movement']} | FV {movement['fv_movement']}"
            )

    existing[key] = full_stake
    if existing_theme_stakes is not None:
        exposure_key = get_exposure_key(row)
        existing_theme_stakes[exposure_key] = (
            existing_theme_stakes.get(exposure_key, 0.0) + row["stake"]
        )
        save_persisted_theme_stakes(existing_theme_stakes)

    edge = round(row["blended_prob"] - implied_prob(row["market_odds"]), 4)

    print(
        f"\n📦 Logging Bet: {row['game_id']} | {row['market']} ({row.get('market_class', '?')}) | {row['side']}"
    )

    print(f"   • Entry Type : {row['entry_type']}")
    stake_desc = (
        "full" if row["entry_type"] == "first" else f"delta of {row['stake']:.2f}u"
    )
    print(f"   • Stake      : {row['stake']:.2f}u ({stake_desc})")
    print(f"   • Odds       : {row['market_odds']} | Book: {row['sportsbook']}")
    print(
        f"   • EV         : {row['ev_percent']:+.2f}% | Blended: {row['blended_prob']:.4f} | Edge: {edge:+.4f}\n"
    )

    return row


def ensure_consensus_books(row):
    if "consensus_books" not in row or not row["consensus_books"]:
        if isinstance(row.get("_raw_sportsbook"), dict) and row["_raw_sportsbook"]:
            row["consensus_books"] = row["_raw_sportsbook"]
        elif isinstance(row.get("sportsbook"), str) and isinstance(
            row.get("market_odds"), (int, float)
        ):
            row["consensus_books"] = {row["sportsbook"]: row["market_odds"]}


def log_bets(
    game_id,
    sim_results,
    market_odds,
    odds_start_times=None,
    min_ev=0.05,
    log_path="logs/market_evals.csv",
    dry_run=False,
    cache_func=None,
    session_exposure=None,
    skipped_bets=None,
    existing=None,
):

    from datetime import datetime
    from core.market_pricer import decimal_odds, implied_prob, kelly_fraction
    from utils import convert_full_team_spread_to_odds_key

    game_id = canonical_game_id(game_id)

    date_sim = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    candidates = []

    markets = sim_results.get("markets", [])
    if not markets:
        print(f"⚠️ No 'markets' array found in {game_id}")
        return

    start_dt = odds_start_times.get(game_id)
    hours_to_game = 8.0
    if start_dt:
        now = datetime.now(start_dt.tzinfo)
        hours_to_game = (start_dt - now).total_seconds() / 3600

    if hours_to_game < 0:
        print(
            f"⏱️ Skipping {game_id} — game has already started ({hours_to_game:.2f}h ago)"
        )
        return

    for entry in markets:
        market_key = entry.get("market")
        side = entry.get("side")
        fair_odds = entry["fair_odds"]

        if not market_key or not side or fair_odds is None:
            continue

        sim_segment = classify_market_segment(market_key)

        if market_key == "h2h" and any(
            x in side for x in ["+1.5", "-1.5", "+0.5", "-0.5"]
        ):
            print(f"⚠️ Correcting mislabeled spread → {side} marked as h2h")
            market_key = "spreads"

        side_clean = standardize_derivative_label(side)

        if market_key in {"spreads", "h2h"}:
            raw_lookup = convert_full_team_spread_to_odds_key(side_clean)
            lookup_side = normalize_label(raw_lookup)  # ✅ Use canonical label

        elif market_key == "totals":
            lookup_side = normalize_to_abbreviation(side_clean)
        else:
            lookup_side = normalize_to_abbreviation(
                get_normalized_lookup_side(side_clean, market_key)
            )

        market_entry, best_book, matched_key, segment, price_source = (
            get_market_entry_with_alternate_fallback(
                market_odds, market_key, lookup_side, debug=True
            )
        )
        if not assert_segment_match(market_key, matched_key):
            log_segment_mismatch(market_key, matched_key)
            continue

        if not isinstance(market_entry, dict):
            print(f"        ❌ No match for {side} in market: {market_key}")
            continue

        # Safely get the correct sim line (now that matched_key is known)
        sim_entry = find_sim_entry(
            sim_results["markets"], matched_key, side, allow_fallback=False
        )
        if not sim_entry:
            print(f"❌ No valid sim entry for: {side} @ {matched_key} — skipping")
            continue

        sim_prob = sim_entry["sim_prob"]
        fair_odds = sim_entry["fair_odds"]

        market_price = market_entry.get("price")
        market_fv = market_entry.get("consensus_odds")
        consensus_prob = market_entry.get("consensus_prob")
        pricing_method = market_entry.get("pricing_method")
        books_used = market_entry.get("books_used")
        if market_price is None:
            continue

        raw_books = get_contributing_books(
            market_odds, market_key=matched_key, lookup_side=lookup_side
        )
        book_prices = clean_book_prices(raw_books)

        if not book_prices:
            fallback_source = str(best_book or "fallback")
            book_prices = {fallback_source: market_price}

        p_market = consensus_prob if consensus_prob else implied_prob(market_price)
        p_blended, w_model, p_model, _ = blend_prob(
            sim_prob, market_price, market_key, hours_to_game, p_market
        )

        ev_calc = calculate_ev_from_prob(p_blended, market_price)
        stake = kelly_fraction(p_blended, market_price, fraction=0.25)

        print(
            f"📝 Logging → game: {game_id} | market: {matched_key} | side: '{side_clean}' | normalized: '{lookup_side}' | source: {price_source} | segment: {segment}"
        )

        row = {
            "game_id": game_id,
            "market": matched_key.replace("alternate_", ""),
            "market_class": price_source,
            "side": side,
            "lookup_side": lookup_side,
            "sim_prob": round(sim_prob, 4),
            "fair_odds": round(fair_odds, 2),
            "market_prob": round(p_market, 4),
            "market_fv": market_fv,
            "consensus_prob": consensus_prob,
            "pricing_method": pricing_method,
            "books_used": (
                ", ".join(books_used) if isinstance(books_used, list) else books_used
            ),
            "model_edge": round(sim_prob - p_market, 4),
            "market_odds": market_price,
            "ev_percent": round(ev_calc, 2),
            "blended_prob": round(p_blended, 4),
            "blended_fv": to_american_odds(p_blended),
            "hours_to_game": round(hours_to_game, 2),
            "blend_weight_model": round(w_model, 2),
            "stake": stake,
            "entry_type": "",
            "segment": segment,
            "segment_label": get_segment_label(matched_key, side_clean),
            "price_source": price_source,
            "sportsbook": book_prices,
            "best_book": (
                extract_best_book(book_prices)
                if isinstance(book_prices, dict)
                else best_book
            ),
            "date_simulated": date_sim,
            "result": "",
        }

        if isinstance(book_prices, dict):
            row["_raw_sportsbook"] = book_prices.copy()

        # 📝 Track every evaluated bet before applying stake/EV filters
        tracker_key = f"{row['game_id']}:{row['market']}:{row['side']}"
        prior = MARKET_EVAL_TRACKER.get(tracker_key)

        movement = detect_market_movement(row, prior)
        if should_log_movement():
            print(
                f"🧠 Movement for {tracker_key}: EV {movement['ev_movement']} | FV {movement['fv_movement']}"
            )
            if movement.get("is_new"):
                print(f"🟡 First-time seen → {tracker_key}")
            else:
                try:
                    print(
                        f"🧠 Prior FV: {prior.get('blended_fv')} → New FV: {row.get('blended_fv')}"
                    )
                except Exception:
                    pass

            print(
                f"📦 Matched: {matched_key} | Price Source: {price_source} | Segment: {segment}"
            )
            print(f"📊 Odds: {market_price} | Stake: {stake:.2f}u | EV: {ev_calc:.2f}%")

        # Continue with staking filters, logging, top-up checks...

        row["full_stake"] = stake

        if ev_calc < min_ev * 100:
            print(f"        🟡 Skipped — low EV ({ev_calc:.2f}%)\n")
            if ev_calc >= 5.0 and skipped_bets is not None:
                row["skip_reason"] = "low_ev"
                skipped_bets.append(row)
            continue

        if stake < 1.00:
            print(f"        🟡 Skipped — low stake ({stake:.2f}u)\n")
            if dry_run:
                candidates.append(row)
            row["skip_reason"] = "low_stake"
            if ev_calc >= 5.0 and skipped_bets is not None:
                skipped_bets.append(row)
            continue

        key = (game_id, matched_key, side)
        prev = existing.get(key, 0)
        full_stake = stake
        delta = round(full_stake - prev, 2)
        if delta <= 0:
            print(f"⛔ Skipped — no stake delta for {key}")
            continue

        row["stake"] = delta
        row["full_stake"] = full_stake
        row["entry_type"] = "top-up" if prev > 0 else "first"
        row["result"] = ""
        row.pop("consensus_books", None)

        ensure_consensus_books(row)

        if dry_run:
            candidates.append(row)

        if cache_func:
            cache_func(row, segment=segment)


def log_derivative_bets(
    game_id,
    derivative_segments,
    market_odds=None,
    odds_start_times=None,
    min_ev=0.05,
    log_path="logs/market_evals.csv",
    dry_run=False,
    cache_func=None,
    session_exposure=None,
    skipped_bets=None,
    existing=None,
):
    from datetime import datetime
    from core.market_pricer import decimal_odds, implied_prob, kelly_fraction
    from utils import convert_full_team_spread_to_odds_key

    date_sim = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    candidates = []

    start_dt = odds_start_times.get(game_id)
    hours_to_game = 8.0
    if start_dt:
        now = datetime.now(start_dt.tzinfo)
        hours_to_game = (start_dt - now).total_seconds() / 3600

    if hours_to_game < 0:
        print(
            f"⏱️ Skipping {game_id} — game has already started ({hours_to_game:.2f}h ago)"
        )
        return

    for segment, seg_data in derivative_segments.items():
        if not isinstance(seg_data, dict):
            continue

        markets = seg_data.get("markets", {})
        for market_type, options in markets.items():
            for label, sim in options.items():

                if prob is None or fair_odds is None:
                    continue

                market_key = {
                    "moneyline": "h2h",
                    "runline": "spreads",
                    "total": "totals",
                }.get(market_type.lower())

                if not market_key:
                    continue

                segment_clean = normalize_segment_name(segment)

                entry = find_sim_entry(
                    sim_data.get("markets", []),
                    f"{market_key}_{segment_clean}",
                    label,
                    allow_fallback=False,
                )
                if not entry:
                    print(
                        f"❌ No valid sim entry for {label} @ {market_key}_{segment_clean} — skipping derivative bet"
                    )
                    continue

                prob = entry["sim_prob"]
                fair_odds = entry["fair_odds"]

                side_clean = standardize_derivative_label(label)

                if market_key in {"spreads", "h2h"}:
                    lookup_side = normalize_to_abbreviation(
                        convert_full_team_spread_to_odds_key(side_clean)
                    )
                elif market_key == "totals":
                    lookup_side = normalize_to_abbreviation(side_clean)
                else:
                    lookup_side = normalize_to_abbreviation(
                        get_normalized_lookup_side(side_clean, market_key)
                    )

                # Try both "alternate_" and regular market key fallback
                market_entry = None
                source = "unknown"
                prefixes = ["", "alternate_"] if market_key != "h2h" else [""]
                market_full = f"{market_key}_{segment_clean}"  # Default fallback

                for prefix in prefixes:
                    full_key = f"{prefix}{market_key}"
                    print(
                        f"🔍 Attempting lookup: {full_key} | {side_clean} → {lookup_side}"
                    )

                    # 🔍 Match using updated fallback (primary + alternate + normalized side)
                    market_entry, best_book, matched_key, segment, price_source = (
                        get_market_entry_with_alternate_fallback(
                            market_odds, market_key, lookup_side, debug=True
                        )
                    )

                    # Enforce segment match between sim market and odds market
                    from utils import classify_market_segment

                    sim_segment = classify_market_segment(
                        f"{market_key}_{segment_clean}"
                    )
                    book_segment = classify_market_segment(matched_key)

                    if sim_segment != book_segment:
                        log_segment_mismatch(sim_segment, book_segment)
                        continue

                    if not isinstance(market_entry, dict):
                        print(
                            f"        🟡 No match for {label} in {market_key}_{segment_clean}"
                        )
                        continue

                    market_full = matched_key  # set final market key (e.g., totals, alternate_totals, etc.)
                    print(
                        f"📦 Matched via {market_full} | Segment: {segment} | Price Source: {price_source}"
                    )

                if not isinstance(market_entry, dict):
                    print(f"                🟡 No odds for {label} in {market_full}")
                    continue

                market_price = market_entry.get("price")
                market_fv = market_entry.get("consensus_odds")
                consensus_prob = market_entry.get("consensus_prob")

                if market_price is None:
                    continue

                raw_books = get_contributing_books(
                    market_odds, market_key=market_full, lookup_side=lookup_side
                )
                book_prices = clean_book_prices(raw_books)

                if raw_books and not book_prices:
                    print(
                        f"⚠️ Raw books existed but cleaned empty — {game_id} | {lookup_side}: {raw_books}"
                    )
                else:
                    print(
                        f"📦 {game_id} | {market_full} | {lookup_side} → book_prices: {book_prices}"
                    )

                if not book_prices:
                    fallback_source = str(
                        market_entry.get("source") or source or "unknown"
                    )
                    book_prices = {fallback_source: market_price}
                    print(
                        f"⚠️ Consensus missing — using fallback source: {fallback_source} @ {market_price}"
                    )

                # 💡 Blending market and model probabilities
                if consensus_prob is not None and consensus_prob > 0:
                    p_market = consensus_prob
                else:
                    p_market = implied_prob(market_price)

                p_blended, w_model, p_model, _ = blend_prob(
                    p_model=prob,
                    market_odds=market_price,
                    market_type=market_key,
                    hours_to_game=hours_to_game,
                    p_market=p_market,
                )

                print(
                    f"🧪 Blending: Model {p_model:.4f} | Market {p_market:.4f} | Blended {p_blended:.4f} | Weight Model: {w_model:.2f}"
                )

                dec_odds = decimal_odds(market_price)
                blended_fair_odds = 1 / p_blended
                ev_calc = calculate_ev_from_prob(p_blended, market_price)  # ✅ correct
                stake = kelly_fraction(p_blended, market_price, fraction=0.25)

                print(
                    f"        🕒 Game in {hours_to_game:.2f}h → model weight: {w_model:.2f}"
                )
                print(f"        🔎 {game_id} | {market_full} | {side_clean}")
                print(
                    f"        → EV: {ev_calc:.2f}% | Stake: {stake:.2f}u | Model: {p_model:.1%} | Market: {p_market:.1%} | Odds: {market_price}"
                )

                key = (game_id, market_full, side_clean)
                prev = existing.get(key, 0)

                sportsbook_source = source if isinstance(source, str) else "fallback"

                print(
                    f"📝 Logging → game: {game_id} | market: {matched_key} | side: '{side_clean}' | normalized: '{lookup_side}' | source: {price_source} | segment: {segment}"
                )

                row = {
                    "game_id": game_id,
                    "market": market_full.replace("alternate_", ""),
                    "market_class": price_source,
                    "side": side_clean,
                    "lookup_side": lookup_side,
                    "sim_prob": round(prob, 4),
                    "fair_odds": round(fair_odds, 2),
                    "market_prob": round(
                        (
                            consensus_prob
                            if consensus_prob is not None
                            else implied_prob(market_price)
                        ),
                        4,
                    ),
                    "market_fv": market_fv,
                    "consensus_prob": consensus_prob,
                    "pricing_method": pricing_method,
                    "books_used": (
                        ", ".join(books_used)
                        if isinstance(books_used, list)
                        else books_used
                    ),
                    "model_edge": round(prob - (consensus_prob or 0), 4),
                    "market_odds": market_price,
                    "ev_percent": round(ev_calc, 2),
                    "blended_prob": round(p_blended, 4),
                    "blended_fv": to_american_odds(p_blended),
                    "hours_to_game": round(hours_to_game, 2),
                    "blend_weight_model": round(w_model, 2),
                    "stake": stake,  # Will be updated to delta after comparing `prev`
                    "entry_type": "",  # Set below based on `prev`
                    "segment": segment,
                    "segment_label": get_segment_label(market_full, side_clean),
                    "sportsbook": (
                        book_prices
                        if book_prices
                        else {sportsbook_source: market_price}
                    ),
                    "best_book": (
                        extract_best_book(book_prices)
                        if isinstance(book_prices, dict)
                        else sportsbook_source
                    ),
                    "date_simulated": date_sim,
                    "result": "",
                }

                if isinstance(book_prices, dict):
                    row["_raw_sportsbook"] = book_prices.copy()

                print(f"📦 Books stored in row: {book_prices}")
                print(f"🏦 Best Book Selected: {row['best_book']}")
                # 📝 Track every evaluated bet before applying stake/EV filters
                tracker_key = f"{row['game_id']}:{row['market']}:{row['side']}"
                prior = MARKET_EVAL_TRACKER.get(tracker_key)
                movement = detect_market_movement(
                    row,
                    MARKET_EVAL_TRACKER.get(tracker_key),
                )
                if should_log_movement():
                    print(
                        f"🧠 Movement for {tracker_key}: EV {movement['ev_movement']} | FV {movement['fv_movement']}"
                    )
                    if movement.get("is_new"):
                        print(f"🟡 First-time seen → {tracker_key}")
                    else:
                        try:
                            print(
                                f"🧠 Prior FV: {prior.get('blended_fv')} → New FV: {row.get('blended_fv')}"
                            )
                        except Exception:
                            pass
                # Tracker update moved below evaluation to preserve prior state
                row["full_stake"] = stake
                row["price_source"] = price_source
                row["segment"] = segment

                # ✅ Show EV/stake even if we skip
                print(f"        🔎 {game_id} | {market_full} | {side_clean}")
                print(
                    f"        → EV: {ev_calc:.2f}% | Stake: {stake:.2f}u | Model: {p_model:.1%} | Market: {p_market:.1%} | Odds: {market_price}"
                )

                if ev_calc < min_ev * 100:
                    print(f"        🟡 Skipped — low EV ({ev_calc:.2f}%)\n")
                    if ev_calc >= 5.0 and skipped_bets is not None:
                        row["skip_reason"] = "low_ev"
                        skipped_bets.append(row)
                    continue

                if stake < 1.00:
                    print(f"        🟡 Skipped — low stake ({stake:.2f}u)\n")
                    row["skip_reason"] = "low_stake"
                    ensure_consensus_books(row)
                    if dry_run:
                        candidates.append(row)
                    if ev_calc >= 5.0 and skipped_bets is not None:
                        skipped_bets.append(row)
                    continue

                full_stake = stake
                delta = round(full_stake - prev, 2)
                row["stake"] = delta
                row["full_stake"] = full_stake
                row["entry_type"] = "top-up" if prev > 0 else "first"
                row["result"] = ""
                row.pop("consensus_books", None)

                ensure_consensus_books(row)

                if dry_run:
                    candidates.append(row)

                if cache_func:
                    cache_func(row, segment=segment)


def send_summary_to_discord(skipped_bets, webhook_url):
    if not webhook_url:
        return

    now = datetime.now().strftime("%I:%M %p")

    if not skipped_bets:
        payload = {
            "content": f"✅ No high-EV model bets were skipped due to stake rules — {now}."
        }
    else:
        fields = []

        for b in skipped_bets:
            consensus_books = b.get("consensus_books") or b.get("_raw_sportsbook") or {}
            books_str = "N/A"

            if not consensus_books:
                print(
                    f"⚠️ No consensus_books for: {b['game_id']} | {b['market']} | {b['side']}"
                )

            if isinstance(consensus_books, dict) and consensus_books:
                sorted_books = sorted(
                    consensus_books.items(),
                    key=lambda x: decimal_odds(x[1]),
                    reverse=True,
                )

                books_lines = []
                for idx, (book, price) in enumerate(sorted_books[:3]):
                    emoji = "🏆" if idx == 0 else "•"
                    books_lines.append(f"{emoji} {book}: {price:+}")
                if len(sorted_books) > 3:
                    books_lines.append(f"(+{len(sorted_books) - 3} more)")
                books_str = "\n".join(books_lines)

            elif isinstance(b.get("sportsbook"), str):
                odds_value = b.get("market_odds")
                if isinstance(odds_value, (int, float)):
                    books_str = f"🏦 {b['sportsbook']}: {odds_value:+}"

            skip_reason = b.get("skip_reason", "N/A").replace("_", " ").capitalize()

            field = {
                "name": f"📅 {b['game_id']} | {b['market']} | {b['side']}",
                "value": (
                    f"💸 Fair Odds: `{b['blended_fv']}`\n"
                    f"💰 Stake: `{b.get('full_stake', b['stake']):.2f}u` @ `{b['market_odds']}`\n"
                    f"📈 EV: `{b['ev_percent']:+.2f}%`\n"
                    f"🚫 Reason: `{skip_reason}`\n"
                    f"🏦 Books:\n{books_str}"
                ),
                "inline": False,
            }
            fields.append(field)

        embed = {
            "title": f"📊 Skipped but Model-Favored Bets — {now}",
            "color": 3447003,
            "fields": fields[:20],
            "footer": {
                "text": "These bets were skipped due to stake rules, but met the EV and model criteria."
            },
        }

        payload = {"embeds": [embed]}

    try:
        requests.post(webhook_url, json=payload, timeout=5)
        print(f"✅ Summary sent to Discord ({len(skipped_bets)} bets)")
    except Exception as e:
        print(f"❌ Failed to send summary to Discord: {e}")


def run_batch_logging(
    eval_folder,
    market_odds,
    min_ev,
    dry_run=False,
    debug=False,
    image=False,
    output_dir="logs",
):
    from collections import defaultdict
    import os, json
    from dotenv import load_dotenv

    load_dotenv()

    DISCORD_SUMMARY_WEBHOOK_URL = os.getenv("DISCORD_SUMMARY_WEBHOOK_URL")
    summary_candidates = []

    if isinstance(market_odds, str):
        all_market_odds = safe_load_json(market_odds)
        if all_market_odds is None:
            print(f"❌ Failed to load odds file {market_odds}")
            return
    else:
        all_market_odds = market_odds


    def extract_start_times(odds_data):
        from dateutil import parser

        start_times = {}
        for game_id, game in odds_data.items():
            if not isinstance(game, dict):
                continue
            if "start_time" in game:
                try:
                    start_times[game_id] = parser.parse(game["start_time"])
                except:
                    pass
        return start_times

    existing = load_existing_stakes("logs/market_evals.csv")
    market_evals_path = "logs/market_evals.csv"
    if os.path.exists(market_evals_path):
        market_evals_df = pd.read_csv(market_evals_path)
        market_evals_df.columns = market_evals_df.columns.str.strip()
        print(
            f"📋 Loaded market_evals.csv with columns: {market_evals_df.columns.tolist()}"
        )

        # ✅ Ensure 'segment' column exists (required for correct should_log_bet evaluation)
        if "segment" not in market_evals_df.columns:
            print("🔧 Adding missing 'segment' column to market_evals_df...")
            market_evals_df["segment"] = "mainline"
    else:
        market_evals_df = pd.DataFrame()

    MARKET_EVAL_TRACKER.clear()
    MARKET_EVAL_TRACKER.update(load_tracker())

    # ✅ Ensure all required columns exist for downstream filters like should_log_bet
    required_cols = [
        "game_id",
        "market",
        "side",
        "lookup_side",
        "sim_prob",
        "fair_odds",
        "market_prob",
        "market_fv",
        "model_edge",
        "market_odds",
        "ev_percent",
        "blended_prob",
        "blended_fv",
        "hours_to_game",
        "blend_weight_model",
        "stake",
        "entry_type",
        "segment",
        "sportsbook",
        "best_book",
        "date_simulated",
        "result",
    ]

    for col in required_cols:
        if col not in market_evals_df.columns:
            market_evals_df[col] = None

    session_exposure = defaultdict(set)
    global theme_logged
    theme_logged = defaultdict(lambda: defaultdict(dict))

    def cache_theme_bet(row, segment):
        theme = get_theme(row)
        game_id = row["game_id"]
        market = row["market"]

        if "spreads" in market or "h2h" in market or "runline" in market:
            theme_key = f"{theme}_spread"
        elif "totals" in market:
            theme_key = f"{theme}_total"
        else:
            theme_key = f"{theme}_other"

        bets = theme_logged[game_id][theme_key]
        current_best = bets.get(segment)

        if not current_best or row["ev_percent"] >= current_best["ev_percent"]:
            bets[segment] = row.copy()
        else:
            print(
                f"🧹 Skipped in cache — {market} | {row['side']} | "
                f"EV {row['ev_percent']} not better than current {current_best['ev_percent']}"
            )

    existing_theme_stakes = load_existing_theme_stakes("logs/market_evals.csv")
    persisted = load_persisted_theme_stakes()
    if persisted:
        existing_theme_stakes.update(persisted)

    for (gid, market, side), stake in existing.items():
        if stake >= 1.00:
            theme = get_theme({"side": side, "market": market})
            if (
                market.startswith("spreads")
                or market.startswith("h2h")
                or market.startswith("runline")
            ):
                theme_key = f"{theme}_spread"
            elif market.startswith("totals"):
                theme_key = f"{theme}_total"
            else:
                theme_key = f"{theme}_other"

            segment = (
                "derivative" if "1st" in market or "7_innings" in market else "mainline"
            )
            existing_theme_stakes[(gid, theme_key, segment)] += stake

    odds_start_times = extract_start_times(all_market_odds)

    for fname in os.listdir(eval_folder):
        if not fname.endswith(".json"):
            continue

        # 🔄 Reload exposure from file before evaluating each game
        existing_theme_stakes = load_existing_theme_stakes("logs/market_evals.csv")
        persisted = load_persisted_theme_stakes()
        if persisted:
            existing_theme_stakes.update(persisted)

        raw_game_id = fname.replace(".json", "")
        game_id = canonical_game_id(raw_game_id)
        sim_path = os.path.join(eval_folder, fname)

        if not os.path.exists(sim_path):
            continue

        sim = safe_load_json(sim_path)
        if sim is None:
            print(f"❌ Failed to load simulation file {sim_path}")
            continue

        mkt = all_market_odds.get(game_id)
        if not mkt:
            print(
                f"❌ No market odds for {raw_game_id} (normalized: {game_id}), skipping."
            )
            continue

        log_bets(
            game_id=game_id,
            sim_results=sim,
            market_odds=mkt,
            odds_start_times=odds_start_times,
            min_ev=min_ev,
            dry_run=dry_run,
            cache_func=cache_theme_bet,
            session_exposure=session_exposure,
            skipped_bets=summary_candidates,
            existing=existing,
        )

    process_theme_logged_bets(
        theme_logged=theme_logged,
        existing_theme_stakes=existing_theme_stakes,
        existing=existing,
        session_exposure=session_exposure,
        dry_run=dry_run,
        skipped_bets=summary_candidates,
        webhook_url=DISCORD_SUMMARY_WEBHOOK_URL,
        market_evals_df=market_evals_df,
        snapshot_ev=args.min_ev,
        image=image,
        output_dir=output_dir,
    )

    if summary_candidates:
        os.makedirs("logs", exist_ok=True)
        with open("logs/skipped_bets.json", "w") as f:
            json.dump(summary_candidates, f, indent=2)
        print(
            f"📁 Saved {len(summary_candidates)} summary candidates to logs/skipped_bets.json"
        )


def process_theme_logged_bets(
    theme_logged,
    existing_theme_stakes,
    existing,
    session_exposure,
    dry_run,
    skipped_bets,
    webhook_url="",
    market_evals_df=None,
    snapshot_ev=5.0,
    image=False,
    output_dir="logs",
):
    print("\n🧾 Final Trimmed Bets to Log:")

    skipped_counts = {
        "duplicate": 0,
        "low_initial": 0,
        "low_topup": 0,
        "already_logged": 0,
    }

    MAX_ALLOWED_EV = 20.0
    stake_mode = "model"  # or "actual" if you're filtering only logged bets

    seen_keys = set()
    seen_lines = set()
    game_summary = defaultdict(list)
    # Track the best bet per (game_id, market, segment)
    best_market_segment = {}

    def safe_remove_segment(game_id, theme_key, segment=None):
        if segment:
            if theme_logged[game_id].get(theme_key, {}).get(segment):
                del theme_logged[game_id][theme_key][segment]
                print(f"⚠️  Removed segment '{segment}' from {theme_key}")
        else:
            segments = list(theme_logged[game_id].get(theme_key, {}).keys())
            for seg in segments:
                del theme_logged[game_id][theme_key][seg]
                print(f"⚠️  Removed segment '{seg}' from {theme_key}")

    for game_id in theme_logged:
        print(f"\n🔍 Game: {game_id}")
        # 🔄 Refresh theme exposure from the latest CSV before evaluating bets
        existing_theme_stakes = load_existing_theme_stakes("logs/market_evals.csv")
        persisted = load_persisted_theme_stakes()
        if persisted:
            existing_theme_stakes.update(persisted)

        print("\n📊 Theme Map:")
        for theme_key, segment_map in theme_logged[game_id].items():
            for segment, row in segment_map.items():
                stake = round(float(row.get("full_stake", row.get("stake", 0))), 2)
                ev = row.get("ev_percent", 0)
                print(
                    f"   - {theme_key} [{segment}] → {row['side']} ({row['market']}) @ {stake:.2f}u | EV: {ev:.2f}%"
                )

        # 🔁 Over vs Under pruning (mainline segment only)
        for theme_key in list(theme_logged[game_id].keys()):
            if theme_key.startswith("Over") or theme_key.startswith("Under"):
                other_theme = (
                    theme_key.replace("Over", "Under")
                    if theme_key.startswith("Over")
                    else theme_key.replace("Under", "Over")
                )
                this_ev = (
                    theme_logged[game_id][theme_key]
                    .get("mainline", {})
                    .get("ev_percent", 0)
                )
                that_ev = (
                    theme_logged[game_id]
                    .get(other_theme, {})
                    .get("mainline", {})
                    .get("ev_percent", 0)
                )

                this_has_mainline = "mainline" in theme_logged[game_id][theme_key]
                that_has_mainline = "mainline" in theme_logged[game_id].get(
                    other_theme, {}
                )

                if this_has_mainline and that_has_mainline:
                    if this_ev > MAX_ALLOWED_EV and that_ev > MAX_ALLOWED_EV:
                        print(
                            f"⚖️ Discarding both Over and Under due to excessive EVs ({this_ev:.2f}%, {that_ev:.2f}%)"
                        )
                        safe_remove_segment(game_id, theme_key, "mainline")
                        safe_remove_segment(game_id, other_theme, "mainline")
                    elif this_ev > MAX_ALLOWED_EV:
                        print(
                            f"⚖️ Discarding {theme_key} (EV {this_ev:.2f}%) — exceeds cap {MAX_ALLOWED_EV:.2f}%"
                        )
                        safe_remove_segment(game_id, theme_key, "mainline")
                    elif that_ev > MAX_ALLOWED_EV:
                        print(
                            f"⚖️ Discarding {other_theme} (EV {that_ev:.2f}%) — exceeds cap {MAX_ALLOWED_EV:.2f}%"
                        )
                        safe_remove_segment(game_id, other_theme, "mainline")
                    elif this_ev >= that_ev:
                        print(
                            f"⚖️ Keeping {theme_key} (EV {this_ev:.2f}%) over {other_theme} (EV {that_ev:.2f}%)"
                        )
                        safe_remove_segment(game_id, other_theme, "mainline")
                    else:
                        print(
                            f"⚖️ Keeping {other_theme} (EV {that_ev:.2f}%) over {theme_key} (EV {this_ev:.2f}%)"
                        )
                        safe_remove_segment(game_id, theme_key, "mainline")

        for theme_key, segment_map in theme_logged[game_id].items():
            for segment, row in segment_map.items():
                if row.get("ev_percent", 0) > MAX_ALLOWED_EV:
                    print(
                        f"                ⛔ Skipped      : EV exceeds cap ({row['ev_percent']:.2f}% > {MAX_ALLOWED_EV:.2f}%) — {row['side']} in {row['market']}"
                    )
                    continue

                proposed_stake = round(float(row.get("full_stake", 0)), 2)
                key = (row["game_id"], row["market"], row["side"])
                line_key = (row["market"], row["side"])
                exposure_key = get_exposure_key(row)
                theme_total = existing_theme_stakes.get(exposure_key, 0.0)
                delta = round(proposed_stake - theme_total, 2)
                is_initial_bet = theme_total == 0.0

                print(
                    f"🔁 Evaluating: {row['side']} | {row['market']} (EV: {row['ev_percent']}%)"
                )
                print(f"                ➤ Segment     : {segment}")
                print(f"                ➤ Theme       : {theme_key}")
                print(
                    f"                ➤ Proposed    : {proposed_stake:.2f}u | EV: {row['ev_percent']:.2f}%"
                )

                existing_stake = existing.get(key, 0.0)
                if existing_stake > 0:
                    print(
                        f"                🧾 Existing     : {existing_stake:.2f}u already logged in market_evals.csv"
                    )

                if key in seen_keys or line_key in seen_lines:
                    print(
                        f"                ⚠️ Skipped      : Duplicate line already logged this run."
                    )
                    skipped_counts["duplicate"] += 1
                    continue

                if theme_total >= proposed_stake:
                    print(
                        f"                ⛔ Skipped      : Already logged {theme_total:.2f}u ≥ proposed {proposed_stake:.2f}u"
                    )
                    skipped_counts["already_logged"] += 1
                    if should_include_in_summary(row):
                        row["skip_reason"] = "already_logged"
                        ensure_consensus_books(row)
                        skipped_bets.append(row)
                    continue

                if is_initial_bet and proposed_stake < 1.00:
                    print(
                        f"                ⛔ Skipped      : Initial stake too low ({proposed_stake:.2f}u < 1.00u)"
                    )
                    skipped_counts["low_initial"] += 1
                    if should_include_in_summary(row):
                        row["skip_reason"] = "low_initial"
                        ensure_consensus_books(row)
                        skipped_bets.append(row)
                    continue

                if not is_initial_bet and delta < 0.50:
                    print(
                        f"                ⛔ Skipped      : Top-up delta too small ({delta:.2f}u < 0.50u)"
                    )
                    skipped_counts["low_topup"] += 1
                    if should_include_in_summary(row):
                        row["skip_reason"] = "low_topup"
                        ensure_consensus_books(row)
                        skipped_bets.append(row)
                    continue

                print(
                    f"                ✅ Logged       : {'First bet' if is_initial_bet else 'Top-up'} | Delta: {delta:.2f}u → Total: {proposed_stake:.2f}u"
                )

                seen_keys.add(key)
                seen_lines.add(line_key)
                row["entry_type"] = "top-up" if not is_initial_bet else "first"
                row["stake"] = delta
                row["segment"] = segment

                row_copy = row.copy()
                # 🛡️ Protect against derivative market flattening
                if row.get("segment") == "derivative" and "_" not in row.get(
                    "market", ""
                ):
                    print(
                        f"❌ [BUG] Derivative market improperly named: {row['market']} — should be something like totals_1st_5_innings"
                    )

                evaluated = should_log_bet(
                    row_copy,
                    existing_theme_stakes,
                    eval_tracker=MARKET_EVAL_TRACKER,
                    reference_tracker=MARKET_EVAL_TRACKER_BEFORE_UPDATE,
                )

                # 📝 Update tracker for every evaluated bet
                t_key = f"{row_copy['game_id']}:{row_copy['market']}:{row_copy['side']}"
                prior = MARKET_EVAL_TRACKER.get(t_key)
                movement = detect_market_movement(
                    row_copy,
                    MARKET_EVAL_TRACKER.get(t_key),
                )
                if should_log_movement():
                    print(
                        f"🧠 Movement for {t_key}: EV {movement['ev_movement']} | FV {movement['fv_movement']}"
                    )
                    if movement.get("is_new"):
                        print(f"🟡 First-time seen → {t_key}")
                    else:
                        try:
                            print(
                                f"🧠 Prior FV: {prior.get('blended_fv')} → New FV: {row_copy.get('blended_fv')}"
                            )
                        except Exception:
                            pass
                if evaluated:
                    evaluated["market"] = row["market"].replace("alternate_", "")
                    key_best = (
                        evaluated["game_id"],
                        evaluated["market"],
                        evaluated.get("segment"),
                    )
                    current_best = best_market_segment.get(key_best)

                    if (
                        not current_best
                        or evaluated["ev_percent"] > current_best.get("ev_percent", -999)
                    ):
                        best_market_segment[key_best] = evaluated

    # ➡️ Log only the best bet per (game_id, market, segment)
    logged_bets_this_loop = []
    for best_row in best_market_segment.values():
        print(
            f"📄 Logging: {best_row['game_id']} | {best_row['market']} | {best_row['side']} @ {best_row['stake']}u"
        )
        result = write_to_csv(
            best_row,
            "logs/market_evals.csv",
            existing,
            session_exposure,
            existing_theme_stakes,
            dry_run=dry_run,
        )
        if result:
            print(
                f"✅ CSV Log Success → {best_row['game_id']} | {best_row['market']} | {best_row['side']}"
            )
            logged_bets_this_loop.append(result)
            game_summary[best_row["game_id"]].append(best_row)
            logged_stake = best_row["stake"]
            exposure_key = get_exposure_key(best_row)
            existing_theme_stakes[exposure_key] = (
                existing_theme_stakes.get(exposure_key, 0.0) + logged_stake
            )
            save_persisted_theme_stakes(existing_theme_stakes)
            if should_include_in_summary(best_row):
                ensure_consensus_books(best_row)
                skipped_bets.append(best_row)
        else:
            print(
                f"⛔ CSV Log Failed → {best_row['game_id']} | {best_row['market']} | {best_row['side']}"
            )

    for row in logged_bets_this_loop:
        print(
            f"📤 Dispatching to Discord → {row['game_id']} | {row['market']} | {row['side']}"
        )
        send_discord_notification(row)


    # ✅ Expand snapshot per book with proper stake & EV% logic
    snapshot_raw = [r for rows in game_summary.values() for r in rows] + skipped_bets
    final_snapshot = expand_snapshot_rows_with_kelly(
        snapshot_raw, min_ev=snapshot_ev, min_stake=0.5
    )

    if VERBOSE:
        print("\n🧠 Snapshot Prob Consistency Check:")
        for row in final_snapshot:
            key = f"{row['game_id']}:{row['market']}:{row['side']}"
            prior = row.get("_prior_snapshot")
            if prior:
                print(f"🧠 {key} | Prior market_prob: {prior.get('market_prob')} | Current: {row.get('market_prob')}")
            else:
                print(f"⚠️  {key} has no _prior_snapshot attached.")




    if image:
        if final_snapshot:
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, "mlb_summary_table_model.png")
            generate_clean_summary_image(
                final_snapshot, output_path=output_path, stake_mode="model"
            )
            upload_summary_image_to_discord(output_path, webhook_url)

    save_tracker(MARKET_EVAL_TRACKER)


if __name__ == "__main__":
    p = argparse.ArgumentParser("Log value bets from sim output")
    p.add_argument(
        "--eval-folder", required=True, help="Folder containing simulation JSON files"
    )
    p.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    p.add_argument(
        "--min-ev", type=float, default=0.05, help="Minimum EV% threshold for bets"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Preview bets without writing to CSV"
    )
    p.add_argument(
        "--debug", action="store_true", help="Enable deep inspection debug mode"
    )
    p.add_argument(
        "--image",
        action="store_true",
        help="Generate summary image and post to Discord",
    )
    p.add_argument("--output-dir", default="logs", help="Directory for summary image")
    p.add_argument("--show-skipped", action="store_true", help="Show skipped bet details")
    p.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = p.parse_args()

    VERBOSE = args.verbose
    SHOW_SKIPPED = args.show_skipped

    date_tag = os.path.basename(args.eval_folder)

    # ✅ Check if eval-folder exists before proceeding
    if not os.path.exists(args.eval_folder):
        print(f"⚠️ Skipping log run — folder does not exist: {args.eval_folder}")
        sys.exit(0)

    if args.odds_path:
        odds = safe_load_json(args.odds_path)
        if odds is None:
            print(f"❌ Failed to load odds file {args.odds_path}")
            sys.exit(1)
        odds_file = args.odds_path
    else:
        games = [
            f.replace(".json", "")
            for f in os.listdir(args.eval_folder)
            if f.endswith(".json")
        ]
        print(f"📡 Fetching market odds for {len(games)} games on {date_tag}...")
        odds = fetch_market_odds_from_api(games)
        timestamp_tag = now_eastern().strftime("market_odds_%Y%m%dT%H%M")
        odds_file = save_market_odds_to_file(odds, timestamp_tag)

    run_batch_logging(
        eval_folder=args.eval_folder,
        market_odds=odds,
        min_ev=args.min_ev,
        dry_run=args.dry_run,
        debug=args.debug,  # ✅ New debug toggle wired up!
        image=args.image,
        output_dir=args.output_dir,
    )