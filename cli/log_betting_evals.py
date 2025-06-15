# === Path Setup ===
from core import config
import os
import sys
from core.bootstrap import *  # noqa

# === Core Imports ===
import json, csv, math, argparse
from datetime import datetime
from collections import defaultdict

# === External Notification / Environment ===
import requests
from dotenv import load_dotenv

from core.market_eval_tracker import load_tracker, save_tracker, build_tracker_key
from core.lock_utils import with_locked_file
from core.skip_reasons import SkipReason
from utils import safe_load_json, now_eastern, EASTERN_TZ, parse_game_id
from utils import canonical_game_id
from utils.book_helpers import ensure_consensus_books
import re

load_dotenv()
from core.logger import get_logger, set_log_level

logger = get_logger(__name__)

# === Console Output Controls ===
SEGMENT_SKIP_LIMIT = 5
segment_skip_count = 0
MOVEMENT_LOG_LIMIT = 5
movement_log_count = 0
VERBOSE = False
DEBUG = False
SHOW_PENDING = False


def log_segment_mismatch(sim_segment: str, book_segment: str, debug: bool = False) -> None:
    """Print a segment mismatch message with truncation after a limit."""
    debug = debug or config.DEBUG_MODE or config.VERBOSE_MODE
    if not debug:
        return

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
    if not DEBUG:
        return False
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

# Configurable quiet hours (Eastern Time)
quiet_hours_start = int(os.getenv("QUIET_HOURS_START", 22))  # default: 10 PM ET
quiet_hours_end = int(os.getenv("QUIET_HOURS_END", 8))       # default: 8 AM ET


def should_skip_due_to_quiet_hours(
    now=None,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> bool:
    """Return ``True`` if logging should be skipped due to quiet hours."""
    from utils import logging_allowed_now

    return not logging_allowed_now(
        now=now,
        quiet_hours_start=quiet_hours_start if start_hour is None else start_hour,
        quiet_hours_end=quiet_hours_end if end_hour is None else end_hour,
    )

# === Market Confirmation Tracker ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MARKET_CONF_TRACKER_PATH = os.path.join(
    SCRIPT_DIR, "..", "data", "trackers", "market_conf_tracker.json"
)


def load_market_conf_tracker(path: str = MARKET_CONF_TRACKER_PATH):
    """Load last seen consensus probabilities for bets."""
    data = safe_load_json(path)
    if isinstance(data, dict):
        return data
    if os.path.exists(path):
        print(f"⚠️ Could not load market confirmation tracker at {path}, starting fresh.")
    # Return empty dict if file missing or failed to load
    return {}


def save_market_conf_tracker(tracker: dict, path: str = MARKET_CONF_TRACKER_PATH):
    """Atomically save tracker data to disk with a lock."""
    lock = f"{path}.lock"
    tmp = f"{path}.tmp"
    try:
        with with_locked_file(lock):
            with open(tmp, "w") as f:
                json.dump(tracker, f, indent=2)
            os.replace(tmp, path)
    except Exception as e:
        logger.warning("❌ Failed to save market confirmation tracker: %s", e)


import copy
from datetime import datetime

# Load market confirmation tracker
MARKET_CONF_TRACKER = load_market_conf_tracker()

# Base schema for market_evals.csv. Additional columns may be appended
# later (e.g. by update_clv_column.py). When writing to an existing CSV
# we read its header to determine the active schema.
BASE_CSV_COLUMNS = [
    "Date",
    "Time",
    "Start Time (ISO)",
    "Matchup",
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
    "stake",
    "cumulative_stake",
    "entry_type",
    "segment",
    "segment_label",
    "best_book",
    "date_simulated",
    "result",
]


def latest_snapshot_path(folder="backtest"):
    """Return the most recent snapshot file from the given folder."""
    files = sorted(
        [
            f
            for f in os.listdir(folder)
            if f.startswith("market_snapshot_") and f.endswith(".json")
        ],
        reverse=True,
    )
    return os.path.join(folder, files[0]) if files else None


# Load tracker for updates during logging
MARKET_EVAL_TRACKER = load_tracker()

# Load most recent snapshot file for movement comparison
SNAPSHOT_PATH_USED = latest_snapshot_path("backtest")
STALE_SNAPSHOT = False
if SNAPSHOT_PATH_USED and os.path.exists(SNAPSHOT_PATH_USED):
    print(
        f"📂 Using prior snapshot for market movement detection: {SNAPSHOT_PATH_USED}"
    )

    # Determine snapshot timestamp from filename or modification time
    snap_dt = None
    m = re.search(
        r"market_snapshot_(\d{8}T\d{4})", os.path.basename(SNAPSHOT_PATH_USED)
    )
    if m:
        try:
            snap_dt = datetime.strptime(m.group(1), "%Y%m%dT%H%M").replace(
                tzinfo=EASTERN_TZ
            )
        except Exception:
            snap_dt = None
    if snap_dt is None:
        try:
            snap_dt = datetime.fromtimestamp(
                os.path.getmtime(SNAPSHOT_PATH_USED), tz=EASTERN_TZ
            )
        except Exception:
            snap_dt = None

    if snap_dt is not None:
        age_hours = (now_eastern() - snap_dt).total_seconds() / 3600.0
        if age_hours > 2:
            logger.warning(
                "⚠️ Snapshot is over 2 hours old – movement tracking may be stale."
            )
            STALE_SNAPSHOT = True

    prior_snapshot_data = safe_load_json(SNAPSHOT_PATH_USED) or []

    if isinstance(prior_snapshot_data, list):
        # Convert snapshot list to tracker-style dict
        prior_snapshot_tracker = {
            build_tracker_key(r["game_id"], r["market"], r["side"]): r
            for r in prior_snapshot_data
            if "game_id" in r and "market" in r and "side" in r
        }
        print(f"🔁 Loaded {len(prior_snapshot_tracker)} entries from snapshot.")
    else:
        prior_snapshot_tracker = prior_snapshot_data  # already dict

    MARKET_EVAL_TRACKER_BEFORE_UPDATE = {} if STALE_SNAPSHOT else prior_snapshot_tracker
else:
    print("⚠️ No valid prior snapshot found — using fallback copy of tracker.")
    MARKET_EVAL_TRACKER_BEFORE_UPDATE = copy.deepcopy(MARKET_EVAL_TRACKER)


# === Local Modules ===
def _game_id_display_fields(game_id: str) -> tuple[str, str, str]:
    """Return Date, Matchup and Time strings from a game_id."""
    parts = parse_game_id(str(game_id))
    date = parts.get("date", "")
    matchup = f"{parts.get('away', '')} @ {parts.get('home', '')}".strip()
    time = ""
    time_part = parts.get("time", "")
    if isinstance(time_part, str) and time_part.startswith("T"):
        raw = time_part.split("-")[0][1:]
        try:
            time = datetime.strptime(raw, "%H%M").strftime("%-I:%M %p")
        except Exception:
            try:
                time = datetime.strptime(raw, "%H%M").strftime("%I:%M %p").lstrip("0")
            except Exception:
                time = ""
    return date, matchup, time


from core.market_pricer import (
    implied_prob,
    decimal_odds,
    to_american_odds,
    kelly_fraction,
    calculate_ev_from_prob,
    extract_best_book,
)
from core.confirmation_utils import confirmation_strength
from core.snapshot_core import annotate_display_deltas
from core.scaling_utils import blend_prob
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
    normalize_label_for_odds,
    convert_full_team_spread_to_odds_key,
    assert_segment_match,
    classify_market_segment,
    find_sim_entry,
    normalize_label,
    get_segment_label,
    canonical_game_id,
    now_eastern,
)
from core.time_utils import compute_hours_to_game


# === Staking Logic Refactor ===
from core.should_log_bet import should_log_bet
from core.market_eval_tracker import load_tracker, save_tracker, build_tracker_key
from core.market_movement_tracker import (
    track_and_update_market_movement,
    detect_market_movement,
)
from core.theme_exposure_tracker import (
    load_tracker as load_theme_stakes,
    save_tracker as save_theme_stakes,
)
from core.format_utils import format_market_odds_and_roles
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

    df[["Date", "Matchup", "Time"]] = df["game_id"].apply(
        lambda gid: pd.Series(_game_id_display_fields(gid))
    )
    if df["Time"].eq("").all():
        df.drop(columns=["Time"], inplace=True)

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

    cols = ["Date"]
    if "Time" in df.columns:
        cols.append("Time")
    cols += [
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
    display_df = df[cols].rename(columns={"side": "Bet", "best_book": "Book"})

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
    df[["Date", "Matchup", "Time"]] = df["game_id"].apply(
        lambda gid: pd.Series(_game_id_display_fields(gid))
    )
    if df["Time"].eq("").all():
        df.drop(columns=["Time"], inplace=True)

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

    cols = ["Date"]
    if "Time" in df.columns:
        cols.append("Time")
    cols += [
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
    display_df = df[cols].rename(columns={"side": "Bet", "best_book": "Book"})

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


def expand_snapshot_rows_with_kelly(final_snapshot, min_ev=1.0, min_stake=0.5):
    """
    Expand snapshot rows into 1 row per sportsbook, recalculating EV% and stake using Quarter-Kelly.
    """
    from core.market_pricer import calculate_ev_from_prob

    expanded_rows = []

    for bet in final_snapshot:
        # ✅ Normalize market_prob from consensus_prob if not already present
        if "market_prob" not in bet and "consensus_prob" in bet:
            bet["market_prob"] = bet["consensus_prob"]
        base_fields = {
            "game_id": bet.get("game_id", "unknown"),
            "league": bet.get("league", "MLB"),
            "Date": bet.get("Date", ""),
            "Matchup": bet.get("Matchup", bet.get("game_id", "")[-7:]),
            "Time": bet.get("Time", ""),
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
            "ev_movement",
            "fv_movement",
            "odds_movement",
            "stake_movement",
            "sim_movement",
            "mkt_movement",
            "is_new",
        ]:
            if field in bet:
                base_fields[field] = bet[field]

        for field in [
            "ev_display",
            "fv_display",
            "odds_display",
            "stake_display",
            "sim_prob_display",
            "mkt_prob_display",
        ]:
            if field in bet:
                base_fields[field] = bet[field]

        if not isinstance(bet.get("_raw_sportsbook", None), dict):
            print(
                f"⚠️ No expansion data available — keeping existing row: {bet['side']} @ {bet['market']}"
            )
            ensure_consensus_books(bet)
            expanded_rows.append(bet)
            continue

        raw_books = bet.get("_raw_sportsbook") or bet.get("consensus_books", {})
        if not isinstance(raw_books, dict):
            continue  # skip malformed entries

        for book, odds in raw_books.items():
            try:
                p = base_fields.get("blended_prob", base_fields.get("sim_prob", 0))
                fraction = 0.125 if bet.get("market_class") == "alternate" else 0.25
                prior_snapshot_row = bet.get("_prior_snapshot")

                raw_kelly = kelly_fraction(p, odds, fraction=fraction)

                prev_prob = None
                if prior_snapshot_row:
                    prev_prob = prior_snapshot_row.get("market_prob") or prior_snapshot_row.get("consensus_prob")
                curr_prob = bet.get("market_prob") or bet.get("consensus_prob")
                try:
                    observed_move = float(curr_prob) - float(prev_prob)
                except Exception:
                    observed_move = 0.0

                hours = bet.get("hours_to_game")
                strength = confirmation_strength(observed_move, hours)
                stake = round(raw_kelly * (strength ** 1.5), 4)
                ev = calculate_ev_from_prob(p, odds)

                if base_fields["side"] == "St. Louis Cardinals":
                    print(f"🔍 {book}: EV={ev:.2f}%, Odds={odds}, Stake={stake:.2f}u")

                tracker_key = build_tracker_key(
                    base_fields["game_id"],
                    base_fields["market"],
                    base_fields["side"],
                )

                # 🧪 Optional Debug
                if VERBOSE and not prior_snapshot_row:
                    print(
                        f"⚠️ Missing _prior_snapshot for {tracker_key} in expanded_row"
                    )

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
                        "raw_kelly": raw_kelly,
                        "adjusted_kelly": stake,
                        "_prior_snapshot": prior_snapshot_row,
                        "_raw_sportsbook": raw_books,
                        "consensus_books": raw_books,
                    }

                    for field in [
                        "ev_movement",
                        "fv_movement",
                        "odds_movement",
                        "stake_movement",
                        "sim_movement",
                        "mkt_movement",
                        "is_new",
                    ]:
                        if field in base_fields:
                            expanded_row[field] = base_fields[field]

                    for disp in [
                        "ev_display",
                        "fv_display",
                        "odds_display",
                        "stake_display",
                        "sim_prob_display",
                        "mkt_prob_display",
                    ]:
                        if disp in base_fields:
                            expanded_row[disp] = base_fields[disp]

                    ensure_consensus_books(expanded_row)
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


def market_prob_increase_threshold(
    hours_to_game: float, market_type: str = ""
) -> float:
    """Return required market_prob delta for logging based on time to game.

    A lower threshold is returned for derivative markets (e.g. F5, 1st inning) to
    allow small market moves to pass the filter.
    """

    market_key = market_type.lower() if isinstance(market_type, str) else ""
    is_derivative = any(x in market_key for x in ["1st", "f5", "innings"])

    if hours_to_game >= 48:
        return 0.004 if is_derivative else 0.005
    elif hours_to_game <= 6:
        return 0.001 if is_derivative else 0.002
    else:
        decay = 0.003
        floor = 0.001 if is_derivative else 0.002
        return floor + (decay * (hours_to_game - 6) / 42)


def should_include_in_summary(row):
    """
    Return True if the row qualifies to appear in summary notifications.
    Currently defined as EV ≥ 5.0%.
    """
    return row.get("ev_percent", 0) >= 5.0


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
    keyed by (game_id, market, side) → cumulative stake
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
                delta = float(stake_str) if stake_str else 0.0
                existing[key] = existing.get(key, 0.0) + delta
            except Exception as e:
                print(f"⚠️ Error parsing row {row}: {e}")
    return existing

def get_market_class_emoji(segment_label: str) -> str:
    """Return an emoji representing the market class."""
    mapping = {
        "alt_line": "\U0001F4D0",  # 📐
        "derivative": "\U0001F9E9",  # 🧩
        "team_total": "\U0001F3AF",  # 🎯
        "pk_equiv": "\u2796",  # ➖
    }
    return mapping.get(segment_label, "\U0001F4CA")  # 📊 by default


def get_topup_note(ev: float, stake: float, full_stake: float, entry_type: str, market_class: str | None) -> tuple[str, str, str, str]:
    """Return tag, header, bet label and optional top-up note."""

    if entry_type == "top-up":
        bet_label = "\U0001F501 Top-Up"
    elif market_class == "alternate":
        bet_label = "\U0001F7E2 First Bet (\u215B Kelly)"
    else:
        bet_label = "\U0001F7E2 First Bet"

    if entry_type == "top-up":
        tag = "\U0001F501"
        header = "**Top-Up Bet Logged**"
    else:
        tag = "\U0001F7E2" if ev >= 10 else "\U0001F7E1" if ev >= 5 else "⚪"
        header = "**New Bet Logged**"

    note = ""
    if entry_type == "top-up":
        note = f"\U0001F501 Top-Up: `{stake:.2f}u` added → Total: `{full_stake:.2f}u`"

    return tag, header, bet_label, note


def build_discord_embed(row: dict) -> str:
    """Return the Discord message body for a logged bet."""
    ev = float(row.get("ev_percent", 0))
    stake = round(float(row.get("stake", 0)), 2)
    full_stake = round(float(row.get("full_stake", stake)), 2)
    entry_type = row.get("entry_type", "first")

    tag, header, bet_label, topup_note = get_topup_note(
        ev, stake, full_stake, entry_type, row.get("market_class")
    )

    if row.get("test_mode"):
        header = f"[TEST] {header}"

    game_id = row["game_id"]
    side = row["side"]
    market = row["market"]

    segment_label = row.get("segment_label", "mainline")
    from utils import format_segment_header
    segment_header = format_segment_header(segment_label)

    odds = row["market_odds"]
    if isinstance(odds, (int, float)) and odds > 0:
        odds = f"+{int(odds) if float(odds).is_integer() else odds}"

    from datetime import datetime, timedelta

    now = datetime.now()
    parts = parse_game_id(game_id)
    game_date = datetime.strptime(parts["date"], "%Y-%m-%d").date()

    if game_date == now.date():
        game_day_tag = "\U0001F4C5 *Today*"
    elif game_date == (now.date() + timedelta(days=1)):
        game_day_tag = "\U0001F4C5 *Tomorrow*"
    else:
        game_day_tag = f"\U0001F4C5 *{game_date.strftime('%A')}*"

    from utils import TEAM_ABBR_TO_NAME

    try:
        away_team = TEAM_ABBR_TO_NAME.get(parts["away"], parts["away"])
        home_team = TEAM_ABBR_TO_NAME.get(parts["home"], parts["home"])
        event_label = f"{away_team} @ {home_team}"
        game_time = row.get("Time")
        if isinstance(game_time, str) and game_time.strip():
            event_label += f" ({game_time} ET)"
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

    tracker_key = build_tracker_key(game_id, market, side)
    prior = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key)
    movement = row.get("_movement")
    if movement is None:
        movement = detect_market_movement(row, prior)
        row["_movement"] = movement
    print(
        f"\U0001f4e2 Sending alert for {tracker_key} | Mkt: {market} | Side: {side} | EV%: {ev}"
    )

    sim_prob = row.get("sim_prob")
    # ✅ Normalize market_prob from consensus_prob if not already present
    if "market_prob" not in row and "consensus_prob" in row:
        row["market_prob"] = row["consensus_prob"]
    consensus_prob = row.get("market_prob")
    blended_prob = row.get("blended_prob")

    def _parse_odds_dict(val):
        if isinstance(val, dict):
            if len(val) == 1:
                ((k, v),) = val.items()
                if isinstance(k, str) and k.strip().startswith("{") and k.strip().endswith("}"):
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

    def to_decimal(american_odds):
        try:
            return 100 / abs(american_odds) + 1 if american_odds < 0 else (american_odds / 100) + 1
        except Exception:
            return 0.0

    ev_map = {}
    if isinstance(all_odds_dict, dict):
        for book, price in all_odds_dict.items():
            try:
                ev_map[book.lower()] = (
                    blended_prob * to_decimal(float(price)) - 1
                ) * 100
            except Exception:
                continue

    odds_str, roles_text = format_market_odds_and_roles(
        best_book,
        all_odds_dict if isinstance(all_odds_dict, dict) else {},
        ev_map,
        BOOKMAKER_TO_ROLE,
    )

    if roles_text:
        roles = set(roles_text.replace("📣", "").split())
        if len(roles) > 1:
            print(f"🔔 Multiple books tagged: {', '.join(sorted(roles))}")

    market_prob_str = row.get("mkt_prob_display")
    if not market_prob_str:
        prev_market_prob = None
        if isinstance(prior, dict):
            prev_market_prob = prior.get("market_prob")
        if prev_market_prob is not None:
            market_prob_str = f"{prev_market_prob:.1%} → {consensus_prob:.1%}"
        else:
            market_prob_str = f"{consensus_prob:.1%}"

    ev_str = row.get("ev_display", f"{ev:+.2f}%")

    parts = [
        f"{tag} {header}",
        "",
        f"{game_day_tag} | {segment_header}",
        f"🏟️ Game: **{event_label}**",
        f"🧾 Market: **{market} — {side}**",
        f"💰 Stake: **{stake:.2f}u @ {odds}** → {bet_label}",
    ]
    if topup_note:
        parts.append(topup_note)
    parts.append("")
    parts.append("---")
    parts.append("")
    parts.extend(
        [
            "📈 **Model vs. Market**",
            f"• Sim Win Rate: **{sim_prob:.1%}**",
            f"• Market Implied: **{market_prob_str}**",
            f"• Blended: **{blended_prob:.1%}**",
            f"💸 Fair Value: **{row.get('blended_fv')}**",
            f"📊 EV: **{ev_str}**",
            "",
            "---",
            "",
            f"🏦 **Best Book**: {best_book}",
            f"📉 **Market Odds**:\n{odds_str}",
        ]
    )
    if roles_text:
        parts.extend(["", roles_text])

    return "\n".join(parts)


def send_discord_notification(row, skipped_bets=None):
    """Send a bet alert to Discord."""

    # NOTE: All bet alerts route to master feed only — role-specific routing is handled by snapshots.
    webhook_url = OFFICIAL_PLAYS_WEBHOOK_URL or DISCORD_WEBHOOK_URL
    if not webhook_url:
        print("⚠️ No Discord webhook configured. Notification skipped.")
        if skipped_bets is not None and should_include_in_summary(row):
            row["skip_reason"] = SkipReason.NO_WEBHOOK.value
            ensure_consensus_books(row)
            skipped_bets.append(row)
        return

    print(f"Webhook URL resolved: {webhook_url}")

    stake = round(float(row.get("stake", 0)), 2)
    full_stake = round(float(row.get("full_stake", stake)), 2)
    entry_type = row.get("entry_type", "first")
    print(
        f"📬 Sending Discord Notification → stake: {stake}, full: {full_stake}, type: {entry_type}"
    )

    message = build_discord_embed(row)

    try:
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
    force_log=False,
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
        Mutable mapping tracking theme exposure in-memory. This function only
        updates the provided dict. Persisting the updated exposure data is
        handled by the caller.
    """
    if not force_log and should_skip_due_to_quiet_hours(
        start_hour=quiet_hours_start,
        end_hour=quiet_hours_end,
    ):
        print(
            f"🕒 Logging disabled during quiet hours ({quiet_hours_start:02d}:00-"
            f"{quiet_hours_end:02d}:00 ET). Skipping CSV write."
        )
        row["skip_reason"] = SkipReason.QUIET_HOURS.value
        return None

    # 🗓️ Derive human-friendly fields from game_id
    parsed = parse_game_id(str(row.get("game_id", "")))
    row["Date"] = parsed.get("date", "")
    row["Matchup"] = f"{parsed.get('away', '')} @ {parsed.get('home', '')}".strip()
    time_part = parsed.get("time", "")
    time_formatted = ""
    if isinstance(time_part, str) and time_part.startswith("T"):
        raw = time_part.split("-")[0][1:]
        try:
            time_formatted = datetime.strptime(raw, "%H%M").strftime("%-I:%M %p")
        except Exception:
            try:
                time_formatted = (
                    datetime.strptime(raw, "%H%M").strftime("%I:%M %p").lstrip("0")
                )
            except Exception:
                time_formatted = ""
    row["Time"] = time_formatted
    key = (row["game_id"], row["market"], row["side"])
    tracker_key = build_tracker_key(row["game_id"], row["market"], row["side"])

    new_conf = row.get("consensus_prob")
    try:
        new_conf_val = float(new_conf) if new_conf is not None else None
    except Exception:
        new_conf_val = None

    prev_conf_val = None
    if isinstance(MARKET_CONF_TRACKER.get(tracker_key), dict):
        prev_conf_val = MARKET_CONF_TRACKER[tracker_key].get("consensus_prob")

    if new_conf_val is None:
        print(f"  ⛔ No valid consensus_prob for {tracker_key} — skipping")
        row["skip_reason"] = SkipReason.NO_CONSENSUS.value
        return None

    # if prev_conf_val is not None and new_conf_val <= prev_conf_val:
    #     print(
    #         f"  ⛔ Market confirmation not improved ({new_conf_val:.4f} ≤ {prev_conf_val:.4f}) — skipping {tracker_key}"
    #     )
    #     return 0
    full_stake = round(float(row.get("full_stake", 0)), 2)
    entry_type = row.get("entry_type", "first")
    stake_to_log = row.get("stake", full_stake)

    prev = existing.get(key, 0)
    row["cumulative_stake"] = prev + stake_to_log
    # Preserve the total intended exposure in full_stake
    row["full_stake"] = full_stake
    row["result"] = ""

    if dry_run:
        print(
            f"📝 [Dry Run] Would log: {key} | Stake: {stake_to_log:.2f}u | EV: {row['ev_percent']:.2f}%"
        )
        return None

    if VERBOSE and "_prior_snapshot" not in row:
        print(f"⚠️ _prior_snapshot not present in row for {tracker_key}")

    # ===== Market Confirmation =====

    if VERBOSE:
        if "_prior_snapshot" in row:
            print(f"📥 Using injected _prior_snapshot for movement check.")
        else:
            print(
                f"📥 Falling back to MARKET_EVAL_TRACKER_BEFORE_UPDATE for movement check."
            )

    prior_snapshot = row.get(
        "_prior_snapshot"
    ) or MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key)

    if VERBOSE:
        print(
            f"📈 Prior Tracker market_prob : {MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key, {}).get('market_prob')}"
        )
        print(
            f"📈 Attached Snapshot market_prob: {row.get('_prior_snapshot', {}).get('market_prob')}"
        )
        print(f"📈 New market_prob             : {row.get('market_prob')}")

        if row.get("_prior_snapshot") != MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(
            tracker_key
        ):
            print(f"⚠️ Snapshot mismatch for {tracker_key}")

    movement = detect_market_movement(row, prior_snapshot)
    row["_movement"] = movement  # store for Discord/export/debug

    if DEBUG:
        # 🔍 Snapshot Debug Metadata
        print(f"\n🔎 Movement Debug for {tracker_key}:")
        print(f"    • Simulated EV           : {row.get('ev_percent')}%")
        print(f"    • Market Prob (New)      : {row.get('market_prob')}")
        print(
            f"    • Market Prob (Prior)    : {prior_snapshot.get('market_prob') if prior_snapshot else 'None'}"
        )
        print(f"    • Movement               : {movement.get('mkt_movement')}")

        if isinstance(MARKET_EVAL_TRACKER_BEFORE_UPDATE, dict):
            print(
                f"    • Tracker Source         : Snapshot-Based Tracker (Length: {len(MARKET_EVAL_TRACKER_BEFORE_UPDATE)})"
            )
        else:
            print(f"    • Tracker Source         : Unknown")

        try:
            print(f"    • Snapshot File Used     : {SNAPSHOT_PATH_USED}")
        except NameError:
            print(f"    • Snapshot File Used     : Not available in this scope")

    prior_prob = prior_snapshot.get("market_prob") if prior_snapshot else None
    # ✅ Normalize market_prob from consensus_prob if not already present
    if "market_prob" not in row and "consensus_prob" in row:
        row["market_prob"] = row["consensus_prob"]
    new_prob = row.get("market_prob")
    hours_to_game = row.get("hours_to_game", 8)

    threshold = market_prob_increase_threshold(hours_to_game, row.get("market", ""))

    if row.get("entry_type") == "first":
        if prior_prob is None or new_prob is None:
            print(
                "⛔ No prior market probability — building baseline and skipping log."
            )
            if VERBOSE:
                print(
                    f"⛔ Skipping {row.get('entry_type')} bet — no prior market probability ({prior_prob} → {new_prob})"
                )
            if prior_prob is None:
                current_consensus_prob = new_conf_val if new_conf_val is not None else new_prob
                MARKET_CONF_TRACKER[tracker_key] = {
                    "consensus_prob": current_consensus_prob,
                    "status": "pending",
                    "timestamp": datetime.now().isoformat(),
                }
            movement = track_and_update_market_movement(
                row,
                MARKET_EVAL_TRACKER,
                MARKET_EVAL_TRACKER_BEFORE_UPDATE,
            )
            prior_row = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key) or {}
            row.update(
                {
                    "prev_sim_prob": prior_row.get("sim_prob"),
                    "prev_market_prob": prior_row.get("market_prob"),
                    "prev_blended_fv": prior_row.get("blended_fv"),
                }
            )
            annotate_display_deltas(row, prior_row)
            row["_movement_str"] = row.get("mkt_prob_display")
            row["_movement"] = movement
            row["skip_reason"] = SkipReason.MARKET_NOT_MOVED.value
            return None
        elif new_prob <= prior_prob:
            print("⛔ Market probability did not improve — skipping.")
            if VERBOSE:
                print(
                    f"⛔ Skipping {row.get('entry_type')} bet — market probability did not improve ({new_prob:.4f} ≤ {prior_prob:.4f})"
                )
            if prior_prob is None:
                current_consensus_prob = new_conf_val if new_conf_val is not None else new_prob
                MARKET_CONF_TRACKER[tracker_key] = {
                    "consensus_prob": current_consensus_prob,
                    "status": "pending",
                    "timestamp": datetime.now().isoformat(),
                }
            row["skip_reason"] = SkipReason.MARKET_NOT_MOVED.value
            return None
        elif (new_prob - prior_prob) < threshold:
            delta = new_prob - prior_prob
            print(
                f"⛔ Market % increase too small ({delta:.4f} < {threshold:.4f}) — skipping."
            )
            if VERBOSE:
                print(
                    f"⛔ Skipping {row.get('entry_type')} bet — market % increase too small ({delta:.4f} < {threshold:.4f})"
                )
            if prior_prob is None:
                current_consensus_prob = new_conf_val if new_conf_val is not None else new_prob
                MARKET_CONF_TRACKER[tracker_key] = {
                    "consensus_prob": current_consensus_prob,
                    "status": "pending",
                    "timestamp": datetime.now().isoformat(),
                }
            row["skip_reason"] = SkipReason.MARKET_NOT_MOVED.value
            return None

    # Clean up non-persistent keys
    row.pop("consensus_books", None)

    is_new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not is_new:
        with open(path, "r", newline="") as existing_file:
            reader = csv.DictReader(existing_file)
            fieldnames = reader.fieldnames or BASE_CSV_COLUMNS
        if not set(BASE_CSV_COLUMNS).issubset(set(fieldnames)):
            raise ValueError(
                "[CSV Logger] Existing CSV missing required columns"
            )
    else:
        fieldnames = BASE_CSV_COLUMNS

    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()

        # ✅ Serialize books_used dict safely
        if isinstance(row.get("books_used"), dict):
            row["books_used"] = json.dumps(row["books_used"])

        blend_weight = row.get("blend_weight_model")
        row.pop("blend_weight_model", None)

        # Remove transient keys not meant for CSV output
        for k in ["_movement", "_movement_str", "_prior_snapshot", "full_stake"]:
            row.pop(k, None)

        # Ensure required columns present in the row
        missing_required = [c for c in BASE_CSV_COLUMNS if c not in row]
        if missing_required:
            raise ValueError(
                f"[CSV Logger] Row is missing required keys: {missing_required}"
            )

        row_to_write = {k: row.get(k, "") for k in fieldnames}
        writer.writerow(row_to_write)
        if config.VERBOSE_MODE:
            print(f"✅ Logged to CSV → {row['game_id']} | {row['market']} | {row['side']}")
            if DEBUG and blend_weight is not None:
                print(f"🔢 Blend Weight (Model): {blend_weight:.2f}")
        else:
            print(
                f"✅ Logged {row['game_id']} {row['side']} ({row['market']}) — EV {row['ev_percent']:+.1f}%, Stake {row['stake']:.2f}u"
            )

        # Update market confirmation tracker on successful log
        MARKET_CONF_TRACKER[tracker_key] = {
            "consensus_prob": new_conf_val,
            "timestamp": datetime.now().isoformat(),
        }

        movement = track_and_update_market_movement(
            row,
            MARKET_EVAL_TRACKER,
            MARKET_EVAL_TRACKER_BEFORE_UPDATE,
        )
        prior_row = MARKET_EVAL_TRACKER_BEFORE_UPDATE.get(tracker_key) or {}
        row.update(
            {
                "prev_sim_prob": prior_row.get("sim_prob"),
                "prev_market_prob": prior_row.get("market_prob"),
                "prev_blended_fv": prior_row.get("blended_fv"),
            }
        )
        annotate_display_deltas(row, prior_row)
        row["_movement_str"] = row.get("mkt_prob_display")
        row["_movement"] = movement
        if should_log_movement():
            print(
                f"🧠 Movement for {tracker_key}: EV {movement['ev_movement']} | FV {movement['fv_movement']}"
            )

    existing[key] = prev + stake_to_log
    if existing_theme_stakes is not None:
        exposure_key = get_exposure_key(row)
        existing_theme_stakes[exposure_key] = (
            existing_theme_stakes.get(exposure_key, 0.0) + row["stake"]
        )

    edge = round(row["blended_prob"] - implied_prob(row["market_odds"]), 4)

    if config.VERBOSE_MODE:
        print(
            f"\n📦 Logging Bet: {row['game_id']} | {row['market']} ({row.get('market_class', '?')}) | {row['side']}"
        )

        print(f"   • Entry Type : {row['entry_type']}")
        stake_desc = (
            "full" if row["entry_type"] == "first" else f"delta of {row['stake']:.2f}u"
        )
        print(f"   • Stake      : {row['stake']:.2f}u ({stake_desc})")
        print(f"   • Odds       : {row['market_odds']} | Book: {row['best_book']}")
        print(f"   • Market Prob: {row['market_prob']*100:.1f}%")
        print(
            f"   • EV         : {row['ev_percent']:+.2f}% | Blended: {row['blended_prob']:.4f} | Edge: {edge:+.4f}\n"
        )

    return row



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

    odds_start_times = odds_start_times or {}

    date_sim = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    candidates = []

    markets = sim_results.get("markets", [])
    if not markets:
        print(f"⚠️ No 'markets' array found in {game_id}")
        return

    start_dt = odds_start_times.get(game_id)
    if not start_dt:
        start_str = (
            sim_results.get("start_time_iso")
            or sim_results.get("Start Time (ISO)")
        )
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            except Exception:
                logger.warning("❌ Failed to parse start time %s", start_str)
        if not start_dt:
            print(
                f"⚠️ No start time found for game_id: {game_id} — defaulting to 8.0 hours"
            )
    hours_to_game = 8.0
    if start_dt:
        hours_to_game = compute_hours_to_game(start_dt)

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
            lookup_side = normalize_label_for_odds(raw_lookup, market_key)
        elif market_key == "totals":
            lookup_side = normalize_label_for_odds(side_clean, market_key)
        else:
            lookup_side = normalize_label_for_odds(
                get_normalized_lookup_side(side_clean, market_key), market_key
            )

        market_entry, best_book, matched_key, segment, price_source = (
            get_market_entry_with_alternate_fallback(
                market_odds, market_key, lookup_side, debug=DEBUG
            )
        )
        if not assert_segment_match(market_key, matched_key):
            log_segment_mismatch(market_key, matched_key)
            continue

        if not isinstance(market_entry, dict):
            logger.warning("❌ No odds for %s — market %s", side, market_key)
            continue

        # Safely get the correct sim line (now that matched_key is known)
        sim_entry = find_sim_entry(
            sim_results["markets"], matched_key, side, allow_fallback=False
        )
        if not sim_entry:
            logger.warning(
                "❌ No odds for %s — missing sim entry for %s", side, matched_key
            )
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
        book_odds_list = [implied_prob(v) for v in book_prices.values()]

        tracker_key = build_tracker_key(game_id, matched_key.replace("alternate_", ""), side)
        prior = MARKET_EVAL_TRACKER.get(tracker_key)

        prev_prob = None
        if prior:
            prev_prob = prior.get("market_prob") or prior.get("consensus_prob")
        curr_prob = p_market
        try:
            observed_move = float(curr_prob) - float(prev_prob)
        except Exception:
            observed_move = 0.0

        strength = confirmation_strength(observed_move, hours_to_game)

        p_blended, w_model, p_model, _ = blend_prob(
            sim_prob,
            market_price,
            market_key,
            hours_to_game,
            p_market,
            book_odds_list=book_odds_list,
            line_move=0.0,
            observed_move=observed_move,
        )

        ev_calc = calculate_ev_from_prob(p_blended, market_price)
        stake_fraction = 0.125 if price_source == "alternate" else 0.25

        raw_kelly = kelly_fraction(p_blended, market_price, fraction=stake_fraction)
        stake = round(
            raw_kelly * (strength ** 1.5),
            4,
        )

        # print statement below was previously used for every bet processed
        # but created noisy output during batch logging. It has been removed
        # in favor of an optional debug message controlled by ``VERBOSE_MODE``.
        if config.VERBOSE_MODE:
            print(
                f"[DEBUG] Preparing to evaluate: game={game_id}, market={matched_key}, side={side_clean}"
            )

        best_book_str = (
            extract_best_book(book_prices) if isinstance(book_prices, dict) else best_book
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
            "raw_kelly": raw_kelly,
            "adjusted_kelly": stake,
            "entry_type": "",
            "segment": segment,
            "segment_label": get_segment_label(matched_key, side_clean),
            "price_source": price_source,
            "best_book": best_book_str,
            "date_simulated": date_sim,
            "result": "",
        }

        # Preserve the raw start timestamp for filtering/debugging
        row["Start Time (ISO)"] = market_odds.get("start_time", "")

        if isinstance(book_prices, dict):
            row["_raw_sportsbook"] = book_prices.copy()
            row["consensus_books"] = book_prices.copy()
        else:
            row["consensus_books"] = {best_book_str: market_price}

        # 📝 Track every evaluated bet before applying stake/EV filters
        tracker_key = build_tracker_key(row["game_id"], row["market"], row["side"])
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

        key = (game_id, matched_key, side)
        prev = existing.get(key, 0)
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

    odds_start_times = odds_start_times or {}

    start_dt = odds_start_times.get(game_id)
    if not start_dt:
        start_str = (
            derivative_segments.get("start_time_iso")
            or derivative_segments.get("Start Time (ISO)")
        )
        if start_str:
            try:
                start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            except Exception:
                logger.warning("❌ Failed to parse start time %s", start_str)
        if not start_dt:
            print(
                f"⚠️ No start time found for game_id: {game_id} — defaulting to 8.0 hours"
            )
    hours_to_game = 8.0
    if start_dt:
        hours_to_game = compute_hours_to_game(start_dt)

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
                    lookup_side = normalize_label_for_odds(
                        convert_full_team_spread_to_odds_key(side_clean), market_key
                    )
                elif market_key == "totals":
                    lookup_side = normalize_label_for_odds(side_clean, market_key)
                else:
                    lookup_side = normalize_label_for_odds(
                        get_normalized_lookup_side(side_clean, market_key), market_key
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
                            market_odds, market_key, lookup_side, debug=DEBUG
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
                        logger.warning(
                            "❌ No odds for %s in %s_%s",
                            label,
                            market_key,
                            segment_clean,
                        )
                        continue

                    market_full = matched_key  # set final market key
                    logger.debug(
                        "📦 Matched via %s | Segment: %s | Price Source: %s",
                        market_full,
                        segment,
                        price_source,
                    )

                if not isinstance(market_entry, dict):
                    logger.warning(
                        "❌ No odds for %s in %s",
                        label,
                        market_full,
                    )
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
                    logger.debug(
                        "⚠️ Raw books existed but cleaned empty — %s | %s: %s",
                        game_id,
                        lookup_side,
                        raw_books,
                    )
                else:
                    logger.debug(
                        "📦 %s | %s | %s → book_prices: %s",
                        game_id,
                        market_full,
                        lookup_side,
                        book_prices,
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

                book_odds_list = [implied_prob(v) for v in book_prices.values()]

                tracker_key = build_tracker_key(game_id, market_full.replace("alternate_", ""), side_clean)
                prior = MARKET_EVAL_TRACKER.get(tracker_key)

                prev_prob = None
                if prior:
                    prev_prob = prior.get("market_prob") or prior.get("consensus_prob")
                curr_prob = p_market
                try:
                    observed_move = float(curr_prob) - float(prev_prob)
                except Exception:
                    observed_move = 0.0

                strength = confirmation_strength(observed_move, hours_to_game)

                p_blended, w_model, p_model, _ = blend_prob(
                    p_model=prob,
                    market_odds=market_price,
                    market_type=market_key,
                    hours_to_game=hours_to_game,
                    p_market=p_market,
                    book_odds_list=book_odds_list,
                    line_move=0.0,
                    observed_move=observed_move,
                )

                print(
                    f"🧪 Blending: Model {p_model:.4f} | Market {p_market:.4f} | Blended {p_blended:.4f} | Weight Model: {w_model:.2f}"
                )

                dec_odds = decimal_odds(market_price)
                blended_fair_odds = 1 / p_blended
                ev_calc = calculate_ev_from_prob(p_blended, market_price)  # ✅ correct
                stake_fraction = 0.125 if price_source == "alternate" else 0.25

                raw_kelly = kelly_fraction(p_blended, market_price, fraction=stake_fraction)
                stake = round(raw_kelly * (strength ** 1.5), 4)

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

                # Removed noisy print that logged every bet. Use verbose mode
                # for optional debug visibility when needed.
                if config.VERBOSE_MODE:
                    print(
                        f"[DEBUG] Preparing to evaluate: game={game_id}, market={matched_key}, side={side_clean}"
                    )

                best_book_str = (
                    extract_best_book(book_prices) if isinstance(book_prices, dict) else sportsbook_source
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
                    "raw_kelly": raw_kelly,
                    "adjusted_kelly": stake,
                    "entry_type": "",  # Set below based on `prev`
                    "segment": segment,
                    "segment_label": get_segment_label(market_full, side_clean),
                    "best_book": best_book_str,
                    "date_simulated": date_sim,
                    "result": "",
                }

                # Preserve the raw start timestamp for filtering/debugging
                row["Start Time (ISO)"] = market_odds.get("start_time", "")

                if isinstance(book_prices, dict):
                    row["_raw_sportsbook"] = book_prices.copy()
                    row["consensus_books"] = book_prices.copy()
                else:
                    row["consensus_books"] = {best_book_str: market_price}

                if config.DEBUG_MODE or config.VERBOSE_MODE:
                    print(f"📦 Books stored in row: {book_prices}")
                    print(f"🏦 Best Book Selected: {row['best_book']}")
                # 📝 Track every evaluated bet before applying stake/EV filters
                tracker_key = build_tracker_key(
                    row["game_id"], row["market"], row["side"]
                )
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

                full_stake = stake
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
        print("⚠️ No Discord summary webhook URL provided. Skipping Discord summary.")
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

            elif isinstance(b.get("best_book"), str):
                odds_value = b.get("market_odds")
                if isinstance(odds_value, (int, float)):
                    books_str = f"🏦 {b['best_book']}: {odds_value:+}"

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


def save_skipped_bets(skipped_bets: list, base_dir: str = os.path.join("logs", "skipped_bets")) -> str:
    """Persist ``skipped_bets`` as a JSON file named by today's date.

    Returns the final file path written.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    os.makedirs(base_dir, exist_ok=True)
    tmp_path = os.path.join(base_dir, f"{today}.json.tmp")
    final_path = os.path.join(base_dir, f"{today}.json")
    with open(tmp_path, "w") as f:
        json.dump(skipped_bets, f, indent=2)
    os.replace(tmp_path, final_path)
    logger.info("💾 Saved %d skipped bets to %s", len(skipped_bets), final_path)
    return final_path


def run_batch_logging(
    eval_folder,
    market_odds,
    min_ev,
    dry_run=False,
    debug=False,
    image=False,
    output_dir="logs",
    fallback_odds_path=None,
    force_log=False,
    no_save_skips=False,
):
    from collections import defaultdict
    import os, json
    from dotenv import load_dotenv

    load_dotenv()

    if market_odds is None:
        logger.warning(
            "❌ No odds data provided. Use --odds-path or pass market_odds as a dict."
        )
        return

    DISCORD_SUMMARY_WEBHOOK_URL = os.getenv("DISCORD_SUMMARY_WEBHOOK_URL")
    summary_candidates = []

    if isinstance(market_odds, str):
        all_market_odds = safe_load_json(market_odds)
        if all_market_odds is None:
            logger.warning("❌ Failed to load odds file %s", market_odds)
            return
        if not all_market_odds or not isinstance(all_market_odds, dict):
            logger.warning(
                "❌ Odds file %s is empty or malformed — skipping logging.", market_odds
            )
            return
    else:
        all_market_odds = market_odds

    fallback_odds = {}
    if fallback_odds_path:
        fallback_odds = safe_load_json(fallback_odds_path) or {}
        if not isinstance(fallback_odds, dict):
            fallback_odds = {}
        print(
            f"📂 Loaded fallback odds from {fallback_odds_path} ({len(fallback_odds)} games)"
        )

    if fallback_odds:
        merged_odds = dict(fallback_odds)
        merged_odds.update(all_market_odds)
        all_market_odds = merged_odds

    def extract_start_times(odds_data):
        from dateutil import parser
        from pytz import timezone
        from utils import canonical_game_id

        if not isinstance(odds_data, dict):
            print(
                "⚠️ extract_start_times: odds_data is None or invalid, returning empty dict."
            )
            return {}

        eastern = timezone("US/Eastern")
        start_times = {}
        for game_id, game in odds_data.items():
            if not isinstance(game, dict):
                continue
            if "start_time" in game:
                try:
                    canon_id = canonical_game_id(game_id)
                    start_times[canon_id] = parser.parse(game["start_time"]).astimezone(
                        eastern
                    )
                except Exception:
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
            if DEBUG:
                print(
                    f"🧹 Skipped in cache — {market} | {row['side']} | "
                    f"EV {row['ev_percent']} not better than current {current_best['ev_percent']}"
                )

    existing_theme_stakes = load_theme_stakes()

    odds_start_times = extract_start_times(all_market_odds)

    for fname in os.listdir(eval_folder):
        if not fname.endswith(".json"):
            continue



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

        if not mkt and "-T" in game_id:
            from datetime import datetime, timedelta
            from utils import parse_game_id

            try:
                parts = parse_game_id(game_id)
                date = parts["date"]
                away = parts["away"]
                home = parts["home"]
                suffix = parts.get("time", "").split("-")[0]  # just T####

                if suffix.startswith("T"):
                    t_str = suffix[1:]
                    t_dt = datetime.strptime(t_str, "%H%M")

                    # Filter only game_ids with the same teams and date
                    candidate_ids = [
                        gid
                        for gid in all_market_odds
                        if f"{date}-{away}@{home}" in gid and "-T" in gid
                    ]

                    closest_match = None
                    min_diff = float("inf")

                    for cid in candidate_ids:
                        try:
                            c_parts = parse_game_id(cid)
                            c_suffix = c_parts.get("time", "").split("-")[0]
                            if c_suffix.startswith("T"):
                                c_t_str = c_suffix[1:]
                                c_dt = datetime.strptime(c_t_str, "%H%M")
                                diff = abs((t_dt - c_dt).total_seconds()) / 60
                                if diff <= 2 and diff < min_diff:
                                    closest_match = cid
                                    min_diff = diff
                        except Exception:
                            continue

                    if closest_match:
                        print(f"🔄 Fuzzy matched {game_id} → {closest_match}")
                        mkt = all_market_odds.get(closest_match)
            except Exception:
                pass

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
        force_log=force_log,
    )

    if summary_candidates:
        from core.pending_bets import queue_pending_bet

        for bet in summary_candidates:
            try:
                queue_pending_bet(bet)
            except Exception:
                pass
        print(f"📁 Queued {len(summary_candidates)} pending bets to pending_bets.json")

    if summary_candidates and not no_save_skips:
        save_skipped_bets(summary_candidates)


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
    force_log=False,
):
    print("🧾 Final Trimmed Bets to Log:")

    skipped_counts = {
        "duplicate": 0,
        SkipReason.LOW_INITIAL.value: 0,
        SkipReason.LOW_TOPUP.value: 0,
        SkipReason.ALREADY_LOGGED.value: 0,
        "low_ev": 0,
        "low_stake": 0,
    }

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
        print(f"🔍 Game: {game_id}")

        print("📊 Theme Map:")
        for theme_key, segment_map in theme_logged[game_id].items():
            ordered_rows = []
            for segment, row in segment_map.items():
                ordered_rows.append((segment, row))
            ordered_rows.sort(
                key=lambda x: 1 if x[1].get("market_class") == "alternate" else 0
            )
            for segment, row in ordered_rows:
                stake = round(float(row.get("full_stake", row.get("stake", 0))), 2)
                ev = row.get("ev_percent", 0)
                print(
                    f"   - {theme_key} [{segment}] → {row['side']} ({row['market']}) @ {stake:.2f}u | EV: {ev:.2f}%"
                )


        for theme_key, segment_map in theme_logged[game_id].items():
            ordered_rows = []
            for segment, row in segment_map.items():
                ordered_rows.append((segment, row))
            ordered_rows.sort(
                key=lambda x: 1 if x[1].get("market_class") == "alternate" else 0
            )
            for segment, row in ordered_rows:
                proposed_stake = round(float(row.get("full_stake", 0)), 2)
                key = (row["game_id"], row["market"], row["side"])
                line_key = (row["market"], row["side"])
                exposure_key = get_exposure_key(row)
                theme_total = existing_theme_stakes.get(exposure_key, 0.0)
                is_initial_bet = theme_total == 0.0

                skip_reason = None
                should_log = True

                existing_stake = existing.get(key, 0.0)
                if existing_stake > 0:
                    print(
                        f"                🧾 Existing     : {existing_stake:.2f}u already logged in market_evals.csv"
                    )

                if key in seen_keys or line_key in seen_lines:
                    skip_reason = "duplicate"
                    skipped_counts["duplicate"] += 1
                    should_log = False

                if theme_total >= proposed_stake:
                    skip_reason = SkipReason.ALREADY_LOGGED.value
                    skipped_counts[SkipReason.ALREADY_LOGGED.value] += 1
                    if should_include_in_summary(row):
                        row["skip_reason"] = SkipReason.ALREADY_LOGGED.value
                        ensure_consensus_books(row)
                        skipped_bets.append(row)
                    should_log = False


                if should_log:
                    if config.VERBOSE_MODE:
                        print(
                            f"✅ Logged {row['game_id']} {row['side']} ({segment}) — EV {row['ev_percent']:+.1f}%"
                        )
                elif config.VERBOSE_MODE:
                    print(
                        f"⛔ Skipped {row['game_id']} {row['side']} — Reason: {skip_reason}"
                    )
                if not should_log:
                    continue

                seen_keys.add(key)
                seen_lines.add(line_key)
                row["entry_type"] = "top-up" if not is_initial_bet else "first"
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
                    verbose=config.VERBOSE_MODE,
                    eval_tracker=MARKET_EVAL_TRACKER,
                    reference_tracker=MARKET_EVAL_TRACKER_BEFORE_UPDATE,
                )

                if not evaluated:
                    reason = row_copy.get("skip_reason", "skipped")
                    skipped_counts[reason] = skipped_counts.get(reason, 0) + 1
                    if should_include_in_summary(row):
                        row["skip_reason"] = reason
                        ensure_consensus_books(row)
                        skipped_bets.append(row)
                    continue

                # 📝 Update tracker for every evaluated bet
                t_key = build_tracker_key(
                    row_copy["game_id"], row_copy["market"], row_copy["side"]
                )
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

                    if not current_best or evaluated["ev_percent"] > current_best.get(
                        "ev_percent", -999
                    ):
                        best_market_segment[key_best] = evaluated

    # ➡️ Log only the best bet per (game_id, market, segment)
    logged_bets_this_loop = []
    final_rows = []
    for best_row in best_market_segment.values():
        if config.VERBOSE_MODE:
            print(
                f"📄 Logging: {best_row['game_id']} | {best_row['market']} | {best_row['side']} @ {best_row['stake']}u"
            )
        try:
            result = write_to_csv(
                best_row,
                "logs/market_evals.csv",
                existing,
                session_exposure,
                existing_theme_stakes,
                dry_run=dry_run,
                force_log=force_log,
            )
            final_rows.append(best_row)
        except Exception as e:  # pragma: no cover - unexpected failure
            label_key = f"{best_row.get('game_id')}|{best_row.get('market')}|{best_row.get('side')}"
            logger.error(
                "❌ Failed to write row to market_evals.csv: %s → %s",
                label_key,
                e,
            )
            continue

        if result:
            logged_bets_this_loop.append(result)
            game_summary[best_row["game_id"]].append(best_row)
            logged_stake = best_row["stake"]
            exposure_key = get_exposure_key(best_row)
            existing_theme_stakes[exposure_key] = (
                existing_theme_stakes.get(exposure_key, 0.0) + logged_stake
            )
            if should_include_in_summary(best_row):
                ensure_consensus_books(best_row)
                skipped_bets.append(best_row)
        else:
            print(
                f"⛔ CSV Log Failed → {best_row['game_id']} | {best_row['market']} | {best_row['side']}"
            )
            if best_row.get("skip_reason") and should_include_in_summary(best_row):
                ensure_consensus_books(best_row)
                skipped_bets.append(best_row)

    for row in logged_bets_this_loop:
        print(
            f"📤 Dispatching to Discord → {row['game_id']} | {row['market']} | {row['side']}"
        )
        send_discord_notification(row, skipped_bets)

    print(f"🧾 Summary: {len(logged_bets_this_loop)} logged, {sum(skipped_counts.values())} skipped")

    # ✅ Expand snapshot per book with proper stake & EV% logic
    snapshot_raw = final_rows + skipped_bets
    final_snapshot = expand_snapshot_rows_with_kelly(
        snapshot_raw, min_ev=snapshot_ev, min_stake=0.5
    )

    if VERBOSE:
        print("\n🧠 Snapshot Prob Consistency Check:")
        for row in final_snapshot:
            key = build_tracker_key(row["game_id"], row["market"], row["side"])
            prior = row.get("_prior_snapshot")
            if prior:
                print(
                    f"🧠 {key} | Prior market_prob: {prior.get('market_prob')} | Current: {row.get('market_prob')}"
                )
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

    # Persist updated theme exposure once per batch
    save_theme_stakes(existing_theme_stakes)

    try:
        save_tracker(MARKET_EVAL_TRACKER)
        logger.info(
            "\u2705 Tracker saved with %d entries.", len(MARKET_EVAL_TRACKER)
        )
    except Exception as e:  # pragma: no cover - unexpected save failure
        logger.warning("\u26A0\ufe0f Failed to save market eval tracker: %s", e)

    try:
        save_market_conf_tracker(MARKET_CONF_TRACKER)
    except Exception as e:
        logger.warning("\u26A0\ufe0f Failed to save market confirmation tracker: %s", e)

    if not config.DEBUG_MODE:
        print(
            f"\n🧾 Summary: {len(logged_bets_this_loop)} logged, {sum(skipped_counts.values())} skipped"
        )
        for reason, count in skipped_counts.items():
            print(f"  - {count} skipped due to {reason}")


if __name__ == "__main__":
    p = argparse.ArgumentParser("Log value bets from sim output")
    p.add_argument(
        "--eval-folder", required=True, help="Folder containing simulation JSON files"
    )
    p.add_argument("--odds-path", default=None, help="Path to cached odds JSON")
    p.add_argument(
        "--fallback-odds-path",
        default=None,
        help="Path to prior odds JSON for fallback lookup",
    )
    p.add_argument(
        "--min-ev", type=float, default=0.05, help="Minimum EV% threshold for bets"
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Preview bets without writing to CSV"
    )
    p.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )
    p.add_argument(
        "--image",
        action="store_true",
        help="Generate summary image and post to Discord",
    )
    p.add_argument("--output-dir", default="logs", help="Directory for summary image")
    p.add_argument(
        "--show-pending", action="store_true", help="Show pending bet details"
    )
    p.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    p.add_argument(
        "--force-log",
        action="store_true",
        help="Bypass quiet hours and allow logging at any time",
    )
    p.add_argument(
        "--no_save_skips",
        action="store_true",
        help="Disable saving skipped bets to disk",
    )
    args = p.parse_args()

    if args.debug:
        set_log_level("DEBUG")
    DEBUG = args.debug

    VERBOSE = args.verbose
    SHOW_PENDING = args.show_pending
    force_log = args.force_log

    config.DEBUG_MODE = args.debug
    config.VERBOSE_MODE = args.verbose
    if config.DEBUG_MODE:
        print("🧪 DEBUG_MODE ENABLED — Verbose output activated")

    date_tag = os.path.basename(args.eval_folder)

    # ✅ Check if eval-folder exists before proceeding
    if not os.path.exists(args.eval_folder):
        logger.warning(
            "⚠️ Skipping log run — folder does not exist: %s", args.eval_folder
        )
        sys.exit(0)

    if args.odds_path:
        odds = safe_load_json(args.odds_path)
        if odds is None:
            logger.warning("❌ Failed to load odds file %s", args.odds_path)
            sys.exit(1)
        odds_file = args.odds_path
    else:
        from pathlib import Path

        sim_dir = Path(args.eval_folder)
        games = [f.stem for f in sim_dir.glob("*.json") if "-T" in f.stem]
        logger.info(
            "📡 Fetching market odds for %d games on %s...",
            len(games),
            date_tag,
        )
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
        fallback_odds_path=args.fallback_odds_path,
        force_log=force_log,
        no_save_skips=args.no_save_skips,
    )
