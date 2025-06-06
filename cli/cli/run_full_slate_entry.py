#!/usr/bin/env python
import sys
import os
from datetime import date

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.logger import get_logger
logger = get_logger(__name__)

from assets.probable_pitchers import fetch_probable_pitchers
from run_distribution_simulator import simulate_distribution
from utils import normalize_game_id

# === Config ===
DEFAULT_LINE = 9.5

# ----------------------------
# CLI HELP
# ----------------------------
def print_help():
    print(f"""
Usage: python {os.path.basename(__file__)} [DATE] [options]

Options:
  DATE                     Target date in YYYY-MM-DD (default: today)
  --debug                  Enable debug logging
  --no-weather             Disable weather adjustments
  --line=FLOAT             Total line override (default: {DEFAULT_LINE})
  --days-ahead=INT         Look ahead days when listing games (default: 1)
  --export-folder=PATH     Override JSON export root folder
  --help                   Show this help message and exit

Examples:
  python {os.path.basename(__file__)} 2025-04-17 --debug --line=8.5
  python {os.path.basename(__file__)} --days-ahead=2
"""
)

# ----------------------------
# CLI ARG PARSING
# ----------------------------
def parse_args():
    args = sys.argv[1:]
    if "--help" in args:
        print_help()
        sys.exit(0)

    date_arg = None
    debug = False
    no_weather = False
    line = DEFAULT_LINE
    days_ahead = 1
    export_folder = None

    for arg in args:
        if arg == "--debug":
            debug = True
        elif arg == "--no-weather":
            no_weather = True
        elif arg.startswith("--line="):
            try:
                line = float(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg.startswith("--days-ahead="):
            try:
                days_ahead = int(arg.split("=", 1)[1])
            except ValueError:
                pass
        elif arg.startswith("--export-folder="):
            export_folder = arg.split("=", 1)[1]
        else:
            date_arg = arg

    if not date_arg:
        date_arg = date.today().strftime("%Y-%m-%d")

    return date_arg, debug, no_weather, line, days_ahead, export_folder

# ----------------------------
# Full Slate Distribution Runner
# ----------------------------
def main():
    date_str, debug, no_weather, line, days_ahead, export_folder = parse_args()
    print(f"\n📅 Running full slate distribution for {date_str}...\n")

    # Fetch all games for the specified window
    matchups = fetch_probable_pitchers(days_ahead=days_ahead)
    game_ids = sorted(gid for gid in matchups if gid.startswith(date_str))

    if not game_ids:
        print(f"❌ No games found for {date_str}")
        sys.exit(1)

    # Loop through each game and delegate to the distribution simulator
    for gid in game_ids:
        normalized_id = normalize_game_id(gid)

        # Determine export path
        export_json = None
        if export_folder:
            folder_path = os.path.join(export_folder, date_str)
            os.makedirs(folder_path, exist_ok=True)
            export_json = os.path.join(folder_path, f"{normalized_id}.json")

        try:
            simulate_distribution(
                game_id=normalized_id,
                line=line,
                debug=debug,
                no_weather=no_weather,
                export_json=export_json
            )
            if export_json and debug:
                print(f"💾 Exported simulation JSON to {export_json}")
        except Exception as e:
            print(f"[ERROR] Simulation failed for {normalized_id} (orig {gid}): {e}")

    # Summary
    print(f"\n✅ Simulated {len(game_ids)} games for {date_str}.")

if __name__ == "__main__":
    main()

# References:
# - Original run_full_slate.py logic citeturn0file0
# - simulate_distribution from run_distribution_simulator.py citeturn0file1
