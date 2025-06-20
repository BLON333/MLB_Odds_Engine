#!/usr/bin/env python
import sys
import os
from core.bootstrap import *  # noqa
from core.config import DEBUG_MODE, VERBOSE_MODE
import json
import re
from datetime import date


from core.logger import get_logger
logger = get_logger(__name__)

# === Core Modules ===
from assets.probable_pitchers import fetch_probable_pitchers
from cli.run_distribution_simulator import simulate_distribution
from core.utils import canonical_game_id


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
  --edge-threshold=FLOAT   Minimum edge threshold (e.g., 0.05)
  --line=FLOAT             Total line override (default: {DEFAULT_LINE})
  --days-ahead=INT         Look ahead days when listing games (default: 1)
  --export-folder=PATH     Override JSON export root folder
  --safe                   Skip games that fail to simulate instead of exiting
  --help                   Show this help message and exit

Examples:
  python {os.path.basename(__file__)} 2025-04-17 --debug --edge-threshold=0.04 --line=8.5
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
    edge_threshold = None
    line = DEFAULT_LINE
    days_ahead = 1
    export_folder = None
    safe_mode = False

    for arg in args:
        if arg == "--debug":
            debug = True
        elif arg == "--no-weather":
            no_weather = True
        elif arg.startswith("--edge-threshold="):
            try:
                edge_threshold = float(arg.split("=", 1)[1])
            except ValueError:
                pass
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
        elif arg == "--safe":
            safe_mode = True
        else:
            date_arg = arg

    if not date_arg:
        date_arg = date.today().strftime("%Y-%m-%d")

    return (
        date_arg,
        debug,
        no_weather,
        edge_threshold,
        line,
        days_ahead,
        export_folder,
        safe_mode,
    )

# ----------------------------
# Full Slate Distribution Runner
# ----------------------------
def main():
    (
        date_str,
        debug,
        no_weather,
        edge_threshold,
        line,
        days_ahead,
        export_folder,
        safe_mode,
    ) = parse_args()
    logger.info("\n📅 Running full slate distribution for %s...\n", date_str)

    # Fetch all games for the specified window
    matchups = fetch_probable_pitchers(days_ahead=days_ahead)
    game_ids = sorted(
        canonical_game_id(gid)
        for gid in matchups
        if gid.startswith(date_str)
    )

    if not game_ids:
        logger.error("❌ No games found for %s", date_str)
        sys.exit(1)

    # Loop through each game and delegate to the distribution simulator
    for gid in game_ids:
        canonical_id = canonical_game_id(gid)

        # Determine export path
        export_json = None
        if export_folder:
            folder_path = os.path.join(export_folder, date_str)
            os.makedirs(folder_path, exist_ok=True)
            export_json = os.path.join(folder_path, f"{canonical_id}.json")

        try:
            simulate_distribution(
                game_id=canonical_id,
                line=line,
                debug=debug,
                no_weather=no_weather,
                edge_threshold=edge_threshold,
                export_json=export_json,
                n_simulations=10000,
            )
            if export_json and debug:
                logger.debug("💾 Exported simulation JSON to %s", export_json)
        except Exception as e:
            logger.warning(
                "Failed to simulate game %s: %s", canonical_id, str(e)
            )
            if not safe_mode:
                sys.exit(1)

    # Summary
    logger.info("\n✅ Simulated %s games for %s.", len(game_ids), date_str)

if __name__ == "__main__":
    main()

# References:
# - Original run_full_slate.py logic citeturn0file0
# - simulate_distribution from run_distribution_simulator.py citeturn0file1