#!/usr/bin/env python
"""Generate today's unified snapshot then dispatch the sim-only version."""

import argparse
import subprocess
import sys
import os
from datetime import datetime

# Ensure project root on path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from core.utils import now_eastern
from core.logger import get_logger
from core.dispatch_sim_only_snapshot import latest_snapshot_path

logger = get_logger(__name__)


def run_cmd(cmd: list[str]) -> int:
    """Run a subprocess command in the project root."""
    logger.info("🚀 Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=ROOT_DIR)
    return proc.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate and dispatch sim-only snapshot"
    )
    parser.add_argument("--date", default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument(
        "--max-rows", type=int, default=None, help="Limit rows when dispatching"
    )
    args = parser.parse_args()

    date_str = args.date or now_eastern().strftime("%Y-%m-%d")

    gen_cmd = [
        sys.executable,
        os.path.join("core", "unified_snapshot_generator.py"),
        "--date",
        date_str,
    ]
    if run_cmd(gen_cmd) != 0:
        logger.error("❌ Snapshot generation failed")
        sys.exit(1)

    snapshot_path = latest_snapshot_path(os.path.join(ROOT_DIR, "backtest"))
    if not snapshot_path:
        logger.error("❌ Unable to locate generated snapshot")
        sys.exit(1)

    dispatch_cmd = [
        sys.executable,
        os.path.join("core", "dispatch_sim_only_snapshot.py"),
        "--snapshot-path",
        snapshot_path,
        "--date",
        date_str,
        "--output-discord",
    ]
    if args.max_rows:
        dispatch_cmd.append(f"--max-rows={args.max_rows}")

    if run_cmd(dispatch_cmd) == 0:
        logger.info("✅ Sim-only snapshot posted to Discord")
    else:
        logger.error("❌ Failed to dispatch sim-only snapshot")


if __name__ == "__main__":
    main()