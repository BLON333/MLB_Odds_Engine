from core import config
import argparse
import sys
import os

import sys

if sys.version_info >= (3, 7):
    import os

    os.environ["PYTHONIOENCODING"] = "utf-8"

# Ensure project root is on the path regardless of where this script is invoked
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)
PYTHON = sys.executable

from dotenv import load_dotenv

load_dotenv()
from core.logger import get_logger

logger = get_logger(__name__)

parser = argparse.ArgumentParser("Auto sim loop")
parser.add_argument("--debug", action="store_true", help="Enable debug logging")
parser.add_argument(
    "--verbose", action="store_true", help="Enable verbose logging"
)
args = parser.parse_args()

config.DEBUG_MODE = args.debug
config.VERBOSE_MODE = args.verbose
if config.DEBUG_MODE:
    print("🧪 DEBUG_MODE ENABLED — Verbose output activated")

import time
import subprocess


from datetime import timedelta
from utils import now_eastern
from core.odds_fetcher import fetch_all_market_odds, save_market_odds_to_file

EDGE_THRESHOLD = 0.05
MIN_EV = 0.05
SIM_INTERVAL = 60 * 30  # Every 30 minutes
LOG_INTERVAL = 60 * 5  # Every 5 minutes
last_sim_time = 0
last_log_time = 0
last_snapshot_time = 0

# Track the closing odds monitor subprocess so we can restart if it exits
closing_monitor_proc = None
early_bet_monitor_proc = None
active_processes: list[dict] = []  # Track background subprocesses


def seconds_to_readable(seconds: float) -> str:
    """Return minutes remaining rounded to the nearest whole minute."""
    minutes = int(seconds // 60)
    if minutes <= 0:
        return "<1m"
    return f"{minutes}m"


def run_subprocess(cmd):
    """Run a subprocess synchronously and log output."""
    timestamp = now_eastern()
    logger.info("\n%s", "═" * 60)
    logger.info("🚀 [%s] Starting subprocess:", timestamp)
    logger.info("👉 %s", " ".join(cmd))
    logger.info("%s\n", "═" * 60)

    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            encoding="utf-8",  # ✅ ensures emoji + UTF-8 output renders cleanly
            errors="replace",  # ✅ prevents crashes from odd characters
            check=True,
            env={**os.environ, "PYTHONPATH": ROOT_DIR},
        )

        if proc.stdout:
            logger.debug("📤 STDOUT:\n%s", proc.stdout)

        if proc.stderr:
            logger.debug("⚠️ STDERR:\n%s", proc.stderr)

        logger.info(
            "\n✅ [%s] Subprocess completed with exit code %s",
            now_eastern(),
            proc.returncode,
        )
        return proc.returncode

    except subprocess.CalledProcessError as e:
        if e.stdout:
            logger.debug("📤 STDOUT (on error):\n%s", e.stdout)

        if e.stderr:
            logger.debug("⚠️ STDERR (on error):\n%s", e.stderr)

        logger.error(
            "\n❌ [%s] Command %s exited with code %s",
            now_eastern(),
            " ".join(cmd),
            e.returncode,
        )
        return e.returncode


def launch_process(name: str, cmd: list[str]) -> subprocess.Popen:
    """Launch a subprocess asynchronously and track it."""
    proc = subprocess.Popen(cmd, cwd=ROOT_DIR, env={**os.environ, "PYTHONPATH": ROOT_DIR})
    active_processes.append({"name": name, "proc": proc, "start": time.time()})
    logger.info("🚀 [%s] Started %s (PID %d)", now_eastern(), name, proc.pid)
    return proc


def poll_active_processes() -> None:
    """Check running processes and log when they finish."""
    for entry in list(active_processes):
        ret = entry["proc"].poll()
        if ret is None:
            time_running = time.time() - entry["start"]
            if entry["name"].startswith("FullSlateSim") and time_running > 45 * 60:
                logger.warning(
                    "⏳ [%s] %s still running after %dm \u2014 possible stall",
                    now_eastern(),
                    entry["name"],
                    int(time_running // 60),
                )
            elif entry["name"].startswith("LogEval") and time_running > 10 * 60:
                logger.warning(
                    "⏳ [%s] %s still running after %dm \u2014 possible stall",
                    now_eastern(),
                    entry["name"],
                    int(time_running // 60),
                )
            continue
        runtime = time.time() - entry["start"]
        if ret == 0:
            logger.info(
                "✅ [%s] Subprocess '%s' (PID %d) completed in %.1fs",
                now_eastern(),
                entry["name"],
                entry["proc"].pid,
                runtime,
            )
        else:
            logger.error(
                "❌ [%s] Subprocess '%s' (PID %d) exited with code %s after %.1fs",
                now_eastern(),
                entry["name"],
                entry["proc"].pid,
                ret,
                runtime,
            )
        active_processes.remove(entry)


def ensure_closing_monitor_running() -> bool:
    """Launch ``closing_odds_monitor.py`` if not already running.

    Returns ``True`` if the monitor was restarted or started for the first
    time, ``False`` if it was already running.
    """
    global closing_monitor_proc
    restarted = False

    exit_code = None if closing_monitor_proc is None else closing_monitor_proc.poll()

    if closing_monitor_proc is None or exit_code is not None:
        # If the previous monitor exited, log the exit code before restarting
        if closing_monitor_proc is not None and exit_code is not None:
            logger.warning(
                "⚠️ [%s] Closing odds monitor exited with code %s, restarting...",
                now_eastern(),
                exit_code,
            )

        script_path = os.path.join("cli", "closing_odds_monitor.py")
        if not os.path.exists(script_path):
            script_path = "closing_odds_monitor.py"

        closing_monitor_proc = launch_process(
            "closing_odds_monitor",
            [PYTHON, script_path],
        )

        logger.info(
            "🎯 [%s] Launching closing odds monitor (PID %d)",
            now_eastern(),
            closing_monitor_proc.pid,
        )

        restarted = True

    return restarted


def ensure_early_monitor_running() -> bool:
    """Launch ``monitor_early_bets.py`` if not already running.

    Returns ``True`` if the monitor was restarted or started for the first
    time, ``False`` if it was already running.
    """

    global early_bet_monitor_proc
    restarted = False

    exit_code = None if early_bet_monitor_proc is None else early_bet_monitor_proc.poll()

    if early_bet_monitor_proc is None or exit_code is not None:
        if early_bet_monitor_proc is not None and exit_code is not None:
            logger.warning(
                "⚠️ [%s] Early bet monitor exited with code %s, restarting...",
                now_eastern(),
                exit_code,
            )

        script_path = os.path.join("cli", "monitor_early_bets.py")
        if not os.path.exists(script_path):
            script_path = "monitor_early_bets.py"

        early_bet_monitor_proc = launch_process(
            "early_bet_monitor",
            [PYTHON, script_path],
        )

        logger.info(
            "🎯 [%s] Launching early bet monitor (PID %d)",
            now_eastern(),
            early_bet_monitor_proc.pid,
        )

        restarted = True

    return restarted


def get_date_strings():
    now = now_eastern()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return today_str, tomorrow_str


def get_today_str() -> str:
    """Return today's date as YYYY-MM-DD."""
    return now_eastern().strftime("%Y-%m-%d")


def fetch_and_cache_odds_snapshot() -> str | None:
    """Fetch market odds once per loop and save to a timestamped file."""

    logger.info(
        "\n📡 [%s] Fetching market odds for today and tomorrow...", now_eastern()
    )
    odds = fetch_all_market_odds(lookahead_days=2)
    if not odds or not isinstance(odds, dict) or len(odds) == 0:
        logger.error(
            "❌ Fetched odds snapshot is empty or invalid — skipping loop cycle."
        )
        return None
    logger.debug("📊 Fetched game_ids: %s", list(odds.keys()))
    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    tag = f"market_odds_{timestamp}"
    odds_path = save_market_odds_to_file(odds, tag)
    logger.info("✅ [%s] Saved shared odds snapshot: %s", now_eastern(), odds_path)
    return odds_path


def run_simulation():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info(
            "\n🎯 [%s] Launching full slate simulation for %s...",
            now_eastern(),
            date_str,
        )
        cmd = [
            PYTHON,
            os.path.join("cli", "full_slate_runner.py"),
            date_str,
            "--export-folder=backtest/sims",
            f"--edge-threshold={EDGE_THRESHOLD}",
        ]
        launch_process(f"FullSlateSim {date_str}", cmd)


def run_logger(odds_path: str):
    """Launch logging for today and tomorrow using a cached odds snapshot."""

    log_bets_with_snapshot_odds(odds_path)


def log_bets_with_snapshot_odds(odds_path: str, sim_dir: str = "backtest/sims"):
    """Launch log_betting_evals.py for today and tomorrow using the provided odds snapshot."""

    today_str, tomorrow_str = get_date_strings()
    default_script = os.path.join("cli", "log_betting_evals.py")
    if not os.path.exists(default_script):
        default_script = "log_betting_evals.py"

    for date_str in [today_str, tomorrow_str]:
        eval_folder = os.path.join(sim_dir, date_str)

        if date_str == tomorrow_str:
            if not os.path.isdir(eval_folder) or not any(
                f.endswith(".json")
                for f in os.listdir(eval_folder)
                if os.path.isfile(os.path.join(eval_folder, f))
            ):
                logger.info("⏭ Skipping tomorrow's eval: sim data not ready yet")
                continue

        cmd = [
            PYTHON,
            default_script,
            f"--eval-folder={eval_folder}",
            f"--odds-path={odds_path}",
            f"--min-ev={MIN_EV}",
            "--debug",
            "--output-dir=logs",
        ]
        launch_process(f"LogBets {eval_folder}", cmd)


def run_unified_snapshot_and_dispatch(odds_path: str):
    """Generate a unified snapshot for today and tomorrow then dispatch alerts."""

    today_str, tomorrow_str = get_date_strings()
    date_arg = f"{today_str},{tomorrow_str}"

    exit_code = run_subprocess(
        [
            PYTHON,
            "core/unified_snapshot_generator.py",
            "--odds-path",
            odds_path,
            "--date",
            date_arg,
        ]
    )

    if exit_code != 0:
        logger.error(
            "❌ [%s] Unified snapshot generation failed (code %s); skipping dispatch.",
            now_eastern(),
            exit_code,
        )
        return

    for script in [
        "dispatch_live_snapshot.py",
        "dispatch_fv_drop_snapshot.py",
        "dispatch_best_book_snapshot.py",
        "dispatch_personal_snapshot.py",
        "dispatch_sim_only_snapshot.py",
    ]:
        cmd = [PYTHON, f"core/{script}", "--output-discord"]

        launch_process(script, cmd)

    # Dispatch CLV snapshot after other role-based snapshots
    clv_cmd = [PYTHON, "core/dispatch_clv_snapshot.py", "--output-discord"]
    launch_process("dispatch_clv_snapshot.py", clv_cmd)


logger.info(
    "🔄 [%s] Starting auto loop... "
    "(Sim: 30 min | Log & Snapshot Dispatch: 5 min, for today and tomorrow)",
    now_eastern(),
)

ensure_closing_monitor_running()
ensure_early_monitor_running()

logger.info(
    "🟢 [%s] First-time launch → fetching odds and dispatching logs",
    now_eastern(),
)
initial_odds = fetch_and_cache_odds_snapshot()
if initial_odds:
    last_snapshot_time = time.time()
    last_log_time = last_snapshot_time
    last_sim_time = last_snapshot_time
    run_logger(initial_odds)
    run_unified_snapshot_and_dispatch(initial_odds)

start_time = time.time()
loop_count = 0
if not initial_odds:
    last_log_time = start_time
    last_sim_time = start_time

while True:
    now = time.time()
    loop_count += 1

    logger.debug(
        "Loop tick → now: %s, log Δ: %.1f",
        now,
        now - last_log_time,
    )

    # Check on any active subprocesses
    poll_active_processes()

    monitor_restarted = ensure_closing_monitor_running()
    early_monitor_restarted = ensure_early_monitor_running()

    triggered_sim = False
    triggered_log = False

    if now - last_sim_time > SIM_INTERVAL:
        run_simulation()
        last_sim_time = now
        triggered_sim = True

    if now - last_log_time > LOG_INTERVAL:
        logger.info(
            "🟢 [%s] Fetching odds snapshot and dispatching logs", now_eastern()
        )
        odds_file = fetch_and_cache_odds_snapshot()
        if odds_file:
            last_snapshot_time = now
            run_logger(odds_file)
            run_unified_snapshot_and_dispatch(odds_file)
        last_log_time = now
        triggered_log = True

    uptime = str(timedelta(seconds=int(now - start_time)))
    timestamp = now_eastern().strftime("%Y-%m-%d %H:%M:%S")

    def next_in(last, interval):
        return seconds_to_readable(interval - (now - last))

    sim_msg = (
        "🟢 triggered"
        if triggered_sim
        else f"⏭ (next ~{next_in(last_sim_time, SIM_INTERVAL)})"
    )
    log_msg = (
        "🟢 triggered"
        if triggered_log
        else f"⏭ (next ~{next_in(last_log_time, LOG_INTERVAL)})"
    )
    monitor_msg = "restarted" if monitor_restarted else "OK"
    early_monitor_msg = "restarted" if early_monitor_restarted else "OK"

    logger.info(
        "\n🔁 [%s] Loop %d (uptime %s):\nSim %s | Log %s | Closing Monitor %s | Early Monitor %s",
        timestamp,
        loop_count,
        uptime,
        sim_msg,
        log_msg,
        monitor_msg,
        early_monitor_msg,
    )

    time.sleep(10)
