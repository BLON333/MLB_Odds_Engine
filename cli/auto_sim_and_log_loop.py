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

import time
import subprocess


from datetime import timedelta
from utils import now_eastern
from core.odds_fetcher import fetch_all_market_odds, save_market_odds_to_file

EDGE_THRESHOLD = 0.05
MIN_EV = 0.05
SIM_INTERVAL = 60 * 30  # Every 30 minutes
LOG_INTERVAL = 60 * 5  # Every 5 minutes
SNAPSHOT_INTERVAL = 60 * 5  # Every 5 minutes

last_sim_time = 0
last_log_time = 0
last_snapshot_time = 0

# Track the closing odds monitor subprocess so we can restart if it exits
closing_monitor_proc = None


def run_subprocess(cmd):
    """Run a subprocess synchronously and log output."""
    timestamp = now_eastern()
    logger.info("\n%s", "═" * 60)
    logger.info("⚙️  [%s] Starting subprocess:", timestamp)
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
            env=os.environ,
        )

        if proc.stdout:
            logger.debug("📤 STDOUT:\n%s", proc.stdout)

        if proc.stderr:
            logger.debug("⚠️ STDERR:\n%s", proc.stderr)

        logger.info("\n✅ Subprocess completed with exit code %s", proc.returncode)
        return proc.returncode

    except subprocess.CalledProcessError as e:
        if e.stdout:
            logger.debug("📤 STDOUT (on error):\n%s", e.stdout)

        if e.stderr:
            logger.debug("⚠️ STDERR (on error):\n%s", e.stderr)

        logger.error("\n❌ Command %s exited with code %s", " ".join(cmd), e.returncode)
        return e.returncode


def ensure_closing_monitor_running():
    """Launch closing_odds_monitor.py if not already running."""
    global closing_monitor_proc
    if closing_monitor_proc is None or closing_monitor_proc.poll() is not None:
        logger.info("\n🎯 [%s] Starting closing odds monitor...", now_eastern())
        closing_monitor_proc = subprocess.Popen(
            [PYTHON, "cli/closing_odds_monitor.py"],
            cwd=ROOT_DIR,
            env=os.environ,
        )


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

    logger.info("\n📡 Fetching market odds for today and tomorrow...")
    odds = fetch_all_market_odds(lookahead_days=2)
    if odds is None:
        logger.error("❌ Failed to fetch market odds — skipping snapshot")
        return None

    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    tag = f"market_odds_{timestamp}"
    odds_path = save_market_odds_to_file(odds, tag)
    if odds_path:
        logger.info("✅ Saved shared odds snapshot: %s", odds_path)
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
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started simulation subprocess for %s", date_str)


def run_logger():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n📝 [%s] Launching log evals for %s...", now_eastern(), date_str)
        default_script = os.path.join("cli", "log_betting_evals.py")
        if not os.path.exists(default_script):
            default_script = "log_betting_evals.py"
        cmd = [
            PYTHON,
            default_script,
            "--eval-folder",
            f"backtest/sims/{date_str}",
            "--min-ev",
            str(MIN_EV),
        ]
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started log eval subprocess for %s", date_str)


def log_bets_with_snapshot_odds(odds_path: str, sim_dir: str = "backtest/sims"):
    """Launch log_betting_evals.py for today and tomorrow using the provided odds snapshot."""

    today_str, tomorrow_str = get_date_strings()
    default_script = os.path.join("cli", "log_betting_evals.py")
    if not os.path.exists(default_script):
        default_script = "log_betting_evals.py"

    for date_str in [today_str, tomorrow_str]:
        eval_folder = os.path.join(sim_dir, date_str)
        cmd = [
            PYTHON,
            default_script,
            f"--eval-folder={eval_folder}",
            f"--odds-path={odds_path}",
            f"--min-ev={MIN_EV}",
            "--debug",
            "--output-dir=logs",
        ]
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started log bets subprocess for %s", eval_folder)


def run_unified_snapshot_and_dispatch(odds_path: str):
    """Generate a unified snapshot for today and tomorrow then dispatch alerts."""

    today_str, tomorrow_str = get_date_strings()
    date_arg = f"{today_str},{tomorrow_str}"

    subprocess.run(
        [
            PYTHON,
            "core/unified_snapshot_generator.py",
            "--odds-path",
            odds_path,
            "--date",
            date_arg,
        ],
        cwd=ROOT_DIR,
        env=os.environ,
        check=True,
    )

    for script in [
        "dispatch_live_snapshot.py",
        "dispatch_fv_drop_snapshot.py",
        "dispatch_best_book_snapshot.py",
        "dispatch_personal_snapshot.py",
    ]:
        subprocess.Popen(
            [
                PYTHON,
                f"core/{script}",
                "--output-discord",
                "--diff-highlight",
            ],
            cwd=ROOT_DIR,
            env=os.environ,
        )


logger.info(
    "🔄 Starting auto loop... "
    "(Sim: 30 min | Log & Snapshot Dispatch: 5 min, for today and tomorrow)"
)

ensure_closing_monitor_running()

logger.info(
    "🟢 First-time launch → triggering run_logger and snapshot dispatch immediately"
)
run_logger()
initial_odds = fetch_and_cache_odds_snapshot()
if initial_odds:
    log_bets_with_snapshot_odds(initial_odds)
    run_unified_snapshot_and_dispatch(initial_odds)

while True:
    now = time.time()

    logger.debug(
        "Loop tick → now: %s, log Δ: %.1f, snap Δ: %.1f",
        now,
        now - last_log_time,
        now - last_snapshot_time,
    )

    ensure_closing_monitor_running()

    if now - last_sim_time > SIM_INTERVAL:
        run_simulation()
        last_sim_time = now

    if now - last_log_time > LOG_INTERVAL:
        logger.info("🟢 Triggering run_logger()")
        run_logger()
        last_log_time = now

    if now - last_snapshot_time > SNAPSHOT_INTERVAL:
        logger.info("🟢 Triggering snapshot scripts")
        odds_file = fetch_and_cache_odds_snapshot()
        if odds_file:
            log_bets_with_snapshot_odds(odds_file)
            run_unified_snapshot_and_dispatch(odds_file)
        last_snapshot_time = now

    time.sleep(10)