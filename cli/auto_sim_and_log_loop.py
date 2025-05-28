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
from core.odds_fetcher import fetch_market_odds_from_api, save_market_odds_to_file

EDGE_THRESHOLD = 0.05
MIN_EV = 0.05
SIM_INTERVAL = 60 * 30      # Every 30 minutes
LOG_INTERVAL = 60 * 5       # Every 5 minutes
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
            encoding="utf-8",   # ✅ ensures emoji + UTF-8 output renders cleanly
            errors="replace",   # ✅ prevents crashes from odd characters
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
        script_path = os.path.join("cli", "closing_odds_monitor.py")
        if not os.path.exists(script_path):
            script_path = "closing_odds_monitor.py"
        logger.info("\n🎯 [%s] Starting closing odds monitor...", now_eastern())
        closing_monitor_proc = subprocess.Popen(
            [PYTHON, script_path], cwd=ROOT_DIR, env=os.environ
        )

def get_date_strings():
    now = now_eastern()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return today_str, tomorrow_str


def fetch_and_cache_odds_snapshot() -> str | None:
    """Fetch market odds once per loop and save to a timestamped file."""
    today_str, tomorrow_str = get_date_strings()
    game_ids = []
    for date_str in [today_str, tomorrow_str]:
        sim_dir = os.path.join("backtest", "sims", date_str)
        if os.path.isdir(sim_dir):
            for f in os.listdir(sim_dir):
                if f.endswith(".json"):
                    game_ids.append(f.replace(".json", ""))

    if not game_ids:
        logger.warning("⚠️ No game IDs found for odds fetch.")
        return None

    logger.info("\n📡 Fetching market odds for %s games...", len(game_ids))
    odds = fetch_market_odds_from_api(game_ids)
    timestamp = now_eastern().strftime("%Y%m%dT%H%M")
    tag = f"market_odds_{timestamp}"
    odds_path = save_market_odds_to_file(odds, tag)
    logger.info("✅ Saved shared odds snapshot: %s", odds_path)
    return odds_path

def run_simulation():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n🎯 [%s] Launching full slate simulation for %s...", now_eastern(), date_str)
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


def run_live_snapshot(odds_path: str | None = None):
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n📸 [%s] Running live snapshot generator for %s...", now_eastern(), date_str)
        logger.info("🔍 Diff highlighting enabled — comparing against last snapshot")
        # Determine the correct path for live_snapshot_generator
        default_script = os.path.join("core", "live_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "live_snapshot_generator.py"
        cmd = [
            PYTHON,
            default_script,
            f"--date={date_str}",
            f"--min-ev={MIN_EV}",
            "--diff-highlight",
            "--output-discord",
        ]
        if odds_path:
            cmd.append(f"--odds-path={odds_path}")
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started live snapshot subprocess for %s", date_str)



def run_personal_snapshot(odds_path: str | None = None):
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n📣 [%s] Running personal snapshot generator for %s...", now_eastern(), date_str)
        default_script = os.path.join("core", "personal_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "personal_snapshot_generator.py"
        cmd = [
            PYTHON,
            default_script,
            f"--date={date_str}",
            f"--min-ev={MIN_EV}",
            "--diff-highlight",
            "--output-discord",
        ]
        if odds_path:
            cmd.append(f"--odds-path={odds_path}")
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started personal snapshot subprocess for %s", date_str)

def run_best_book_snapshot(odds_path: str | None = None):
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n📚 [%s] Running best-book snapshot generator for %s...", now_eastern(), date_str)
        default_script = os.path.join("core", "best_book_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "best_book_snapshot_generator.py"
        cmd = [
            PYTHON,
            default_script,
            f"--date={date_str}",
            f"--min-ev={MIN_EV}",
            "--diff-highlight",
            "--output-discord",
        ]
        if odds_path:
            cmd.append(f"--odds-path={odds_path}")
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started best-book snapshot subprocess for %s", date_str)


def run_fv_drop_snapshot(odds_path: str | None = None):
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        logger.info("\n🔻 [%s] Running FV drop snapshot generator for %s...", now_eastern(), date_str)
        default_script = os.path.join("core", "fv_drop_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "fv_drop_snapshot_generator.py"
        cmd = [
            PYTHON,
            default_script,
            f"--date={date_str}",
            f"--min-ev={MIN_EV}",
            "--diff-highlight",
            "--output-discord",
        ]
        if odds_path:
            cmd.append(f"--odds-path={odds_path}")
        subprocess.Popen(cmd, cwd=ROOT_DIR, env=os.environ)
        logger.info("🚀 Started FV drop snapshot subprocess for %s", date_str)


logger.info(
    "🔄 Starting auto loop... "
    "(Sim: 30 min | Log & Snapshots (live, personal, best-book, FV drop): 5 min, for today and tomorrow)"
)

ensure_closing_monitor_running()

logger.info("🟢 First-time launch → triggering run_logger and all snapshots immediately")
run_logger()
initial_odds = fetch_and_cache_odds_snapshot()
run_live_snapshot(initial_odds)
run_personal_snapshot(initial_odds)
run_best_book_snapshot(initial_odds)
run_fv_drop_snapshot(initial_odds)

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
        run_live_snapshot(odds_file)
        run_personal_snapshot(odds_file)
        run_best_book_snapshot(odds_file)
        run_fv_drop_snapshot(odds_file)
        last_snapshot_time = now

    time.sleep(10)
