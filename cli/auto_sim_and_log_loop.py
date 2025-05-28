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
    """Run a subprocess synchronously and log output line-by-line."""
    timestamp = now_eastern()
    print(f"\n{'═'*60}")
    print(f"⚙️  [{timestamp}] Starting subprocess:")
    print(f"👉 {' '.join(cmd)}")
    print(f"{'═'*60}\n")

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
            print("📤 STDOUT:")
            print(proc.stdout)

        if proc.stderr:
            print("⚠️ STDERR:")
            print(proc.stderr)

        print(f"\n✅ Subprocess completed with exit code {proc.returncode}")
        return proc.returncode

    except subprocess.CalledProcessError as e:
        if e.stdout:
            print("📤 STDOUT (on error):")
            print(e.stdout)

        if e.stderr:
            print("⚠️ STDERR (on error):")
            print(e.stderr)

        print(f"\n❌ Command {' '.join(cmd)} exited with code {e.returncode}")
        return e.returncode


def ensure_closing_monitor_running():
    """Launch closing_odds_monitor.py if not already running."""
    global closing_monitor_proc
    if closing_monitor_proc is None or closing_monitor_proc.poll() is not None:
        script_path = os.path.join("cli", "closing_odds_monitor.py")
        if not os.path.exists(script_path):
            script_path = "closing_odds_monitor.py"
        print(f"\n🎯 [{now_eastern()}] Starting closing odds monitor...")
        closing_monitor_proc = subprocess.Popen(
            [PYTHON, script_path], cwd=ROOT_DIR, env=os.environ
        )

def get_date_strings():
    now = now_eastern()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return today_str, tomorrow_str

def run_simulation():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n🎯 [{now_eastern()}] Launching full slate simulation for {date_str}...")
        cmd = [
            PYTHON,
            os.path.join("cli", "full_slate_runner.py"),
            date_str,
            "--export-folder=backtest/sims",
            f"--edge-threshold={EDGE_THRESHOLD}",
        ]
        run_subprocess(cmd)



def run_logger():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📝 [{now_eastern()}] Launching log evals for {date_str}...")
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
        run_subprocess(cmd)


def run_live_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📸 [{now_eastern()}] Running live snapshot generator for {date_str}...")
        print("🔍 Diff highlighting enabled — comparing against last snapshot")
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
        run_subprocess(cmd)



def run_personal_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📣 [{now_eastern()}] Running personal snapshot generator for {date_str}...")
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
        run_subprocess(cmd)

def run_best_book_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📚 [{now_eastern()}] Running best-book snapshot generator for {date_str}...")
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
        run_subprocess(cmd)


def run_fv_drop_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n🔻 [{now_eastern()}] Running FV drop snapshot generator for {date_str}...")
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
        run_subprocess(cmd)


print(
    "🔄 Starting auto loop... "
    "(Sim: 30 min | Log & Snapshots (live, personal, best-book, FV drop): 5 min, for today and tomorrow)"
)

ensure_closing_monitor_running()

print("🟢 First-time launch → triggering run_logger and all snapshots immediately")
run_logger()
run_live_snapshot()
run_personal_snapshot()
run_best_book_snapshot()
run_fv_drop_snapshot()

while True:
    now = time.time()

    print(f"[DEBUG] Loop tick → now: {now}, log Δ: {now - last_log_time:.1f}, snap Δ: {now - last_snapshot_time:.1f}")

    ensure_closing_monitor_running()

    if now - last_sim_time > SIM_INTERVAL:
        run_simulation()
        last_sim_time = now

    if now - last_log_time > LOG_INTERVAL:
        print("🟢 Triggering run_logger()")
        run_logger()
        last_log_time = now

    if now - last_snapshot_time > SNAPSHOT_INTERVAL:
        print("🟢 Triggering snapshot scripts")
        run_live_snapshot()
        run_personal_snapshot()
        run_best_book_snapshot()
        run_fv_drop_snapshot()
        last_snapshot_time = now

    time.sleep(10)
