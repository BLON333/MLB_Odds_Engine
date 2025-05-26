import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

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

def ensure_closing_monitor_running():
    """Launch closing_odds_monitor.py if not already running."""
    global closing_monitor_proc
    if closing_monitor_proc is None or closing_monitor_proc.poll() is not None:
        script_path = os.path.join("cli", "closing_odds_monitor.py")
        if not os.path.exists(script_path):
            script_path = "closing_odds_monitor.py"
        print(f"\n🎯 [{now_eastern()}] Starting closing odds monitor...")
        closing_monitor_proc = subprocess.Popen(f"python {script_path}", shell=True)

def get_date_strings():
    now = now_eastern()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return today_str, tomorrow_str

def run_simulation():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n🎯 [{now_eastern()}] Launching full slate simulation for {date_str}...")
        cmd = f"python cli/full_slate_runner.py {date_str} --export-folder=backtest/sims --edge-threshold={EDGE_THRESHOLD}"
        subprocess.Popen(cmd, shell=True)


def run_logger():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📝 [{now_eastern()}] Launching log evals for {date_str}...")
        default_script = os.path.join("cli", "log_betting_evals.py")
        if not os.path.exists(default_script):
            default_script = "log_betting_evals.py"
        cmd = f"python {default_script} --eval-folder backtest/sims/{date_str} --min-ev {MIN_EV}"
        subprocess.Popen(cmd, shell=True)


def run_live_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📸 [{now_eastern()}] Running live snapshot generator for {date_str}...")
        print("🔍 Diff highlighting enabled — comparing against last snapshot")
        # Determine the correct path for live_snapshot_generator
        default_script = os.path.join("core", "live_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "live_snapshot_generator.py"
        cmd = f"python {default_script} --date={date_str} --min-ev={MIN_EV} --diff-highlight --output-discord"
        subprocess.Popen(cmd, shell=True)


def run_personal_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📣 [{now_eastern()}] Running personal snapshot generator for {date_str}...")
        default_script = os.path.join("core", "personal_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "personal_snapshot_generator.py"
        cmd = f"python {default_script} --date={date_str} --min-ev={MIN_EV} --diff-highlight --output-discord"
        subprocess.Popen(cmd, shell=True)


def run_best_book_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📚 [{now_eastern()}] Running best-book snapshot generator for {date_str}...")
        default_script = os.path.join("core", "best_book_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "best_book_snapshot_generator.py"
        cmd = f"python {default_script} --date={date_str} --min-ev={MIN_EV} --diff-highlight --output-discord"
        subprocess.Popen(cmd, shell=True)


def run_fv_drop_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n🔻 [{now_eastern()}] Running FV drop snapshot generator for {date_str}...")
        default_script = os.path.join("core", "fv_drop_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "fv_drop_snapshot_generator.py"
        cmd = f"python {default_script} --date={date_str} --min-ev={MIN_EV} --diff-highlight --output-discord"
        subprocess.Popen(cmd, shell=True)


print(
    "🔄 Starting auto loop... "
    "(Sim: 30 min | Log & Snapshots (live, personal, best-book, FV drop): 5 min, for today and tomorrow)"
)

ensure_closing_monitor_running()

while True:
    now = time.time()
    ensure_closing_monitor_running()

    if now - last_sim_time > SIM_INTERVAL:
        run_simulation()
        last_sim_time = now

    if now - last_log_time > LOG_INTERVAL:

        run_logger()
        last_log_time = now

    if now - last_snapshot_time > SNAPSHOT_INTERVAL:
        try:
            run_live_snapshot()
            run_personal_snapshot()
            run_best_book_snapshot()
            run_fv_drop_snapshot()
        except Exception as e:
            print(f"❌ Live snapshot failed: {e}")
        last_snapshot_time = now

    time.sleep(10)  # lightweight polling loop
