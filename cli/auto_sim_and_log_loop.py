import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

import time
import subprocess


from datetime import datetime, timedelta

EDGE_THRESHOLD = 0.05
MIN_EV = 0.05
SIM_INTERVAL = 60 * 30      # Every 30 minutes
LOG_INTERVAL = 60 * 5       # Every 5 minutes
SNAPSHOT_INTERVAL = 60 * 5  # Every 5 minutes

last_sim_time = 0
last_log_time = 0
last_snapshot_time = 0

def get_date_strings():
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    return today_str, tomorrow_str

def run_simulation():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n🎯 [{datetime.now()}] Launching full slate simulation for {date_str}...")
        cmd = f"python cli/full_slate_runner.py {date_str} --export-folder=backtest/sims --edge-threshold={EDGE_THRESHOLD}"
        subprocess.Popen(cmd, shell=True)


def run_logger():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📝 [{datetime.now()}] Launching log evals for {date_str}...")
        cmd = f"python log_betting_evals.py --eval-folder backtest/sims/{date_str} --min-ev {MIN_EV}"
        subprocess.Popen(cmd, shell=True)


def run_live_snapshot():
    today_str, tomorrow_str = get_date_strings()
    for date_str in [today_str, tomorrow_str]:
        print(f"\n📸 [{datetime.now()}] Running live snapshot generator for {date_str}...")
        print("🔍 Diff highlighting enabled — comparing against last snapshot")
        # Determine the correct path for live_snapshot_generator
        default_script = os.path.join("core", "live_snapshot_generator.py")
        if not os.path.exists(default_script):
            default_script = "live_snapshot_generator.py"
        cmd = f"python {default_script} --date={date_str} --min-ev={MIN_EV} --diff-highlight"
        subprocess.Popen(cmd, shell=True)


print(
    "🔄 Starting auto loop... "
    "(Sim: 30 min | Log & Snapshot: 5 min, for today and tomorrow)"
)

while True:
    now = time.time()

    if now - last_sim_time > SIM_INTERVAL:
        run_simulation()
        last_sim_time = now

    if now - last_log_time > LOG_INTERVAL:

        run_logger()
        last_log_time = now

    if now - last_snapshot_time > SNAPSHOT_INTERVAL:
        try:
            run_live_snapshot()
        except Exception as e:
            print(f"❌ Live snapshot failed: {e}")
        last_snapshot_time = now

    time.sleep(10)  # lightweight polling loop
