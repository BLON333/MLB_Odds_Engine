from core.config import DEBUG_MODE, VERBOSE_MODE
import csv
from collections import defaultdict
import argparse

from core.logger import get_logger
logger = get_logger(__name__)

SEGMENTS = ["mainline", "alt_line", "team_total", "derivative", "pk_equiv"]

def generate_recap(csv_path):
    counts = defaultdict(int)
    stakes = defaultdict(float)

    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = row.get("segment_label", "mainline")
                stake = float(row.get("stake", 0) or 0)
                counts[label] += 1
                stakes[label] += stake
    except FileNotFoundError:
        print(f"File not found: {csv_path}")
        return

    print("Segment Recap")
    for label in SEGMENTS:
        print(f"{label:10} | {counts[label]:3d} bets | {stakes[label]:.2f}u")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate bet recap by segment")
    p.add_argument("csv", nargs="?", default="logs/market_evals.csv")
    args = p.parse_args()
    generate_recap(args.csv)
