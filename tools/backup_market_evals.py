#!/usr/bin/env python3
"""Backup the logs/market_evals.csv file with a timestamp."""
import os
import shutil
from datetime import datetime


def backup_market_evals(src_path: str = "logs/market_evals.csv", backup_dir: str = "logs/backups") -> None:
    """Copy ``src_path`` to ``backup_dir`` with a datestamped filename."""
    if not os.path.exists(src_path):
        print(f"❌ Source not found: {src_path}")
        return

    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d")
    dest_path = os.path.join(backup_dir, f"market_evals_{timestamp}.csv")

    shutil.copy2(src_path, dest_path)
    print(f"✅ Backed up {src_path} → {dest_path}")


if __name__ == "__main__":
    backup_market_evals()

