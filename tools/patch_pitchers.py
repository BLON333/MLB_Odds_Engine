import csv
import os
import shutil
from assets.stats_loader import normalize_name

PITCHERS_CSV = "data/Pitchers.csv"
PATCH_FILE = "data/estimated_hrfb_patch.csv"
OUTPUT_FILE = "Pitchers_patched.csv"
BACKUP_FILE = "data/Pitchers_backup.csv"

def load_patch_data(path):
    patch = {}
    try:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("Name", "").strip()
                rate = row.get("Estimated_HR_FB", "").strip()
                if name and rate:
                    try:
                        patch[normalize_name(name)] = round(float(rate), 4)
                    except ValueError:
                        continue
    except FileNotFoundError:
        print(f"[❌] Patch file not found: {path}")
        return {}
    return patch

def patch_pitchers_csv():
    patch_data = load_patch_data(PATCH_FILE)
    if not patch_data:
        print("[⚠️] No patch data loaded. Aborting.")
        return

    try:
        with open(PITCHERS_CSV, newline="", encoding="utf-8-sig", errors="replace") as infile:
            reader = csv.DictReader(infile)
            fieldnames = [col.strip() for col in reader.fieldnames]
            rows = list(reader)
    except FileNotFoundError:
        print(f"[❌] Pitchers.csv not found: {PITCHERS_CSV}")
        return

    if "Name" not in fieldnames or "HR/FB" not in fieldnames:
        print("[❌] Required columns 'Name' and 'HR/FB' not found in Pitchers.csv")
        return

    patched_count = 0
    skipped = 0
    updated_rows = []

    for row in rows:
        row = {k.strip(): v for k, v in row.items()}
        raw_name = row.get("Name", "").strip()
        norm_name = normalize_name(raw_name)
        hrfb = row.get("HR/FB", "").strip()

        if (not hrfb or hrfb in ["0", "0.0", "NaN"]) and norm_name in patch_data:
            row["HR/FB"] = patch_data[norm_name]
            patched_count += 1
        else:
            skipped += 1

        updated_rows.append(row)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig", errors="replace") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_rows)

    if os.path.exists(PITCHERS_CSV):
        shutil.copy(PITCHERS_CSV, BACKUP_FILE)
        shutil.move(OUTPUT_FILE, PITCHERS_CSV)
        print(f"[🔁] Created backup → {BACKUP_FILE}")
        print(f"[✅] Patched {patched_count} pitchers → overwritten {PITCHERS_CSV}")
    else:
        print(f"[✅] Patched {patched_count} pitchers → saved to {OUTPUT_FILE}")

    print(f"[ℹ️] Skipped {skipped} pitchers without matching patch data.")

if __name__ == "__main__":
    patch_pitchers_csv()




