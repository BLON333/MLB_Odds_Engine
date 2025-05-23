import csv
import json
import re
import unicodedata
from collections import defaultdict

TEAM_NAME_TO_ABBR = {
    "Diamondbacks": "ARI", "Braves": "ATL", "Orioles": "BAL", "Red Sox": "BOS",
    "White Sox": "CWS", "Cubs": "CHC", "Reds": "CIN", "Guardians": "CLE",
    "Rockies": "COL", "Tigers": "DET", "Astros": "HOU", "Royals": "KC",
    "Angels": "LAA", "Dodgers": "LAD", "Marlins": "MIA", "Brewers": "MIL",
    "Twins": "MIN", "Mets": "NYM", "Yankees": "NYY", "Athletics": "OAK",
    "Phillies": "PHI", "Pirates": "PIT", "Padres": "SD", "Giants": "SF",
    "Mariners": "SEA", "Cardinals": "STL", "Rays": "TB", "Rangers": "TEX",
    "Blue Jays": "TOR", "Nationals": "WSH"
}

TEAM_ALIAS_MAP = {
    "Sox": "Red Sox",
    "Jays": "Blue Jays",
    "Yanks": "Yankees",
    "NYY": "Yankees",
    "BOS": "Red Sox",
    "TOR": "Blue Jays",
    # Add more aliases if needed
}

MIN_IP = 0  # Set to 10 if you want to exclude very low-usage relievers

def assign_roles_by_ip(relievers):
    sorted_relievers = sorted(relievers, key=lambda r: r["ip"], reverse=True)
    for i, r in enumerate(sorted_relievers):
        if i == 0:
            r["role"] = "Closer"
        elif i <= 2:
            r["role"] = "Setup"
        else:
            r["role"] = "Middle"
    return sorted_relievers

def clean_team_header(header):
    # Normalize unicode (fix BOMs, smart quotes, accents, etc.)
    normalized = unicodedata.normalize("NFKD", header)
    cleaned = re.sub(r"^#\s*\d+\s*", "", normalized)
    cleaned = re.sub(r"[^\w\s]", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

def convert_csv_to_bullpen_json(csv_path, output_path):
    team_bullpens = defaultdict(list)
    current_team = None

    with open(csv_path, newline='', encoding='utf-8-sig', errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue

            first_cell = row[0].strip()

            if first_cell.startswith("#"):
                team_name_raw = clean_team_header(first_cell)
                canonical_name = TEAM_ALIAS_MAP.get(team_name_raw, team_name_raw)

                if canonical_name in TEAM_NAME_TO_ABBR:
                    current_team = canonical_name
                else:
                    print(f"[⚠️] Unrecognized team: {repr(team_name_raw)}")
                    current_team = None
                continue

            if first_cell.lower() in {"name", "total", "team"} or current_team is None:
                continue

            try:
                name = unicodedata.normalize("NFKD", first_cell.strip()).encode("ascii", "ignore").decode()
                ip = float(row[1].strip())

                if ip >= MIN_IP:
                    team_bullpens[current_team].append({
                        "name": name,
                        "ip": ip,
                        "role": "Unspecified"
                    })
            except (ValueError, IndexError):
                print(f"[⚠️] Skipped bad row for {current_team}: {row}")

    # Assign roles
    for team, relievers in team_bullpens.items():
        team_bullpens[team] = assign_roles_by_ip(relievers)

    with open(output_path, "w", encoding="utf-8") as outfile:
        json.dump(team_bullpens, outfile, indent=2)

    print(f"[✅] Exported bullpen JSON to {output_path}")

if __name__ == "__main__":
    convert_csv_to_bullpen_json("data/relievers.csv", "data/reliever_depth_chart_2025-04-03.json")



