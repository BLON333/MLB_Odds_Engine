import json

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

def generate_empty_bullpen_json(out_path="data/reliever_depth_chart_2025-04-03.json"):
    template = {}
    for team_name in TEAM_NAME_TO_ABBR.keys():
        template[team_name] = [
            {
                "name": "Placeholder Reliever 1",
                "ip": 0.0,
                "role": "Unspecified"
            },
            {
                "name": "Placeholder Reliever 2",
                "ip": 0.0,
                "role": "Unspecified"
            }
        ]
    with open(out_path, "w") as f:
        json.dump(template, f, indent=2)
    print(f"[✅] Created template: {out_path}")

if __name__ == "__main__":
    generate_empty_bullpen_json()
