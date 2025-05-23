
import time
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from assets.stats_loader import normalize_name
from datetime import datetime
import argparse

# Map full team names to official MLB abbreviations
TEAM_ABBR_MAP = {
    "angels": "LAA",
    "astros": "HOU",
    "athletics": "OAK",
    "blue jays": "TOR",
    "braves": "ATL",
    "brewers": "MIL",
    "cardinals": "STL",
    "cubs": "CHC",
    "diamondbacks": "ARI",
    "dodgers": "LAD",
    "giants": "SF",
    "guardians": "CLE",
    "mariners": "SEA",
    "marlins": "MIA",
    "mets": "NYM",
    "nationals": "WSH",
    "orioles": "BAL",
    "padres": "SD",
    "phillies": "PHI",
    "pirates": "PIT",
    "rangers": "TEX",
    "rays": "TB",
    "reds": "CIN",
    "red sox": "BOS",
    "rockies": "COL",
    "royals": "KC",
    "tigers": "DET",
    "twins": "MIN",
    "white sox": "CHW",
    "yankees": "NYY"
}

def fuzzy_team_match(name):
    name_clean = re.sub(r"[^\w\s]", "", name).lower().strip()

    if name_clean in TEAM_ABBR_MAP:
        return TEAM_ABBR_MAP[name_clean]

    for key, abbr in TEAM_ABBR_MAP.items():
        if key in name_clean:
            return abbr

    print(f"⚠️ Unmatched team name: '{name}' — using fallback abbreviation.")
    return name.strip()[:3].upper()

def fetch_lineups_selenium(for_date=None):
    from selenium.common.exceptions import WebDriverException

    if not for_date:
        for_date = datetime.today().strftime("%Y-%m-%d")

    url = f"https://fantasydata.com/mlb/daily-lineups?date={for_date}"
    print(f"🌐 Loading FantasyData daily lineups for: {for_date}")

    from webdriver_manager.chrome import ChromeDriverManager

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    # Auto-download the compatible ChromeDriver version
    service = Service(ChromeDriverManager().install())


    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(url)
        time.sleep(8)

        if "Daily MLB Lineups" not in driver.page_source:
            print("⚠️ Page may not have fully loaded — content check failed.")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()
    except WebDriverException as e:
        print(f"❌ Selenium failed to load page: {e}")
        return {}

    matchups = soup.select("#lineups > div")
    if not matchups:
        print("⚠️ No matchups found — selector may have changed.")
    else:
        print(f"Found {len(matchups)} matchup blocks.\n")

    lineups = {}

    for matchup in matchups:
        header = matchup.select_one(".header .info div")
        if not header or "@" not in header.get_text():
            continue

        away_team, home_team = header.get_text(strip=True).split("@")
        away_abbr = fuzzy_team_match(away_team)
        home_abbr = fuzzy_team_match(home_team)

        def extract_batters(team_div):
            batters = []
            rows = team_div.find_all("div", recursive=False)
            for row in rows:
                row_text = row.get_text(" ", strip=True)
                match = re.search(r"^\d+\.\s*(.*?)\s+\((R|L|S)\)", row_text)
                if match:
                    name = normalize_name(match.group(1).strip())
                    hand = match.group(2)
                    batters.append({"name": name, "handedness": hand})
                if len(batters) == 9:
                    break
            return batters

        away_block = matchup.select_one("div.lineup > div.away")
        home_block = matchup.select_one("div.lineup > div.home")

        if away_block:
            away_batters = extract_batters(away_block)
            lineups[away_abbr] = away_batters
            print(f"Away lineup for {away_abbr}: {len(away_batters)} batters")
            if len(away_batters) < 9:
                print(f"⚠️ Incomplete lineup: {away_abbr} has only {len(away_batters)} batters")

        if home_block:
            home_batters = extract_batters(home_block)
            lineups[home_abbr] = home_batters
            print(f"Home lineup for {home_abbr}: {len(home_batters)} batters")
            if len(home_batters) < 9:
                print(f"⚠️ Incomplete lineup: {home_abbr} has only {len(home_batters)} batters")

    print(f"\n✅ Finished scraping lineups. Final type: {type(lineups)} | Keys: {list(lineups.keys())}")

    if not isinstance(lineups, dict):
        raise TypeError(f"❌ Expected dict return, got {type(lineups)}")

    return lineups

# CLI test
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape MLB daily lineups from FantasyData")
    parser.add_argument("--date", help="Date in YYYY-MM-DD format", default=None)
    args = parser.parse_args()

    lineups = fetch_lineups_selenium(for_date=args.date)
    for team, batters in lineups.items():
        print(f"\n{team} Lineup:")
        for i, b in enumerate(batters, 1):
            print(f"  {i}. {b['name']} ({b['handedness']})")

    print(f"\n✅ Finished scraping lineups. Final type: {type(lineups)} | Keys: {list(lineups.keys())}")

    if not isinstance(lineups, dict):
        raise TypeError(f"❌ Expected lineups to be dict, got {type(lineups)}")
