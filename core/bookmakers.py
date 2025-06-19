# core/bookmakers.py

BOOKMAKER_CATALOG = {
    "us": {
        "betonlineag": "BetOnline.ag",
        "betmgm": "BetMGM",
        "betrivers": "BetRivers",
        "betus": "BetUS",
        "bovada": "Bovada",
        "williamhill_us": "Caesars",
        "draftkings": "DraftKings",
        "fanatics": "Fanatics",
        "fanduel": "FanDuel",
        "lowvig": "LowVig.ag",
        "mybookieag": "MyBookie.ag"
    },
    "us2": {
        "ballybet": "Bally Bet",
        "betanysports": "BetAnySports",
        "betparx": "betPARX",
        "espnbet": "ESPN BET",
        "fliff": "Fliff",
        "hardrockbet": "Hard Rock Bet",
        "windcreek": "Wind Creek (Betfred PA)"
    },
    "us_ex": {
        "betopenly": "BetOpenly",
        "novig": "Novig",
        "prophetx": "ProphetX"
    },
    "us_dfs": {
        "prizepicks": "PrizePicks",
        "underdog": "Underdog Fantasy"
    },
    "uk": {
        "sport888": "888sport",
        "betfair_ex_uk": "Betfair Exchange",
        "betfair_sb_uk": "Betfair Sportsbook",
        "betvictor": "Bet Victor",
        "betway": "Betway",
        "boylesports": "BoyleSports",
        "casumo": "Casumo",
        "coral": "Coral",
        "grosvenor": "Grosvenor",
        "ladbrokes_uk": "Ladbrokes",
        "leovegas": "LeoVegas",
        "livescorebet": "LiveScore Bet",
        "matchbook": "Matchbook",
        "paddypower": "Paddy Power",
        "skybet": "Sky Bet",
        "smarkets": "Smarkets",
        "unibet_uk": "Unibet",
        "virginbet": "Virgin Bet",
        "williamhill": "William Hill (UK)"
    },
    "eu": {
        "onexbet": "1xBet",
        "sport888": "888sport",
        "betclic": "Betclic",
        "betanysports": "BetAnySports",
        "betfair_ex_eu": "Betfair Exchange",
        "betonlineag": "BetOnline.ag",
        "betsson": "Betsson",
        "betvictor": "Bet Victor",
        "coolbet": "Coolbet",
        "everygame": "Everygame",
        "gtbets": "GTbets",
        "marathonbet": "Marathon Bet",
        "matchbook": "Matchbook",
        "mybookieag": "MyBookie.ag",
        "nordicbet": "NordicBet",
        "pinnacle": "Pinnacle",
        "suprabets": "Suprabets",
        "tipico_de": "Tipico (DE)",
        "unibet_eu": "Unibet",
        "williamhill": "William Hill",
        "winamax_de": "Winamax (DE)",
        "winamax_fr": "Winamax (FR)"
    },
    "au": {
        "betfair_ex_au": "Betfair Exchange",
        "betr_au": "Betr",
        "betright": "Bet Right",
        "boombet": "BoomBet",
        "ladbrokes_au": "Ladbrokes",
        "neds": "Neds",
        "playup": "PlayUp",
        "pointsbetau": "PointsBet (AU)",
        "sportsbet": "SportsBet",
        "tab": "TAB",
        "tabtouch": "TABtouch",
        "unibet": "Unibet"
    }
}

def get_us_bookmakers(include_exchanges=False, include_dfs=False, include_secondary=False):
    keys = list(BOOKMAKER_CATALOG["us"].keys())
    if include_secondary:
        keys += BOOKMAKER_CATALOG["us2"].keys()
    if include_exchanges:
        keys += BOOKMAKER_CATALOG["us_ex"].keys()
    if include_dfs:
        keys += BOOKMAKER_CATALOG["us_dfs"].keys()
    return keys

def get_all_bookmaker_keys():
    return [key for region in BOOKMAKER_CATALOG.values() for key in region.keys()]

def get_bookmaker_label(book_key):
    for region in BOOKMAKER_CATALOG.values():
        if book_key in region:
            return region[book_key]
    return book_key  # fallback to key if not found