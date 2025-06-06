from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import json
import traceback

UNMATCHED_MARKET_LOOKUPS = defaultdict(list)

# Timezone helpers
try:
    EASTERN_TZ = ZoneInfo("US/Eastern")
except ZoneInfoNotFoundError:
    print("⚠️ 'US/Eastern' timezone not found, trying 'America/New_York'.")
    try:
        EASTERN_TZ = ZoneInfo("America/New_York")
    except ZoneInfoNotFoundError:
        print("⚠️ Falling back to UTC timezone.")
        EASTERN_TZ = ZoneInfo("UTC")

def now_eastern() -> datetime:
    """Return the current time in US/Eastern timezone."""
    return datetime.now(EASTERN_TZ)

def to_eastern(dt: datetime) -> datetime:
    """Convert an aware or naive UTC datetime to US/Eastern."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(EASTERN_TZ)


def safe_load_json(path: str):
    """Load JSON from ``path`` with helpful diagnostics on failure."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as e:
        print(f"\u274c JSON decode error in {path}: {e}")
        try:
            with open(path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
            start = max(e.lineno - 2, 0)
            end = min(e.lineno + 1, len(lines))
            context = ''.join(f"{i+1}: {line}" for i, line in enumerate(lines[start:end], start))
            print(f"Context around line {e.lineno}:\n{context}")
        except Exception as ctx_err:
            print(f"⚠️ Could not read context for {path}: {ctx_err}")
        print("🔧 Ensure commas separate all objects and arrays correctly.")
        return None
    except Exception:
        print(f"\u274c Failed to load JSON from {path}\n{traceback.format_exc()}")
        return None

TEAM_ABBR_FIXES = {
    "CHW": "CWS", "WSN": "WSH", "KCR": "KC", "TBD": "TB", "ATH": "OAK"
}

TEAM_ABBR = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH"
}

TEAM_NAME_TO_ABBR = {k.title(): v.upper() for k, v in TEAM_ABBR.items()}
TEAM_ABBR_TO_NAME = {v.upper(): k.title() for k, v in TEAM_ABBR.items()}

# Mapping for normalizing non-standard team abbreviations encountered in
# simulation files or other data sources.
TEAM_FIXES = {
    "ATH": "OAK",
    "WSN": "WSH",
    "CHW": "CWS",
    "KCR": "KC",
    "TBD": "TB",
}


def merge_offers_with_alternates(offers: dict, alt_map: dict = None) -> dict:
    """
    Merge alternate market lines into their base market equivalents for normalization.
    Example: 'alternate_spreads' merged into 'spreads'.
    """
    if alt_map is None:
        alt_map = {
            "spreads": ["spreads", "alternate_spreads"],
            "totals": ["totals", "alternate_totals"],
            "h2h": ["h2h"],
            "team_totals": ["team_totals", "alternate_team_totals"],
            "spreads_1st_1_innings": ["spreads_1st_1_innings", "alternate_spreads_1st_1_innings"],
            "spreads_1st_3_innings": ["spreads_1st_3_innings", "alternate_spreads_1st_3_innings"],
            "spreads_1st_5_innings": ["spreads_1st_5_innings", "alternate_spreads_1st_5_innings"],
            "spreads_1st_7_innings": ["spreads_1st_7_innings", "alternate_spreads_1st_7_innings"],
            "totals_1st_1_innings": ["totals_1st_1_innings", "alternate_totals_1st_1_innings"],
            "totals_1st_3_innings": ["totals_1st_3_innings", "alternate_totals_1st_3_innings"],
            "totals_1st_5_innings": ["totals_1st_5_innings", "alternate_totals_1st_5_innings"],
            "totals_1st_7_innings": ["totals_1st_7_innings", "alternate_totals_1st_7_innings"],
            "team_totals_1st_1_innings": ["team_totals_1st_1_innings", "alternate_team_totals_1st_1_innings"],
            "team_totals_1st_3_innings": ["team_totals_1st_3_innings", "alternate_team_totals_1st_3_innings"],
            "team_totals_1st_5_innings": ["team_totals_1st_5_innings", "alternate_team_totals_1st_5_innings"],
            "team_totals_1st_7_innings": ["team_totals_1st_7_innings", "alternate_team_totals_1st_7_innings"],
        }

    merged = {}
    for canonical_key, source_keys in alt_map.items():
        merged[canonical_key] = {}
        for key in source_keys:
            market_data = offers.get(key, {})
            for book, lines in market_data.items():
                merged[canonical_key].setdefault(book, {}).update(lines)

    return merged


def merge_book_sources_for(market_key, offers):
    def try_get(k):
        return offers.get(k, {})

    alt_map = {
        "spreads": ["spreads", "alternate_spreads"],
        "totals": ["totals", "alternate_totals"],
        "team_totals": ["team_totals", "alternate_team_totals"],
        "spreads_1st_5_innings": ["spreads_1st_5_innings", "alternate_spreads_1st_5_innings"],
        "totals_1st_7_innings": ["totals_1st_7_innings", "alternate_totals_1st_7_innings"],
        "team_totals_1st_1_innings": ["team_totals_1st_1_innings", "alternate_team_totals_1st_1_innings"],
        "team_totals_1st_3_innings": ["team_totals_1st_3_innings", "alternate_team_totals_1st_3_innings"],
        "team_totals_1st_5_innings": ["team_totals_1st_5_innings", "alternate_team_totals_1st_5_innings"],
        "team_totals_1st_7_innings": ["team_totals_1st_7_innings", "alternate_team_totals_1st_7_innings"],
        # Add more if needed
    }

    merged = {}
    for key in alt_map.get(market_key, [market_key]):
        for label, book_data in try_get(f"{key}_source").items():
            norm = normalize_label(label).strip()
            merged.setdefault(norm, {}).update(book_data)
    return merged


def find_sim_entry(sim_markets: list, target_market_key: str, side_label: str, allow_fallback: bool = False) -> dict | None:
    """
    Retrieves a simulation entry for a given market+side with strict segment match.
    If no match is found, can optionally return diagnostic fallback candidates from other segments.

    Args:
        sim_markets: list of simulation entries
        target_market_key: e.g. 'spreads', 'totals_1st_5_innings'
        side_label: the side (e.g. 'LAA +1.5')
        allow_fallback: if True, will return a list of candidates across other segments

    Returns:
        dict (single match) or list (fallback options) or None
    """
    from utils import normalize_label, classify_market_segment

    normalized_target = normalize_label(side_label)
    target_segment = classify_market_segment(target_market_key)

    strict_matches = []
    loose_matches = []

    for entry in sim_markets:
        if not isinstance(entry, dict):
            continue

        label = normalize_label(entry.get("side", ""))
        market = entry.get("market", "")
        if not market:
            continue

        if label == normalized_target:
            seg = classify_market_segment(market)
            if seg == target_segment:
                strict_matches.append(entry)
            else:
                loose_matches.append((entry, seg))

    if strict_matches:
        return strict_matches[0]

    if allow_fallback and loose_matches:
        from core.logger import get_logger
        logger = get_logger(__name__)
        logger.debug("⚠️ Fallback: Sim value for '%s' found in other segments:", side_label)
        for fallback_entry, seg in loose_matches:
            logger.debug("   → %s | P=%.4f | Segment=%s", fallback_entry['market'], fallback_entry['sim_prob'], seg)
        return [e for e, _ in loose_matches]  # return list for inspection

    return None




def get_normalized_lookup_side(label: str, market_key: str) -> str:
    """
    Normalize the label for a specific market.
    Ensures spread/h2h markets are abbreviated;
    totals/team totals are left alone unless unclean.
    """
    label = label.strip().replace("+", " +").replace("-", " -").replace("  ", " ")

    if label.split()[0] in TEAM_NAME_TO_ABBR:
        clean = label
    else:
        clean = standardize_derivative_label(label)

    if market_key in {"spreads", "h2h"}:
        return normalize_to_abbreviation(clean)

    return clean


def assert_segment_match(sim_market_key: str, matched_market_key: str) -> bool:
    """
    Confirms that both simulation and matched market are from the same segment.
    Raises warning or returns False if mismatched.
    """
    from utils import classify_market_segment

    sim_segment = classify_market_segment(sim_market_key)
    book_segment = classify_market_segment(matched_market_key)

    if sim_segment != book_segment:
        from core.logger import get_logger
        logger = get_logger(__name__)
        logger.debug(
            "❌ [SEGMENT MISMATCH] Sim: %s (%s) ≠ Book: %s (%s)",
            sim_segment,
            sim_market_key,
            book_segment,
            matched_market_key,
        )
        return False

    from core.logger import get_logger
    logger = get_logger(__name__)
    logger.debug(
        "✅ Segment match: %s (%s) == %s (%s)",
        sim_segment,
        sim_market_key,
        book_segment,
        matched_market_key,
    )
    return True


def convert_full_team_spread_to_odds_key(label: str) -> str:
    """
    Converts 'Arizona Diamondbacks +1.5' → 'ARI +1.5'
    For lookup in market odds files.
    """
    label = label.strip()
    parts = label.rsplit(" ", 1)
    if len(parts) == 2:
        team_name, spread = parts
        abbr = TEAM_ABBR.get(team_name)
        if abbr:
            return f"{abbr} {spread}"
    return label

def get_segment_from_market(market: str) -> str:
    if "_" in market and any(x in market for x in ["1st", "3_innings", "5_innings", "7_innings"]):
        return "derivative"
    return "full_game"  # ✅ renamed from 'mainline'


def extract_segment_suffix(market_key: str) -> str:
    """Return the segment portion of a market key."""
    parts = market_key.split("_")
    return "_".join(parts[1:]) if len(parts) > 1 else ""


def get_segment_label(market_key: str, side: str) -> str:
    """Return high level segment label for routing and summaries."""
    m = market_key.lower()
    label = side.strip()

    if "team_totals" in m:
        return "team_total"

    if "alternate_" in m:
        base = m.replace("alternate_", "")
        # Alternate derivatives still considered derivative
        if any(x in base for x in ["1st", "3_innings", "5_innings", "7_innings"]):
            return "derivative"
        return "alt_line"

    if any(x in m for x in ["1st", "3_innings", "5_innings", "7_innings"]):
        return "derivative"

    if m.startswith("spreads"):
        try:
            point = float(label.split()[-1])
            if abs(point) == 0:
                return "pk_equiv"
        except Exception:
            pass

    return "mainline"




def normalize_to_abbreviation(label: str) -> str:
    """
    Normalize full team name label to abbreviation.
    - 'Washington Nationals -1.5' → 'WSH -1.5'
    - Leaves 'Over 8.5' or 'Under 4.5' untouched
    """
    label = label.strip().replace("+", " +").replace("-", " -").replace("  ", " ")

    if label.startswith(("Over", "Under")):
        return label

    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if label.startswith(full_name + " ") or label == full_name:
            rest = label[len(full_name):].strip()
            return f"{abbr} {rest}".strip() if rest else abbr

    return label  # fallback


def normalize_line_label(label: str):
    """Extract prefix and numeric line value from a label string."""
    if not isinstance(label, str):
        return None, None

    cleaned = label.strip().replace("+", " +").replace("-", " -")
    parts = cleaned.split()
    if not parts:
        return None, None

    if parts[0].lower() in {"over", "under"}:
        prefix = parts[0].capitalize()
        try:
            value = float(parts[1])
        except (IndexError, ValueError):
            value = None
        return prefix, value

    prefix = normalize_to_abbreviation(parts[0])
    try:
        value = float(parts[-1])
    except (ValueError, IndexError):
        value = None
    return prefix.upper(), value



def build_point_str(point, market_key=None):
    """Return formatted point string for a given market."""
    try:
        value = float(point)
    except (TypeError, ValueError):
        return ""

    if market_key and "spreads" in market_key:
        return f"{value:+.1f}".replace("+0.0", "0.0").replace("-0.0", "0.0")

    return f"{value:.1f}"


def build_full_label(normalized_label, market_key, point):
    """Construct a standardized betting label."""
    mkey = market_key.lower()
    point_str = build_point_str(point, mkey)

    if point_str:
        if "team_totals" in mkey:
            parts = normalized_label.split()
            if len(parts) >= 2:
                team, side = parts[0], parts[1]
                return f"{team} {side} {point_str}"
            return f"{normalized_label} {point_str}".strip()
        if "totals" in mkey:
            side = normalized_label.split()[0]
            return f"{side} {point_str}"
        if "spreads" in mkey:
            base = normalized_label.split()[0].strip()
            return f"{base} {point_str}"

    return normalized_label.strip()

def classify_market_segment(market_key: str) -> str:
    """
    Returns the segment type of a given market key.

    Examples:
        "totals"                       → "full_game"
        "spreads_1st_5_innings"       → "F5"
        "totals_1st_3_innings"        → "F3"
        "h2h_1st_1_innings"           → "F1"
        "spreads_1st_7_innings"       → "F7"
        "team_totals"                 → "full_game"
    """
    if "_" not in market_key:
        return "full_game"

    if "1st_1_innings" in market_key:
        return "F1"
    elif "1st_3_innings" in market_key:
        return "F3"
    elif "1st_5_innings" in market_key:
        return "F5"
    elif "1st_7_innings" in market_key:
        return "F7"
    else:
        return "full_game"  # fallback for team_totals or odd edge cases




def fallback_source(label, price):
    """
    Provides a fallback source mapping if no books matched for a given label.
    Used to ensure *_source fields are never empty.
    """
    return {label: {"fallback": price}}


def print_market_debug(market_key, label, price, books):
    """Logs formatted debug message when consensus pairing fails."""
    from core.logger import get_logger
    logger = get_logger(__name__)
    logger.debug("⚠️ No consensus match for %s | %s", market_key, label)
    logger.debug("   ➤ Using fallback price: %s", price)
    if books:
        logger.debug("   🏦 Books considered: %s", ", ".join(books))


def get_contributing_books(market_odds, market_key, lookup_side):
    """
    Returns a dictionary of books and their prices that contributed to a specific market label.
    """
    source_key = f"{market_key}_source"
    return market_odds.get(source_key, {}).get(lookup_side, {})


import unicodedata

def clean_book_prices(book_dict):
    """
    Sanitize a dict of {book: odds}, ensuring all odds are float-compatible.
    Filters out invalid values like None, empty strings, or non-numeric types.
    Converts odds like '+290' to 290.0.
    """
    cleaned = {}
    for book, price in book_dict.items():
        try:
            if isinstance(price, str):
                price = price.replace("+", "")
            price = float(price)
            cleaned[book] = price
        except (ValueError, TypeError):
            continue
    return cleaned


def normalize_segment_name(segment: str) -> str:
    """
    Normalizes segment names like 'First Inning' or '1st Inning' to '1st_1_innings'.
    Leaves plural segment names like '1st_3_innings' untouched.
    """
    segment_clean = (
        segment.lower()
            .replace("first", "1st")
            .replace(" ", "_")
    )

    if segment_clean in {"1st_inning", "1st_1_inning"}:
        return "1st_1_innings"

    return segment_clean



def normalize_lookup_side(side):
    """
    Normalize side label for odds dictionary lookup:
    - Expands abbreviations like 'TEX+0.5' → 'Texas Rangers +0.5'
    - Leaves Over/Under labels unchanged
    """
    if not isinstance(side, str):
        return side

    tokens = side.split()
    if len(tokens) == 3 and tokens[1] in {"Over", "Under"}:
        abbr = tokens[0]
        rest = "".join(tokens[1:])  # → "Over4.5"
        return f"{abbr}{rest}"

    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if side.startswith(abbr):
            suffix = side[len(abbr):].strip()
            return f"{full_name} {suffix}".strip()

    return side.strip()


def normalize_name(name):
    """
    Normalize a player name by:
    - Lowercasing, stripping whitespace, removing punctuation and accents.
    - Converts "Last, First" → "first last".
    """
    if not isinstance(name, str):
        return ""

    name = name.strip().lower()

    # ✅ Remove accents with Unicode normalization
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")

    for orig, repl in [(".", ""), ("’", ""), ("‘", ""), ("-", " "), ("_", " ")]:
        name = name.replace(orig, repl)

    if "," in name:
        parts = name.split(",", 1)
        name = f"{parts[1].strip()} {parts[0].strip()}"

    tokens = name.split()
    suffixes = {"jr", "ii", "iii"}
    if tokens and tokens[-1] in suffixes:
        tokens = tokens[:-1]

    particles = {"de", "del", "la", "van", "von", "da", "du"}
    if len(tokens) > 2 and tokens[1] in particles:
        return tokens[0] + " " + " ".join(tokens[1:])
    elif len(tokens) >= 2:
        return tokens[0] + " " + tokens[-1]
    else:
        return name


import re

def normalize_label(label):
    if not isinstance(label, str):
        return label

    import re
    from utils import TEAM_ABBR  # Ensure this is in scope
    label = re.sub(r'\s+', ' ', label.strip().replace("−", "-"))  # collapse whitespace

    # Handle totals: "Over 8.5", "Under9", "Over8.0"
    if label.lower().startswith("over") or label.lower().startswith("under"):
        tokens = label.split()
        if len(tokens) == 2:
            side, val = tokens
            try:
                return f"{side.title()} {float(val):.1f}"
            except:
                return label
        m = re.match(r"^(Over|Under)([0-9\.]+)$", label, re.IGNORECASE)
        if m:
            return f"{m.group(1).title()} {float(m.group(2)):.1f}"
        return label.title()

    # Normalize spreads: "Milwaukee Brewers +1.5" → "MIL +1.5"
    for team_name, abbr in TEAM_ABBR.items():
        if label.startswith(team_name):
            suffix = label[len(team_name):].strip()
            return f"{abbr.upper()} {suffix}"

    # Catch fallback: if already abbreviated form, standardize case
    parts = label.split()
    if len(parts) == 2 and parts[0].upper() in TEAM_ABBR.values():
        return f"{parts[0].upper()} {parts[1]}"

    return label



def standardize_derivative_label(label):
    """
    Normalize label for derivative markets (like 'Over 0.5' or 'PIT +0.5')
    """
    if not isinstance(label, str):
        return label

    label = label.strip().replace("+", " +").replace("-", " -").replace("  ", " ")

    if label.lower() in {"score ≥1 run", "score >0", "score at least 1"}:
        return "Over 0.5"
    if label.lower() in {"score <1", "score = 0", "score 0 runs"}:
        return "Under 0.5"

    if label.endswith(" win"):
        abbr = label.replace(" win", "").strip()
        return TEAM_ABBR_TO_NAME.get(abbr, abbr)

    if label.startswith("Run line (") and label.endswith(")"):
        inside = label[len("Run line ("):-1]
        parts = inside.split()
        if len(parts) == 2:
            abbr, spread = parts
            full_name = TEAM_ABBR_TO_NAME.get(abbr, abbr)
            return f"{full_name} {spread}".strip()
        return inside

    if label.startswith("Total >"):
        return f"Over {label.split('>')[1].strip()}"
    if label.startswith("Total <"):
        return f"Under {label.split('<')[1].strip()}"

    for abbr, full_name in TEAM_ABBR_TO_NAME.items():
        if label.startswith(abbr + " "):
            rest = label[len(abbr):].strip()
            return f"{full_name} {rest}".strip()


    return label



def remap_side_key(side):
    side = side.strip()
    side = side.replace("+", " +").replace("-", " -").replace("  ", " ")

    for team_name, abbr in TEAM_NAME_TO_ABBR.items():
        if side == team_name:
            return abbr
        elif side.startswith(team_name + " "):
            suffix = side[len(team_name):].strip()
            return f"{abbr} {suffix}".strip()

    if side.split()[0] in {"Over", "Under"} and len(side.split()) == 2:
        return side  # already in normalized Over/Under format

    return side  # fallback



def trim_duplicate_suffix(key):
    parts = key.split()
    if len(parts) >= 3 and parts[-1] == parts[-2]:
        return " ".join(parts[:-1])
    return key


def format_market_key(row):
    market = row["market"].strip()
    side = row["side"].strip()

    if market == "moneyline":
        return ("h2h", side)
    elif market == "runline":
        parts = side.replace("+", " +").replace("-", " -").split()
        if len(parts) == 2:
            return ("spreads", f"{normalize_team_name(parts[0])} {parts[1]}")
        return ("spreads", side)
    elif market == "total":
        return ("totals", side.capitalize())
    elif market.startswith("First") or market.startswith("1st"):
        label = market.replace("First ", "1st ").lower().replace(" ", "_")
        if "run line" in side.lower():
            val = side.replace("Run line (", "").replace(")", "")
            parts = val.replace("+", " +").replace("-", " -").split()
            if len(parts) == 2:
                return (f"spreads_{label}", f"{normalize_team_name(parts[0])} {parts[1]}")
            return (f"spreads_{label}", val)
        elif "over" in side.lower() or "under" in side.lower():
            return (f"totals_{label}", side.capitalize())
        else:
            return (f"h2h_{label}", side)
    else:
        return (market, side)

def build_entry(market, side, prob, odds, ev=0, source="simulator"):
    return {
        "market":     market,
        "side":       side,
        "sim_prob":   round(prob, 4),
        "fair_odds":  round(odds, 2),
        "ev_percent": round(ev, 2),
        "source":     source
    }



def get_combined_market(game_odds: dict, base_key: str) -> dict:
    """
    Returns a combined dict of market odds from base and alternate markets.
    Example: base_key='spreads' returns merged data from 'spreads' and 'alternate_spreads'.
    """
    alt_key = f"alternate_{base_key}"
    combined = {}

    for key in (base_key, alt_key):
        market = game_odds.get(key, {})
        if isinstance(market, dict):
            combined.update(market)

    return combined




def canonical_label(label):
    """
    Canonicalize a label:
    - Lowercase
    - Remove spaces
    """
    if not isinstance(label, str):
        return label
    return label.replace(" ", "").lower()

def get_contributing_books(market_odds, market_key, lookup_side):
    """
    Returns a dictionary of books and their prices that contributed to a specific market label.
    Tries primary, alternate, and team_ market sources.
    """
    sources = [
        f"{market_key}_source",
        f"alternate_{market_key}_source",
        f"team_{market_key}_source"
    ]
    
    for source_key in sources:
        if lookup_side in market_odds.get(source_key, {}):
            return market_odds[source_key][lookup_side]
    
    return {}



def get_market_entry_with_alternate_fallback(market_odds, market_key, lookup_side, debug=False):
    """Lookup a market entry with alternate-line fallback.

    Returns a tuple of ``(price, source_tag, matched_key, segment, source_type)``.
    Only markets belonging to the same segment family (e.g. ``1st_5_innings``)
    are searched to avoid mismatches.
    """

    from core.logger import get_logger
    logger = get_logger(__name__)

    def log(msg: str) -> None:
        if debug:
            logger.debug(msg)

    if not isinstance(market_odds, dict):
        log(f"[MATCHER] invalid market_odds type: {type(market_odds)}")
        return None, "unknown", "❌", "❌", "❌"

    normalized = lookup_side.strip()
    segment = classify_market_segment(market_key)

    log(f"\n🧠 [MATCHER] Lookup {market_key} | side: {lookup_side} → {normalized}")
    log(f"   • Segment      : {segment}")

    suffix = extract_segment_suffix(market_key)
    prefix = market_key.split("_")[0]

    search_keys = []
    for key in (market_key, f"alternate_{market_key}"):
        if key in market_odds:
            search_keys.append(key)

    for key in market_odds.keys():
        if key.startswith(prefix) and extract_segment_suffix(key) == suffix and key not in search_keys:
            search_keys.append(key)

    for key in search_keys:
        block = market_odds.get(key, {})
        if normalized in block:
            source_type = "alternate" if key.startswith("alternate_") else "mainline"
            source_map = market_odds.get(f"{key}_source", {})
            source_tag = source_map.get(normalized, source_type)
            log(f"✅ Found '{normalized}' in {key}")
            return (
                block[normalized],
                source_tag,
                key,
                get_segment_from_market(key),
                source_type,
            )

    log(f"❌ No match for '{normalized}' in: {search_keys}")
    return None, "unknown", "❌", "❌", "❌"





def extract_game_id_from_event(away_team, home_team, start_time_utc):
    """Construct a game ID (YYYY-MM-DD-AWAY@HOME) using US/Eastern time."""
    try:
        local_date = to_eastern(start_time_utc).strftime("%Y-%m-%d")
        away_abbr = TEAM_ABBR.get(away_team, away_team)
        home_abbr = TEAM_ABBR.get(home_team, home_team)
        return f"{local_date}-{away_abbr}@{home_abbr}"
    except Exception as e:
        from core.logger import get_logger
        get_logger(__name__).debug("[DEBUG] extract_game_id_from_event error: %s", e)
        return None


def normalize_game_id(game_id):
    """
    Standardizes game ID by correcting team abbreviations and ensuring consistent format.
    Example: '2025-04-15-CHW@KC' → '2025-04-15-CWS@KC'
    """
    try:
        date_part, matchup = game_id.split("-", 1)
        away, home = matchup.split("@")
        away = TEAM_ABBR_FIXES.get(away.upper(), away.upper())
        home = TEAM_ABBR_FIXES.get(home.upper(), home.upper())
        return f"{date_part}-{away}@{home}"
    except Exception as e:
        from core.logger import get_logger
        get_logger(__name__).debug("[DEBUG] normalize_game_id error: %s | Returning raw game_id", e)
        return game_id


def canonical_game_id(game_id: str) -> str:
    """Normalize ``game_id`` using :data:`TEAM_FIXES` for consistent comparisons."""
    try:
        date_parts = game_id.split("-", 3)[:3]
        matchup = game_id.split("-")[3]
        away, home = matchup.split("@")
        away_fixed = TEAM_FIXES.get(away, away)
        home_fixed = TEAM_FIXES.get(home, home)
        return f"{'-'.join(date_parts)}-{away_fixed}@{home_fixed}"
    except Exception:
        return game_id


def normalize_name(name):
    """
    Normalize a player name by:
    - Lowercasing, stripping whitespace, and removing punctuation/accents.
    - Converts "Last, First" → "first last".
    - Removes suffixes and handles common name particles.
    """
    if not isinstance(name, str):
        return ""

    name = name.strip().lower()

    for orig, repl in [
        (".", ""), ("é", "e"), ("á", "a"), ("í", "i"),
        ("ó", "o"), ("ú", "u"), ("ñ", "n"),
        ("’", ""), ("‘", ""), ("", ""), ("-", " "), ("_", " ")
    ]:
        name = name.replace(orig, repl)

    if "," in name:
        parts = name.split(",", 1)
        name = f"{parts[1].strip()} {parts[0].strip()}"

    tokens = name.split()
    suffixes = {"jr", "ii", "iii"}
    if tokens and tokens[-1] in suffixes:
        tokens = tokens[:-1]

    particles = {"de", "del", "la", "van", "von", "da", "du"}
    if len(tokens) > 2 and tokens[1] in particles:
        return tokens[0] + " " + " ".join(tokens[1:])
    elif len(tokens) >= 2:
        return tokens[0] + " " + tokens[-1]
    else:
        return name


TEAM_NAME_FROM_ABBR = {
    "ARI": "Diamondbacks",
    "ATL": "Braves",
    "BAL": "Orioles",
    "BOS": "Red Sox",
    "CHC": "Cubs",
    "CWS": "White Sox",
    "CIN": "Reds",
    "CLE": "Guardians",
    "COL": "Rockies",
    "DET": "Tigers",
    "HOU": "Astros",
    "KC":  "Royals",
    "LAA": "Angels",
    "LAD": "Dodgers",
    "MIA": "Marlins",
    "MIL": "Brewers",
    "MIN": "Twins",
    "NYM": "Mets",
    "NYY": "Yankees",
    "OAK": "Athletics",
    "PHI": "Phillies",
    "PIT": "Pirates",
    "SD":  "Padres",
    "SEA": "Mariners",
    "SF":  "Giants",
    "STL": "Cardinals",
    "TB":  "Rays",
    "TEX": "Rangers",
    "TOR": "Blue Jays",
    "WSH": "Nationals"
}


def normalize_team_abbr_to_name(abbr):
    """
    Normalize team abbreviation (e.g. 'PIT') to full team name (e.g. 'Pirates'),
    for use with reliever depth charts or display purposes.
    """
    return TEAM_NAME_FROM_ABBR.get(abbr.upper(), abbr.upper())