"""Utility helpers for core modules."""


def validate_bet_schema(bet_dict):
    """Validate required keys exist in a bet evaluation result.

    Parameters
    ----------
    bet_dict : dict
        Result dictionary returned from ``should_log_bet`` or other
        bet evaluation helpers.

    Raises
    ------
    ValueError
        If any required key is missing.
    """
    required_keys = ["skip", "full_stake", "log"]
    for key in required_keys:
        if key not in bet_dict:
            raise ValueError(f"Missing required key in bet evaluation: {key}")
    return True


def parse_game_id(game_id: str):
    """
    Parses a game_id string like 'mlb.20250619.NYM@ATL' and returns (date_str, away, home).
    """
    try:
        _, date_str, matchup = game_id.split(".")
        away, home = matchup.split("@")
        return date_str, away, home
    except Exception as e:
        raise ValueError(f"Invalid game_id format: {game_id}") from e


def canonical_game_id(sport: str, date_str: str, away: str, home: str):
    """
    Returns the canonical game_id string like 'mlb.20250619.NYM@ATL'.

    Parameters
    ----------
    sport : str
        The sport code (e.g. 'mlb').
    date_str : str
        The date in 'YYYYMMDD' format.
    away : str
        The away team abbreviation.
    home : str
        The home team abbreviation.

    Returns
    -------
    str
        Canonical game_id string.
    """
    return f"{sport}.{date_str}.{away}@{home}"


def normalize_line_label(label: str) -> tuple[str, float | None]:
    """
    Normalize a line label like 'O9', 'U 8.5', 'Over 9', 'Under 8.5' into a tuple.
    Returns: ('Over', 9.0) or ('Under', 8.5)
    If no match found, returns (label, None)
    """
    label = label.strip().lower().replace("ov", "over").replace("un", "under")

    if label.startswith("o") and label[1:].replace(".", "", 1).isdigit():
        return "Over", float(label[1:])
    elif label.startswith("u") and label[1:].replace(".", "", 1).isdigit():
        return "Under", float(label[1:])
    elif label.startswith("over"):
        try:
            return "Over", float(label.split(" ")[1])
        except Exception:
            return "Over", None
    elif label.startswith("under"):
        try:
            return "Under", float(label.split(" ")[1])
        except Exception:
            return "Under", None
    return label, None
