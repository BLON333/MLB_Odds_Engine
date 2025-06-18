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

