# fatigue_modeling.py
from core.config import DEBUG_MODE, VERBOSE_MODE
from core.logger import get_logger

logger = get_logger(__name__)

def apply_fatigue_modifiers(pitcher_stats, pitcher_state):
    """
    Apply fatigue effects to pitcher stats based on pitch count and TTO.
    HR/FB adjustments are no longer applied in the new HR/PA model.
    
    Parameters:
      pitcher_stats: dictionary of pitcher statistics.
      pitcher_state: dictionary containing "pitch_count", "tto_count", etc.
      
    Returns:
      A new dictionary with fatigue-modified stats (K and BB rates, stuff, location, etc.).
    """
    adjusted = pitcher_stats.copy()
    
    pitch_count = pitcher_state.get("pitch_count", 0)
    tto = pitcher_state.get("tto_count", 1)
    
    # Adjust strikeout and walk rates based on TTO.
    if tto == 2:
        tto_penalty = 0.015
    elif tto == 3:
        tto_penalty = 0.035
    elif tto >= 4:
        tto_penalty = 0.060
    else:
        tto_penalty = 0.0
    
    adjusted["k_rate"] *= (1 - tto_penalty)
    adjusted["bb_rate"] *= (1 + tto_penalty)
    
    # Adjust for overall pitch count fatigue.
    fatigue_level = max(0, (pitch_count - 75) / 25)
    k_decay = 1.0 - 0.02 * fatigue_level
    bb_inflate = 1.0 + 0.03 * fatigue_level
    
    adjusted["k_rate"] *= k_decay
    adjusted["bb_rate"] *= bb_inflate
    
    # Apply fatigue to metrics like stuff, command, and location.
    for key in ["stuff_plus", "command_plus", "location_plus"]:
        if key in adjusted:
            decay = max(0.85, 1 - 0.015 * fatigue_level)
            adjusted[key] *= decay
    
    # HR/FB is not adjusted in the new model.
    adjusted["hr_fb_rate"] = "N/A"
    
    return adjusted

if __name__ == '__main__':
    sample_pitcher = {
        "k_rate": 0.232,
        "bb_rate": 0.07,
        "stuff_plus": 100,
        "location_plus": 100,
        "command_plus": 100,
        "hr_fb_rate": 0.1
    }
    sample_state = {"pitch_count": 100, "tto_count": 3}
    print("Adjusted stats:", apply_fatigue_modifiers(sample_pitcher, sample_state))
