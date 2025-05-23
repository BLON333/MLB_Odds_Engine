import numpy as np
import random
import numpy as np


def resolve_bip(bip_type, ev=None, la=None, batter_speed=50, fielder_rating=50):
    """
    Resolve a batted ball in play (BIP) into a hit or out based on type and modifiers.
    """

    # Base BABIP by BIP type
    base_babip = {
        "LD": 0.70,   # line drive
        "GB": 0.305,  # ground ball
        "FB": 0.10,   # fly ball
        "POP": 0.02   # pop-up
    }

    prob = base_babip.get(bip_type.upper(), 0.30)

    # Apply speed modifier
    if batter_speed > 65:
        prob *= 1.025  # was 1.05
    elif batter_speed < 40:
        prob *= 0.97   # was 0.92

    # Adjust for fielder range (50 = neutral)
    if fielder_rating > 60:
        prob *= 0.97
    elif fielder_rating < 40:
        prob *= 1.03

    # Exit velo and launch angle modifiers
    hard_hit = ev if ev is not None else 88
    barrels = la if la is not None else 12

    # Reduce only if contact quality is VERY poor or VERY good
    if hard_hit < 85:
        prob *= 0.97
    elif hard_hit > 95:
        prob *= 1.05

    if barrels < 6.0:
        prob *= 0.96
    elif barrels > 10.0:
        prob *= 1.04


    # Cap BABIP to avoid extreme overproduction
    prob = max(min(prob, 0.65), 0.05)

    return random.random() < prob

