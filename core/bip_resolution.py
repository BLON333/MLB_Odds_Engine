from core.config import DEBUG_MODE, VERBOSE_MODE
import numpy as np
import random
import numpy as np


def resolve_bip(bip_type, ev=None, la=None, batter_speed=50, fielder_rating=50, debug=False):
    """
    Resolve a batted ball in play (BIP) into a hit or out based on type and modifiers.
    """

    # Base BABIP by BIP type
    base_babip = {
        "LD": 0.65,   # 🔻 reduce from 0.70
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
    ev_la_mult = 1.0
    if hard_hit < 85:
        ev_la_mult *= 0.97
    elif hard_hit > 95:
        ev_la_mult *= 1.03  # reduced from 1.05

    if barrels < 6.0:
        ev_la_mult *= 0.96
    elif barrels > 10.0:
        ev_la_mult *= 1.03  # was 1.04

    # Limit combined EV/LA effect to ~6%
    ev_la_mult = min(ev_la_mult, 1.06)

    prob *= ev_la_mult


    # Cap BABIP to avoid extreme overproduction
    prob = max(min(prob, 0.55), 0.05)

    if debug:
        print(f"resolve_bip: {bip_type} EV={ev} LA={la} -> prob={prob:.3f}")

    return random.random() < prob

