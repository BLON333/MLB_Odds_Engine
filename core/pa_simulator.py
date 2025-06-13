from core.config import DEBUG_MODE, VERBOSE_MODE
import numpy as np
import random
from core.bip_resolution import resolve_bip
from core.logger import get_logger

logger = get_logger(__name__)

# Global outcome log for plate appearances
PA_RECAP_LOG = {
    "HOME": {"K": 0, "BB": 0, "1B": 0, "2B": 0, "3B": 0, "HR": 0, "OUT": 0},
    "AWAY": {"K": 0, "BB": 0, "1B": 0, "2B": 0, "3B": 0, "HR": 0, "OUT": 0}
}

def log_pa_outcome(team, outcome):
    """Safely log outcome to PA_RECAP_LOG."""
    if team not in PA_RECAP_LOG:
        PA_RECAP_LOG[team] = {k: 0 for k in ["K", "BB", "1B", "2B", "3B", "HR", "OUT"]}
    if outcome in PA_RECAP_LOG[team]:
        PA_RECAP_LOG[team][outcome] += 1

# === NEW: Beta sampling noise for probability variance ===
def beta_noise(p, weight=30, rng=None):
    """Return probability with Beta noise applied using ``rng``."""
    if p <= 0.0:
        return 0.0
    if p >= 1.0:
        return 1.0
    alpha = p * weight
    beta = (1 - p) * weight
    rand = rng if rng is not None else np.random
    return rand.beta(alpha, beta)


def resolve_base_outcome(sample, k_prob, bb_prob):
    """Resolve base outcome from random sample and probabilities."""
    if sample < k_prob:
        return "K"
    if sample < k_prob + bb_prob:
        return "BB"
    return "Contact"


def check_home_run(pitcher, weather_mult=1.0, rng=None):
    """Return tuple of (is_hr, hr_prob) based on pitcher and weather."""
    hr_prob = pitcher.get("hr_pa", {}).get("hr_pa_projected", 0.046) * weather_mult
    rand = rng if rng is not None else np.random
    return rand.random() < hr_prob, hr_prob


def resolve_contact(batter, pitcher, debug=False, rng=None):
    """Resolve a ball in play into hit type or out."""
    rand = rng if rng is not None else np.random
    bip_type = rand.choice(["GB", "LD", "FB", "POP"], p=[0.28, 0.32, 0.30, 0.10])
    if debug:
        print(f"DEBUG: BIP Type: {bip_type}")

    is_hit = resolve_bip(
        bip_type,
        ev=pitcher.get("exit_velocity_avg"),
        la=pitcher.get("launch_angle_avg"),
        batter_speed=batter.get("speed", 50),
        fielder_rating=pitcher.get("fielder_rating", 50),
        debug=debug,
    )

    if is_hit:
        probs = [0.76, 0.22, 0.02]
        probs[0] *= 1.01
        total = sum(probs)
        probs = [p / total for p in probs]
        outcome = rand.choice(["1B", "2B", "3B"], p=probs)
    else:
        if rand.random() < 0.10:
            fb_probs = [0.92, 0.08]
            fb_probs[0] *= 1.01
            total = sum(fb_probs)
            fb_probs = [p / total for p in fb_probs]
            outcome = rand.choice(["1B", "2B"], p=fb_probs)
            if debug:
                print(f"DEBUG: Infield single fallback triggered → {outcome}")
        else:
            outcome = "OUT"

    if debug:
        print(f"DEBUG: Hit? {is_hit} → {bip_type} → {outcome}")

    return outcome


def simulate_pa(
    batter,
    pitcher,
    umpire_modifiers=None,
    weather_hr_mult=1.0,
    batters_faced=0,
    env=None,
    debug=False,
    return_probs=False,
    batting_team="HOME",
    use_noise=False,
    rng=None,
):
    """Simulate a single plate appearance and return the outcome."""

    rand = rng if rng is not None else np.random

    k_rate = (batter.get("k_rate", 0.22) + pitcher.get("k_rate", 0.22)) / 2
    bb_rate = (batter.get("bb_rate", 0.08) + pitcher.get("bb_rate", 0.08)) / 2
    contact_prob = 1 - k_rate - bb_rate

    if umpire_modifiers:
        k_rate *= umpire_modifiers.get("k_mod", 1.0)
        bb_rate *= umpire_modifiers.get("bb_mod", 1.0)

    # Normalize and optionally apply noise
    k_prob = beta_noise(k_rate, rng=rand) if use_noise else k_rate
    bb_prob = beta_noise(bb_rate, rng=rand) if use_noise else bb_rate
    bb_prob *= 1.01
    contact_prob = max(0.0, 1 - k_prob - bb_prob)

    result = rand.random() if hasattr(rand, "random") else rand.rand()

    base_outcome = resolve_base_outcome(result, k_prob, bb_prob)

    is_hr, effective_hr_pa = check_home_run(pitcher, weather_hr_mult, rng=rand)

    if base_outcome == "Contact":
        if is_hr:
            outcome = "HR"
        else:
            outcome = resolve_contact(batter, pitcher, debug=debug, rng=rand)
    else:
        outcome = base_outcome

    log_pa_outcome(batting_team, outcome)

    return (outcome, {
        "K": k_prob,
        "BB": bb_prob,
        "HR": effective_hr_pa,
        "Contact": contact_prob
    }) if return_probs else outcome



# === Standalone Test Mode ===
if __name__ == '__main__':
    test_batter = {"name": "Test Batter", "k_rate": 0.22, "bb_rate": 0.08, "speed": 50}
    test_pitcher = {
        "name": "Test Pitcher",
        "k_rate": 0.23,
        "bb_rate": 0.07,
        "hr_pa": {
            "hr_pa_projected": 0.03,
            "empirical_hr_pa": 0.03,
            "model_estimate_hr_pa": 0.03,
            "hr_per_9": 2.5
        },
        "exit_velocity_avg": 90.1,
        "launch_angle_avg": 15.3,
        "fielder_rating": 50
    }

    outcome, pa_probs = simulate_pa(
        test_batter,
        test_pitcher,
        debug=True,
        return_probs=True,
        use_noise=True,
        rng=np.random.default_rng(42),
    )
    print(f"Outcome: {outcome}")
    print(f"Probabilities: {pa_probs}")
