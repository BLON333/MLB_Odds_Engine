# half_inning_simulator.py
from core.config import DEBUG_MODE, VERBOSE_MODE
import numpy as np
import random
from core.pa_simulator import simulate_pa
from core.fatigue_modeling import apply_fatigue_modifiers
from core.logger import get_logger

logger = get_logger(__name__)


def maybe_inject_misc_run(runs, runner_reached, rng=None):
    """Occasionally convert a scoreless inning into a one-run frame."""
    rand = rng if rng is not None else random
    if runner_reached and runs == 0 and rand.random() < 0.011:
        return runs + 1
    return runs


def maybe_inject_ghost_run(runs, runner_reached, rng=None):
    """Add a possible unearned ghost run if a runner reached base."""
    rand = rng if rng is not None else random
    if runner_reached and rand.random() < 0.01:
        return runs + 1
    return runs


def maybe_score_from_second(before_state, after_state, outs, rng=None):
    """With two outs, occasionally score a runner from second on a single."""
    rand = rng if rng is not None else random
    if (
        outs == 2
        and before_state[1] is not None
        and after_state[2] is not None
        and rand.random() < 0.10
    ):
        after_state[2] = None
        return after_state, 1
    return after_state, 0


def _advance_bases(base_state, transitions, batter=None, debug=False):
    """Return new base state and runs scored after applying transitions."""
    new_state = [None, None, None]
    runs = 0

    for i in range(3):
        runner = base_state[i]
        dest = transitions.get(i, i)
        if runner:
            if dest == "home":
                runs += 1
                if debug:
                    logger.debug(f"     Runner from base {i+1} scores")
            else:
                new_state[dest] = runner
                if debug and dest != i:
                    logger.debug(f"     Runner from base {i+1} -> base {dest+1}")

    if "batter" in transitions:
        dest = transitions["batter"]
        if dest == "home":
            runs += 1
            if debug:
                logger.debug("     Batter scores")
        else:
            new_state[dest] = batter
            if debug:
                logger.debug(f"     Batter -> base {dest+1}")

    return new_state, runs

def _handle_out(base_state, outs, rng=None, debug=False):
    """Handle strikeouts and generic outs."""
    rand = rng if rng is not None else random
    if base_state[0] and outs < 2 and rand.random() < 0.14:
        if debug:
            logger.debug("     Double play chance triggered")
        new_state = base_state.copy()
        new_state[0] = None
        return new_state, 0, 2
    return base_state, 0, 1

def _handle_walk(base_state, batter, rng=None, debug=False):
    mapping = {}
    if all(base_state):
        mapping[2] = "home"
    if base_state[1]:
        mapping[1] = 2
    if base_state[0]:
        mapping[0] = 1
    mapping["batter"] = 0
    new_state, runs = _advance_bases(base_state, mapping, batter, debug=debug)
    return new_state, runs, 0


def _handle_single(base_state, batter, outs, rng=None, debug=False):
    rand = rng if rng is not None else random
    mapping = {}
    if base_state[2]:
        mapping[2] = "home"
    if base_state[1]:
        mapping[1] = "home" if rand.random() < 0.4 else 2
    if base_state[0]:
        mapping[0] = 1 if rand.random() < 0.8 else 0
    mapping["batter"] = 0
    new_state, runs = _advance_bases(base_state, mapping, batter, debug=debug)
    new_state, extra = maybe_score_from_second(base_state, new_state, outs, rng=rand)
    return new_state, runs + extra, 0


def _handle_double(base_state, batter, rng=None, debug=False):
    rand = rng if rng is not None else random
    mapping = {}
    if base_state[2]:
        mapping[2] = "home"
    if base_state[1]:
        mapping[1] = "home"
    if base_state[0]:
        mapping[0] = "home" if rand.random() < 0.4 else 2
    mapping["batter"] = 1
    new_state, runs = _advance_bases(base_state, mapping, batter, debug=debug)
    return new_state, runs, 0


def _handle_triple(base_state, batter, rng=None, debug=False):
    mapping = {0: "home", 1: "home", 2: "home", "batter": 2}
    new_state, runs = _advance_bases(base_state, mapping, batter, debug=debug)
    return new_state, runs, 0


def _handle_home_run(base_state, batter, rng=None, debug=False):
    runs = sum(1 for b in base_state if b) + 1
    if debug:
        logger.debug(f"     Home run! {runs} run(s) score")
    return [None, None, None], runs, 0

def simulate_half_inning(
    lineup,
    pitcher,
    context,
    start_batter_index=0,
    pitcher_state=None,
    inning=1,
    half="top",
    env=None,
    debug=False,
    use_noise=True,
    rng=None
):
    """Simulate a half inning and return run totals and events."""
    outs = 0
    runs = 0
    batter_idx = start_batter_index
    events = []
    base_state = [None, None, None]  # [1B, 2B, 3B]
    runner_reached = False

    pitcher_state = pitcher_state or {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}
    max_pa = 30
    pa_count = 0
    team_key = "AWAY" if half == "top" else "HOME"

    outcome_handlers = {
        "K": _handle_out,
        "OUT": _handle_out,
        "BB": _handle_walk,
        "1B": _handle_single,
        "2B": _handle_double,
        "3B": _handle_triple,
        "HR": _handle_home_run,
    }

    while outs < 3 and pa_count < max_pa:
        batter = lineup[batter_idx % len(lineup)]

        if batter_idx > 0 and batter_idx % len(lineup) == 0:
            pitcher_state["tto_count"] += 1

        adj_pitcher = apply_fatigue_modifiers(pitcher, pitcher_state)

        result = simulate_pa(
            batter,
            adj_pitcher,
            context.get("umpire", {}),
            context.get("weather_hr", 1.0),
            pitcher_state["batters_faced"],
            env=env,
            debug=debug,
            return_probs=True,
            batting_team=team_key,
            use_noise=use_noise,
            rng=rng,
        )

        outcome = result[0] if isinstance(result, tuple) else result

        if debug:
            logger.debug(f"⚾ {half.upper()} {inning} | Batter: {batter['name']} → {outcome}")
            logger.debug(f"     Bases before PA: {base_state}")

        handler = outcome_handlers.get(outcome, _handle_out)

        if outcome in ("K", "OUT"):
            base_state, runs_this_play, outs_added = handler(base_state, outs, rng=rng, debug=debug)
        elif outcome == "1B":
            base_state, runs_this_play, outs_added = handler(base_state, batter, outs, rng=rng, debug=debug)
        else:
            base_state, runs_this_play, outs_added = handler(base_state, batter, rng=rng, debug=debug)
        outs += outs_added

        if outs >= 3:
            runs_this_play = 0

        runs += runs_this_play
        if not runner_reached and (runs_this_play > 0 or any(base_state)):
            runner_reached = True
        pitcher_state["batters_faced"] += 1
        pitcher_state["pitch_count"] += 1
        batter_idx += 1
        pa_count += 1

        events.append(
            {
                "inning": inning,
                "half": half,
                "batter": batter["name"],
                "pitcher": pitcher["name"],
                "outcome": outcome,
                "runs_scored": runs_this_play,
            }
        )

        if debug:
            logger.debug(f"     Runs scored this play: {runs_this_play}")
            logger.debug(f"     Bases after PA: {[bool(base_state[i]) for i in range(3)]}")
            logger.debug(f"     Total outs: {outs}, Total runs: {runs}\n")

    if pa_count >= max_pa:
        logger.debug(f"⚠️ Max PA cap reached ({pa_count}) — potential infinite loop in {half} of inning {inning}")

    new_runs = maybe_inject_misc_run(runs, runner_reached, rng=rng)
    if new_runs > runs:
        events.append({"inning": inning, "half": half, "batter": None, "pitcher": pitcher["name"], "outcome": "MISC_RUN", "runs_scored": 1})
        runs = new_runs

    new_runs = maybe_inject_ghost_run(runs, runner_reached, rng=rng)
    if new_runs > runs:
        events.append({"inning": inning, "half": half, "batter": None, "pitcher": pitcher["name"], "outcome": "GHOST_RUN", "runs_scored": 1})
        runs = new_runs

    return {
        "runs_scored": runs,
        "outs": outs,
        "events": events,
        "next_batter_index": batter_idx % len(lineup),
        "pitcher_state": pitcher_state,
    }




if __name__ == '__main__':
    # Basic testing code here
    dummy_lineup = [
        {"name": "Batter 1", "k_rate": 0.22, "bb_rate": 0.08, "speed": 50},
        {"name": "Batter 2", "k_rate": 0.22, "bb_rate": 0.08, "speed": 50},
        {"name": "Batter 3", "k_rate": 0.22, "bb_rate": 0.08, "speed": 50}
    ]
    dummy_pitcher = {
        "name": "Dummy Pitcher",
        "k_rate": 0.232,
        "bb_rate": 0.07,
        "stuff_plus": 100,
        "location_plus": 100,
        "hr_pa": {"hr_pa_projected": 0.03, "empirical_hr_pa": 0.03, "model_estimate_hr_pa": 0.03, "hr_per_9": 2.5},
        "exit_velocity_avg": 90.1,
        "launch_angle_avg": 15.3,
        "fielder_rating": 50
    }
    dummy_pitcher_state = {"batters_faced": 0, "pitch_count": 75, "tto_count": 2}
    hi = simulate_half_inning(
        dummy_lineup,
        dummy_pitcher,
        context={"umpire": {}, "weather_hr": 1.0},
        debug=True,
        rng=np.random.default_rng(42)
    )
    logger.debug("Simulated half inning outcomes: %s", hi)
