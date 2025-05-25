# half_inning_simulator.py
import random
from core.pa_simulator import simulate_pa
from core.fatigue_modeling import apply_fatigue_modifiers


class BaseState:
    """Simple engine for tracking runners on base."""

    def __init__(self):
        # Index 0 -> 1B, 1 -> 2B, 2 -> 3B
        self.bases = [None, None, None]

    def occupied(self, base: int) -> bool:
        """Return True if the specified base is occupied."""
        return self.bases[base - 1] is not None

    def add(self, base: int, runner) -> None:
        """Place a runner on a given base."""
        self.bases[base - 1] = runner

    def clear(self, base: int) -> None:
        """Empty the specified base."""
        self.bases[base - 1] = None

    def move(self, start: int, end: int) -> None:
        """Move a runner from one base to another."""
        self.bases[end - 1] = self.bases[start - 1]
        self.bases[start - 1] = None

    def score(self, base: int) -> int:
        """Remove a runner from the specified base and return 1 if occupied."""
        if self.occupied(base):
            self.clear(base)
            return 1
        return 0

    def as_bools(self) -> list:
        """Return boolean occupancy for [1B, 2B, 3B]."""
        return [bool(r) for r in self.bases]

    def __repr__(self) -> str:
        labels = ["1B", "2B", "3B"]
        occupied = ", ".join(lbl for lbl, r in zip(labels, self.bases) if r)
        return f"BaseState({occupied or 'empty'})"


# Outcome Handlers -----------------------------------------------------------


def handle_walk(bases: BaseState, batter) -> int:
    """Process a walk and return the number of runners scoring."""
    runs = 0
    force1 = bases.occupied(1)
    force2 = force1 and bases.occupied(2)
    force3 = force2 and bases.occupied(3)

    if force3:
        runs += bases.score(3)
    if force2:
        bases.move(2, 3)
    if force1:
        bases.move(1, 2)

    bases.add(1, batter)
    return runs


def handle_single(bases: BaseState, batter) -> int:
    """Handle a single and return runs scored."""
    runs = bases.score(3)
    if bases.occupied(2):
        if random.random() < 0.6:
            runs += bases.score(2)
        else:
            bases.move(2, 3)
    if bases.occupied(1):
        if random.random() < 0.4:
            bases.move(1, 3)
        else:
            bases.move(1, 2)
    bases.add(1, batter)
    return runs


def handle_double(bases: BaseState, batter) -> int:
    """Handle a double and return runs scored."""
    runs = bases.score(3)
    runs += bases.score(2)
    if bases.occupied(1):
        if random.random() < 0.6:
            runs += bases.score(1)
        else:
            bases.move(1, 3)
    bases.add(2, batter)
    return runs


def handle_triple(bases: BaseState, batter) -> int:
    """Handle a triple and return runs scored."""
    runs = 0
    for b in (3, 2, 1):
        runs += bases.score(b)
    bases.add(3, batter)
    return runs


def handle_hr(bases: BaseState, batter) -> int:
    """Handle a home run and return runs scored including the batter."""
    runs = 1
    for b in (3, 2, 1):
        runs += bases.score(b)
    return runs


def handle_out(bases: BaseState, outs: int) -> tuple[int, int]:
    """Record an out and possibly clear first base on a double play."""
    if bases.occupied(1) and outs < 2 and random.random() < 0.11:
        outs += 2
        bases.clear(1)
    else:
        outs += 1
    return outs, 0


# Main Simulation -----------------------------------------------------------


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
):
    """Simulate a half inning using the base state engine.

    Args:
        lineup: List of batter dictionaries.
        pitcher: Pitcher dictionary.
        context: Additional environmental context.
        start_batter_index: Index of lineup to begin at.
        pitcher_state: Dict tracking fatigue and batters faced.
        inning: Current inning number.
        half: "top" or "bottom".
        env: Optional environment reference for nested calls.
        debug: If True, prints debug output.
        use_noise: If True, randomize PA results slightly.

    Returns:
        Dictionary with runs, outs, events, next batter index and updated
        pitcher state.
    """

    def log(msg):
        if debug:
            print(msg)

    outs = 0
    runs = 0
    batter_idx = start_batter_index
    events = []
    bases = BaseState()

    pitcher_state = pitcher_state or {
        "batters_faced": 0,
        "pitch_count": 0,
        "tto_count": 1,
    }
    max_pa = 30
    pa_count = 0
    team_key = "AWAY" if half == "top" else "HOME"

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
        )

        outcome = result[0] if isinstance(result, tuple) else result
        log(f"⚾ {half.upper()} {inning} | Batter: {batter['name']} → {outcome}")
        log(f"     Bases before PA: {bases.as_bools()}")

        if outcome in ["K", "OUT"]:
            outs, runs_this_play = handle_out(bases, outs)
        elif outcome == "BB":
            runs_this_play = handle_walk(bases, batter)
        elif outcome == "1B":
            runs_this_play = handle_single(bases, batter)
        elif outcome == "2B":
            runs_this_play = handle_double(bases, batter)
        elif outcome == "3B":
            runs_this_play = handle_triple(bases, batter)
        elif outcome == "HR":
            runs_this_play = handle_hr(bases, batter)
        else:
            runs_this_play = 0

        if outs >= 3:
            runs_this_play = 0

        runs += runs_this_play
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

        log(f"     Runs scored this play: {runs_this_play}")
        log(f"     Bases after PA: {bases.as_bools()}")
        log(f"     Total outs: {outs}, Total runs: {runs}\n")

    if pa_count >= max_pa:
        print(
            f"⚠️ Max PA cap reached ({pa_count}) — potential infinite loop in {half} of inning {inning}"
        )

    return {
        "runs_scored": runs,
        "outs": outs,
        "events": events,
        "next_batter_index": batter_idx % len(lineup),
        "pitcher_state": pitcher_state,
    }
