# half_inning_simulator.py
import numpy as np
import random
from core.pa_simulator import simulate_pa
from core.fatigue_modeling import apply_fatigue_modifiers

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
    use_noise=True
):
    outs = 0
    runs = 0
    batter_idx = start_batter_index
    events = []
    base_state = [None, None, None]  # [1B, 2B, 3B]

    pitcher_state = pitcher_state or {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}
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
            use_noise=use_noise  
        )

        outcome = result[0] if isinstance(result, tuple) else result
        runs_this_play = 0

        if debug:
            print(f"⚾ {half.upper()} {inning} | Batter: {batter['name']} → {outcome}")
            print(f"     Bases before PA: {base_state}")

        if outcome in ["K", "OUT"]:
            if base_state[0] and outs < 2 and random.random() < 0.11:
                outs += 2
                base_state[0] = None
            else:
                outs += 1

        elif outcome == "BB":
            if all(base_state):
                runs_this_play += 1
            if base_state[1]:
                base_state[2] = base_state[1]
            if base_state[0]:
                base_state[1] = base_state[0]
            base_state[0] = batter

        elif outcome == "1B":
            if base_state[2]: runs_this_play += 1
            if base_state[1] and random.random() < 0.6: runs_this_play += 1
            third = base_state[0] if base_state[0] and random.random() < 0.4 else None
            base_state = [batter, third, base_state[2]]

        elif outcome == "2B":
            if base_state[2]: runs_this_play += 1
            if base_state[1]: runs_this_play += 1
            if base_state[0] and random.random() < 0.6: runs_this_play += 1
            base_state = [None, batter, None]

        elif outcome == "3B":
            runs_this_play += sum(1 for b in base_state if b)
            base_state = [None, None, batter]

        elif outcome == "HR":
            runs_this_play += 1 + sum(1 for b in base_state if b)
            base_state = [None, None, None]

        if outs >= 3:
            runs_this_play = 0

        runs += runs_this_play
        pitcher_state["batters_faced"] += 1
        pitcher_state["pitch_count"] += 1
        batter_idx += 1
        pa_count += 1

        events.append({
            "inning": inning,
            "half": half,
            "batter": batter["name"],
            "pitcher": pitcher["name"],
            "outcome": outcome,
            "runs_scored": runs_this_play
        })

        if debug:
            print(f"     Runs scored this play: {runs_this_play}")
            print(f"     Bases after PA: {[bool(base_state[i]) for i in range(3)]}")
            print(f"     Total outs: {outs}, Total runs: {runs}\n")

    if pa_count >= max_pa:
        print(f"⚠️ Max PA cap reached ({pa_count}) — potential infinite loop in {half} of inning {inning}")

    return {
        "runs_scored": runs,
        "outs": outs,
        "events": events,
        "next_batter_index": batter_idx % len(lineup),
        "pitcher_state": pitcher_state
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
    hi = simulate_half_inning(dummy_lineup, dummy_pitcher, context={"umpire": {}, "weather_hr": 1.0}, debug=True)
    print("Simulated half inning outcomes:", hi)
