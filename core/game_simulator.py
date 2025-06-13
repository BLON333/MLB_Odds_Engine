from core.config import DEBUG_MODE, VERBOSE_MODE
from core.half_inning_simulator import simulate_half_inning
from assets.bullpen_utils import simulate_reliever_chain
from core.logger import get_logger

logger = get_logger(__name__)

def should_replace_pitcher(pitcher_state, pitch_limit=90, tto_limit=3):
    return (
        pitcher_state.get("pitch_count", 0) > pitch_limit or
        pitcher_state.get("tto_count", 1) >= tto_limit
    )

def simulate_game(
    home_lineup,
    away_lineup,
    home_pitcher,
    away_pitcher,
    env,
    home_bullpen=None,
    away_bullpen=None,
    debug=False,
    return_inning_scores=False,
    use_noise=True
):
    home_score = 0
    away_score = 0
    innings_data = []
    home_batter_idx = 0
    away_batter_idx = 0

    home_pitcher_state = {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}
    away_pitcher_state = {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}

    current_home_pitcher = home_pitcher
    current_away_pitcher = away_pitcher
    used_home_relievers = []
    used_away_relievers = []

    if debug:
        print(f"\n🧩 Starting game simulation...")

    inning = 1
    while True:
        if debug:
            print(f"\n➡️ Inning {inning} begins")

        if should_replace_pitcher(home_pitcher_state):
            if home_bullpen:
                relievers = simulate_reliever_chain(home_bullpen, num_needed=1)
                if relievers:
                    current_home_pitcher = relievers[0]
                    used_home_relievers.append(current_home_pitcher.get("name", "Unknown"))
                    home_pitcher_state = {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}

        away_half = simulate_half_inning(
            lineup=away_lineup,
            pitcher=current_home_pitcher,
            context=env,
            start_batter_index=away_batter_idx,
            pitcher_state=home_pitcher_state,
            inning=inning,
            half="top",
            env=env,
            debug=debug,
            use_noise=use_noise
        )
        away_batter_idx = away_half.get("next_batter_index", 0)
        away_score += away_half.get("runs_scored", 0)
        home_pitcher_state = away_half.get("pitcher_state", home_pitcher_state)

        if not (inning == 9 and home_score > away_score):
            if should_replace_pitcher(away_pitcher_state):
                if away_bullpen:
                    relievers = simulate_reliever_chain(away_bullpen, num_needed=1)
                    if relievers:
                        current_away_pitcher = relievers[0]
                        used_away_relievers.append(current_away_pitcher.get("name", "Unknown"))
                        away_pitcher_state = {"batters_faced": 0, "pitch_count": 0, "tto_count": 1}

            home_half = simulate_half_inning(
                lineup=home_lineup,
                pitcher=current_away_pitcher,
                context=env,
                start_batter_index=home_batter_idx,
                pitcher_state=away_pitcher_state,
                inning=inning,
                half="bottom",
                env=env,
                debug=debug,
                use_noise=use_noise
            )
            home_batter_idx = home_half.get("next_batter_index", 0)
            home_score += home_half.get("runs_scored", 0)
            away_pitcher_state = home_half.get("pitcher_state", away_pitcher_state)
        else:
            home_half = {"runs_scored": 0, "events": []}

        innings_data.append({
            "inning": inning,
            "away_runs": away_half.get("runs_scored", 0),
            "home_runs": home_half.get("runs_scored", 0),
            "top_half_events": away_half.get("events", []),
            "bottom_half_events": home_half.get("events", [])
        })

        if debug:
            print(f"📊 End of Inning {inning} | Score: Away {away_score} - Home {home_score}")

        if inning >= 9 and home_score != away_score:
            break

        inning += 1

    recap = {k: 0 for k in ["K", "BB", "1B", "2B", "3B", "HR", "OUT"]}
    for inning_data in innings_data:
        for half_key in ["top_half_events", "bottom_half_events"]:
            for event in inning_data.get(half_key, []):
                outcome = event.get("outcome")
                if outcome in recap:
                    recap[outcome] += 1

    total_runs = home_score + away_score
    run_margin = abs(home_score - away_score)
    if total_runs <= 3:
        game_type = "Pitcher's Duel"
    elif run_margin >= 7:
        game_type = "Blowout"
    elif run_margin == 1 and total_runs >= 6:
        game_type = "Tight Offensive Battle"
    else:
        game_type = "Balanced"

    result = {
        "home_score": home_score,
        "away_score": away_score,
        "innings": innings_data,
        "used_home_relievers": used_home_relievers,
        "used_away_relievers": used_away_relievers,
        "home_pitcher_state": home_pitcher_state,
        "away_pitcher_state": away_pitcher_state,
        "game_type": game_type,
        "recap": recap
    }

    if return_inning_scores:
        result["inning_scores"] = {
            i["inning"]: {"home": i["home_runs"], "away": i["away_runs"]}
            for i in innings_data
        }

    if debug:
        print(f"\n✅ Game simulation complete: {game_type}")
        print(f"Final Score: Away {away_score} - Home {home_score}\n")

    return result



def build_sample_lineup(num_batters=9):
    """
    Build a sample lineup of batters.
    Each batter is represented as a dictionary with basic stats.
    """
    sample_batter = {
        "name": "Average Batter",
        "handedness": "R",
        "k_rate": 0.225,
        "bb_rate": 0.082,
        "iso": 0.145,
        "avg": 0.245,
        "woba": 0.320
    }
    return [sample_batter.copy() for _ in range(num_batters)]

def build_sample_pitcher():
    """Return a generic pitcher profile for quick simulations."""

    return {
        "name": "Average Pitcher",
        "throws": "R",
        "k_rate": 0.232,
        "bb_rate": 0.07,
        "stuff_plus": 100,
        "location_plus": 100,
        "hr_pa": {
            "hr_pa_projected": 0.03,
            "empirical_hr_pa": 0.03,
            "model_estimate_hr_pa": 0.03,
            "hr_per_9": 2.5,
        },
        "exit_velocity_avg": 90.1,
        "launch_angle_avg": 15.3,
        "fielder_rating": 50,
    }


if __name__ == '__main__':
    # Build sample assets for testing.
    home_lineup = build_sample_lineup()
    away_lineup = build_sample_lineup()

    home_pitcher = build_sample_pitcher()
    away_pitcher = build_sample_pitcher()

    # Optionally, create a small bullpen for each team.
    home_bullpen = [build_sample_pitcher() for _ in range(3)]
    away_bullpen = [build_sample_pitcher() for _ in range(3)]

    # Define a neutral simulation environment.
    env = {
        "park_hr_mult": 1.00,
        "single_mult": 1.00,
        "weather_hr_mult": 1.00,
        "adi_mult": 1.00,
        "umpire": {"K": 1.0, "BB": 1.0}
    }

    # Run a test game simulation with debug output enabled.
    game_result = simulate_game(
        home_lineup=home_lineup,
        away_lineup=away_lineup,
        home_pitcher=home_pitcher,
        away_pitcher=away_pitcher,
        env=env,
        home_bullpen=home_bullpen,
        away_bullpen=away_bullpen,
        debug=False
    )

    print("\nFinal Game Result:")
    print("Home Score:", game_result["home_score"])
    print("Away Score:", game_result["away_score"])
    print("Game Type:", game_result["game_type"])
    print("Innings Details:", game_result["innings"])
