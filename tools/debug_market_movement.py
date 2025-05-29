import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.market_movement_tracker import detect_market_movement


def run_case(desc, prior, current, key, expected):
    result = detect_market_movement(current, prior)
    actual = result.get(key)
    status = "✅" if actual == expected else "❌"
    print(f"{status} {desc}: expected {expected}, got {actual}")


def main():
    base = {
        "ev_percent": 0.500,
        "market_prob": 0.497,
        "blended_fv": -110,
        "market_odds": -110,
        "stake": 100.0,
        "sim_prob": 0.500,
    }

    # EV percent should detect a small improvement
    run_case(
        "EV% small increase",
        base,
        {**base, "ev_percent": 0.501},
        "ev_movement",
        "better",
    )

    # Market prob small increase
    run_case(
        "Mkt% small increase",
        base,
        {**base, "market_prob": 0.499},
        "mkt_movement",
        "better",
    )

    # Stake decrease should be marked worse
    run_case(
        "Stake decrease",
        base,
        {**base, "stake": 99.0},
        "stake_movement",
        "worse",
    )

    # Simulated win prob small increase
    run_case(
        "Sim% small increase",
        base,
        {**base, "sim_prob": 0.502},
        "sim_movement",
        "better",
    )

    # Slightly better market odds
    run_case(
        "Odds improved",
        base,
        {**base, "market_odds": -108},
        "odds_movement",
        "better",
    )


if __name__ == "__main__":
    main()
