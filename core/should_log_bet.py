import pandas as pd

def should_log_bet(new_bet: dict,
                   market_evals: pd.DataFrame,
                   verbose: bool = True,
                   min_ev: float = 0.05,
                   min_stake: float = 1.0) -> dict | None:
    game_id = new_bet["game_id"]
    market = new_bet["market"]
    side = new_bet["side"]
    stake = new_bet["full_stake"]
    ev = new_bet["ev_percent"]
    segment = new_bet.get("segment", "mainline")
    from utils import normalize_to_abbreviation, get_normalized_lookup_side
    side = normalize_to_abbreviation(get_normalized_lookup_side(side, new_bet["market"]))
    new_bet["side"] = side  # update reference in place


    # 🚫 Basic EV and stake threshold
    if ev < min_ev * 100 or stake < min_stake:
        if verbose:
            print(f"⛔ should_log_bet: Rejected due to EV/stake threshold → EV: {ev:.2f}%, Stake: {stake:.2f}u")
        return None

    # 🔁 Top-up logic — only allow if ≥ 0.5u delta
    prior_bets = market_evals[market_evals["game_id"] == game_id]
    prior_bets_segment_filtered = prior_bets[
        prior_bets["segment"].fillna("mainline") == segment
    ]

    match = prior_bets_segment_filtered[
        (prior_bets_segment_filtered["market"] == market) &
        (prior_bets_segment_filtered["side"] == side)
    ]
    if not match.empty:
        previous_stake = match["stake"].sum()
        if stake - previous_stake >= 0.5:
            new_bet["stake"] = round(stake - previous_stake, 2)
            new_bet["entry_type"] = "top-up"
            if verbose:
                print(f"🔼 Top-Up Allowed → {side} | Delta: {new_bet['stake']:.2f}")
            return new_bet
        else:
            if verbose:
                print(f"⛔ should_log_bet: Rejected top-up — delta too small (< 0.5u) → {side}")
            return None

    # 🚧 Conflict check — reject if opposite side already logged
    opposite_exact = False
    tokens = side.split()
    # 🧠 Try to parse team total format: "Over 4.5 LAD" or "Under 4.5 BOS"
    if len(tokens) >= 3 and tokens[0] in {"Over", "Under"}:
        direction, value, *team_parts = tokens
        team = " ".join(team_parts)
        opposite = f"{'Under' if direction == 'Over' else 'Over'} {value} {team}"
        opposite_exact = True
    elif "Over" in side:
        opposite = "Under"
    elif "Under" in side:
        opposite = "Over"
    elif any(x in side for x in ["+", "-", "ML"]):
        parts = side.split()
        if len(parts) > 1 and parts[1].startswith("+"):
            opposite = parts[0] + " -"
        elif len(parts) > 1 and parts[1].startswith("-"):
            opposite = parts[0] + " +"
        else:
            opposite = None
    else:
        opposite = None

    if opposite:
        if opposite_exact:
            conflict = prior_bets_segment_filtered[
                (prior_bets_segment_filtered["market"] == market) &
                (prior_bets_segment_filtered["side"].astype(str).str.lower() == opposite.lower()) &
                (prior_bets_segment_filtered["segment"].fillna("mainline") == segment)
            ]
        else:
            conflict = prior_bets_segment_filtered[
                (prior_bets_segment_filtered["market"] == market) &
                (prior_bets_segment_filtered["side"].astype(str).str.contains(opposite, case=False, na=False)) &
                (prior_bets_segment_filtered["segment"].fillna("mainline") == segment)
            ]
        if not conflict.empty:
            if verbose:
                print(f"❌ should_log_bet: Rejected due to theme conflict with existing {opposite} bet in same market/segment")
            return None

    # ✅ Passed all checks
    new_bet["stake"] = stake
    new_bet["entry_type"] = "first"
    if verbose:
        print(f"✅ should_log_bet: Accepted → {side} | {market} | Stake: {stake:.2f}u | EV: {ev:.2f}%")
    return new_bet
