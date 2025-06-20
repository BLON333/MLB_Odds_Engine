# consensus_pricer.py (final patch — paired_key fix for spreads)

from core.config import DEBUG_MODE, VERBOSE_MODE
from core.market_pricer import implied_prob, to_american_odds
from core.utils import (
    normalize_label,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
    get_teams_from_game_id,
)

_DEVIG_WARNING_LOGGED = set()

DEFAULT_CONSENSUS_BOOKS = [
    "pinnacle", "betonlineag", "fanduel", "betmgm", "draftkings", "williamhill_us", "mybookieag"
]


SIM_ONLY = {
    "consensus_prob": None,
    "fair_odds": None,
    "pricing_method": "sim_only",
}


def calculate_consensus_prob(
    game_id,
    market_odds,
    market_key,
    label,
    consensus_books=DEFAULT_CONSENSUS_BOOKS,
    debug=False,
    throttle_logs=True,
):
    def sim_only(reason):
        if debug:
            print(f"🟡 Devig failed for {label} in {market_key} → {reason}; using sim_only")
        return SIM_ONLY, "sim_only"
    base_market_keys = [market_key]

    if not market_key.startswith("alternate_"):
        if "team_totals" in market_key:
            base_market_keys.append(
                market_key.replace("team_totals", "alternate_team_totals")
            )
        elif "totals" in market_key:
            base_market_keys.append(market_key.replace("totals", "alternate_totals"))
        elif "spreads" in market_key:
            base_market_keys.append(
                market_key.replace("spreads", "alternate_spreads")
            )
    else:
        if "alternate_team_totals" in market_key:
            base_market_keys.append(
                market_key.replace("alternate_team_totals", "team_totals")
            )
        elif "alternate_totals" in market_key:
            base_market_keys.append(market_key.replace("alternate_", ""))
        elif "alternate_spreads" in market_key:
            base_market_keys.append(market_key.replace("alternate_", ""))

    # Normalize incoming label
    label = normalize_label(label).replace("+0.0", "0.0").replace("-0.0", "0.0").strip()

    label_point = None
    label_split = label.split()
    if len(label_split) == 2 and label_split[0] in TEAM_NAME_TO_ABBR:
        try:
            label_point = float(label_split[1])
        except:
            pass
    elif len(label_split) == 2 and label_split[0] in ["Over", "Under"]:
        label_point = float(label_split[1])

    for mkt_key in base_market_keys:
        market = market_odds.get(game_id, {}).get(mkt_key, {})
        if not isinstance(market, dict):
            continue

        # Robust lookup for label key
        label_key = next((k for k in market if normalize_label(k) == label), None)

        # TEAM TOTALS → Over/Under devig logic
        if "team_totals" in mkt_key:
            if not label_key:
                return sim_only("missing label")
            try:
                parts = label.split()
                if len(parts) != 3 or parts[1] not in {"Over", "Under"}:
                    return sim_only("unsupported team_totals label")
                team, side, point = parts
                point = float(point)
                paired_side = "Under" if side == "Over" else "Over"
                paired_label = f"{team} {paired_side} {point:.1f}"
                paired_key = next((k for k in market if normalize_label(k) == normalize_label(paired_label)), None)
                if not paired_key:
                    return sim_only("missing paired label")
                books_label = market[label_key].get("per_book", {})
                books_pair = market[paired_key].get("per_book", {})
            except:
                return sim_only("team_totals parse error")



        # TOTALS (includes alternate_totals)
        elif "totals" in mkt_key:
            if not label_key:
                return sim_only("missing label")
            if not label.startswith("Over") and not label.startswith("Under"):
                return sim_only("unsupported label in totals")
            paired_label = f"{'Under' if label.startswith('Over') else 'Over'} {label_point}"
            paired_key = next((k for k in market if normalize_label(k) == normalize_label(paired_label)), None)
            if not paired_key:
                return sim_only("missing paired label")
            books_label = market[label_key].get("per_book", {})
            books_pair = market[paired_key].get("per_book", {})

        # H2H
        elif mkt_key.startswith("h2h"):
            if not label_key:
                return sim_only("missing label")
            paired_label = get_paired_label(label, mkt_key, game_id)
            paired_key = next((k for k in market if normalize_label(k) == normalize_label(paired_label)), None)
            if not paired_key:
                return sim_only("missing paired label")
            books_label = market[label_key].get("per_book", {})
            books_pair = market[paired_key].get("per_book", {})

        # SPREADS (includes alternate_spreads)
        elif mkt_key.startswith("spreads") or mkt_key.startswith("alternate_spreads"):
            try:
                team, line = label.split()
                line = line.replace("+0.0", "0.0").replace("-0.0", "0.0")

                # PK spreads → route to h2h market
                if line in {"0", "0.0"}:
                    h2h_market = market_odds.get(game_id, {}).get("h2h", {})
                    h2h_label = team
                    label_key = next((k for k in h2h_market if normalize_label(k) == normalize_label(h2h_label)), None)
                    paired_label = get_paired_label(h2h_label, "h2h", game_id)
                    paired_key = next((k for k in h2h_market if normalize_label(k) == normalize_label(paired_label)), None)
                    if not label_key or not paired_key:
                        return sim_only("PK line fallback failed")
                    books_label = h2h_market[label_key].get("per_book", {})
                    books_pair = h2h_market[paired_key].get("per_book", {})
                    mkt_key = "h2h"
                else:
                    if not label_key:
                        # try next market key if label missing
                        continue
                    team_full = TEAM_ABBR_TO_NAME.get(team, team)
                    opp_abbr = get_opponent_abbr_by_game_id(team_full, game_id)

                    if line.startswith("+"):
                        paired_label = f"{opp_abbr} -{line[1:]}"
                    elif line.startswith("-"):
                        paired_label = f"{opp_abbr} +{line[1:]}"
                    else:
                        return sim_only("invalid spread line")

                    paired_key = next((k for k in market if normalize_label(k) == normalize_label(paired_label)), None)
                    source_market = market  # default to current market

                    if not paired_key:
                        alt_mkt_key = (
                            mkt_key.replace("spreads", "alternate_spreads")
                            if mkt_key.startswith("spreads")
                            else mkt_key.replace("alternate_spreads", "spreads")
                        )
                        alt_market = market_odds.get(game_id, {}).get(alt_mkt_key, {})
                        paired_key = next((k for k in alt_market if normalize_label(k) == normalize_label(paired_label)), None)
                        source_market = alt_market

                        if paired_key:
                            if debug:
                                print(f"✅ Found paired label in {alt_mkt_key}: {paired_key}")
                        else:
                            if debug:
                                print(f"⚠️ Paired label not found for: '{paired_label}'")
                                print(f"   ➤ Normalized paired label: '{normalize_label(paired_label)}'")
                                print(f"   ➤ Keys in {mkt_key}: {[k for k in market]}")
                                print(f"   ➤ Normalized {mkt_key} keys: {[normalize_label(k) for k in market]}")
                                print(f"   ➤ Keys in {alt_mkt_key}: {[k for k in alt_market]}")
                                print(f"   ➤ Normalized {alt_mkt_key} keys: {[normalize_label(k) for k in alt_market]}")
                                print(f"🔍 Attempted to match normalized: '{normalize_label(paired_label)}'")
                                print(f"🔍 Comparisons against:")
                                for k in alt_market:
                                    print(f"   - '{k}' → '{normalize_label(k)}'")
                            return sim_only("missing paired label")

                    books_label = market[label_key].get("per_book", {})
                    books_pair = source_market[paired_key].get("per_book", {})
            except:
                return sim_only("spread parse error")

        else:
            return sim_only("unrecognized market")


        shared_books = [b for b in consensus_books if b in books_label and b in books_pair]
        if not shared_books:
            # attempt alternate market if we haven't already
            alt_mkt_key = (
                mkt_key.replace("spreads", "alternate_spreads")
                if mkt_key.startswith("spreads")
                else mkt_key.replace("alternate_spreads", "spreads")
            )
            alt_market = market_odds.get(game_id, {}).get(alt_mkt_key, {})
            alt_label_key = next((k for k in alt_market if normalize_label(k) == normalize_label(label)), None)
            alt_paired_key = next((k for k in alt_market if normalize_label(k) == normalize_label(paired_label)), None)
            if alt_label_key and alt_paired_key:
                alt_books_label = alt_market[alt_label_key].get("per_book", {})
                alt_books_pair = alt_market[alt_paired_key].get("per_book", {})
                alt_shared = [b for b in consensus_books if b in alt_books_label and b in alt_books_pair]
                if alt_shared:
                    books_label = alt_books_label
                    books_pair = alt_books_pair
                    shared_books = alt_shared
                    mkt_key = alt_mkt_key
            if not shared_books:
                return sim_only("no shared books")
     
        if len(shared_books) == 1 and not debug:
            should_log = not throttle_logs or game_id not in _DEVIG_WARNING_LOGGED
            if should_log:
                print(f"⚠️ Only 1 book used for devig → {shared_books[0]}")
                if throttle_logs:
                    _DEVIG_WARNING_LOGGED.add(game_id)

        book_probs = {}
        for book in shared_books:
            try:
                p1 = implied_prob(books_label[book])
                p2 = implied_prob(books_pair[book])
                total = p1 + p2
                if total == 0:
                    continue
                prob = round(p1 / total, 6)
                book_probs[book] = prob
            except:
                continue

        if not book_probs:

            return sim_only("no devigged values")

        avg_prob = round(sum(book_probs.values()) / len(book_probs), 6)
        fair_odds = to_american_odds(avg_prob)

        return {
            "consensus_prob": avg_prob,
            "fair_odds": fair_odds,
            "bookwise_probs": book_probs,
            "books_used": list(book_probs.keys()),
            "pricing_method": "devig",
        }, "devig"

    return sim_only("exhausted market keys")


def get_paired_label(label, market_key, game_id, point=None):
    if label.startswith("Over"):
        return f"Under {point}" if point is not None else label.replace("Over", "Under")
    if label.startswith("Under"):
        return f"Over {point}" if point is not None else label.replace("Under", "Over")

    if market_key.startswith("h2h"):
        try:
            away_abbr, home_abbr = get_teams_from_game_id(game_id)
            away_name = TEAM_ABBR_TO_NAME.get(away_abbr, away_abbr)
            home_name = TEAM_ABBR_TO_NAME.get(home_abbr, home_abbr)

            label_norm = normalize_label(label)
            if label_norm in {normalize_label(away_abbr), normalize_label(away_name)}:
                return home_abbr
            if label_norm in {normalize_label(home_abbr), normalize_label(home_name)}:
                return away_abbr
            return None
        except Exception:
            return None

    if market_key.startswith("spreads") or market_key.startswith("alternate_spreads"):
        try:
            team, line = label.split()
            opp_abbr = get_opponent_abbr_by_game_id(team, game_id)
            if line.startswith("+"):
                return normalize_label(f"{opp_abbr} -{line[1:]}")
            elif line.startswith("-"):
                return normalize_label(f"{opp_abbr} +{line[1:]}")
        except:
            return None

    return None


def get_opponent_abbr_by_game_id(team_name, game_id):
    away, home = get_teams_from_game_id(game_id)
    team_abbr = TEAM_NAME_TO_ABBR.get(team_name, team_name)
    return home if team_abbr.upper() == away.upper() else away


def extract_point(label):
    try:
        return float(label.split()[-1])
    except:
        return None