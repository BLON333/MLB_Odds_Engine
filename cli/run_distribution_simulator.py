#!/usr/bin/env python
# cli/run_distribution_simulator.py
# Fully revised script: simulates run distributions, builds derivative segments,
# provides CLI with --debug, --no-weather, --edge-threshold, --export-json, and --list
# Source base: run_distribution_simulator.py citeturn0file0

from core.config import DEBUG_MODE, VERBOSE_MODE
import re
import sys
import os
from core.bootstrap import *  # noqa
import json
import numpy as np
import tempfile
import matplotlib.pyplot as plt
from collections import Counter
from datetime import datetime

from core.logger import get_logger
logger = get_logger(__name__)

from core.game_simulator import simulate_game
from core.pricing_engine import MLBPricingEngine

SNAPSHOT_PATH = os.path.join("backtest", "last_table_snapshot.json")
from core.game_asset_builder import build_game_assets
from core.data_loader import load_all_stats
from assets.probable_pitchers import fetch_probable_pitchers
from core.stats_tools import summarize_pmf, calculate_tail_probability
from core.market_pricer import compute_moneyline, to_american_odds
from core.market_pricer import print_market_summary
from assets.env_builder import (
    get_park_name,
    get_park_factors,
    get_weather_hr_mult,
    get_noaa_weather,
    compute_weather_multipliers
)
from core.utils import (
    canonical_game_id,
    parse_game_id,
    get_teams_from_game_id,
    base_game_id,
    TEAM_ABBR_TO_NAME,
    TEAM_NAME_TO_ABBR,
    normalize_label_for_odds,
    safe_load_json,
    game_id_to_dt,
)
from core.scaling_utils import scale_distribution


N_SIMULATIONS = 10000

# Cache of loaded market odds by date
_MARKET_ODDS_CACHE: dict[str, dict | None] = {}


def _load_market_odds(date_str: str) -> dict | None:
    """Load market odds JSON for ``date_str`` once and cache the result."""
    if date_str not in _MARKET_ODDS_CACHE:
        path = os.path.join("data", "market_odds", f"{date_str}.json")
        _MARKET_ODDS_CACHE[date_str] = safe_load_json(path) if os.path.exists(path) else None
    return _MARKET_ODDS_CACHE[date_str]


def percent_in_range(scores, low=2, high=9):
    """
    Percentage of score events that fall within [low, high], inclusive.
    """
    return 100 * sum(1 for s in scores if low <= s <= high) / len(scores)

def pitcher_has_enrichment(p):
    return all(
        k in p
        and isinstance(p[k], (int, float))
        and not isinstance(p[k], str)
        for k in [
            "exit_velocity_avg",
            "launch_angle_avg",
            "barrel_batted_rate",
            "stuff_plus",
            "HR",
            "TBF"
        ]
    )


def apply_segment_scaling(values, target_mean=None, target_sd=None):
    """Compatibility wrapper using the general scale_distribution."""
    return scale_distribution(values, target_mean=target_mean, target_sd=target_sd)




def extract_universal_markets(game_id, full_game_market, derivative_segments, run_distribution=None):
    from core.market_pricer import to_american_odds, adjust_for_push
    from core.stats_tools import calculate_tail_probability
    from core.utils import normalize_to_abbreviation

    def build_entry(market, side, prob, odds):
        return {
            "market": market,
            "side": side,
            "sim_prob": round(prob, 4),
            "fair_odds": round(odds, 2),
            "source": "simulator"
        }

    away, home = get_teams_from_game_id(game_id)
    away = away.upper()
    home = home.upper()
    entries = []

    # === Moneyline (H2H)
    for team, obj in full_game_market.get("moneyline", {}).items():
        entries.append(build_entry("h2h", team, obj["prob"], obj["odds"]))  # team already abbreviated

    # === Spreads (Runlines)
    for team_abbr in [away, home]:
        for line in [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]:
            label = normalize_label_for_odds(team_abbr, "spreads", line)
            lookup_key = f"{team_abbr} {'+' if line > 0 else ''}{line}"
            spread_data = full_game_market.get("runline", {}).get(lookup_key, {})
            if "prob" in spread_data and "odds" in spread_data:
                entries.append(build_entry("spreads", label, spread_data["prob"], spread_data["odds"]))


    # === Totals
    if run_distribution is not None:
        for line in [x / 2 for x in range(13, 26)]:  # 6.5 to 12.5
            for side in ["Over", "Under"]:
                label = f"{side} {line}"
                prob = calculate_tail_probability(run_distribution, line, direction=side.lower())
                odds = to_american_odds(prob)

                # Handle push adjustment for integer totals
                if line % 1 == 0:
                    push_prob = calculate_tail_probability(run_distribution, line, direction="exact")
                    over = calculate_tail_probability(run_distribution, line, direction="over")
                    under = calculate_tail_probability(run_distribution, line, direction="under")
                    adj_over, adj_under = adjust_for_push(over, under)
                    prob = adj_over if side == "Over" else adj_under
                    odds = to_american_odds(prob)

                entries.append(build_entry("totals", label, prob, odds))

    # === Team Totals
    for raw_label, obj in full_game_market.get("team_totals", {}).items():
        label = normalize_to_abbreviation(raw_label.strip())
        entries.append(build_entry("team_totals", label, obj.get("prob"), obj.get("odds")))

    # === Derivative Markets
    segment_to_market_key = {
        "1st Inning": "1st_1_innings",
        "First 3 Innings": "1st_3_innings",
        "First 5 Innings": "1st_5_innings",
        "First 7 Innings": "1st_7_innings"
    }

    for segment, seg_data in derivative_segments.items():
        if segment not in segment_to_market_key:
            continue

        seg_suffix = segment_to_market_key[segment]
        for mkt_type, options in seg_data.get("markets", {}).items():
            market_prefix = {
                "moneyline": "h2h",
                "runline": "spreads",
                "total": "totals",
                "totals": "totals",
                "team_totals": "team_totals"
            }.get(mkt_type)

            if not market_prefix:
                continue

            market_name = f"{market_prefix}_{seg_suffix}"
            for raw_label, sim in options.items():
                if market_prefix in {"h2h", "spreads"}:
                    label = normalize_label_for_odds(raw_label.strip(), market_prefix)
                elif market_prefix == "team_totals":
                    label = normalize_label_for_odds(raw_label.strip(), "team_totals")
                else:
                    label = normalize_label_for_odds(raw_label.strip(), market_prefix)

                entries.append(build_entry(market_name, label, sim["prob"], sim["fair_odds"]))

    


    return entries


def simulate_distribution(game_id, line, debug=False, no_weather=False, edge_threshold=None, export_json=None, n_simulations=10000):
    from core.market_pricer import to_american_odds

    benchmark_totals = {
        "full_game": {
            "mean_total": 9.00,
            "std_total": 4.30,
            "mean_diff": 0.00,
            "std_diff": 4.30
        },
        "f1": {
            "mean_total": 1.05,
            "std_total": 1.55,
            "mean_diff": 0.00,
            "std_diff": 1.55
        },
        "f3": {
            "mean_total": 3.30,
            "std_total": 2.70,
            "mean_diff": 0.00,
            "std_diff": 2.70
        },
        "f5": {
            "mean_total": 5.40,
            "std_total": 3.35,
            "mean_diff": 0.00,
            "std_diff": 3.35
        },
        "f7": {
            "mean_total": 7.25,
            "std_total": 3.85,
            "mean_diff": 0.00,
            "std_diff": 3.85
        }
    }


    game_id = canonical_game_id(game_id)
    print(f"\n🔁 Simulating {n_simulations} games for {game_id} (Line: {line})...\n")

    parts = parse_game_id(game_id)
    away_abbr = parts["away"]
    home_abbr = parts["home"]
    game_date = parts["date"]
    if game_date > datetime.today().strftime("%Y-%m-%d"):
        print(f"[📅] Simulating a future game — projected lineups may be used.")

    odds_data = _load_market_odds(game_date)
    start_time_iso = None
    if isinstance(odds_data, dict):
        entry = odds_data.get(game_id) or odds_data.get(base_game_id(game_id))
        if isinstance(entry, dict):
            start_time_iso = entry.get("game_time") or entry.get("start_time")
    if not start_time_iso:
        dt = game_id_to_dt(game_id)
        start_time_iso = dt.isoformat() if dt else None

    batter_stats, pitcher_stats = load_all_stats()
    try:
        assets = build_game_assets(game_id, batter_stats, pitcher_stats)
        if assets is None:
            print(f"❌ build_game_assets() returned None.")
            return
    except Exception as e:
        print(f"❌ Asset build failed: {e}")
        return



    lineups = assets.get("lineups", {})
    pitcher_data = assets.get("pitchers", {})
    home_bullpen = assets.get("bullpens", {}).get("home", [])
    away_bullpen = assets.get("bullpens", {}).get("away", [])

    print("\n🔍 Pitcher Enrichment Check")
    for side in ["home", "away"]:
        p = pitcher_data[side]
        print(f"\n  ➤ {side.title()} Starter: {p.get('name', 'Unknown')}")
        print(f"      Stuff+: {p.get('stuff_plus', 'N/A')}")
        print(f"      Exit Velo: {p.get('exit_velocity_avg', 'N/A')}")
        print(f"      Launch Angle: {p.get('launch_angle_avg', 'N/A')}")
        print(f"      Barrel%: {p.get('barrel_batted_rate', 'N/A')}")
        print(f"      HR: {p.get('HR', 'N/A')} | TBF: {p.get('TBF', 'N/A')} | IP: {p.get('IP', 'N/A')}")
        print(f"      HR/PA Projection: {p.get('hr_pa', {}).get('hr_pa_projected', 'N/A')}")
        fallback_fields = []
        for k, fallback in {
            "stuff_plus": 100,
            "exit_velocity_avg": 88.0,
            "launch_angle_avg": 13.0,
            "barrel_batted_rate": 0.06
        }.items():
            val = p.get(k)
            if val is None or (isinstance(val, float) and round(val, 3) == fallback):
                fallback_fields.append(k)
        if fallback_fields:
            print(f"      ⚠️  Using fallback values for: {', '.join(fallback_fields)}")
        else:
            print(f"      ✅ Fully enriched with Statcast data")

    print("\n🔍 Lineup Stat Quality Check")
    for side in ["home", "away"]:
        team = lineups[side]
        print(f"  ➤ {side.title()} Lineup:")
        for b in team:
            fallback_keys = []
            for k, fallback in {
                "k_rate": 0.225, "bb_rate": 0.082, "iso": 0.145, "avg": 0.245, "woba": 0.320
            }.items():
                if b.get(k) is None or round(b.get(k), 4) == fallback:
                    fallback_keys.append(k)
            name = b.get("name", "Unknown")
            if fallback_keys:
                print(f"    - {name:20} | ⚠️  Fallbacks used: {', '.join(fallback_keys)}")
            else:
                print(f"    - {name:20} | ✅ OK")


    # Environment
    park_name = get_park_name(game_id)
    park_factors = get_park_factors(park_name)
    cache_path = f"data/weather_cache/{park_name.replace(' ', '_')}.json"
    if not no_weather:
        try:
            if os.path.exists(cache_path):
                weather_profile = safe_load_json(cache_path)
            else:
                weather_profile = get_noaa_weather(park_name)
            if not isinstance(weather_profile, dict):
                weather_profile = get_noaa_weather(park_name)
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, "w") as f:
                json.dump(weather_profile, f, indent=2)
        except Exception:
            weather_profile = {"wind_direction": "none", "wind_speed": 0, "temperature": 70, "humidity": 50}
    else:
        weather_profile = {"wind_direction": "none", "wind_speed": 0, "temperature": 70, "humidity": 50}

    weather_hr_mult = get_weather_hr_mult(weather_profile)
    weather_multipliers = compute_weather_multipliers(weather_profile)

    env = {
        "park_hr_mult": park_factors["hr_mult"],
        "single_mult": park_factors["single_mult"],
        "weather_hr_mult": weather_hr_mult,
        "adi_mult": weather_multipliers["adi_mult"],
        "umpire": {"K": 1.0, "BB": 1.0}
    }

    print("\n📦 Simulation Environment Config:")
    print(f"  Park: {park_name}")
    print(f"  Weather Profile: {weather_profile}")
    for k, v in env.items():
        print(f"  {k}: {v}")
    print(f"  Starters: {away_abbr} → {pitcher_data['away']['name']} | {home_abbr} → {pitcher_data['home']['name']}")

    # Load calibration
    calibration = safe_load_json("logs/calibration_offset.json")
    if not isinstance(calibration, dict):
        calibration = {}
    pricing_engine = MLBPricingEngine(calibration)

    print(f"\n🔧 Calibration Loaded:")
    print(f"   - Run Scaling:     x{pricing_engine.run_scaling_factor:.4f}")
    print(f"   - StdDev Scaling:  x{pricing_engine.stddev_scaling_factor:.4f}")
    print(f"   - RunDiff Scaling: x{pricing_engine.run_diff_scaling_factor:.4f}")
    print(
        f"   - Team Totals (Home): mean x{pricing_engine.home_mean_factor:.4f}, sd x{pricing_engine.home_std_factor:.4f}"
    )
    print(
        f"   - Team Totals (Away): mean x{pricing_engine.away_mean_factor:.4f}, sd x{pricing_engine.away_std_factor:.4f}"
    )

    # Run simulations
    raw_home_scores, raw_away_scores, all_results = [], [], []
    for i in range(n_simulations):
        result = simulate_game(
            home_lineup=lineups["home"],
            away_lineup=lineups["away"],
            home_pitcher=pitcher_data["home"],
            away_pitcher=pitcher_data["away"],
            env=env,
            home_bullpen=home_bullpen,
            away_bullpen=away_bullpen,
            use_noise=True
        )
        raw_home_scores.append(result["home_score"])
        raw_away_scores.append(result["away_score"])
        all_results.append(result)

        if i < 5:
            print(f"\n🧪 Simulation #{i + 1}")
            print(f"  ➤ Score: {away_abbr} {result['away_score']} — {home_abbr} {result['home_score']}")
            print(f"  ➤ Innings played: {len(result['innings'])}")
            print(f"  ➤ Away relievers used: {', '.join(result.get('used_away_relievers', [])) or 'None'}")
            print(f"  ➤ Home relievers used: {', '.join(result.get('used_home_relievers', [])) or 'None'}")

    # 🔁 Track reliever usage
    reliever_usage = {"home": {}, "away": {}}
    for res in all_results:
        for side in ["home", "away"]:
            used = res.get(f"used_{side}_relievers", [])
            for name in set(used):
                reliever_usage[side][name] = reliever_usage[side].get(name, 0) + 1

    print("\n📊 Reliever Usage Summary:")
    for side in ["home", "away"]:
        team = home_abbr if side == "home" else away_abbr
        print(f"\n  ➤ {side.title()} Bullpen ({team}):")
        if not reliever_usage[side]:
            print("    (None used)")
            continue
        total = sum(reliever_usage[side].values())
        sorted_usage = sorted(reliever_usage[side].items(), key=lambda x: x[1] / n_simulations, reverse=True)
        for name, count in sorted_usage:
            pct = 100 * count / n_simulations
            print(f"    - {name:20} → {pct:.1f}% of sims")

    # Extract raw segment scores before calibration
    segment_raw = {}
    for cap, key in [(1, "f1"), (3, "f3"), (5, "f5"), (7, "f7")]:
        home_seg = [sum(inn["home_runs"] for inn in r["innings"] if inn["inning"] <= cap) for r in all_results]
        away_seg = [sum(inn["away_runs"] for inn in r["innings"] if inn["inning"] <= cap) for r in all_results]
        totals_seg = [h + a for h, a in zip(home_seg, away_seg)]
        diffs_seg = [h - a for h, a in zip(home_seg, away_seg)]
        segment_raw[key] = {
            "total": totals_seg,
            "diff": diffs_seg,
            "home": home_seg,
            "away": away_seg,
        }

    # Raw distributions
    raw_totals = [h + a for h, a in zip(raw_home_scores, raw_away_scores)]
    raw_diffs = [h - a for h, a in zip(raw_home_scores, raw_away_scores)]
    raw_distributions = {
        "totals": {"values": raw_totals, "mean": float(np.mean(raw_totals)), "std": float(np.std(raw_totals))},
        "run_diffs": {"values": raw_diffs, "std": float(np.std(raw_diffs))},
    }

    seg_name_map = {
        "f1": "1st_1_innings",
        "f3": "1st_3_innings",
        "f5": "1st_5_innings",
        "f7": "1st_7_innings",
    }
    for seg_id, data in segment_raw.items():
        seg_key = seg_name_map[seg_id]
        raw_distributions[f"totals_{seg_key}"] = {
            "values": data["total"],
            "mean": float(np.mean(data["total"])),
            "std": float(np.std(data["total"])),
        }
        raw_distributions[f"run_diffs_{seg_key}"] = {
            "values": data["diff"],
            "std": float(np.std(data["diff"])),
        }

    scaled_totals = scale_distribution(
        raw_totals,
        target_mean=benchmark_totals["full_game"]["mean_total"],
        target_sd=benchmark_totals["full_game"]["std_total"],
    )
    scaled_diffs = scale_distribution(
        raw_diffs,
        target_sd=benchmark_totals["full_game"]["std_total"],
    )
    scaled_distributions = {
        "totals": {
            "values": scaled_totals,
            "mean": float(np.mean(scaled_totals)),
            "std": float(np.std(scaled_totals)),
        },
        "run_diffs": {
            "values": scaled_diffs,
            "std": float(np.std(scaled_diffs)),
        },
    }

    print(f"\n📊 PMF: totals_full_game")
    print(
        f"Raw Mean: {np.mean(raw_distributions['totals']['values']):.2f} → Scaled Mean: {np.mean(scaled_distributions['totals']['values']):.2f}"
    )
    print(
        f"Raw SD: {np.std(raw_distributions['totals']['values']):.2f} → Scaled SD: {np.std(scaled_distributions['totals']['values']):.2f}"
    )
    print(f"\n📊 PMF: spreads_full_game")
    print(
        f"Raw SD: {np.std(raw_distributions['run_diffs']['values']):.2f} → Scaled SD: {np.std(scaled_distributions['run_diffs']['values']):.2f}"
    )

    home_scores = [(t + d) / 2 for t, d in zip(scaled_totals, scaled_diffs)]
    away_scores = [(t - d) / 2 for t, d in zip(scaled_totals, scaled_diffs)]
    home_scores = pricing_engine.apply_team_total_scaling(home_scores, is_home=True)
    away_scores = pricing_engine.apply_team_total_scaling(away_scores, is_home=False)
    home_scores = [round(x, 1) for x in home_scores]
    away_scores = [round(x, 1) for x in away_scores]

    # Print basic summary
    print(f"\n🎯 Scaled Output:")
    print(f"   - Home Mean Score:  {np.mean(home_scores):.2f}")
    print(f"   - Away Mean Score:  {np.mean(away_scores):.2f}")
    print(f"   - Total Run Volatility: Std = {np.std(np.array(home_scores) + np.array(away_scores)):.2f}")

    # Compute PMFs
    total = np.array(home_scores) + np.array(away_scores)
    run_diff = np.array(scaled_distributions["run_diffs"]["values"])
    run_pmf_rounded = summarize_pmf(np.round(total).astype(int))
    run_pmf_raw = summarize_pmf(np.round(raw_distributions["totals"]["values"]).astype(int))
    run_diff_pmf = summarize_pmf(np.round(run_diff).astype(int))

    pmfs = {
        "totals": {
            "full_game": {
                "raw": summarize_pmf(np.round(raw_distributions["totals"]["values"]).astype(int)),
                "scaled": run_pmf_rounded,
            }
        },
        "spreads": {
            "full_game": {
                "raw": summarize_pmf(np.round(raw_distributions["run_diffs"]["values"]).astype(int)),
                "scaled": run_diff_pmf,
            }
        },
    }

    # === Build Full-Game Market with Alt Lines ===
    runline_dict = {}
    spread_lines = [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]
    for line in spread_lines:
        # Home -line (covering spread)
        prob_home_minus = calculate_tail_probability(run_diff_pmf, line, direction="over")
        prob_away_plus = 1 - prob_home_minus

        runline_dict[f"{home_abbr} -{line}"] = {
            "prob": round(prob_home_minus, 4),
            "odds": to_american_odds(prob_home_minus)
        }
        runline_dict[f"{away_abbr} +{line}"] = {
            "prob": round(prob_away_plus, 4),
            "odds": to_american_odds(prob_away_plus)
        }

        # Away -line (covering spread)
        prob_away_minus = calculate_tail_probability(run_diff_pmf, -line, direction="under")
        prob_home_plus = 1 - prob_away_minus

        runline_dict[f"{away_abbr} -{line}"] = {
            "prob": round(prob_away_minus, 4),
            "odds": to_american_odds(prob_away_minus)
        }
        runline_dict[f"{home_abbr} +{line}"] = {
            "prob": round(prob_home_plus, 4),
            "odds": to_american_odds(prob_home_plus)
        }



    total_dict = {}
    for line in [x / 2 for x in range(13, 26)]:  # 6.5 to 12.5
        over = calculate_tail_probability(run_pmf_rounded, line, direction="over")
        under = 1 - over
        if line % 1 == 0:
            # Push handling for integer lines
            push = calculate_tail_probability(run_pmf_rounded, line, direction="exact")
            denom = max(1.0 - push, 1e-6)
            over /= denom
            under /= denom
        total_dict[f"Over {line}"] = {
            "prob": round(over, 4),
            "odds": to_american_odds(over)
        }
        total_dict[f"Under {line}"] = {
            "prob": round(under, 4),
            "odds": to_american_odds(under)
        }

    moneyline = compute_moneyline(home_scores, away_scores)
    moneyline_dict = {
        away_abbr: {
            "prob": moneyline["away"]["prob"],
            "odds": to_american_odds(moneyline["away"]["prob"])
        },
        home_abbr: {
            "prob": moneyline["home"]["prob"],
            "odds": to_american_odds(moneyline["home"]["prob"])
        }
    }
    prob_over = calculate_tail_probability(run_pmf_rounded, threshold=line, direction="over")

    # === Team Totals — Full Game ===
    team_totals_dict = {}
    full_home_scores = home_scores
    full_away_scores = away_scores

    for line in [1.5, 2.5, 3.5, 4.5, 5.5, 6.5]:
        for team_abbr, scores in [(home_abbr, full_home_scores), (away_abbr, full_away_scores)]:
            pmf = summarize_pmf(np.round(scores).astype(int))
            p_over = calculate_tail_probability(pmf, line, direction="over")
            p_under = 1 - p_over
            over_label = normalize_label_for_odds(f"{team_abbr} Over", "team_totals", line)
            under_label = normalize_label_for_odds(f"{team_abbr} Under", "team_totals", line)
            team_totals_dict[over_label] = {
                "prob": round(p_over, 4),
                "odds": to_american_odds(p_over),
            }
            team_totals_dict[under_label] = {
                "prob": round(p_under, 4),
                "odds": to_american_odds(p_under),
            }

    full_game_market = {
        "moneyline": moneyline_dict,
        "runline": runline_dict,
        "total": {
            "line": line,
            "over": {
                "prob": prob_over,
                "odds": to_american_odds(prob_over)
            },
            "under": {
                "prob": 1 - prob_over,
                "odds": to_american_odds(1 - prob_over)
            }
        },
        "team_totals": team_totals_dict  # ✅ new addition
    }


    # === Derivative Segments with Alt Lines ===
    derivative_segments = {}
    segment_configs = {
        "F1": {
            "label": "1st Inning",
            "innings": 1,
            "total_lines": [0.5]
        },
        "F3": {
            "label": "First 3 Innings",
            "innings": 3,
            "total_lines": [2.5, 3.5, 4.5],
            "spread_lines": [0.5, 1.5, 2.5],
            "team_total_lines": [0.5, 1.5, 2.5, 3.5]
        },
        "F5": {
            "label": "First 5 Innings",
            "innings": 5,
            "total_lines": [3.5, 4.5, 5.5, 6.5],
            "spread_lines": [0.5, 1.5, 2.5],
            "team_total_lines": [1.5, 2.5, 3.5, 4.5, 5.5]
        },
        "F7": {
            "label": "First 7 Innings",
            "innings": 7,
            "total_lines": [5.5, 6.5, 7.5, 8.5],
            "spread_lines": [0.5, 1.5, 2.5],
            "team_total_lines": [2.5, 3.5, 4.5, 5.5, 6.5]
        }
    }


    key_map = {"F1": "f1", "F3": "f3", "F5": "f5", "F7": "f7"}

    for seg_key, config in segment_configs.items():
        label = config["label"]
        innings_cap = config["innings"]
        stats = compute_partial_derivatives(all_results, innings_cap)
        seg = {"label": label, "markets": {}}

        seg_id = key_map.get(seg_key)
        seg_cal = pricing_engine.segment_scaling.get(seg_id, {})
        seg_totals_scaled = apply_segment_scaling(
            segment_raw[seg_id]["total"],
            target_mean=seg_cal.get("run_mean"),
            target_sd=seg_cal.get("run_sd")
        )
        seg_diffs_scaled = apply_segment_scaling(
            segment_raw[seg_id]["diff"],
            target_mean=None,
            target_sd=seg_cal.get("diff_sd")
        )


        seg_key_name = seg_name_map[seg_id]

        scaled_distributions[f"totals_{seg_key_name}"] = {
            "values": seg_totals_scaled,
            "mean": float(np.mean(seg_totals_scaled)),
            "std": float(np.std(seg_totals_scaled)),
        }
        scaled_distributions[f"run_diffs_{seg_key_name}"] = {
            "values": seg_diffs_scaled,
            "std": float(np.std(seg_diffs_scaled)),
        }

        pmf_total_seg = summarize_pmf(np.round(scaled_distributions[f"totals_{seg_key_name}"]["values"]).astype(int))
        pmf_diff_seg = summarize_pmf(np.round(scaled_distributions[f"run_diffs_{seg_key_name}"]["values"]).astype(int))

        pmfs["totals"][seg_key_name] = {
            "raw": summarize_pmf(np.round(raw_distributions[f"totals_{seg_key_name}"]["values"]).astype(int)),
            "scaled": pmf_total_seg,
        }
        pmfs["spreads"][seg_key_name] = {
            "raw": summarize_pmf(np.round(raw_distributions[f"run_diffs_{seg_key_name}"]["values"]).astype(int)),
            "scaled": pmf_diff_seg,
        }

        print(f"\n📊 PMF: totals_{seg_key_name}")
        print(
            f"Raw Mean: {np.mean(raw_distributions[f'totals_{seg_key_name}']["values"]):.2f} → Scaled Mean: {np.mean(scaled_distributions[f'totals_{seg_key_name}']["values"]):.2f}"
        )
        print(
            f"Raw SD: {np.std(raw_distributions[f'totals_{seg_key_name}']["values"]):.2f} → Scaled SD: {np.std(scaled_distributions[f'totals_{seg_key_name}']["values"]):.2f}"
        )
        print(f"\n📊 PMF: spreads_{seg_key_name}")
        print(
            f"Raw SD: {np.std(raw_distributions[f'run_diffs_{seg_key_name}']["values"]):.2f} → Scaled SD: {np.std(scaled_distributions[f'run_diffs_{seg_key_name}']["values"]):.2f}"
        )

        totals = {}
        for line in config.get("total_lines", []):
            over_prob = calculate_tail_probability(pmf_total_seg, line, direction="over")
            under_prob = 1 - over_prob
            totals[f"Over {line}"] = {"prob": round(over_prob, 4), "fair_odds": to_american_odds(over_prob)}
            totals[f"Under {line}"] = {"prob": round(under_prob, 4), "fair_odds": to_american_odds(under_prob)}
        seg["markets"]["totals"] = totals

        if seg_key == "F1":
            # Special case: "Score in 1st inning"
            p = calculate_tail_probability(pmf_total_seg, 0.5, direction="over")
            seg["markets"]["totals"] = {
                "Over 0.5": {"prob": p, "fair_odds": to_american_odds(p)},
                "Under 0.5": {"prob": 1 - p, "fair_odds": to_american_odds(1 - p)}
            }
        else:
            # Moneyline
            ml = stats["moneyline"]
            seg["markets"]["moneyline"] = {
                home_abbr: {"prob": ml["home"], "fair_odds": to_american_odds(ml["home"])},
                away_abbr: {"prob": ml["away"], "fair_odds": to_american_odds(ml["away"])}
            }

            # Spreads
            spreads = {}
            for line in config.get("spread_lines", []):
                # Home -line
                prob_home_minus = calculate_tail_probability(pmf_diff_seg, line, direction="over")
                prob_away_plus = 1 - prob_home_minus

                spreads[f"{home_abbr} -{line}"] = {
                    "prob": round(prob_home_minus, 4),
                    "fair_odds": to_american_odds(prob_home_minus)
                }
                spreads[f"{away_abbr} +{line}"] = {
                    "prob": round(prob_away_plus, 4),
                    "fair_odds": to_american_odds(prob_away_plus)
                }

                # Away -line
                prob_away_minus = calculate_tail_probability(pmf_diff_seg, -line, direction="under")
                prob_home_plus = 1 - prob_away_minus

                spreads[f"{away_abbr} -{line}"] = {
                    "prob": round(prob_away_minus, 4),
                    "fair_odds": to_american_odds(prob_away_minus)
                }
                spreads[f"{home_abbr} +{line}"] = {
                    "prob": round(prob_home_plus, 4),
                    "fair_odds": to_american_odds(prob_home_plus)
                }


            seg["markets"]["runline"] = spreads

            # Team Totals
            team_totals = {}
            home_scores = [sum(inn["home_runs"] for inn in r["innings"] if inn["inning"] <= innings_cap) for r in all_results]
            away_scores = [sum(inn["away_runs"] for inn in r["innings"] if inn["inning"] <= innings_cap) for r in all_results]
            home_scores = pricing_engine.apply_team_total_scaling(home_scores, is_home=True)
            away_scores = pricing_engine.apply_team_total_scaling(away_scores, is_home=False)

            for line in config.get("team_total_lines", []):
                for team_abbr, scores in [(home_abbr, home_scores), (away_abbr, away_scores)]:
                    pmf = summarize_pmf(np.round(scores).astype(int))
                    p_over = calculate_tail_probability(pmf, line, direction="over")
                    p_under = 1 - p_over
                    over_label = normalize_label_for_odds(f"{team_abbr} Over", "team_totals", line)
                    under_label = normalize_label_for_odds(f"{team_abbr} Under", "team_totals", line)
                    team_totals[over_label] = {
                        "prob": round(p_over, 4),
                        "fair_odds": to_american_odds(p_over),
                    }
                    team_totals[under_label] = {
                        "prob": round(p_under, 4),
                        "fair_odds": to_american_odds(p_under),
                    }

            seg["markets"]["team_totals"] = team_totals


        derivative_segments[label] = seg

    # === Output JSON ===
    date_tag = "-".join(game_id.split("-")[:3])
    target_path = export_json or os.path.join("backtest", "sims", date_tag, f"{game_id}.json")

    # 📊 Segment-level summaries
    def inning_summary(inning_cap, label, benchmark=None):
        home = [sum(inn["home_runs"] for inn in r["innings"] if inn["inning"] <= inning_cap) for r in all_results]
        away = [sum(inn["away_runs"] for inn in r["innings"] if inn["inning"] <= inning_cap) for r in all_results]
        home = pricing_engine.apply_team_total_scaling(home, is_home=True)
        away = pricing_engine.apply_team_total_scaling(away, is_home=False)
        summary = summarize_distribution(home, away, label, benchmark=benchmark)
        return summary, home, away

    summary_full = summarize_distribution(raw_home_scores, raw_away_scores, label="Full Game", benchmark=benchmark_totals["full_game"])
    summary_f1, home_f1, away_f1 = inning_summary(1, "First Inning", benchmark=benchmark_totals["f1"])
    summary_f3, home_f3, away_f3 = inning_summary(3, "First 3 Innings", benchmark=benchmark_totals["f3"])
    summary_f5, home_f5, away_f5 = inning_summary(5, "First 5 Innings", benchmark=benchmark_totals["f5"])
    summary_f7, home_f7, away_f7 = inning_summary(7, "First 7 Innings", benchmark=benchmark_totals["f7"])

    # ✅ Extract markets into memory first
    markets_debug = extract_universal_markets(
        game_id,
        full_game_market,
        derivative_segments,
        run_distribution=run_pmf_rounded
    )

    print(f"\n🧪 Market Entries Extracted: {len(markets_debug)}")

    output = {
        "home_score": float(np.mean(raw_home_scores)),
        "away_score": float(np.mean(raw_away_scores)),
        "start_time_iso": start_time_iso,
        "run_distribution": run_pmf_rounded,
        "run_distribution_raw": run_pmf_raw,
        "run_diff_distribution": run_diff_pmf,
        "pmfs": pmfs,
        "raw_distributions": raw_distributions,
        "scaled_distributions": scaled_distributions,
        "markets": markets_debug,
        "summary_metrics": {
            "full_game": summary_full,
            "f1": summary_f1,
            "f3": summary_f3,
            "f5": summary_f5,
            "f7": summary_f7
        }
    }

    # ✅ Save output atomically
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=os.path.dirname(target_path), delete=False, suffix=".tmp") as tmpf:
        json.dump(output, tmpf, indent=2)
        temp_path = tmpf.name
    os.replace(temp_path, target_path)

    # Write simplified snapshot for downstream comparison
    snapshot_dict = {
        f"{e['market']}:{e['side']}": {"fair_odds": e.get('fair_odds'), "ev_percent": e.get('ev_percent')}
        for e in markets_debug
    }
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshot_dict, f, indent=2)

    # 🧠 Fatigue debug summary
    print_fatigue_summary(pitcher_data["home"], f"{pitcher_data['home']['name']} (Home Starter)")
    print_fatigue_summary(pitcher_data["away"], f"{pitcher_data['away']['name']} (Away Starter)")

    print(f"\n💾 Saved simulation output → {target_path}")



def print_fatigue_summary(pitcher_data, label):
    fatigue_log = pitcher_data.get("fatigue_log")
    if not fatigue_log:
        return

    print(f"\n📊 Fatigue Summary — {label}")
    for inning in sorted(fatigue_log):
        entries = fatigue_log[inning]
        avg_k = np.mean([e["k_rate"] for e in entries])
        avg_bb = np.mean([e["bb_rate"] for e in entries])
        avg_stuff = np.mean([e["stuff_plus"] for e in entries])
        avg_loc = np.mean([e["location_plus"] for e in entries])
        print(f"  Inning {inning}: K%={avg_k:.3f}, BB%={avg_bb:.3f}, Stuff+={avg_stuff:.1f}, Loc+={avg_loc:.1f}")

def summarize_distribution(home_scores, away_scores, label="Full Game", benchmark=None):
    diffs = np.array(home_scores) - np.array(away_scores)
    totals = np.array(home_scores) + np.array(away_scores)

    metrics = {
        "mean_total": float(totals.mean()),
        "std_total": float(totals.std()),
        "mean_diff": float(diffs.mean()),
        "std_diff": float(diffs.std())
    }

    def delta_line(key, val):
        if benchmark and key in benchmark:
            delta = val - benchmark[key]
            return f"{val:.2f}   (Δ vs MLB: {delta:+.2f})"
        return f"{val:.2f}"

    print(f"\n📊 Summary — {label}")
    print(f"  ➤ Mean Total Runs:       {delta_line('mean_total', metrics['mean_total'])}")
    print(f"  ➤ Std Total Runs:        {delta_line('std_total', metrics['std_total'])}")
    print(f"  ➤ Mean Run Differential: {delta_line('mean_diff', metrics['mean_diff'])}")
    print(f"  ➤ Std Run Differential:  {delta_line('std_diff', metrics['std_diff'])}")

    return metrics




# ----------------------------
# CLI ARG PARSING
# ----------------------------
def resolve_game_id_from_args():
    import datetime
    import re
    args = sys.argv[1:]
    debug = "--debug" in args
    no_weather = "--no-weather" in args
    export_json = None
    export_folder = "backtest/sims"  # default folder path
    edge_threshold = None
    days_ahead = 1

    # Handle optional argument values like --export-json=path or --edge-threshold=0.05
    for arg in args:
        if arg.startswith("--export-json="):
            export_json = arg.split("=")[1]
        elif arg.startswith("--edge-threshold="):
            edge_threshold = float(arg.split("=")[1])
        elif arg.startswith("--days-ahead="):
            days_ahead = int(arg.split("=")[1])

    cleaned = [arg for arg in args if not arg.startswith("--")]

    # ✅ Handle --list or no args provided
    if "--list" in args or (not cleaned and "--mode" not in args):
        from assets.probable_pitchers import fetch_probable_pitchers
        matchups = fetch_probable_pitchers(days_ahead=days_ahead)
        print(f"\n📋 Available Game IDs (next {days_ahead + 1} days):")
        for gid in sorted(matchups.keys()):
            print("  ", gid)
        sys.exit()

    # ✅ Full slate mode (by date)
    if "--mode" in args and "full_slate" in args:
        if len(cleaned) >= 1 and re.match(r"^\d{4}-\d{2}-\d{2}$", cleaned[0]):
            return cleaned[0], debug, no_weather, 9.5, edge_threshold, export_json, export_folder
        else:
            today = str(datetime.date.today())
            return today, debug, no_weather, 9.5, edge_threshold, export_json, export_folder

    # ✅ Distribution mode (expects game ID + optional line)
    gid = cleaned[0] if cleaned else None
    line = float(cleaned[1]) if len(cleaned) > 1 else 9.5

    return gid, debug, no_weather, line, edge_threshold, export_json, export_folder




# ----------------------------
# PARTIAL DERIVATIVES
# ----------------------------
def compute_partial_derivatives(all_results, innings_range):
    total_runs=[]; wh=0; wa=0; pushes=0; rl_away=0
    for res in all_results:
        h=a=0
        for inn in res['innings']:
            if inn['inning']>innings_range: break
            h+=inn['home_runs']; a+=inn['away_runs']
        total_runs.append(h+a)
        if h>a: wh+=1
        elif a>h: wa+=1
        else: pushes+=1
        if a+0.5>h: rl_away+=1
    sims=len(all_results)
    return {"avg_total":round(np.mean(total_runs),2),
            "moneyline":{"home":round(wh/sims,3),"away":round(wa/sims,3),"push":round(pushes/sims,3)},
            "total_overs":{"{:.1f}".format(np.floor(np.mean(total_runs))+0.5):round(np.mean(np.array(total_runs)>(np.floor(np.mean(total_runs))+0.5)),3)},
            "runline":{"away_plus_half":round(rl_away/sims,3)},
            "score_1plus":round(np.mean(np.array(total_runs)>0),3)}


# ----------------------------
# MAIN ENTRYPOINT
# ----------------------------
if __name__ == "__main__":
    gid, debug, no_weather, line, edge_threshold, export_json, export_folder = resolve_game_id_from_args()

    simulate_distribution(
        game_id=gid,
        line=line,
        debug=debug,
        no_weather=no_weather,
        edge_threshold=edge_threshold,
        export_json=export_json
    )