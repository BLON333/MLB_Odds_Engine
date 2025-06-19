import sys
import os
from core.bootstrap import *  # noqa
from core.config import DEBUG_MODE, VERBOSE_MODE


import numpy as np
import csv
import argparse
from collections import defaultdict
from datetime import datetime, timedelta

from core.logger import get_logger
logger = get_logger(__name__)

def decimal_odds(american):
    return 100 / abs(american) + 1 if american < 0 else american / 100 + 1

def grade_bet(row, bankroll_per_unit):
    try:
        stake_units = float(row["stake"])
        odds = float(row["market_odds"])
        result = row.get("result", "").strip().lower()
    except:
        return 0.0

    stake_dollars = stake_units * bankroll_per_unit
    if result == "win":
        return stake_dollars * (decimal_odds(odds) - 1)
    elif result == "loss":
        return -stake_dollars
    elif result == "push":
        return 0.0
    else:
        return 0.0

def run_bankroll_sim(log_path, starting_bankroll=40000, unit_percent=1.0, start_date=None, end_date=None):
    bet_rows = []

    def colorize(value, is_percent=False):
        value_str = f"{value:+.2f}%" if is_percent else f"${value:+.2f}"
        return f"\033[92m{value_str}\033[0m" if value >= 0 else f"\033[91m{value_str}\033[0m"

    bankroll_per_unit = starting_bankroll * (unit_percent / 100.0)

    total_profit = 0.0
    total_bets = 0
    total_wins = total_losses = total_pushes = 0
    total_staked = 0.0
    total_bankroll = starting_bankroll

    market_stats = defaultdict(lambda: {"profit": 0.0, "bets": 0, "staked": 0.0})
    ev_buckets = {
        "<3%": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0},
        "3%-5%": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0},
        "5%-8%": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0},
        "8%-12%": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0},
        "12%-20%": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0},
        "20%+": {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "staked": 0.0}
    }

    daily_stats = defaultdict(lambda: {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "pushes": 0, "staked": 0.0})
    time_buckets = {
        "<2h": {"profit": 0.0, "bets": 0, "staked": 0.0},
        "2-6h": {"profit": 0.0, "bets": 0, "staked": 0.0},
        "6-12h": {"profit": 0.0, "bets": 0, "staked": 0.0},
        "12h+": {"profit": 0.0, "bets": 0, "staked": 0.0}
    }

    ev_5_20_no_h2h = {"profit": 0.0, "bets": 0, "wins": 0, "losses": 0, "pushes": 0, "staked": 0.0}
    top_ev_market_stats = defaultdict(lambda: {"profit": 0.0, "bets": 0, "staked": 0.0})

    today = datetime.today()
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else datetime.strptime(yesterday_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else start_dt

    with open(log_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result = row.get("result", "").strip().lower()
            if result not in {"win", "loss", "push"}:
                continue

            game_id = row.get("game_id", "")
            try:
                date_obj = datetime.strptime("-".join(game_id.split("-")[:3]), "%Y-%m-%d")
            except:
                continue

            delta = grade_bet(row, bankroll_per_unit)
            total_bankroll += delta
            total_profit += delta

            total_bets += 1
            if result == "win":
                total_wins += 1
            elif result == "loss":
                total_losses += 1
            elif result == "push":
                total_pushes += 1

            try:
                stake = float(row["stake"])
                odds = float(row["market_odds"])
                market = row.get("market", "Unknown")
                ev_percent = float(row.get("ev_percent", 0))
                hours_to_game_str = row.get("hours_to_game", "").strip()
                hours_to_game = float(hours_to_game_str) if hours_to_game_str else 8.0
                staked_amount = stake * bankroll_per_unit
                total_staked += staked_amount
            except:
                continue

            if hours_to_game < 2:
                bucket = "<2h"
            elif 2 <= hours_to_game < 6:
                bucket = "2-6h"
            elif 6 <= hours_to_game < 12:
                bucket = "6-12h"
            else:
                bucket = "12h+"
            time_buckets[bucket]["profit"] += delta
            time_buckets[bucket]["bets"] += 1
            time_buckets[bucket]["staked"] += staked_amount

            market_stats[market]["profit"] += delta
            market_stats[market]["bets"] += 1
            market_stats[market]["staked"] += staked_amount

            if ev_percent < 3:
                bucket_ev = "<3%"
            elif ev_percent < 5:
                bucket_ev = "3%-5%"
            elif ev_percent < 8:
                bucket_ev = "5%-8%"
            elif ev_percent < 12:
                bucket_ev = "8%-12%"
            elif ev_percent < 20:
                bucket_ev = "12%-20%"
            else:
                bucket_ev = "20%+"
            ev_buckets[bucket_ev]["profit"] += delta
            ev_buckets[bucket_ev]["bets"] += 1
            ev_buckets[bucket_ev]["staked"] += staked_amount
            if result == "win":
                ev_buckets[bucket_ev]["wins"] += 1
            elif result == "loss":
                ev_buckets[bucket_ev]["losses"] += 1

            if 5 <= ev_percent < 20:
                ev_5_20_no_h2h["profit"] += delta
                ev_5_20_no_h2h["bets"] += 1
                ev_5_20_no_h2h["staked"] += staked_amount
                if result == "win":
                    ev_5_20_no_h2h["wins"] += 1
                elif result == "loss":
                    ev_5_20_no_h2h["losses"] += 1
                elif result == "push":
                    ev_5_20_no_h2h["pushes"] += 1

                top_ev_market_stats[market]["profit"] += delta
                top_ev_market_stats[market]["bets"] += 1
                top_ev_market_stats[market]["staked"] += staked_amount

            if start_dt <= date_obj <= end_dt:
                date_key = date_obj.strftime("%Y-%m-%d")
                daily_stats[date_key]["profit"] += delta
                daily_stats[date_key]["bets"] += 1
                daily_stats[date_key]["staked"] += staked_amount
                if result == "win":
                    daily_stats[date_key]["wins"] += 1
                elif result == "loss":
                    daily_stats[date_key]["losses"] += 1
                elif result == "push":
                    daily_stats[date_key]["pushes"] += 1
                row["date"] = date_key
                row["ev"] = ev_percent
                row["market"] = market
                row["result_amount"] = delta
                row["result"] = result
                bet_rows.append(row)

    # ========== REPORTING SECTION ==========
    for date_key, stats in sorted(daily_stats.items()):
        filtered_bets = [
            row for row in bet_rows
            if row["date"] == date_key and float(row.get("ev", 0)) <= 20

        ]
        if not filtered_bets:
            continue
        profit = sum(float(r["result_amount"]) for r in filtered_bets)
        staked = sum(float(r["stake"]) * bankroll_per_unit for r in filtered_bets)
        roi = (profit / staked * 100) if staked > 0 else 0.0
        wins = sum(1 for r in filtered_bets if r["result"] == "win")
        losses = sum(1 for r in filtered_bets if r["result"] == "loss")
        pushes = sum(1 for r in filtered_bets if r["result"] == "push")
        emoji = "📈" if profit >= 0 else "📉"
        print(f"\n📆 Daily Recap (EV 5%–20% Bets) — {date_key}")
        print("-------------------------------------")
        print(
            f"{emoji} Profit: {colorize(profit)} | ROI: {colorize(roi, is_percent=True)} | "
            f"Bets: {len(filtered_bets)} (W:{wins} / L:{losses} / P:{pushes})"
        )

    if ev_5_20_no_h2h["bets"] > 0:
        roi_5_20 = (
            ev_5_20_no_h2h["profit"] / ev_5_20_no_h2h["staked"] * 100
        ) if ev_5_20_no_h2h["staked"] > 0 else 0.0
        winrate_5_20 = (ev_5_20_no_h2h["wins"] / ev_5_20_no_h2h["bets"]) * 100
        print("\n🎯 EV 5%-20% BETS SUMMARY")
        print("-------------------------------------")
        print(f"Total Bets: {ev_5_20_no_h2h['bets']}")
        print(f"Profit:     {colorize(ev_5_20_no_h2h['profit'])}")
        print(f"ROI:        {colorize(roi_5_20, is_percent=True)}")
        print(f"Win Rate:   {colorize(winrate_5_20, is_percent=True)} ({ev_5_20_no_h2h['wins']} W / {ev_5_20_no_h2h['losses']} L / {ev_5_20_no_h2h['pushes']} P)")

    print("\n" + "-" * 40)

    roi = (total_profit / total_staked * 100) if total_staked > 0 else 0.0
    win_rate = (total_wins / total_bets) * 100 if total_bets else 0
    print("\n**🏦 Overall Bankroll Performance to Date**")
    print("------------------------------")
    print(f"Start Bankroll:     ${starting_bankroll:.2f}")
    print(f"Final Bankroll:     {colorize(total_bankroll)}")
    print(f"Total Profit:       {colorize(total_profit)}")
    print(f"ROI:                {colorize(roi, is_percent=True)}")
    print(f"Total Bets Graded:  {total_bets}")
    print(f"Win Rate:           {colorize(win_rate, is_percent=True)} ({total_wins} W / {total_losses} L / {total_pushes} P)")
    print("------------------------------")

    if market_stats:
        print("\n📊 MARKET PERFORMANCE SUMMARY")
        print("------------------------------------------------")
        def market_roi(item):
            profit = item[1]["profit"]
            staked = item[1]["staked"]
            return profit / staked if staked > 0 else float('-inf')

        sorted_markets = sorted(market_stats.items(), key=market_roi, reverse=True)
        for market, stats in sorted_markets:
            staked = stats["staked"]
            roi = (stats["profit"] / staked * 100) if staked > 0 else 0.0
            print(
                f"    - {market:<22} | {stats['bets']:>3} bets | Profit: {colorize(stats['profit'])} | "
                f"Staked: ${staked:.2f} | ROI: {colorize(roi, is_percent=True)}"
            )

    print("\n📈 ROI by EV% Range:")
    for bucket, stats in ev_buckets.items():
        roi = (stats["profit"] / stats["staked"] * 100) if stats["staked"] > 0 else 0.0
        winrate = (stats["wins"] / stats["bets"] * 100) if stats["bets"] else 0.0
        print(f"    - {bucket:<7} | {stats['bets']:>3} bets | ROI: {colorize(roi, is_percent=True)} | Win Rate: {colorize(winrate, is_percent=True)}")

    if sum(stats["bets"] for stats in time_buckets.values()) > 0:
        print("\n⏰ ROI by Hours-to-Game Window:")
        for bucket, stats in time_buckets.items():
            roi = (stats["profit"] / stats["staked"] * 100) if stats["staked"] > 0 else 0.0
            print(f"    - {bucket:<5} | {stats['bets']:>3} bets | ROI: {colorize(roi, is_percent=True)}")
    else:
        print("\n⏰ ROI by Hours-to-Game Window: [No valid hours_to_game data available]")

    print("\n✅ Graded all logged bets.\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate bankroll from graded bets")
    parser.add_argument("--log", required=True, help="Path to market_evals.csv")
    parser.add_argument("--bankroll", type=float, default=1000.0, help="Starting bankroll")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    run_bankroll_sim(
        log_path=args.log,
        starting_bankroll=args.bankroll,
        start_date=args.start_date,
        end_date=args.end_date
    )