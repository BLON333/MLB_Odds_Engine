import argparse
import pandas as pd

from core.logger import get_logger

logger = get_logger(__name__)


def format_units(value: float) -> str:
    """Return value formatted in betting units."""
    return f"{value:.2f}u"


def format_roi(value: float) -> str:
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.1f}%"


def format_table(headers, rows):
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    lines = [sep,
             "| " + " | ".join(f"{headers[i]:{widths[i]}}" for i in range(len(headers))) + " |",
             sep]
    for row in rows:
        lines.append("| " + " | ".join(f"{str(row[i]):{widths[i]}}" for i in range(len(headers))) + " |")
    lines.append(sep)
    return "\n".join(lines)


def assign_ev_bucket(ev: float) -> str:
    if ev < 10:
        return "5-10%"
    if ev < 15:
        return "10-15%"
    if ev < 20:
        return "15-20%"
    return "20%+"


def load_bets(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["result"] = pd.to_numeric(df.get("result"), errors="coerce")
    df = df[df["result"].notna()]
    df["stake"] = pd.to_numeric(df.get("stake"), errors="coerce").fillna(0)
    df["ev_percent"] = pd.to_numeric(df.get("ev_percent"), errors="coerce").fillna(0)
    df["market_odds"] = pd.to_numeric(df.get("market_odds"), errors="coerce")
    df["date"] = pd.to_datetime(df["date_simulated"]).dt.date
    return df


def summarize(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    grp = df.groupby(group_col).agg(Bets=("stake", "size"),
                                    Staked=("stake", "sum"),
                                    Profit=("result", "sum"))
    grp["ROI"] = grp["Profit"] / grp["Staked"] * 100
    return grp


def print_summary(title: str, df: pd.DataFrame, group_col: str, header: str) -> None:
    data = summarize(df, group_col)
    if data.empty:
        return
    rows = []
    for idx, row in data.sort_index().iterrows():
        if row["Bets"] == 0:
            continue
        rows.append([
            idx,
            int(row["Bets"]),
            format_units(row["Staked"]),
            format_units(row["Profit"]),
            format_roi(row["ROI"]),
        ])
    if rows:
        print(f"\n{title}")
        print(format_table([header, "Bets", "Staked", "Profit", "ROI"], rows))


def print_best_worst(df: pd.DataFrame) -> None:
    top = df.sort_values("result", ascending=False).head(5)
    bottom = df.sort_values("result").head(5)

    if not top.empty:
        rows = [
            [r.game_id, r.side, f"{r.ev_percent:.2f}%", format_units(r.stake), format_units(r.result), r.market_odds, r.best_book]
            for r in top.itertuples()
        ]
        print("\n🏅 Best Bets")
        print(format_table(["Game", "Side", "EV%", "Stake", "Result", "Odds", "Book"], rows))

    if not bottom.empty:
        rows = [
            [r.game_id, r.side, f"{r.ev_percent:.2f}%", format_units(r.stake), format_units(r.result), r.market_odds, r.best_book]
            for r in bottom.itertuples()
        ]
        print("\n🔻 Worst Bets")
        print(format_table(["Game", "Side", "EV%", "Stake", "Result", "Odds", "Book"], rows))


def main(log_path: str) -> None:
    df = load_bets(log_path)
    if df.empty:
        print("No graded bets found.")
        return

    df["ev_bucket"] = df["ev_percent"].apply(assign_ev_bucket)

    if "market_class" in df.columns:
        df["line_type"] = df["market_class"].str.contains("alt", case=False, na=False)
        df["line_type"] = df["line_type"].map({True: "alternate", False: "mainline"})
    else:
        df["line_type"] = df["market"].str.contains("alternate", case=False, na=False)
        df["line_type"] = df["line_type"].map({True: "alternate", False: "mainline"})

    print_summary("📅 Daily Summary", df, "date", "Date")
    print_summary("🔍 EV Buckets", df, "ev_bucket", "Bucket")
    print_summary("📊 Market Type Performance", df, "market", "Market")
    print_summary("🧭 Segment Type Performance", df, "segment", "Segment")
    print_summary("📈 Mainline vs Alternate", df, "line_type", "Type")
    print_best_worst(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bankroll performance summary")
    parser.add_argument("--log", required=True, help="Path to market_evals.csv")
    args = parser.parse_args()
    main(args.log)
