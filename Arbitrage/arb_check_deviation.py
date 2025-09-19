import pandas as pd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pinnacle_nfl_odds_A import fetch_pinnacle_nfl_df
from kalshi_nfl_odds_A import fetch_kalshi_nfl_df

TEAM_MAP = {
    "Arizona": "Arizona Cardinals",
    "Atlanta": "Atlanta Falcons",
    "Baltimore": "Baltimore Ravens",
    "Buffalo": "Buffalo Bills",
    "Carolina": "Carolina Panthers",
    "Chicago": "Chicago Bears",
    "Chicago B": "Chicago Bears",
    "Cincinnati": "Cincinnati Bengals",
    "Cleveland": "Cleveland Browns",
    "Dallas": "Dallas Cowboys",
    "Denver": "Denver Broncos",
    "Detroit": "Detroit Lions",
    "Green Bay": "Green Bay Packers",
    "Houston": "Houston Texans",
    "Indianapolis": "Indianapolis Colts",
    "Jacksonville": "Jacksonville Jaguars",
    "Kansas City": "Kansas City Chiefs",
    "Las Vegas": "Las Vegas Raiders",
    "Los Angeles C": "Los Angeles Chargers",
    "Los Angeles R": "Los Angeles Rams",
    "Miami": "Miami Dolphins",
    "Minnesota": "Minnesota Vikings",
    "New England": "New England Patriots",
    "New Orleans": "New Orleans Saints",
    "New York G": "New York Giants",
    "New York J": "New York Jets",
    "Philadelphia": "Philadelphia Eagles",
    "Pittsburgh": "Pittsburgh Steelers",
    "San Francisco": "San Francisco 49ers",
    "Seattle": "Seattle Seahawks",
    "Tampa Bay": "Tampa Bay Buccaneers",
    "Tennessee": "Tennessee Titans",
    "Washington": "Washington Commanders"
}

def normalize_team(name: str) -> str:
    return TEAM_MAP.get(name, name)

def detect_profitable_arbs():
    kalshi_df = fetch_kalshi_nfl_df()
    pinnacle_df = fetch_pinnacle_nfl_df()

    if kalshi_df.empty or pinnacle_df.empty:
        print("No data found.")
        return

    # normalize Kalshi team names
    kalshi_df["away"] = kalshi_df["title"].str.split(" at ").str[0].map(normalize_team)
    kalshi_df["home"] = kalshi_df["title"].str.split(" at ").str[1].map(normalize_team)

    # implied probabilities from Pinnacle moneyline
    pinnacle_df["home_prob"] = 1 / pinnacle_df["moneyline_home"]
    pinnacle_df["away_prob"] = 1 / pinnacle_df["moneyline_away"]

    # Kalshi probabilities with fee
    kalshi_df["yes_prob"] = kalshi_df["yes_ask"] * 1.035
    kalshi_df["no_prob"] = kalshi_df["no_ask"] * 1.035

    # merge datasets
    merged = kalshi_df.merge(
        pinnacle_df,
        on=["home", "away"],
        how="inner",
        suffixes=("_kalshi", "_pinnacle")
    ).drop_duplicates(subset=["home", "away"]).copy()

    # check arb sides
    merged["arb_home"] = merged["no_prob"] < merged["home_prob"]   # Kalshi NO vs Pinnacle home
    merged["arb_away"] = merged["yes_prob"] < merged["away_prob"]  # Kalshi YES vs Pinnacle away
    merged["has_arb"] = merged["arb_home"] | merged["arb_away"]

    # deviations (post-fee)
    merged["dev_home"] = merged["no_prob"] - merged["home_prob"]
    merged["dev_away"] = merged["yes_prob"] - merged["away_prob"]

    merged["arb_deviation"] = merged.apply(
        lambda row: row["dev_away"] if row["arb_away"] else (row["dev_home"] if row["arb_home"] else 0),
        axis=1
    )

    # profitable opportunities (arb_deviation < 0)
    profitable = merged[(merged["has_arb"]) & (merged["arb_deviation"] < 0)][[
        "home", "away", "arb_home", "arb_away", "arb_deviation",
        "no_prob", "home_prob", "yes_prob", "away_prob"
    ]]

    print("\nProfitable arbitrage opportunities (after 3.5% fee):")
    print(profitable.sort_values("arb_deviation"))

    # plot deviations
    plt.figure(figsize=(12,6))
    sns.histplot(profitable["arb_deviation"], bins=20, kde=True, color="green")
    plt.axvline(0, color='black', linestyle='--')
    plt.title("Distribution of Profitable Arbitrage Deviations (Kalshi vs Pinnacle, post-fee)")
    plt.xlabel("Deviation (Kalshi - Pinnacle, after fee)")
    plt.ylabel("Number of Games")
    plt.show()

if __name__ == "__main__":
    detect_profitable_arbs()