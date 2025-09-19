"""
-Fetching NFL odds from Kalshi & Pinnacle
- Normalizing team names to match between datasets
- Converting moneyline odds to probabilities (1/moneyline)
- Merging dataset
- Flagging games where arb exists
    - NO on Kalshi < Home on Pinnacle
    - YES on Kalshi < Away on Pinnacle
- Print arb opps

"""
import time
import pandas as pd
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

# COMBINED PAYOUT > 1 ARB
def detect_arbitrage():
    # Fetch data
    kalshi_df = fetch_kalshi_nfl_df() # Kalshi - all currently open NFL game markets with current bid/ask prices for both yes & no sides
    pinnacle_df = fetch_pinnacle_nfl_df() # Pinnacle: upcoming NFL games that currently have open betting markets (not live)

    if kalshi_df.empty or pinnacle_df.empty:
        print("No data found.")
        return

    # Keep only full-game rows for Pinnacle
    pinnacle_df = pinnacle_df[pinnacle_df["period"] == "Game"].copy()
    pinnacle_df = pinnacle_df.dropna(subset=["moneyline_home", "moneyline_away"])

    # Convert decimal odds to implied probabilities
    pinnacle_df["home_prob"] = 1 / pinnacle_df["moneyline_home"]
    pinnacle_df["away_prob"] = 1 / pinnacle_df["moneyline_away"]

    # Normalize Kalshi team names
    kalshi_df["away"] = kalshi_df["title"].str.split(" at ").str[0].map(normalize_team)
    kalshi_df["home"] = kalshi_df["title"].str.split(" at ").str[1].map(normalize_team)
    kalshi_df["yes_prob"] = kalshi_df["yes_ask"]
    kalshi_df["no_prob"] = kalshi_df["no_ask"]

    # Merge datasets
    merged = kalshi_df.merge(
        pinnacle_df,
        on=["home", "away"],
        how="inner"
    ).drop_duplicates(subset=["home", "away"]).copy()

    # Detect true arbitrage: combined inverse probabilities < 1 (ALLOWING FOR TOLERANCE/VIG --> CAN REMOVE)
    tolerance = 0.02
    merged["arb_away"] = (1 / merged["yes_prob"] + 1 / merged["away_prob"]) < 1 + tolerance
    merged["arb_home"] = (1 / merged["no_prob"] + 1 / merged["home_prob"]) < 1 + tolerance
    merged["has_arb"] = merged["arb_home"] | merged["arb_away"]

    # Compute profit %
    merged["arb_profit_away"] = 1 - (1 / merged["yes_prob"] + 1 / merged["away_prob"])
    merged["arb_profit_home"] = 1 - (1 / merged["no_prob"] + 1 / merged["home_prob"])

    # Show only arbitrage opportunities
    arb_opps = merged[merged["has_arb"]][[
        "home", "away",
        "yes_prob", "no_prob", "home_prob", "away_prob",
        "arb_home", "arb_away",
        "arb_profit_home", "arb_profit_away"
    ]]

    print(arb_opps)

#LOOSE ARB (KALSHI < PINNACLE)
"""
def detect_arbitrage():
    kalshi_df = fetch_kalshi_nfl_df()
    pinnacle_df = fetch_pinnacle_nfl_df()

    if kalshi_df.empty or pinnacle_df.empty:
        print("No data found.")
        return

    # normalize Kalshi team names
    kalshi_df["away"] = kalshi_df["title"].str.split(" at ").str[0].map(normalize_team)
    kalshi_df["home"] = kalshi_df["title"].str.split(" at ").str[1].map(normalize_team)

    # implied probs
    pinnacle_df["home_prob"] = 1 / pinnacle_df["moneyline_home"]
    pinnacle_df["away_prob"] = 1 / pinnacle_df["moneyline_away"]

    kalshi_df["yes_prob"] = kalshi_df["yes_ask"]
    kalshi_df["no_prob"] = kalshi_df["no_ask"]

    # merge
    merged = kalshi_df.merge(
        pinnacle_df,
        on=["home", "away"],
        how="inner",
        suffixes=("_kalshi", "_pinnacle")
    )
    merged_unique = merged.drop_duplicates(subset=["home", "away"])
    merged_unique = merged_unique.copy()

    #Kalshi probabilities (yes_prob, no_prob) // Pinnacle probabilities (home_prob, away_prob)
    #Kalshi YES → Pinnacle away win // Kalshi NO → Pinnacle home win
    # detect arb: if Kalshi YES prob < Pinnacle away prob, or Kalshi NO prob < Pinnacle home prob

    # Recompute arbitrage flags
    merged_unique["arb_home"] = merged_unique["no_prob"] < merged_unique["home_prob"]
    merged_unique["arb_away"] = merged_unique["yes_prob"] < merged_unique["away_prob"]
    merged_unique["has_arb"] = merged_unique["arb_home"] | merged_unique["arb_away"]

    # Show only arb opportunities
    arb_opps = merged_unique[merged_unique["has_arb"]][[
        "home", "away", "yes_prob", "no_prob", "home_prob", "away_prob", "arb_home", "arb_away"
    ]]
    print(arb_opps)
"""

if __name__ == "__main__":
    while True:
        detect_arbitrage()
        time.sleep(60)  # wait 60 seconds before next scan