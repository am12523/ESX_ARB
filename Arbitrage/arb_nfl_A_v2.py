"""
ARB - Pinnacle and Kalshi

How it works:
- Fetch Kalshi orderbook rows and Pinnacle pre-match moneyline markets
- Normalize team names and merge on home/away
- Compute devigged probabilities from Pinnacle (derive "true" p_home/p_away)
- Compare Kalshi ask prices and Pinnacle implied/devig prices for arbitrage
- When a cross appears, place limit orders on the expensive market (to improve price) and, when that limit fills, take the market on the other side to hedge.
- Size each trade using a configurable fractional Kelly heuristic.

"""
import time
import pandas as pd

from pinnacle_nfl_odds_A import fetch_pinnacle_nfl_df
from kalshi_nfl_odds_A import fetch_kalshi_nfl_df


kalshi_fee = 0.003
bankroll = 10_000
fractional_kelly = 0.2


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

def devig(home_prob, away_prob):
    r = home_prob + away_prob 
    return home_prob / r, away_prob / r 


def kelly_fraction(pinnacle_odds, kalshi_odds, shrinkage=0.2):
    kelly = (kalshi_odds * pinnacle_odds - 1) / (kalshi_odds - 1)
    return max(0, shrinkage * kelly)

# COMBINED PAYOUT > 1 ARB
def simulate_trade():
    t0 = time.time()

    kalshi_df = fetch_kalshi_nfl_df() # Kalshi - all currently open NFL game markets with current bid/ask prices for both yes & no sides
    pinnacle_df = fetch_pinnacle_nfl_df() # Pinnacle: upcoming NFL games that currently have open betting markets (not live)

    if kalshi_df.empty or pinnacle_df.empty:
        print("No data found.")
        return pd.DataFrame()

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


    trade_log = []
    for _, row in merged.iterrows():
        event = f"{row['away']} at {row['home']}"

        # Devig Pinnacle
        home_devig, away_devig = devig(row["home_prob"], row["away_prob"])

        trades=[]

        # Arb 1: Buy YES on Kalshi vs Away Pinnacle
        kalshi_yes_cost = row["yes_prob"] * (1 + kalshi_fee)
        if kalshi_yes_cost < away_devig:
            edge = away_devig - kalshi_yes_cost
            frac = kelly_fraction(away_devig, row["yes_prob"], fractional_kelly)
            bet_size = frac * bankroll
            profit = bet_size * edge
            trades.append({
                "event": event,
                "side": "YES Kalshi / Away Pinnacle",
                "kalshi_price": kalshi_yes_cost,
                "pinnacle_prob_devig": away_devig,
                "edge": edge,
                "kelly_fraction": frac,
                "bet_size": bet_size,
                "profit": profit
            })

        # Arb 2: Buy NO on Kalshi vs Home Pinnacle
        kalshi_no_cost = row["no_prob"] * (1 + kalshi_fee)
        if kalshi_no_cost < home_devig:
            edge = home_devig - kalshi_no_cost
            frac = kelly_fraction(home_devig, row["no_prob"], fractional_kelly)
            bet_size = frac * bankroll
            profit = bet_size * edge
            trades.append({
                "event": event,
                "side": "NO Kalshi / Home Pinnacle",
                "kalshi_price": kalshi_no_cost,
                "pinnacle_prob_devig": home_devig,
                "edge": edge,
                "kelly_fraction": frac,
                "bet_size": bet_size,
                "profit": profit
            })
        if trades:
            best_trade = max(trades, key=lambda x: x["edge"])
            trade_log.append(best_trade)


    t1 = time.time()
    trades_df = pd.DataFrame(trade_log)

    #metrics
    metrics = {
        "average_edge": trades_df["edge"].mean() if not trades_df.empty else 0,
        "average_pct_gain": trades_df["profit"].mean() / bankroll if not trades_df.empty else 0,
        "total_PnL": trades_df["profit"].sum() if not trades_df.empty else 0,
        "data_processing_speed_sec": t1 - t0,
        "data_retrieval_speed_sec": None,         # placeholders for infra metrics
        "execution_speed_sec": None, 
    }

    return trades_df, metrics


if __name__ == "__main__":
    trades_df, metrics = simulate_trade()
    print("\nTrades:")
    print(trades_df)
    print("\nMetrics:")
    print(metrics)









"""
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

"""





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


if __name__ == "__main__":
    while True:
        detect_arbitrage()
        time.sleep(60)  # wait 60 seconds before next scan"

     """