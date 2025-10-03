Current version (necessary files):

1. pinnacle_nfl_odds_A.py

Fetches NFL market data from Pinnacle API and returns it as a Pandas DataFrame
- There's a commented out version to obtain the dataframe as a CSV ("pinnacle_nfl_markets.csv")

Requirements:
.env with USER_API_KEY
odds-collector-script-main (directory)
Python 3.9+

2. kalshi_nfl_odds_A.py

Fetches open NFL markets from Kalshi and compiles them into a Pandas DataFrame
- There's a commented out version to obtain the dataframe as a CSV
- Functions for placing kalshi orders have been commented out (to be utilized later)

Requirements:
Python 3.9+
(Once we start to place orders, .env with KALSHI_ACCESS KEY)

3. arb_nfl_odds_A_v2.py

Simulation tool that earches for arbitrage opportunities between Pinnacle Sportsbook (pre-match NFL moneyline odds) and Kalshi prediction markets (YES/NO contracts) and computes optimal bet sizing.
- Actual trade execution logic not implemented yet

Requirements:
Python 3.9+
pinnacle_nfl_odds_A 
kalshi_nfl_odds_A
