"""
Fetches open NFL markets from Kalshi and compiles them into a Pandas DataFrame.
- Does not require authentication key for retrieving markets
- Will require authentication key once we start to trade on kalshi
"""
#Imports
import requests
import pandas as pd
from datetime import datetime

# Base URL for Kalshi trading API
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

def get_events(series_ticker="KXNFLGAME"):
    """
    Fetches open events from Kalshi for a given series.
    Args: series_ticker (str): The Kalshi series identifier (default "KXNFLGAME").
    Returns:list: A list of event dictionaries containing metadata for each open event.
    """
    url = f"{BASE_URL}/events"
    resp = requests.get(url, params={"status": "open", "series_ticker": series_ticker})
    resp.raise_for_status()
    return resp.json().get("events", [])

def get_markets(event_ticker):
    """
    Fetches all markets associated with a given event.
    Args:event_ticker (str): The ticker symbol for the event.
    Returns:list: A list of market dictionaries, each containing bid/ask and contract info.
    """
    url = f"{BASE_URL}/markets"
    resp = requests.get(url, params={"event_ticker": event_ticker})
    resp.raise_for_status()
    return resp.json().get("markets", [])

def fetch_kalshi_nfl_df():
    """
    - Retrieves all open events for the NFL series ("KXNFLGAME").
    - For each event, fetches all associated markets.
    - Calculates 'yes' and 'no' bid/ask prices in decimal form (0-1 range).
    - Creates a DataFrame containing metadata and bid/ask prices for each market.

    """
    rows = []
    events = get_events("KXNFLGAME")

    for event in events:
        markets = get_markets(event["event_ticker"])
        for m in markets:
            # each market object already has bid/ask data
            yes_bid = m.get("yes_bid") / 100 if m.get("yes_bid") else None
            yes_ask = m.get("yes_ask") / 100 if m.get("yes_ask") else None
            no_bid  = (100 - m["yes_ask"]) / 100 if m.get("yes_ask") else None
            no_ask  = (100 - m["yes_bid"]) / 100 if m.get("yes_bid") else None

            rows.append({
                "event_ticker": event["event_ticker"],
                "market_ticker": m["ticker"],
                "title": event.get("title"),
                "contract_name": m.get("title"),
                "yes_bid": yes_bid,
                "yes_ask": yes_ask,
                "no_bid": no_bid,
                "no_ask": no_ask
            })
    return pd.DataFrame(rows)

# Commented out section for saving dataframe to csv
"""
if __name__ == "__main__":
    nfl_df = fetch_kalshi_nfl_df()
    if nfl_df.empty:
        print("No NFL orderbooks found.")
    else:
        # save with timestamp
        output_file = f"kalshi_nfl_orderbooks_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        nfl_df.to_csv(output_file, index=False)
        print(f"Saved {len(nfl_df)} rows to {output_file}")
        print(nfl_df.head())
"""


# ---- SKIP FOR NOW ----  (commented out section for placing kalshi orders)
"""
def get_kalshi_orderbook_arrays_with_tickers(ticker_a, ticker_b):
    # get orderbooks for ticker_a and ticker_b
    # ------------------------------
    # ticker_a orderbook
    # ------------------------------
    kalshi_ticker_a_orderbook = get_kalshi_orderbook(ticker_a, depth=10) 
    kalshi_orderbooks_a = [np.array(kalshi_ticker_a_orderbook.get("orderbook").get(option)) for option in ["yes", "no"]] # get orderbooks for yes and no

    # yes_bid_prices_a: bid price for yes, ask price for no
    kalshi_yes_bid_prices_a = kalshi_orderbooks_a[0]
    kalshi_no_ask_prices_a = kalshi_orderbooks_a[0].copy() # Mirror yes_bid_prices_a to get no_ask_prices_a
    kalshi_no_ask_prices_a[:, 0] = 100 - kalshi_no_ask_prices_a[:, 0] # subtract each value in the *first column* from 100 to get ask price for the other side

    # no_bid_prices_a: ask price for yes, bid price for no
    kalshi_no_bid_prices_a = kalshi_orderbooks_a[1]
    kalshi_yes_ask_prices_a = kalshi_orderbooks_a[1].copy() # Mirror
    kalshi_yes_ask_prices_a[:, 0] = 100 - kalshi_yes_ask_prices_a[:, 0] # subtract each value in the *first column* from 100 to get ask price for the other side


    # ------------------------------
    # ticker_b orderbook
    # ------------------------------
    kalshi_ticker_b_orderbook = get_kalshi_orderbook(ticker_b, depth=10) 
    kalshi_orderbooks_b = [np.array(kalshi_ticker_b_orderbook.get("orderbook").get(option)) for option in ["yes", "no"]] # get orderbooks for yes and no

    # yes_bid_prices_b: bid price for yes, ask price for no
    kalshi_yes_bid_prices_b = kalshi_orderbooks_b[0]
    kalshi_no_ask_prices_b = kalshi_orderbooks_b[0].copy() # Mirror yes_bid_prices_b to get no_ask_prices_b
    kalshi_no_ask_prices_b[:, 0] = 100 - kalshi_no_ask_prices_b[:, 0] # subtract each value in the *first column* from 100 to get ask price for the other side

    # no_bid_prices_b: ask price for yes, bid price for no
    kalshi_no_bid_prices_b = kalshi_orderbooks_b[1]
    kalshi_yes_ask_prices_b = kalshi_orderbooks_b[1].copy() # Mirror
    kalshi_yes_ask_prices_b[:, 0] = 100 - kalshi_yes_ask_prices_b[:, 0] # subtract each value in the *first column* from 100 to get ask price for the other side
    return kalshi_yes_ask_prices_a, kalshi_yes_bid_prices_a, kalshi_no_ask_prices_a, kalshi_no_bid_prices_a, kalshi_yes_ask_prices_b, kalshi_yes_bid_prices_b, kalshi_no_ask_prices_b, kalshi_no_bid_prices_b


def trade_limit_order_kalshi(action, ticker, price, count, side='yes'):
    BASE     = "https://api.elections.kalshi.com"
    ENDPOINT = "/trade-api/v2/portfolio/orders"

    # 1) HEAD → get server time as an *aware* datetime
    head      = requests.head(BASE + ENDPOINT, timeout=5)
    server_dt = parsedate_to_datetime(head.headers["Date"])    # e.g. 23:06:58+00:00

    # 2) Build timestamp ms directly from that
    ts_ms = str(int(server_dt.timestamp() * 1000))             # correct UTC epoch

    # 3) Build & sign the preimage with RSA-PSS
    preimage = (ts_ms + "POST" + ENDPOINT).encode()
    pem      = open(os.environ["KALSHI_PRIVATE_KEY_PATH"], "rb").read()
    priv     = serialization.load_pem_private_key(pem, password=None)
    sig      = priv.sign(
        preimage,
        padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()),
        salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    signature_b64 = base64.b64encode(sig).decode()

    # 4) Assemble headers & body
    headers = {
    "KALSHI-ACCESS-KEY":       os.getenv("KALSHI_ACCESS_KEY"),
    "KALSHI-ACCESS-TIMESTAMP": ts_ms,
    "KALSHI-ACCESS-SIGNATURE": signature_b64,
    "Content-Type":            "application/json",
    }

    # Kalshi has to specify "yes_price" or "no_price"
    side_price = "yes_price" if side == "yes" else "no_price"
    body = {
    "action":          action, # buy or sell
    "client_order_id": str(uuid.uuid4()), # unique order id
    "count":           count, # number of contracts
    "side":            side, # yes or no
    "ticker":          ticker, # ticker of the contract
    "type":            "limit", # limit order to match "maker"
    side_price:        price # price of the contract
    # They also have extra field like :
    #   "post_only": If this flag is set to true, an order will be rejected if it crosses the spread and executes.
    #   "sell_position_floor": SellPositionFloor will not let you flip position for a market order if set to 0.
    #   "time_in_force": Only "fill_or_kill"is supported. Other time in forces are controlled through expiration_ts
    #   "expiration_ts": Expiration time of the order, in unix seconds. Use for Good 'Till Cancelled (GTC).
    }

    # 5) Fire off the order
    resp = requests.post(BASE + ENDPOINT, headers=headers, json=body, timeout=10)
    print(resp.status_code, resp.json())
    return

"""
