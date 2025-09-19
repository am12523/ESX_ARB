import os
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client

# import your fetchers
from pinnacle_nfl_odds_A import fetch_pinnacle_nfl_df
from kalshi_nfl_odds_A import fetch_kalshi_nfl_df

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def insert_df(df: pd.DataFrame, table: str):
    if df.empty:
        print(f"No rows to insert for {table}")
        return
    records = df.to_dict(orient="records")
    batch_size = 500
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        supabase.table(table).insert(chunk).execute()
    print(f"Inserted {len(records)} rows into {table}")

def backfill_past_three_months():
    start_date = datetime.utcnow() - timedelta(days=90)
    today = datetime.utcnow()

    # iterate day by day
    date = start_date
    while date <= today:
        print(f"Fetching for {date.date()}...")

        # Pinnacle snapshot
        pinnacle_df = fetch_pinnacle_nfl_df()
        pinnacle_df["snapshot_time"] = datetime.utcnow().isoformat()
        insert_df(pinnacle_df, "pinnacle_odds")

        # Kalshi snapshot
        kalshi_df = fetch_kalshi_nfl_df()
        kalshi_df["snapshot_time"] = datetime.utcnow().isoformat()
        insert_df(kalshi_df, "kalshi_odds")

        # advance one day
        date += timedelta(days=1)

if __name__ == "__main__":
    backfill_past_three_months()
