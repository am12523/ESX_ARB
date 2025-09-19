#!/usr/bin/env python3
"""
Simple example to extract MLB odds from Pinnacle via RapidAPI
Ensures all results have category = 'sports'
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pinnacle_mlb_odds import get_pinnacle_mlb_odds, process_mlb_odds

def quick_test():
    """
    Quick test function to extract MLB odds
    """
    # Your RapidAPI key - get it from https://rapidapi.com/tipsters/api/pinnacle-odds
    api_key = "3a838783f5mshe3f66788f4b7b41p183801jsne8483b25eaa2"
    
    if api_key == "YOUR_RAPIDAPI_KEY_HERE":
        print("🚨 API Key Required!")
        print("1. Sign up at https://rapidapi.com/tipsters/api/pinnacle-odds")
        print("2. Subscribe to the API")
        print("3. Replace 'YOUR_RAPIDAPI_KEY_HERE' with your actual key")
        return
    
    print("⚾ Getting MLB odds from Pinnacle...")
    
    # Fetch MLB data
    mlb_data = get_pinnacle_mlb_odds(api_key)
    
    if mlb_data:
        # Process the data
        df = process_mlb_odds(mlb_data)
        
        if df is not None and not df.empty:
            print(f"\n✅ Successfully extracted {len(df)} MLB games")
            print("\n📊 Sample data:")
            print(df[['home_team', 'away_team', 'moneyline_home', 'moneyline_away', 'category']].head())
            
            # Verify all records have category = 'sports'
            sports_only = df[df['category'] == 'sports']
            print(f"\n🎯 Records with category = 'sports': {len(sports_only)}/{len(df)}")
            
            if len(sports_only) == len(df):
                print("✅ All records are sports category!")
            else:
                print("⚠️  Some records are not sports category")
            
        else:
            print("❌ No MLB data found")
    else:
        print("❌ Failed to fetch data")

if __name__ == "__main__":
    quick_test() 