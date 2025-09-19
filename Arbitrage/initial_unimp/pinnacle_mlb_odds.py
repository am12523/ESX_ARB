import requests
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
import time

def get_pinnacle_mlb_odds(api_key):
    """
    Extract MLB odds from Pinnacle via RapidAPI
    Specifically targets MLB league within Baseball sport
    """
    
    # RapidAPI Pinnacle Odds endpoint
    base_url = "https://pinnacle-odds.p.rapidapi.com"
    
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
    }
    
    # Common endpoints for Pinnacle odds - let's try different patterns
    endpoints = {
        'sports': '/kit/v1/sports',
        'leagues': '/kit/v1/leagues',
        # Try different endpoint patterns
        'fixtures_v1': '/v1/fixtures',
        'odds_v1': '/v1/odds',
        'fixtures_v2': '/v2/fixtures', 
        'odds_v2': '/v2/odds',
        'fixtures_kit': '/kit/v1/fixtures',
        'odds_kit': '/kit/v1/odds',
        'fixtures_simple': '/fixtures',
        'odds_simple': '/odds'
    }
    
    mlb_data = {}
    
    try:
        # 1. Get available sports to find Baseball sport ID
        print("🔍 Fetching available sports...")
        sports_response = requests.get(f"{base_url}{endpoints['sports']}", headers=headers)
        
        print(f"Sports API Status: {sports_response.status_code}")
        print(f"Sports API Response: {sports_response.text[:500]}...")
        
        if sports_response.status_code == 200:
            sports_data = sports_response.json()
            
            # Find Baseball sport
            baseball_sport_id = None
            print("\n📋 Available sports:")
            for sport in sports_data:
                print(f"- {sport.get('name')} (ID: {sport.get('id')})")
                if sport.get('name', '').lower() == 'baseball':
                    baseball_sport_id = sport.get('id')
                    print(f"✅ Found Baseball sport ID: {baseball_sport_id}")
            
            if not baseball_sport_id:
                print("❌ Baseball sport ID not found")
                return None
            
            mlb_data['sport_id'] = baseball_sport_id
            
        else:
            print(f"❌ Failed to fetch sports: {sports_response.status_code}")
            return None
        
        # 2. Get Baseball leagues to find MLB specifically
        print(f"\n🏟️ Fetching Baseball leagues...")
        leagues_params = {'sport_id': baseball_sport_id}
        leagues_response = requests.get(f"{base_url}{endpoints['leagues']}", 
                                      headers=headers, params=leagues_params)
        
        print(f"Leagues API Status: {leagues_response.status_code}")
        print(f"Leagues API Response: {leagues_response.text[:500]}...")
        
        mlb_league_id = None
        if leagues_response.status_code == 200:
            leagues_data = leagues_response.json()
            print(f"\n📊 Baseball leagues found: {len(leagues_data) if isinstance(leagues_data, list) else 'N/A'}")
            
            # Handle nested leagues structure
            if isinstance(leagues_data, dict) and 'leagues' in leagues_data:
                leagues_list = leagues_data['leagues']
                print(f"📊 Nested leagues found: {len(leagues_list)}")
            elif isinstance(leagues_data, list):
                leagues_list = leagues_data
                print(f"📊 Direct leagues found: {len(leagues_list)}")
            else:
                print(f"❌ Unexpected leagues data structure: {type(leagues_data)}")
                leagues_list = []
            
            # Find MLB league specifically
            print("\n🔍 Available Baseball leagues:")
            mlb_candidates = []
            for league in leagues_list:
                league_name = league.get('name', '')
                league_id = league.get('id')
                region = league.get('container', 'Unknown')
                event_count = league.get('event_count', 0)
                has_offerings = league.get('has_offerings', False)
                print(f"- {league_name} (ID: {league_id}, Region: {region}, Events: {event_count}, Active: {has_offerings})")
                
                # Look for MLB specifically - collect candidates
                if any(mlb_keyword in league_name.lower() for mlb_keyword in ['mlb', 'major league baseball', 'major league']):
                    mlb_candidates.append({
                        'id': league_id,
                        'name': league_name,
                        'events': event_count,
                        'active': has_offerings
                    })
            
            # Choose best MLB league (prefer active leagues with most events)
            if mlb_candidates:
                # Sort by: active first, then by event count
                mlb_candidates.sort(key=lambda x: (x['active'], x['events']), reverse=True)
                best_mlb = mlb_candidates[0]
                mlb_league_id = best_mlb['id']
                print(f"✅ Found MLB league: {best_mlb['name']} (ID: {mlb_league_id}, Events: {best_mlb['events']}, Active: {best_mlb['active']})")
                
                if len(mlb_candidates) > 1:
                    print(f"📋 Other MLB options:")
                    for candidate in mlb_candidates[1:]:
                        print(f"  - {candidate['name']} (ID: {candidate['id']}, Events: {candidate['events']}, Active: {candidate['active']})")
            else:
                print("⚠️ No MLB league found by name")
                # Fallback: look for leagues with high event counts
                active_leagues = [l for l in leagues_list if l.get('event_count', 0) > 0]
                if active_leagues:
                    best_active = max(active_leagues, key=lambda x: x.get('event_count', 0))
                    mlb_league_id = best_active.get('id')
                    print(f"🔄 Using most active league: {best_active.get('name')} (ID: {mlb_league_id}, Events: {best_active.get('event_count')})")
                else:
                    mlb_league_id = baseball_sport_id  # Final fallback
            
            mlb_data['leagues'] = leagues_list
            mlb_data['mlb_league_id'] = mlb_league_id
            
            if not mlb_league_id:
                print("⚠️ MLB league not found. Using Baseball sport ID for all leagues.")
                mlb_league_id = baseball_sport_id  # Fallback to sport ID
        else:
            print(f"⚠️ Leagues request failed: {leagues_response.status_code}")
            mlb_league_id = baseball_sport_id  # Fallback to sport ID
        
        # 3. Get MLB fixtures/games (use league ID if available)
        print(f"\n🎮 Fetching MLB fixtures...")
        if mlb_league_id and mlb_league_id != baseball_sport_id:
            # Use league ID for more specific results
            fixtures_params = {'league_id': mlb_league_id}
            print(f"Using MLB league ID: {mlb_league_id}")
        else:
            # Fallback to sport ID
            fixtures_params = {'sport_id': baseball_sport_id}
            print(f"Using Baseball sport ID: {baseball_sport_id}")
            
        fixtures_response = requests.get(f"{base_url}{endpoints['fixtures_v1']}", 
                                       headers=headers, params=fixtures_params)
        
        print(f"Fixtures API Status: {fixtures_response.status_code}")
        print(f"Fixtures API Response: {fixtures_response.text[:500]}...")
        
        if fixtures_response.status_code == 200:
            fixtures_data = fixtures_response.json()
            
            # Handle nested fixtures structure
            if isinstance(fixtures_data, dict) and 'fixtures' in fixtures_data:
                fixtures_list = fixtures_data['fixtures']
                print(f"\n🎯 MLB fixtures found: {len(fixtures_list)}")
            elif isinstance(fixtures_data, list):
                fixtures_list = fixtures_data
                print(f"\n🎯 MLB fixtures found: {len(fixtures_list)}")
            else:
                print(f"\n🎯 MLB fixtures found: {len(fixtures_data) if isinstance(fixtures_data, list) else 'N/A'}")
                fixtures_list = fixtures_data
                
            mlb_data['fixtures'] = fixtures_list
        else:
            print(f"⚠️ Fixtures request failed: {fixtures_response.status_code}")
        
        # 4. Get MLB odds (use league ID if available)
        print(f"\n💰 Fetching MLB odds...")
        if mlb_league_id and mlb_league_id != baseball_sport_id:
            # Use league ID for more specific results
            odds_params = {'league_id': mlb_league_id}
            print(f"Using MLB league ID: {mlb_league_id}")
        else:
            # Fallback to sport ID
            odds_params = {'sport_id': baseball_sport_id}
            print(f"Using Baseball sport ID: {baseball_sport_id}")
            
        odds_response = requests.get(f"{base_url}{endpoints['odds_v1']}", 
                                   headers=headers, params=odds_params)
        
        print(f"Odds API Status: {odds_response.status_code}")
        print(f"Odds API Response: {odds_response.text[:500]}...")
        
        if odds_response.status_code == 200:
            odds_data = odds_response.json()
            
            # Handle nested odds structure
            if isinstance(odds_data, dict) and 'odds' in odds_data:
                odds_list = odds_data['odds']
                print(f"\n🎲 MLB odds found: {len(odds_list)}")
            elif isinstance(odds_data, list):
                odds_list = odds_data
                print(f"\n🎲 MLB odds found: {len(odds_list)}")
            else:
                print(f"\n🎲 MLB odds found: {len(odds_data) if isinstance(odds_data, list) else 'N/A'}")
                odds_list = odds_data
                
            mlb_data['odds'] = odds_list
        else:
            print(f"⚠️ Odds request failed: {odds_response.status_code}")
        
        return mlb_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def process_mlb_odds(mlb_data):
    """
    Process the raw MLB odds data into a clean DataFrame
    """
    if not mlb_data:
        print("❌ No MLB data provided")
        return None
    
    print(f"\n🔄 Processing MLB data...")
    print(f"Available data keys: {list(mlb_data.keys())}")
    
    # Check if we have odds data
    if 'odds' not in mlb_data:
        print("❌ No odds data available")
        return None
    
    odds_data = mlb_data['odds']
    
    # Since we already processed the nested structure, odds_data should be a list
    if isinstance(odds_data, list):
        print(f"📊 Odds data is a list with {len(odds_data)} items")
        games_list = odds_data
    elif isinstance(odds_data, dict):
        print("📊 Odds data is still a dictionary, looking for nested structure")
        # If odds_data is a dict, look for games in common keys
        if 'games' in odds_data:
            games_list = odds_data['games']
        elif 'data' in odds_data:
            games_list = odds_data['data']
        elif 'odds' in odds_data:
            games_list = odds_data['odds']
        else:
            print("📋 Odds data structure:", json.dumps(odds_data, indent=2)[:1000])
            return None
    else:
        print(f"❌ Unexpected odds data type: {type(odds_data)}")
        return None
    
    if not games_list:
        print("❌ No games found in odds data")
        return None
    
    print(f"🎮 Found {len(games_list)} games")
    
    odds_list = []
    
    for i, game in enumerate(games_list):
        print(f"\n🎯 Processing game {i+1}: {json.dumps(game, indent=2)[:300]}...")
        
        # Extract game information
        odds_info = {
            'game_id': game.get('id', f'game_{i}'),
            'start_time': game.get('starts') or game.get('start_time') or game.get('commence_time'),
            'home_team': game.get('home') or game.get('home_team'),
            'away_team': game.get('away') or game.get('away_team'),
            'league': game.get('league'),
            'league_id': mlb_data.get('mlb_league_id'),
            'sport': game.get('sport', 'baseball'),
            'sport_id': mlb_data.get('sport_id', 9),
            'moneyline_home': None,
            'moneyline_away': None,
            'spread_home': None,
            'spread_away': None,
            'total_over': None,
            'total_under': None,
            'total_points': None
        }
        
        # Extract betting odds from different possible structures
        if 'periods' in game:
            for period in game['periods']:
                if period.get('number') == 0:  # Full game
                    # Moneyline
                    if 'moneyline' in period:
                        ml = period['moneyline']
                        odds_info['moneyline_home'] = ml.get('home')
                        odds_info['moneyline_away'] = ml.get('away')
                    
                    # Spread
                    if 'spread' in period:
                        spread = period['spread']
                        odds_info['spread_home'] = spread.get('home')
                        odds_info['spread_away'] = spread.get('away')
                    
                    # Total
                    if 'totals' in period:
                        total = period['totals']
                        odds_info['total_over'] = total.get('over')
                        odds_info['total_under'] = total.get('under')
                        odds_info['total_points'] = total.get('points')
        
        # Alternative structure for odds
        elif 'bookmakers' in game:
            for bookmaker in game['bookmakers']:
                if bookmaker.get('key') == 'pinnacle':
                    for market in bookmaker.get('markets', []):
                        if market.get('key') == 'h2h':  # Head to head (moneyline)
                            outcomes = market.get('outcomes', [])
                            for outcome in outcomes:
                                if outcome.get('name') == odds_info['home_team']:
                                    odds_info['moneyline_home'] = outcome.get('price')
                                elif outcome.get('name') == odds_info['away_team']:
                                    odds_info['moneyline_away'] = outcome.get('price')
        
        odds_list.append(odds_info)
    
    if odds_list:
        df = pd.DataFrame(odds_list)
        print(f"\n✅ Created DataFrame with {len(df)} games")
        
        # Filter for MLB games if we have league information
        if mlb_data.get('mlb_league_id'):
            mlb_games = df[df['league_id'] == mlb_data['mlb_league_id']]
            if not mlb_games.empty:
                print(f"🎯 Filtered to {len(mlb_games)} MLB-specific games")
                return mlb_games
        
        return df
    
    print("❌ No odds data processed")
    return None

def get_mlb_league_info(api_key, sport_id):
    """
    Helper function to get detailed MLB league information
    """
    base_url = "https://pinnacle-odds.p.rapidapi.com"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
    }
    
    print(f"\n🔍 Getting detailed league info for Baseball sport ID: {sport_id}")
    
    try:
        leagues_response = requests.get(f"{base_url}/kit/v1/leagues", 
                                      headers=headers, params={'sport_id': sport_id})
        
        if leagues_response.status_code == 200:
            leagues_data = leagues_response.json()
            
            # Handle nested leagues structure
            if isinstance(leagues_data, dict) and 'leagues' in leagues_data:
                leagues_list = leagues_data['leagues']
            elif isinstance(leagues_data, list):
                leagues_list = leagues_data
            else:
                print(f"❌ Unexpected leagues data structure: {type(leagues_data)}")
                return None
            
            print(f"\n📋 All Baseball leagues ({len(leagues_list)}):")
            for league in leagues_list:
                league_name = league.get('name', 'Unknown')
                league_id = league.get('id', 'Unknown')
                region = league.get('container', 'Unknown')
                has_offerings = league.get('has_offerings', False)
                event_count = league.get('event_count', 0)
                print(f"- {league_name} (ID: {league_id}, Region: {region}, Events: {event_count}, Active: {has_offerings})")
                
            return leagues_list
        else:
            print(f"❌ Failed to get leagues: {leagues_response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error getting league info: {e}")
        return None

def test_endpoints(api_key, mlb_league_id, baseball_sport_id):
    """
    Test different endpoint patterns to find working ones
    """
    base_url = "https://pinnacle-odds.p.rapidapi.com"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
    }
    
    # Different endpoint patterns to try
    endpoint_patterns = [
        '/v1/fixtures',
        '/v2/fixtures', 
        '/kit/v1/fixtures',
        '/fixtures',
        '/api/v1/fixtures',
        '/v1/odds',
        '/v2/odds',
        '/kit/v1/odds',
        '/odds',
        '/api/v1/odds',
        '/v1/events',
        '/v2/events',
        '/events',
        '/v1/lines',
        '/v2/lines',
        '/lines'
    ]
    
    print(f"\n🧪 Testing different endpoint patterns...")
    
    working_endpoints = []
    
    for endpoint in endpoint_patterns:
        try:
            print(f"Testing: {endpoint}")
            
            # Test with league ID first
            params = {'league_id': mlb_league_id}
            response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
            
            if response.status_code == 200:
                print(f"✅ {endpoint} works with league_id! Status: {response.status_code}")
                working_endpoints.append((endpoint, 'league_id', response.text[:200]))
            elif response.status_code == 404:
                print(f"❌ {endpoint} does not exist (404)")
            else:
                print(f"⚠️  {endpoint} returned status: {response.status_code}")
                
            # Also test with sport ID
            params = {'sport_id': baseball_sport_id}
            response = requests.get(f"{base_url}{endpoint}", headers=headers, params=params)
            
            if response.status_code == 200:
                print(f"✅ {endpoint} works with sport_id! Status: {response.status_code}")
                working_endpoints.append((endpoint, 'sport_id', response.text[:200]))
            elif response.status_code != 404:
                print(f"⚠️  {endpoint} with sport_id returned status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error testing {endpoint}: {e}")
    
    return working_endpoints

def main():
    # You need to get your RapidAPI key from https://rapidapi.com/tipsters/api/pinnacle-odds
    api_key = "3a838783f5mshe3f66788f4b7b41p183801jsne8483b25eaa2"  # Replace with your actual API key
    
    if api_key == "YOUR_RAPIDAPI_KEY_HERE":
        print("⚠️  Please set your RapidAPI key!")
        print("1. Go to https://rapidapi.com/tipsters/api/pinnacle-odds")
        print("2. Subscribe to the API")
        print("3. Copy your API key and replace 'YOUR_RAPIDAPI_KEY_HERE' above")
        return
    
    print("⚾ Fetching MLB odds from Pinnacle via RapidAPI...")
    print("=" * 60)
    
    # Get MLB data
    mlb_data = get_pinnacle_mlb_odds(api_key)
    
    if mlb_data:
        print(f"\n📊 Raw MLB data keys: {list(mlb_data.keys())}")
        
        # Show league information
        if mlb_data.get('sport_id'):
            league_info = get_mlb_league_info(api_key, mlb_data['sport_id'])
            
        # Test endpoints to find working ones
        if mlb_data.get('mlb_league_id') and mlb_data.get('sport_id'):
            print(f"\n🧪 Testing API endpoints...")
            working_endpoints = test_endpoints(api_key, mlb_data['mlb_league_id'], mlb_data['sport_id'])
            
            if working_endpoints:
                print(f"\n✅ Found {len(working_endpoints)} working endpoints:")
                for endpoint, param_type, sample in working_endpoints:
                    print(f"- {endpoint} (with {param_type})")
                    print(f"  Sample response: {sample}...")
            else:
                print(f"\n❌ No working endpoints found for fixtures/odds")
        
        # Process the data
        df = process_mlb_odds(mlb_data)
        
        if df is not None and not df.empty:
            print(f"\n✅ Successfully processed {len(df)} MLB games")
            print("\n📋 Sample data:")
            print(df[['home_team', 'away_team', 'league', 'league_id', 'moneyline_home', 'moneyline_away']].head())
            
            # Save to CSV
            filename = f"mlb_pinnacle_odds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(filename, index=False)
            print(f"\n💾 Data saved to: {filename}")
            
            # Display summary
            print(f"\n📈 Summary:")
            print(f"- Total games: {len(df)}")
            print(f"- Games with moneyline: {df['moneyline_home'].notna().sum()}")
            print(f"- Games with spread: {df['spread_home'].notna().sum()}")
            print(f"- Games with totals: {df['total_over'].notna().sum()}")
            print(f"- Games with team names: {df['home_team'].notna().sum()}")
            print(f"- MLB league ID used: {mlb_data.get('mlb_league_id', 'N/A')}")
            
        else:
            print("❌ No MLB odds data processed successfully")
    else:
        print("❌ Failed to fetch MLB data")

if __name__ == "__main__":
    main() 