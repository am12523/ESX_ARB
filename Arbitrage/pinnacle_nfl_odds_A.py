# %%
import csv
import os
import sys
sys.path.append(os.path.abspath("odds-collector-script-main"))
from api import PinnacleOddsClient
import pandas as pd

API_KEY = os.getenv("USER_API_KEY")
SPORT_ID = 7
LEAGUE_ID = 889
OUTPUT_FILE = "pinnacle_nfl_markets.csv"

def fetch_pinnacle_nfl_df():
    client = PinnacleOddsClient(api_key=API_KEY)

    # Fetch all live/prematch markets
    events_data = client.list_markets(sport_id=SPORT_ID, event_type="prematch", is_have_odds=True)

    # Determine correct event list (sometimes nested under leagues)
    if "events" in events_data:
        all_events = events_data["events"]
    elif "leagues" in events_data:
        all_events = []
        for league in events_data["leagues"]:
            all_events.extend(league.get("events", []))
    else:
        all_events = []

    # Filter events by target league
    league_events = [e for e in all_events if e.get("league_id") == LEAGUE_ID]

    rows = []

    for event in league_events:
        event_id = event.get("event_id")
        sport_id = event.get("sport_id")
        league_id = event.get("league_id")
        league_name = event.get("league_name")
        home = event.get("home")
        away = event.get("away")
        start = event.get("starts")
        event_type = event.get("event_type")
        open_flags = event.get("open_flags") or {}

        periods = event.get("periods") or {}
        for period_key, period in periods.items():
            period_name = period.get("description")
            if period_name != "Game":
                continue
            # Flatten moneyline, spreads, totals, team totals
            row = {
                "event_id": event_id,
                "sport_id": sport_id,
                "league_id": league_id,
                "league_name": league_name,
                "home": home,
                "away": away,
                "starts": start,
                "event_type": event_type,
                "period": period_name,
                "moneyline_home": None,
                "moneyline_draw": None,
                "moneyline_away": None,
                "spread_handicap": None,
                "spread_home": None,
                "spread_away": None,
                "total_points": None,
                "total_over": None,
                "total_under": None,
                "home_team_over": None,
                "home_team_under": None,
                "away_team_over": None,
                "away_team_under": None,
                "is_open": open_flags.get("is_open", True)
            }

            # Moneyline
            ml = period.get("money_line") or {}
            row["moneyline_home"] = ml.get("home")
            row["moneyline_draw"] = ml.get("draw")
            row["moneyline_away"] = ml.get("away")

            # Spread (just take the main if available)
            spreads = period.get("spreads") or {}
            if spreads:
                main_spread = list(spreads.values())[0]
                row["spread_handicap"] = main_spread.get("hdp")
                row["spread_home"] = main_spread.get("home")
                row["spread_away"] = main_spread.get("away")

            # Totals (main)
            totals = period.get("totals") or {}
            if totals:
                main_total = list(totals.values())[0]
                row["total_points"] = main_total.get("points")
                row["total_over"] = main_total.get("over")
                row["total_under"] = main_total.get("under")

            # Team totals
            team_total = period.get("team_total") or {}
            home_tt = team_total.get("home") or {}
            away_tt = team_total.get("away") or {}
            row["home_team_over"] = home_tt.get("over")
            row["home_team_under"] = home_tt.get("under")
            row["away_team_over"] = away_tt.get("over")
            row["away_team_under"] = away_tt.get("under")

            rows.append(row)
    return pd.DataFrame(rows)

    """
    # Write CSV
    if rows:
        fieldnames = list(rows[0].keys())
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    print(f"Saved {len(rows)} market lines to {OUTPUT_FILE}")
    """
