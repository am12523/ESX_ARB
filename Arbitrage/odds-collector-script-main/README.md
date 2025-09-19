### terminal_ui.py
- Interactive browser for sports → leagues → events.
- Export cleaned, tick-by-tick odds for a selected event to a CSV.
- Stays on the same page so you can export multiple events in one session.
- Default shows major US leagues; use --all to show everything.
- Output filename: YYYY-MM-DD_Team1_Team2.csv

Usage:
```bash
python terminal_ui.py [--all] [--debug]
```

### fetch_leagues.py
- Batch workflow: dedupe a league CSV and fetch odds for every event_id.
- Dedupe is in-place by (starts, home, away).
- Writes one cleaned tick CSV per event into the specified folder.

Usage:
```bash
python fetch_leagues.py --csv "path/to/league.csv" --outdir "odds_output_dir" [--limit N]
```

### Requirements
- .env with USER_API_KEY
- Python 3.9+