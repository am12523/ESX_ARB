"""
Utilities to fetch odds and related info by event_id using PinnacleOddsClient.

Functions:
- get_event_history(event_id)
- get_event_current_markets(event_id, event_type_preference=("prematch","live"))
- get_event_moneyline(event_id)
- get_event_main_spread(event_id)
- get_event_main_total(event_id)
- get_event_team_totals(event_id)
- get_event_open_flags(event_id)
- get_event_specials(event_id)
- get_event_summary(event_id)

These functions discover sport_id from event details, then pull the event snapshot
from markets and extract useful odds. Designed for direct importing in your app.

User configuration (optional):
- Set USER_API_KEY and USER_EVENT_ID below, then run: python odds_fetch.py
- Or set RAPIDAPI_KEY env var and only set USER_EVENT_ID.

Prereq: pip install requests
Docs: https://rapidapi.com/tipsters/api/pinnacle-odds
"""

import json
import os
import sys
import requests
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # load variables from .env if present
except Exception:
    pass

from api import PinnacleOddsClient


# ===== User configuration =====
# Only API key is read from .env as USER_API_KEY. Other values are set here.
USER_API_KEY: Optional[str] = os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
USER_EVENT_ID: Optional[int] = 1609669239  # set your target event id here
USER_SPORT_ID: Optional[int] = 7  # optional sport override; set None to auto-detect
USER_EVENT_TYPE_PREFERENCE: Tuple[str, str] = ("prematch", "live")




def _normalize(text: str) -> str:
    return str(text).strip().lower()


def _pick_main_spread_line(spreads: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not spreads:
        return None
    try:
        candidates: List[Tuple[float, str, Dict[str, Any]]] = []
        for key, obj in spreads.items():
            try:
                hdp = float(key)
            except (TypeError, ValueError):
                continue
            candidates.append((abs(hdp), key, obj))
        candidates.sort(key=lambda x: x[0])
        _, key, obj = candidates[0]
        return key, obj
    except Exception:
        return None


def _pick_main_total_line(totals: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not totals:
        return None
    try:
        candidates: List[Tuple[float, str, Dict[str, Any]]] = []
        for key, obj in totals.items():
            try:
                pts = float(key)
            except (TypeError, ValueError):
                continue
            candidates.append((abs(pts), key, obj))
        candidates.sort(key=lambda x: x[0])
        _, key, obj = candidates[0]
        return key, obj
    except Exception:
        return None


def _resolve_sport_id_from_details(client: PinnacleOddsClient, event_id: int) -> int:
    # Prefer explicit override from user config
    if USER_SPORT_ID is not None:
        return int(USER_SPORT_ID)
    details = client.event_details(event_id=event_id)
    # Try common locations for sport_id in details payload
    if isinstance(details, dict):
        for key in ("sport_id", "sportId"):
            if key in details:
                try:
                    return int(details[key])
                except Exception:
                    pass
        # Sometimes nested under event or meta
        for path in ("event", "meta", "info", "data"):
            nested = details.get(path)
            if isinstance(nested, dict):
                for key in ("sport_id", "sportId"):
                    if key in nested:
                        try:
                            return int(nested[key])
                        except Exception:
                            pass
    raise ValueError("Could not determine sport_id from event details")


def _extract_list(value: Any, keys: Tuple[str, ...]) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        for k in keys:
            maybe = value.get(k)
            if isinstance(maybe, list):
                return [x for x in maybe if isinstance(x, dict)]
    return []


def _resolve_sport_id_by_scanning_markets(client: PinnacleOddsClient, event_id: int) -> Optional[int]:
    # Try to find the event by scanning markets across sports (prematch, live, then any)
    sports_resp = client.list_sports()
    sports = _extract_list(sports_resp, ("sports", "data", "result", "response"))
    if not sports:
        return None

    def get_id(item: Dict[str, Any]) -> Optional[int]:
        for key in ("id", "sport_id"):
            if key in item:
                try:
                    return int(item[key])
                except Exception:
                    continue
        return None

    def get_name(item: Dict[str, Any]) -> str:
        for key in ("name", "sport_name", "title"):
            if key in item and item[key] is not None:
                return str(item[key])
        return ""

    # Try American Football first if present
    ordered_sports: List[Dict[str, Any]] = []
    preferred = [s for s in sports if "american" in _normalize(get_name(s)) and "football" in _normalize(get_name(s))]
    others = [s for s in sports if s not in preferred]
    ordered_sports.extend(preferred + others)

    for sport in ordered_sports:
        sid = get_id(sport)
        if sid is None:
            continue
        for evt_type in ("prematch", "live", None):
            payload = client.list_markets(sport_id=sid, event_type=evt_type, is_have_odds=None)
            events = payload.get("events") if isinstance(payload, dict) else None
            if not isinstance(events, list):
                continue
            for ev in events:
                try:
                    if int(ev.get("event_id") or 0) == int(event_id):
                        return sid
                except Exception:
                    continue
    return None


def _fetch_event_snapshot(
    client: PinnacleOddsClient,
    sport_id: int,
    event_id: int,
    event_type_preference: Tuple[str, str] = ("prematch", "live"),
) -> Optional[Dict[str, Any]]:
    # Try preferred event types first, then fallback to any
    tried_types: List[Optional[str]] = [event_type_preference[0], event_type_preference[1], None]
    for evt_type in tried_types:
        payload = client.list_markets(sport_id=sport_id, event_type=evt_type, is_have_odds=None)
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            continue
        for ev in events:
            try:
                if int(ev.get("event_id") or 0) == int(event_id):
                    return ev
            except Exception:
                continue
    return None


def get_event_history(client: PinnacleOddsClient, event_id: int) -> Any:
    return client.event_details(event_id=event_id)


def get_event_current_markets(
    client: PinnacleOddsClient,
    event_id: int,
    event_type_preference: Tuple[str, str] = ("prematch", "live"),
) -> Dict[str, Any]:
    try:
        sport_id = _resolve_sport_id_from_details(client, event_id)
    except ValueError:
        # Fallback: scan markets across sports to locate this event_id
        resolved = _resolve_sport_id_by_scanning_markets(client, event_id)
        if resolved is None:
            raise
        sport_id = resolved
    event = _fetch_event_snapshot(client, sport_id=sport_id, event_id=event_id, event_type_preference=event_type_preference)
    if not event:
        raise ValueError("Event not found in current markets. It may be inactive or filtered.")
    return event


def get_event_moneyline(client: PinnacleOddsClient, event_id: int) -> Optional[Dict[str, Any]]:
    event = get_event_current_markets(client, event_id)
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    ml = main.get("money_line") or {}
    return ml or None


def get_event_main_spread(client: PinnacleOddsClient, event_id: int) -> Optional[Dict[str, Any]]:
    event = get_event_current_markets(client, event_id)
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    spreads = main.get("spreads") or {}
    pick = _pick_main_spread_line(spreads)
    if not pick:
        return None
    key, obj = pick
    return {"handicap": key, **obj}


def get_event_main_total(client: PinnacleOddsClient, event_id: int) -> Optional[Dict[str, Any]]:
    event = get_event_current_markets(client, event_id)
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    totals = main.get("totals") or {}
    pick = _pick_main_total_line(totals)
    if not pick:
        return None
    key, obj = pick
    return {"points": key, **obj}


def get_event_team_totals(client: PinnacleOddsClient, event_id: int) -> Optional[Dict[str, Any]]:
    event = get_event_current_markets(client, event_id)
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    team_total = main.get("team_total") or {}
    return team_total or None


def get_event_open_flags(client: PinnacleOddsClient, event_id: int) -> Optional[Dict[str, Any]]:
    event = get_event_current_markets(client, event_id)
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    meta = main.get("meta") or {}
    return meta or None


def get_event_specials(client: PinnacleOddsClient, event_id: int) -> List[Dict[str, Any]]:
    sport_id = _resolve_sport_id_from_details(client, event_id)
    try:
        specials_payload = client.list_specials(sport_id=sport_id)
    except requests.HTTPError as http_err:
        # Some sports or plans may not expose specials; treat 404 as "no specials" gracefully
        if http_err.response is not None and http_err.response.status_code == 404:
            return []
        raise
    specials = specials_payload.get("specials") if isinstance(specials_payload, dict) else None
    if not isinstance(specials, list):
        return []
    return [sp for sp in specials if int(sp.get("event_id") or 0) == int(event_id)]


def get_event_summary(client: PinnacleOddsClient, event_id: int) -> Dict[str, Any]:
    event = get_event_current_markets(client, event_id)
    summary: Dict[str, Any] = {
        "event_id": event.get("event_id"),
        "sport_id": event.get("sport_id"),
        "league_id": event.get("league_id"),
        "league_name": event.get("league_name"),
        "home": event.get("home"),
        "away": event.get("away"),
        "starts": event.get("starts"),
        "event_type": event.get("event_type"),
        "moneyline": get_event_moneyline(client, event_id),
        "main_spread": get_event_main_spread(client, event_id),
        "main_total": get_event_main_total(client, event_id),
        "team_totals": get_event_team_totals(client, event_id),
        "open_flags": get_event_open_flags(client, event_id),
    }
    return summary


def _should_pause() -> bool:
    return str(os.getenv("NO_PAUSE", "")).strip().lower() not in ("1", "true", "t", "yes", "y")


def main() -> int:
    api_key = USER_API_KEY or os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("Error: Provide RapidAPI key via USER_API_KEY in your .env.", file=sys.stderr)
        if _should_pause():
            input("Press Enter to exit...")
        return 1

    if USER_EVENT_ID is None:
        print("Error: Set USER_EVENT_ID at the top of odds_fetch.py to the desired event ID.", file=sys.stderr)
        if _should_pause():
            input("Press Enter to exit...")
        return 2

    client = PinnacleOddsClient(api_key=api_key)

    try:
        # Summary and specials
        summary = get_event_summary(client, USER_EVENT_ID)
        specials = get_event_specials(client, USER_EVENT_ID)

        print("Summary:")
        print(json.dumps(summary, indent=2))
        print("\nSpecials (truncated to 10 lines):")
        print(json.dumps(specials[:10], indent=2))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if _should_pause():
            input("Press Enter to exit...")
        return 3

    if _should_pause():
        input("Press Enter to exit...")
    return 0


if __name__ == "__main__":
    sys.exit(main())


