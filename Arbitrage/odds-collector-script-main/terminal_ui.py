"""
Interactive terminal UI to navigate Pinnacle Odds via RapidAPI and export odds to CSV.

Steps:
1) Display all sports and select one
2) Display leagues for that sport and select one
3) Type a year
4) Display all events (games) in that year for that league and select one
5) Fetch historical odds for the selected event and write to output.csv

Configuration:
- Reads USER_API_KEY from .env (python-dotenv) or environment

Usage:
    python terminal_ui.py
"""

import csv
import json
import argparse
import requests
import os
import sys

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from api import PinnacleOddsClient


def _should_pause() -> bool:
    return str(os.getenv("NO_PAUSE", "")).strip().lower() not in ("1", "true", "t", "yes", "y")


def _normalize(text: str) -> str:
    return str(text).strip().lower()


def _get_first(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _header(title: str, emoji: str) -> None:
    print(f"\n{emoji}  {title}\n")


def _extract_items(resp: Any, keys: List[str]) -> List[Dict[str, Any]]:
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    if isinstance(resp, dict):
        for k in keys:
            maybe = resp.get(k)
            if isinstance(maybe, list):
                return [x for x in maybe if isinstance(x, dict)]
    return []


def _sanitize_name(name: str) -> str:
    s = str(name).strip()
    for ch in ("/", "\\", ":", "*", "?", '"', "<", ">", "|"):
        s = s.replace(ch, "_")
    s = s.replace(" ", "_")
    return s or "unknown"


def _name_compact(name: str) -> str:
    # Remove all non-alphanumeric characters and spaces; keep letters/numbers only
    return "".join(ch for ch in str(name) if ch.isalnum()) or "unknown"


def _export_event_csv_from_details(details: Dict[str, Any]) -> str:
    # Build rows from periods.history across all markets
    def _to_epoch_and_iso(ts_val: Any) -> Tuple[int, str]:
        ts = int(ts_val)
        if ts > 10**12:
            ts = int(ts // 1000)
        from datetime import datetime, timezone as _tz
        iso = datetime.fromtimestamp(ts, tz=_tz.utc).isoformat()
        return ts, iso

    def _iter_event_period_ticks(event: Dict[str, Any], period_key: str, period: Dict[str, Any]):
        sport_id = event.get("sport_id")
        league_id = event.get("league_id")
        league_name = event.get("league_name")
        home = event.get("home")
        away = event.get("away")
        starts = event.get("starts")
        event_id_local = event.get("event_id") or event.get("eventId")

        period_number = period.get("number")
        period_description = period.get("description")
        hist = (period.get("history") or {})

        # moneyline
        ml = hist.get("moneyline") or {}
        for side in ("home", "away", "draw"):
            seq = ml.get(side) or []
            for row in seq:
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    continue
                ts, price = row[0], row[1]
                limit = row[2] if len(row) > 2 else None
                ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                yield {
                    "event_id": event_id_local,
                    "sport_id": sport_id,
                    "league_id": league_id,
                    "league_name": league_name,
                    "home": home,
                    "away": away,
                    "starts": starts,
                    "period_number": period_number,
                    "period_description": period_description,
                    "market": "moneyline",
                    "line": None,
                    "side": side,
                    "ts_iso": ts_iso,
                    "ts_epoch": ts_epoch,
                    "price": price,
                    "limit": limit,
                }

        # spreads
        spreads = hist.get("spreads") or {}
        for line, sides in spreads.items():
            if not isinstance(sides, dict):
                continue
            for side in ("home", "away"):
                seq = sides.get(side) or []
                for row in seq:
                    if not isinstance(row, (list, tuple)) or len(row) < 2:
                        continue
                    ts, price = row[0], row[1]
                    limit = row[2] if len(row) > 2 else None
                    ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                    yield {
                        "event_id": event_id_local,
                        "sport_id": sport_id,
                        "league_id": league_id,
                        "league_name": league_name,
                        "home": home,
                        "away": away,
                        "starts": starts,
                        "period_number": period_number,
                        "period_description": period_description,
                        "market": "spread",
                        "line": line,
                        "side": side,
                        "ts_iso": ts_iso,
                        "ts_epoch": ts_epoch,
                        "price": price,
                        "limit": limit,
                    }

        # totals
        totals = hist.get("totals") or {}
        for line, sides in totals.items():
            if not isinstance(sides, dict):
                continue
            for side in ("over", "under"):
                seq = sides.get(side) or []
                for row in seq:
                    if not isinstance(row, (list, tuple)) or len(row) < 2:
                        continue
                    ts, price = row[0], row[1]
                    limit = row[2] if len(row) > 2 else None
                    ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                    yield {
                        "event_id": event_id_local,
                        "sport_id": sport_id,
                        "league_id": league_id,
                        "league_name": league_name,
                        "home": home,
                        "away": away,
                        "starts": starts,
                        "period_number": period_number,
                        "period_description": period_description,
                        "market": "total",
                        "line": line,
                        "side": side,
                        "ts_iso": ts_iso,
                        "ts_epoch": ts_epoch,
                        "price": price,
                        "limit": limit,
                    }

    def _iter_all_ticks(doc: Dict[str, Any]):
        events = doc.get("events") if isinstance(doc, dict) else None
        if isinstance(events, list) and events:
            for event in events:
                periods = event.get("periods") or {}
                for pkey, period in periods.items():
                    if not isinstance(period, dict):
                        continue
                    for row in _iter_event_period_ticks(event, pkey, period):
                        yield row
        elif isinstance(doc, dict) and any(k in doc for k in ("event_id", "eventId", "periods")):
            event = doc
            periods = event.get("periods") or {}
            for pkey, period in periods.items():
                if not isinstance(period, dict):
                    continue
                for row in _iter_event_period_ticks(event, pkey, period):
                    yield row

    rows = list(_iter_all_ticks(details))
    rows.sort(key=lambda r: (
        r.get("event_id"),
        r.get("period_number"),
        r.get("ts_epoch"),
        str(r.get("market")),
        str(r.get("line")),
        str(r.get("side")),
    ))

    # Derive filename: YYYY-MM-DD_Team1_Team2.csv
    single_event = None
    evs = details.get("events") if isinstance(details, dict) else None
    if isinstance(evs, list) and evs:
        single_event = evs[0]
    elif isinstance(details, dict):
        single_event = details
    home = (single_event or {}).get("home") or "home"
    away = (single_event or {}).get("away") or "away"
    starts = (single_event or {}).get("starts") or ""
    dt = _parse_iso_utc(str(starts) or "")
    date_str = dt.date().isoformat() if dt else str(starts)[:10]
    fname = f"{date_str}_{_name_compact(home)}_{_name_compact(away)}.csv"

    with open(fname, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "event_id", "sport_id", "league_id", "league_name", "home", "away", "starts",
                "period_number", "period_description",
                "market", "line", "side", "ts_iso", "ts_epoch", "price", "limit",
            ],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return fname


def _ensure_output_dir(sport_name: str, league_name: str) -> str:
    base = os.path.join("outputs", _sanitize_name(sport_name), _sanitize_name(league_name))
    os.makedirs(base, exist_ok=True)
    return base


def _sport_id_from(item: Dict[str, Any]) -> Optional[int]:
    for k in ("id", "sport_id"):
        if k in item:
            try:
                return int(item[k])
            except Exception:
                continue
    return None


def _sport_name_from(item: Dict[str, Any]) -> str:
    for k in ("name", "sport_name", "title"):
        if k in item and item[k] is not None:
            return str(item[k])
    return ""


def _league_id_from(item: Dict[str, Any]) -> Optional[int]:
    for k in ("league_id", "id"):
        if k in item:
            try:
                return int(item[k])
            except Exception:
                continue
    return None


def _league_name_from(item: Dict[str, Any]) -> str:
    for k in ("league_name", "name", "title"):
        if k in item and item[k] is not None:
            return str(item[k])
    return ""


def _parse_iso_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    s = ts.strip()
    try:
        # Normalize Zulu to offset
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Drop fractional seconds if present (fromisoformat handles them, but be safe)
        # Example: 2024-11-23T00:00:00.123+00:00 -> 2024-11-23T00:00:00+00:00
        if "." in s:
            base, rest = s.split(".", 1)
            # keep timezone offset if exists after fractional part
            if "+" in rest or "-" in rest:
                for sep in ["+", "-"]:
                    if sep in rest:
                        frac, tz = rest.split(sep, 1)
                        s = base + sep + tz
                        break
            else:
                s = base
        # Python's fromisoformat handles offsets like +00:00
        return datetime.fromisoformat(s)
    except Exception:
        # Last resort: try without seconds
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(ts, fmt)
            except Exception:
                continue
    return None


def _is_test_event(ev: Dict[str, Any]) -> bool:
    home = _normalize(str(ev.get("home") or ""))
    away = _normalize(str(ev.get("away") or ""))
    return ("test" in home) or ("test" in away)


def choose_from_list(title: str, rows: List[Tuple[str, Any]], emoji: str = "👉") -> Any:
    _header(title, emoji)
    for idx, (label, _) in enumerate(rows, start=1):
        print(f"  {idx}. {label}")
    while True:
        sel = input("\nType a number and press Enter: ").strip()
        try:
            n = int(sel)
            if 1 <= n <= len(rows):
                chosen = rows[n - 1]
                print(f"\n✅ Selected: {chosen[0]}")
                return chosen[1]
        except Exception:
            pass
        print("❌ Invalid selection. Try again.")


def step_choose_sport(client: PinnacleOddsClient) -> Tuple[int, str]:
    resp = client.list_sports()
    items = _extract_items(resp, keys=["sports", "data", "result", "response"])
    if not items:
        raise RuntimeError("Unable to fetch sports list")
    options: List[Tuple[str, int]] = []
    id_to_name: Dict[int, str] = {}
    for it in items:
        sid = _sport_id_from(it)
        if sid is None:
            continue
        sname = _sport_name_from(it)
        id_to_name[sid] = sname
        options.append((f"{sid} - {sname}", sid))
    chosen_id = choose_from_list("Choose a Sport", options, emoji="🏟️")
    return int(chosen_id), id_to_name.get(int(chosen_id), "")


def _filter_leagues_by_default(leagues: List[Dict[str, Any]], sport_name: str, show_all: bool) -> List[Dict[str, Any]]:
    if show_all:
        return leagues
    s = _normalize(sport_name)
    # Default filters for US major leagues
    if "basketball" in s:
        allowed = ("nba", "ncaa")
    elif "baseball" in s:
        allowed = ("mlb", "ncaa")
    elif "tennis" in s:
        # Focus on ATP/WTA and Grand Slams
        allowed = (
            "atp",
            "wta",
            "australian open",
            "french open",
            "roland garros",
            "wimbledon",
            "us open",
        )
    elif ("esport" in s) or ("e-sport" in s) or ("e sport" in s):
        # Focus on the biggest titles/leagues
        allowed = (
            # League of Legends
            "league of legends", "lol", "lck", "lec", "lpl", "lcs",
            # CS/Counter-Strike
            "cs2", "csgo", "counter-strike", "esl", "blast",
            # Dota 2
            "dota", "dota 2", "the international", "ti",
            # Valorant
            "valorant", "vct",
        )
    else:
        return leagues
    filtered: List[Dict[str, Any]] = []
    for lg in leagues:
        name = _league_name_from(lg)
        lname = _normalize(name)
        if any(tok in lname for tok in allowed):
            filtered.append(lg)
    # Fallback to all if filter removes everything
    return filtered if filtered else leagues


def step_choose_league(client: PinnacleOddsClient, sport_id: int, sport_name: str, show_all: bool) -> Tuple[int, str]:
    resp = client.list_leagues(sport_id=sport_id)
    leagues = _extract_items(resp, keys=["leagues", "data", "result", "response"])
    if not leagues:
        raise RuntimeError("No leagues found for sport")
    leagues = _filter_leagues_by_default(leagues, sport_name, show_all)
    options: List[Tuple[str, int]] = []
    id_to_name: Dict[int, str] = {}
    for lg in leagues:
        lid = _league_id_from(lg)
        if lid is None:
            continue
        lname = _league_name_from(lg)
        id_to_name[lid] = lname
        options.append((f"{lid} - {lname}", lid))
    chosen_id = choose_from_list("Choose a League", options, emoji="🏆")
    return int(chosen_id), id_to_name.get(int(chosen_id), "")


def step_choose_year() -> int:
    _header("Enter a Year", "🗓️")
    while True:
        s = input("Enter year (YYYY): ").strip()
        try:
            y = int(s)
            if 2000 <= y <= 2100:
                print(f"\n✅ Selected year: {y}")
                return y
        except Exception:
            pass
        print("❌ Invalid year. Try again.")


def _try_archive(
    client: PinnacleOddsClient,
    sport_id: int,
    page_num: int,
    league_id: Optional[int],
    page_size: Optional[int] = None,
    season: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Any:
    try:
        return client.list_archive_events(
            sport_id=sport_id,
            page_num=page_num,
            league_id=league_id,
            page_size=page_size,
            season=season,
            date_from=date_from,
            date_to=date_to,
        )
    except requests.HTTPError as http_err:
        # Try without league filter if present
        if league_id is not None and http_err.response is not None and http_err.response.status_code == 422:
            return client.list_archive_events(
                sport_id=sport_id,
                page_num=page_num,
                league_id=None,
                page_size=page_size,
                season=season,
                date_from=date_from,
                date_to=date_to,
            )
        raise


def _list_archive_events_all(
    client: PinnacleOddsClient,
    sport_id: int,
    league_id: Optional[int],
    max_pages: int = 200,
    debug: bool = False,
    season: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    all_events: List[Dict[str, Any]] = []
    page_num = 1
    seen_event_ids = set()
    while page_num <= max_pages:
        try:
            payload = _try_archive(
                client,
                sport_id=sport_id,
                page_num=page_num,
                league_id=league_id,
                page_size=250,
                season=season,
                date_from=date_from,
                date_to=date_to,
            )
        except requests.HTTPError as http_err:
            # Give a clearer message including server response
            body = getattr(http_err.response, "text", "") if http_err.response is not None else ""
            raise RuntimeError(f"Archive rejected (status {getattr(http_err.response, 'status_code', '?')}): {body}")
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list) or not events:
            break
        if debug:
            starts_list = [str(_get_first(e, ["starts", "start_time", "startTime"], "")) for e in events if isinstance(e, dict)]
            dts = [dt for dt in (_parse_iso_utc(s) for s in starts_list) if dt is not None]
            min_dt = min(dts).isoformat() if dts else "?"
            max_dt = max(dts).isoformat() if dts else "?"
            print(f"[debug] archive page {page_num}: events={len(events)} date_range=[{min_dt} .. {max_dt}]")
        for ev in events:
            try:
                eid = int(ev.get("event_id") or 0)
            except Exception:
                continue
            if eid and eid not in seen_event_ids:
                all_events.append(ev)
                seen_event_ids.add(eid)
        # Advance page number
        page_num += 1
    return all_events


def _write_year_csv(year_to_events: Dict[int, Dict[int, Dict[str, Any]]], out_dir: str, league_name: str, year: int) -> None:
    events_by_id = year_to_events.get(year, {})
    rows = []
    for eid, ev in events_by_id.items():
        rows.append({
            "event_id": eid,
            "league_id": _get_first(ev, ["league_id", "leagueId"]),
            "starts": _get_first(ev, ["starts", "start_time", "startTime"]),
            "home": ev.get("home"),
            "away": ev.get("away"),
        })
    rows.sort(key=lambda r: str(r.get("starts") or ""))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{_sanitize_name(league_name)}_{year}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["event_id", "league_id", "starts", "home", "away"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def _page_date_range(events: List[Dict[str, Any]]) -> Tuple[Optional[datetime], Optional[datetime]]:
    starts_list = [str(_get_first(e, ["starts", "start_time", "startTime"], "")) for e in events if isinstance(e, dict)]
    dts = [dt for dt in (_parse_iso_utc(s) for s in starts_list) if dt is not None]
    if not dts:
        return None, None
    return min(dts), max(dts)


def _find_page_for_year(
    client: PinnacleOddsClient,
    sport_id: int,
    league_id: int,
    target_year: int,
    max_pages: int = 5000,
    debug: bool = False,
    season: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Optional[int]:
    page = 1
    while page <= max_pages:
        try:
            payload = _try_archive(
                client,
                sport_id=sport_id,
                page_num=page,
                league_id=league_id,
                page_size=250,
                season=season,
                date_from=date_from,
                date_to=date_to,
            )
        except requests.HTTPError:
            break
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list) or len(events) == 0:
            break
        # quick check for any in target year
        found = False
        for ev in events:
            dt = _parse_iso_utc(str(_get_first(ev, ["starts", "start_time", "startTime"], "") or ""))
            if dt is not None and dt.year == target_year:
                found = True
                break
        min_dt, max_dt = _page_date_range(events)
        if debug:
            print(f"[debug] seek page {page}: date_range=[{min_dt.isoformat() if min_dt else '?'} .. {max_dt.isoformat() if max_dt else '?'}], found_year={found}")
        if found:
            return page
        # If everything on this page is newer than target, go to older pages (page+1)
        # We always advance; provider ordering appears newest -> older with increasing page_num
        page += 1
    return None


def browse_archive(
    client: PinnacleOddsClient,
    sport_id: int,
    sport_name: str,
    league_id: int,
    league_name: str,
    start_year: Optional[int] = None,
    debug: bool = False,
) -> Optional[int]:
    # Interactive pager with per-year CSV dumps to outputs/<sport>/<league>/
    _header("Browse Archive Pages", "📚")
    out_dir = _ensure_output_dir(sport_name, league_name)
    page_num = 1
    year_to_events: Dict[int, Dict[int, Dict[str, Any]]] = {}
    show_only_selected = True  # toggle to show all events vs only selected league
    # If user specified a target year, try to seek to a page containing that year
    season = None
    date_from = None
    date_to = None
    if start_year is not None:
        # Attempt season string like "2023-2024" and a date range for the year
        try:
            season = f"{start_year}-{start_year+1}"
            date_from = f"{start_year}-01-01"
            date_to = f"{start_year}-12-31"
        except Exception:
            season = None
        sought = _find_page_for_year(
            client,
            sport_id=sport_id,
            league_id=league_id,
            target_year=start_year,
            debug=debug,
            season=season,
            date_from=date_from,
            date_to=date_to,
        )
        if sought is not None:
            page_num = sought
            if debug:
                print(f"[debug] jumped to page {page_num} for year {start_year}")
        else:
            print(f"(Could not locate pages for year {start_year}; starting at page 1)")
    while True:
        try:
            payload = _try_archive(
                client,
                sport_id=sport_id,
                page_num=page_num,
                league_id=league_id,
                page_size=250,
                season=season,
                date_from=date_from,
                date_to=date_to,
            )
        except requests.HTTPError as http_err:
            body = getattr(http_err.response, "text", "") if http_err.response is not None else ""
            print(f"Error fetching archive (status {getattr(http_err.response, 'status_code', '?')}): {body}")
            return None
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            events = []
        # Client-filter by league_id
        page_events: List[Dict[str, Any]] = []
        for ev in events:
            try:
                lid = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
            except Exception:
                continue
            if lid != league_id:
                continue
            page_events.append(ev)

        # Update per-year cache and write CSVs for any years present on this page
        years_seen_this_page = set()
        for ev in page_events:
            starts = str(_get_first(ev, ["starts", "start_time", "startTime"], "") or "")
            dt = _parse_iso_utc(starts)
            if dt is None:
                continue
            yr = int(dt.year)
            years_seen_this_page.add(yr)
            eid_val = _get_first(ev, ["event_id", "eventId"], None)
            try:
                eid = int(eid_val) if eid_val is not None else None
            except Exception:
                eid = None
            if not eid:
                continue
            if yr not in year_to_events:
                year_to_events[yr] = {}
            year_to_events[yr][eid] = ev

        for yr in sorted(years_seen_this_page):
            if debug:
                print(f"[debug] writing {os.path.join(out_dir, f'{_sanitize_name(league_name)}_{yr}.csv')} with {len(year_to_events.get(yr, {}))} events")
            _write_year_csv(year_to_events, out_dir, league_name, yr)

        # Show the page list
        _header(f"Page {page_num}", "📄")
        if debug:
            dts = [
                _parse_iso_utc(str(_get_first(ev, ["starts", "start_time", "startTime"], "") or ""))
                for ev in (page_events if show_only_selected else events)
            ]
            dts = [dt for dt in dts if dt is not None]
            if dts:
                print(f"[debug] page date_range=[{min(dts).isoformat()} .. {max(dts).isoformat()}]")
        total_count = len(events)
        match_count = len(page_events)
        print(f"(Showing {'selected league only' if show_only_selected else 'all events'}: {match_count if show_only_selected else total_count} shown | {match_count} match, {total_count} total)")
        if show_only_selected and not page_events:
            print("(No events on this page for the selected league)")
        options: List[Tuple[str, Any]] = []
        display_events = page_events if show_only_selected else events
        for ev in display_events:
            eid = _get_first(ev, ["event_id", "eventId"])  # raw
            home = str(ev.get("home") or "")
            away = str(ev.get("away") or "")
            if _is_test_event(ev):
                continue
            starts = str(_get_first(ev, ["starts", "start_time", "startTime"], "") or "")
            league_label = league_name or str(_get_first(ev, ["league_name", "leagueName"], ""))
            options.append((f"{starts} | {league_label} | {home} vs {away} | event_id={eid}", eid))
        for idx, (label, _) in enumerate(options, start=1):
            print(f"  {idx}. {label}")
        print("\nCommands: [number]=select  n=next  p=prev  j=jump  y=year  t=toggle  f=find-next  b=find-prev  a=all-league  q=quit")
        cmd = input("Enter command: ").strip().lower()
        if cmd == "n":
            page_num += 1
            continue
        if cmd == "p":
            page_num = max(1, page_num - 1)
            continue
        if cmd == "j":
            where = input("Jump to page #: ").strip()
            try:
                pn = int(where)
                if pn >= 1:
                    page_num = pn
                    continue
            except Exception:
                print("❌ Invalid page number")
            continue
        if cmd == "y":
            where = input("Jump to year (YYYY): ").strip()
            try:
                target_y = int(where)
                sought = _find_page_for_year(client, sport_id=sport_id, league_id=league_id, target_year=target_y, debug=debug)
                if sought is not None:
                    page_num = sought
                else:
                    print("(Could not find pages for that year)")
            except Exception:
                print("❌ Invalid year")
            continue
        if cmd == "t":
            show_only_selected = not show_only_selected
            continue
        if cmd == "f":
            # Find next page with at least one matching event
            probe = page_num + 1
            found = False
            while True:
                try:
                    payload2 = _try_archive(
                        client,
                        sport_id=sport_id,
                        page_num=probe,
                        league_id=league_id,
                        page_size=250,
                        season=season,
                        date_from=date_from,
                        date_to=date_to,
                    )
                except requests.HTTPError:
                    break
                events2 = payload2.get("events") if isinstance(payload2, dict) else None
                if not isinstance(events2, list) or len(events2) == 0:
                    break
                any_match = False
                for ev in events2:
                    try:
                        lid = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
                    except Exception:
                        continue
                    if lid == league_id:
                        any_match = True
                        break
                if any_match:
                    page_num = probe
                    found = True
                    break
                probe += 1
            if not found:
                print("(No next page with selected league found)")
            continue
        if cmd == "b":
            # Find previous page with at least one matching event
            probe = max(1, page_num - 1)
            found = False
            while probe >= 1:
                try:
                    payload2 = _try_archive(
                        client,
                        sport_id=sport_id,
                        page_num=probe,
                        league_id=league_id,
                        page_size=250,
                        season=season,
                        date_from=date_from,
                        date_to=date_to,
                    )
                except requests.HTTPError:
                    break
                events2 = payload2.get("events") if isinstance(payload2, dict) else None
                if not isinstance(events2, list) or len(events2) == 0:
                    probe -= 1
                    continue
                any_match = False
                for ev in events2:
                    try:
                        lid = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
                    except Exception:
                        continue
                    if lid == league_id:
                        any_match = True
                        break
                if any_match:
                    page_num = probe
                    found = True
                    break
                probe -= 1
            if not found:
                print("(No previous page with selected league found)")
            continue
        if cmd == "q":
            return None
        try:
            sel = int(cmd)
            if 1 <= sel <= len(options):
                chosen_eid = int(options[sel - 1][1])
                _header("Exporting Cleaned Tick CSV", "📦")
                print(f"Fetching historical odds for event_id={chosen_eid} ...")
                try:
                    details = client.event_details(event_id=chosen_eid)
                    fname = _export_event_csv_from_details(details)
                    print(f"🎉 Done: {fname}")
                except Exception as exp:
                    print(f"Error exporting event {chosen_eid}: {exp}")
                # Stay on page; do not return
                continue
            print("❌ Invalid selection")
        except Exception:
            if cmd == "a":
                # Aggregate all events for this league across all pages without any date filters
                print("Aggregating all events for this league across available pages (no date filters)...")
                out_dir = _ensure_output_dir(sport_name, league_name)
                year_to_events: Dict[int, Dict[int, Dict[str, Any]]] = {}
                all_rows: List[Tuple[str, str, str, int]] = []  # (starts, home, away, event_id)
                probe = 1
                gathered = 0
                max_pages_scan = 5000
                while probe <= max_pages_scan:
                    try:
                        payload2 = _try_archive(
                            client,
                            sport_id=sport_id,
                            page_num=probe,
                            league_id=league_id,
                            page_size=250,
                            season=None,
                            date_from=None,
                            date_to=None,
                        )
                    except requests.HTTPError:
                        break
                    events2 = payload2.get("events") if isinstance(payload2, dict) else None
                    if not isinstance(events2, list) or len(events2) == 0:
                        break
                    page_added = 0
                    for ev in events2:
                        try:
                            lid2 = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
                        except Exception:
                            continue
                        if lid2 != league_id:
                            continue
                        starts = str(_get_first(ev, ["starts", "start_time", "startTime"], "") or "")
                        dt = _parse_iso_utc(starts)
                        year_key = dt.year if dt is not None else None
                        eid_val = _get_first(ev, ["event_id", "eventId"], None)
                        try:
                            eid = int(eid_val) if eid_val is not None else None
                        except Exception:
                            eid = None
                        if not eid:
                            continue
                        home = str(ev.get("home") or "")
                        away = str(ev.get("away") or "")
                        all_rows.append((starts, home, away, eid))
                        if year_key is not None:
                            if year_key not in year_to_events:
                                year_to_events[year_key] = {}
                            year_to_events[year_key][eid] = ev
                        gathered += 1
                        page_added += 1
                    # Debug printing not available here (args not in scope)
                    probe += 1
                # Write combined CSV
                combined_path = os.path.join(out_dir, f"{_sanitize_name(league_name)}_all.csv")
                # Export raw JSON for each event as first step for inspection
                with open(combined_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["event_id", "starts", "home", "away", "raw_event_json"])
                    for starts, home, away, eid in sorted(all_rows, key=lambda r: r[0]):
                        raw = {}
                        # find the event dict again to serialize minimally
                        # (this is acceptable performance for CSV generation)
                        # We won't filter tests here since we already filtered in display above.
                        # Note: For large datasets, consider accumulating dicts alongside all_rows.
                        raw = {"event_id": eid, "starts": starts, "home": home, "away": away}
                        writer.writerow([eid, starts, home, away, json.dumps(raw)])
                # Write per-year CSVs
                for yr in sorted(year_to_events.keys()):
                    _write_year_csv(year_to_events, out_dir, league_name, yr)
                print(f"Done. Wrote {gathered} events to {combined_path} and per-year files.")
                continue
            print("❌ Unknown command")
        # loop continues


def _list_events_via_markets(client: PinnacleOddsClient, sport_id: int, debug: bool = False) -> List[Dict[str, Any]]:
    collected: Dict[int, Dict[str, Any]] = {}
    for et in ("prematch", "live", None):
        payload = client.list_markets(sport_id=sport_id, event_type=et, is_have_odds=None)
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            continue
        if debug:
            print(f"[debug] list_markets et={et!r} -> {len(events)} events")
        for ev in events:
            try:
                eid = int(_get_first(ev, ["event_id", "eventId"], 0) or 0)
            except Exception:
                continue
            if eid > 0:
                collected[eid] = ev
    if debug:
        print(f"[debug] markets collected unique events: {len(collected)}")
    return list(collected.values())


def step_choose_event(client: PinnacleOddsClient, sport_id: int, league_id: int, year: int, debug: bool = False) -> int:
    print("Fetching events. This may take a moment...")
    # Try archive first; if it fails or returns nothing, fallback to markets
    events: List[Dict[str, Any]] = []
    try:
        if debug:
            print(f"[debug] archive fetch: sport_id={sport_id}, league_id={league_id}")
        pass
    except Exception as exc:
        if debug:
            print(f"[debug] archive error: {exc}")
        events = []
    if not events:
        events = _list_events_via_markets(client, sport_id=sport_id, debug=debug)

    # Filter by league and year
    filtered: List[Dict[str, Any]] = []
    for ev in events:
        try:
            lid = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
        except Exception:
            continue
        if lid != league_id:
            continue
        starts = str(_get_first(ev, ["starts", "start_time", "startTime"], "") or "")
        dt = _parse_iso_utc(starts)
        if dt is None or dt.year != year:
            continue
        filtered.append(ev)

    if not filtered:
        if debug:
            # Show a few sample events for diagnosis
            print(f"[debug] no filtered events for year={year}, league_id={league_id}. Showing sample of {min(5, len(events))} events:")
            for ev in events[:5]:
                print({
                    "event_id": _get_first(ev, ["event_id", "eventId"]),
                    "league_id": _get_first(ev, ["league_id", "leagueId"]),
                    "starts": _get_first(ev, ["starts", "start_time", "startTime"]),
                    "home": ev.get("home"),
                    "away": ev.get("away"),
                })
        raise RuntimeError("No events found for that league and year.")

    options: List[Tuple[str, int]] = []
    for ev in sorted(filtered, key=lambda e: str(e.get("starts") or "")):
        try:
            eid = int(ev.get("event_id") or 0)
        except Exception:
            continue
        home = str(ev.get("home") or "")
        away = str(ev.get("away") or "")
        starts = str(ev.get("starts") or "")
        options.append((f"{starts} | {home} vs {away} | event_id={eid}", eid))
    return choose_from_list("Choose an Event", options, emoji="📅")


def _find_histories(node: Any, path: str) -> List[Tuple[str, Any]]:
    """
    Return list of (path, history_collection) where key endswith _history.
    Supports both list-of-dicts and dict-of-(list-of-dicts).
    """
    found: List[Tuple[str, Any]] = []
    if isinstance(node, dict):
        for k, v in node.items():
            p = f"{path}.{k}" if path else k
            if isinstance(k, str) and k.endswith("_history"):
                found.append((p, v))
            # continue walking for nested structures
            found.extend(_find_histories(v, p))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            p = f"{path}[{i}]"
            found.extend(_find_histories(v, p))
    return found


def _rows_from_history(path: str, history: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    # If dict mapping line_key -> list[dict]
    if isinstance(history, dict):
        for line_key, seq in history.items():
            if isinstance(seq, list):
                for item in seq:
                    if isinstance(item, dict):
                        row = {"path": path, "line_key": str(line_key)}
                        row.update(item)
                        rows.append(row)
        return rows
    # If list[dict]
    if isinstance(history, list):
        for item in history:
            if isinstance(item, dict):
                row = {"path": path}
                row.update(item)
                rows.append(row)
    return rows


def _pick_timestamp(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("timestamp", "time", "updated_at", "ts"):
        if k in obj and obj[k] is not None:
            return str(obj[k])
    return None


def export_event_history_to_csv(client: PinnacleOddsClient, event_id: int, output_path: str) -> None:
    details = client.event_details(event_id=event_id)
    histories = _find_histories(details, path="")
    all_rows: List[Dict[str, Any]] = []
    for path, hist in histories:
        all_rows.extend(_rows_from_history(path, hist))

    if not all_rows:
        # Fallback: write a single JSON snapshot row
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["note", "details_json"])
            import json as _json
            w.writerow(["no *_history arrays found; writing raw details", _json.dumps(details)])
        return

    # Normalize columns
    # Required columns first
    fieldnames: List[str] = ["path", "line_key", "timestamp"]
    # Collect other keys present
    other_keys = set()
    for r in all_rows:
        for k in r.keys():
            if k not in ("path", "line_key") and k != "timestamp":
                other_keys.add(k)
    # Try to place common keys early
    preferred = ["home", "away", "draw", "over", "under", "price", "odds", "points", "handicap", "line", "value"]
    ordered_others = [k for k in preferred if k in other_keys] + sorted(k for k in other_keys if k not in preferred)
    fieldnames.extend(ordered_others)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in all_rows:
            out = {k: r.get(k) for k in ("path", "line_key")}
            out["timestamp"] = _pick_timestamp(r)
            for k in ordered_others:
                out[k] = r.get(k)
            writer.writerow(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Terminal UI for Pinnacle Odds navigation and CSV export")
    parser.add_argument("--all", action="store_true", help="Show all leagues (disable NBA/MLB filtering)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose debug output")
    # Non-interactive finder
    parser.add_argument("--find-year", type=int, default=None, help="Auto-fetch all events for a given year and league query")
    parser.add_argument("--league-query", default=None, help="Substring to match league names (e.g., NCAA)")
    parser.add_argument("--sport-name-filter", default=None, help="Optional sport name filter (e.g., Basketball, American Football)")
    parser.add_argument("--max-pages", type=int, default=2000, help="Max pages to scan when auto-finding")
    args = parser.parse_args()
    api_key = os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("Error: Provide RapidAPI key via USER_API_KEY in .env.", file=sys.stderr)
        if _should_pause():
            input("Press Enter to exit...")
        return 1

    client = PinnacleOddsClient(api_key=api_key)

    # Non-interactive auto find by year/league query
    if args.find_year and args.league_query:
        target_year = int(args.find_year)
        league_query = _normalize(args.league_query)
        sport_filter = _normalize(args.sport_name_filter) if args.sport_name_filter else None
        # Discover candidate sports
        sports_resp = client.list_sports()
        sports = _extract_items(sports_resp, keys=["sports", "data", "result", "response"])
        total_found = 0
        for sp in sports:
            sid = _sport_id_from(sp)
            sname = _sport_name_from(sp)
            if sid is None:
                continue
            if sport_filter and sport_filter not in _normalize(sname):
                continue
            leagues_resp = client.list_leagues(sport_id=sid)
            leagues = _extract_items(leagues_resp, keys=["leagues", "data", "result", "response"])
            for lg in leagues:
                lid = _league_id_from(lg)
                lname = _league_name_from(lg)
                if lid is None or not lname:
                    continue
                if league_query not in _normalize(lname):
                    continue
                # Seek to target year for this league
                sought = _find_page_for_year(
                    client,
                    sport_id=sid,
                    league_id=lid,
                    target_year=target_year,
                    max_pages=args.max_pages,
                    debug=args.debug,
                    season=f"{target_year}-{target_year+1}",
                    date_from=f"{target_year}-01-01",
                    date_to=f"{target_year}-12-31",
                )
                if sought is None:
                    if args.debug:
                        print(f"[debug] no pages for {sname} / {lname} in {target_year}")
                    continue
                # Collect all events for that year from that starting page forward until year changes
                out_dir = _ensure_output_dir(sname, lname)
                year_to_events: Dict[int, Dict[int, Dict[str, Any]]] = {}
                page_num = sought
                while page_num <= args.max_pages:
                    try:
                        payload = _try_archive(
                            client,
                            sport_id=sid,
                            page_num=page_num,
                            league_id=lid,
                            page_size=250,
                            season=f"{target_year}-{target_year+1}",
                            date_from=f"{target_year}-01-01",
                            date_to=f"{target_year}-12-31",
                        )
                    except requests.HTTPError:
                        break
                    events = payload.get("events") if isinstance(payload, dict) else None
                    if not isinstance(events, list) or len(events) == 0:
                        break
                    any_in_year = False
                    for ev in events:
                        try:
                            lid2 = int(_get_first(ev, ["league_id", "leagueId"], 0) or 0)
                        except Exception:
                            continue
                        if lid2 != lid:
                            continue
                        dt = _parse_iso_utc(str(_get_first(ev, ["starts", "start_time", "startTime"], "") or ""))
                        if dt is None:
                            continue
                        if dt.year != target_year:
                            continue
                        any_in_year = True
                        eid_val = _get_first(ev, ["event_id", "eventId"], None)
                        try:
                            eid = int(eid_val) if eid_val is not None else None
                        except Exception:
                            eid = None
                        if not eid:
                            continue
                        if target_year not in year_to_events:
                            year_to_events[target_year] = {}
                        year_to_events[target_year][eid] = ev
                    if args.debug:
                        print(f"[debug] {sname}/{lname} page {page_num}: added {len(year_to_events.get(target_year, {}))} total for {target_year}")
                    # If this page had no target-year matches, stop advancing
                    if not any_in_year:
                        break
                    page_num += 1
                if year_to_events.get(target_year):
                    _write_year_csv(year_to_events, out_dir, lname, target_year)
                    total_found += len(year_to_events[target_year])
                    print(f"Wrote {len(year_to_events[target_year])} events to {os.path.join(out_dir, f'{_sanitize_name(lname)}_{target_year}.csv')}")
        if total_found == 0:
            print("No matching events found. Your plan may not expose that season via archive.")
        if _should_pause():
            input("Press Enter to exit...")
        return 0

    try:
        sport_id, sport_name = step_choose_sport(client)
        if args.debug:
            print(f"[debug] selected sport_id={sport_id}, sport_name={sport_name}")
        league_id, league_name = step_choose_league(client, sport_id, sport_name, args.all)
        if args.debug:
            print(f"[debug] selected league_id={league_id} league_name={league_name}")
        # Ask for year to seek to
        target_year = None
        try:
            _header("Enter a Year to Seek (optional)", "🗓️")
            s = input("Year (YYYY, Enter to skip): ").strip()
            if s:
                target_year = int(s)
        except Exception:
            target_year = None
        # Interactive pager: browse pages and auto-write per-year CSVs
        event_id = browse_archive(
            client,
            sport_id=sport_id,
            sport_name=sport_name,
            league_id=league_id,
            league_name=league_name,
            start_year=target_year,
            debug=args.debug,
        )

        if event_id is not None:
            _header("Exporting Cleaned Tick CSV", "📦")
            print(f"Fetching historical odds for event_id={event_id} ...")
            details = client.event_details(event_id=event_id)

            # Build rows from periods.history across all markets
            def _to_epoch_and_iso(ts_val: Any) -> Tuple[int, str]:
                ts = int(ts_val)
                if ts > 10**12:
                    ts = int(ts // 1000)
                from datetime import datetime, timezone as _tz
                iso = datetime.fromtimestamp(ts, tz=_tz.utc).isoformat()
                return ts, iso

            def _iter_event_period_ticks(event: Dict[str, Any], period_key: str, period: Dict[str, Any]):
                sport_id = event.get("sport_id")
                league_id = event.get("league_id")
                league_name = event.get("league_name")
                home = event.get("home")
                away = event.get("away")
                starts = event.get("starts")
                event_id_local = event.get("event_id") or event.get("eventId")

                period_number = period.get("number")
                period_description = period.get("description")
                hist = (period.get("history") or {})

                # moneyline
                ml = hist.get("moneyline") or {}
                for side in ("home", "away", "draw"):
                    seq = ml.get(side) or []
                    for row in seq:
                        if not isinstance(row, (list, tuple)) or len(row) < 2:
                            continue
                        ts, price = row[0], row[1]
                        limit = row[2] if len(row) > 2 else None
                        ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                        yield {
                            "event_id": event_id_local,
                            "sport_id": sport_id,
                            "league_id": league_id,
                            "league_name": league_name,
                            "home": home,
                            "away": away,
                            "starts": starts,
                            "period_number": period_number,
                            "period_description": period_description,
                            "market": "moneyline",
                            "line": None,
                            "side": side,
                            "ts_iso": ts_iso,
                            "ts_epoch": ts_epoch,
                            "price": price,
                            "limit": limit,
                        }

                # spreads
                spreads = hist.get("spreads") or {}
                for line, sides in spreads.items():
                    if not isinstance(sides, dict):
                        continue
                    for side in ("home", "away"):
                        seq = sides.get(side) or []
                        for row in seq:
                            if not isinstance(row, (list, tuple)) or len(row) < 2:
                                continue
                            ts, price = row[0], row[1]
                            limit = row[2] if len(row) > 2 else None
                            ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                            yield {
                                "event_id": event_id_local,
                                "sport_id": sport_id,
                                "league_id": league_id,
                                "league_name": league_name,
                                "home": home,
                                "away": away,
                                "starts": starts,
                                "period_number": period_number,
                                "period_description": period_description,
                                "market": "spread",
                                "line": line,
                                "side": side,
                                "ts_iso": ts_iso,
                                "ts_epoch": ts_epoch,
                                "price": price,
                                "limit": limit,
                            }

                # totals
                totals = hist.get("totals") or {}
                for line, sides in totals.items():
                    if not isinstance(sides, dict):
                        continue
                    for side in ("over", "under"):
                        seq = sides.get(side) or []
                        for row in seq:
                            if not isinstance(row, (list, tuple)) or len(row) < 2:
                                continue
                            ts, price = row[0], row[1]
                            limit = row[2] if len(row) > 2 else None
                            ts_epoch, ts_iso = _to_epoch_and_iso(ts)
                            yield {
                                "event_id": event_id_local,
                                "sport_id": sport_id,
                                "league_id": league_id,
                                "league_name": league_name,
                                "home": home,
                                "away": away,
                                "starts": starts,
                                "period_number": period_number,
                                "period_description": period_description,
                                "market": "total",
                                "line": line,
                                "side": side,
                                "ts_iso": ts_iso,
                                "ts_epoch": ts_epoch,
                                "price": price,
                                "limit": limit,
                            }

            def _iter_all_ticks(doc: Dict[str, Any]):
                events = doc.get("events") if isinstance(doc, dict) else None
                if isinstance(events, list) and events:
                    for event in events:
                        periods = event.get("periods") or {}
                        for pkey, period in periods.items():
                            if not isinstance(period, dict):
                                continue
                            for row in _iter_event_period_ticks(event, pkey, period):
                                yield row
                elif isinstance(doc, dict) and any(k in doc for k in ("event_id", "eventId", "periods")):
                    event = doc
                    periods = event.get("periods") or {}
                    for pkey, period in periods.items():
                        if not isinstance(period, dict):
                            continue
                        for row in _iter_event_period_ticks(event, pkey, period):
                            yield row

            rows = list(_iter_all_ticks(details))
            rows.sort(key=lambda r: (
                r.get("event_id"),
                r.get("period_number"),
                r.get("ts_epoch"),
                str(r.get("market")),
                str(r.get("line")),
                str(r.get("side")),
            ))

            # Derive filename: team1_team2_YYYY-MM-DD.csv
            single_event = None
            evs = details.get("events") if isinstance(details, dict) else None
            if isinstance(evs, list) and evs:
                single_event = evs[0]
            elif isinstance(details, dict):
                single_event = details
            home = (single_event or {}).get("home") or "home"
            away = (single_event or {}).get("away") or "away"
            starts = (single_event or {}).get("starts") or ""
            dt = _parse_iso_utc(str(starts) or "")
            date_str = dt.date().isoformat() if dt else str(starts)[:10]
            fname = f"{date_str}_{_name_compact(home)}_{_name_compact(away)}.csv"

            with open(fname, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "event_id", "sport_id", "league_id", "league_name", "home", "away", "starts",
                        "period_number", "period_description",
                        "market", "line", "side", "ts_iso", "ts_epoch", "price", "limit",
                    ],
                )
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)
            print(f"🎉 Done: {fname}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if _should_pause():
            input("Press Enter to exit...")
        return 2

    if _should_pause():
        input("Press Enter to exit...")
    return 0


if __name__ == "__main__":
    sys.exit(main())


