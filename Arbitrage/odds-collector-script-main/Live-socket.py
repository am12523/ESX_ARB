"""
Live polling speed tester ("Live-socket") for Pinnacle Odds via RapidAPI.

Purpose:
- Let you select a game (by event_id or by sport/league/team search)
- Repeatedly fetch the game snapshot and measure retrieval latency
- Print per-iteration timings and summary stats (min/avg/p50/p95/max)

Usage examples:
  python Live-socket.py --event-id 1609669239 --polls 20 --interval 1.0 --measure markets --print-json
  python Live-socket.py --sport "Baseball" --league "MLB" --home "Yankees" --away "Braves" --polls 15 --interval 2

API key resolution:
- Reads USER_API_KEY or RAPIDAPI_KEY from environment (or .env if present)

Notes:
- There is no real-time websocket in this API; we simulate "live" by polling.
- Endpoints:
    measure=markets -> GET /kit/v1/markets (current snapshot)
    measure=details -> GET /kit/v1/details (historical details)
"""

import argparse
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import requests

from api import PinnacleOddsClient


def _normalize(text: str) -> str:
    return str(text or "").strip().lower()


def _should_pause() -> bool:
    return str(os.getenv("NO_PAUSE", "")).strip().lower() not in ("1", "true", "t", "yes", "y")


def _extract_list(resp: Any, keys: Tuple[str, ...]) -> List[Dict[str, Any]]:
    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)]
    if isinstance(resp, dict):
        for k in keys:
            maybe = resp.get(k)
            if isinstance(maybe, list):
                return [x for x in maybe if isinstance(x, dict)]
    return []


def _pick_event_from_markets(
    client: PinnacleOddsClient,
    sport_id: Optional[int],
    league_query: Optional[str],
    home_substr: Optional[str],
    away_substr: Optional[str],
    event_type_preference: Tuple[str, str] = ("prematch", "live"),
    interactive: bool = True,
) -> Optional[int]:
    def list_events_for_sport(sid: int) -> List[Dict[str, Any]]:
        collected: Dict[int, Dict[str, Any]] = {}
        for et in (event_type_preference[0], event_type_preference[1], None):
            payload = client.list_markets(sport_id=sid, event_type=et, is_have_odds=None)
            events = payload.get("events") if isinstance(payload, dict) else None
            if not isinstance(events, list):
                continue
            for ev in events:
                try:
                    eid = int(ev.get("event_id") or 0)
                except Exception:
                    continue
                if eid > 0:
                    collected[eid] = ev
        return list(collected.values())

    def sport_id_from_name(name_substr: Optional[str]) -> Optional[int]:
        if not name_substr:
            return None
        s_norm = _normalize(name_substr)
        sports = _extract_list(client.list_sports(), ("sports", "data", "result", "response"))
        for sp in sports:
            for k in ("name", "sport_name", "title"):
                nm = sp.get(k)
                if nm and s_norm in _normalize(str(nm)):
                    for idk in ("id", "sport_id"):
                        if idk in sp:
                            try:
                                return int(sp[idk])
                            except Exception:
                                continue
        return None

    sid = sport_id or sport_id_from_name(args.sport)
    events: List[Dict[str, Any]] = []

    if sid is not None:
        events = list_events_for_sport(sid)
    else:
        # No sport specified: scan all sports
        sports = _extract_list(client.list_sports(), ("sports", "data", "result", "response"))
        for sp in sports:
            try:
                sid2 = int(sp.get("id") or sp.get("sport_id") or 0)
            except Exception:
                continue
            if sid2 <= 0:
                continue
            events.extend(list_events_for_sport(sid2))

    # filter
    lq = _normalize(league_query) if league_query else None
    hs = _normalize(home_substr) if home_substr else None
    as_ = _normalize(away_substr) if away_substr else None
    filtered: List[Dict[str, Any]] = []
    for ev in events:
        league_name = str(ev.get("league_name") or "")
        home = str(ev.get("home") or "")
        away = str(ev.get("away") or "")
        if lq and lq not in _normalize(league_name):
            continue
        if hs and hs not in _normalize(home):
            continue
        if as_ and as_ not in _normalize(away):
            continue
        filtered.append(ev)

    if not filtered:
        return None

    # choose
    filtered.sort(key=lambda e: str(e.get("starts") or ""))
    if interactive and len(filtered) > 1:
        print("\nMultiple matches. Choose one:\n")
        for i, ev in enumerate(filtered[:25], start=1):
            label = f"{ev.get('starts')} | {ev.get('league_name')} | {ev.get('home')} vs {ev.get('away')} | event_id={ev.get('event_id')}"
            print(f"  {i}. {label}")
        while True:
            sel = input("Type a number (1..{n}) or Enter for 1st: ".format(n=min(25, len(filtered)))).strip()
            if not sel:
                chosen = filtered[0]
                return int(chosen.get("event_id") or 0)
            try:
                n = int(sel)
                if 1 <= n <= min(25, len(filtered)):
                    chosen = filtered[n - 1]
                    return int(chosen.get("event_id") or 0)
            except Exception:
                pass
            print("Invalid selection. Try again.")
    # Non-interactive or single match
    return int(filtered[0].get("event_id") or 0)


def _peek_snapshot_fields_from_markets(event: Dict[str, Any]) -> Dict[str, Any]:
    periods = event.get("periods") or {}
    main = periods.get("num_0") or {}
    ml = main.get("money_line") or {}
    meta = main.get("meta") or {}
    return {
        "moneyline": ml,
        "meta": meta,
    }


def _find_event_in_markets_payload(payload: Dict[str, Any], event_id: int) -> Optional[Dict[str, Any]]:
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return None
    for ev in events:
        try:
            if int(ev.get("event_id") or 0) == int(event_id):
                return ev
        except Exception:
            continue
    return None


def measure_polling(
    client: PinnacleOddsClient,
    event_id: int,
    sport_id_hint: Optional[int],
    measure: str,
    polls: int,
    interval_sec: float,
    timeout_seconds: float,
    print_json: bool,
    csv_path: Optional[str],
    event_type_preference: Tuple[str, str],
) -> None:
    # Adjust client timeout
    client.timeout_seconds = float(timeout_seconds)

    rows: List[Tuple[int, float, Optional[str]]] = []  # (iteration, latency_ms, error)

    def do_call_markets() -> Tuple[Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.perf_counter()
        try:
            # If sport_id not provided, find it via details once (lazy)
            nonlocal sport_id_hint
            if sport_id_hint is None:
                details = client.event_details(event_id=event_id)
                if isinstance(details, dict):
                    for key in ("sport_id", "sportId"):
                        if key in details:
                            try:
                                sport_id_hint = int(details[key])
                                break
                            except Exception:
                                pass
            # Fallback: scan markets across sports
            if sport_id_hint is None:
                sports = _extract_list(client.list_sports(), ("sports", "data", "result", "response"))
                for sp in sports:
                    try:
                        sid2 = int(sp.get("id") or sp.get("sport_id") or 0)
                    except Exception:
                        continue
                    if sid2 <= 0:
                        continue
                    payload = client.list_markets(sport_id=sid2, event_type=event_type_preference[0], is_have_odds=None)
                    if _find_event_in_markets_payload(payload, event_id):
                        sport_id_hint = sid2
                        break
            if sport_id_hint is None:
                raise RuntimeError("Could not resolve sport_id for event")
            # Fetch snapshot for the target sport
            payload = client.list_markets(sport_id=sport_id_hint, event_type=event_type_preference[0], is_have_odds=None)
            ev = _find_event_in_markets_payload(payload, event_id)
            end = time.perf_counter()
            latency_ms = (end - start) * 1000.0
            return ev, latency_ms, None
        except Exception as exc:
            end = time.perf_counter()
            latency_ms = (end - start) * 1000.0
            return None, latency_ms, str(exc)

    def do_call_details() -> Tuple[Optional[Dict[str, Any]], float, Optional[str]]:
        start = time.perf_counter()
        try:
            data = client.event_details(event_id=event_id)
            end = time.perf_counter()
            latency_ms = (end - start) * 1000.0
            return data if isinstance(data, dict) else None, latency_ms, None
        except Exception as exc:
            end = time.perf_counter()
            latency_ms = (end - start) * 1000.0
            return None, latency_ms, str(exc)

    print(f"\nStarting polling: event_id={event_id} measure={measure} polls={polls} interval={interval_sec}s timeout={timeout_seconds}s")
    for i in range(1, polls + 1):
        if measure == "markets":
            doc, latency_ms, err = do_call_markets()
            if print_json and isinstance(doc, dict):
                # brief snapshot
                if doc.get("event_id") is None:
                    # assume it's event doc if found; otherwise skip JSON print
                    pass
                else:
                    brief = {
                        "event_id": doc.get("event_id"),
                        "home": doc.get("home"),
                        "away": doc.get("away"),
                        "starts": doc.get("starts"),
                        "event_type": doc.get("event_type"),
                        "peek": _peek_snapshot_fields_from_markets(doc),
                    }
                    print(json.dumps(brief, indent=2))
        else:
            doc, latency_ms, err = do_call_details()
            if print_json and isinstance(doc, dict):
                # minimal print to avoid flooding
                ev = None
                evs = doc.get("events") if isinstance(doc, dict) else None
                if isinstance(evs, list) and evs:
                    ev = evs[0]
                elif isinstance(doc, dict) and any(k in doc for k in ("event_id", "eventId", "periods")):
                    ev = doc
                if isinstance(ev, dict):
                    brief = {k: ev.get(k) for k in ("event_id", "home", "away", "starts")}
                    print(json.dumps(brief, indent=2))

        status = "ok" if err is None else f"err: {err}"
        print(f"[{i:03d}] {latency_ms:.1f} ms  {status}")
        rows.append((i, latency_ms, err))
        if i < polls:
            time.sleep(max(0.0, interval_sec))

    # summary
    timings = [lat for _, lat, e in rows if e is None]
    if timings:
        timings_sorted = sorted(timings)
        p50 = statistics.median(timings_sorted)
        def percentile(vals: List[float], p: float) -> float:
            if not vals:
                return float("nan")
            k = (len(vals) - 1) * (p / 100.0)
            f = int(k)
            c = min(f + 1, len(vals) - 1)
            if f == c:
                return vals[f]
            d0 = vals[f] * (c - k)
            d1 = vals[c] * (k - f)
            return d0 + d1
        p95 = percentile(timings_sorted, 95)
        print("\nSummary (successful calls only):")
        print(f"  count={len(timings)} min={min(timings):.1f} ms avg={statistics.mean(timings):.1f} ms p50={p50:.1f} ms p95={p95:.1f} ms max={max(timings):.1f} ms")
    else:
        print("\nSummary: no successful calls.")

    if csv_path:
        try:
            import csv
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["iteration", "latency_ms", "error"])
                for it, lat, err in rows:
                    w.writerow([it, f"{lat:.3f}", err or ""])
            print(f"Wrote timings to {csv_path}")
        except Exception as exc:
            print(f"Failed to write CSV: {exc}")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="Live polling speed tester for Pinnacle Odds")
    parser.add_argument("--event-id", type=int, default=None, help="Target event_id (skips search)")
    parser.add_argument("--sport", default=None, help="Sport name substring (e.g., Baseball, Basketball)")
    parser.add_argument("--league", default=None, help="League name substring (e.g., MLB, NBA)")
    parser.add_argument("--home", dest="home", default=None, help="Home team substring")
    parser.add_argument("--away", dest="away", default=None, help="Away team substring")
    parser.add_argument("--event-type", choices=["prematch", "live"], default="prematch", help="Preferred event type for markets")
    parser.add_argument("--polls", type=int, default=10, help="Number of polls")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between polls")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    parser.add_argument("--measure", choices=["markets", "details"], default="markets", help="Endpoint to measure")
    parser.add_argument("--csv", dest="csv_path", default=None, help="Optional path to write timings CSV")
    parser.add_argument("--print-json", action="store_true", help="Print brief JSON per poll")
    parser.add_argument("--non-interactive", action="store_true", help="Disable interactive selection when multiple matches")
    args = parser.parse_args(argv[1:])

    api_key = os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("Error: Provide RapidAPI key via USER_API_KEY or RAPIDAPI_KEY in your environment.", file=sys.stderr)
        if _should_pause():
            try:
                input("Press Enter to exit...")
            except EOFError:
                pass
        return 1

    client = PinnacleOddsClient(api_key=api_key, timeout_seconds=float(args.timeout))

    # Resolve event_id if not provided
    event_id: Optional[int] = args.event_id
    sport_id_hint: Optional[int] = None

    if event_id is None:
        # If both home and away are not provided, ask user
        home_sub = args.home
        away_sub = args.away
        if not home_sub and not away_sub:
            print("Enter team substrings to search (press Enter to skip a field)")
            try:
                home_sub = input("Home contains: ").strip() or None
                away_sub = input("Away contains: ").strip() or None
            except Exception:
                pass
        eid = _pick_event_from_markets(
            client=client,
            sport_id=None,
            league_query=args.league,
            home_substr=home_sub,
            away_substr=away_sub,
            event_type_preference=(args.event_type, "live" if args.event_type == "prematch" else "prematch"),
            interactive=(not args.non_interactive),
        )
        if not eid:
            print("No matching event found.")
            if _should_pause():
                try:
                    input("Press Enter to exit...")
                except EOFError:
                    pass
            return 2
        event_id = int(eid)

    # Try resolve sport_id hint from details once upfront (best effort)
    try:
        details = client.event_details(event_id=event_id)
        if isinstance(details, dict):
            for key in ("sport_id", "sportId"):
                if key in details:
                    try:
                        sport_id_hint = int(details[key])
                        break
                    except Exception:
                        pass
        ev = None
        evs = details.get("events") if isinstance(details, dict) else None
        if isinstance(evs, list) and evs:
            ev = evs[0]
        elif isinstance(details, dict) and any(k in details for k in ("event_id", "eventId", "periods")):
            ev = details
        if isinstance(ev, dict):
            print(f"Target: {ev.get('home')} vs {ev.get('away')} | starts={ev.get('starts')} | league={ev.get('league_name')}")
    except requests.HTTPError as http_err:
        print(f"Warning: initial details fetch failed: {getattr(http_err.response, 'status_code', '?')} {getattr(http_err.response, 'text', '')[:200]}")
    except Exception as exc:
        print(f"Warning: initial details fetch error: {exc}")

    measure_polling(
        client=client,
        event_id=int(event_id),
        sport_id_hint=sport_id_hint,
        measure=str(args.measure),
        polls=int(args.polls),
        interval_sec=float(args.interval),
        timeout_seconds=float(args.timeout),
        print_json=bool(args.print_json),
        csv_path=args.csv_path,
        event_type_preference=(args.event_type, "live" if args.event_type == "prematch" else "prematch"),
    )

    if _should_pause():
        try:
            input("Press Enter to exit...")
        except EOFError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


