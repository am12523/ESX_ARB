import os
import sys
import json
import csv
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from api import PinnacleOddsClient


def _should_pause() -> bool:
    return str(os.getenv("NO_PAUSE", "")).strip().lower() not in ("1", "true", "t", "yes", "y")


def _normalize(text: str) -> str:
    return str(text or "").strip().lower()


def _name_compact(name: str) -> str:
    return "".join(ch for ch in str(name) if ch.isalnum()) or "unknown"


# Set this to a date string 'YYYY-MM-DD' to force a specific run date.
# Example: FORCE_DATE_ISO = '2025-07-18'
# Set to None to use today's date (UTC) by default.

FORCE_DATE_ISO: Optional[str] = '2025-08-22'

def get_target_date_iso(date_str: Optional[str] = None) -> str:
    """
    Return YYYY-MM-DD in UTC. If date_str provided (YYYY-MM-DD), validate and return it,
    otherwise return today's date in UTC.
    Optionally respects env DAILY_DATE when date_str is None.

    Set DAILY_DATE in your environment or .env, for example:
      DAILY_DATE=2025-07-18
    """
    s = date_str or os.getenv("DAILY_DATE") or ""
    s = (s or "").strip()
    if s:
        try:
            # Basic validation
            dt = datetime.strptime(s, "%Y-%m-%d")
            return dt.date().isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).date().isoformat()


def _export_event_csv_from_details(details: Dict[str, Any], out_dir: str) -> Optional[str]:
    from datetime import datetime, timezone as _tz

    def _to_epoch_and_iso(ts_val: Any):
        ts = int(ts_val)
        if ts > 10**12:
            ts = int(ts // 1000)
        iso = datetime.fromtimestamp(ts, tz=_tz.utc).isoformat()
        return ts, iso

    def _iter_event_period_ticks(event: Dict[str, Any], period: Dict[str, Any]):
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

        ml = hist.get("moneyline") or {}
        for side in ("home", "away", "draw"):
            for row in (ml.get(side) or []):
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

        spreads = hist.get("spreads") or {}
        for line, sides in spreads.items():
            if not isinstance(sides, dict):
                continue
            for side in ("home", "away"):
                for row in (sides.get(side) or []):
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

        totals = hist.get("totals") or {}
        for line, sides in totals.items():
            if not isinstance(sides, dict):
                continue
            for side in ("over", "under"):
                for row in (sides.get(side) or []):
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

    def _parse_iso_utc(ts: str):
        s = (ts or "").strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            if "." in s:
                base, rest = s.split(".", 1)
                if "+" in rest or "-" in rest:
                    for sep in ["+", "-"]:
                        if sep in rest:
                            frac, tz = rest.split(sep, 1)
                            s = base + sep + tz
                            break
                else:
                    s = base
            from datetime import datetime
            return datetime.fromisoformat(s)
        except Exception:
            return None

    events = details.get("events") if isinstance(details, dict) else None
    if isinstance(events, list) and events:
        event = events[0]
    else:
        event = details if isinstance(details, dict) else {}

    rows: List[Dict[str, Any]] = []
    for period in (event.get("periods") or {}).values():
        if isinstance(period, dict):
            rows.extend(list(_iter_event_period_ticks(event, period)))
    rows.sort(key=lambda r: (
        r.get("event_id"),
        r.get("period_number"),
        r.get("ts_epoch"),
        str(r.get("market")),
        str(r.get("line")),
        str(r.get("side")),
    ))

    home = (event.get("home") or "home")
    away = (event.get("away") or "away")
    starts = (event.get("starts") or "")
    dt = _parse_iso_utc(str(starts))
    date_str = dt.date().isoformat() if dt else str(starts)[:10]
    fname = f"{date_str}_{_name_compact(home)}_{_name_compact(away)}.csv"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, fname)

    # Skip if already exported
    if os.path.exists(out_path):
        return None

    # Avoid empty CSVs
    if not rows:
        return None

    with open(out_path, "w", newline="", encoding="utf-8") as f:
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
    return out_path


def run_daily(config_path: str, date_iso: Optional[str] = None) -> int:
    api_key = os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("Error: Provide RapidAPI key via USER_API_KEY in .env.")
        return 2
    client = PinnacleOddsClient(api_key=api_key)

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    target_date = get_target_date_iso(date_iso)
    today_utc = datetime.now(timezone.utc).date().isoformat()
    is_today = (target_date == today_utc)
    total = 0
    for job in cfg.get("jobs", []):
        sport_id = int(job["sport_id"])  # required
        leagues: List[int] = [int(x) for x in job.get("league_ids", [])]
        out_dir = job.get("out_dir") or f"daily_odds_{target_date}"

        events: List[Dict[str, Any]] = []
        if is_today:
            # Current/upcoming snapshot
            payload = client.list_markets(sport_id=sport_id, event_type="prematch", is_have_odds=True)
            events = payload.get("events") if isinstance(payload, dict) else None
            events = events if isinstance(events, list) else []
        else:
            # Past date: pull from archive constrained to the target date
            def fetch_archive_for_league(lid_opt: Optional[int]) -> None:
                page_num = 1
                while True:
                    payload = client.list_archive_events(
                        sport_id=sport_id,
                        page_num=page_num,
                        page_size=250,
                        league_id=lid_opt,
                        date_from=target_date,
                        date_to=target_date,
                    )
                    evs = payload.get("events") if isinstance(payload, dict) else None
                    if not isinstance(evs, list) or len(evs) == 0:
                        break
                    events.extend(evs)
                    page_num += 1

            if leagues:
                for lid in leagues:
                    fetch_archive_for_league(lid)
            else:
                fetch_archive_for_league(None)

        # Process events list with dedupe by (starts, home, away)
        seen_keys = set()
        for ev in events:
            try:
                lid = int(ev.get("league_id") or 0)
                eid = int(ev.get("event_id") or 0)
            except Exception:
                continue
            if leagues and lid not in leagues:
                continue
            starts = str(ev.get("starts") or "")
            if not starts.startswith(target_date):
                continue
            # skip test fixtures
            h = str(ev.get("home") or "").strip().lower()
            a = str(ev.get("away") or "").strip().lower()
            if "test" in h or h in ("test 1", "test1", "test 2", "test2"):
                continue
            if "test" in a or a in ("test 1", "test1", "test 2", "test2"):
                continue
            key = (starts, h, a)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            try:
                details = client.event_details(event_id=eid)
                out_path = _export_event_csv_from_details(details, out_dir=out_dir)
                if out_path:
                    total += 1
                    print(f"[ok] {out_path}")
                else:
                    # Either already exists or empty; report and continue
                    print(f"[skip] {starts} {h} vs {a} (exists or empty)")
            except Exception as exc:
                print(f"[skip] event {eid}: {exc}")

    print(f"Done. Exported {total} event CSVs for {target_date}.")
    return 0


def main(argv: List[str]) -> int:
    # No flags: read default JSON config from project root
    config_path = "daily_odds.json"
    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}")
        if _should_pause():
            try:
                input("Press Enter to exit...")
            except EOFError:
                pass
        return 2
    rc = run_daily(config_path, date_iso=FORCE_DATE_ISO)
    if _should_pause():
        try:
            input("Press Enter to exit...")
        except EOFError:
            pass
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


