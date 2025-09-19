import csv
import os
import sys
from typing import Any, Dict, Iterable, List, Tuple

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from api import PinnacleOddsClient


def _should_pause() -> bool:
    return str(os.getenv("NO_PAUSE", "")).strip().lower() not in ("1", "true", "t", "yes", "y")


def _parse_rows(reader: csv.DictReader) -> Iterable[Dict[str, str]]:
    required = ["event_id", "starts", "home", "away"]
    for r in reader:
        if all(k in r for k in required):
            yield {
                "event_id": (r.get("event_id") or "").strip(),
                "starts": (r.get("starts") or "").strip(),
                "home": (r.get("home") or "").strip(),
                "away": (r.get("away") or "").strip(),
            }
        else:
            keys = reader.fieldnames or []
            if len(keys) >= 4:
                yield {
                    "event_id": (r.get(keys[0]) or "").strip(),
                    "starts": (r.get(keys[1]) or "").strip(),
                    "home": (r.get(keys[2]) or "").strip(),
                    "away": (r.get(keys[3]) or "").strip(),
                }


def dedupe_in_place(csv_path: str) -> int:
    seen = set()
    rows_out_map: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    has_downloaded_col = False
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        has_downloaded_col = "downloaded" in fieldnames
        for raw in reader:
            r = {
                "event_id": (raw.get("event_id") or raw.get(fieldnames[0] if fieldnames else "") or "").strip(),
                "starts": (raw.get("starts") or (fieldnames[1] if len(fieldnames) > 1 else "") and raw.get(fieldnames[1]) or "").strip(),
                "home": (raw.get("home") or (fieldnames[2] if len(fieldnames) > 2 else "") and raw.get(fieldnames[2]) or "").strip(),
                "away": (raw.get("away") or (fieldnames[3] if len(fieldnames) > 3 else "") and raw.get(fieldnames[3]) or "").strip(),
            }
            # Optional downloaded passthrough
            downloaded_val = (raw.get("downloaded") or "").strip() if has_downloaded_col else ""
            if has_downloaded_col:
                r["downloaded"] = downloaded_val

            # filter out test rows
            h = (r.get("home") or "").strip().lower()
            a = (r.get("away") or "").strip().lower()
            if "test" in h or h in ("test 1", "test1"):
                continue
            if "test" in a or a in ("test 1", "test1"):
                    continue
            key = (r.get("starts", ""), r.get("home", ""), r.get("away", ""))
            if key in seen:
                # Merge downloaded state if present
                if has_downloaded_col and (downloaded_val.lower() in ("1", "true", "yes", "y")):
                    rows_out_map[key]["downloaded"] = "yes"
                continue
            seen.add(key)
            rows_out_map[key] = r

    rows_out = list(rows_out_map.values())
    fieldnames_out: List[str] = ["event_id", "starts", "home", "away"]
    if has_downloaded_col:
        fieldnames_out.append("downloaded")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_out)
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)
    return len(rows_out)


def _name_compact(name: str) -> str:
    return "".join(ch for ch in str(name) if ch.isalnum()) or "unknown"


def _export_event_csv_from_details(details: Dict[str, Any], out_dir: str) -> str:
    from datetime import datetime, timezone as _tz

    def _to_epoch_and_iso(ts_val: Any) -> Tuple[int, str]:
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

    rows = []
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


def main(argv: list[str]) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Dedupe league CSV and fetch odds for all events")
    parser.add_argument("--csv", dest="csv_path", required=True, help="Path to league CSV (will be deduped in-place)")
    parser.add_argument("--outdir", dest="out_dir", default="odds", help="Directory to write per-event odds CSVs")
    parser.add_argument("--limit", dest="limit", type=int, default=None, help="Optional limit of events to fetch")
    parser.add_argument("--skip-existing", dest="skip_existing", action="store_true", help="Skip API call if expected output file already exists")
    parser.add_argument("--skip-downloaded", dest="skip_downloaded", action="store_true", help="Skip rows already marked downloaded in input CSV")
    parser.add_argument("--mark-downloaded", dest="mark_downloaded", action="store_true", help="After run, mark downloaded=yes for rows exported or already present")
    args = parser.parse_args(argv[1:])

    csv_path = args.csv_path
    out_dir = args.out_dir
    if not os.path.exists(csv_path):
        print(f"Input not found: {csv_path}", file=sys.stderr)
        if _should_pause():
            try:
                input("Press Enter to exit...")
            except EOFError:
                pass
        return 2

    # Dedupe in place (preserves optional 'downloaded' column)
    n_unique = dedupe_in_place(csv_path)
    print(f"Deduped CSV in-place: {csv_path} ({n_unique} unique rows)")

    # Helpers for existence checks and parsing
    def _parse_iso_utc_for_row(ts: str):
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

    def expected_out_path_for_row(row: Dict[str, str], base_dir: str) -> str:
        dt = _parse_iso_utc_for_row(row.get("starts", ""))
        date_str = dt.date().isoformat() if dt else (row.get("starts", "")[:10])
        fname = f"{date_str}_{_name_compact(row.get('home', 'home'))}_{_name_compact(row.get('away', 'away'))}.csv"
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, fname)

    # Read rows to fetch with optional skipping
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        original_rows: List[Dict[str, str]] = list(reader)

    rows: List[Dict[str, str]] = []
    already_done_event_ids: set[int] = set()
    for raw in original_rows:
        pr = {
            "event_id": (raw.get("event_id") or "").strip(),
            "starts": (raw.get("starts") or "").strip(),
            "home": (raw.get("home") or "").strip(),
            "away": (raw.get("away") or "").strip(),
        }
        h = pr["home"].lower()
        a = pr["away"].lower()
        if "test" in h or h in ("test 1", "test1"):
            continue
        if "test" in a or a in ("test 1", "test1"):
            continue
        # Skip if marked downloaded
        if args.skip_downloaded and (raw.get("downloaded", "").strip().lower() in ("1", "true", "yes", "y")):
            try:
                already_done_event_ids.add(int(pr.get("event_id") or 0))
            except Exception:
                pass
            continue
        # Skip if file already exists
        if args.skip_existing:
            expected_path = expected_out_path_for_row(pr, out_dir)
            if os.path.exists(expected_path):
                try:
                    already_done_event_ids.add(int(pr.get("event_id") or 0))
                except Exception:
                    pass
                continue
        rows.append(pr)

    api_key = os.getenv("USER_API_KEY") or os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("Error: Provide RapidAPI key via USER_API_KEY in .env.", file=sys.stderr)
        if _should_pause():
            try:
                input("Press Enter to exit...")
            except EOFError:
                pass
        return 3
    client = PinnacleOddsClient(api_key=api_key)

    exported = 0
    processed_success_event_ids: set[int] = set()
    for idx, r in enumerate(rows, start=1):
        if args.limit is not None and exported >= args.limit:
            break
        eid_raw = r.get("event_id")
        try:
            eid = int(eid_raw)
        except Exception:
            print(f"Skipping row with invalid event_id: {eid_raw}")
            continue
        try:
            details = client.event_details(event_id=eid)
            out_path = _export_event_csv_from_details(details, out_dir=out_dir)
            exported += 1
            print(f"[{exported}] Wrote {out_path}")
            processed_success_event_ids.add(eid)
        except Exception as exc:
            print(f"Failed to fetch/export event {eid}: {exc}")

    # Optionally mark downloaded column in the CSV
    if args.mark_downloaded:
        # An event is considered downloaded if we successfully exported it or it already existed (when skip_existing is used)
        downloaded_ids = set(processed_success_event_ids) | already_done_event_ids
        # Re-read post-dedupe rows
        with open(csv_path, "r", encoding="utf-8") as f:
            reader2 = csv.DictReader(f)
            fieldnames2 = reader2.fieldnames or []
            rows2 = list(reader2)
        fieldnames_out2: List[str] = ["event_id", "starts", "home", "away"]
        if "downloaded" not in fieldnames2:
            fieldnames_out2.append("downloaded")
        else:
            fieldnames_out2.append("downloaded")
        # Write with updated downloaded column
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer2 = csv.DictWriter(f, fieldnames=fieldnames_out2)
            writer2.writeheader()
            for raw in rows2:
                eid_str = (raw.get("event_id") or "").strip()
                try:
                    eid_int = int(eid_str)
                except Exception:
                    eid_int = -1
                downloaded_val = "yes" if eid_int in downloaded_ids else (raw.get("downloaded") or "")
                writer2.writerow({
                    "event_id": (raw.get("event_id") or "").strip(),
                    "starts": (raw.get("starts") or "").strip(),
                    "home": (raw.get("home") or "").strip(),
                    "away": (raw.get("away") or "").strip(),
                    "downloaded": downloaded_val,
                })

    if _should_pause():
        try:
            input("Press Enter to exit...")
        except EOFError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


