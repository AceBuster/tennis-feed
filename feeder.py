#!/usr/bin/env python3
"""
tennis-feed: OUR OWN data service (like the-odds-api.com, but ours!)

Runs on GitHub Actions every 2 hours (or locally:  python feeder.py).
It builds three JSON files into ./data :

  matches.json  - all 2026 matches (ATP+WTA, final player ids inside)
  rankings.json - ONLY the newest ranking week for all players
  today.json    - upcoming fixtures of the next 2 days (needs ODDS_API_KEYsecret)
  feed_meta.json- stamp: when was this built

LEGAL: source data is Jeff Sackmann's archive, license CC BY-NC-SA 4.0.
We must credit him (see README). Do NOT sell the raw data as your own -
sell the PREDICTIONS/derived products, not the raw archive itself.

Environment:
  ODDS_API_KEY   optional - without it today.json has an empty events list
"""
import csv
import io
import json
import os
import pathlib
import urllib.request
from datetime import datetime, timezone

HERE = pathlib.Path(__file__).resolve().parent
DATA = HERE / "data"
DATA.mkdir(exist_ok=True)
STATE_FILE = DATA / "odds_state.json"

HF = "https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive/resolve/main"
GH = "https://raw.githubusercontent.com/Kadantte/tennis_atp/master"
ODDS_BASE = "https://api.the-odds-api.com/v4"
WTA_OFFSET = 1_000_000
UA = {"User-Agent": "tennis-feed/1.0 (personal project)"}
MAX_ODDS_CALLS_PER_RUN = 8


def fetch_bytes(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read()
    except Exception as e:
        print("  fetch failed:", url, "->", e)
        return None


def csv_rows(text: str) -> list[dict]:
    rows = list(csv.DictReader(io.StringIO(text)))
    return rows


def build_matches() -> list[dict]:
    """All 2026 matches from both tours, ready for the app."""
    out = []
    for tour, paths in (('ATP', [f"{GH}/atp_matches_2026.csv", f"{HF}/atp/atp_matches_2026.csv"]),
                        ('WTA', [f"{HF}/wta/wta_matches_2026.csv"])):
        raw = None
        for u in paths:
            raw = fetch_bytes(u)
            if raw:
                print(f"matches {tour}: {len(raw)//1024} KB from {u}")
                break
        if not raw:
            continue
        off = WTA_OFFSET if tour == 'WTA' else 0
        for r in csv_rows(raw.decode('utf-8', 'ignore')):
            wid, lid, d8, num, tid = r.get('winner_id', ''), r.get('loser_id', ''), r.get('tourney_date', ''), r.get('match_num', ''), r.get('tourney_id', '')
            if not wid or not lid or len(d8) != 8 or not num:
                continue
            out.append({
                "id": ('W' if tour == 'WTA' else '') + f"{tid}_{num}_{d8[:4]}",
                "tour": tour,
                "date": f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}",
                "p1": int(wid) + off,
                "p2": int(lid) + off,
                "w": int(wid) + off,
                "s": r.get('surface') or "Hard",
                "r": r.get('round'),
                "n1": r.get('winner_name', ''),
                "n2": r.get('loser_name', ''),
            })
    return out


def build_rankings() -> dict:
    """Only the NEWEST ranking week, ids already final (WTA pre-offset)."""
    res = {"ATP": [], "WTA": [], "date": None}
    for tour, paths in (('ATP', [f"{GH}/atp_rankings_current.csv", f"{HF}/atp/atp_rankings_current.csv"]),
                        ('WTA', [f"{HF}/wta/wta_rankings_current.csv"])):
        raw = None
        for u in paths:
            raw = fetch_bytes(u)
            if raw:
                break
        if not raw:
            continue
        rows = csv_rows(raw.decode('utf-8', 'ignore'))
        dates = [r.get('ranking_date', '') for r in rows if r.get('ranking_date')]
        latest = max(dates) if dates else None
        if not latest:
            continue
        if res["date"] is None or latest > res["date"]:
            res["date"] = latest
        off = WTA_OFFSET if tour == 'WTA' else 0
        week = [[int(r['player']) + off, int(r['rank'])]
                for r in rows
                if r.get('ranking_date') == latest and r.get('player', '').strip() and r.get('rank', '').strip().isdigit()]
        print(f"rankings {tour}: latest={latest}, {len(week)} players")
        res[tour] = week
    return res


def build_today(key: str | None) -> list[dict]:
    """Next-48h fixtures via TheOddsAPI. If no key -> empty list (still valid feed)."""
    if not key:
        print("today: no ODDS_API_KEY -> empty events (feed still works)")
        return []
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            state = {}
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    sports = fetch_bytes(f"{ODDS_BASE}/sports?apiKey={key}")
    if not sports:
        return []
    try:
        arr = json.loads(sports.decode('utf-8', 'ignore'))
    except Exception:
        return []
    keys = [(o.get('key', ''), o.get('title', '')) for o in arr
            if isinstance(o, dict) and o.get('active') and str(o.get('key', '')).startswith('tennis_')]
    if not keys:
        return []

    # credit discipline: fetch each tournament at most once per day, max N per run
    todo = [k for k in keys if state.get(k[0], {}).get('last_fetch', '') != today][:MAX_ODDS_CALLS_PER_RUN]
    events = []
    for k, title in todo:
        raw = fetch_bytes(f"{ODDS_BASE}/sports/{k}/odds?apiKey={key}&regions=eu&markets=h2h")
        state.setdefault(k, {})['last_fetch'] = today
        if not raw:
            continue
        try:
            arr2 = json.loads(raw.decode('utf-8', 'ignore'))
        except Exception:
            continue
        tour = 'ATP' if k.startswith('tennis_atp') else ('WTA' if k.startswith('tennis_wta') else 'ATP?')
        for e in arr2:
            comm = e.get('commence_time', '')
            if len(comm) < 10:
                continue
            # keep next 48h: today & tomorrow
            if comm[:10] > datetime.now(timezone.utc).strftime('%Y-%m-%d') + '+2':
                continue
            best = {}
            for b in e.get('bookmakers', []):
                for m in b.get('markets', []):
                    if m.get('key') != 'h2h':
                        continue
                    for o in m.get('outcomes', []):
                        name, price = o.get('name', ''), o.get('price')
                        if name and isinstance(price, (int, float)) and name not in best:
                            best[name] = price
            events.append({
                "id": "odds_" + str(e.get('id', '')),
                "tour": tour,
                "t": title,
                "date": comm[:10],
                "n1": e.get('home_team', ''),
                "n2": e.get('away_team', ''),
                "price1": best.get(e.get('home_team', '')),
                "price2": best.get(e.get('away_team', '')),
            })
        print(f"today {k}: {len(arr2)} events")
    STATE_FILE.write_text(json.dumps(state))
    return events


def main():
    print("== tennis-feed builder ==")
    matches = build_matches()
    print("matches total:", len(matches))
    rankings = build_rankings()
    events = build_today(os.environ.get("ODDS_API_KEY"))
    print("today events:", len(events))
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')

    (DATA / "matches.json").write_text(json.dumps(
        {"generated": now, "source": "Jeff Sackmann archive - CC BY-NC-SA 4.0", "matches": matches}, separators=(',', ':')))
    (DATA / "rankings.json").write_text(json.dumps(rankings, separators=(',', ':')))
    (DATA / "today.json").write_text(json.dumps(
        {"generated": now, "events": events}, separators=(',', ':')))
    (DATA / "feed_meta.json").write_text(json.dumps(
        {"generated": now,
         "attribution": "Match & ranking data: Jeff Sackmann (tennis_atp / tennis_wta), CC BY-NC-SA 4.0",
         "counters": {"matches": len(matches), "ranked": len(rankings['ATP']) + len(rankings['WTA']), "today": len(events)}},
        indent=1))
    print("wrote", [p.name for p in DATA.glob('*.json')])


if __name__ == '__main__':
    main()
