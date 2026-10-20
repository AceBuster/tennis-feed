#!/usr/bin/env python3
"""
tennis-feed v3: OUR OWN data service - now with REAL-TIME results!
NO third-party API providers, NO API keys needed!

Sources (all free, keyless):
  * Historic backbone to 2026-05-25: Jeff Sackmann archive (CC BY-NC-SA 4.0;
    credit him - we do, in every output file)
  * Fresh 2026 data: TennisMyLife open database (stats.tennismylife.org, MIT)
      - main tour + challenger season files (updated daily)
      - *_ongoing_tourneys.csv - RESULTS of tournaments in progress, updated
        in REAL TIME (the freshness gold: sees today's matches while the
        season file can lag a day or two!)

Outputs into ./data :
  matches.json  - NEW matches only (after 2026-05-25, deduped by key AND by
                  date+names so no real match is counted twice)
  rankings.json - ONLY the newest archive ranking week (ids final: WTA +1M)
  today.json    - placeholder (fixtures still deferred)
  feed_meta.json - stamp + counters + attribution
"""
import csv
import io
import json
import pathlib
import urllib.request
from datetime import datetime, timezone

HERE = pathlib.Path(__file__).resolve().parent
DATA = HERE / "data"
DATA.mkdir(exist_ok=True)

# ---- sources ----------------------------------------------------------------
HF = "https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive/resolve/main"
GH = "https://raw.githubusercontent.com/Kadantte/tennis_atp/master"
SACK_ATP_2026 = [f"{GH}/atp_matches_2026.csv", f"{HF}/atp/atp_matches_2026.csv"]
SACK_ATP_CH_2026 = [f"{GH}/atp_matches_qual_chall_2026.csv", f"{HF}/atp/atp_matches_qual_chall_2026.csv"]
SACK_WTA_2026 = [f"{HF}/wta/wta_matches_2026.csv"]
SACK_WTA_ITF_2026 = [f"{HF}/wta/wta_matches_qual_itf_2026.csv"]
TLM_MAIN = [
    ('ATP', "https://stats.tennismylife.org/data/2026.csv"),
    ('WTA', "https://stats.tennismylife.org/data/2026_wta.csv"),
]
TLM_CH = [
    ('ATP', "https://stats.tennismylife.org/data/2026_challenger.csv"),
]
TLM_ONGOING = [
    ('ATP', "https://stats.tennismylife.org/data/ongoing_tourneys.csv"),
    ('WTA', "https://stats.tennismylife.org/data/wta_ongoing_tourneys.csv"),
    ('ATP', "https://stats.tennismylife.org/data/challenger_ongoing_tourneys.csv"),
]
RANK_FILES = [
    ('ATP', [f"{GH}/atp_rankings_current.csv", f"{HF}/atp/atp_rankings_current.csv"]),
    ('WTA', [f"{HF}/wta/wta_rankings_current.csv"]),
]

CUTOFF = "20260525"          # the archive freezes at this date
WTA_OFFSET = 1_000_000       # archive-wide WTA id rule (kept for ranks)
TLM_OFFSET = 50_000_000      # 4-char TLM player codes -> base36 shifted up

UA = {"User-Agent": "tennis-feed/1.0 (personal project)"}


def fetch_bytes(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=90) as r:
            return r.read()
    except Exception as e:
        print("  fetch failed:", url, "->", e)
        return None


def fetch_first(urls):
    for u in urls:
        raw = fetch_bytes(u)
        if raw:
            print(f"  {len(raw)//1024} KB from {u.rsplit('/', 1)[-1]}")
            return raw
    return None


def csv_rows(raw: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(raw.decode('utf-8', 'ignore'))))


def tlm_player_id(code: str, tour: str) -> int | None:
    """TLM ids: WTA & some ATP rows are numeric (join archive namespace),
    newest players have 4-char codes -> base36 -> distant numeric bucket."""
    code = code.strip()
    if not code:
        return None
    if code.isdigit():
        return int(code) + (WTA_OFFSET if tour == 'WTA' else 0)
    try:
        return int(code, 36) + TLM_OFFSET
    except ValueError:
        return None


def triples_from(raw: bytes) -> set:
    got = set()
    for r in csv_rows(raw):
        d8 = r.get('tourney_date', '') or ''
        wn = (r.get('winner_name') or '').strip().lower()
        ln = (r.get('loser_name') or '').strip().lower()
        if len(d8) == 8 and wn and ln:
            got.add((d8, wn, ln))
    return got


def known_triples() -> set:
    """(date, winner, loser) from the FROZEN archive files only - so a real match
    already in the archive namespace is never published under a TLM key.
    Cross-file overlaps between TLM files are handled during collection."""
    seen = set()
    for urls in (SACK_ATP_2026, SACK_ATP_CH_2026, SACK_WTA_2026, SACK_WTA_ITF_2026):
        raw = fetch_first(urls) if isinstance(urls, list) else fetch_bytes(urls)
        if raw:
            seen |= triples_from(raw)
    print("dedupe set size:", len(seen))
    return seen


def collect(sources, seen_triples, seen_ids, out, tag):
    """Append NEW match rows from `sources` ([(tour, url), ...])."""
    total_new = 0
    for tour, url in sources:
        raw = fetch_bytes(url)
        if not raw:
            print(f"!! {tag}/{tour} file unreachable: {url.rsplit('/', 1)[-1]}")
            continue
        rows = csv_rows(raw)
        new_rows = 0
        for r in rows:
            d8 = (r.get('tourney_date') or '').strip()
            wid = (r.get('winner_id') or '').strip()
            lid = (r.get('loser_id') or '').strip()
            wn = (r.get('winner_name') or '').strip()
            ln = (r.get('loser_name') or '').strip()
            if len(d8) != 8 or not wid or not lid or not wn or not ln:
                continue
            if d8 <= CUTOFF:
                continue                        # the archive covers everything until the freeze
            p1 = tlm_player_id(wid, tour)
            p2 = tlm_player_id(lid, tour)
            if p1 is None or p2 is None:
                continue
            tid = (r.get('tourney_id') or '').strip()
            num = (r.get('match_num') or '').strip()
            mid = f"tlm_{tour}_{tid}_{num}_{d8[:4]}"
            triple = (d8, wn.lower(), ln.lower())
            if mid in seen_ids or triple in seen_triples:
                continue                        # same key, or same real match seen elsewhere
            seen_ids.add(mid); seen_triples.add(triple)
            out.append({
                "id": mid,
                "tour": tour,
                "date": f"{d8[:4]}-{d8[4:6]}-{d8[6:8]}",
                "p1": p1, "p2": p2, "w": p1,
                "s": (r.get('surface') or '').strip() or "Hard",
                "r": (r.get('round') or '').strip(),
                "n1": wn, "n2": ln,
                "c1": (r.get('winner_ioc') or '').strip(),
                "c2": (r.get('loser_ioc') or '').strip(),
            })
            new_rows += 1
        print(f"{tag} {tour}: {len(rows)} rows -> {new_rows} NEW")
        total_new += new_rows
    return total_new


def build_rankings() -> dict:
    res = {"ATP": [], "WTA": [], "date": None}
    for tour, urls in RANK_FILES:
        raw = fetch_first(urls)
        if not raw:
            continue
        rows = csv_rows(raw)
        dates = [r.get('ranking_date', '') for r in rows if r.get('ranking_date', '').isdigit()]
        latest = max(dates) if dates else None
        if not latest:
            continue
        if res["date"] is None or latest > res["date"]:
            res["date"] = latest
        off = WTA_OFFSET if tour == 'WTA' else 0
        week = [[int(r['player']) + off, int(r['rank'])]
                for r in rows
                if r.get('ranking_date') == latest and (r.get('player') or '').strip() and (r.get('rank') or '').strip().isdigit()]
        print(f"rankings {tour}: latest={latest}, {len(week)} players")
        res[tour] = week
    return res


def main():
    print("== tennis-feed builder v3 (real-time results edition) ==")
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')

    seen_triples = known_triples()
    matches, seen_ids = [], set()
    n_main = collect(TLM_MAIN, seen_triples, seen_ids, matches, "main2026")
    n_ch = collect(TLM_CH, seen_triples, seen_ids, matches, "challenger2026")
    n_on = collect(TLM_ONGOING, seen_triples, seen_ids, matches, "ONGOING")
    print("NEW matches total:", len(matches), f"(main {n_main} / challer {n_ch} / ongoing {n_on})")

    rankings = build_rankings()

    (DATA / "matches.json").write_text(json.dumps(
        {"generated": now, "matches": matches}, separators=(',', ':')))
    (DATA / "rankings.json").write_text(json.dumps(rankings, separators=(',', ':')))
    (DATA / "today.json").write_text(json.dumps(
        {"generated": now, "events": [],
         "note": "fixtures feed is deferred; ONGOING results already flow through matches.json"},
        separators=(',', ':')))
    (DATA / "feed_meta.json").write_text(json.dumps({
        "generated": now,
        "build": "v3",
        "attribution": [
            "historic data: Jeff Sackmann archive, CC BY-NC-SA 4.0",
            "fresh data: TennisMyLife (stats.tennismylife.org), MIT license",
        ],
        "counters": {
            "new_matches": len(matches),
            "ranked": len(rankings['ATP']) + len(rankings['WTA']),
        },
    }, indent=1))
    print("wrote", [p.name for p in sorted(DATA.glob('*.json'))])


if __name__ == '__main__':
    main()
