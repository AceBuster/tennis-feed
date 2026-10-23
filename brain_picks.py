#!/usr/bin/env python3
"""
tennis-feed BRAIN v1 - the robot that turns a pasted draw into auto_picks.json.

THE ONE-PASTE-A-DAY PIPELINE:
  1. you paste the day's schedule text into  data/draws/YYYY-MM-DD.txt
  2. this job runs nightly (~00:25 & ~09:25 UTC) or when you press
     "Run workflow" in the Actions tab
  3. data/auto_picks.json is regenerated; the phone grabs it on its next sync

NO API KEYS. Legal sources only:
  * Jeff Sackmann archive yearly files 2015..now  (CC BY-NC-SA 4.0 - the
    attribution travels inside every output file)
  * TennisMyLife open database 2026 (stats.tennismylife.org, MIT):
    season + challenger + ongoing result files
  * current ATP/WTA ranking files (same mirrors as feeder v3)

BRAINS = exactly the shipped phone v7 models, embedded below bit-for-bit:
  logistic_v7_atp.json (65.3% honest, temp 1.30)
  logistic_v7_wta.json (66.2% honest, temp 1.05)

IDENTITY: players are keyed by their normalized NAME KEY (= lowercase letters
of the full name, diacritics stripped, hyphens/dots as spaces, letter-tokens
sorted). Name variants like "Jan-Lennard Struff" / "Jan Lennard Struff" merge
into one identity automatically and twins never double-count - no id maps
needed anywhere. Fixture names ("Tsitsipas S.") resolve by surname + first
initial, exactly like the lab predictor does.

PUBLISH RULE (same as the lab predictor): only >= 65% picks (the phone's
lightning zone) where BOTH sides have real recorded matches. All other rows
are printed in the run log, not published.
"""
import argparse
import csv
import difflib
import io
import json
import math
import pathlib
import re
import sqlite3
import unicodedata
import urllib.request
from collections import defaultdict, Counter
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# shipped v7 brains (embedded verbatim from ml_training/models/*.json)
# ----------------------------------------------------------------------------
ATP_MODEL = {
    "type": "logistic_v7_atp", "test_accuracy": 0.653, "temp": 1.3,
    "w": [0.5028108576358137, -0.07783793803355175, 0.2825721456701167,
          0.025093354215866552, 0.5145914419724881],
    "b": -0.0017232671079069016,
    "means": [-0.0037023059224199567, -0.001923833984781987, -0.004689174705251876,
              0.00039762271259412247, -0.0031298835440166407],
    "stds": [0.3866859781435569, 0.28304708398549716, 1.3665315012476316,
             0.2863388948517964, 1.4615490230337778],
}
WTA_MODEL = {
    "type": "logistic_v7_wta", "test_accuracy": 0.6616, "temp": 1.05,
    "w": [0.30745685614888474, 0.0579123067156283, 0.007973100920115788,
          0.04217694662819348, 0.480247530526705],
    "b": -0.01479370960543118,
    "means": [-0.003160480720562869, -0.0015033291439366342, 0.00012086539623705733,
              -0.0027742054259777553, -0.0021430887507386557],
    "stds": [0.3086573927372962, 0.22219419542762325, 0.8836006771260048,
             0.2832313367706706, 1.4370099977455493],
}
INI, K, DECAY, WINDOWS = 1500.0, 32, 0.85, 10
ZONE = 65                      # the lightning zone
FOUNDATION = 2015              # archive year depth

ATTRIBUTION = ("tennis-feed robot brain - same v7 brains as the app; "
               "historic data: Jeff Sackmann archive, CC BY-NC-SA 4.0; "
               "fresh data: TennisMyLife (stats.tennismylife.org), MIT license")

# ----------------------------------------------------------------------------
# sources (identical mirrors to feeder v3)
# ----------------------------------------------------------------------------
GH = "https://raw.githubusercontent.com/Kadantte/tennis_atp/master"
HF = "https://huggingface.co/datasets/Aneeshers/tennis-sackmann-archive/resolve/main"
UA = {"User-Agent": "tennis-feed-brain/1.0 (personal project, keyless)"}

TLM = [
    ("ATP", "https://stats.tennismylife.org/data/2026.csv"),
    ("WTA", "https://stats.tennismylife.org/data/2026_wta.csv"),
    ("ATP", "https://stats.tennismylife.org/data/2026_challenger.csv"),
    ("ATP", "https://stats.tennismylife.org/data/ongoing_tourneys.csv"),
    ("WTA", "https://stats.tennismylife.org/data/wta_ongoing_tourneys.csv"),
    ("ATP", "https://stats.tennismylife.org/data/challenger_ongoing_tourneys.csv"),
]
RANKS = [
    ("ATP", [f"{GH}/atp_rankings_current.csv", f"{HF}/atp/atp_rankings_current.csv"]),
    ("WTA", [f"{HF}/wta/wta_rankings_current.csv"]),
]


def fetch_bytes(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as e:
        print(f"  fetch failed: {url.rsplit('/', 1)[-1]} -> {e}", flush=True)
        return None


def fetch_first(urls):
    for u in urls:
        raw = fetch_bytes(u)
        if raw:
            print(f"  {len(raw) // 1024} KB  {u.rsplit('/', 1)[-1]}", flush=True)
            return raw
    return None


def csv_rows(raw):
    return list(csv.DictReader(io.StringIO(raw.decode("utf-8", "ignore"))))


# ----------------------------------------------------------------------------
# name keys (the twin-proof identity)
# ----------------------------------------------------------------------------
def norm(s):
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower().replace(".", " ").replace("-", " ").strip()


def namekey(full):
    toks = [t for t in norm(full).split() if len(t) > 1]
    return " ".join(sorted(toks)) if toks else norm(full)


# ----------------------------------------------------------------------------
# inbox parser (inline copy of draw_inbox.py so the robot needs ONE file)
# ----------------------------------------------------------------------------
COURT_RE = re.compile(r"^(?P<city>[A-Z][^(),]{0,40}?)\s*\((?P<country>[^)]+)\)\s*,\s*"
                      r"(?P<surf>hard|clay|grass|carpet)")
SECTION_RE = re.compile(r"^(?P<first>[ATPWCI][A-Za-z ]*?)\s*-?\s*(?P<kind>SINGLES|DOUBLES)", re.I)
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
PREFIX_TIME_RE = re.compile(r"^(\d{1,2}:\d{2})\s*[:\-]?\s+(\S.*)$")
NUMPREFIX_RE = re.compile(r"^\s*(?:\[\d+\]|\(\d+\)|\d+[.)]|[Rr]\d+|Q\d|SE)\s+(?=[A-Z])")
SPLIT_RE = re.compile(r"\s+(?:-{1,2}|\u2013|\u2014|vs\.?|v\.?|/)\s+", re.I)
CAPTOK = re.compile(r"[A-Za-z][A-Za-z'\-]{1,}")
INIT_START = re.compile(r"[A-Za-z]+\.[\sA-Za-z]*$")
QUOTE = {"FRO", "Draw"}
RESULT_MARKERS = {"Finished", "Cancelled", "Walkover", "Retired"}
TELNUM = re.compile(r"^[\d\-\s:apm+\.]+$")
SURF = {"hard": "Hard", "clay": "Clay", "grass": "Grass", "carpet": "Hard"}

# words that mark a line as LISTING NOISE, not a player
BLOCK = {
    "draw", "round", "rounds", "qualifying", "qual", "qualification",
    "doubles", "singles", "court", "centre", "center", "order", "play",
    "schedule", "result", "results", "finished", "cancelled", "walkover",
    "retired", "seed", "seeds", "prize", "money", "points", "final",
    "finale", "semifinal", "semifinals", "semis", "quarterfinal",
    "quarterfinals", "quarters", "match", "matches", "game", "games",
    "score", "scores", "live", "order", "pdf", "draws", "bracket", "table",
    "follow", "streaming", "video", "make", "money", "betting", "odds",
    "stream", "guides", "insight", "tennis", "photo", "photos", "history",
    "ranking", "rankings", "news", "hopman", "season", "winners", "comeback",
    "legends", "tour", "tourney", "main", "sun", "mon", "tue",
    "wed", "thu", "fri", "sat", "today", "tomorrow", "yesterday", "update",
    "feeds", "stats", "watch", "listen", "app", "site", "fan",
}


def _shape_initial(t):
    """Old classic shape: ends with an initial dot cluster - 'Bellucci M.'."""
    if not re.search(r"[A-Za-z]", t):
        return False
    return bool(INIT_START.search(t))


def _shape_fullname(t):
    """Bracket shape: >= 2 capitalized alpha words, no digits, no noise words."""
    if re.search(r"\d", t):
        return False
    toks = CAPTOK.findall(t)
    if len(toks) < 2:
        return False
    caps = [w for w in toks if w[0].isupper()]
    if len(caps) < 2:
        return False
    if any(w.lower().strip("'") in BLOCK for w in toks):
        return False
    return True


def _is_name(t):
    t = t.strip()
    if len(t) < 3:
        return False
    if TELNUM.match(t):
        return False
    if t in QUOTE or t in RESULT_MARKERS:
        return False
    if t.endswith(":") or set(t) == {"_"}:
        return False
    if COURT_RE.match(t):
        return False
    return bool(_shape_initial(t) or _shape_fullname(t))


def parse_inbox(text):
    """Scan -> typed events (each carries court context) -> pair walk with a
    1-step look-ahead so 'A / B ... Finished' rows are dropped."""
    ctx, ev = {"court": None, "tour": "ATP", "surf": "Hard"}, []

    def push_names(chunk):
        chunk = NUMPREFIX_RE.sub("", chunk).strip()
        if _is_name(chunk):
            ev.append(("name", chunk, dict(ctx)))

    for s in text.splitlines():
        t = s.strip()
        if not t:
            continue
        m = COURT_RE.match(t)
        if m:
            ctx = {"court": m["city"].strip(), "tour": ctx["tour"],
                   "surf": SURF[m["surf"].lower()]}
            continue
        m = SECTION_RE.match(t)
        if m:
            first = m["first"].upper()
            ctx = {"court": ctx["court"], "surf": ctx["surf"],
                   "tour": "WTA" if ("WTA" in first or "WOMEN" in first) else "ATP"}
            ev.append(("kind", m["kind"].upper()))
            continue
        if TIME_RE.match(t):
            ev.append(("time", t))
            continue
        m = PREFIX_TIME_RE.match(t)          # "09:30 rest..." -> time + rest
        if m:
            ev.append(("time", m.group(1)))
            t = m.group(2)
        if TELNUM.match(t) or t in QUOTE or set(t) == {"_"}:
            continue
        if t in RESULT_MARKERS:
            ev.append(("marker", t))
            continue
        parts = SPLIT_RE.split(t)
        clean = [p.strip() for p in parts if p.strip()]
        if len(clean) == 2 and _is_name(clean[0]) and _is_name(clean[1]):
            push_names(clean[0])             # two names on ONE line -> two events
            push_names(clean[1])
            continue
        push_names(t)

    out, last_kind, got, pending_time = [], "SINGLES", None, None
    for i, e in enumerate(ev):
        if e[0] == "kind":
            last_kind, got, pending_time = e[1], None, None
            continue
        if last_kind != "SINGLES" or e[0] != "name":
            continue
        _, name, c = e
        if got is None:
            got = name
            for j in range(i - 1, -1, -1):
                if ev[j][0] == "time":
                    pending_time = ev[j][1]
                    break
                if ev[j][0] == "kind":
                    break
            continue
        nxt = ev[i + 1] if i + 1 < len(ev) else None
        if nxt and nxt[0] == "marker":
            got, pending_time = None, None
            continue
        out.append({"court": c["court"], "tour": c["tour"], "surface": c["surf"],
                    "time": pending_time or "", "p1": got, "p2": name})
        got, pending_time = None, None
    return out


# ----------------------------------------------------------------------------
# match corpus (Sackmann main tours 2015..now + TLM 2026 fresh/ongoing)
# ----------------------------------------------------------------------------
def surf_norm(s):
    return s if s in ("Hard", "Clay", "Grass") else "Hard"


class Corpus:
    """One flat, deduped, chronological match list; name-key identity.
    Rows are stored as (winner_key, loser_key) - the walk relies on that."""

    def __init__(self):
        self.rows = []          # (d8, seq, wkey, lkey, surf)
        self.votes = defaultdict(Counter)   # key -> full-name spelling votes
        self.triples = set()
        self.id2key = {}        # (tour, sackmann_id) -> namekey (rank lookup)
        self.match_rank = {}    # key -> (d8, rank)  freshest match-row evidence
        self.seq = 0

    def add(self, d8, wname, lname, surf, tour="", wid="", lid="", wr="", lr=""):
        if len(d8) != 8 or not d8.isdigit() or not wname or not lname:
            return False
        triple = (d8, wname.lower(), lname.lower())
        if triple in self.triples:
            return False
        wk, lk = namekey(wname), namekey(lname)
        if wk == lk:
            return False
        self.triples.add(triple)
        self.votes[wk][wname] += 1
        self.votes[lk][lname] += 1
        for key, rk in ((wk, wr), (lk, lr)):          # freshest rank evidence wins
            rk = (rk or "").strip()
            if rk.isdigit() and 0 < int(rk) < 9000:
                keep = self.match_rank.get(key)
                if not keep or d8 >= keep[0]:
                    self.match_rank[key] = (d8, int(rk))
        if wid and lid and wid.isdigit() and lid.isdigit():
            self.id2key.setdefault((tour, wid), wk)
            self.id2key.setdefault((tour, lid), lk)
        self.rows.append((d8, self.seq, wk, lk, surf_norm(surf or "Hard")))
        self.seq += 1
        return True

    def load_network(self):
        print(f"== corpus: Sackmann archive {FOUNDATION}..now (main tours) ==")
        n_arch = 0
        for tour, urls_for_year in (("ATP", lambda y: [f"{GH}/atp_matches_{y}.csv",
                                                      f"{HF}/atp/atp_matches_{y}.csv"]),
                                    ("WTA", lambda y: [f"{HF}/wta/wta_matches_{y}.csv"])):
            for y in range(FOUNDATION, datetime.now(timezone.utc).year + 1):
                raw = fetch_first(urls_for_year(y))
                if not raw:
                    print(f"  !! {tour} {y}: all mirrors unreachable - year skipped")
                    continue
                added = 0
                for r in csv_rows(raw):
                    if self.add((r.get("tourney_date") or "").strip(),
                                (r.get("winner_name") or "").strip(),
                                (r.get("loser_name") or "").strip(),
                                r.get("surface") or "Hard", tour,
                                (r.get("winner_id") or "").strip(),
                                (r.get("loser_id") or "").strip(),
                                r.get("winner_rank") or "",
                                r.get("loser_rank") or ""):
                        added += 1
                n_arch += added
                print(f"  {tour} {y}: +{added}", flush=True)
        print(f"== corpus: TennisMyLife 2026 fresh+ongoing ==")
        for tour, url in TLM:
            raw = fetch_bytes(url)
            if not raw:
                print(f"  !! TLM {url.rsplit('/', 1)[-1]} unreachable")
                continue
            added = 0
            for r in csv_rows(raw):
                if self.add((r.get("tourney_date") or "").strip(),
                            (r.get("winner_name") or "").strip(),
                            (r.get("loser_name") or "").strip(),
                            r.get("surface") or "Hard", tour,
                            wr=r.get("winner_rank") or "",
                            lr=r.get("loser_rank") or ""):
                    added += 1
            print(f"  TLM {url.rsplit('/', 1)[-1]}: +{added}", flush=True)
        print(f"archive rows kept: {n_arch}", flush=True)

    def load_lab_db(self, dbpath):
        print(f"== corpus: LAB DB {dbpath} (local test mode) ==")
        conn = sqlite3.connect(dbpath)
        rows = conn.execute("""
            SELECT w.full_name, pl2.full_name, COALESCE(m.surface,'Hard'), m.match_date
            FROM matches m
            JOIN players w ON w.id = m.winner_id
            JOIN players pl2 ON pl2.id = m.player2_id
            WHERE m.winner_id IS NOT NULL
            ORDER BY m.match_date, m.id""").fetchall()
        conn.close()
        n = 0
        for wname, lname, surf, d in rows:
            if self.add((d or "").replace("-", ""), wname, lname, surf or "Hard"):
                n += 1
        print(f"  lab rows: {len(rows)} -> kept {n}", flush=True)


# ----------------------------------------------------------------------------
# current rankings: archive id -> namekey -> rank
# ----------------------------------------------------------------------------
def load_rankings(id2key):
    key2rank = {}
    week = None
    for tour, urls in RANKS:
        raw = fetch_first(urls)
        if not raw:
            continue
        rows = csv_rows(raw)
        dates = [r.get("ranking_date", "") for r in rows
                 if (r.get("ranking_date") or "").isdigit()]
        latest = max(dates) if dates else None
        if not latest:
            continue
        mapped = total = 0
        for r in rows:
            if r.get("ranking_date") != latest:
                continue
            pid = (r.get("player") or "").strip()
            rk = (r.get("rank") or "").strip()
            if not pid.isdigit() or not rk.isdigit():
                continue
            total += 1
            key = id2key.get((tour, pid))
            if key:
                key2rank[key] = int(rk)
                mapped += 1
        print(f"  ranks {tour}: latest={latest}, mapped {mapped}/{total}")
        week = latest if week is None or latest > week else week
    return key2rank, week


# ----------------------------------------------------------------------------
# the v7 math - replicated line-for-line from predict_today.py
# ----------------------------------------------------------------------------
def wmean(streak):
    if not streak:
        return 0.5
    s = w = 0.0
    for i, v in enumerate(streak):
        wgt = DECAY ** i
        s += wgt * v
        w += wgt
    return s / w


def sigmoid(z):
    if z > 60:
        return 1.0
    if z < -60:
        return 0.0
    return 1.0 / (1.0 + math.exp(-z))


def overall(d):
    return sum(d.values()) / 3.0


def rank_term(r1, r2):
    if not r1 or not r2 or r1 >= 9000 or r2 >= 9000:
        return 0.0
    return math.log(r2 / r1)


def parse_fix(fx):
    toks = norm(fx).split()
    inits = []
    while toks and len(toks[-1]) <= 2:
        inits.append(toks.pop(-1))
    init = inits[-1][0] if inits else None
    surname = toks[-1] if toks else ""
    return surname, init


class Brain:
    def __init__(self, order, votes, key2rank):
        self.surf_elo = defaultdict(lambda: {"Hard": INI, "Clay": INI, "Grass": INI})
        self.h2h = defaultdict(lambda: [0, 0])
        self.form = defaultdict(list)
        self.walk(order)
        self.players = self.inventory(votes, key2rank)

    def walk(self, order):
        """rows arrive as (winner, loser) - exactly the lab walk with r=1."""
        for d8, _seq, p1, p2, surf in order:
            e1s, e2s = self.surf_elo[p1][surf], self.surf_elo[p2][surf]
            ex = 1.0 / (1.0 + 10 ** ((e2s - e1s) / 400.0))
            self.surf_elo[p1][surf] = e1s + K * (1.0 - ex)
            self.surf_elo[p2][surf] = e2s + K * (ex - 1.0)
            a, b = (p1, p2) if p1 <= p2 else (p2, p1)
            if p1 == a:
                self.h2h[(a, b)] = [self.h2h[(a, b)][0] + 1, self.h2h[(a, b)][1]]
            else:
                self.h2h[(a, b)] = [self.h2h[(a, b)][0], self.h2h[(a, b)][1] + 1]
            self.form[p1].append(1)
            self.form[p2].append(0)

    def inventory(self, votes, key2rank):
        inv = []
        for key, form in self.form.items():
            if not form:
                continue
            raw = (votes.get(key) or Counter()).most_common(1)[0][0] if votes.get(key) else key
            toks = norm(raw).split()
            inv.append({"key": key, "raw": raw,
                        "rank": key2rank.get(key, 0),
                        "surname": toks[-1] if toks else key,
                        "init": toks[0][0] if toks else "",
                        "matches": len(form)})
        return {p["key"]: p for p in inv}

    def resolve(self, fx):
        """Returns (player, exact) - exact=False for fuzzy guesses, which may
        appear in the log but must NEVER carry a published pick."""
        surname, init = parse_fix(fx)
        pool = [p for p in self.players.values()
                if p["surname"] == surname and (init is None or p["init"] == init)]
        if pool:
            return sorted(pool, key=lambda p: (-p["matches"], p["rank"] or 9999))[0], True
        if init:
            surns = sorted({p["surname"] for p in self.players.values() if p["init"] == init})
            for s in difflib.get_close_matches(surname, surns, n=3, cutoff=0.7):
                pool = [p for p in self.players.values()
                        if p["surname"] == s and p["init"] == init]
                if pool:
                    print(f'   [*] "{fx}" fuzzy -> "{[p["raw"] for p in pool][0]}"')
                    return sorted(pool, key=lambda p: (-p["matches"], p["rank"] or 9999))[0], False
        return None, True

    def h2h_diff(self, a, b):
        key = (a, b) if a <= b else (b, a)
        h = self.h2h.get(key)
        if not h:
            return 0
        return (h[0] - h[1]) if a == key[0] else (h[1] - h[0])

    def predict(self, tour, P1, P2):
        s1, s2 = self.surf_elo[P1["key"]], self.surf_elo[P2["key"]]
        hd = self.h2h_diff(P1["key"], P2["key"])
        fa, fb = self.form[P1["key"]][:WINDOWS], self.form[P2["key"]][:WINDOWS]
        fd = wmean(fa) - wmean(fb)
        M = WTA_MODEL if tour == "WTA" else ATP_MODEL
        W, B, MEANS, STDS = M["w"], M["b"], M["means"], M["stds"]
        x = [(s1["Hard"] - s2["Hard"]) / 400.0, (overall(s1) - overall(s2)) / 400.0,
             float(hd), fd, rank_term(P1["rank"], P2["rank"])]
        T = M.get("temp") or 1.0
        z = B / T
        for j in range(5):
            z += W[j] * (x[j] - MEANS[j]) / (STDS[j] or 1.0) / T
        p = sigmoid(z)
        p1p = max(1, min(99, int(round(max(0.03, min(0.97, p)) * 100))))
        p2p = 100 - p1p
        zone = "⚡" if max(p1p, p2p) >= ZONE else "🌫"
        return s1, s2, fa, fb, p1p, p2p, zone


# ============================================================================
def main():
    ap = argparse.ArgumentParser(description="tennis-feed robot brain")
    ap.add_argument("--date", help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--draw", help="explicit draw inbox path (default data/draws/<date>.txt)")
    ap.add_argument("--out", help="output json (default data/auto_picks.json)")
    ap.add_argument("--lab-db", help="local test: walk the lab master sqlite instead of the network")
    ap.add_argument("--print-top", type=int, default=5,
                    help="print top-N most-matched players as a sanity check")
    ap.add_argument("--dry-run", action="store_true", help="print only, write no file")
    args = ap.parse_args()

    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    the_date = args.date or today_utc
    repo = pathlib.Path(__file__).resolve().parent
    data = repo / "data"
    draw_path = pathlib.Path(args.draw) if args.draw else data / "draws" / f"{the_date}.txt"

    if not draw_path.exists():
        print(f"no draw inbox for {the_date} at {draw_path}")
        print("leaving existing auto_picks.json untouched")
        return

    # the published pick date = the INBOX FILENAME date (user's intent beats clocks)
    stem = pathlib.Path(draw_path).stem
    ASKED = stem if re.fullmatch(r"\d{4}-\d{2}-\d{2}", stem) else the_date
    if ASKED != the_date:
        print(f"note: inbox {draw_path.name} processed on {the_date} -> publishing for {ASKED}")

    fixtures = parse_inbox(draw_path.read_text(encoding="utf-8", errors="ignore"))
    print(f"inbox {draw_path.name}: {len(fixtures)} singles fixtures parsed")
    if not fixtures:
        print("nothing usable in the inbox - leaving existing auto_picks.json untouched")
        return

    corpus = Corpus()
    if args.lab_db:
        corpus.load_lab_db(args.lab_db)
    else:
        corpus.load_network()
    order = sorted(corpus.rows, key=lambda r: (r[0], r[1]))
    print(f"corpus: {len(order)} matches (deduped, chronological)")

    if args.lab_db:
        print("== ranks: lab players.current_rank ==")
        conn = sqlite3.connect(args.lab_db)
        key2rank = {}
        for name, rk in conn.execute("SELECT full_name, current_rank FROM players WHERE current_rank IS NOT NULL"):
            key2rank[namekey(name)] = int(rk)
        conn.close()
    else:
        print("== ranks: current week ==")
        key2rank, week = load_rankings(corpus.id2key)
        fresh = {k: (d, rk) for k, (d, rk) in corpus.match_rank.items()
                 if week and d > week}
        for k, (d, rk) in fresh.items():
            key2rank[k] = rk
        print(f"rank freshening: {len(fresh)} players use post-mirror match-row "
              f"ranks (mirror week: {week})")

    brain = Brain(order, corpus.votes, key2rank)

    top = sorted(brain.players.values(),
                 key=lambda p: (-p["matches"], p["rank"] or 99999))
    if args.print_top:
        print("sanity - most-matched players (Hard Elo):")
        for p in top[:args.print_top]:
            e = brain.surf_elo[p["key"]]
            print(f"  {p['raw']:<22} #{p['rank'] or '-':<4} matches {p['matches']:<4}"
                  f" Hard {e['Hard']:.0f}  overall {overall(e):.0f}")

    # ---------------- predict the draw ----------------
    print()
    PUB = []
    court = None
    for fx in fixtures:
        courtname, tour, t, f1, f2 = fx["court"], fx["tour"], fx["time"], fx["p1"], fx["p2"]
        if courtname != court:
            court = courtname
            print(f"== {court} ({tour}) ==")
        P1, P1x = brain.resolve(f1)
        P2, P2x = brain.resolve(f2)
        if P1 is None or P2 is None:
            miss = f1 if P1 is None else f2
            print(f"  {t}  {f1} vs {f2}   -> NO DATA ({miss} has no archived matches): skip")
            continue
        s1, s2, fa, fb, p1p, p2p, zone = brain.predict(tour, P1, P2)
        pickname = f1 if p1p > 50 else f2
        fw = lambda l: f"{sum(l)}W-{len(l) - sum(l)}L"
        print(f"  {t}  {f1} (#{P1['rank'] or '-'}, Elo {s1['Hard']:.0f}, {fw(fa)})"
              f" vs {f2} (#{P2['rank'] or '-'}, Elo {s2['Hard']:.0f}, {fw(fb)})"
              f"  -> {pickname}: {p1p}% / {p2p}%  {zone}")
        if max(p1p, p2p) >= ZONE and fa and fb and P1x and P2x:   # both EXACT matches only
            PUB.append({"court": courtname, "time": t, "tour": tour,
                        "n1": P1["raw"], "n2": P2["raw"],
                        "surface": fx["surface"],
                        "p1pct": p1p, "p2pct": p2p, "zone": zone})

    if args.dry_run:
        print(f"\n(dry-run) {len(PUB)} confident picks; nothing written")
        return
    out = {"generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "date": ASKED, "source": ATTRIBUTION, "picks": PUB}
    out_path = pathlib.Path(args.out) if args.out else data / "auto_picks.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"\npublished {len(PUB)} confident picks -> {out_path}")


if __name__ == "__main__":
    main()
