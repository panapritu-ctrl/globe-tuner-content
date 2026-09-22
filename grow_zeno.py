#!/usr/bin/env python3
"""
Discovers new stations for the standalone Editor's Picks collection
(zeno/zeno_stations.json) and appends verified-live ones.

This pool is deliberately kept separate from the general radio/ catalog —
the stations here started out merged into radio/radio_*.json, but were
migrated out because that meant a dedicated home-screen section would show
content already reachable via Near You/Featured/Explore. Keep it that way:
every candidate is checked against radio/*.json and skipped if it's already
there.

History: this used to filter Radio Browser results down to zeno.fm-hosted
streams only. In Sept 2026 Zeno.fm moved to signed stream URLs carrying a
JWT that expires ~60s after issue, so bare zeno.fm URLs began returning 401
and the collection collapsed from ~915 stations to ~190 as the daily verify
correctly pruned them. A stored URL from a host like that can never work —
it's dead long before a user presses play — so zeno.fm (and its white-label
sibling surfernetwork.com, which issues the same 60s tokens) are now
permanently excluded, and selection is by quality signal instead of host.

Selection favours stations Radio Browser's own users actually listen to
(clickcount/votes) and spreads picks across countries, so the collection
reads as curated rather than as whatever happened to sort first.

Usage:
    python3 grow_zeno.py                  # discover + append (cap 100 new)
    python3 grow_zeno.py --dry-run
    python3 grow_zeno.py --max-new 50
"""
import argparse
import asyncio
import hashlib
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

from verify_stations import check_one, HEADERS

REPO_ROOT = Path(__file__).resolve().parent
INDEX_PATH = REPO_ROOT / "zeno_index.json"
FILE_PATH = REPO_ROOT / "zeno" / "zeno_stations.json"
RADIO_INDEX_PATH = REPO_ROOT / "radio_index.json"
RB_BASE = "https://all.api.radio-browser.info/json/stations/search"

# Hosts that hand out stream URLs signed with a short-lived JWT. They test
# fine the instant you fetch them and are dead ~60s later, so anything from
# them is worthless in a static catalog no matter how healthy it looks.
BANNED_HOSTS = ("zeno.fm", "surfernetwork.com")
# Any URL carrying an embedded, expiring signature: Zeno/surfernetwork
# JWTs (zt/zs), radiojar tokens, and AWS CloudFront signed URLs
# (Policy/Signature/Key-Pair-Id) all go stale and 401 later, so they can't
# be stored. Caught KBS and Radio Eins this way during a dry run.
_TOKEN_PARAMS = re.compile(
    r"[?&](zt|zs|rj-tok|token|adtonosListenerId|aw_0_req_lsid"
    r"|Policy|Signature|Key-Pair-Id)=", re.I)

GENERIC_GENRES = {"", "radio", "music", "unknown", "various"}


def slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return s[:40] or "station"


def stable_id(title: str, country_code: str, stream_url: str) -> str:
    h = hashlib.sha1(stream_url.encode("utf-8")).hexdigest()[:8]
    cc = (country_code or "xx").lower()
    return f"{slugify(title)}_{cc}_{h}"


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def main_catalog_urls() -> set:
    """Every stream URL already in radio/*.json. Editor's Picks must not
    repeat these — that's the whole reason the collection exists."""
    urls = set()
    try:
        index = load_json(RADIO_INDEX_PATH)
    except FileNotFoundError:
        return urls
    for rel in index.get("files", []):
        p = REPO_ROOT / rel
        if not p.exists():
            continue
        for s in load_json(p).get("stations", []):
            u = (s.get("streamUrl") or "").strip()
            if u:
                urls.add(u)
    return urls


def usable_url(url: str) -> bool:
    if not url.startswith("http"):
        return False
    low = url.lower()
    if any(h in low for h in BANNED_HOSTS):
        return False
    if _TOKEN_PARAMS.search(url):
        return False
    return True


def quality(s: dict) -> float:
    """Rank by what Radio Browser's own users do: plays and votes, nudged
    by bitrate and whether the station bothered to register a homepage."""
    clicks = float(s.get("clickcount") or 0)
    votes = float(s.get("votes") or 0)
    trend = float(s.get("clicktrend") or 0)
    bitrate = float(s.get("bitrate") or 0)
    score = clicks + (votes * 5) + (trend * 20) + min(bitrate, 320) / 32
    if (s.get("homepage") or "").strip():
        score += 10
    if (s.get("favicon") or "").strip():
        score += 5
    return score


async def fetch_candidates(session, limit, timeout):
    params = {
        "hidebroken": "true",
        "order": "clickcount",
        "reverse": "true",
        "limit": str(limit),
    }
    try:
        async with session.get(
            RB_BASE, params=params, timeout=aiohttp.ClientTimeout(total=timeout), headers=HEADERS
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json(content_type=None)
    except Exception:
        return []

    out = []
    for s in data:
        stream = (s.get("url_resolved") or s.get("url") or "").strip()
        title = (s.get("name") or "").strip()
        if not usable_url(stream) or not title or title.lower() == "unknown station":
            continue
        if len(title) > 60 or title.count(",") > 3:
            continue
        tags = s.get("tags") or ""
        genre = tags.split(",")[0].strip().title() if tags else "Radio"
        if genre.lower() in GENERIC_GENRES:
            genre = "Radio"
        state = (s.get("state") or "").strip()
        country = (s.get("country") or "").strip()
        out.append(
            {
                "title": title,
                "streamUrl": stream,
                "countryCode": (s.get("countrycode") or "").strip().upper(),
                "genre": genre,
                "subtitle": (s.get("codec") or "").strip().upper() or "Radio",
                "location": ", ".join(x for x in (state, country) if x) or "Unknown",
                "score": quality(s),
            }
        )
    return out


def pick_balanced(candidates, cap):
    """Round-robin across countries so one station-rich country can't take
    every slot, taking each country's best-scoring station first."""
    by_cc = defaultdict(list)
    for c in candidates:
        by_cc[c["countryCode"] or "??"].append(c)
    for cc in by_cc:
        by_cc[cc].sort(key=lambda x: x["score"], reverse=True)

    order = sorted(by_cc, key=lambda cc: -by_cc[cc][0]["score"])
    picked, ptr = [], {cc: 0 for cc in order}
    while len(picked) < cap:
        moved = False
        for cc in order:
            i = ptr[cc]
            if i < len(by_cc[cc]):
                picked.append(by_cc[cc][i])
                ptr[cc] = i + 1
                moved = True
            if len(picked) >= cap:
                break
        if not moved:
            break
    return picked


async def main_async(args) -> int:
    index = load_json(INDEX_PATH)
    data = load_json(FILE_PATH)

    existing_urls = {(s.get("streamUrl") or "").strip() for s in data["stations"]}
    existing_ids = {s.get("id") for s in data["stations"]}
    catalog_urls = main_catalog_urls()

    print(f"Existing Editor's Picks: {len(data['stations'])} stations")
    print(f"Main radio catalog: {len(catalog_urls)} stream URLs (excluded from picks)")
    print(f"Fetching {args.fetch} candidates from Radio Browser...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        candidates = await fetch_candidates(session, args.fetch, args.timeout)

    print(f"Got {len(candidates)} usable candidates from the batch.")

    new_candidates, seen = [], set()
    for c in candidates:
        u = c["streamUrl"]
        if u in existing_urls or u in catalog_urls or u in seen:
            continue
        seen.add(u)
        new_candidates.append(c)

    print(f"{len(new_candidates)} are new to both the picks and the main catalog.")
    if not new_candidates:
        return 0

    # Verify more than needed: a good share won't actually stream, and it's
    # better to over-test and fill the quota than to add something unheard.
    shortlist = pick_balanced(new_candidates, args.max_new * 3)
    print(f"Verifying {len(shortlist)} candidates are actually live...")

    results_alive = [None] * len(shortlist)
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def verify(i, item):
            async with sem:
                results_alive[i] = await check_one(session, item["streamUrl"], args.timeout)

        await asyncio.gather(*(verify(i, it) for i, it in enumerate(shortlist)))

    verified = [it for it, ok in zip(shortlist, results_alive) if ok]
    print(f"{len(verified)}/{len(shortlist)} verified live.")
    if not verified:
        return 0

    verified = pick_balanced(verified, args.max_new)

    if args.dry_run:
        for it in verified[:20]:
            print(f"  + {it['title']} ({it['countryCode']}) {it['streamUrl']}")
        if len(verified) > 20:
            print(f"  ... and {len(verified) - 20} more")
        return 0

    added = []
    for it in verified:
        sid = stable_id(it["title"], it["countryCode"], it["streamUrl"])
        if sid in existing_ids:
            continue
        existing_ids.add(sid)
        station = {
            "id": sid,
            "title": it["title"],
            "subtitle": it["subtitle"],
            "location": it["location"],
            "genre": it["genre"],
            "streamUrl": it["streamUrl"],
        }
        if it["countryCode"]:
            station["countryCode"] = it["countryCode"]
        data["stations"].append(station)
        added.append(station)

    if not added:
        print("Nothing new to add.")
        return 0

    data["version"] = data.get("version", 0) + 1
    data["updated"] = now_iso()
    dump_json(FILE_PATH, data)

    for region in index["regions"]:
        if region["file"] == "zeno/zeno_stations.json":
            region["count"] = len(data["stations"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    countries = len({s.get("countryCode") for s in data["stations"]})
    print(f"\nAdded {len(added)} new stations. Collection now {len(data['stations'])} "
          f"across {countries} countries.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=100, help="Cap on new stations added per run")
    parser.add_argument("--fetch", type=int, default=8000, help="Raw candidates to pull from Radio Browser before filtering")
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--timeout", type=float, default=8.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
