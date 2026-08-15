#!/usr/bin/env python3
"""
Discovers new Zeno.fm-hosted stations and appends verified-live ones to the
standalone zeno/ collection.

This pool is deliberately kept separate from the general radio/ catalog —
the 134 stations here started out merged into radio/radio_*.json, but were
migrated out because that meant a dedicated home-screen section would show
content already reachable via Near You/Featured/Explore. Keep it that way:
don't add anything here that's also being added to radio/ by
grow_stations.py, and don't merge this back into the general catalog.

Radio Browser has no direct "filter by stream host" parameter, so this
pulls a large batch (sorted by clickcount, same approach as
bulk_import_radio_stations.py) and filters client-side for zeno.fm URLs,
rather than grow_stations.py's per-country querying.

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
from pathlib import Path

import aiohttp

from verify_stations import check_one, HEADERS

REPO_ROOT = Path(__file__).resolve().parent
INDEX_PATH = REPO_ROOT / "zeno_index.json"
FILE_PATH = REPO_ROOT / "zeno" / "zeno_stations.json"
RB_BASE = "https://de1.api.radio-browser.info/json/stations/search"

_ZENO_HOST = re.compile(r"zeno\.fm", re.IGNORECASE)
_ZENO_ID = re.compile(r"zeno\.fm/([a-z0-9]+)", re.IGNORECASE)
_ZENO_TOKEN_PARAMS = ("zt", "zs", "adtonosListenerId", "aw_0_req_lsid")


def _strip_zeno_tokens(url: str) -> str:
    """Zeno.fm embeds short-lived JWTs (?zt=/?zs=) that expire within ~60s
    of generation — the exact same issue PlayerProvider._sanitizeStreamUrl
    already strips at play time. Storing the bare URL (which works without
    any token) instead of a token that's dead before this even gets
    reviewed keeps the catalog data honest about what's actually needed."""
    from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse
    parsed = urlparse(url)
    kept = [(k, v) for k, v in parse_qsl(parsed.query) if k not in _ZENO_TOKEN_PARAMS]
    return urlunparse(parsed._replace(query=urlencode(kept)))


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
        if not _ZENO_HOST.search(stream):
            continue
        title = (s.get("name") or "").strip()
        if not stream.startswith("http") or not title or title.lower() == "unknown station":
            continue
        if len(title) > 60 or title.count(",") > 3:
            continue
        stream = _strip_zeno_tokens(stream)
        tags = s.get("tags") or ""
        genre = tags.split(",")[0].strip().title() if tags else "Radio"
        out.append(
            {
                "title": title,
                "streamUrl": stream,
                "countryCode": (s.get("countrycode") or "").strip().upper(),
                "genre": genre,
                "subtitle": genre,
                "location": s.get("country") or "Unknown",
                "clickCount": s.get("clickcount") or 0,
            }
        )
    return out


async def main_async(args) -> int:
    index = load_json(INDEX_PATH)
    data = load_json(FILE_PATH)
    existing_ids = set()
    for s in data["stations"]:
        m = _ZENO_ID.search(s.get("streamUrl", ""))
        if m:
            existing_ids.add(m.group(1).lower())

    print(f"Existing zeno collection: {len(data['stations'])} stations")
    print(f"Fetching {args.fetch} candidates from Radio Browser...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        candidates = await fetch_candidates(session, args.fetch, args.timeout)

    print(f"Got {len(candidates)} zeno.fm-hosted candidates from the batch.")

    new_candidates = []
    seen_this_run = set()
    for c in candidates:
        m = _ZENO_ID.search(c["streamUrl"])
        if not m:
            continue
        sid = m.group(1).lower()
        if sid in existing_ids or sid in seen_this_run:
            continue
        seen_this_run.add(sid)
        new_candidates.append(c)

    print(f"Found {len(new_candidates)} candidates not already in the zeno collection.")
    if not new_candidates:
        return 0

    new_candidates.sort(key=lambda x: x["clickCount"], reverse=True)
    new_candidates = new_candidates[: args.max_new]

    print(f"Verifying {len(new_candidates)} candidates are actually live...")
    results_alive = [None] * len(new_candidates)
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def verify(i, item):
            async with sem:
                results_alive[i] = await check_one(session, item["streamUrl"], args.timeout)

        await asyncio.gather(*(verify(i, it) for i, it in enumerate(new_candidates)))

    verified = [it for it, ok in zip(new_candidates, results_alive) if ok]
    print(f"{len(verified)}/{len(new_candidates)} verified live.")
    if not verified:
        return 0

    if args.dry_run:
        for it in verified[:20]:
            print(f"  + {it['title']} ({it['countryCode']}) {it['streamUrl']}")
        if len(verified) > 20:
            print(f"  ... and {len(verified) - 20} more")
        return 0

    added = []
    for it in verified:
        sid = stable_id(it["title"], it["countryCode"], it["streamUrl"])
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

    data["version"] = data.get("version", 0) + 1
    data["updated"] = now_iso()
    dump_json(FILE_PATH, data)

    for region in index["regions"]:
        if region["file"] == "zeno/zeno_stations.json":
            region["count"] = len(data["stations"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    print(f"\nAdded {len(added)} new zeno stations. Collection now {len(data['stations'])} total.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=100, help="Cap on new stations added per run")
    parser.add_argument("--fetch", type=int, default=8000, help="Raw candidates to pull from Radio Browser before filtering for zeno.fm")
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--timeout", type=float, default=8.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
