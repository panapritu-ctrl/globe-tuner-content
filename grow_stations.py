#!/usr/bin/env python3
"""
Discovers new radio stations and appends verified-live ones — the growth
counterpart to verify_stations.py, which only removes dead entries.

Covers every country Radio Browser has at least one station for (currently
241). For the ~219 already represented here, new stations land alongside
their existing ones. For the handful not yet represented (small/low-traffic
countries — Fiji, Bhutan, Seychelles, etc., usually single digits of
stations each), FALLBACK_CONTINENT below routes them to the right regional
file group the first time one of their stations passes verification.

Usage:
    python3 grow_stations.py                  # discover + append (cap 500 new)
    python3 grow_stations.py --dry-run
    python3 grow_stations.py --max-new 200
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
INDEX_PATH = REPO_ROOT / "radio_index.json"
LEGACY_PATH = REPO_ROOT / "radio.json"
RB_BASE = "https://all.api.radio-browser.info/json"

# Only needed for countries with zero stations in the catalog today, so
# pick_target_file has nowhere to look them up yet. Every country Radio
# Browser has any station for (241 total) is otherwise handled generically
# via country_files, built fresh from whatever's already on disk.
FALLBACK_CONTINENT = {
    "FJ": "oceania", "KI": "oceania", "CK": "oceania", "FM": "oceania",
    "NR": "oceania", "SB": "oceania", "TV": "oceania",
    "SC": "africa", "ST": "africa", "GN": "africa", "GQ": "africa",
    "SZ": "africa", "DJ": "africa", "GM": "africa", "LR": "africa",
    "MR": "africa",
    "TJ": "asia", "BT": "asia", "TL": "asia",
    "MF": "americas", "TC": "americas",
    "SJ": "europe",
}


def slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return s[:60] or "station"


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


def build_catalog_state():
    """Scan every region file once: existing stream URLs + (title, country)
    pairs for dedup, and which region file each country code currently
    lives in — so new stations for a known country land in the same
    continent grouping instead of needing a separate country->continent
    lookup table."""
    index = load_json(INDEX_PATH)
    existing_urls = set()
    existing_title_country = set()
    country_files: dict[str, set] = {}
    file_station_counts: dict[str, int] = {}
    file_data_cache: dict[str, dict] = {}

    for region in index["regions"]:
        rel = region["file"]
        data = load_json(REPO_ROOT / rel)
        file_data_cache[rel] = data
        stations = data["stations"]
        file_station_counts[rel] = len(stations)
        for s in stations:
            url = s.get("streamUrl", "")
            if url:
                existing_urls.add(url)
            title = (s.get("title") or "").strip().lower()
            cc = s.get("countryCode")
            if title and cc:
                existing_title_country.add((title, cc))
            if cc:
                country_files.setdefault(cc, set()).add(rel)

    return index, file_data_cache, existing_urls, existing_title_country, country_files, file_station_counts


def pick_target_file(country_code, country_files, file_station_counts, index):
    """Prefer a file that already carries this country's other stations
    (keeps the catalog tidy); among those, the least-full one, so files
    stay roughly balanced as the catalog grows over time. For a country
    with nothing in the catalog yet, fall back to the least-full file in
    its continent's region group instead of picking any file globally."""
    candidates = country_files.get(country_code)
    if not candidates:
        group = FALLBACK_CONTINENT.get(country_code, "other")
        candidates = {r["file"] for r in index["regions"] if r["region"] == group}
    if not candidates:
        candidates = set(file_station_counts.keys())
    return min(candidates, key=lambda f: file_station_counts[f])


async def discover_candidates(session, country_code, timeout):
    url = f"{RB_BASE}/stations/bycountrycodeexact/{country_code}"
    params = {
        "hidebroken": "true",
        "lastcheckok": "1",
        "order": "clickcount",
        "reverse": "true",
        "limit": "300",
    }
    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=timeout), headers=HEADERS
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json(content_type=None)
    except Exception:
        return []

    out = []
    for s in data:
        stream = s.get("url_resolved") or s.get("url") or ""
        title = (s.get("name") or "").strip()
        if not stream.startswith("http") or not title or title.lower() == "unknown station":
            continue
        # Radio Browser has no length/format limit on station names, and some
        # owners abuse that for SEO — e.g. a "name" that's actually a
        # 200-character wall of genre keywords. A real UI can't display that
        # (breaks every card/list title everywhere it appears), so treat an
        # implausible title as a quality signal, not just a display nuisance.
        if len(title) > 60 or title.count(",") > 3:
            continue
        bitrate = s.get("bitrate") or 0
        if isinstance(bitrate, str):
            try:
                bitrate = int(bitrate)
            except ValueError:
                bitrate = 0
        if bitrate and bitrate < 32:
            continue
        tags = s.get("tags") or ""
        genre = tags.split(",")[0].strip().title() if tags else "Radio"
        out.append(
            {
                "title": title,
                "streamUrl": stream,
                "countryCode": country_code,
                "language": (s.get("language") or "").split(",")[0].strip() or None,
                "genre": genre,
                "subtitle": genre,
                "location": s.get("country") or "Unknown",
                "clickCount": s.get("clickcount") or 0,
                "bitrate": bitrate,
            }
        )
    return out


async def main_async(args) -> int:
    (
        index,
        file_data_cache,
        existing_urls,
        existing_title_country,
        country_files,
        file_station_counts,
    ) = build_catalog_state()
    covered_countries = sorted(set(country_files.keys()) | set(FALLBACK_CONTINENT.keys()))
    print(f"Discovering new stations across {len(covered_countries)} countries...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def fetch(cc):
            async with sem:
                return cc, await discover_candidates(session, cc, args.timeout)

        results = await asyncio.gather(*(fetch(cc) for cc in covered_countries))

    new_candidates = []
    seen_this_run = set()
    for _cc, items in results:
        for it in items:
            url = it["streamUrl"]
            key = (it["title"].strip().lower(), it["countryCode"])
            if url in existing_urls or url in seen_this_run or key in existing_title_country:
                continue
            seen_this_run.add(url)
            new_candidates.append(it)

    print(f"Found {len(new_candidates)} candidates not already in the catalog.")
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

    per_file_additions: dict[str, list] = {}
    added_ids = set()
    for it in verified:
        sid = stable_id(it["title"], it["countryCode"], it["streamUrl"])
        if sid in added_ids:
            continue
        added_ids.add(sid)
        target = pick_target_file(it["countryCode"], country_files, file_station_counts, index)
        station = {
            "id": sid,
            "title": it["title"],
            "subtitle": it["subtitle"],
            "location": it["location"],
            "genre": it["genre"],
            "countryCode": it["countryCode"],
            "language": it["language"],
            "clickCount": it["clickCount"],
            "bitrate": it["bitrate"],
            "streamUrl": it["streamUrl"],
        }
        per_file_additions.setdefault(target, []).append(station)
        file_station_counts[target] += 1

    all_new = []
    for rel, additions in per_file_additions.items():
        data = file_data_cache[rel]
        data["stations"].extend(additions)
        data["updated"] = now_iso()
        dump_json(REPO_ROOT / rel, data)
        all_new.extend(additions)
        print(f"  {rel}: +{len(additions)}")

    for region in index["regions"]:
        rel = region["file"]
        if rel in file_data_cache:
            region["count"] = len(file_data_cache[rel]["stations"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    all_stations = []
    for rel in index["files"]:
        all_stations.extend(file_data_cache[rel]["stations"])
    dump_json(LEGACY_PATH, all_stations)

    print(f"\nAdded {len(all_new)} new stations total. Legacy radio.json resynced with {len(all_stations)} items.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=500, help="Cap on new stations added per run")
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=8.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
