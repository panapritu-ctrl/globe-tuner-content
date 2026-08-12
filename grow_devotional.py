#!/usr/bin/env python3
"""
Discovers new devotional items and appends verified-live ones — the
devotional counterpart to grow_stations.py (radio). Growth-only;
verify_stations.py (--only devotional) is still what removes dead items.

Source: archive.org's advancedsearch API, queried per religion using the
exact same _religionQueries strings DevotionalApiService uses in the app
(see lib/app/services/devotional_api_service.dart), so what this discovers
lines up with what the app itself already searches for.

A candidate is verified the same way verify_stations.py's
check_archive_item() does: its archive.org/metadata/{identifier} must
resolve AND actually list at least one playable audio file.

Usage:
    python3 grow_devotional.py                  # discover + append (cap 200 new)
    python3 grow_devotional.py --dry-run
    python3 grow_devotional.py --max-new 100
"""
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiohttp

REPO_ROOT = Path(__file__).resolve().parent
INDEX_PATH = REPO_ROOT / "devotional_index.json"
LEGACY_PATH = REPO_ROOT / "devotional.json"
SEARCH_URL = "https://archive.org/advancedsearch.php"
META_URL = "https://archive.org/metadata"
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}

# Same queries as DevotionalApiService._religionQueries in the app, mapped
# onto the religion file names already used in devotional_index.json
# (lowercase of the app's religion label).
RELIGION_QUERIES = {
    "hindu":     '(subject:(hinduism OR bhajan OR mantra OR "bhagavad gita" OR hanuman OR vedic OR krishna OR ramayan OR aarti OR chalisa OR shiv)) AND mediatype:(audio)',
    "islam":     '(subject:(quran OR islamic OR "quran recitation" OR namaz)) AND mediatype:(audio)',
    "christian": '(subject:(bible OR christian OR gospel OR hymn OR prayer)) AND mediatype:(audio)',
    "sikh":      '(subject:(gurbani OR sikhism OR "guru granth" OR kirtan)) AND mediatype:(audio)',
    "buddhist":  '(subject:(buddhism OR meditation OR "guided meditation" OR zen OR dharma)) AND mediatype:(audio)',
}
AUDIO_EXTS = (".mp3", ".m4a", ".ogg", ".flac", ".wav")


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
    index = load_json(INDEX_PATH)
    existing_ids = set()
    file_data_cache = {}

    for rel in index["files"]:
        data = load_json(REPO_ROOT / rel)
        file_data_cache[rel] = data
        for it in data["items"]:
            ident = it.get("identifier")
            if ident:
                existing_ids.add(ident)

    return index, file_data_cache, existing_ids


def file_for_religion(religion, index):
    for entry in index["religions"]:
        if entry["religion"] == religion:
            return entry["file"]
    return index["files"][0]


async def search_religion(session, religion, query, rows, timeout):
    params = {
        "q": query,
        "fl[]": "identifier,title,creator,downloads",
        # Real space, not "downloads+desc" — see grow_audiobooks.py for why
        # a literal "+" here gets percent-encoded to %2B by aiohttp and
        # silently produces zero results instead of an error.
        "sort[]": "downloads desc",
        "rows": str(rows),
        "start": "0",
        "output": "json",
    }
    try:
        async with session.get(
            SEARCH_URL, params=params, timeout=aiohttp.ClientTimeout(total=timeout), headers=HEADERS
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json(content_type=None)
    except Exception:
        return []

    out = []
    for doc in (data.get("response") or {}).get("docs", []):
        identifier = doc.get("identifier") or ""
        # Archive.org returns 'title' (like 'creator'/'subject') as either a
        # plain string or a list inconsistently across items — see the same
        # normalization DevotionalApiService._search does in the app.
        raw_title = doc.get("title")
        title = (raw_title[0] if isinstance(raw_title, list) and raw_title else raw_title or "").strip()
        if not identifier or not title:
            continue
        creator = doc.get("creator")
        author = creator[0] if isinstance(creator, list) and creator else (creator or "Unknown")
        out.append(
            {
                "religion": religion,
                "identifier": identifier,
                "title": title,
                "creator": str(author),
                "downloads": doc.get("downloads") or 0,
            }
        )
    return out


async def verify_item(session, identifier, timeout):
    if not identifier:
        return False
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.get(f"{META_URL}/{identifier}", timeout=to, headers=HEADERS) as resp:
            if resp.status >= 400:
                return False
            data = await resp.json(content_type=None)
    except Exception:
        return False
    files = data.get("files") or []
    return any(str(f.get("name", "")).lower().endswith(AUDIO_EXTS) for f in files)


async def main_async(args) -> int:
    known_religions = {e["religion"] for e in load_json(INDEX_PATH)["religions"]}
    unknown = [r for r in RELIGION_QUERIES if r not in known_religions]
    if unknown:
        print(f"RELIGION_QUERIES has religion(s) not in devotional_index.json: {unknown} — fix before running.", file=sys.stderr)
        return 2

    index, file_data_cache, existing_ids = build_catalog_state()
    print(f"Discovering new devotional items across {len(RELIGION_QUERIES)} religions...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def fetch(religion, query):
            async with sem:
                return await search_religion(session, religion, query, args.fetch_rows, args.timeout)

        results = await asyncio.gather(*(fetch(r, q) for r, q in RELIGION_QUERIES.items()))

    new_candidates = []
    seen_this_run = set()
    for items in results:
        for it in items:
            ident = it["identifier"]
            if ident in existing_ids or ident in seen_this_run:
                continue
            seen_this_run.add(ident)
            new_candidates.append(it)

    print(f"Found {len(new_candidates)} candidates not already in the catalog.")
    if not new_candidates:
        return 0

    new_candidates.sort(key=lambda x: x["downloads"], reverse=True)
    new_candidates = new_candidates[: args.max_new]

    print(f"Verifying {len(new_candidates)} archive.org items actually have playable audio...")
    verified_flags = [None] * len(new_candidates)
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def verify(i, item):
            async with sem:
                verified_flags[i] = await verify_item(session, item["identifier"], args.timeout)

        await asyncio.gather(*(verify(i, it) for i, it in enumerate(new_candidates)))

    verified = [it for it, ok in zip(new_candidates, verified_flags) if ok]
    print(f"{len(verified)}/{len(new_candidates)} verified with real audio.")
    if not verified:
        return 0

    if args.dry_run:
        for it in verified[:20]:
            print(f"  + [{it['religion']}] {it['title']} — {it['creator']} ({it['identifier']})")
        if len(verified) > 20:
            print(f"  ... and {len(verified) - 20} more")
        return 0

    per_file_additions = {}
    added_ids = set()
    for it in verified:
        ident = it["identifier"]
        if ident in added_ids:
            continue
        added_ids.add(ident)
        target = file_for_religion(it["religion"], index)
        entry = {
            "identifier": ident,
            "title": it["title"],
            "creator": it["creator"],
            "religion": it["religion"].title(),
        }
        per_file_additions.setdefault(target, []).append(entry)

    all_new = []
    for rel, additions in per_file_additions.items():
        data = file_data_cache[rel]
        data["items"].extend(additions)
        data["updated"] = now_iso()
        dump_json(REPO_ROOT / rel, data)
        all_new.extend(additions)
        print(f"  {rel}: +{len(additions)}")

    for entry in index["religions"]:
        rel = entry["file"]
        if rel in file_data_cache:
            entry["count"] = len(file_data_cache[rel]["items"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    all_items = []
    for rel in index["files"]:
        all_items.extend(file_data_cache[rel]["items"])
    dump_json(LEGACY_PATH, all_items)

    print(f"\nAdded {len(all_new)} new devotional items total. Legacy devotional.json resynced with {len(all_items)} items.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=200, help="Cap on new devotional items added per run")
    parser.add_argument("--fetch-rows", type=int, default=800, help="Candidates to pull from archive.org per religion before filtering")
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
