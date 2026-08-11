#!/usr/bin/env python3
"""
Discovers new audiobooks and appends verified-live ones — the audiobooks
counterpart to grow_stations.py (radio). Growth-only; verify_stations.py
(--only audiobooks) is still what removes dead items.

Source: archive.org's advancedsearch API, queried per genre using the exact
same _genreQueries strings AudiobookApiService uses in the app (see
lib/app/services/audiobook_api_service.dart), minus the 'All' bucket (every
genre here is already collection:(librivoxaudio), so 'All' would just
re-discover the same items already covered by the six specific genres).

A candidate is verified the same way verify_stations.py's
check_archive_item() does: its archive.org/metadata/{identifier} must
resolve AND actually list at least one playable audio file — an item can
exist in search results but have no audio attached (text-only scan, video-
only upload, etc.), so this catches that before it lands in the catalog.

Usage:
    python3 grow_audiobooks.py                  # discover + append (cap 300 new)
    python3 grow_audiobooks.py --dry-run
    python3 grow_audiobooks.py --max-new 100
"""
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiohttp

REPO_ROOT = Path(__file__).resolve().parent
INDEX_PATH = REPO_ROOT / "audiobooks_index.json"
LEGACY_PATH = REPO_ROOT / "audiobooks.json"
SEARCH_URL = "https://archive.org/advancedsearch.php"
META_URL = "https://archive.org/metadata"
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}

# Same queries as AudiobookApiService._genreQueries in the app (minus 'All'
# — see module docstring), mapped onto the genre file names already used in
# audiobooks_index.json.
GENRE_QUERIES = {
    "love_story": 'collection:(librivoxaudio) AND subject:(romance OR love)',
    "fictions":   'collection:(librivoxaudio) AND subject:(fiction)',
    "history":    'collection:(librivoxaudio) AND subject:(history)',
    "self_help":  'collection:(librivoxaudio) AND subject:("self help" OR self-improvement OR philosophy OR inspirational)',
    "mystery":    'collection:(librivoxaudio) AND subject:(mystery OR detective)',
    "biography":  'collection:(librivoxaudio) AND subject:(biography OR autobiography)',
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


def file_for_genre(genre, index):
    for entry in index["genres"]:
        if entry["genre"] == genre:
            return entry["file"]
    return index["files"][0]


async def search_genre(session, genre, query, rows, timeout):
    params = {
        "q": query,
        "fl[]": "identifier,title,creator,downloads,subject",
        # A literal space, not "downloads+desc" — aiohttp's params dict
        # percent-encodes a literal "+" in a value as %2B (it only treats "+"
        # as a space when it's doing the space->+ substitution itself), so
        # "+" here silently produced an unrecognized sort archive.org ignored
        # 0 results for. A real space encodes correctly either way.
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
        # normalization AudiobookApiService._search does in the app.
        raw_title = doc.get("title")
        title = (raw_title[0] if isinstance(raw_title, list) and raw_title else raw_title or "").strip()
        if not identifier or not title:
            continue
        creator = doc.get("creator")
        author = creator[0] if isinstance(creator, list) and creator else (creator or "Unknown Author")
        out.append(
            {
                "genre": genre,
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
    known_genres = {e["genre"] for e in load_json(INDEX_PATH)["genres"]}
    unknown = [g for g in GENRE_QUERIES if g not in known_genres]
    if unknown:
        print(f"GENRE_QUERIES has genre(s) not in audiobooks_index.json: {unknown} — fix before running.", file=sys.stderr)
        return 2

    index, file_data_cache, existing_ids = build_catalog_state()
    print(f"Discovering new audiobooks across {len(GENRE_QUERIES)} genres...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def fetch(genre, query):
            async with sem:
                return await search_genre(session, genre, query, args.fetch_rows, args.timeout)

        results = await asyncio.gather(*(fetch(g, q) for g, q in GENRE_QUERIES.items()))

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
            print(f"  + [{it['genre']}] {it['title']} — {it['creator']} ({it['identifier']})")
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
        target = file_for_genre(it["genre"], index)
        entry = {
            "identifier": ident,
            "title": it["title"],
            "creator": it["creator"],
            "genre": it["genre"].replace("_", " ").title(),
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

    for entry in index["genres"]:
        rel = entry["file"]
        if rel in file_data_cache:
            entry["count"] = len(file_data_cache[rel]["items"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    all_items = []
    for rel in index["files"]:
        all_items.extend(file_data_cache[rel]["items"])
    dump_json(LEGACY_PATH, all_items)

    print(f"\nAdded {len(all_new)} new audiobooks total. Legacy audiobooks.json resynced with {len(all_items)} items.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=300, help="Cap on new audiobooks added per run")
    parser.add_argument("--fetch-rows", type=int, default=200, help="Candidates to pull from archive.org per genre before filtering")
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
