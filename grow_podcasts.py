#!/usr/bin/env python3
"""
Discovers new podcasts and appends verified-live ones — the podcasts
counterpart to grow_stations.py (radio). Growth-only; verify_stations.py
(--only podcasts) is still what removes dead feeds.

Source: iTunes Search API (media=podcast), queried per category using the
exact same genreQueries terms PodcastApiService uses in the app (see
lib/app/services/podcast_api_service.dart) so what this discovers lines up
with what the app itself already searches for. "Music" maps to the
culture_music bucket; candidates matching no known category fall back to
"other" instead of being dropped.

A candidate's feedUrl is verified two ways before it's added: the URL must
actually resolve (HTTP reachable), and the body must look like a real
RSS/Atom feed rather than an HTML error page some hosts return with a 200 —
the same class of false-positive check_one() in verify_stations.py guards
against for radio/podcast streams.

Usage:
    python3 grow_podcasts.py                  # discover + append (cap 300 new)
    python3 grow_podcasts.py --dry-run
    python3 grow_podcasts.py --max-new 100
"""
import argparse
import asyncio
import sys
import time
from pathlib import Path

import aiohttp

REPO_ROOT = Path(__file__).resolve().parent
INDEX_PATH = REPO_ROOT / "podcasts_index.json"
LEGACY_PATH = REPO_ROOT / "podcasts.json"
ITUNES_SEARCH = "https://itunes.apple.com/search"
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}

# Same terms as PodcastApiService.genreQueries in the app, mapped onto the
# category file names already used in podcasts_index.json. "Music" is the
# one rename (-> culture_music, matching the existing file); every other
# key is a direct lowercase/underscore of the app's genre label.
CATEGORY_QUERIES = {
    "news": "news",
    "comedy": "comedy",
    "true_crime": "true crime",
    "business": "business",
    "health": "health fitness",
    "technology": "technology",
    "sports": "sports",
    "education": "education",
    "history": "history",
    "culture_music": "music",
}
FALLBACK_CATEGORY = "other"

_BAD_CONTENT_TYPES = ("text/html", "application/json")
_HTML_SNIFF = (b"<html", b"<!doctype html", b"<HTML", b"<!DOCTYPE HTML")
_FEED_SNIFF = (b"<rss", b"<feed", b"<?xml")


def load_json(path: Path):
    import json
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, data):
    import json
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_catalog_state():
    """Scan every category file once: existing collectionIds + feedUrls for
    dedup, and current per-file item counts (for balancing which file a
    genre-mismatched/new-bucket item would land in, mirroring
    pick_target_file's approach in grow_stations.py)."""
    index = load_json(INDEX_PATH)
    existing_ids = set()
    existing_feed_urls = set()
    file_data_cache = {}
    file_counts = {}

    for rel in index["files"]:
        data = load_json(REPO_ROOT / rel)
        file_data_cache[rel] = data
        items = data["items"]
        file_counts[rel] = len(items)
        for it in items:
            cid = it.get("collectionId")
            if cid is not None:
                existing_ids.add(cid)
            feed = (it.get("feedUrl") or "").strip()
            if feed:
                existing_feed_urls.add(feed)

    return index, file_data_cache, existing_ids, existing_feed_urls, file_counts


def file_for_category(category, index):
    for entry in index["categories"]:
        if entry["category"] == category:
            return entry["file"]
    # Category not represented by any file yet (shouldn't normally happen
    # since CATEGORY_QUERIES keys are checked against the index up front) —
    # fall back to "other".
    for entry in index["categories"]:
        if entry["category"] == FALLBACK_CATEGORY:
            return entry["file"]
    return index["files"][0]


def _looks_like_real_feed(content_type: str, body_sniff: bytes) -> bool:
    ct = (content_type or "").lower()
    if "xml" in ct or "rss" in ct:
        return True
    if any(bad in ct for bad in _BAD_CONTENT_TYPES):
        # Some hosts mislabel a real feed as text/html but the body is
        # still real XML — sniff the body before trusting the header alone.
        pass
    stripped = body_sniff.lstrip()
    if any(stripped.startswith(marker) for marker in _HTML_SNIFF):
        return False
    return any(marker in body_sniff[:200] for marker in _FEED_SNIFF)


async def verify_feed(session, feed_url, timeout):
    if not feed_url or not feed_url.startswith("http"):
        return False
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.get(feed_url, timeout=to, allow_redirects=True, headers=HEADERS) as resp:
            if resp.status >= 400:
                return False
            sniff = b""
            try:
                sniff = await resp.content.read(2048)
            except Exception:
                pass
            return _looks_like_real_feed(resp.headers.get("Content-Type", ""), sniff)
    except Exception:
        return False


async def search_category(session, category, term, timeout):
    params = {"media": "podcast", "term": term, "limit": "200"}
    try:
        async with session.get(
            ITUNES_SEARCH, params=params, timeout=aiohttp.ClientTimeout(total=timeout), headers=HEADERS
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json(content_type=None)
    except Exception:
        return []

    out = []
    for r in data.get("results", []):
        feed_url = (r.get("feedUrl") or "").strip()
        title = (r.get("collectionName") or "").strip()
        collection_id = r.get("collectionId")
        if not feed_url.startswith("http") or not title or collection_id is None:
            continue
        genres = r.get("genres") or []
        out.append(
            {
                "category": category,
                "collectionId": collection_id,
                "title": title,
                "artist": (r.get("artistName") or "Unknown Artist").strip(),
                "genre": (genres[0] if genres else term.title()),
                "feedUrl": feed_url,
                "artworkUrl": r.get("artworkUrl600") or r.get("artworkUrl100") or "",
                "trackCount": r.get("trackCount") or 0,
                "country": r.get("country") or "",
            }
        )
    return out


async def main_async(args) -> int:
    unknown = [c for c in CATEGORY_QUERIES if c not in {e["category"] for e in load_json(INDEX_PATH)["categories"]}]
    if unknown:
        print(f"CATEGORY_QUERIES has categor(y/ies) not in podcasts_index.json: {unknown} — fix before running.", file=sys.stderr)
        return 2

    index, file_data_cache, existing_ids, existing_feed_urls, file_counts = build_catalog_state()
    print(f"Discovering new podcasts across {len(CATEGORY_QUERIES)} categories...")

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def fetch(category, term):
            async with sem:
                return await search_category(session, category, term, args.timeout)

        results = await asyncio.gather(*(fetch(c, t) for c, t in CATEGORY_QUERIES.items()))

    new_candidates = []
    seen_this_run = set()
    for items in results:
        for it in items:
            cid = it["collectionId"]
            feed = it["feedUrl"]
            if cid in existing_ids or feed in existing_feed_urls or cid in seen_this_run:
                continue
            seen_this_run.add(cid)
            new_candidates.append(it)

    print(f"Found {len(new_candidates)} candidates not already in the catalog.")
    if not new_candidates:
        return 0

    new_candidates.sort(key=lambda x: x["trackCount"], reverse=True)
    new_candidates = new_candidates[: args.max_new]

    print(f"Verifying {len(new_candidates)} feed URLs are actually live RSS...")
    verified_flags = [None] * len(new_candidates)
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        sem = asyncio.Semaphore(args.concurrency)

        async def verify(i, item):
            async with sem:
                verified_flags[i] = await verify_feed(session, item["feedUrl"], args.timeout)

        await asyncio.gather(*(verify(i, it) for i, it in enumerate(new_candidates)))

    verified = [it for it, ok in zip(new_candidates, verified_flags) if ok]
    print(f"{len(verified)}/{len(new_candidates)} verified live.")
    if not verified:
        return 0

    if args.dry_run:
        for it in verified[:20]:
            print(f"  + [{it['category']}] {it['title']} — {it['artist']} ({it['feedUrl']})")
        if len(verified) > 20:
            print(f"  ... and {len(verified) - 20} more")
        return 0

    per_file_additions = {}
    added_ids = set()
    for it in verified:
        cid = it["collectionId"]
        if cid in added_ids:
            continue
        added_ids.add(cid)
        target = file_for_category(it["category"], index)
        entry = {
            "collectionId": cid,
            "title": it["title"],
            "artist": it["artist"],
            "genre": it["genre"],
            "feedUrl": it["feedUrl"],
            "artworkUrl": it["artworkUrl"],
            "trackCount": it["trackCount"],
            "country": it["country"],
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

    for entry in index["categories"]:
        rel = entry["file"]
        if rel in file_data_cache:
            entry["count"] = len(file_data_cache[rel]["items"])
    index["updated"] = now_iso()
    dump_json(INDEX_PATH, index)

    all_items = []
    for rel in index["files"]:
        all_items.extend(file_data_cache[rel]["items"])
    dump_json(LEGACY_PATH, all_items)

    print(f"\nAdded {len(all_new)} new podcasts total. Legacy podcasts.json resynced with {len(all_items)} items.")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-new", type=int, default=300, help="Cap on new podcasts added per run")
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
