#!/usr/bin/env python3
"""
Verifies every stream URL in this repo is actually reachable right now, and
rewrites the JSON files to drop whatever isn't — so the app only ever serves
stations confirmed live at the moment this last ran, instead of a static list
that silently rots as stations go offline over time.

Usage:
    python3 verify_stations.py                 # verify + rewrite everything
    python3 verify_stations.py --dry-run        # report only, no writes
    python3 verify_stations.py --only radio     # radio | podcasts | audiobooks | devotional | all
    python3 verify_stations.py --concurrency 300

Exit code is nonzero if verification looks abnormal (e.g. almost everything
failed, which usually means a network/proxy problem here, not that 90% of
stations actually died since the last run) — a CI job should treat that as
"don't trust this run" rather than committing a near-empty catalog.
"""
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiohttp

REPO_ROOT = Path(__file__).resolve().parent

# Kinds of content this script knows how to verify. Radio/podcasts store a
# direct playable URL; audiobooks/devotional are Internet Archive items that
# only store an `identifier` (the app resolves the actual track list from
# archive.org/metadata/{identifier} at play time), so those two need a
# synthesized URL instead of a plain field lookup.
KINDS = {
    "radio": {"index": "radio_index.json", "legacy": "radio.json", "url_field": "streamUrl"},
    "podcasts": {"index": "podcasts_index.json", "legacy": "podcasts.json", "url_field": "feedUrl"},
    "audiobooks": {"index": "audiobooks_index.json", "legacy": "audiobooks.json", "url_field": "identifier", "archive_org": True},
    "devotional": {"index": "devotional_index.json", "legacy": "devotional.json", "url_field": "identifier", "archive_org": True},
}

# A handful of stream hosts are known to reject HEAD (405) or reject the
# generic client / lack of Range support outright but work fine for an actual
# player — so a failed check here always falls back to a real GET before a
# station is declared dead.
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 13) GlobeTuner-Verify/1.0"}


async def check_one(session: aiohttp.ClientSession, url: str, timeout: float) -> bool:
    if not url or not url.startswith("http"):
        return False
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.head(url, timeout=to, allow_redirects=True, headers=HEADERS) as resp:
            if resp.status < 400:
                return True
    except Exception:
        pass
    # Fallback: some radio/podcast hosts don't implement HEAD at all.
    try:
        async with session.get(url, timeout=to, allow_redirects=True, headers=HEADERS) as resp:
            if resp.status < 400:
                # Drain a tiny bit so we don't hold the connection open on a
                # live audio stream longer than necessary.
                try:
                    await resp.content.read(256)
                except Exception:
                    pass
                return True
    except Exception:
        pass
    return False


async def check_archive_item(session: aiohttp.ClientSession, identifier: str, timeout: float) -> bool:
    """An Internet Archive item is 'live' if its metadata resolves and it
    actually lists at least one playable audio file — matching what
    devotional_api_service.dart / AudiobookApiService do at play time."""
    if not identifier:
        return False
    url = f"https://archive.org/metadata/{identifier}"
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.get(url, timeout=to, headers=HEADERS) as resp:
            if resp.status >= 400:
                return False
            data = await resp.json(content_type=None)
    except Exception:
        return False
    files = data.get("files") or []
    audio_exts = (".mp3", ".m4a", ".ogg", ".flac", ".wav")
    return any(str(f.get("name", "")).lower().endswith(audio_exts) for f in files)


async def verify_batch(items, url_field, concurrency, timeout, retries, archive_org=False):
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)
    results = [None] * len(items)

    async def worker(i, item):
        value = item.get(url_field, "")
        async with sem:
            ok = False
            for _ in range(retries):
                ok = await (check_archive_item(session, value, timeout) if archive_org
                            else check_one(session, value, timeout))
                if ok:
                    break
            results[i] = ok

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [worker(i, item) for i, item in enumerate(items)]
        done = 0
        for fut in asyncio.as_completed(tasks):
            await fut
            done += 1
            if done % 1000 == 0 or done == len(tasks):
                print(f"    checked {done}/{len(tasks)}", flush=True)
    return results


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def dump_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


async def process_kind(kind: str, cfg: dict, args) -> dict:
    index_path = REPO_ROOT / cfg["index"]
    if not index_path.exists():
        print(f"[{kind}] no index file, skipping")
        return {"kind": kind, "before": 0, "after": 0, "aborted": False}

    index = load_json(index_path)
    url_field = cfg["url_field"]
    archive_org = cfg.get("archive_org", False)

    # Pass 1: verify everything and hold results in memory before writing
    # anything — a category-wide near-total wipeout (e.g. a wrong field name
    # for this content shape, like audiobooks/devotional not actually having
    # a plain URL field) must never partially land on disk.
    per_file = []  # (file_path, data, key, kept)
    total_before = 0
    total_after = 0
    all_kept = []

    files = index.get("files", [])
    for rel_file in files:
        file_path = REPO_ROOT / rel_file
        if not file_path.exists():
            print(f"  ! missing {rel_file}, skipping")
            continue
        data = load_json(file_path)
        items = data.get("stations") if "stations" in data else data.get("items", [])
        key = "stations" if "stations" in data else "items"

        print(f"[{kind}] {rel_file}: verifying {len(items)} items...")
        results = await verify_batch(items, url_field, args.concurrency, args.timeout, args.retries, archive_org)
        kept = [it for it, ok in zip(items, results) if ok]
        dropped = len(items) - len(kept)

        total_before += len(items)
        total_after += len(kept)
        all_kept.extend(kept)
        per_file.append((file_path, data, key, kept))

        print(f"  -> {len(kept)}/{len(items)} still live ({dropped} removed)")

    # Per-category safety valve: this used to only check the OVERALL ratio
    # across all four categories, which let audiobooks/devotional silently
    # go to zero while radio's healthy 89% masked it in the aggregate.
    survival = (total_after / total_before) if total_before else 1.0
    aborted = total_before > 0 and survival < 0.5
    if aborted:
        print(
            f"[{kind}] ABORTED: only {survival*100:.1f}% verified live — treating this as a "
            "checker problem (wrong field/endpoint for this content shape), not real content "
            "death. Not writing any changes for this category.",
            file=sys.stderr,
        )
        return {"kind": kind, "before": total_before, "after": total_before, "aborted": True}

    if args.dry_run:
        return {"kind": kind, "before": total_before, "after": total_after, "aborted": False}

    # Pass 2: safe to write
    for file_path, data, key, kept in per_file:
        data[key] = kept
        data["updated"] = now_iso()
        dump_json(file_path, data)

    for region in index.get("regions", []):
        f = REPO_ROOT / region["file"]
        if f.exists():
            d = load_json(f)
            region["count"] = len(d.get("stations", d.get("items", [])))
    index["updated"] = now_iso()
    dump_json(index_path, index)

    legacy_path = REPO_ROOT / cfg["legacy"]
    if legacy_path.exists():
        dump_json(legacy_path, all_kept)
        print(f"[{kind}] legacy {cfg['legacy']} resynced with {len(all_kept)} items")

    return {"kind": kind, "before": total_before, "after": total_after, "aborted": False}


async def main_async(args):
    kinds = list(KINDS.keys()) if args.only in (None, "all") else [args.only]
    summary = []
    for kind in kinds:
        summary.append(await process_kind(kind, KINDS[kind], args))

    print("\n=== Summary ===")
    any_aborted = False
    for s in summary:
        pct = (s["after"] / s["before"] * 100) if s["before"] else 0
        flag = "  [ABORTED — not written, see stderr above]" if s.get("aborted") else ""
        print(f"  {s['kind']:12s} {s['after']:6d} / {s['before']:6d}  ({pct:.1f}% live){flag}")
        any_aborted = any_aborted or s.get("aborted", False)

    return 1 if any_aborted else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't modify files")
    parser.add_argument("--only", choices=["radio", "podcasts", "audiobooks", "devotional", "all"], default="all")
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args()

    exit_code = asyncio.run(main_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
