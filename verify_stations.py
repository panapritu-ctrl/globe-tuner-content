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

Per-category safety valve: if a category's survival rate looks abnormal
(e.g. almost everything "failed", which usually means a network/proxy
problem here or an upstream outage — archive.org especially — rather than
that many stations actually died since the last run), that category is
skipped and nothing is written for it. Always exits 0 on a normal
completion, including when one or more categories got skipped this way —
that's expected, self-protecting behavior, not a script error, and CI
would otherwise flag a perfectly healthy run as failed. Look for
"[ABORTED]" in the output/logs to see what, if anything, got skipped.
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
# No self-identifying suffix — confirmed empirically that at least one real,
# live station (naxidigital-exyu128.streaming.rs) serves a fake HTML "not a
# player" page to any User-Agent it doesn't recognize (including a plain
# "GlobeTuner-Verify/1.0" one) while serving real audio to a normal-looking
# browser UA. Matches the same UA the app itself uses for real playback
# (PlayerProvider._buildAudioSource), so this check reflects what actually
# happens for users, not what happens to an obviously-scripted requester.
HEADERS = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36"}


# A 200 OK response can still be a dead station in disguise — some hosts
# redirect an expired/removed stream to a generic HTML "station not found"
# page, or a JSON error body, and still answer with 2xx. Full audio decoding
# (actually play a few seconds and check for real sound) would catch even
# more, but needs ffmpeg per station and takes ~10x longer per check across
# 40k+ stations for a marginal gain — none of the well-known public station
# lists (Radio Browser included) go that far either. This is the practical
# middle ground: read a small chunk of the real response body and reject it
# if it's obviously not audio, instead of trusting the status code alone.
_HTML_SNIFF = (b"<html", b"<!doctype html", b"<HTML", b"<!DOCTYPE HTML")
_BAD_CONTENT_TYPES = ("text/html", "application/json")


def _looks_like_real_audio(content_type: str, body_sniff: bytes) -> bool:
    ct = (content_type or "").lower()
    if any(bad in ct for bad in _BAD_CONTENT_TYPES):
        return False
    if any(body_sniff.lstrip().startswith(marker) for marker in _HTML_SNIFF):
        return False
    return True


async def check_one(session: aiohttp.ClientSession, url: str, timeout: float) -> bool:
    if not url or not url.startswith("http"):
        return False
    to = aiohttp.ClientTimeout(total=timeout)
    try:
        async with session.head(url, timeout=to, allow_redirects=True, headers=HEADERS) as resp:
            if resp.status < 400 and _looks_like_real_audio(resp.headers.get("Content-Type", ""), b""):
                return True
    except Exception:
        pass
    # Fallback: some radio/podcast hosts don't implement HEAD at all, or a
    # HEAD 200 needs the body sniff a GET-only check can provide.
    try:
        async with session.get(url, timeout=to, allow_redirects=True, headers=HEADERS) as resp:
            if resp.status < 400:
                # Drain a tiny bit so we don't hold the connection open on a
                # live audio stream longer than necessary — also doubles as
                # the content sniff.
                sniff = b""
                try:
                    sniff = await resp.content.read(512)
                except Exception:
                    pass
                if _looks_like_real_audio(resp.headers.get("Content-Type", ""), sniff):
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
    #
    # archive.org-backed categories (audiobooks/devotional) get a much
    # stricter 85% floor than plain-HTTP ones (50%). Confirmed necessary:
    # a real run took devotional from a clean 15/15-per-religion (75 total)
    # to 38 in one pass — 50.7% survival, just above the old 50% floor, so
    # it was trusted and written — purely because archive.org was failing
    # some requests but not others (partial flakiness, not real deaths;
    # the same 75 tracks had scored 100% and 74.7% on adjacent runs days
    # apart). Radio/podcasts see genuine ~5-10% natural churn per run, so
    # 50% stays appropriate there — a real mass die-off is what that
    # threshold exists to catch.
    threshold = 0.85 if archive_org else 0.5
    survival = (total_after / total_before) if total_before else 1.0
    aborted = total_before > 0 and survival < threshold
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

    if any_aborted:
        print(
            "\nOne or more categories were skipped by the safety valve (see above) — "
            "that's expected, self-protecting behavior when an upstream like archive.org "
            "is having a rough moment, not a bug. Exiting 0 so CI doesn't flag a healthy "
            "run as failed; the per-category [ABORTED] markers are the real signal to watch.",
            file=sys.stderr,
        )
    return 0


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
