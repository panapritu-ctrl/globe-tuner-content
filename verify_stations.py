#!/usr/bin/env python3
"""
Verifies every stream URL in this repo is actually reachable right now, and
rewrites the JSON files to drop whatever isn't — so the app only ever serves
stations confirmed live at the moment this last ran, instead of a static list
that silently rots as stations go offline over time.

Usage:
    python3 verify_stations.py                              # verify + rewrite everything
    python3 verify_stations.py --dry-run                     # report only, no writes
    python3 verify_stations.py --only radio                  # radio | podcasts | audiobooks | devotional | all
    python3 verify_stations.py --only podcasts,devotional     # comma-separated selection
    python3 verify_stations.py --concurrency 300

    # Radio-only deep check: HTTP reachability AND real decodable audio
    # (via ffprobe), one 1/15th slice of the catalog per day so the full
    # ~33k-station radio catalog gets fully re-verified every 15 days
    # without one huge burst. See check_audio_real()/shard_of() below.
    python3 verify_stations.py --only radio --ffprobe --shard-cycle-days 15 --concurrency 25

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
import datetime
import hashlib
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


def shard_of(station_id: str, cycle_days: int) -> int:
    """Stable shard assignment from the station's own id — NOT list
    position, which would shift every time grow_stations.py inserts or
    verify_stations.py removes something, silently skipping stations or
    double-checking others across the cycle. Hashing the id keeps a given
    station in the same shard for its whole life regardless of catalog
    churn elsewhere."""
    return int(hashlib.md5(str(station_id).encode()).hexdigest(), 16) % cycle_days


def today_shard(cycle_days: int) -> int:
    """Deterministic from the calendar date alone — no cursor file to
    read/write/corrupt. If a scheduled run is ever delayed or skipped by
    GitHub, the next run just computes whatever shard today's date maps
    to; nothing compounds or drifts, it self-heals."""
    return datetime.date.today().toordinal() % cycle_days


async def check_audio_real(url: str, timeout: float) -> bool:
    """Actually asks ffprobe to open the stream and identify a real,
    decodable audio codec — catches the case a plain HTTP check can't:
    a station whose web server is perfectly healthy (200 OK, plausible
    Content-Type) but whose underlying encoder feed has died, so there's
    no real audio behind the response. Capped analyze window so this stays
    fast (a few seconds/KB, not downloading the whole stream) — format-
    agnostic, so one method covers MP3/AAC/AAC+/Opus/Vorbis/whatever a
    given station happens to use, no per-codec library needed.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-analyzeduration", "3000000",  # 3s of stream data
        "-probesize", "131072",         # or 128KB, whichever comes first
        "-select_streams", "a",
        "-show_entries", "stream=codec_type",
        "-of", "csv=p=0",
        url,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        # ffprobe isn't installed on this runner — fail loudly rather than
        # silently treating every station as "audio check failed" without
        # ever actually checking. main() verifies ffprobe exists up front
        # specifically so this path should never be hit in practice.
        raise
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout + 3)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return False
    return proc.returncode == 0 and b"audio" in stdout


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


async def verify_batch(items, url_field, concurrency, timeout, retries, archive_org=False, ffprobe_timeout=None):
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
            # Deep layer: only spent on items that already passed the cheap
            # HTTP check — no point spawning an ffprobe process for a URL
            # that's already confirmed unreachable.
            if ok and ffprobe_timeout is not None:
                ok = await check_audio_real(value, ffprobe_timeout)
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

    # Radio-only, opt-in: check just today's 1/Nth slice of the catalog
    # instead of everything, so a daily run completes the full catalog once
    # per N days without one huge burst. shard_of() is id-based (stable
    # under catalog growth/shrinkage), today_shard() is date-based (no
    # cursor file needed — self-heals if a run is ever missed).
    shard_days = args.shard_cycle_days if (kind == "radio" and args.shard_cycle_days) else None
    todays_shard = today_shard(shard_days) if shard_days else None
    ffprobe_timeout = args.timeout if (kind == "radio" and args.ffprobe) else None

    files = index.get("files", [])
    for rel_file in files:
        file_path = REPO_ROOT / rel_file
        if not file_path.exists():
            print(f"  ! missing {rel_file}, skipping")
            continue
        data = load_json(file_path)
        items = data.get("stations") if "stations" in data else data.get("items", [])
        key = "stations" if "stations" in data else "items"

        if shard_days:
            to_check_idx = [i for i, it in enumerate(items) if shard_of(it.get("id", ""), shard_days) == todays_shard]
        else:
            to_check_idx = list(range(len(items)))
        to_check = [items[i] for i in to_check_idx]

        shard_note = f" (shard {todays_shard}/{shard_days}, rest untouched today)" if shard_days else ""
        probe_note = " + ffprobe audio check" if ffprobe_timeout else ""
        print(f"[{kind}] {rel_file}: verifying {len(to_check)}/{len(items)} items{shard_note}{probe_note}...")

        results = await verify_batch(to_check, url_field, args.concurrency, args.timeout, args.retries,
                                      archive_org, ffprobe_timeout=ffprobe_timeout)
        result_by_idx = dict(zip(to_check_idx, results))

        # Curated items (manually vetted on user request, not bulk-imported
        # from an API) are always kept regardless of this run's check result
        # — a transient failure here shouldn't silently delete something a
        # human specifically asked to add. Still checked and logged so a
        # persistently-dead curated entry is visible, just never auto-removed.
        kept = []
        still_flagged = 0
        checked_count = 0
        checked_passed = 0
        for i, it in enumerate(items):
            if i not in result_by_idx:
                kept.append(it)  # not today's shard — left exactly as-is
                continue
            checked_count += 1
            ok = result_by_idx[i]
            if ok:
                checked_passed += 1
                kept.append(it)
            elif it.get("curated"):
                still_flagged += 1
                kept.append(it)
            # else: genuinely dropped
        if still_flagged:
            print(f"    (kept {still_flagged} curated item(s) despite a failed check this run)")
        dropped = checked_count - checked_passed - still_flagged

        # Safety-valve ratio is computed against the CHECKED subset, not the
        # whole file — with sharding, the untouched majority would otherwise
        # always trivially "survive" and mask a genuinely bad day for the
        # slice actually checked today.
        total_before += checked_count
        total_after += checked_passed + still_flagged
        all_kept.extend(kept)
        per_file.append((file_path, data, key, kept))

        print(f"  -> {checked_passed + still_flagged}/{checked_count} checked items still live ({dropped} removed); "
              f"{len(kept)}/{len(items)} total remain in file")

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
    kinds = list(KINDS.keys()) if args.only in (None, "all") else args.only.split(",")
    unknown = [k for k in kinds if k not in KINDS]
    if unknown:
        print(f"Unknown --only value(s): {unknown} — choose from {list(KINDS.keys())} or 'all'", file=sys.stderr)
        return 2
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
    parser.add_argument("--only", default="all",
                         help="Comma-separated: radio,podcasts,audiobooks,devotional — or 'all' (default)")
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=8.0, help="Per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--ffprobe", action="store_true",
                         help="Radio only: also verify real decodable audio via ffprobe, not just HTTP reachability")
    parser.add_argument("--shard-cycle-days", type=int, default=None,
                         help="Radio only: check just 1/Nth of the catalog today (id-hash based), "
                              "so a daily run fully re-verifies everything once every N days")
    args = parser.parse_args()

    if args.ffprobe:
        import shutil
        if shutil.which("ffprobe") is None:
            print("--ffprobe was requested but ffprobe is not on PATH — install ffmpeg first "
                  "(apt-get install -y ffmpeg on the GitHub Actions runner). Refusing to silently "
                  "run without the check it was asked to perform.", file=sys.stderr)
            sys.exit(2)

    exit_code = asyncio.run(main_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
