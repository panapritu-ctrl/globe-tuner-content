# Globe Tuner — Remote Content

Editable content lists for the Globe Tuner app, fetched at runtime so a fix or
addition here goes live without an app store release.

## Layout (split by index, 2026-07)

Each content type now follows the same pattern: an index file at the repo
root listing category files, plus the category files themselves in a
subfolder. This replaced single monolithic files (`radio.json` had grown to
~1.5MB / 5,000 stations / 45,000 lines, unwieldy to hand-edit on github.com)
without breaking older app builds — see "Legacy files" below.

| Content   | Index                    | Category files                     | Split by            |
|-----------|---------------------------|-------------------------------------|----------------------|
| Radio     | `radio_index.json`         | `radio/radio_<region>.json`         | continent            |
| Podcasts  | `podcasts_index.json`      | `podcasts/podcasts_<category>.json` | genre bucket         |
| Audiobooks| `audiobooks_index.json`    | `audiobooks/audiobooks_<genre>.json`| genre                |
| Devotional| `devotional_index.json`    | `devotional/devotional_<religion>.json` | religion         |

An index file looks like:

```json
{
  "version": 2,
  "updated": "2026-07-29T00:00:00Z",
  "files": ["radio/radio_europe.json", "radio/radio_asia.json", "..."]
}
```

The app fetches the index, then fetches every listed file in parallel and
merges the results — same `Future.wait` fan-out pattern it already used for
the 4 top-level fetches (see `RemoteContentService` in the app repo).

**To add/fix a station**: edit the one region file it lives in (or add a new
region file and list it in `radio_index.json`) — never the whole list.

**To add a brand new region/category file**: create it, then add its path
to the relevant `_index.json`'s `files` array. No app update needed.

## Legacy files (`radio.json`, `podcasts.json`, `audiobooks.json`,
`devotional.json`)

Kept at the repo root, same filename/shape as before, and **must stay in
sync** with the split files — they're the fallback for app installs that
predate the split-fetch code (which checks the index first, and falls back
to fetching the single legacy file by its old name if the index 404s or is
empty). `radio.json` is the union of all `radio/radio_*.json` files;
likewise for the other three. If you edit a category file, regenerate (or
manually mirror the change into) the matching legacy file too, or older
app installs won't see the update until they're upgraded.

## Radio: regional coverage (2026-07 refresh, expanded same month)

`radio/` was expanded from the original ~5,000 curated stations in two
passes, both pulling verified-working (`hidebroken=true`, `lastcheckok=1`,
ordered by votes) stations per country from the
[Radio Browser API](https://api.radio-browser.info) — a community-run,
continuously-verified directory of internet radio streams — so small/
less-popular countries get representation rather than the list skewing
toward the US/UK. Deduped at every step against the existing list by
normalized stream URL and by (title, country).

- Pass 1: top ~15/country → 5,788 total.
- Pass 2: full per-country pull (no cap, still `hidebroken`-filtered) →
  **39,626 total**.

| Region    | Stations |
|-----------|----------|
| Europe    | 19,905   |
| Americas  | 10,209   |
| Asia      | 6,694    |
| Oceania   | 1,806    |
| Africa    | 963      |
| Other     | 49       |

Region sizes reflect real-world coverage in Radio Browser's database, not a
curation choice — Africa has far fewer catalogued streams there than Europe/
Americas. `radio/radio_europe.json` is ~6.9MB uncompressed; the app fetches
it (and every region file) with a 25s timeout and falls back to bundled
content silently if that's too slow on a given connection.

## Editing a station entry

Each station:

```json
{
  "id": "unique_snake_case_id",
  "title": "Station Name",
  "subtitle": "Short tagline",
  "location": "City, Country",
  "genre": "News | Talk | Pop | Rock | Electronic | Jazz | Classical | ...",
  "countryCode": "GB",
  "streamUrl": "https://... (direct audio stream, mp3/aac/hls)"
}
```

Before adding a station, verify the `streamUrl` actually resolves to audio
(not a redirect to a webpage) — e.g. `curl -I <url>` should return `200` (or
a `30x` redirect that itself lands on `200` with an audio content-type).
Several stations in the app's original bundled list had gone dead this way
(old CDN paths retired, domain migrations) with nothing visibly wrong until
someone tried to actually play them.

## Caching note

`raw.githubusercontent.com` caches aggressively (~5 minutes). If you push a
fix and it's not showing up immediately in the app, that's expected — it'll
propagate shortly.
