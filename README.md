# Globe Tuner — Remote Content

Editable content lists for the Globe Tuner app, fetched at runtime so a fix or
addition here goes live without an app store release.

## Files

- `radio.json` — curated live radio stations. **Primary source** for the
  "Featured" radio rows; the app's bundled station list is the offline
  fallback if this can't be reached, and the Radio Browser API covers
  everything else (country browsing, search).
- `podcasts.json`, `audiobooks.json`, `devotional.json` — optional
  "Editor's Picks" layered on top of their live APIs (iTunes Search /
  Internet Archive), which are already large and self-updating. Leave
  `items: []` until you want to curate a specific list.

## Editing `radio.json`

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
