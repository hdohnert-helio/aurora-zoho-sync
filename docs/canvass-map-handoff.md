# Canvass map: handoff for the canvassing app

**For:** the code session building Helio's canvassing app.
**From:** the `aurora-zoho-sync` session that built the canvass map (live since 2026-10-22).
**Why:** the canvass map is a short-term tool so Helio's canvasser (JP) can use something now. Long term, the canvassing app should cover the same ground: either build something similar or reuse parts of this one. This file lists every feature, the data rules learned along the way, and which code can be lifted as-is.

The one thing the canvassing app doesn't have today is **Helio's completed installs**: where they are, when they went in, and which walkable zone they belong to. That's what this map provides.

## Where it lives

- **Repo:** `github.com/hdohnert-helio/aurora-zoho-sync`, branch `main`.
- **Running at:** `https://aurora-zoho-sync.onrender.com/canvass`. A FastAPI service on Render, password login.
- **Files:**
  - `canvass.py` (~1,000 lines): sync, geocoding, town assignment, clustering, auth, API.
  - `canvass_static/app.html`: the mobile Leaflet app, a single file with no build step.
  - `canvass_static/login.html`: the login page.
  - `data/ct_towns.geojson`: boundaries for all 169 CT towns.
  - Wired in from the bottom of `main.py` via `canvass.register(app, scheduler, get_zoho_access_token, _build_sheets_service)`.
- **Background:** the "Canvass map" section of `CLAUDE.md` in that repo has the full decision log.

## Features (what the rep sees)

| Feature | Detail |
|---|---|
| Install pins | One dot per completed CT install. Shows the full street address and install month, marked "est." when the date is estimated. No customer names, prices or system details. Pins merge into numbered bubbles when zoomed out. |
| Canvass zones | Orange polygons, each about a 15-minute walk around a group of installs, colored by score (darker = work first). A zone never crosses a town line. |
| Town shading | Green, shaded by install count when zoomed out; fades out at street zoom. |
| Zone panel | Tap a zone to see: town, utility (Eversource/UI), install count, most recent install month, score, and streets with installs (house numbers + months). Also has Google Maps / Apple Maps directions to the zone center, a Walk sheet button and a Town notes button. |
| List view | Ranked zone list, sortable by score or by **nearest to me** (GPS). |
| Filters | Town, installed within (6/12/24 months/all), minimum zone size, utility. Remembered per phone in localStorage. |
| GPS | Live blue dot via `watchPosition`. |
| Satellite layer | Esri World Imagery toggle, useful for spotting panels. |
| Walk sheet | Printable page per zone (`/canvass?sheet=<zone id>`): map, street list, the town's permit notes. |
| Town notes | Per town: solicitor/peddler permit rules, permit office contact, other notes. **The rep edits them** in the app; they're stored in a Google Sheet. |
| Admin extras | Admin login adds "Refresh from Zoho now" and a review queue (records left off the map, and why). |

## Data rules (the hard-won part)

### Which installs count

Source is the Zoho **Installs** module (`CustomModule6`). A record is **installed** when:

- `Project_Stage` is one of `Inspection`, `Witness Test / PTO`, `Energized`, `Project Closeout`, **or**
- `Substantial_Completion` is set and the stage isn't `Canceled`.

As of 2026-10-22: 520 Installs, 414 installed, 394 on the map (the rest are out of state or have unusable addresses).

### Install date (used for recency)

First available, in order:

1. `Substantial_Completion` (actual)
2. `Actual_Installation_Date` (actual)
3. The earlier of `Inspection_Completed` / `Utility_PTO` (estimated; both happen after the install)
4. The **earliest** of Deal `Closing_Date`, Deal `Created_Time` and Install `Created_Time`, plus the median sale-to-completion lag (estimated). The lag is **measured from the data on every run** (92 days from 224 samples on 2026-10-22), not hard-coded.

It takes the earliest of those dates because bulk backfills (Feb–Mar 2025, Jun 2025) created Install records months or years after the sale, and the June 2025 batch stamped Deal `Closing_Date` with the backfill day.

### Zoho API gotchas

- **`GET /Installs` (list or by id) silently omits `Created_Time`**, even when it's named in `fields=`. Only `/Installs/search` returns it, and `Created_Time` itself isn't searchable, so search by stage instead. Search caps at 2000 results. Deals read with `?ids=` *do* return `Created_Time`.
- The repo's OAuth app only has `ZohoCRM.modules.ALL`: no `/coql`, no `/settings/fields`. Don't re-authorize it; the refresh token is shared with other jobs.
- **Every write sends `"trigger": []`** so Zoho workflows don't fire.
- **Double fields reject more decimal places than they're configured for**, and one bad value fails the whole 100-record batch (`INVALID_DATA`, `maximum_decimal_place`). Round coordinates to 6 decimals.
- A PUT or GET naming a field that doesn't exist returns **200 success** and silently drops it. Check that a field exists by GETting one record by id with no `fields` param.

### Addresses and geocoding

- Addresses come only from `Site_Location`, a free-text textarea with no separate street/city/zip fields and inconsistent formatting (commas, newlines, "CT.", "Connecticut", "USA").
- `_clean_address`: flattens newlines, strips "USA", fixes "CT.", and turns a hyphenated house-number range (`116-118`) into its first number.
- **Geocoder order:** US Census (free) first, then Google (`GOOGLE_GEOCODING_API_KEY`; Geocoding API only, GCP project `maps-to-zoho-form`).
- **What counts as a confident match:**
  - Census, one result: the house number and zip both match the input.
  - Census, several results: accepted only if **exactly one** has the same house number, zip and street name as the address as written. Example: `Five Fields Rd` and `Five Field Rd` in Madison are both real streets, about 0.2 mi apart. `11 Belden Street` matching only `Belden Hill Rd/Ln` stays unconfirmed.
  - Google: `ROOFTOP` or `RANGE_INTERPOLATED` only.
  - Anything weaker goes to the review queue and stays off the map.
- **Cache:** results are written back to Zoho on each Install: `Canvass_Lat`, `Canvass_Lng` (6 decimals), `Canvass_Town`, `Canvass_Geocode_Match` (e.g. `census:exact`, `google:ROOFTOP`, `failed`) and `Canvass_Address_Hash` (sha256 of the cleaned address). An address is re-geocoded only when its hash changes. Unconfirmed results are retried every run (about 20).
- **Every Install with an address is geocoded**, including pipeline and canceled jobs, not just installed ones.
- Census lags on new construction. Google placed all 7 CT addresses Census didn't know.

### Town = permitting jurisdiction (not the mailing city)

- The town comes from **point-in-polygon** against the official boundaries, never from the mailing city. Mailing names like Cos Cob, Amston, Winsted, Willimantic and Southport are villages inside other towns, and permits follow the town.
- **Boundary file:** Census TIGERweb "County Subdivisions" for STATE=09, simplified to about 30 m, with the 5 "not defined" water polygons removed. That leaves exactly 169 towns.
- A point within about 300 ft of a boundary snaps to the nearest town. Anything else outside CT becomes `Outside CT` and goes to review.

### Street names

`_street_of` strips the house number, cuts at the last street-type word, normalizes the suffix (Road→Rd, Street→St…), and handles ordinals and directionals. On one-line addresses it strips a trailing state and zip only when a zip is present. The result is that `25 SPELLMAN POINT ROAD` and `27 Spellman Point Rd.` group as the same street.

### Utility

Normalized from `Utility_Provider` to `Eversource` (anything containing "eversource" or "light & power") or `UI` ("illumin"). When blank, it takes the majority utility of the other installs in the same town.

## Zones

- **Per town:** DBSCAN on haversine distance. A small hand-written version (n is about 400; no sklearn).
  - `CANVASS_EPS_MI` = **0.75** (production; the code default is 0.4)
  - `CANVASS_MIN_SAMPLES` = 2
- **Shape:** each install is buffered by `CANVASS_RADIUS_MI` = **0.4** (production; default 0.25). The buffers are unioned per cluster and clipped to the town polygon. The radius has to be at least half the grouping distance (eps) or a zone splits into disconnected circles.
  - Geometry runs in a local equirectangular projection in miles (lat0 = 41.6°), which is under 1% off across CT, using shapely.
- **Seed vs cluster:** a single install becomes a "seed" zone; two or more make a "cluster".
- **Score:**
  - Formula: `1.0 × count + 2.0 × Σ 0.5^(age_days / 365) + 0.05 × installs per sq mi`.
  - Recency uses a 12-month half-life.
  - The weights are the env vars `CANVASS_W_COUNT`, `CANVASS_W_RECENCY`, `CANVASS_W_DENSITY` and `CANVASS_RECENCY_HALF_LIFE_MONTHS`.
- **Stable zone IDs:** a town code plus 4 hex characters of the sha1 of the zone's **oldest** install id, e.g. `MIL-FAD8`.
  - Anchoring on the oldest install means a new install joining a zone doesn't rename it (a centroid would drift every time).
  - Town codes are the shortest unique prefix, minimum 3 letters, of the uppercased town name: TRU, BETHA/BETHE/BETHL, NEWHAV.
- **Result as of 2026-10-22:** 265 zones from 394 installs at 0.75/0.4. At 0.4/0.25 it was 327 zones, 277 of them single-install.

## API (JSON the canvassing app could consume)

All routes are behind a session cookie (rep or admin):

- `GET /canvass/api/data` returns:
  ```json
  {
    "synced_at": "2026-10-22T15:28:53Z",
    "lag_days": 92,
    "config": {"eps_mi": 0.75, "min_samples": 2, "radius_mi": 0.4},
    "installs": [{"id": "<zoho id>", "lat": 41.16, "lng": -73.11, "town": "Stratford",
                  "address": "1095 Stratford Rd, Stratford, CT 06615", "street": "Stratford Rd",
                  "date": "2026-08-31", "month": "Aug 2026", "estimated": false,
                  "date_source": "completion", "utility": "UI", "zone": "STR-C20E"}],
    "zones": [{"id": "MIL-FAD8", "kind": "cluster", "town": "Milford", "utility": "UI",
               "count": 3, "latest": "2025-10-23", "area_sqmi": 0.6, "density": 5.0,
               "score": 6.47, "center": [41.22, -73.06], "streets": ["Clover St", "Vine St"],
               "geometry": {"type": "Polygon", "coordinates": [[...]]}}],
    "towns": {"type": "FeatureCollection", "features": [{"properties": {"town": "Milford", "code": "MIL", "installs": 31, "zones": ["MIL-FAD8"]}, "geometry": {...}}]},
    "notes": {"Milford": {"permit": "", "contact": "", "notes": "", "updated": "", "updated_by": ""}},
    "role": "rep"
  }
  ```
  The payload is about 660 KB raw, about 180 KB gzipped.
- `POST /canvass/api/towns/{town}/notes` with body `{permit, contact, notes}` saves to the Sheet.
- `POST /canvass/api/refresh` (admin) starts a sync in the background.
- `GET /canvass/api/admin` (admin) returns `{syncing, runs, review}`.

There is **no service-to-service token yet**. If the canvassing app should call this API from its server, add a bearer-token route. That's small.

## Auth

- **One shared rep password and one admin password**, in the env vars `CANVASS_REP_PASSWORD` and `CANVASS_ADMIN_PASSWORD`. There are no per-user accounts.
- **Session cookie:** an HMAC-signed `role|expiry` with `CANVASS_SESSION_SECRET`. HttpOnly, Secure, SameSite=Lax, 30 days, path `/canvass`.
- **Login throttle:** 10 tries per 15 minutes per IP.
- **No python-multipart dependency:** the login form is parsed by hand with `parse_qs`, because `request.form()` needs that package.

The canvassing app will want real per-rep accounts instead; this model only fits a single rep.

## Storage and ops

- **No database.** Geocodes live on the Zoho record. The processed result (installs, zones, towns) is kept **in memory** and rebuilt by a sync:
  - nightly at 06:00 UTC (APScheduler)
  - 20 seconds after every startup or deploy
  - on demand from the admin refresh button
- **Sync time:** about 20 seconds with a warm cache; about 4 minutes for a cold first run (geocoding 520 addresses).
- **Google Sheet** `1QhN0V_IXHjyaoBYCh0ZEgpZOFzyZKCQ0AETB62gQryc`, shared with the service account `zoho-calendar-bridge@install-calendar-access.iam.gserviceaccount.com`. Tabs:
  - `Town Notes`: rep-editable notes.
  - `Review`: rewritten every run with the records left off the map: out of state, geocode failed, low confidence, no address.
  - `Sync Log`: one row per run with counts, the measured lag, duration and errors. A run whose Zoho writes fail is marked `partial`.
- Writes to the Sheet always use an explicit row range, never `append()`. Checkbox validation in this Sheet once made `append()` land rows about 1,600 rows down.
- **Render:** 512 MB instances. It must be on a paid tier (Starter or above), because the free tier sleeps and scheduled jobs don't fire.

## Privacy decisions

- Pins show the full street address and install month. No names, prices or system details.
- Field guidance for the rep: **name the street, not the house**. Don't show the screen to homeowners. Don't screenshot or forward addresses. A customer is only named if they've agreed to be a reference; there's no Zoho flag for that yet.
- Out-of-state, canceled and pipeline records never reach the map, though they are geocoded and cached in Zoho.

## How the canvassing app can use this, cheapest first

1. **Read Zoho directly.** Every Install now carries `Canvass_Lat`, `Canvass_Lng` and `Canvass_Town`, refreshed nightly. Any app with Zoho read access gets install locations and permitting towns with no geocoding of its own. Apply the "installed" rule above to filter, and compute zones client-side or skip them.
2. **Call this service's API.** Add a service token and pull `/canvass/api/data` to get installs, scored zones, town polygons and notes ready-made.
3. **Lift the pure logic.** These functions in `canvass.py` depend only on `requests` and `shapely`, with no FastAPI, Zoho or Sheets:
   - `_clean_address`, `_street_of`, `_geocode_census`, `_geocode_google`, `_geocode`
   - `_town_for_point` (+ `data/ct_towns.geojson`), `_town_codes`
   - `_utility`, `_dbscan`, `_build_zones`, `_zone_id`, `_install_date` / `_calibrate_lag`

   Moving them into a standalone `geo.py` first would make them easy to copy or share.

The front end (`canvass_static/app.html`, Leaflet 1.9.4 + markercluster 1.5.3 from cdnjs, about 360 lines) is self-contained and can serve as a reference for the map UI: bottom-sheet panels, the list view, filters and the print layout.

## Open items

- **Oscar Orella** (`5264387000032297001`): the address has no street name, so it's off the map. Harry is coming back to it.
- **Pascal Fassa Kamano** (`5264387000031713001`): no address at all, off the map. Harry is coming back to it.
- 18 installs outside CT (MD, NJ, NY, DE, PA) are deliberately off the map. To add another state, make the state and boundary file configurable; today both are hard-coded constants (`STATE_CODE`, `BOUNDARY_FILE`).
- Ideas from the original spec that weren't built:
  - logging doors knocked per zone
  - creating Zoho Leads from the app, tagged with the zone ID
  - multiple reps, each with their own zone assignments
  - shading zones by days since last canvassed
  - a "referenceable customer" checkbox in Zoho
  - an OSM street list, and dropping commercial or apartment zones
