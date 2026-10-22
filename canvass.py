"""
Canvass map: where Helio has finished installs, grouped into walkable zones
that never cross a town line, served to a door-to-door rep behind a login.

Zoho stays the only source of truth. A nightly sync reads every Install,
geocodes each address once (cached on the record itself in the Canvass_*
fields), assigns the permitting town by point-in-polygon against the official
CT town boundaries (NOT the mailing city -- Cos Cob, Amston, Winsted etc. are
villages inside other towns), clusters installs within each town, and keeps
the processed result in memory. Nothing here writes to Zoho except the five
Canvass_* geocode-cache fields.

Wired into main.py via register(); see the "Canvass map" section of CLAUDE.md.
"""

import datetime
import hashlib
import hmac
import json
import logging
import math
import os
import re
import statistics
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, quote

import requests
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from shapely.geometry import Point, mapping, shape
from shapely.ops import transform, unary_union
from shapely.prepared import prep

logger = logging.getLogger(__name__)

router = APIRouter()

# ------------------------
# Config -- every tunable lives here or in an env var of the same name
# ------------------------
def _env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default

CONFIG = {
    # DBSCAN neighbourhood: ~10-minute walk. Suburban towns may want more.
    "eps_mi": _env_float("CANVASS_EPS_MI", 0.4),
    "min_samples": int(_env_float("CANVASS_MIN_SAMPLES", 2)),
    # Each install is buffered by this to draw the walkable zone.
    "radius_mi": _env_float("CANVASS_RADIUS_MI", 0.25),
    # score = w_count * installs + w_recency * sum(half-life decay) + w_density * installs/sq mi
    "w_count": _env_float("CANVASS_W_COUNT", 1.0),
    "w_recency": _env_float("CANVASS_W_RECENCY", 2.0),
    "w_density": _env_float("CANVASS_W_DENSITY", 0.05),
    "recency_half_life_months": _env_float("CANVASS_RECENCY_HALF_LIFE_MONTHS", 12),
    # Used only when too few records carry both a sale date and a completion date
    # to measure the sale-to-install lag from the data.
    "default_lag_days": 75,
    "min_lag_samples": 20,
}

STATE_CODE = "CT"
BOUNDARY_FILE = Path(__file__).parent / "data" / "ct_towns.geojson"

INSTALLED_STAGES = {"Inspection", "Witness Test / PTO", "Energized", "Project Closeout"}
DEAD_STAGES = {"Canceled"}

INSTALL_FIELDS = ",".join([
    "Project_Stage", "Substantial_Completion", "Actual_Installation_Date",
    "Inspection_Completed", "Utility_PTO", "Site_Location", "Utility_Provider",
    "Created_Time", "Opportunity",
    "Canvass_Lat", "Canvass_Lng", "Canvass_Town", "Canvass_Geocode_Match",
    "Canvass_Address_Hash",
])

# Our own placeholder values for Canvass_Town / Canvass_Geocode_Match.
OUTSIDE_STATE = f"Outside {STATE_CODE}"
GEOCODE_FAILED = "failed"

# Shared in-memory result. Rebuilt by every sync; the startup sync refills it
# after a deploy (geocodes are cached in Zoho, so that run is cheap).
_STATE = {
    "data": None,          # what /canvass/api/data serves
    "review": [],          # admin review queue
    "runs": [],            # sync log, newest first, last 30
    "notes": {},           # town -> notes dict
    "syncing": False,
}
_SYNC_LOCK = threading.Lock()

_deps = {}  # filled by register(): zoho token getter, sheets service builder


# ------------------------
# Geometry helpers -- local equirectangular projection in miles.
# CT spans ~1.5 deg of latitude, so distortion is well under 1% at this scale.
# ------------------------
_LAT0 = 41.6
_MI_PER_DEG_LAT = 69.0
_MI_PER_DEG_LON = 69.17 * math.cos(math.radians(_LAT0))


def _to_mi(lon, lat, z=None):
    return lon * _MI_PER_DEG_LON, lat * _MI_PER_DEG_LAT


def _to_deg(x, y, z=None):
    return x / _MI_PER_DEG_LON, y / _MI_PER_DEG_LAT


def _haversine_mi(lat1, lon1, lat2, lon2):
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _town_codes(names):
    """
    Shortest spaceless-uppercase prefix (min 3 chars) unique among all towns:
    Trumbull -> TRU, Bethany -> BETHA, Bethel -> BETHE, New Haven -> NEWHAV.
    Deterministic for a fixed boundary file, so zone IDs stay stable.
    """
    keys = {n: re.sub(r"[^A-Z]", "", n.upper()) for n in names}
    codes = {}
    for n, k in keys.items():
        length = 3
        while length < len(k) and any(o != n and keys[o].startswith(k[:length]) for o in keys):
            length += 1
        codes[n] = k[:length]
    return codes


_TOWNS = None  # loaded lazily: list of dicts {name, code, geom_deg, geom_mi, prepared}


def _load_towns():
    global _TOWNS
    if _TOWNS is not None:
        return _TOWNS
    gj = json.loads(BOUNDARY_FILE.read_text())
    names = [f["properties"]["town"] for f in gj["features"]]
    codes = _town_codes(names)
    towns = []
    for f in gj["features"]:
        g = shape(f["geometry"])
        if not g.is_valid:
            g = g.buffer(0)
        towns.append({
            "name": f["properties"]["town"],
            "code": codes[f["properties"]["town"]],
            "geom_deg": g,
            "geom_mi": transform(_to_mi, g),
            "prepared": prep(g),
        })
    _TOWNS = towns
    return towns


def _town_for_point(lat, lon):
    p = Point(lon, lat)
    for t in _load_towns():
        if t["prepared"].contains(p):
            return t["name"]
    # A point sitting exactly on a shared boundary or just offshore: take the
    # nearest town within ~300 ft rather than calling it out of state.
    best = min(_load_towns(), key=lambda t: t["geom_deg"].distance(p))
    if best["geom_deg"].distance(p) < 0.001:
        return best["name"]
    return None


# ------------------------
# Address handling
# ------------------------
_STATE_WORDS = {"CONNECTICUT": "CT", "NEW YORK": "NY", "NEW JERSEY": "NJ", "MARYLAND": "MD",
                "PENNSYLVANIA": "PA", "DELAWARE": "DE", "MASSACHUSETTS": "MA", "RHODE ISLAND": "RI"}


def _clean_address(raw):
    """Flatten Site_Location's free text into one geocodable line."""
    if not raw:
        return ""
    s = raw.replace("\n", ", ")
    s = re.sub(r",\s*USA\s*$", "", s, flags=re.I)
    s = re.sub(r"\b(CT|Ct|ct)\.", "CT", s)
    s = re.sub(r"\s+,", ",", s)
    s = re.sub(r",\s*,", ",", s)
    s = re.sub(r"\s{2,}", " ", s).strip(" ,.")
    # Hyphenated house-number range (116-118 Marion St): geocode the first number.
    s = re.sub(r"^(\d+)-\d+\b", r"\1", s)
    return s


def _address_hash(clean):
    return hashlib.sha256(clean.upper().encode()).hexdigest()


def _input_state(clean):
    up = clean.upper()
    for word, code in _STATE_WORDS.items():
        if re.search(rf"\b{word}\b", up):
            return code
    m = re.search(r"\b([A-Z]{2})\b[\s,]*\d{5}", up)
    return m.group(1) if m else None


_SUFFIXES = {
    "street": "St", "st": "St", "avenue": "Ave", "ave": "Ave", "av": "Ave", "road": "Rd", "rd": "Rd",
    "drive": "Dr", "dr": "Dr", "lane": "Ln", "ln": "Ln", "court": "Ct", "ct": "Ct", "place": "Pl",
    "pl": "Pl", "terrace": "Ter", "ter": "Ter", "circle": "Cir", "cir": "Cir", "boulevard": "Blvd",
    "blvd": "Blvd", "way": "Way", "trail": "Trl", "trl": "Trl", "turnpike": "Tpke", "tpke": "Tpke",
    "highway": "Hwy", "hwy": "Hwy", "parkway": "Pkwy", "pkwy": "Pkwy", "extension": "Ext", "ext": "Ext",
}


def _street_of(address):
    """Street name without house number, normalised so the same street groups
    together: '25 SPELLMAN POINT ROAD' and '27 Spellman Point Rd.' both give
    'Spellman Point Rd'. One-line addresses with no comma after the street
    ('1524 Bronson Rd Fairfield, CT') are cut at the last street-type word."""
    first = re.split(r"[,\n]", address or "")[0].strip()
    tokens = [t.strip(".") for t in re.sub(r"^[\d\-]+[A-Za-z]?\s+", "", first).split()]
    # Trailing state/zip on a one-line address ('Alden St. Stamford CT 06902'),
    # or 'CT' would be read as the street type Court.
    # Only when the street wasn't comma-separated, so '1 Elm Ct, Milford' keeps its Ct.
    # A bare 2-letter state is only stripped when a zip followed it ('1 Elm Ct' stays Ct).
    whole_line = not re.search(r"[,\n]", (address or "").strip())
    if whole_line and len(tokens) > 1 and re.fullmatch(r"\d{5}(-\d{4})?", tokens[-1]):
        tokens.pop()
        if len(tokens) > 1 and tokens[-1].upper() in set(_STATE_WORDS.values()):
            tokens.pop()
    if whole_line and len(tokens) > 1 and tokens[-1].upper() in _STATE_WORDS:
        tokens.pop()
    cut = max((i for i, t in enumerate(tokens) if i > 0 and t.lower() in _SUFFIXES), default=None)
    if cut is not None:
        tokens = tokens[:cut] + [_SUFFIXES[tokens[cut].lower()]]
    words = []
    for t in tokens:
        if re.fullmatch(r"\d+(st|nd|rd|th)", t, re.I):
            words.append(t.lower())
        elif t.upper() in ("N", "S", "E", "W", "NE", "NW", "SE", "SW") or t.upper().startswith("US-"):
            words.append(t.upper())
        else:
            words.append(t[:1].upper() + t[1:].lower())
    return " ".join(words)


def _display_address(raw):
    s = (raw or "").replace("\n", ", ")
    s = re.sub(r",\s*USA\s*$", "", s, flags=re.I)
    return re.sub(r"\s{2,}", " ", s).strip(" ,")


# ------------------------
# Geocoding: US Census first (free), Google fallback when a key is configured
# ------------------------
def _geocode_census(clean):
    url = ("https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
           f"?address={quote(clean)}&benchmark=Public_AR_Current&format=json")
    try:
        r = requests.get(url, timeout=30)
        matches = r.json().get("result", {}).get("addressMatches") or []
    except (requests.RequestException, ValueError):
        logger.warning("canvass: Census geocoder request failed")
        return None
    if not matches:
        return None
    m = matches[0]
    comp = m.get("addressComponents") or {}
    in_num = re.match(r"\s*(\d+)", clean)
    out_num = re.match(r"\s*(\d+)", m.get("matchedAddress") or "")
    in_zip = re.search(r"\b(\d{5})\b(?!.*\b\d{5}\b)", clean)
    same_num = bool(in_num and out_num and in_num.group(1) == out_num.group(1))
    same_zip = (not in_zip) or comp.get("zip") == in_zip.group(1)
    quality = "exact" if (same_num and same_zip and len(matches) == 1) else "partial"
    return {"lat": m["coordinates"]["y"], "lng": m["coordinates"]["x"],
            "match": f"census:{quality}", "good": quality == "exact"}


def _geocode_google(clean):
    key = os.getenv("GOOGLE_GEOCODING_API_KEY")
    if not key:
        return None
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json",
                         params={"address": clean, "key": key}, timeout=30)
        results = r.json().get("results") or []
    except (requests.RequestException, ValueError):
        logger.warning("canvass: Google geocoder request failed")
        return None
    if not results:
        return None
    g = results[0]["geometry"]
    loc_type = g.get("location_type", "")
    return {"lat": g["location"]["lat"], "lng": g["location"]["lng"],
            "match": f"google:{loc_type}",
            "good": loc_type in ("ROOFTOP", "RANGE_INTERPOLATED")}


def _geocode(clean):
    census = _geocode_census(clean)
    if census and census["good"]:
        return census
    google = _geocode_google(clean)
    if google and google["good"]:
        return google
    return census or google


def _is_good_match(match):
    return bool(match) and match in ("census:exact", "google:ROOFTOP", "google:RANGE_INTERPOLATED")


# ------------------------
# Zoho I/O (record scope only -- no /coql, no /settings; see CLAUDE.md)
# ------------------------
def _zoho_headers():
    token = _deps["get_zoho_access_token"]()
    if not token:
        raise RuntimeError("Zoho token unavailable")
    return {"Authorization": f"Zoho-oauthtoken {token}"}


def _api_domain():
    return os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")


def _fetch_installs():
    headers = _zoho_headers()
    out, page_token, page = [], None, 1
    while True:
        url = f"{_api_domain()}/crm/v7/Installs?fields={INSTALL_FIELDS}&per_page=200"
        url += f"&page_token={page_token}" if page_token else f"&page={page}"
        r = requests.get(url, headers=headers, timeout=60)
        if r.status_code == 204:
            break
        if r.status_code != 200:
            raise RuntimeError(f"Installs fetch failed status={r.status_code} body={r.text[:200]}")
        body = r.json()
        out.extend(body.get("data") or [])
        info = body.get("info") or {}
        if not info.get("more_records"):
            break
        page_token = info.get("next_page_token")
        page += 1
    created = _fetch_created_times()
    for rec in out:
        rec["Created_Time"] = created.get(rec["id"])
    return out


def _fetch_created_times():
    """Created_Time for installed-stage records. The list endpoint silently
    omits Created_Time even when asked for it (verified 2026-10-22); only
    /search returns it, and Created_Time itself isn't searchable, so search by
    stage. Search caps at 2000 results -- logged loudly if that's ever hit."""
    headers = _zoho_headers()
    criteria = "(" + "or".join(f"(Project_Stage:equals:{s})" for s in sorted(INSTALLED_STAGES)) + ")"
    out, page = {}, 1
    while True:
        r = requests.get(f"{_api_domain()}/crm/v7/Installs/search?criteria={quote(criteria)}"
                         f"&fields=Created_Time&per_page=200&page={page}", headers=headers, timeout=60)
        if r.status_code != 200:
            if r.status_code != 204:
                logger.warning(f"canvass: Created_Time search failed status={r.status_code} body={r.text[:200]}")
            break
        body = r.json()
        for d in body.get("data") or []:
            out[d["id"]] = d.get("Created_Time")
        if not (body.get("info") or {}).get("more_records"):
            break
        page += 1
        if page > 10:
            logger.error("canvass: Created_Time search hit the 2000-record search cap; older installs lose their fallback date")
            break
    return out


def _fetch_deals(deal_ids):
    headers = _zoho_headers()
    deals = {}
    ids = sorted(set(deal_ids))
    for i in range(0, len(ids), 100):
        chunk = ",".join(ids[i:i + 100])
        r = requests.get(f"{_api_domain()}/crm/v7/Deals?ids={chunk}&fields=Closing_Date,Created_Time",
                         headers=headers, timeout=60)
        if r.status_code == 200:
            for d in r.json().get("data") or []:
                deals[d["id"]] = d
        elif r.status_code != 204:
            logger.warning(f"canvass: Deals fetch failed status={r.status_code} body={r.text[:200]}")
    return deals


def _write_geocodes(updates):
    """PUT Canvass_* fields in batches of 100. Never fires workflows."""
    if not updates:
        return 0
    headers = {**_zoho_headers(), "Content-Type": "application/json"}
    written = 0
    for i in range(0, len(updates), 100):
        payload = {"data": updates[i:i + 100], "trigger": []}
        r = requests.put(f"{_api_domain()}/crm/v7/Installs", headers=headers,
                         data=json.dumps(payload), timeout=60)
        if r.status_code not in (200, 202):
            logger.error(f"canvass: geocode write failed status={r.status_code} body={r.text[:300]}")
            continue
        written += sum(1 for d in r.json().get("data") or [] if d.get("status") == "success")
    return written


# ------------------------
# Install date
# ------------------------
def _parse_date(v):
    if not v:
        return None
    try:
        return datetime.date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _sale_anchor(rec, deal):
    """Earliest of Deal Closing_Date, Deal Created_Time, Install Created_Time.

    Earliest, because the Feb-Mar 2025 and Jun 2025 backfills created Install
    records months or years after the sale, and the Jun 2025 batch even had the
    Deal's Closing_Date stamped with the backfill day. The earliest of the three
    is the one least likely to be a data-entry artifact."""
    candidates = [_parse_date(rec.get("Created_Time"))]
    if deal:
        candidates += [_parse_date(deal.get("Closing_Date")), _parse_date(deal.get("Created_Time"))]
    candidates = [c for c in candidates if c]
    return min(candidates) if candidates else None


def _calibrate_lag(records, deals):
    lags = []
    for rec in records:
        sc = _parse_date(rec.get("Substantial_Completion"))
        if not sc:
            continue
        anchor = _sale_anchor(rec, deals.get((rec.get("Opportunity") or {}).get("id")))
        if anchor and 0 <= (sc - anchor).days <= 365:
            lags.append((sc - anchor).days)
    if len(lags) < CONFIG["min_lag_samples"]:
        return CONFIG["default_lag_days"], len(lags)
    return int(statistics.median(lags)), len(lags)


def _install_date(rec, deal, lag_days, today):
    """Returns (date, estimated, source)."""
    sc = _parse_date(rec.get("Substantial_Completion"))
    if sc:
        return sc, False, "completion"
    actual = _parse_date(rec.get("Actual_Installation_Date"))
    if actual:
        return actual, False, "install date"
    # Inspection and PTO happen weeks AFTER the install, so either one bounds it.
    later = [d for d in (_parse_date(rec.get("Inspection_Completed")), _parse_date(rec.get("Utility_PTO"))) if d]
    if later:
        return min(later), True, "inspection/PTO"
    anchor = _sale_anchor(rec, deal)
    if anchor:
        return min(anchor + datetime.timedelta(days=lag_days), today), True, "sale + typical lag"
    return None, True, "unknown"


# ------------------------
# Utility normalisation
# ------------------------
def _utility(raw):
    s = (raw or "").lower()
    if "eversource" in s or "light & power" in s:
        return "Eversource"
    if "illumin" in s or s.strip() in ("ui", "uico"):
        return "UI"
    return None


# ------------------------
# Clustering: plain DBSCAN on haversine distance, per town (n is small)
# ------------------------
def _dbscan(points, eps_mi, min_samples):
    n = len(points)
    neighbors = [[j for j in range(n)
                  if _haversine_mi(points[i][0], points[i][1], points[j][0], points[j][1]) <= eps_mi]
                 for i in range(n)]
    labels = [None] * n
    cluster = -1
    for i in range(n):
        if labels[i] is not None:
            continue
        if len(neighbors[i]) < min_samples:
            labels[i] = -1
            continue
        cluster += 1
        labels[i] = cluster
        queue = list(neighbors[i])
        while queue:
            j = queue.pop()
            if labels[j] == -1:
                labels[j] = cluster
            if labels[j] is not None:
                continue
            labels[j] = cluster
            if len(neighbors[j]) >= min_samples:
                queue.extend(neighbors[j])
    return labels


def _zone_id(town_code, members, used):
    """Anchored on the oldest install in the zone, so the ID survives new
    installs joining it (a centroid would drift every time one did)."""
    anchor = min(members, key=lambda m: (m["date"] or datetime.date.max, m["id"]))
    h = hashlib.sha1(anchor["id"].encode()).hexdigest()
    length = 4
    while f"{town_code}-{h[:length]}".upper() in used:
        length += 1
    zid = f"{town_code}-{h[:length]}".upper()
    used.add(zid)
    return zid


def _build_zones(installs, today):
    towns = {t["name"]: t for t in _load_towns()}
    by_town = {}
    for inst in installs:
        by_town.setdefault(inst["town"], []).append(inst)
    zones, used_ids = [], set()
    half_life_days = CONFIG["recency_half_life_months"] * 30.44
    for town_name, members in by_town.items():
        town = towns[town_name]
        labels = _dbscan([(m["lat"], m["lng"]) for m in members], CONFIG["eps_mi"], CONFIG["min_samples"])
        groups = {}
        for m, lab in zip(members, labels):
            key = ("c", lab) if lab >= 0 else ("s", m["id"])
            groups.setdefault(key, []).append(m)
        for (kind, _), group in groups.items():
            buffers = [Point(*_to_mi(m["lng"], m["lat"])).buffer(CONFIG["radius_mi"], 24) for m in group]
            poly_mi = unary_union(buffers).intersection(town["geom_mi"])
            if poly_mi.is_empty:
                continue
            area = poly_mi.area
            recency = sum(0.5 ** (max((today - m["date"]).days, 0) / half_life_days)
                          for m in group if m["date"])
            density = len(group) / area if area else 0
            score = (CONFIG["w_count"] * len(group) + CONFIG["w_recency"] * recency
                     + CONFIG["w_density"] * density)
            centroid = transform(_to_deg, poly_mi.centroid)
            dated = [m["date"] for m in group if m["date"]]
            zid = _zone_id(town["code"], group, used_ids)
            for m in group:
                m["zone"] = zid
            zones.append({
                "id": zid,
                "kind": "cluster" if kind == "c" else "seed",
                "town": town_name,
                "utility": _majority([m["utility"] for m in group]),
                "count": len(group),
                "latest": max(dated).isoformat() if dated else None,
                "area_sqmi": round(area, 3),
                "density": round(density, 1),
                "score": round(score, 2),
                "center": [round(centroid.y, 6), round(centroid.x, 6)],
                "streets": sorted({m["street"] for m in group if m["street"]}),
                "geometry": mapping(transform(_to_deg, poly_mi.simplify(0.005))),
            })
    zones.sort(key=lambda z: -z["score"])
    return zones


def _majority(values):
    vals = [v for v in values if v]
    return max(set(vals), key=vals.count) if vals else None


# ------------------------
# Town notes -- Google Sheet tab, editable by the rep from the app
# ------------------------
NOTES_TAB = "Town Notes"
NOTES_HEADERS = ["Town", "Solicitor permit", "Permit office contact", "Notes", "Updated", "Updated By"]
REVIEW_TAB = "Review"
LOG_TAB = "Sync Log"


def _sheet():
    sheet_id = os.getenv("CANVASS_SHEET_ID")
    svc = _deps["build_sheets_service"]() if sheet_id else None
    return (svc, sheet_id) if svc else (None, None)


def _ensure_tab(svc, sheet_id, title, headers):
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id, fields="sheets.properties.title").execute()
    if title not in {s["properties"]["title"] for s in meta.get("sheets", [])}:
        svc.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body={
            "requests": [{"addSheet": {"properties": {"title": title}}}]}).execute()
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"'{title}'!A1", valueInputOption="RAW",
            body={"values": [headers]}).execute()


def _load_notes():
    svc, sheet_id = _sheet()
    if not svc:
        return
    try:
        _ensure_tab(svc, sheet_id, NOTES_TAB, NOTES_HEADERS)
        rows = svc.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=f"'{NOTES_TAB}'!A2:F").execute().get("values", [])
    except Exception:
        logger.exception("canvass: reading town notes failed")
        return
    notes = {}
    for row in rows:
        row = row + [""] * (6 - len(row))
        if row[0]:
            notes[row[0]] = {"permit": row[1], "contact": row[2], "notes": row[3],
                             "updated": row[4], "updated_by": row[5]}
    _STATE["notes"] = notes


def _save_note(town, note):
    svc, sheet_id = _sheet()
    if not svc:
        raise HTTPException(503, "Town notes sheet is not configured (CANVASS_SHEET_ID)")
    _ensure_tab(svc, sheet_id, NOTES_TAB, NOTES_HEADERS)
    rows = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"'{NOTES_TAB}'!A2:A").execute().get("values", [])
    names = [r[0] if r else "" for r in rows]
    values = [[town, note["permit"], note["contact"], note["notes"], note["updated"], note["updated_by"]]]
    # Explicit row range, never append(): see the Reconciliation-tab bug in CLAUDE.md.
    row = names.index(town) + 2 if town in names else len(names) + 2
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"'{NOTES_TAB}'!A{row}:F{row}",
        valueInputOption="RAW", body={"values": values}).execute()


def _write_sheet_reports(review, run):
    svc, sheet_id = _sheet()
    if not svc:
        return
    try:
        review_headers = ["Zoho ID", "Issue", "Stage", "Address", "Detail"]
        _ensure_tab(svc, sheet_id, REVIEW_TAB, review_headers)
        svc.spreadsheets().values().clear(spreadsheetId=sheet_id, range=f"'{REVIEW_TAB}'!A2:E").execute()
        if review:
            svc.spreadsheets().values().update(
                spreadsheetId=sheet_id, range=f"'{REVIEW_TAB}'!A2", valueInputOption="RAW",
                body={"values": [[r["id"], r["issue"], r["stage"], r["address"], r["detail"]] for r in review]},
            ).execute()
        log_headers = ["Run at (UTC)", "Status", "Installs read", "Installed", "On map", "Zones",
                       "Geocoded this run", "Review items", "Lag days (samples)", "Seconds", "Error"]
        _ensure_tab(svc, sheet_id, LOG_TAB, log_headers)
        rows = svc.spreadsheets().values().get(
            spreadsheetId=sheet_id, range=f"'{LOG_TAB}'!A2:A").execute().get("values", [])
        row = len(rows) + 2
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"'{LOG_TAB}'!A{row}:K{row}", valueInputOption="RAW",
            body={"values": [[run["at"], run["status"], run.get("read", ""), run.get("installed", ""),
                              run.get("on_map", ""), run.get("zones", ""), run.get("geocoded", ""),
                              run.get("review", ""), run.get("lag", ""), run.get("seconds", ""),
                              run.get("error", "")]]},
        ).execute()
    except Exception:
        logger.exception("canvass: writing review/log tabs failed")


# ------------------------
# The sync
# ------------------------
def run_sync():
    if not _SYNC_LOCK.acquire(blocking=False):
        logger.info("canvass: sync already running, skipping")
        return
    _STATE["syncing"] = True
    started = time.time()
    run = {"at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "status": "ok"}
    try:
        _sync(run)
    except Exception as e:
        logger.exception("canvass: sync failed")
        run["status"] = "failed"
        run["error"] = str(e)[:300]
    finally:
        run["seconds"] = round(time.time() - started, 1)
        _STATE["runs"] = ([run] + _STATE["runs"])[:30]
        _STATE["syncing"] = False
        _SYNC_LOCK.release()
        _write_sheet_reports(_STATE["review"], run)
        logger.info(f"canvass: sync {run}")


def _sync(run):
    today = datetime.date.today()
    records = _fetch_installs()
    run["read"] = len(records)
    deal_ids = [(r.get("Opportunity") or {}).get("id") for r in records]
    deals = _fetch_deals([d for d in deal_ids if d])
    lag_days, lag_n = _calibrate_lag(records, deals)
    run["lag"] = f"{lag_days} ({lag_n})"

    review, updates, installs = [], [], []
    installed_count = 0
    for rec in records:
        stage = rec.get("Project_Stage") or ""
        raw_addr = rec.get("Site_Location") or ""
        clean = _clean_address(raw_addr)
        installed = stage in INSTALLED_STAGES or (
            bool(rec.get("Substantial_Completion")) and stage not in DEAD_STAGES)
        if stage in DEAD_STAGES and rec.get("Substantial_Completion"):
            review.append(_review(rec, "Canceled but has completion date", ""))
        if rec.get("Substantial_Completion") and stage not in INSTALLED_STAGES | DEAD_STAGES:
            review.append(_review(rec, "Completion date but pre-install stage", "Counted as installed"))
        installed_count += installed

        # --- geocode (every record with an address, cached by address hash) ---
        lat, lng = rec.get("Canvass_Lat"), rec.get("Canvass_Lng")
        match, town = rec.get("Canvass_Geocode_Match"), rec.get("Canvass_Town")
        h = _address_hash(clean) if clean else None
        if clean and h != rec.get("Canvass_Address_Hash"):
            g = _geocode(clean)
            if g:
                # Canvass_Lat/Lng allow max 7 decimals; Census returns 12 and Zoho
                # rejects the whole 100-record batch over one (verified 2026-10-22).
                lat, lng, match = round(g["lat"], 6), round(g["lng"], 6), g["match"]
                town = _town_for_point(lat, lng) or OUTSIDE_STATE
            else:
                lat = lng = None
                match, town = GEOCODE_FAILED, None
            updates.append({"id": rec["id"], "Canvass_Lat": lat, "Canvass_Lng": lng,
                            "Canvass_Town": town, "Canvass_Geocode_Match": match,
                            "Canvass_Address_Hash": h})

        if not installed:
            continue
        if not clean:
            review.append(_review(rec, "No address", ""))
            continue
        if match == GEOCODE_FAILED or lat is None:
            review.append(_review(rec, "Geocode failed", match or ""))
            continue
        if town == OUTSIDE_STATE:
            st = _input_state(clean)
            review.append(_review(rec, f"Outside {STATE_CODE}", st or "state not parsed"))
            continue
        if not _is_good_match(match):
            review.append(_review(rec, "Low-confidence geocode", match))
            continue

        deal = deals.get((rec.get("Opportunity") or {}).get("id"))
        d, estimated, source = _install_date(rec, deal, lag_days, today)
        installs.append({
            "id": rec["id"],
            "lat": float(lat), "lng": float(lng),
            "town": town,
            "address": _display_address(raw_addr),
            "street": _street_of(raw_addr),
            "date": d, "estimated": estimated, "date_source": source,
            "utility": _utility(rec.get("Utility_Provider")),
        })

    run["geocoded"] = _write_geocodes(updates)
    if run["geocoded"] < len(updates):
        # Map still builds from this run's in-memory geocodes, but the cache didn't
        # land, so every later run re-geocodes these. Surface it, don't hide it.
        run["status"] = "partial"
        run["error"] = f"{len(updates) - run['geocoded']} of {len(updates)} geocode writes to Zoho failed"

    # Blank utility: take the majority among other installs in the same town.
    town_util = {}
    for i in installs:
        if i["utility"]:
            town_util.setdefault(i["town"], []).append(i["utility"])
    for i in installs:
        if not i["utility"]:
            i["utility"] = _majority(town_util.get(i["town"], []))
            i["utility_inferred"] = bool(i["utility"])

    zones = _build_zones(installs, today)
    run.update({"installed": installed_count, "on_map": len(installs), "zones": len(zones),
                "review": len(review)})

    counts = {}
    for i in installs:
        counts[i["town"]] = counts.get(i["town"], 0) + 1
    town_zones = {}
    for z in zones:
        town_zones.setdefault(z["town"], []).append(z["id"])

    _load_notes()
    _STATE["review"] = review
    _STATE["data"] = {
        "synced_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "lag_days": lag_days,
        "config": {k: CONFIG[k] for k in ("eps_mi", "min_samples", "radius_mi")},
        "installs": [{
            "id": i["id"], "lat": round(i["lat"], 6), "lng": round(i["lng"], 6),
            "town": i["town"], "address": i["address"], "street": i["street"],
            "date": i["date"].isoformat() if i["date"] else None,
            "month": i["date"].strftime("%b %Y") if i["date"] else None,
            "estimated": i["estimated"], "date_source": i["date_source"],
            "utility": i["utility"], "zone": i.get("zone"),
        } for i in installs],
        "zones": zones,
        "towns": {"type": "FeatureCollection", "features": [{
            "type": "Feature",
            "properties": {"town": t["name"], "code": t["code"], "installs": counts.get(t["name"], 0),
                           "zones": town_zones.get(t["name"], [])},
            "geometry": mapping(t["geom_deg"]),
        } for t in _load_towns()]},
    }


def _review(rec, issue, detail):
    return {"id": rec["id"], "issue": issue, "stage": rec.get("Project_Stage") or "",
            "address": _display_address(rec.get("Site_Location")), "detail": detail}


# ------------------------
# Auth: one rep password, one admin password, HMAC-signed cookie
# ------------------------
COOKIE = "canvass_session"
SESSION_DAYS = 30
_LOGIN_ATTEMPTS = {}  # ip -> [timestamps]


def _secret():
    s = os.getenv("CANVASS_SESSION_SECRET")
    if not s:
        raise HTTPException(503, "Canvass map is not configured (CANVASS_SESSION_SECRET)")
    return s.encode()


def _sign(role, expires):
    msg = f"{role}|{expires}"
    return msg + "|" + hmac.new(_secret(), msg.encode(), hashlib.sha256).hexdigest()


def _session_role(request):
    raw = request.cookies.get(COOKIE) or ""
    parts = raw.split("|")
    if len(parts) != 3:
        return None
    role, expires, sig = parts
    expected = hmac.new(_secret(), f"{role}|{expires}".encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected) or not expires.isdigit() or int(expires) < time.time():
        return None
    return role if role in ("rep", "admin") else None


def _require(request, admin=False):
    role = _session_role(request)
    if not role or (admin and role != "admin"):
        raise HTTPException(401, "Not signed in")
    return role


@router.post("/canvass/login")
async def canvass_login(request: Request):
    ip = request.client.host if request.client else "?"
    recent = [t for t in _LOGIN_ATTEMPTS.get(ip, []) if t > time.time() - 900]
    if len(recent) >= 10:
        raise HTTPException(429, "Too many attempts, try again in 15 minutes")
    _LOGIN_ATTEMPTS[ip] = recent + [time.time()]
    # Parsed by hand: request.form() needs python-multipart, not otherwise a dependency.
    form = parse_qs((await request.body()).decode("utf-8", "replace"))
    pw = (form.get("password") or [""])[0]
    role = None
    for name, r in (("CANVASS_ADMIN_PASSWORD", "admin"), ("CANVASS_REP_PASSWORD", "rep")):
        expected = os.getenv(name)
        if expected and hmac.compare_digest(pw.encode(), expected.encode()):
            role = r
            break
    if not role:
        return HTMLResponse(_page("login", error="Wrong password"), status_code=401)
    expires = int(time.time()) + SESSION_DAYS * 86400
    resp = RedirectResponse("/canvass", status_code=303)
    resp.set_cookie(COOKIE, _sign(role, expires), max_age=SESSION_DAYS * 86400,
                    httponly=True, secure=True, samesite="lax", path="/canvass")
    return resp


@router.get("/canvass/logout")
async def canvass_logout():
    resp = RedirectResponse("/canvass", status_code=303)
    resp.delete_cookie(COOKIE, path="/canvass")
    return resp


# ------------------------
# Pages and API
# ------------------------
_STATIC = Path(__file__).parent / "canvass_static"


def _page(name, **subs):
    html = (_STATIC / f"{name}.html").read_text()
    for k, v in subs.items():
        html = html.replace("{{" + k + "}}", v)
    return html.replace("{{error}}", "")


@router.get("/canvass", response_class=HTMLResponse)
async def canvass_app(request: Request):
    role = _session_role(request)
    if not role:
        return HTMLResponse(_page("login"))
    return HTMLResponse(_page("app", role=role))


@router.get("/canvass/api/data")
async def canvass_data(request: Request):
    role = _require(request)
    data = _STATE["data"]
    if data is None:
        return JSONResponse({"loading": True, "syncing": _STATE["syncing"]}, status_code=503)
    return {**data, "notes": _STATE["notes"], "role": role}


@router.post("/canvass/api/towns/{town}/notes")
async def canvass_save_note(town: str, request: Request):
    role = _require(request)
    if town not in {t["name"] for t in _load_towns()}:
        raise HTTPException(404, "Unknown town")
    body = await request.json()
    note = {k: str(body.get(k) or "")[:2000] for k in ("permit", "contact", "notes")}
    note["updated"] = datetime.date.today().isoformat()
    note["updated_by"] = role
    _save_note(town, note)
    _STATE["notes"][town] = note
    return note


@router.post("/canvass/api/refresh")
async def canvass_refresh(request: Request):
    _require(request, admin=True)
    if _STATE["syncing"]:
        return {"status": "already running"}
    threading.Thread(target=run_sync, daemon=True).start()
    return {"status": "started"}


@router.get("/canvass/api/admin")
async def canvass_admin(request: Request):
    _require(request, admin=True)
    return {"syncing": _STATE["syncing"], "runs": _STATE["runs"], "review": _STATE["review"]}


def register(app, scheduler, get_zoho_access_token, build_sheets_service):
    """Called once from main.py."""
    _deps["get_zoho_access_token"] = get_zoho_access_token
    _deps["build_sheets_service"] = build_sheets_service
    app.include_router(router)
    # 06:00 UTC = 1-2am ET, clear of the 07:00 UTC pipeline and the 17:00/18:00 PowerClerk jobs.
    scheduler.add_job(run_sync, "cron", hour=6, minute=0, id="canvass_sync", replace_existing=True)
    # Refill the in-memory result after every deploy/restart.
    scheduler.add_job(run_sync, "date", run_date=datetime.datetime.now() + datetime.timedelta(seconds=20),
                      id="canvass_startup_sync", replace_existing=True)
