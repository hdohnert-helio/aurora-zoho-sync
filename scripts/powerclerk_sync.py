"""PowerClerk -> Zoho interconnection status sync.

Scrapes both PowerClerk portals (reusing the login + GetProjectList3 replay
from powerclerk_audit.py), matches each active Zoho Install to a portal
project by address, and writes IC_Portal_Status, IC_Portal_Status_Date,
IC_Action_Required and IC_Portal_Checked. Fills IC_Project_Number only when
blank -- never overwrites it. Clears IC_Alert_Sent when a project goes from
blocked back to clear, so a future re-block alerts again.

This is the ONLY script that touches PowerClerk. It must stay off-hours:
every login evicts whoever is in the portal (see CLAUDE.md).

Writes to Zoho send "trigger": [] and skip assignment_rules /
connected_workflows, per CLAUDE.md's sync rules. Never overwrites a
non-empty IC_Project_Number -- a mismatch is recorded in IC_Portal_Status
instead of silently changed.

Matching (2026-09-17 revision): IC_Project_Number is checked first when
set -- it's an exact key the utility assigned, not a heuristic. Address
matching is a fallback for installs with no project number recorded yet.
A CT, non-commercial install that WAS matched before and now isn't is
treated as a matcher failure, not a portal change: the previous status
and IC_Action_Required are left untouched and it's logged for review,
rather than overwritten with "NO APPLICATION FOUND" -- "I can't find it"
and "the utility has no record" are different facts. Commercial installs
(Property_Type not a residential type) that have no portal match get
"Commercial - not tracked by this sync" / IC_Action_Required=false,
the same treatment as out-of-territory -- this only applies when there's
no match; a commercial install that DOES match syncs normally.

If the scrape itself fails (bad login, no rows), this exits before touching
Zoho at all: no stamps are written, so IC_Portal_Checked goes stale and
powerclerk_notify.py's staleness check is the signal that this broke.
"""
import argparse
import datetime
import os
import re
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from powerclerk_audit import scrape_all_projects

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

# Stages excluded from "active" -- matches the 49-project reconciliation baseline in CLAUDE.md.
INACTIVE_STAGES = ["Canceled", "Project Closeout"]

NO_APPLICATION_STATUS = "NO APPLICATION FOUND in UI or Eversource portal"

_ACTION_REQUIRED_SUBSTRINGS = [
    "corrections",
    "customer action required",
    "incomplete",
    "unsubmitted",
    "on hold",
    "awaiting meter fee",
]


# ── Zoho helpers ─────────────────────────────────────────────────────────────

def get_zoho_token():
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": os.environ["ZOHO_CLIENT_ID"],
            "client_secret": os.environ["ZOHO_CLIENT_SECRET"],
            "refresh_token": os.environ["ZOHO_REFRESH_TOKEN"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Zoho token exchange failed: {resp.json()}")
    return token


def zoho_headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"}


def fetch_active_installs(token):
    """All Installs not in a closed-out stage, via /search (coql is not in our OAuth scope)."""
    criteria = "and".join(f"(Project_Stage:not_equal:{s})" for s in INACTIVE_STAGES)
    fields = ("id,Name,Site_Location,IC_Project_Number,Project_Stage,Utility_Provider,"
              "Property_Type,IC_Portal_Status,IC_Portal_Status_Date,IC_Action_Required,IC_Alert_Sent")

    results, page = [], 1
    while True:
        r = requests.get(
            f"{API_DOMAIN}/crm/v7/Installs/search",
            headers=zoho_headers(token),
            params={"criteria": criteria, "fields": fields, "page": page, "per_page": 200},
            timeout=30,
        )
        if r.status_code == 204:
            break
        r.raise_for_status()
        body = r.json()
        results.extend(body.get("data", []))
        if not body.get("info", {}).get("more_records"):
            break
        page += 1
    return results


def update_install(record_id, fields, token):
    body = {
        "data": [{"id": record_id, **fields}],
        "trigger": [],
        "skip_feature_execution": [{"name": "assignment_rules"}, {"name": "connected_workflows"}],
    }
    r = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=zoho_headers(token),
                      json=body, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} | {r.text[:300]}")
    result = r.json().get("data", [{}])[0]
    if result.get("status") != "success":
        raise RuntimeError(f"Zoho rejected update: {result}")


# ── Address matching ─────────────────────────────────────────────────────────
# Match Zoho's free-text Site_Location to the portal's separate street/city
# columns. Heuristic, best-effort -- the 49-install reconciliation this
# maintains was done manually; an address that fails to normalise the same
# way on both sides just falls through to "no application found", which is
# the documented behavior for an unmatched active project, not a crash.

_UNIT_RE = re.compile(
    # Greedy to end-of-string: PowerClerk unit suffixes are often multiple
    # trailing words ("UNIT OWNERS MTR", "BLDG OWNERS MTR"), not just one.
    r"\b(FL|FLOOR|UNIT|APT|APARTMENT|BLDG|BUILDING|BSMT|BASEMENT|LOT|STE|SUITE)\b.*$",
    re.I,
)
_DIRECTIONALS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW",
                  "NORTH", "SOUTH", "EAST", "WEST"}
_STREET_TYPE_MAP = {
    "ST": "STREET", "AVE": "AVENUE", "AV": "AVENUE", "RD": "ROAD", "LN": "LANE",
    "DR": "DRIVE", "CT": "COURT", "PL": "PLACE", "BLVD": "BOULEVARD", "CIR": "CIRCLE",
    "TER": "TERRACE", "TERR": "TERRACE", "PKWY": "PARKWAY", "HWY": "HIGHWAY",
    "SQ": "SQUARE", "TPKE": "TURNPIKE", "EXT": "EXTENSION",
}


def normalize_street(s):
    if not s:
        return ""
    s = re.sub(r"[.,]", "", s.upper().strip())
    s = _UNIT_RE.sub("", s).strip()
    tokens = s.split()
    out = []
    for i, tok in enumerate(tokens):
        # keep a hyphenated house-number range (e.g. "28-30") as one token --
        # naive parsing otherwise splits it and leaks "30" into the street name.
        if i == 0 and re.match(r"^\d+-\d+$", tok):
            out.append(tok)
            continue
        # drop a directional wherever it falls -- "N Main St" and "40 W High
        # St" both put it in a different position relative to the house number.
        if tok in _DIRECTIONALS:
            continue
        out.append(_STREET_TYPE_MAP.get(tok, tok))
    return " ".join(t for t in out if t)


def normalize_city(c):
    return re.sub(r"\s+", " ", c.upper().strip()) if c else ""


# Site_Location is inconsistent: sometimes "street, city, state zip" (comma
# before city), sometimes "street city, state zip" (no comma before city --
# city is jammed onto the street, comma only appears before the state). A
# plain comma-split misreads the state as the city on the second form. So:
# strip country/zip/state off the end first, then split on a comma if one is
# left, else find which of the portal's own known city names the remainder
# ends with.
_ZIP_RE = re.compile(r"\d{5}(-\d{4})?$")
_COUNTRY_RE = re.compile(r"(united states|usa)$", re.I)
_STATE_RE = re.compile(r"(connecticut|ct|massachusetts|ma|new york|ny|rhode island|ri)$", re.I)
_STATE_ABBR = {
    "CONNECTICUT": "CT", "CT": "CT",
    "MASSACHUSETTS": "MA", "MA": "MA",
    "NEW YORK": "NY", "NY": "NY",
    "RHODE ISLAND": "RI", "RI": "RI",
}


def _strip_end(s, pattern):
    s = s.rstrip(", ")
    m = pattern.search(s)
    return s[:m.start()].rstrip(", ") if m else s


def _match_known_city_suffix(remainder, known_cities):
    upper = remainder.upper()
    for city in sorted(known_cities, key=len, reverse=True):
        cu = city.upper().strip()
        if not cu or not upper.endswith(cu):
            continue
        boundary = len(upper) - len(cu) - 1
        if boundary < 0 or not upper[boundary].isalnum():
            return remainder[: len(remainder) - len(city)].strip(), remainder[-len(city):].strip()
    return remainder.strip(), ""


def split_site_location(loc, known_cities=()):
    """Returns (street, city, state). state is a 2-letter code, or None if no
    recognizable state token was found at all (not the same as "found and it's
    not CT" -- see detect_territory)."""
    s = (loc or "").strip()
    s = _strip_end(s, _COUNTRY_RE)
    s = _strip_end(s, _ZIP_RE)

    state = None
    stripped = s.rstrip(", ")
    m = _STATE_RE.search(stripped)
    if m:
        state = _STATE_ABBR.get(m.group(0).upper())
        s = stripped[:m.start()].rstrip(", ")

    if "," in s:
        street, city = s.rsplit(",", 1)
        return street.strip(), city.strip(), state
    street, city = _match_known_city_suffix(s, known_cities)
    return street, city, state


def build_portal_index(rows):
    idx = {}
    for row in rows:
        key = (normalize_street(row.get("street", "")), normalize_city(row.get("city", "")))
        idx.setdefault(key, []).append(row)
    return idx


def build_portal_index_by_number(rows):
    idx = {}
    for row in rows:
        num = (row.get("project_no") or "").strip().upper()
        if num:
            idx.setdefault(num, row)  # first occurrence wins on a rare duplicate
    return idx


def known_cities_from(rows):
    return {row.get("city", "").strip() for row in rows if row.get("city", "").strip()}


def match_install(install, portal_idx, portal_by_number, known_cities):
    """IC_Project_Number first when set -- it's an exact key the utility
    assigned. Only fall back to address matching when it's blank; a number
    that's set but not found in this scrape is NOT retried by address (the
    number is authoritative, not a hint)."""
    existing_num = (install.get("IC_Project_Number") or "").strip().upper()
    if existing_num:
        return portal_by_number.get(existing_num)

    street, city, _ = split_site_location(install.get("Site_Location", ""), known_cities)
    key = (normalize_street(street), normalize_city(city))
    candidates = portal_idx.get(key)
    return candidates[0] if candidates else None


# ── Territory ────────────────────────────────────────────────────────────────
# The scrape only covers UI (CT) and Eversource-CT. An out-of-state install
# would show "no application found" on every run forever -- that's not a
# blocker, it's just not this sync's territory. Detect from the state parsed
# out of Site_Location; only fall back to Utility_Provider when the address
# didn't yield a recognizable state at all (not when it yielded one that
# happens not to be CT -- that's a real answer, not a parse failure).

IN_TERRITORY_STATE = "CT"
OUT_OF_TERRITORY_STATUS = "Outside UI/Eversource territory - not tracked by this sync"
_UTILITY_IN_TERRITORY_RE = re.compile(r"eversource|illuminating", re.I)


def is_in_territory(inst):
    _, _, state = split_site_location(inst.get("Site_Location", ""))
    if state:
        return state == IN_TERRITORY_STATE
    return bool(_UTILITY_IN_TERRITORY_RE.search(inst.get("Utility_Provider") or ""))


# ── Commercial ───────────────────────────────────────────────────────────────
# Commercial jobs follow a different interconnection process and often have
# no record in these two portals at all. Only matters when there's no portal
# match -- a commercial install that DOES match (e.g. Carl Guild, INT-117499)
# syncs completely normally.
#
# Signal used: Property_Type, a real-estate/parcel-data field already on
# Installs (not something matching the word "Commercial" in Name). Verified
# against 5 known records: residential installs all read 'SINGLE FAMILY
# RESIDENCE'; commercial ones read 'COMMERCIAL', 'OFFICE BUILDING', and
# 'EXEMPT' (a tax-exempt org, not literally the word "commercial" -- this is
# why an allowlist of commercial-sounding values would have missed it).
# Classify by ABSENCE of a residential marker rather than presence of a
# commercial one, since the commercial side has many more possible values.

COMMERCIAL_STATUS = "Commercial - not tracked by this sync"
_RESIDENTIAL_PROPERTY_TYPE_RE = re.compile(
    r"RESIDEN|FAMILY|CONDO|TOWNHOUSE|DUPLEX|TRIPLEX", re.I)


def is_commercial(inst):
    ptype = (inst.get("Property_Type") or "").strip()
    if not ptype:
        return False  # no signal either way -- don't guess commercial from silence
    return not _RESIDENTIAL_PROPERTY_TYPE_RE.search(ptype)


# ── Status rules ─────────────────────────────────────────────────────────────

def is_action_required(status_text):
    """CLAUDE.md's IC_Action_Required rule. Only called with a real portal status
    (the no-match case is handled by the caller, not this function)."""
    if not status_text:
        return True
    low = status_text.lower()
    if "re-review" in low:
        return False
    return any(sub in low for sub in _ACTION_REQUIRED_SUBSTRINGS)


def parse_status_date(s):
    """Portal dates render as MM/DD/YYYY."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


# Recognized placeholder statuses this sync itself writes when there's no
# real portal match. Used by the regression guard below to tell "this was
# already a placeholder" from "this was a real matched status".
_PLACEHOLDER_STATUSES = (NO_APPLICATION_STATUS, OUT_OF_TERRITORY_STATUS, COMMERCIAL_STATUS)


def compute_fields(inst, portal_idx, portal_by_number, known_cities, checked_at):
    """Pure derivation: install + portal indexes -> the field dict this record
    would be updated with. No I/O, so --dry-run can call the exact same logic
    the real write path uses.

    Returns (fields, matched, conflict, regression). `regression` means: this
    CT, non-commercial install had a real matched status before, has none
    now, and the previous status/action were deliberately left untouched
    pending review -- that's a matcher failure, not a portal change.
    """
    fields = {"IC_Portal_Checked": checked_at}
    matched = False
    conflict = False
    regression = False

    if not is_in_territory(inst):
        fields["IC_Portal_Status"] = OUT_OF_TERRITORY_STATUS
        action_required = False
    else:
        match = match_install(inst, portal_idx, portal_by_number, known_cities)
        matched = bool(match)

        if match:
            status_text = match.get("status") or ""
            fields["IC_Portal_Status"] = status_text
            date = parse_status_date(match.get("status_at"))
            if date:
                fields["IC_Portal_Status_Date"] = date
            action_required = is_action_required(status_text)

            existing_num = (inst.get("IC_Project_Number") or "").strip()
            matched_num = match.get("project_no") or ""
            if matched_num:
                if not existing_num:
                    fields["IC_Project_Number"] = matched_num
                elif existing_num != matched_num:
                    conflict = True
                    fields["IC_Portal_Status"] = (
                        f"{status_text} | CONFLICT: portal is {matched_num}, Zoho has {existing_num}"
                    )
        elif is_commercial(inst):
            fields["IC_Portal_Status"] = COMMERCIAL_STATUS
            action_required = False
        else:
            existing_status = inst.get("IC_Portal_Status") or ""
            if existing_status and existing_status not in _PLACEHOLDER_STATUSES:
                # Was a real matched status before; losing the match now is a
                # matcher failure, not evidence the utility dropped the
                # record. Keep it untouched and surface it for review instead.
                regression = True
                fields["IC_Portal_Status"] = existing_status
                action_required = bool(inst.get("IC_Action_Required"))
            else:
                fields["IC_Portal_Status"] = NO_APPLICATION_STATUS
                action_required = True

    fields["IC_Action_Required"] = action_required

    was_required = bool(inst.get("IC_Action_Required"))
    if was_required and not action_required and inst.get("IC_Alert_Sent"):
        fields["IC_Alert_Sent"] = None

    return fields, matched, conflict, regression


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                    help="Scrape and compute as normal, but write nothing to Zoho. "
                         "Diffs derived IC_Portal_Status/IC_Action_Required against "
                         "what's currently stored and reports disagreements.")
    return p.parse_args()


def main():
    args = parse_args()

    print("Scraping PowerClerk (UI + Eversource)...")
    portal_rows = scrape_all_projects()  # raises on login failure -- nothing gets stamped
    print(f"scraped {len(portal_rows)} portal projects")
    portal_idx = build_portal_index(portal_rows)
    portal_by_number = build_portal_index_by_number(portal_rows)
    known_cities = known_cities_from(portal_rows)

    token = get_zoho_token()
    installs = fetch_active_installs(token)
    print(f"{len(installs)} active installs in Zoho")

    checked_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    if args.dry_run:
        print("\n=== DRY RUN -- no Zoho writes ===")
        disagreements = 0
        no_match = 0
        out_of_territory = 0
        regressions = 0
        for inst in installs:
            name = inst.get("Name", inst["id"])
            in_territory = is_in_territory(inst)
            fields, matched, conflict, regression = compute_fields(
                inst, portal_idx, portal_by_number, known_cities, checked_at)
            if not in_territory:
                out_of_territory += 1
            elif not matched:
                no_match += 1
            if regression:
                regressions += 1
                print(f"REGRESSION (previously matched, no match now -- kept existing "
                      f"status untouched): {name}")
                print(f"    IC_Portal_Status stays: {inst.get('IC_Portal_Status')!r}")

            cur_status = inst.get("IC_Portal_Status") or ""
            cur_action = bool(inst.get("IC_Action_Required"))
            new_status = fields.get("IC_Portal_Status", cur_status)
            new_action = fields.get("IC_Action_Required", cur_action)

            if new_status != cur_status or new_action != cur_action:
                disagreements += 1
                print(f"DISAGREE: {name}")
                print(f"    IC_Portal_Status:    {cur_status!r} -> {new_status!r}")
                print(f"    IC_Action_Required:  {cur_action} -> {new_action}")
                if conflict:
                    print("    (project-number conflict, not overwritten)")
                if not matched and is_in_territory(inst) and not is_commercial(inst):
                    loc = inst.get("Site_Location") or ""
                    street, city, state = split_site_location(loc, known_cities)
                    key = (normalize_street(street), normalize_city(city))
                    print(f"    Site_Location: {loc!r}")
                    print(f"    parsed street/city/state: {street!r} / {city!r} / {state!r}")
                    print(f"    normalized key: {key!r}")
                    print(f"    key in portal index: {key in portal_idx}")

        print(f"\n{len(installs)} active installs, {no_match} with no portal match, "
              f"{out_of_territory} outside CT (not tracked), {regressions} matcher "
              f"regressions (kept untouched), {disagreements} disagree with current Zoho values")
        return 0

    # Fields worth reporting when they change. IC_Portal_Checked is excluded --
    # it changes on every run by design and isn't news.
    REPORT_FIELDS = ["IC_Portal_Status", "IC_Portal_Status_Date", "IC_Action_Required",
                     "IC_Project_Number", "IC_Alert_Sent"]

    updated = no_match = out_of_territory = conflicts = failed = changed_projects = regressions = 0
    for inst in installs:
        rid = inst["id"]
        name = inst.get("Name", rid)
        fields, matched, conflict, regression = compute_fields(
            inst, portal_idx, portal_by_number, known_cities, checked_at)
        if not is_in_territory(inst):
            out_of_territory += 1
        elif not matched:
            no_match += 1
        if conflict:
            conflicts += 1
        if regression:
            regressions += 1
            print(f"REGRESSION (previously matched, no match now -- kept existing "
                  f"status untouched, needs review): {name}")
            print(f"    IC_Portal_Status stays: {inst.get('IC_Portal_Status')!r}")

        changes = [(f, inst.get(f), fields[f]) for f in REPORT_FIELDS
                   if f in fields and inst.get(f) != fields[f]]

        try:
            update_install(rid, fields, token)
            updated += 1
            if changes:
                changed_projects += 1
                print(f"CHANGED: {name}")
                for f, old, new in changes:
                    print(f"    {f}: {old!r} -> {new!r}")
        except Exception as e:
            failed += 1
            print(f"  FAIL updating {name}: {e}")

    print(f"done: {updated} updated ({changed_projects} with a real field change), "
          f"{no_match} no portal match, {out_of_territory} outside CT (not tracked), "
          f"{regressions} matcher regressions (kept untouched), "
          f"{conflicts} project-number conflicts, {failed} write failures")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
