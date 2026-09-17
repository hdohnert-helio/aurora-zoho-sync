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
    "corrections required",
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
    fields = ("id,Name,Site_Location,IC_Project_Number,Project_Stage,"
              "IC_Portal_Status,IC_Portal_Status_Date,IC_Action_Required,IC_Alert_Sent")

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
        "skip_feature_execution": ["assignment_rules", "connected_workflows"],
    }
    r = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=zoho_headers(token),
                      json=body, timeout=30)
    r.raise_for_status()
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
    r"\b(FL|FLOOR|UNIT|APT|APARTMENT|BLDG|BUILDING|BSMT|BASEMENT|LOT|STE|SUITE)\b\.?\s*[\w-]*",
    re.I,
)
_DIRECTIONAL_RE = re.compile(r"^\s*(NE|NW|SE|SW|N|S|E|W|NORTH|SOUTH|EAST|WEST)\b\.?\s+", re.I)
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
    s = _UNIT_RE.sub("", s)
    s = _DIRECTIONAL_RE.sub("", s)
    tokens = s.split()
    out = []
    for i, tok in enumerate(tokens):
        # keep a hyphenated house-number range (e.g. "28-30") as one token --
        # naive parsing otherwise splits it and leaks "30" into the street name.
        if i == 0 and re.match(r"^\d+-\d+$", tok):
            out.append(tok)
            continue
        out.append(_STREET_TYPE_MAP.get(tok, tok))
    return " ".join(t for t in out if t)


def normalize_city(c):
    return re.sub(r"\s+", " ", c.upper().strip()) if c else ""


def split_site_location(loc):
    """Site_Location is free text; assume 'street, city[, state zip]'."""
    parts = [p.strip() for p in (loc or "").split(",") if p.strip()]
    street = parts[0] if parts else ""
    city = parts[1] if len(parts) > 1 else ""
    return street, city


def build_portal_index(rows):
    idx = {}
    for row in rows:
        key = (normalize_street(row.get("street", "")), normalize_city(row.get("city", "")))
        idx.setdefault(key, []).append(row)
    return idx


def match_install(install, portal_idx):
    street, city = split_site_location(install.get("Site_Location", ""))
    key = (normalize_street(street), normalize_city(city))
    candidates = portal_idx.get(key)
    return candidates[0] if candidates else None


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


def compute_fields(inst, portal_idx, checked_at):
    """Pure derivation: install + portal index -> the field dict this record
    would be updated with. No I/O, so --dry-run can call the exact same logic
    the real write path uses."""
    match = match_install(inst, portal_idx)
    fields = {"IC_Portal_Checked": checked_at}

    if match:
        status_text = match.get("status") or ""
        fields["IC_Portal_Status"] = status_text
        date = parse_status_date(match.get("status_at"))
        if date:
            fields["IC_Portal_Status_Date"] = date
        action_required = is_action_required(status_text)
    else:
        status_text = NO_APPLICATION_STATUS
        fields["IC_Portal_Status"] = status_text
        action_required = True

    fields["IC_Action_Required"] = action_required

    was_required = bool(inst.get("IC_Action_Required"))
    if was_required and not action_required and inst.get("IC_Alert_Sent"):
        fields["IC_Alert_Sent"] = None

    existing_num = (inst.get("IC_Project_Number") or "").strip()
    matched_num = (match or {}).get("project_no") or ""
    conflict = False
    if matched_num:
        if not existing_num:
            fields["IC_Project_Number"] = matched_num
        elif existing_num != matched_num:
            conflict = True
            fields["IC_Portal_Status"] = (
                f"{status_text} | CONFLICT: portal is {matched_num}, Zoho has {existing_num}"
            )

    return fields, bool(match), conflict


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

    token = get_zoho_token()
    installs = fetch_active_installs(token)
    print(f"{len(installs)} active installs in Zoho")

    checked_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    if args.dry_run:
        print("\n=== DRY RUN -- no Zoho writes ===")
        disagreements = 0
        no_match = 0
        for inst in installs:
            name = inst.get("Name", inst["id"])
            fields, matched, conflict = compute_fields(inst, portal_idx, checked_at)
            if not matched:
                no_match += 1

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

        print(f"\n{len(installs)} active installs, {no_match} with no portal match, "
              f"{disagreements} disagree with current Zoho values")
        return 0

    updated = no_match = conflicts = failed = 0
    for inst in installs:
        rid = inst["id"]
        name = inst.get("Name", rid)
        fields, matched, conflict = compute_fields(inst, portal_idx, checked_at)
        if not matched:
            no_match += 1
        if conflict:
            conflicts += 1

        try:
            update_install(rid, fields, token)
            updated += 1
        except Exception as e:
            failed += 1
            print(f"  FAIL updating {name}: {e}")

    print(f"done: {updated} updated, {no_match} no portal match, "
          f"{conflicts} project-number conflicts, {failed} write failures")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
