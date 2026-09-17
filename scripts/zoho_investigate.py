"""Temporary: find a reliable commercial-vs-residential field on Installs.

Fetches full records (no `fields` param -> Zoho returns everything) for a
few known commercial and residential installs and prints them side by side
so a real distinguishing field can be picked instead of matching on the
word "Commercial" in the Name. Read-only. Delete after use.
"""
import os
import sys

import requests

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")

COMMERCIAL_NAMES = ["GVFC ASnell (Commercial)", "Alex Starr | Commercial", "Carl Guild | Commercial"]
RESIDENTIAL_NAMES = ["Kari Webb", "Mohammad Gafur", "Jan Berman"]


def get_token():
    resp = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
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
        raise RuntimeError(f"token exchange failed: {resp.json()}")
    return token


def headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}", "Content-Type": "application/json"}


def find_id_by_name(token, name):
    r = requests.get(
        f"{API_DOMAIN}/crm/v7/Installs/search",
        headers=headers(token),
        params={"criteria": f"(Name:equals:{name})", "fields": "id,Name"},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"  search failed for {name!r}: HTTP {r.status_code} | {r.text[:200]}")
        return None
    data = r.json().get("data", [])
    return data[0]["id"] if data else None


def dump_full_record(token, record_id, label):
    r = requests.get(f"{API_DOMAIN}/crm/v7/Installs/{record_id}", headers=headers(token), timeout=30)
    if r.status_code != 200:
        print(f"  full-record fetch failed for {label}: HTTP {r.status_code} | {r.text[:200]}")
        return {}
    data = r.json().get("data", [{}])[0]
    return data


def main():
    token = get_token()

    print("=== GVFC ASnell (name search failed earlier -- parens break Zoho criteria) ===")
    r = requests.get(
        f"{API_DOMAIN}/crm/v7/Installs/search",
        headers=headers(token),
        params={"criteria": "(Name:starts_with:GVFC)", "fields": "id,Name"},
        timeout=30,
    )
    if r.status_code == 200 and r.json().get("data"):
        rid = r.json()["data"][0]["id"]
        rec = dump_full_record(token, rid, "GVFC ASnell")
        for k in ("Name", "Property_Type", "IC_Project_Number", "Site_Location",
                  "IC_Portal_Status", "IC_Action_Required"):
            print(f"  {k}: {rec.get(k)!r}")
    else:
        print(f"  not found: HTTP {r.status_code} | {r.text[:200]}")
    print()

    print("=== Commercial installs ===")
    commercial_records = {}
    for name in COMMERCIAL_NAMES:
        rid = find_id_by_name(token, name)
        if not rid:
            print(f"  {name!r}: not found")
            continue
        rec = dump_full_record(token, rid, name)
        commercial_records[name] = rec
        print(f"  {name!r} ({rid}): {len(rec)} fields")

    print("\n=== Residential installs ===")
    residential_records = {}
    for name in RESIDENTIAL_NAMES:
        rid = find_id_by_name(token, name)
        if not rid:
            print(f"  {name!r}: not found")
            continue
        rec = dump_full_record(token, rid, name)
        residential_records[name] = rec
        print(f"  {name!r} ({rid}): {len(rec)} fields")

    all_keys = set()
    for rec in list(commercial_records.values()) + list(residential_records.values()):
        all_keys.update(rec.keys())

    print(f"\n=== Field-by-field comparison ({len(all_keys)} total keys) ===")
    print("(showing only keys whose values look 'type'/'segment'/'category'-ish, "
          "or where commercial and residential values differ)")
    for k in sorted(all_keys):
        c_vals = {name: rec.get(k) for name, rec in commercial_records.items()}
        r_vals = {name: rec.get(k) for name, rec in residential_records.items()}
        interesting_name = any(w in k.lower() for w in
                                ("type", "segment", "categ", "commercial", "resid", "market", "sector", "class"))
        c_set = {str(v) for v in c_vals.values()}
        r_set = {str(v) for v in r_vals.values()}
        differs = bool(c_set) and bool(r_set) and not (c_set & r_set)
        if interesting_name or differs:
            print(f"\n  {k}:")
            print(f"    commercial:  {c_vals}")
            print(f"    residential: {r_vals}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
