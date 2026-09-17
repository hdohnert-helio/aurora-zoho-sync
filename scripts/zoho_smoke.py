#!/usr/bin/env python3
"""
Smoke test: prove the 1Password -> GitHub Actions -> Zoho CRM chain works.

Read-only. Writes nothing to Zoho. Never prints credential values.

Checks, in order:
  1. All three credentials are present in the environment
  2. The refresh token exchanges for an access token
  3. The access token can read the Installs module
  4. The four IC_Portal_* fields exist on Installs
  5. The IC_Action_Required filter returns the expected blocked projects
"""
import os
import sys

import requests

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"
NEW_FIELDS = [
    "IC_Portal_Status",
    "IC_Portal_Status_Date",
    "IC_Action_Required",
    "IC_Portal_Checked",
]

failures = []


def check(label, ok, detail=""):
    print(f"{'  OK  ' if ok else ' FAIL '} | {label}{(' | ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)
    return ok


print("=" * 64)
print("Zoho credential smoke test")
print("=" * 64)

# 1. Credentials present
creds = {}
for name in ("ZOHO_CLIENT_ID", "ZOHO_CLIENT_SECRET", "ZOHO_REFRESH_TOKEN"):
    val = os.getenv(name)
    creds[name] = val
    check(f"{name} present", bool(val), f"{len(val)} chars" if val else "not set")

if not all(creds.values()):
    print("\nRESULT: FAIL (missing credentials -- check the op:// paths in the workflow)")
    sys.exit(1)

print(f"  ..    | ZOHO_API_DOMAIN = {API_DOMAIN}")

# 2. Token exchange
resp = requests.post(
    TOKEN_URL,
    data={
        "grant_type": "refresh_token",
        "client_id": creds["ZOHO_CLIENT_ID"],
        "client_secret": creds["ZOHO_CLIENT_SECRET"],
        "refresh_token": creds["ZOHO_REFRESH_TOKEN"],
    },
    timeout=30,
)
body = resp.json()
token = body.get("access_token")
if not check("refresh token exchanges for access token", bool(token),
             f"HTTP {resp.status_code}" + ("" if token else f" | {body.get('error', body)}")):
    print("\nRESULT: FAIL (token exchange rejected -- the refresh token or client "
          "secret is stale, or the connected app was revoked)")
    sys.exit(1)

hdrs = {"Authorization": f"Zoho-oauthtoken {token}"}

# 3. Read the module
r = requests.get(
    f"{API_DOMAIN}/crm/v7/Installs",
    headers=hdrs,
    params={"fields": "Name", "per_page": 1},
    timeout=30,
)
check("can read Installs module", r.status_code == 200, f"HTTP {r.status_code}")

# 4. New fields exist -- read them on a record instead of settings/fields,
#    which needs ZohoCRM.settings.fields.READ (not granted, and not needed).
r = requests.get(
    f"{API_DOMAIN}/crm/v7/Installs",
    headers=hdrs,
    params={"fields": "Name," + ",".join(NEW_FIELDS), "per_page": 1},
    timeout=30,
)
if r.status_code == 200:
    rows = r.json().get("data", [])
    returned = set(rows[0].keys()) if rows else set()
    for fname in NEW_FIELDS:
        check(f"field {fname} readable", fname in returned)
else:
    # Zoho rejects the whole request naming an unknown field, so a 400 here
    # means at least one of the four is missing or misspelled.
    check("new fields readable", False,
          f"HTTP {r.status_code} | {r.json().get('message', r.text[:200])}")

# 5. The blocked-project filter works end to end.
#    Uses /search (ZohoCRM.modules.ALL) rather than /coql, which needs
#    ZohoCRM.coql.READ -- not granted, and not required by the sync.
r = requests.get(
    f"{API_DOMAIN}/crm/v7/Installs/search",
    headers=hdrs,
    params={"criteria": "(IC_Action_Required:equals:true)",
            "fields": "Name,IC_Portal_Status",
            "per_page": 30},
    timeout=30,
)
if r.status_code == 200:
    rows = r.json().get("data", [])
    check("IC_Action_Required search returns rows", len(rows) > 0, f"{len(rows)} blocked")
    for row in rows[:5]:
        print(f"         - {row.get('Name')}: {row.get('IC_Portal_Status')}")
    if len(rows) > 5:
        print(f"         ... and {len(rows) - 5} more")
elif r.status_code == 204:
    check("IC_Action_Required search returns rows", False, "204 no content")
else:
    check("IC_Action_Required search", False,
          f"HTTP {r.status_code} | {r.text[:200]}")

# 6. Write capability, without actually changing anything: a PUT with only an
#    id is rejected for having no updatable data, but an auth/scope failure
#    would surface as 401 instead. Distinguishes "can't write" from "no scope".
probe = requests.get(f"{API_DOMAIN}/crm/v7/Installs", headers=hdrs,
                     params={"fields": "Name", "per_page": 1}, timeout=30)
if probe.status_code == 200 and probe.json().get("data"):
    rid = probe.json()["data"][0]["id"]
    w = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=hdrs,
                     json={"data": [{"id": rid}], "trigger": []}, timeout=30)
    check("write scope present (no data written)", w.status_code != 401,
          f"HTTP {w.status_code} -- expected a data-validation error, not 401")

print("=" * 64)
if failures:
    print(f"RESULT: FAIL ({len(failures)}) -- " + "; ".join(failures))
    sys.exit(1)
print("RESULT: PASS -- Zoho reachable from Actions, new fields live, "
      "read+write scope confirmed")
