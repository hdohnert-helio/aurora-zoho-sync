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

# 4. New fields exist
r = requests.get(
    f"{API_DOMAIN}/crm/v7/settings/fields",
    headers=hdrs,
    params={"module": "Installs"},
    timeout=30,
)
if r.status_code == 200:
    present = {f.get("api_name") for f in r.json().get("fields", [])}
    for fname in NEW_FIELDS:
        check(f"field {fname} exists", fname in present)
else:
    check("can read field metadata", False, f"HTTP {r.status_code}")

# 5. The blocked-project filter works end to end
r = requests.post(
    f"{API_DOMAIN}/crm/v7/coql",
    headers=hdrs,
    json={"select_query":
          "select Name, IC_Portal_Status from Installs "
          "where IC_Action_Required = true limit 30"},
    timeout=30,
)
if r.status_code == 200:
    rows = r.json().get("data", [])
    check("IC_Action_Required query returns rows", len(rows) > 0, f"{len(rows)} blocked")
    for row in rows[:5]:
        print(f"         - {row['Name']}: {row['IC_Portal_Status']}")
    if len(rows) > 5:
        print(f"         ... and {len(rows) - 5} more")
else:
    check("IC_Action_Required query", False, f"HTTP {r.status_code}")

print("=" * 64)
if failures:
    print(f"RESULT: FAIL ({len(failures)}) -- " + "; ".join(failures))
    sys.exit(1)
print("RESULT: PASS -- Zoho is reachable from Actions and the new fields are live")
