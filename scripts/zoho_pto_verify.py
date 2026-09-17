"""Temporary: verify IC_PTO_Alert_Sent backfill actually persisted.

Reads a sample of the 12 backfilled installs directly by ID (not via
/search) and prints Utility_PTO + IC_PTO_Alert_Sent. Also checks the full
active-install set for any installs with Utility_PTO set but
IC_PTO_Alert_Sent still null (should be zero). Delete after use.
"""
import os
import sys

import requests

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
NAMES = [
    "Mohammed Alamgir | 28", "Bishuja Deb", "Mustaque Nabi", "Mohammed Noor | 19",
    "Elsa Ortiz", "Nasima Majid", "Justin Kane",
]


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


def main():
    token = get_token()

    print("=== Direct-by-ID verification of sampled backfilled records ===")
    for name in NAMES:
        r = requests.get(
            f"{API_DOMAIN}/crm/v7/Installs/search",
            headers=headers(token),
            params={"criteria": f"(Name:equals:{name})", "fields": "id"},
            timeout=30,
        )
        if r.status_code != 200 or not r.json().get("data"):
            print(f"  {name}: search failed HTTP {r.status_code}")
            continue
        rid = r.json()["data"][0]["id"]
        rr = requests.get(f"{API_DOMAIN}/crm/v7/Installs/{rid}", headers=headers(token), timeout=30)
        rec = rr.json().get("data", [{}])[0]
        print(f"  {name}: Utility_PTO={rec.get('Utility_PTO')!r}  "
              f"IC_PTO_Alert_Sent={rec.get('IC_PTO_Alert_Sent')!r}")

    print("\n=== Full-set check: any Utility_PTO set but IC_PTO_Alert_Sent still null? ===")
    criteria = "and".join(f"(Project_Stage:not_equal:{s})" for s in ["Canceled", "Project Closeout"])
    results, page = [], 1
    while True:
        r = requests.get(
            f"{API_DOMAIN}/crm/v7/Installs/search",
            headers=headers(token),
            params={"criteria": criteria, "fields": "id,Name,Utility_PTO,IC_PTO_Alert_Sent",
                    "page": page, "per_page": 200},
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

    missing = [i for i in results if (i.get("Utility_PTO") or "").strip() and not i.get("IC_PTO_Alert_Sent")]
    print(f"{len(results)} active installs checked via /search; {len(missing)} with PTO but unstamped")
    for i in missing:
        print(f"  UNSTAMPED: {i.get('Name')} -- Utility_PTO={i.get('Utility_PTO')!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
