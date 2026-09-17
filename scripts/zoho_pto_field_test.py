"""Temporary: diagnose why IC_PTO_Alert_Sent writes don't show up on read.

Finds Mohammed Alamgir | 28 by name, reads it directly by ID (bypassing
/search entirely, which is eventually-consistent), and prints exactly what's
stored for Utility_PTO and IC_PTO_Alert_Sent. Then does a controlled
write + immediate direct-ID read-back to see if the value round-trips at
all, and what happens with a plain date vs a full ISO datetime.
Read/write only on ONE record. Delete after use.
"""
import os
import sys

import requests

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")


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

    r = requests.get(
        f"{API_DOMAIN}/crm/v7/Installs/search",
        headers=headers(token),
        params={"criteria": "(Name:equals:Mohammed Alamgir | 28)", "fields": "id,Name"},
        timeout=30,
    )
    print(f"search HTTP {r.status_code}")
    data = r.json().get("data", [])
    if not data:
        print("not found via search"); return 1
    rid = data[0]["id"]
    print(f"id: {rid}")

    def direct_read(label):
        rr = requests.get(f"{API_DOMAIN}/crm/v7/Installs/{rid}", headers=headers(token), timeout=30)
        rec = rr.json().get("data", [{}])[0]
        print(f"[{label}] direct-by-ID read: HTTP {rr.status_code}")
        print(f"  Utility_PTO: {rec.get('Utility_PTO')!r}")
        print(f"  IC_PTO_Alert_Sent: {rec.get('IC_PTO_Alert_Sent')!r}")
        print(f"  'IC_PTO_Alert_Sent' key present in response: {'IC_PTO_Alert_Sent' in rec}")

    direct_read("before")

    # Try a plain date string first (matches Utility_PTO's own working format).
    test_date = "2026-01-01"
    w = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=headers(token),
                      json={"data": [{"id": rid, "IC_PTO_Alert_Sent": test_date}],
                            "trigger": []}, timeout=30)
    print(f"\nPUT plain date {test_date!r}: HTTP {w.status_code}")
    print(f"  response: {w.text[:500]}")
    direct_read("after plain-date write")

    # Now try a full ISO datetime, like IC_Alert_Sent uses successfully.
    import datetime
    test_dt = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    w2 = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=headers(token),
                       json={"data": [{"id": rid, "IC_PTO_Alert_Sent": test_dt}],
                             "trigger": []}, timeout=30)
    print(f"\nPUT full datetime {test_dt!r}: HTTP {w2.status_code}")
    print(f"  response: {w2.text[:500]}")
    direct_read("after datetime write")

    return 0


if __name__ == "__main__":
    sys.exit(main())
