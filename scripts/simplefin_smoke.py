#!/usr/bin/env python3
"""
Smoke test: prove the 1Password -> Actions -> SimpleFIN -> Chase chain works.

Read-only by protocol; SimpleFIN cannot write to the bank.

Deliberately prints NO financial data -- no balances, no transaction amounts,
no descriptions, and never the access URL or its embedded credentials. CI logs
are not a place for bank data. It reports only shape: how many accounts, how
many transactions, and over what date range.
"""
import argparse
import datetime as dt
import os
import sys
from urllib.parse import urlsplit, urlunsplit

import requests

failures = []

ap = argparse.ArgumentParser()
ap.add_argument("--list-accounts", action="store_true",
                help="Print every account id and name so an allowlist can be built. "
                     "RUN THIS LOCALLY, never in CI -- it reveals personal account names.")
args = ap.parse_args()

# Allowlist of Helio business account ids. SimpleFIN also exposes Harry's
# personal accounts; nothing outside this list may ever enter the pipeline.
# Comma-separated in 1Password. Deliberately an ALLOWLIST, so a newly linked
# personal account is excluded by default rather than silently included.
ALLOWED = {a.strip() for a in (os.getenv("SIMPLEFIN_ACCOUNT_IDS") or "").split(",") if a.strip()}


def check(label, ok, detail=""):
    print(f"{'  OK  ' if ok else ' FAIL '} | {label}{(' | ' + detail) if detail else ''}")
    if not ok:
        failures.append(label)
    return ok


print("=" * 64)
print("SimpleFIN smoke test")
print("=" * 64)

raw = (os.getenv("SIMPLEFIN_ACCESS_URL") or "").strip()
if not check("SIMPLEFIN_ACCESS_URL present", bool(raw), f"{len(raw)} chars"):
    print("\nRESULT: FAIL (not set -- check the op:// path in the workflow)")
    sys.exit(1)

# Trailing '%' is a zsh end-of-line marker, not part of the URL. Strip defensively.
if raw.endswith("%"):
    raw = raw[:-1]
    print("  ..    | stripped a trailing '%' from the stored value")

parts = urlsplit(raw)
if not check("access URL parses with embedded credentials",
             bool(parts.username and parts.password and parts.hostname),
             f"host={parts.hostname}"):
    print("\nRESULT: FAIL (expected scheme://user:pass@host/path)")
    sys.exit(1)

auth = (parts.username, parts.password)
base = urlunsplit((parts.scheme, parts.hostname, parts.path, "", "")).rstrip("/")

# 1. Cheap connectivity probe -- balances only, no transactions.
r = requests.get(f"{base}/accounts", auth=auth, params={"balances-only": 1}, timeout=60)
if not check("GET /accounts (balances-only)", r.status_code == 200, f"HTTP {r.status_code}"):
    print(f"\nRESULT: FAIL ({r.text[:200]})")
    sys.exit(1)

data = r.json()
all_accounts = data.get("accounts", [])
check("SimpleFIN returned accounts", len(all_accounts) > 0, f"{len(all_accounts)} visible")

if args.list_accounts:
    print("\n  --list-accounts: run locally only. Copy the ids for Helio business")
    print("  accounts into 1Password as SIMPLEFIN_ACCOUNT_IDS (comma-separated).\n")
    for a in all_accounts:
        org = (a.get("org") or {}).get("name") or (a.get("org") or {}).get("domain") or "?"
        print(f"    {a.get('id')}   {org} / {a.get('name', '?')}")
    sys.exit(0)

if not ALLOWED:
    print("\nRESULT: FAIL -- SIMPLEFIN_ACCOUNT_IDS is not set.")
    print("SimpleFIN exposes personal accounts as well as Helio business accounts.")
    print("Run this locally with --list-accounts, then store the business account")
    print("ids in 1Password. Refusing to proceed without an explicit allowlist.")
    sys.exit(1)

accounts = [a for a in all_accounts if str(a.get("id")) in ALLOWED]
check("allowlisted accounts found", len(accounts) > 0,
      f"{len(accounts)} of {len(all_accounts)} visible")
unknown = ALLOWED - {str(a.get("id")) for a in all_accounts}
if unknown:
    check("every allowlisted id exists", False,
          f"{len(unknown)} id(s) in the allowlist are not visible in SimpleFIN")
for a in accounts:
    org = (a.get("org") or {}).get("name") or (a.get("org") or {}).get("domain") or "?"
    print(f"         - {org}: {a.get('name', '?')}")
    check(f"balance present for {a.get('name', '?')}", a.get("balance") is not None)

for err in data.get("errors", []) or []:
    print(f"  ..    | SimpleFIN reported: {err}")

# 2. Transactions over the last 30 days (max window is 90).
end = dt.date.today()
start = end - dt.timedelta(days=30)
r = requests.get(
    f"{base}/accounts",
    auth=auth,
    params={"start-date": int(dt.datetime.combine(start, dt.time()).timestamp()),
            "end-date": int(dt.datetime.combine(end, dt.time()).timestamp())},
    timeout=90,
)
if check("GET /accounts with a 30-day window", r.status_code == 200, f"HTTP {r.status_code}"):
    txns = []
    for a in r.json().get("accounts", []):
        if str(a.get("id")) not in ALLOWED:
            continue          # never read transactions from a non-allowlisted account
        txns.extend(a.get("transactions", []) or [])
    check("transactions returned", len(txns) > 0, f"{len(txns)} in last 30d")
    if txns:
        posted = sorted(t.get("posted", 0) for t in txns if t.get("posted"))
        if posted:
            lo = dt.datetime.fromtimestamp(posted[0]).date()
            hi = dt.datetime.fromtimestamp(posted[-1]).date()
            print(f"         date range: {lo} .. {hi}")
        fields = set()
        for t in txns[:50]:
            fields |= set(t.keys())
        print(f"         fields available: {', '.join(sorted(fields))}")

print("=" * 64)
if failures:
    print(f"RESULT: FAIL ({len(failures)}) -- " + "; ".join(failures))
    sys.exit(1)
print("RESULT: PASS -- SimpleFIN reachable, Chase transactions available")
