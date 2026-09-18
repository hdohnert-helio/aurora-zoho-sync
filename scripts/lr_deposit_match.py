#!/usr/bin/env python3
"""
Probe: can LightReach deposits in Chase be matched to projects automatically?

Compares the LightReach funding ledger (artifacts/lr_funding_transactions.csv)
against Chase transactions pulled from SimpleFIN, and tests two hypotheses:

  A) each project's payout lands as its own Chase deposit
  B) LightReach batches -- one Chase deposit per batch date, equal to the sum
     of every project paid in that batch

Read-only. Prints ONLY LightReach-related transactions and aggregate stats --
never a dump of the bank feed. Run locally or in CI; either way it must not
reveal unrelated transactions.
"""
import argparse
import csv
import datetime as dt
import os
import re
import sys
from collections import defaultdict
from urllib.parse import urlsplit, urlunsplit

import requests

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEDGER = os.path.join(HERE, "artifacts", "lr_funding_transactions.csv")

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=90, help="lookback window (max 90)")
ap.add_argument("--tolerance", type=float, default=0.01, help="amount match tolerance in dollars")
args = ap.parse_args()

ALLOWED = {a.strip() for a in (os.getenv("SIMPLEFIN_ACCOUNT_IDS") or "").split(",") if a.strip()}
if not ALLOWED:
    sys.exit("SIMPLEFIN_ACCOUNT_IDS not set -- refusing to read every account.")

raw = (os.getenv("SIMPLEFIN_ACCESS_URL") or "").strip().rstrip("%")
if not raw:
    sys.exit("SIMPLEFIN_ACCESS_URL not set.")
p = urlsplit(raw)
auth = (p.username, p.password)
base = urlunsplit((p.scheme, p.hostname, p.path, "", "")).rstrip("/")

# ---- expected deposits from the LightReach ledger ------------------------
def money(x):
    x = (x or "").replace("$", "").replace(",", "").strip()
    try:
        return float(x)
    except ValueError:
        return None

def batch_date(s):
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", (s or "").strip())
    return dt.date(int(m.group(3)), int(m.group(1)), int(m.group(2))) if m else None

end = dt.date.today()
start = end - dt.timedelta(days=args.days)

expected = []          # (batch_date, amount, project, payout_event)
for row in csv.DictReader(open(LEDGER)):
    b = batch_date(row["batch"])
    amt = money(row["amount"])
    if not b or amt is None or not (start <= b <= end):
        continue
    expected.append((b, amt, row["project_name"], row["payout_event"]))

by_batch = defaultdict(list)
for b, amt, proj, ev in expected:
    by_batch[b].append((amt, proj, ev))

print("=" * 72)
print(f"LightReach deposit matching -- last {args.days} days")
print("=" * 72)
print(f"ledger rows in window: {len(expected)} across {len(by_batch)} batch dates")

# ---- Chase transactions --------------------------------------------------
r = requests.get(base + "/accounts", auth=auth, timeout=120, params={
    "start-date": int(dt.datetime.combine(start - dt.timedelta(days=5), dt.time()).timestamp()),
    "end-date": int(dt.datetime.combine(end + dt.timedelta(days=1), dt.time()).timestamp()),
})
r.raise_for_status()

credits = []           # (date, amount, payee, description, account)
for a in r.json().get("accounts", []):
    if str(a.get("id")) not in ALLOWED:
        continue
    acct = a.get("name", "?")
    for t in a.get("transactions", []) or []:
        amt = float(t.get("amount", 0))
        if amt <= 0:
            continue   # deposits only
        d = dt.datetime.fromtimestamp(t.get("posted") or t.get("transacted_at") or 0).date()
        credits.append((d, amt, (t.get("payee") or "").strip(),
                        (t.get("description") or "").strip(), acct))
print(f"deposits in Chase (allowlisted accounts) in window: {len(credits)}")

def near(a, b):
    return abs(a - b) <= args.tolerance

def find(amount, on, slack=4):
    hits = []
    for d, amt, payee, desc, acct in credits:
        if near(amt, amount) and abs((d - on).days) <= slack:
            hits.append((d, amt, payee, desc, acct))
    return hits

# ---- hypothesis A: one deposit per project payout ------------------------
a_hit = a_miss = 0
payees = defaultdict(int)
for b, amt, proj, ev in expected:
    hits = find(amt, b)
    if hits:
        a_hit += 1
        for h in hits:
            payees[h[2] or "(no payee)"] += 1
    else:
        a_miss += 1
print(f"\nA) per-project deposits: {a_hit} matched, {a_miss} unmatched "
      f"({a_hit / max(1, len(expected)) * 100:.0f}%)")

# ---- hypothesis B: one deposit per batch, summed -------------------------
b_hit = b_miss = 0
misses_b = []
for b, items in sorted(by_batch.items()):
    total = round(sum(i[0] for i in items), 2)
    hits = find(total, b)
    if hits:
        b_hit += 1
        for h in hits:
            payees[h[2] or "(no payee)"] += 1
    else:
        b_miss += 1
        misses_b.append((b, total, len(items)))
print(f"B) batch-total deposits: {b_hit} matched, {b_miss} unmatched "
      f"({b_hit / max(1, len(by_batch)) * 100:.0f}%)")

if payees:
    print("\npayee strings on matched deposits:")
    for k, v in sorted(payees.items(), key=lambda x: -x[1]):
        print(f"   {v:4}x  {k}")

# ---- diagnose the misses ------------------------------------------------
PALMETTO = "palmetto"
palm = [c for c in credits if PALMETTO in (c[2] + " " + c[3]).lower()]
print(f"\nPalmetto deposits seen in Chase: {len(palm)}")

if misses_b:
    print("\nUnmatched batches -- nearest Palmetto deposit and the gap:")
    for b, total, n in misses_b:
        near_palm = sorted(
            [c for c in palm if abs((c[0] - b).days) <= 6],
            key=lambda c: abs((c[0] - b).days),
        )
        print(f"\n   batch {b}  expected {total:>12,.2f}  ({n} ledger rows)")
        if not near_palm:
            print("      no Palmetto deposit within 6 days")
            continue
        for d, amt, payee, desc, acct in near_palm[:3]:
            gap = amt - total
            print(f"      {d}  actual {amt:>12,.2f}  gap {gap:>+12,.2f}  [{acct}]")

# Palmetto deposits that no batch explains at all
explained = set()
for b, items in by_batch.items():
    total = round(sum(i[0] for i in items), 2)
    for h in find(total, b):
        explained.add((h[0], round(h[1], 2)))
orphans = [c for c in palm if (c[0], round(c[1], 2)) not in explained]
if orphans:
    print(f"\nPalmetto deposits not explained by any batch total ({len(orphans)}):")
    for d, amt, payee, desc, acct in sorted(orphans)[:15]:
        print(f"   {d}  {amt:>12,.2f}  [{acct}]")

print("\n" + "=" * 72)
print("Interpretation: whichever hypothesis scores higher is how LightReach pays.")
print("A high payee concentration means matching can key on text, not just amount.")
