#!/usr/bin/env python3
"""
Bank reconciliation -- propose step.

Pulls Chase transactions (via SimpleFIN, both Helio business accounts) and
proposes a match against the Cashflow Google Sheet's current Revenue
(un-received) and Expenses (Active) rows, then POSTs the proposals to Render,
which writes them into the sheet's Reconciliation tab. See CLAUDE.md's "Chase
bank reconciliation" section for the full design.

Credential split, deliberate: this script holds SimpleFIN credentials and
never touches Google Sheets directly; Render holds Sheets credentials and
never touches SimpleFIN. Matches the LightReach deposit-matching precedent
(scripts/lr_deposit_match.py) rather than widening either side's exposure.

Read-only against Chase/SimpleFIN. Writes only the computed match proposals
(dates, amounts, account labels, a short proposed-match string) to Render --
never balances, full descriptions, or anything from a non-allowlisted
account. Prints only aggregate counts, never per-transaction detail, to the
CI log.
"""
import argparse
import datetime as dt
import os
import sys
from urllib.parse import urlsplit, urlunsplit

import requests

RENDER_BASE = os.environ.get("RENDER_BASE_URL", "https://aurora-zoho-sync.onrender.com")

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=45, help="lookback window (max 90)")
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

end = dt.date.today()
start = end - dt.timedelta(days=args.days)

# ---- pull Chase transactions (allowlisted accounts only) -----------------
r = requests.get(base + "/accounts", auth=auth, timeout=120, params={
    "start-date": int(dt.datetime.combine(start, dt.time()).timestamp()),
    "end-date": int(dt.datetime.combine(end + dt.timedelta(days=1), dt.time()).timestamp()),
})
r.raise_for_status()

txns = []  # (date, amount, account_name, payee, description)
for a in r.json().get("accounts", []):
    if str(a.get("id")) not in ALLOWED:
        continue
    acct_name = a.get("name", "?")
    for t in a.get("transactions", []) or []:
        amt = float(t.get("amount", 0))
        if amt == 0:
            continue
        d = dt.datetime.fromtimestamp(t.get("posted") or t.get("transacted_at") or 0).date()
        txns.append((d, amt, acct_name, (t.get("payee") or "").strip(), (t.get("description") or "").strip()))

print(f"Chase transactions pulled (allowlisted accounts, last {args.days}d): {len(txns)}")

# ---- pull current Revenue/Expenses match candidates from Render ----------
resp = requests.get(f"{RENDER_BASE}/internal/cashflow-snapshot", timeout=60)
resp.raise_for_status()
snapshot = resp.json()
if snapshot.get("status") != "ok":
    sys.exit(f"cashflow-snapshot failed: {snapshot}")

revenue_candidates = snapshot["revenue_candidates"]
expense_candidates = snapshot["expense_candidates"]
print(f"Revenue candidates (not yet Received): {len(revenue_candidates)}")
print(f"Expense candidates (Active): {len(expense_candidates)}")


def near(a, b, tol=args.tolerance):
    return abs(a - b) <= tol


def find_best_match(amount, txn_date, candidates, used):
    """Exact-amount match within [week_monday - 3d, week_monday + 10d].
    On multiple ties, picks the one whose window start is closest to the
    transaction date. Marks the chosen candidate used so it can't be
    double-matched against a second transaction."""
    best = None
    best_gap = None
    for i, c in enumerate(candidates):
        if i in used:
            continue
        if not near(amount, c["amount"]):
            continue
        week_monday = dt.date.fromisoformat(c["week_monday"])
        window_start = week_monday - dt.timedelta(days=3)
        window_end = week_monday + dt.timedelta(days=10)
        if not (window_start <= txn_date <= window_end):
            continue
        gap = abs((txn_date - week_monday).days)
        if best is None or gap < best_gap:
            best, best_gap = i, gap
    return best


PALMETTO_PAYEE_HINT = "palmetto"

rev_used, exp_used = set(), set()
proposals = []
matched_rev = matched_exp = palmetto = unmatched = 0

for d, amt, acct_name, payee, desc in txns:
    haystack = f"{payee} {desc}".lower()
    direction = "In" if amt > 0 else "Out"

    if PALMETTO_PAYEE_HINT in haystack:
        palmetto += 1
        proposed, basis, match_key = (
            "LightReach batch -- see lr_deposit_match.py",
            "palmetto payee, not handled by this matcher",
            "",
        )
    elif amt > 0:
        idx = find_best_match(amt, d, revenue_candidates, rev_used)
        if idx is not None:
            rev_used.add(idx)
            c = revenue_candidates[idx]
            matched_rev += 1
            proposed = f"Revenue: {c['name']} -- {c['category']} (wk {c['week_monday']})"
            basis = "exact amount, within window"
            match_key = c["match_key"]
        else:
            unmatched += 1
            proposed, basis, match_key = "UNMATCHED", "no candidate found", ""
    else:
        idx = find_best_match(abs(amt), d, expense_candidates, exp_used)
        if idx is not None:
            exp_used.add(idx)
            c = expense_candidates[idx]
            matched_exp += 1
            proposed = f"Expense: {c['name']} -- {c['category']} (wk {c['week_monday']})"
            basis = "exact amount, within window"
            match_key = c["match_key"]
        else:
            unmatched += 1
            proposed, basis, match_key = "UNMATCHED", "no candidate found", ""

    proposals.append({
        "date": d.isoformat(),
        "amount": round(amt, 2),
        "account": acct_name,
        "direction": direction,
        "payee": payee or desc,
        "proposed_match": proposed,
        "match_basis": basis,
        "match_key": match_key,
    })

print(f"\nMatched to Revenue: {matched_rev}")
print(f"Matched to Expenses: {matched_exp}")
print(f"LightReach batch (labeled, not matched here): {palmetto}")
print(f"Unmatched: {unmatched}")

# ---- write proposals to Render (which writes the Reconciliation tab) -----
if proposals:
    w = requests.post(
        f"{RENDER_BASE}/internal/cashflow-reconcile-write-proposals",
        json={"transactions": proposals},
        timeout=120,
    )
    w.raise_for_status()
    result = w.json()
    print(f"\nReconciliation tab: {result.get('new', 0)} new rows, "
          f"{result.get('updated', 0)} updated, "
          f"{result.get('skipped_applied', 0)} already-applied rows skipped")
else:
    print("\nNo transactions to propose.")
