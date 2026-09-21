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

Matching, in order per transaction:
  1. Vendor Map lookup (Harry-maintained tab, read via the snapshot) --
     recognizes a payee substring and narrows the candidate search to one
     category, or -- for a category with no forecast candidate at all
     (e.g. Engineering Costs/Plansets, Interconnection Fees) -- proposes a
     pre-filled Manual Category row directly.
  2. Amount+date match, category-appropriate tolerance: exact for
     project-linked categories (Commissions, Materials, CT Green Estates,
     SolarInsure, Subcontractor, all Revenue categories), a wider
     max(5%, $50) band for recurring bills (Debt Service, Subscriptions,
     Fleet, Payroll & Benefits).
  3. Name-fuzzy fallback when amount+date alone finds nothing: normalized
     payee text against each candidate's normalized name, with a looser
     amount tolerance -- this is what catches "Online to Mustakahmed"
     against an existing Commissions row for "Mustak Ahmed".
  4. Heartland (revenue side only, card deposits net of a processing fee):
     tries a single Cash-milestone candidate net of an assumed 1.5-4% fee,
     then the sum of exactly two such candidates. A single-candidate match
     applies normally; a two-candidate match can't cleanly stamp one sheet
     row, so it's reported with no Match Key (informational only, Harry
     files it manually if he agrees) -- this is the least certain matcher
     here and is treated accordingly.

Read-only against Chase/SimpleFIN. Writes only the computed match proposals
(dates, amounts, account labels, a short proposed-match string) to Render --
never balances, full descriptions, or anything from a non-allowlisted
account. Prints only aggregate counts, never per-transaction detail, to the
CI log.
"""
import argparse
import datetime as dt
import os
import re
import sys
from urllib.parse import urlsplit, urlunsplit

import requests

RENDER_BASE = os.environ.get("RENDER_BASE_URL", "https://aurora-zoho-sync.onrender.com")

ap = argparse.ArgumentParser()
ap.add_argument("--days", type=int, default=45, help="lookback window (max 90)")
ap.add_argument("--tolerance", type=float, default=0.01, help="exact-match amount tolerance in dollars")
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

# ---- pull current Revenue/Expenses/Vendor Map from Render -----------------
resp = requests.get(f"{RENDER_BASE}/internal/cashflow-snapshot", timeout=60)
resp.raise_for_status()
snapshot = resp.json()
if snapshot.get("status") != "ok":
    sys.exit(f"cashflow-snapshot failed: {snapshot}")

revenue_candidates = snapshot["revenue_candidates"]
expense_candidates = snapshot["expense_candidates"]
vendor_map = snapshot.get("vendor_map", [])
print(f"Revenue candidates (not yet Received): {len(revenue_candidates)}")
print(f"Expense candidates (Active): {len(expense_candidates)}")
print(f"Vendor Map entries: {len(vendor_map)}")

TOLERANT_CATEGORIES = {"Debt Service", "Subscriptions", "Fleet", "Payroll & Benefits"}
CASH_MILESTONE_CATEGORIES = {"Cash Deposit", "Cash Progress", "Cash Final"}
WINDOW_BEFORE_DAYS = 3
WINDOW_AFTER_DAYS = 10


def normalize(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def in_window(week_monday_str, txn_date):
    week_monday = dt.date.fromisoformat(week_monday_str)
    return (week_monday - dt.timedelta(days=WINDOW_BEFORE_DAYS)) <= txn_date <= \
           (week_monday + dt.timedelta(days=WINDOW_AFTER_DAYS))


def amount_tolerance(category, base_amount):
    if category in TOLERANT_CATEGORIES:
        return max(base_amount * 0.05, 50.0)
    return args.tolerance


def vendor_map_lookup(haystack_normalized):
    for v in vendor_map:
        needle = normalize(v["payee_contains"])
        if needle and needle in haystack_normalized:
            return v
    return None


def find_amount_match(amount, txn_date, candidates, used, category_hint=None):
    best, best_gap = None, None
    for i, c in enumerate(candidates):
        if i in used:
            continue
        if category_hint and c["category"] != category_hint:
            continue
        if abs(amount - c["amount"]) > amount_tolerance(c["category"], c["amount"]):
            continue
        if not in_window(c["week_monday"], txn_date):
            continue
        gap = abs((txn_date - dt.date.fromisoformat(c["week_monday"])).days)
        if best is None or gap < best_gap:
            best, best_gap = i, gap
    return best


def find_name_match(amount, txn_date, payee_normalized, candidates, used, category_hint=None):
    """Looser amount tolerance (15%), but requires the candidate's normalized
    name to appear in (or contain) the normalized payee text."""
    best, best_gap = None, None
    for i, c in enumerate(candidates):
        if i in used:
            continue
        if category_hint and c["category"] != category_hint:
            continue
        cname = normalize(c["name"])
        if not cname or (cname not in payee_normalized and payee_normalized not in cname):
            continue
        if abs(amount - c["amount"]) > max(amount_tolerance(c["category"], c["amount"]), c["amount"] * 0.15):
            continue
        if not in_window(c["week_monday"], txn_date):
            continue
        gap = abs((txn_date - dt.date.fromisoformat(c["week_monday"])).days)
        if best is None or gap < best_gap:
            best, best_gap = i, gap
    return best


def find_heartland_match(amount, txn_date, candidates, used):
    """Returns (indices_list, basis_str) or (None, None). A single-candidate
    match is safe to apply normally; a two-candidate sum is reported with no
    Match Key since it can't cleanly stamp one sheet row."""
    FEE_LOW, FEE_HIGH = 0.015, 0.04
    pool = [(i, c) for i, c in enumerate(candidates)
            if i not in used and c["category"] in CASH_MILESTONE_CATEGORIES and in_window(c["week_monday"], txn_date)]

    def fee_ok(total):
        lo, hi = total * (1 - FEE_HIGH), total * (1 - FEE_LOW)
        return (lo - 1) <= amount <= (hi + 1)

    for i, c in pool:
        if fee_ok(c["amount"]):
            fee_pct = 100 * (1 - amount / c["amount"])
            return [i], f"Heartland (net of ~{fee_pct:.1f}% card fee)"
    for a_idx in range(len(pool)):
        for b_idx in range(a_idx + 1, len(pool)):
            i1, c1 = pool[a_idx]
            i2, c2 = pool[b_idx]
            total = c1["amount"] + c2["amount"]
            if fee_ok(total):
                fee_pct = 100 * (1 - amount / total)
                return [i1, i2], f"Heartland (net of ~{fee_pct:.1f}% card fee, 2 deposits: {c1['name']} + {c2['name']})"
    return None, None


PALMETTO_PAYEE_HINT = "palmetto"

rev_used, exp_used = set(), set()
proposals = []
matched_rev = matched_exp = matched_name = matched_heartland = palmetto = manual_prefilled = unmatched = 0

for d, amt, acct_name, payee, desc in txns:
    haystack = f"{payee} {desc}"
    haystack_norm = normalize(haystack)
    direction = "In" if amt > 0 else "Out"
    proposed, basis, match_key, manual_category = "UNMATCHED", "no candidate found", "", ""

    if PALMETTO_PAYEE_HINT in haystack_norm:
        palmetto += 1
        proposed = "LightReach batch -- see lr_deposit_match.py"
        basis = "palmetto payee, not handled by this matcher"

    elif "heartland" in haystack_norm and amt > 0:
        idxs, hbasis = find_heartland_match(amt, d, revenue_candidates, rev_used)
        if idxs:
            matched_heartland += 1
            names = " + ".join(revenue_candidates[i]["name"] for i in idxs)
            proposed = f"Revenue: {names} (Heartland)"
            basis = hbasis
            if len(idxs) == 1:
                rev_used.add(idxs[0])
                match_key = revenue_candidates[idxs[0]]["match_key"]
            # 2-candidate case: no match_key on purpose -- see docstring
        else:
            unmatched += 1
            basis = "Heartland payee, no fee-adjusted candidate found"

    else:
        vm = vendor_map_lookup(haystack_norm)
        category_hint = vm["category"] if vm else None
        candidates = revenue_candidates if amt > 0 else expense_candidates
        used = rev_used if amt > 0 else exp_used
        pool_has_category = any(c["category"] == category_hint for c in candidates) if category_hint else True

        idx = find_amount_match(abs(amt), d, candidates, used, category_hint)
        match_basis = "exact amount, within window"
        if idx is None:
            idx = find_name_match(abs(amt), d, haystack_norm, candidates, used, category_hint)
            match_basis = "payee name match"

        if idx is not None:
            used.add(idx)
            c = candidates[idx]
            if amt > 0:
                matched_rev += 1
            else:
                matched_exp += 1
            if match_basis == "payee name match":
                matched_name += 1
            proposed = f"{'Revenue' if amt > 0 else 'Expense'}: {c['name']} -- {c['category']} (wk {c['week_monday']})"
            basis = match_basis
            match_key = c["match_key"]
        elif category_hint and not pool_has_category:
            # Vendor Map recognizes this vendor, but the category has no
            # forecast candidate at all (not just none in this window) --
            # a genuinely new, currently-unforecasted cost. Pre-fill Manual
            # Category so Harry only has to check Approved.
            manual_prefilled += 1
            proposed = f"UNMATCHED -- recognized vendor: {category_hint}"
            basis = "Vendor Map hit, no forecast candidate for this category"
            manual_category = "Expense" if amt < 0 else "Revenue"
        elif category_hint:
            unmatched += 1
            proposed = f"UNMATCHED -- recognized vendor: {category_hint}"
            basis = "Vendor Map hit, but no candidate matched this window"
        else:
            unmatched += 1

    proposals.append({
        "date": d.isoformat(),
        "amount": round(amt, 2),
        "account": acct_name,
        "direction": direction,
        "payee": payee or desc,
        "proposed_match": proposed,
        "match_basis": basis,
        "match_key": match_key,
        "manual_category": manual_category,
    })

print(f"\nMatched to Revenue (amount+date): {matched_rev}")
print(f"Matched to Expenses (amount+date): {matched_exp}")
print(f"  of which via payee-name fallback: {matched_name}")
print(f"Matched via Heartland fee-adjusted logic: {matched_heartland}")
print(f"LightReach batch (labeled, not matched here): {palmetto}")
print(f"Recognized vendor, pre-filled Manual Category: {manual_prefilled}")
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
