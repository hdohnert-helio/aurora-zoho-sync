"""LightReach (palmetto.finance) funding-page audit -- read-only.

Enumerates accounts from PALMETTO'S OWN Accounts list (via its
/api/accounts/summary JSON API, discovered 2026-09-18 -- see CLAUDE.md's
"Chase via SimpleFIN + Palmetto deposit matching" section), not from Zoho's
LightReach_Account_ID. The old Zoho-driven enumeration missed any LightReach
project not linked in the CRM, which was the root cause of 6 unmatched
Chase deposit batches (every gap was positive -- money LightReach paid on a
project this scrape never knew to look for).

Zoho stays in the picture as an ANNOTATION: each Palmetto account is joined
back to a Zoho Install by LightReach_Account_ID first, then by normalized
address, and marked unmatched (not dropped) when neither works.

Filters to accounts whose currentMilestone is Install or Activation --
Qualification/Notice to Proceed accounts are pre-contract leads (confirmed
by Harry 2026-09-18) with nothing to scrape on a /funding page. Completed/
funded accounts stay tagged currentMilestone=activation (just with a
different status), so passing includeCompletedAccounts=true on the summary
call is what actually surfaces them -- the default view hides them.

Then scrapes each qualifying account's /funding page for the payment plan,
holdback, pricing-setting and transaction-ledger data that exists nowhere
else (not in webhooks, not in Zoho).

Writes nothing to Zoho or to LightReach. Two CSVs land in artifacts/:
  lr_funding_projects.csv     -- one row per project
  lr_funding_transactions.csv -- one row per ledger line

Palmetto does not enforce single-session, unlike PowerClerk, so this can run
any time without evicting anyone.
"""
import csv
import datetime
import os
import pathlib
import re
import sys

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from powerclerk_sync import normalize_street, normalize_city, split_site_location  # noqa: E402

ART = pathlib.Path("artifacts")
ART.mkdir(exist_ok=True)

LR_USER = os.environ.get("LR_USER", "")
LR_PASS = os.environ.get("LR_PASS", "")

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

FUNDING_HOST = "https://palmetto.finance"
PAYMENT_PLAN_TIMEOUT_MS = 45000  # confirmed >10s on at least one account; be generous

# Milestone types worth scraping -- Qualification/Notice to Proceed accounts
# are pre-contract leads with no funding activity (Harry, 2026-09-18).
FUNDED_MILESTONE_TYPES = {"install", "activation"}

PROJECT_COLUMNS = [
    "palmetto_account_id", "primary_applicant_name", "address1", "city", "state",
    "current_milestone_type", "current_milestone_status",
    "zoho_matched", "zoho_match_method", "zoho_id", "Name",
    "LightReach_Account_ID", "Contract_Total", "System_kW_DC",
    "Battery_kWh", "Module_Model", "Inverter_Model", "Inverters", "Equipment_Invoice_Total",
    "Project_Stage",
    "total_project_value", "install_approved_amount", "install_approved_pct",
    "activation_approved_amount", "activation_approved_pct", "vendor_direct_pay",
    "total_holdback", "dc_holdback", "standard_holdback", "payout_to_date",
    "amount_remaining", "standard_holdback_ppw", "domestic_content_modifier_ppw",
    "dc_holdback_ppw", "pricing_lock_date", "funding_url", "scrape_status",
]
TRANSACTION_COLUMNS = [
    "account_id", "project_name", "event_datetime", "payout_event",
    "description", "batch", "status", "amount",
]


# ── Zoho ─────────────────────────────────────────────────────────────────────

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


def fetch_lr_installs(token):
    """Every Install with a non-null LightReach_Account_ID. No stage filter --
    the caller wants everything LightReach-funded, active or not."""
    criteria = "(LightReach_Account_ID:not_equal:null)"
    fields = ("id,Name,LightReach_Account_ID,Contract_Total,System_kW_DC,Battery_kWh,"
              "Module_Model,Inverter_Model,Inverters,Equipment_Invoice_Total,Project_Stage")
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
        if r.status_code >= 400:
            raise RuntimeError(f"Zoho search failed: HTTP {r.status_code} | {r.text[:300]}")
        body = r.json()
        results.extend(body.get("data", []))
        if not body.get("info", {}).get("more_records"):
            break
        page += 1
    # Defensive client-side filter -- never trust the criteria alone.
    return [r for r in results if (r.get("LightReach_Account_ID") or "").strip()]


def fetch_all_zoho_installs(token):
    """Every Install, any stage, with just the fields needed to join a Palmetto
    account back to Zoho -- by LightReach_Account_ID first, by address as a
    fallback. No stage filter: an already-cancelled or closed-out install can
    still carry a LightReach_Account_ID/address worth matching against."""
    fields = "id,Name,Site_Location,LightReach_Account_ID"
    results, page = [], 1
    while True:
        r = requests.get(
            f"{API_DOMAIN}/crm/v7/Installs/search",
            headers=zoho_headers(token),
            params={"criteria": "(id:not_equal:null)", "fields": fields, "page": page, "per_page": 200},
            timeout=30,
        )
        if r.status_code == 204:
            break
        if r.status_code >= 400:
            raise RuntimeError(f"Zoho search failed: HTTP {r.status_code} | {r.text[:300]}")
        body = r.json()
        results.extend(body.get("data", []))
        if not body.get("info", {}).get("more_records"):
            break
        page += 1
    return results


def build_zoho_join_indexes(installs, known_cities):
    """Returns (by_lr_id, by_address) lookup dicts for matching a Palmetto
    account back to one of these Zoho Installs."""
    by_lr_id = {}
    by_address = {}
    for inst in installs:
        lr_id = (inst.get("LightReach_Account_ID") or "").strip()
        if lr_id:
            by_lr_id[lr_id] = inst
        street, city, _state = split_site_location(inst.get("Site_Location") or "", known_cities)
        key = (normalize_street(street), normalize_city(city))
        if key[0] and key[1]:
            by_address.setdefault(key, []).append(inst)
    return by_lr_id, by_address


def match_palmetto_account(account, by_lr_id, by_address):
    """Returns (zoho_install_or_None, method_str)."""
    acc_id = account.get("id")
    if acc_id in by_lr_id:
        return by_lr_id[acc_id], "lightreach_account_id"
    addr = account.get("address") or {}
    key = (normalize_street(addr.get("address1") or ""), normalize_city(addr.get("city") or ""))
    if key[0] and key[1] and key in by_address:
        candidates = by_address[key]
        return candidates[0], "address" if len(candidates) == 1 else "address (ambiguous, multiple Zoho matches)"
    return None, "none"


# ── Palmetto Accounts list ───────────────────────────────────────────────────

def fetch_palmetto_accounts(page, milestone_types=FUNDED_MILESTONE_TYPES, page_size=20):
    """Pages through /api/accounts/summary (the real backing API for the
    Accounts nav, found via network capture 2026-09-18) with
    includeCompletedAccounts=true -- the default view hides completed/funded
    accounts entirely (confirmed: Activation count went from 3 to 90 once
    this flag was set, and 90 + 17 Install = the bulk of the historically
    LightReach-funded book). Filters client-side to milestone_types since the
    API's own currentMilestone param wasn't confirmed to accept a specific
    value server-side.

    Returns the full raw account list (all milestones) plus the filtered
    (funded-only) subset, so the caller can report Palmetto's total account
    count as context even though only the funded subset gets scraped.

    Walking all ~1373 accounts (mostly Notice to Proceed leads, per Harry)
    just to find the ~107 funded ones is slow and, worse, unsafe: the walk
    takes minutes (longer with 429 backoffs) under sort=NEWEST, which is not
    a stable key while the underlying list keeps changing in near-real-time
    -- confirmed 2026-09-18, when a second investigation run found 4 accounts
    genuinely at currentMilestone=install that a same-day full walk had
    missed. So: try the API's own currentMilestone filter first (one
    short, fast walk per funded milestone type); only fall back to the full
    walk (deduped by id, immune to a little count drift) if the server
    doesn't actually honor that filter.
    """
    def _walk(milestone_param, filter_client_side):
        seen = {}
        page_num = 1
        total = None
        empty_streak = 0
        while True:
            url = (
                f"{FUNDING_HOST}/api/accounts/summary?currentMilestone={milestone_param}&pageNum={page_num}"
                "&advancedFilters=%5B%5D&searchTerm=undefined&includeCompletedAccounts=true"
                "&onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST"
            )
            # ~69 pages at 20/page hit HTTP 429 around page 30 on the first real
            # run (2026-09-18) -- back off and retry a few times before giving up,
            # same shape as scrape_account's retry for a timed-out render.
            resp = None
            for attempt in range(1, 6):
                resp = page.request.get(url, timeout=30000)
                if resp.status != 429:
                    break
                wait_s = 5 * attempt
                print(f"  /api/accounts/summary page {page_num}: HTTP 429, "
                      f"backing off {wait_s}s (attempt {attempt}/5)")
                page.wait_for_timeout(wait_s * 1000)
            if not resp.ok:
                raise RuntimeError(f"/api/accounts/summary page {page_num} failed: HTTP {resp.status}")
            body = resp.json()
            if total is None:
                total = body.get("total", 0)
            batch = (body.get("data") or {}).get("accounts") or []
            if not batch:
                empty_streak += 1
                if empty_streak >= 2:  # two empties in a row -- genuinely done, not a transient blip
                    break
            else:
                empty_streak = 0
                for a in batch:
                    seen[a.get("id")] = a
            page_num += 1
            page.wait_for_timeout(400)  # small gap between pages -- avoid tripping the rate limit at all
        accounts = list(seen.values())
        if filter_client_side:
            accounts = [a for a in accounts if (a.get("currentMilestone") or {}).get("type") == milestone_param]
        return accounts, total

    # Try server-side filtering first, one type at a time -- much shorter
    # walks than the full ~1373-account list.
    funded_by_type = {}
    server_filter_worked = True
    for mtype in milestone_types:
        accounts, _ = _walk(mtype, filter_client_side=False)
        if accounts and any((a.get("currentMilestone") or {}).get("type") != mtype for a in accounts):
            server_filter_worked = False
            break
        for a in accounts:
            funded_by_type[a.get("id")] = a

    if server_filter_worked and funded_by_type:
        print(f"  /api/accounts/summary honors currentMilestone= -- used a focused walk per milestone type")
        return None, list(funded_by_type.values())

    print(f"  /api/accounts/summary did NOT filter by currentMilestone= as expected -- "
          f"falling back to a full walk of every account")
    all_accounts, _ = _walk("undefined", filter_client_side=False)
    funded = [a for a in all_accounts if (a.get("currentMilestone") or {}).get("type") in milestone_types]
    return all_accounts, funded


# ── Auth0 login ──────────────────────────────────────────────────────────────

def _click_first(page, locator, timeout=5000):
    if locator.count() > 0:
        locator.first.click(timeout=timeout)
        return True
    return False


def _submit(page):
    btn = page.locator("button[type='submit']")
    if btn.count() > 0 and btn.first.is_visible():
        btn.first.click()
    else:
        page.keyboard.press("Enter")


def login(page, user, password):
    """Auth0 hosted login. No confirmed selectors going in -- this is written
    defensively (identifier-first OR combined form) and logs page.url at each
    step so a failure is diagnosable from the run log."""
    page.goto(FUNDING_HOST + "/", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)
    print(f"  landed on: {page.url}")

    id_selector = "input[type='email'], input[name='username'], input[name='email']"
    if page.locator(id_selector).count() == 0:
        for text in ("Log In", "Log in", "Sign In", "Sign in"):
            try:
                loc = page.get_by_text(text, exact=False)
                if _click_first(page, loc):
                    page.wait_for_timeout(1500)
                    print(f"  clicked {text!r}, now at: {page.url}")
                    break
            except Exception:
                continue

    id_field = page.locator(id_selector).first
    id_field.wait_for(timeout=30000)
    id_field.fill(user)
    print(f"  filled identifier field, at: {page.url}")

    pw_selector = "input[type='password'], input[name='password']"
    pw_field = page.locator(pw_selector)
    if pw_field.count() > 0 and pw_field.first.is_visible():
        pw_field.first.fill(password)
        _submit(page)
    else:
        _submit(page)  # identifier-first flow: submit email, wait for password step
        page.wait_for_timeout(1000)
        pw_field2 = page.locator(pw_selector).first
        pw_field2.wait_for(timeout=30000)
        pw_field2.fill(password)
        _submit(page)

    page.wait_for_load_state("domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)
    print(f"  post-login at: {page.url}")

    if "auth0" in page.url.lower() or "/login" in page.url.lower():
        raise RuntimeError(f"login did not complete -- still at {page.url}")


# ── Funding page parsing ─────────────────────────────────────────────────────

_MONEY_RE = re.compile(r"-?\$[\d,]+\.\d{2}")
_PERCENT_RE = re.compile(r"-?[\d.]+\s?%")
_PPW_RE = re.compile(r"\$[\d.]+\s?/\s?W")
_DATE_RE = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}")


def _money(v):
    return v.replace("$", "").replace(",", "").strip() if v else None


def find_near(lines, labels, value_re, forward=5):
    """Scan for EVERY occurrence of any label (not just the first) and return
    the first nearby value match -- the page repeats some labels (e.g.
    "Standard Holdback" as both a dollar total and a $/W pricing setting)."""
    if isinstance(labels, str):
        labels = [labels]
    labels_low = [l.lower() for l in labels]
    for i, line in enumerate(lines):
        low = line.lower()
        if not any(lbl in low for lbl in labels_low):
            continue
        m = value_re.search(line)
        if m:
            return m.group(0)
        for j in range(i + 1, min(i + 1 + forward, len(lines))):
            m = value_re.search(lines[j])
            if m:
                return m.group(0)
    return None


def parse_funding_fields(lines):
    return {
        "total_project_value": _money(find_near(lines, "Total Project Value", _MONEY_RE)),
        "install_approved_amount": _money(find_near(lines, "Install Approved", _MONEY_RE)),
        "install_approved_pct": find_near(lines, "Install Approved", _PERCENT_RE),
        "activation_approved_amount": _money(find_near(lines, "Activation Approved", _MONEY_RE)),
        "activation_approved_pct": find_near(lines, "Activation Approved", _PERCENT_RE),
        "vendor_direct_pay": _money(find_near(lines, "Vendor Direct Pay", _MONEY_RE)),
        "total_holdback": _money(find_near(lines, "Total Holdback", _MONEY_RE)),
        "dc_holdback": _money(find_near(lines, "DC Holdback", _MONEY_RE)),
        "standard_holdback": _money(find_near(lines, "Standard Holdback", _MONEY_RE)),
        "payout_to_date": _money(find_near(
            lines, ["Payout to Date", "Payout to-date", "Payout To Date"], _MONEY_RE)),
        "amount_remaining": _money(find_near(lines, "Amount Remaining", _MONEY_RE)),
        "standard_holdback_ppw": find_near(lines, "Standard Holdback", _PPW_RE),
        "domestic_content_modifier_ppw": find_near(lines, "Domestic Content Modifier", _PPW_RE),
        "dc_holdback_ppw": find_near(lines, "DC Holdback", _PPW_RE),
        "pricing_lock_date": find_near(lines, ["Pricing Lock", "Price Lock"], _DATE_RE),
    }


def extract_ledger(page):
    """Find the transaction-ledger table (header has both Batch and Status)
    and return its rows as raw cell-text lists, positionally: date, payout
    event, description, batch, status, amount."""
    tables = page.locator("table")
    for i in range(tables.count()):
        t = tables.nth(i)
        try:
            header_text = t.locator("thead").inner_text() if t.locator("thead").count() else ""
            if not header_text:
                header_text = t.inner_text()[:400]
        except Exception:
            continue
        if "batch" in header_text.lower() and "status" in header_text.lower():
            rows_out = []
            rows = t.locator("tbody tr")
            for r in range(rows.count()):
                cells = rows.nth(r).locator("td")
                vals = [cells.nth(c).inner_text().strip() for c in range(cells.count())]
                if vals:
                    rows_out.append(vals)
            return rows_out
    return []


def ledger_row_to_transaction(account_id, project_name, vals):
    """Best-effort positional mapping -- refined once real column order is
    confirmed against a live account."""
    padded = vals + [""] * max(0, 6 - len(vals))
    event_datetime, payout_event, description, batch, status, amount = padded[:6]
    return {
        "account_id": account_id,
        "project_name": project_name,
        "event_datetime": event_datetime,
        "payout_event": payout_event,
        "description": description,
        "batch": batch,
        "status": status,
        "amount": _money(amount) if _MONEY_RE.search(amount or "") else amount,
    }


# ── Per-account scrape ───────────────────────────────────────────────────────

def investigate_missing_accounts(page):
    """One-shot investigation, triggered by LR_INVESTIGATE_MISSING=1.

    The 2026-09-18 real run scraped 107 Palmetto accounts at Install/
    Activation milestone (vs 136 Zoho knew via LightReach_Account_ID) and got
    the deposit-match rate to 16/19 batches -- up from 13/19 before the
    Palmetto-driven enumeration, but not the 19/19 target. The 3 remaining
    misses all show a real Palmetto deposit with a positive, unexplained gap
    over the ledger total -- the same "our gap, not LightReach's" signature
    documented in CLAUDE.md for the original 6 misses.

    Working theory: some of the 29 Zoho-linked accounts NOT currently at
    Install/Activation milestone (e.g. since cancelled, or otherwise moved
    off it) still had real ledger activity that landed in this 90-day window
    before/around that move. This fetches each of those 29 directly by id
    (GET /api/accounts/{id}, no /funding page scrape needed) and dumps
    currentMilestone + inboundPayments so the specific account(s) behind each
    unmatched batch can be identified.
    """
    token = get_zoho_token()
    zoho_installs = fetch_all_zoho_installs(token)
    zoho_lr_ids = {(i.get("LightReach_Account_ID") or "").strip(): i
                   for i in zoho_installs if (i.get("LightReach_Account_ID") or "").strip()}
    print(f"{len(zoho_lr_ids)} Zoho Installs with a LightReach_Account_ID")

    _, funded_accounts = fetch_palmetto_accounts(page)
    funded_ids = {a.get("id") for a in funded_accounts}
    print(f"{len(funded_ids)} currently at Install/Activation milestone in Palmetto")

    missing_ids = set(zoho_lr_ids) - funded_ids
    print(f"{len(missing_ids)} Zoho-linked ids NOT in the current funded list -- fetching each directly\n")

    for acc_id in sorted(missing_ids):
        inst = zoho_lr_ids[acc_id]
        try:
            resp = page.request.get(f"{FUNDING_HOST}/api/accounts/{acc_id}", timeout=15000)
            if not resp.ok:
                print(f"  {acc_id} ({inst.get('Name')}): HTTP {resp.status}")
                continue
            body = resp.json()
            ms = body.get("currentMilestone") or {}
            payments = body.get("inboundPayments")
            print(f"  {acc_id} ({inst.get('Name')}): currentMilestone={ms.get('type')} "
                  f"status={ms.get('status')}")
            if payments:
                import json as _json
                print(f"    inboundPayments: {_json.dumps(payments, default=str)[:2000]}")
        except Exception as e:
            print(f"  {acc_id} ({inst.get('Name')}): fetch failed: {e}")
        page.wait_for_timeout(300)


def discover_accounts_nav(page, user, password):
    """One-shot exploration: log in, find the Accounts nav item CLAUDE.md
    mentions, click it, and dump enough structure (URL, table headers, first
    rows) to design the real list-scraper against. Not used in a normal run --
    triggered by LR_DISCOVER_ACCOUNTS=1 so this can be run standalone and
    cheaply against the real portal before committing to a parsing approach."""
    xhr_hits = []

    def on_response(resp):
        try:
            ct = resp.headers.get("content-type", "")
            if "json" in ct:
                xhr_hits.append((resp.request.method, resp.url, resp.status))
        except Exception:
            pass

    page.on("response", on_response)

    login(page, user, password)
    print("logged in, looking for an Accounts nav item")

    clicked = False
    for text in ("Accounts", "ACCOUNTS", "All Accounts"):
        try:
            loc = page.get_by_text(text, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=5000)
                page.wait_for_timeout(2000)
                clicked = True
                print(f"  clicked nav text {text!r}, now at: {page.url}")
                break
        except Exception as e:
            print(f"  (tried {text!r}: {e})")
    if not clicked:
        print("  could not find an 'Accounts' nav link by text -- dumping nav links instead")
        links = page.locator("a")
        for i in range(min(links.count(), 40)):
            try:
                href = links.nth(i).get_attribute("href")
                txt = links.nth(i).inner_text().strip()
                if txt or href:
                    print(f"    link[{i}]: text={txt!r} href={href!r}")
            except Exception:
                continue
        return

    page.wait_for_timeout(1500)
    text = page.inner_text("body")
    print(f"\n  page text length: {len(text)} chars")
    print("  first 2000 chars:")
    print(text[:2000])

    tables = page.locator("table")
    n_tables = tables.count()
    print(f"\n  {n_tables} <table> elements")
    for i in range(n_tables):
        t = tables.nth(i)
        try:
            print(f"  table[{i}] first 500 chars: {t.inner_text()[:500]!r}")
        except Exception as e:
            print(f"  table[{i}]: (failed: {e})")

    # Look for row links that might carry the account id, same shape as the
    # known /accounts/{id}/funding URL.
    links = page.locator("a[href*='/accounts/']")
    n_links = links.count()
    print(f"\n  {n_links} links containing '/accounts/'")
    for i in range(min(n_links, 20)):
        try:
            href = links.nth(i).get_attribute("href")
            txt = links.nth(i).inner_text().strip()
            print(f"    [{i}] text={txt!r} href={href!r}")
        except Exception:
            continue

    # Pagination hints
    for label in ("Next", "Load more", "Show more", "Rows per page"):
        loc = page.get_by_text(label, exact=False)
        if loc.count() > 0:
            print(f"  pagination hint found: {label!r} ({loc.count()}x)")

    print(f"\n  ALL JSON XHR/fetch responses seen during login + accounts load: {len(xhr_hits)}")
    for method, url, status in xhr_hits:
        print(f"    {method} {status} {url}")

    # List every clickable button/link near the top toolbar (Account/Export/
    # All/Filter/Sort) -- "Filter" as matched by get_by_text may not be the
    # real control; dump actual button elements instead of guessing.
    print("\n  <button> elements on page:")
    buttons = page.locator("button")
    for i in range(min(buttons.count(), 40)):
        try:
            txt = buttons.nth(i).inner_text().strip()
            if txt:
                print(f"    button[{i}]: {txt!r}")
        except Exception:
            continue

    # Try clicking Next and see what changes -- URL query params, or a
    # client-side re-render with no navigation.
    try:
        next_link = page.get_by_text("Next", exact=False)
        if next_link.count() > 0:
            before_url = page.url
            before_n = len(xhr_hits)
            next_link.first.click(timeout=5000)
            page.wait_for_timeout(2000)
            print(f"\n  clicked Next: {before_url} -> {page.url}")
            print(f"  new JSON responses after Next click: {len(xhr_hits) - before_n}")
            for method, url, status in xhr_hits[before_n:]:
                print(f"    {method} {status} {url}")
    except Exception as e:
        print(f"  (Next click failed: {e})")

    # Try Export -- capture any triggered download.
    try:
        export_btn = page.get_by_text("Export", exact=False)
        if export_btn.count() > 0:
            print("\n  trying Export button...")
            with page.expect_download(timeout=8000) as dl_info:
                export_btn.first.click(timeout=5000)
            dl = dl_info.value
            print(f"  Export triggered a download: {dl.suggested_filename}")
    except Exception as e:
        print(f"  (Export click/download failed or no download: {e})")

    # The real backing API: /api/accounts/summary. Default view passes
    # includeCompletedAccounts=false -- replay it from inside the page
    # (shares the browser's session cookies) with that flipped to true to
    # see whether completed/funded accounts (the ones Zoho already knows via
    # LightReach_Account_ID) show up once that flag is set.
    import json as _json
    for label, params in [
        ("default (includeCompletedAccounts=false)",
         "currentMilestone=undefined&pageNum=1&advancedFilters=%5B%5D&searchTerm=undefined"
         "&includeCompletedAccounts=false&onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST"),
        ("includeCompletedAccounts=true",
         "currentMilestone=undefined&pageNum=1&advancedFilters=%5B%5D&searchTerm=undefined"
         "&includeCompletedAccounts=true&onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST"),
    ]:
        url = f"https://palmetto.finance/api/accounts/summary?{params}"
        try:
            resp = page.request.get(url, timeout=15000)
            print(f"\n  API call [{label}]: HTTP {resp.status}")
            if resp.ok:
                body = resp.json()

                def describe(v, k, indent="    "):
                    if isinstance(v, list):
                        print(f"{indent}'{k}' is a list of {len(v)} items")
                        if v:
                            print(f"{indent}sample item keys: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0])}")
                            print(f"{indent}sample item: {_json.dumps(v[0], default=str)[:800]}")
                    elif isinstance(v, dict):
                        print(f"{indent}'{k}' is a dict with keys: {list(v.keys())}")
                        for k2, v2 in v.items():
                            describe(v2, k2, indent + "  ")
                    elif isinstance(v, (int, float, str, bool, type(None))):
                        print(f"{indent}'{k}' = {v!r}")

                if isinstance(body, dict):
                    print(f"    top-level keys: {list(body.keys())}")
                    for k, v in body.items():
                        describe(v, k)
                elif isinstance(body, list):
                    print(f"    top-level list of {len(body)} items")
                    if body:
                        print(f"    sample item: {_json.dumps(body[0], default=str)[:800]}")
            else:
                print(f"    body: {resp.text()[:500]}")
        except Exception as e:
            print(f"  API call [{label}] failed: {e}")

    # Also check the /filters endpoint for the full milestone vocabulary.
    try:
        resp = page.request.get("https://palmetto.finance/api/accounts/filters?cancelled=false&includeOptions=true", timeout=15000)
        print(f"\n  /api/accounts/filters: HTTP {resp.status}")
        if resp.ok:
            print(f"    body: {_json.dumps(resp.json(), default=str)[:1500]}")
    except Exception as e:
        print(f"  /api/accounts/filters failed: {e}")

    # Full milestoneSummary.milestones list (not just the first item) --
    # need every currentMilestone value that exists, not a sample of one.
    try:
        resp = page.request.get(
            "https://palmetto.finance/api/accounts/summary?currentMilestone=undefined&pageNum=1"
            "&advancedFilters=%5B%5D&searchTerm=undefined&includeCompletedAccounts=true"
            "&onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST",
            timeout=15000,
        )
        if resp.ok:
            ms = resp.json().get("data", {}).get("milestoneSummary", {}).get("milestones", [])
            print(f"\n  full milestoneSummary.milestones ({len(ms)} entries): {_json.dumps(ms, default=str)}")
    except Exception as e:
        print(f"  milestoneSummary fetch failed: {e}")

    # Sample a deep page (older accounts under sort=NEWEST) to see what a
    # long-completed account's currentMilestone/status looks like.
    try:
        resp = page.request.get(
            "https://palmetto.finance/api/accounts/summary?currentMilestone=undefined&pageNum=65"
            "&advancedFilters=%5B%5D&searchTerm=undefined&includeCompletedAccounts=true"
            "&onlyCancelledAccounts=false&onlyPostActivationAccounts=false&sort=NEWEST",
            timeout=15000,
        )
        if resp.ok:
            accts = resp.json().get("data", {}).get("accounts", [])
            print(f"\n  deep page (pageNum=65) sample -- {len(accts)} accounts:")
            for a in accts[:20]:
                print(f"    id={a.get('id')} currentMilestone={a.get('currentMilestone')} "
                      f"status={a.get('status')} name={a.get('primaryApplicantName')} "
                      f"ref={a.get('externalReference')}")
    except Exception as e:
        print(f"  deep page fetch failed: {e}")

    # Verify the externalReference hypothesis: is it the Aurora Project ID?
    # Pull one real Zoho Install with both LightReach_Account_ID and
    # Aurora_Project_ID set, fetch that exact Palmetto account, and compare.
    try:
        from urllib.parse import quote
        zoho_token = get_zoho_token()
        r = requests.get(
            f"{API_DOMAIN}/crm/v7/Installs/search",
            headers=zoho_headers(zoho_token),
            params={
                "criteria": "(LightReach_Account_ID:not_equal:null)",
                "fields": "id,Name,LightReach_Account_ID,Aurora_Project_ID",
                "per_page": 5,
            },
            timeout=30,
        )
        cand = None
        if r.status_code == 200:
            for rec in r.json().get("data", []):
                if (rec.get("LightReach_Account_ID") or "").strip() and (rec.get("Aurora_Project_ID") or "").strip():
                    cand = rec
                    break
        if cand:
            acc_id = cand["LightReach_Account_ID"]
            print(f"\n  cross-check candidate: {cand.get('Name')} "
                  f"LightReach_Account_ID={acc_id} Aurora_Project_ID={cand.get('Aurora_Project_ID')}")
            resp = page.request.get(f"https://palmetto.finance/api/accounts/{quote(acc_id)}", timeout=15000)
            print(f"  GET /api/accounts/{acc_id}: HTTP {resp.status}")
            if resp.ok:
                body = resp.json()
                print(f"    externalReference={body.get('externalReference')!r} "
                      f"(compare to Aurora_Project_ID={cand.get('Aurora_Project_ID')!r})")
                print(f"    full body keys: {list(body.keys())}")
        else:
            print("\n  no Zoho Install found with both LightReach_Account_ID and Aurora_Project_ID set")
    except Exception as e:
        print(f"  externalReference cross-check failed: {e}")


def scrape_account(page, account_id, debug=False, retries=3):
    """Retries a timed-out render before giving up. The first full run
    (2026-09-18, 136 accounts) skipped 29 with a suspiciously regular
    ~4-5-account spacing -- more consistent with transient rate-limiting or
    a flaky response than genuine per-account slowness, so a retry is worth
    it before logging a real skip.

    Ledger-table completeness (found 2026-09-18): the 15s wait for
    `table tbody tr` used to swallow its own timeout and just proceed with
    whatever `extract_ledger` found (often 0 rows) -- so a slow-but-real
    render looked identical to a genuinely empty ledger, and a re-run of the
    exact same account could come back with a different row count purely by
    luck. Confirmed by two full runs ~40 minutes apart: same 107 accounts
    both times, but 64 vs 57 total ledger rows and a different set of
    deposit-match batches failing each time. Now an empty ledger table is
    treated as a retriable failure like a missing PAYMENT PLAN panel is,
    and only accepted as genuinely empty on the last attempt.
    """
    url = f"{FUNDING_HOST}/accounts/{account_id}/funding"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_function(
                "() => document.body.innerText.includes('PAYMENT PLAN')",
                timeout=PAYMENT_PLAN_TIMEOUT_MS,
            )
            # The PAYMENT PLAN panel and the transaction ledger table render on
            # separate async timers -- confirmed on Hopley, where the panel
            # appeared well before the ledger table did (0 rows extracted
            # until this wait was added).
            ledger_wait_timed_out = False
            try:
                page.wait_for_selector("table tbody tr", timeout=15000)
            except PlaywrightTimeout:
                ledger_wait_timed_out = True
                if attempt < retries:
                    print(f"  ({account_id}: attempt {attempt}/{retries} -- ledger table "
                          f"didn't render within 15s, retrying)")
                    page.wait_for_timeout(3000)
                    continue  # re-navigate and try the whole page again, not just re-poll
                if debug:
                    print(f"  DEBUG {account_id}: no ledger table rows within 15s on the "
                          f"final attempt -- accepting as a genuinely empty ledger")
            text = page.inner_text("body")
            if debug:
                print(f"  DEBUG raw page text length for {account_id}: {len(text)} chars")
                print("  DEBUG chars 3800-9000:")
                print(text[3800:9000])
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            fields = parse_funding_fields(lines)
            fields["funding_url"] = url
            ledger_rows = extract_ledger(page)
            # Even when the wait succeeded, a render that's still mid-flight can
            # yield a table with 0 rows.  A payment-plan amount without any
            # ledger row at all is a strong signal something is still loading,
            # not that the ledger is really empty -- retry once more before
            # accepting it.
            if (not ledger_rows and not ledger_wait_timed_out
                    and (fields.get("install_approved_amount") or fields.get("activation_approved_amount"))
                    and attempt < retries):
                print(f"  ({account_id}: attempt {attempt}/{retries} -- payment plan has amounts "
                      f"but 0 ledger rows, retrying)")
                page.wait_for_timeout(3000)
                continue
            if debug:
                n_tables = page.locator("table").count()
                print(f"  DEBUG {n_tables} <table> elements on page")
                for ti in range(n_tables):
                    t = page.locator("table").nth(ti)
                    try:
                        print(f"    table[{ti}] first 200 chars: {t.inner_text()[:200]!r}")
                    except Exception as e:
                        print(f"    table[{ti}]: (failed: {e})")
                print(f"  DEBUG ledger rows for {account_id}: {len(ledger_rows)}")
                for row in ledger_rows[:3]:
                    print("   ", row)
            return fields, ledger_rows
        except PlaywrightTimeout as e:
            last_err = e
            if attempt < retries:
                print(f"  ({account_id}: attempt {attempt}/{retries} timed out, retrying)")
                page.wait_for_timeout(3000)
    raise last_err


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not LR_USER or not LR_PASS:
        print("FAIL: LR_USER / LR_PASS not set"); return 1

    if os.environ.get("LR_DISCOVER_ACCOUNTS", "").strip().lower() in ("1", "true", "yes"):
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_context(viewport={"width": 1600, "height": 1200}).new_page()
            try:
                discover_accounts_nav(page, LR_USER, LR_PASS)
            finally:
                browser.close()
        return 0

    if os.environ.get("LR_INVESTIGATE_MISSING", "").strip().lower() in ("1", "true", "yes"):
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_context(viewport={"width": 1600, "height": 1200}).new_page()
            try:
                login(page, LR_USER, LR_PASS)
                investigate_missing_accounts(page)
            finally:
                browser.close()
        return 0

    only = os.environ.get("LR_ONLY", "").strip()  # debug: comma-separated Palmetto account IDs

    token = get_zoho_token()
    zoho_installs = fetch_all_zoho_installs(token)
    zoho_lr_linked = [i for i in zoho_installs if (i.get("LightReach_Account_ID") or "").strip()]
    print(f"{len(zoho_installs)} total Zoho Installs, {len(zoho_lr_linked)} with a "
          f"LightReach_Account_ID (the old enumeration source, expected ~136)")

    project_rows = []
    transaction_rows = []
    ok = skipped = failed = 0

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(viewport={"width": 1600, "height": 1200}).new_page()

        try:
            login(page, LR_USER, LR_PASS)
        except Exception as e:
            print(f"FAIL: login failed: {e}")
            browser.close()
            return 1
        print("logged in")

        all_accounts, funded_accounts = fetch_palmetto_accounts(page)
        if all_accounts is None:
            print(f"Palmetto: {len(funded_accounts)} at Install/Activation milestone "
                  f"(found via a focused per-milestone walk, not a full account listing)")
        else:
            print(f"Palmetto: {len(all_accounts)} total accounts, "
                  f"{len(funded_accounts)} at Install/Activation milestone (the funded subset)")

        # known_cities for split_site_location's no-comma fallback comes from
        # Palmetto's own (clean, separate) city field -- not from Zoho's messy
        # free-text Site_Location, which is the very thing being split.
        known_cities = {(a.get("address") or {}).get("city", "").strip()
                         for a in funded_accounts if (a.get("address") or {}).get("city")}
        by_lr_id, by_address = build_zoho_join_indexes(zoho_installs, known_cities)

        if only:
            wanted = {a.strip() for a in only.split(",") if a.strip()}
            funded_accounts = [a for a in funded_accounts if a.get("id") in wanted]
            print(f"LR_ONLY set -- restricting to {len(funded_accounts)} account(s): {wanted}")

        unmatched = []
        for idx, account in enumerate(funded_accounts):
            account_id = account.get("id")
            addr = account.get("address") or {}
            name = account.get("primaryApplicantName") or account_id
            zoho_install, method = match_palmetto_account(account, by_lr_id, by_address)

            row = {c: None for c in PROJECT_COLUMNS}
            row.update({
                "palmetto_account_id": account_id,
                "primary_applicant_name": name,
                "address1": addr.get("address1"),
                "city": addr.get("city"),
                "state": addr.get("state"),
                "current_milestone_type": (account.get("currentMilestone") or {}).get("type"),
                "current_milestone_status": (account.get("currentMilestone") or {}).get("status"),
                "zoho_matched": bool(zoho_install),
                "zoho_match_method": method,
            })
            if zoho_install:
                row["zoho_id"] = zoho_install.get("id")
                row["Name"] = zoho_install.get("Name")
                row["LightReach_Account_ID"] = zoho_install.get("LightReach_Account_ID")
            else:
                unmatched.append(account)

            try:
                fields, ledger_rows = scrape_account(page, account_id, debug=(idx == 0))
                row.update(fields)
                row["scrape_status"] = "ok"
                ok += 1
                for vals in ledger_rows:
                    transaction_rows.append(ledger_row_to_transaction(account_id, name, vals))
                print(f"  [{idx + 1}/{len(funded_accounts)}] OK {name} ({account_id}) "
                      f"[zoho: {method}]: {len(ledger_rows)} ledger rows")
            except PlaywrightTimeout:
                row["scrape_status"] = "skipped: funding panel never rendered"
                skipped += 1
                print(f"  [{idx + 1}/{len(funded_accounts)}] SKIP {name} ({account_id}): "
                      f"PAYMENT PLAN never appeared within {PAYMENT_PLAN_TIMEOUT_MS}ms")
            except Exception as e:
                row["scrape_status"] = f"error: {e}"
                failed += 1
                print(f"  [{idx + 1}/{len(funded_accounts)}] FAIL {name} ({account_id}): {e}")

            project_rows.append(row)

        browser.close()

    with open(ART / "lr_funding_projects.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=PROJECT_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(project_rows)

    with open(ART / "lr_funding_transactions.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TRANSACTION_COLUMNS)
        w.writeheader()
        w.writerows(transaction_rows)

    print(f"\ndone: {ok} ok, {skipped} skipped, {failed} failed "
          f"({len(transaction_rows)} total ledger rows)")
    print(f"\nPalmetto funded accounts: {len(funded_accounts)} vs {len(zoho_lr_linked)} "
          f"Zoho knew about via LightReach_Account_ID")
    print(f"Unmatched (no Zoho Install found -- LightReach-funded but not linked/found in CRM): "
          f"{len(unmatched)}")
    for a in unmatched:
        addr = a.get("address") or {}
        print(f"  {a.get('id')}  {a.get('primaryApplicantName')}  "
              f"{addr.get('address1')}, {addr.get('city')} {addr.get('state')}  "
              f"milestone={(a.get('currentMilestone') or {}).get('type')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
