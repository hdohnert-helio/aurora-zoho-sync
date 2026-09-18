"""LightReach (palmetto.finance) funding-page audit -- read-only.

Queries Zoho for every Install with a LightReach_Account_ID, logs into
palmetto.finance once via Auth0, and scrapes each account's /funding page
for the payment plan, holdback, pricing-setting and transaction-ledger data
that exists nowhere else (not in webhooks, not in Zoho). See CLAUDE.md's
"LightReach Funding page" section.

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

ART = pathlib.Path("artifacts")
ART.mkdir(exist_ok=True)

LR_USER = os.environ.get("LR_USER", "")
LR_PASS = os.environ.get("LR_PASS", "")

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

FUNDING_HOST = "https://palmetto.finance"
PAYMENT_PLAN_TIMEOUT_MS = 45000  # confirmed >10s on at least one account; be generous

PROJECT_COLUMNS = [
    "zoho_id", "Name", "LightReach_Account_ID", "Contract_Total", "System_kW_DC",
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

def discover_accounts_nav(page, user, password):
    """One-shot exploration: log in, find the Accounts nav item CLAUDE.md
    mentions, click it, and dump enough structure (URL, table headers, first
    rows) to design the real list-scraper against. Not used in a normal run --
    triggered by LR_DISCOVER_ACCOUNTS=1 so this can be run standalone and
    cheaply against the real portal before committing to a parsing approach."""
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


def scrape_account(page, account_id, debug=False, retries=2):
    """Retries a timed-out render before giving up. The first full run
    (2026-09-18, 136 accounts) skipped 29 with a suspiciously regular
    ~4-5-account spacing -- more consistent with transient rate-limiting or
    a flaky response than genuine per-account slowness, so a retry is worth
    it before logging a real skip."""
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
            # until this wait was added). A genuinely ledger-less account
            # (no transactions yet) would never satisfy this, so give it its
            # own bounded wait and proceed with an empty ledger rather than
            # treating that as a full-account failure.
            try:
                page.wait_for_selector("table tbody tr", timeout=15000)
            except PlaywrightTimeout:
                if debug:
                    print(f"  DEBUG {account_id}: no ledger table rows within 15s "
                          f"(may be a genuinely empty ledger)")
            text = page.inner_text("body")
            if debug:
                print(f"  DEBUG raw page text length for {account_id}: {len(text)} chars")
                print("  DEBUG chars 3800-9000:")
                print(text[3800:9000])
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            fields = parse_funding_fields(lines)
            fields["funding_url"] = url
            ledger_rows = extract_ledger(page)
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

    if os.environ.get("LR_DISCOVER_ACCOUNTS", "").strip():
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_context(viewport={"width": 1600, "height": 1200}).new_page()
            try:
                discover_accounts_nav(page, LR_USER, LR_PASS)
            finally:
                browser.close()
        return 0

    only = os.environ.get("LR_ONLY", "").strip()  # debug: comma-separated account IDs

    token = get_zoho_token()
    installs = fetch_lr_installs(token)
    print(f"{len(installs)} Installs with a LightReach_Account_ID (expected ~136)")

    if only:
        wanted = {a.strip() for a in only.split(",") if a.strip()}
        installs = [i for i in installs if i["LightReach_Account_ID"] in wanted]
        print(f"LR_ONLY set -- restricting to {len(installs)} account(s): {wanted}")

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

        for idx, inst in enumerate(installs):
            account_id = inst["LightReach_Account_ID"]
            name = inst.get("Name", inst["id"])
            row = {c: inst.get(c) for c in PROJECT_COLUMNS if c in inst}
            row["zoho_id"] = inst["id"]

            try:
                fields, ledger_rows = scrape_account(page, account_id, debug=(idx == 0))
                row.update(fields)
                row["scrape_status"] = "ok"
                ok += 1
                for vals in ledger_rows:
                    transaction_rows.append(ledger_row_to_transaction(account_id, name, vals))
                print(f"  [{idx + 1}/{len(installs)}] OK {name} ({account_id}): "
                      f"{len(ledger_rows)} ledger rows")
            except PlaywrightTimeout:
                row["scrape_status"] = "skipped: funding panel never rendered"
                skipped += 1
                print(f"  [{idx + 1}/{len(installs)}] SKIP {name} ({account_id}): "
                      f"PAYMENT PLAN never appeared within {PAYMENT_PLAN_TIMEOUT_MS}ms")
            except Exception as e:
                row["scrape_status"] = f"error: {e}"
                failed += 1
                print(f"  [{idx + 1}/{len(installs)}] FAIL {name} ({account_id}): {e}")

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
