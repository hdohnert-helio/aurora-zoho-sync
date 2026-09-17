"""PowerClerk full audit — read-only.

Logs in once, pages through every project in both programs, and prints one
pipe-delimited line per project so the run log can be read directly. Also
writes artifacts/powerclerk_projects.csv.

Writes nothing to PowerClerk or Zoho.
"""
import csv, os, pathlib, re, sys
from playwright.sync_api import sync_playwright

ART = pathlib.Path("artifacts"); ART.mkdir(exist_ok=True)
USER, PASS = os.environ.get("PC_USER", ""), os.environ.get("PC_PASS", "")

LOGIN = ("uinet.powerclerk.com", "MS7SMA7Q77CM")
PROGRAMS = [
    ("UI",         "uinet.powerclerk.com",            "MS7SMA7Q77CM"),
    ("EVERSOURCE", "esctinterconnect.powerclerk.com", "28X4MJXZUR42"),
]

# header-name -> normalised field. lowercased substring match.
FIELD_HINTS = [
    ("project #",            "project_no"),
    ("current status timestamp", "status_at"),
    ("status timestamp",     "status_at"),
    ("timestamp",            "status_at"),
    ("current status",       "status"),
    ("queue position",       "queue"),
    ("assignee",             "assignee"),
    ("customer name",        "name"),
    ("address last",         "name_last"),
    ("address first",        "name_first"),
    ("street address",       "street"),
    ("address line 1",       "street"),
    ("address city",         "city"),
    ("city",                 "city"),
]


def grab_rows(page):
    """Return list of dicts for the table currently rendered."""
    return page.evaluate("""() => {
      const t = [...document.querySelectorAll('table')]
        .find(x => /Project\\s*#/i.test(x.innerText));
      if (!t) return {headers: [], rows: []};
      const headers = [...t.querySelectorAll('thead th')].map(h => h.innerText.trim());
      const rows = [...t.querySelectorAll('tbody tr')].map(
        tr => [...tr.querySelectorAll('td')].map(td => td.innerText.trim().replace(/\\s+/g,' '))
      ).filter(r => r.length > 1);
      return {headers, rows};
    }""")


def normalise(headers, row):
    out = {}
    for i, h in enumerate(headers):
        hl = h.lower()
        for hint, field in FIELD_HINTS:
            if hint in hl and i < len(row):
                if field not in out or not out[field]:
                    out[field] = row[i]
                break
    name = out.get("name") or " ".join(
        x for x in [out.get("name_first", ""), out.get("name_last", "")] if x
    ).strip()
    return {
        "project_no": out.get("project_no", ""),
        "name": name,
        "street": out.get("street", ""),
        "city": out.get("city", ""),
        "status": out.get("status", ""),
        "status_at": out.get("status_at", ""),
        "assignee": out.get("assignee", ""),
        "queue": out.get("queue", ""),
    }


def collect(page, label, host, pid):
    url = f"https://{host}/MvcProjects/ProjectList?ProgramId={pid}"
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3500)

    body = page.inner_text("body")
    m = re.search(r"([\d,]+)\s+Projects?\s+Found", body, re.I)
    total = int(m.group(1).replace(",", "")) if m else 0
    print(f"  {label}: {total} projects reported")

    # Diagnose the pager once so we know what we are dealing with.
    try:
        pager = page.evaluate("""() => {
          const el = [...document.querySelectorAll('*')].find(
            e => /Items per page/i.test(e.textContent||'') && e.children.length < 12);
          return el ? el.innerText.replace(/\\s+/g,' ').slice(0,200) : 'no pager text';
        }""")
        print(f"  pager: {pager}")
    except Exception as e:
        print(f"  pager probe failed: {e}")

    # Prefer raising the page size; fall back to clicking Next.
    for opt in ("500", "250", "100", "50", "25"):
        try:
            sel = page.locator("select").filter(has_text=re.compile(r"\b(10|15|25)\b")).first
            if sel.count():
                sel.select_option(opt); page.wait_for_timeout(3000)
                print(f"  page size -> {opt}"); break
        except Exception:
            continue

    seen, out, guard = set(), [], 0
    while guard < 60:
        guard += 1
        data = grab_rows(page)
        headers, rows = data["headers"], data["rows"]
        new = 0
        for r in rows:
            rec = normalise(headers, r)
            key = rec["project_no"]
            if key and key not in seen:
                seen.add(key); out.append(rec); new += 1
        print(f"  page {guard}: +{new} (total {len(out)}/{total})")
        if len(out) >= total or new == 0:
            break
        clicked = False
        for nsel in ["a[aria-label*='Next' i]", "button[aria-label*='Next' i]",
                     "a[title*='Next' i]", "li.next a", "a.next", "text=Next"]:
            try:
                el = page.locator(nsel).last
                if el.count() and el.is_visible() and el.is_enabled():
                    el.click(); clicked = True; break
            except Exception:
                continue
        if not clicked:
            print("  no Next control found — stopping")
            break
        page.wait_for_timeout(3000)

    print(f"  {label}: collected {len(out)} rows")
    return out


def main():
    if not USER or not PASS:
        print("FAIL: credentials missing."); return 1

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(viewport={"width": 1900, "height": 1400}).new_page()

        page.goto(f"https://{LOGIN[0]}/MvcAccount/Login?ProgramId={LOGIN[1]}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)
        page.locator("#UserName").fill(USER)
        page.locator("#Password").fill(PASS)
        page.locator("#Password").press("Enter")
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)
        if "/MvcAccount/Login" in page.url:
            print("FAIL: login rejected"); return 1
        print("logged in")

        allrows = []
        for label, host, pid in PROGRAMS:
            for r in collect(page, label, host, pid):
                r["program"] = label
                allrows.append(r)
        browser.close()

    cols = ["program", "project_no", "name", "street", "city", "status",
            "status_at", "assignee", "queue"]
    with open(ART / "powerclerk_projects.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(allrows)

    print(f"\n===BEGIN {len(allrows)} ROWS===")
    for r in allrows:
        print("|".join((r.get(c) or "").replace("|", "/") for c in cols))
    print("===END ROWS===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
