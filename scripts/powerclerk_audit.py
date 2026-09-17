"""PowerClerk full audit — read-only.

Logs in once, pages through every project in both programs, and prints one
pipe-delimited line per project so the run log can be read directly. Also
writes artifacts/powerclerk_projects.csv.

Writes nothing to PowerClerk or Zoho.
"""
import csv, json, os, pathlib, re, sys
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
    """Drive the Vue grid's own API instead of clicking the pager.

    The list page POSTs to /MvcProjects/ProjectList/GetProjectList3 with a
    DataTables-style body: {"programId": ..., "tableRequest": {"start", "length",
    "columns":[{"header","property"}...]}}. We capture that request, re-issue it
    with length=2000, and read every row in one shot.
    """
    captured = {}

    def on_req(req):
        if "GetProjectList3" in req.url and not captured:
            try:
                captured["url"] = req.url
                captured["body"] = json.loads(req.post_data or "{}")
                try:
                    captured["headers"] = req.all_headers()
                except Exception:
                    captured["headers"] = {}
            except Exception as e:
                print(f"  capture failed: {e}")

    page.on("request", on_req)
    url = f"https://{host}/MvcProjects/ProjectList?ProgramId={pid}"
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    page.remove_listener("request", on_req)

    body_text = page.inner_text("body")
    m = re.search(r"([\d,]+)\s+Projects?\s+Found", body_text, re.I)
    total = int(m.group(1).replace(",", "")) if m else 0
    print(f"  {label}: {total} projects reported")

    if not captured:
        print("  FAIL: never saw GetProjectList3 — falling back to visible rows")
        data = grab_rows(page)
        return [normalise(data["headers"], r) for r in data["rows"] if r]

    body = captured["body"]
    tr = body.get("tableRequest", {})
    tr["start"] = 0
    tr["length"] = 2000
    body["tableRequest"] = tr

    hdrs = {k: v for k, v in (captured.get("headers") or {}).items()
            if not k.startswith(":") and k.lower() not in
            ("content-length", "host", "accept-encoding", "connection")}
    hdrs["content-type"] = "application/json"

    payload = page.evaluate(
        """async ([url, body, hdrs]) => {
             const r = await fetch(url, {method: 'POST', credentials: 'same-origin',
                                         headers: hdrs, body: JSON.stringify(body)});
             try { return await r.json(); }
             catch (e) { return {status: 'parse_error', http: r.status}; }
           }""",
        [captured["url"], body, hdrs])

    if not isinstance(payload, dict):
        print(f"  FAIL: unexpected payload type {type(payload)}")
        return []
    if payload.get("status") == "error":
        print(f"  API error: {str(payload.get('error'))[:160]}")
        return []
    print(f"  API keys: {list(payload.keys())[:8]}")

    rows = None
    for k in ("data", "Data", "rows", "aaData"):
        if isinstance(payload.get(k), list):
            rows = payload[k]; break
    if rows is None:
        print(f"  FAIL: no row array. sample={str(payload)[:300]}")
        return []
    print(f"  {label}: API returned {len(rows)} rows")

    # property (ProjectDataN) -> header text, from the captured request
    prop2head = {c.get("property"): c.get("header", "")
                 for c in tr.get("columns", []) if c.get("property")}

    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        headers, vals = [], []
        for prop, head in prop2head.items():
            v = r.get(prop)
            if isinstance(v, dict):
                v = v.get("value") or v.get("text") or v.get("display") or ""
            headers.append(head)
            vals.append(re.sub(r"<[^>]+>", " ", str(v or "")).strip())
        rec = normalise(headers, vals)
        if rec["project_no"]:
            out.append(rec)

    print(f"  {label}: collected {len(out)} rows")
    return out


def main():
    if not USER or not PASS:
        print("FAIL: credentials missing."); return 1

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(viewport={"width": 1900, "height": 1400}).new_page()

        # --- capture the XHR/fetch calls the Vue grid makes ---
        seen_api = []

        def _log_req(req):
            try:
                if req.resource_type not in ("xhr", "fetch"):
                    return
                from urllib.parse import urlsplit, parse_qs
                u = urlsplit(req.url)
                keys = sorted(parse_qs(u.query).keys())
                body = ""
                try:
                    d = req.post_data
                    if d:
                        body = d[:300]
                except Exception:
                    pass
                sig = f"{req.method} {u.netloc}{u.path} qkeys={keys} body={body}"
                if sig not in seen_api:
                    seen_api.append(sig)
            except Exception:
                pass

        page.on("request", _log_req)


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
        print("\n=== API CALLS OBSERVED ===")
        for sig in seen_api[:40]:
            print("  " + sig)
        print("=== END API ===")
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
