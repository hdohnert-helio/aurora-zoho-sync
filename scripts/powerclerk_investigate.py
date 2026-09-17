"""Phase 2 investigation -- temporary, delete after use.

Answers, before building anything:
  1. Does GetProjectList3's row payload already carry the opaque ProjectId?
  2. Does clicking a project's row link give a landing-page URL directly
     (and does it need the row-expander at all)?
  3. Does Eversource's "Export to CSV" give a cleaner grid than the API replay?
  4. On a real landing page, what does the Communications table's HTML look
     like, and do "View" links carry the ViewCommunication URL in their href
     (so we can scrape it without opening the new tab)?

Read-only. Writes nothing to PowerClerk or Zoho.
"""
import json
import os
import sys

from playwright.sync_api import sync_playwright

USER, PASS = os.environ.get("PC_USER", ""), os.environ.get("PC_PASS", "")
LOGIN = ("uinet.powerclerk.com", "MS7SMA7Q77CM")
UI = ("uinet.powerclerk.com", "MS7SMA7Q77CM")
EVERSOURCE = ("esctinterconnect.powerclerk.com", "28X4MJXZUR42")


def capture_and_replay(page, host, pid, length=5):
    captured = {}

    def on_req(req):
        if "GetProjectList3" in req.url and not captured:
            captured["url"] = req.url
            try:
                captured["body"] = json.loads(req.post_data or "{}")
            except Exception:
                captured["body"] = {}
            try:
                captured["headers"] = req.all_headers()
            except Exception:
                captured["headers"] = {}

    page.on("request", on_req)
    page.goto(f"https://{host}/MvcProjects/ProjectList?ProgramId={pid}",
              wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    page.remove_listener("request", on_req)

    if not captured:
        print("  FAIL: never saw GetProjectList3")
        return None

    body = captured["body"]
    tr = body.get("tableRequest", {})
    tr["start"] = 0
    tr["length"] = length
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
    return payload


def main():
    if not USER or not PASS:
        print("FAIL: credentials missing"); return 1

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_context(
            viewport={"width": 1900, "height": 1400}, accept_downloads=True
        ).new_page()

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
        print("logged in\n")

        # --- Q1: does the row payload carry ProjectId? ---
        print("=== Q1: GetProjectList3 row keys (UI) ===")
        payload = capture_and_replay(page, UI[0], UI[1])
        if payload:
            rows = (payload.get("Data") or {}).get("ProjectListData", {}).get("data", [])
            if rows:
                print("row0 top-level keys:", list(rows[0].keys()))
                print("row0 full JSON (untruncated except very long values):")
                print(json.dumps(rows[0])[:4000])
            else:
                print("no rows in reply")
        print()

        # --- Q2: what does clicking a project row actually give us? ---
        print("=== Q2: row link / expander behavior (UI) ===")
        page.goto(f"https://{UI[0]}/MvcProjects/ProjectList?ProgramId={UI[1]}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        try:
            links = page.locator("table tbody tr a").all()
            print(f"found {len(links)} <a> tags in the grid body; first 5 hrefs:")
            for l in links[:5]:
                try:
                    print(" ", l.get_attribute("href"))
                except Exception as e:
                    print("  (failed to read href)", e)
        except Exception as e:
            print("row-link inspection failed:", e)
        print()

        # --- Q3: Eversource Export to CSV ---
        print("=== Q3: Eversource Export to CSV ===")
        page.goto(f"https://{EVERSOURCE[0]}/MvcProjects/ProjectList?ProgramId={EVERSOURCE[1]}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        try:
            btn = page.get_by_text("Export to CSV", exact=False).first
            with page.expect_download(timeout=20000) as dl_info:
                btn.click()
            download = dl_info.value
            path = download.path()
            with open(path, "r", errors="replace") as f:
                content = f.read()
            print("export downloaded ok, length:", len(content))
            print("first 2000 chars:")
            print(content[:2000])
        except Exception as e:
            print("export-to-csv failed:", e)
        print()

        # --- Q4: landing page Communications table + View link hrefs ---
        # Use the UI "Application Corrections Required" tab to find a blocked
        # project (CLAUDE.md says this returns Feola + Webb), then open it.
        print("=== Q4: Communications table on a real landing page ===")
        page.goto(f"https://{UI[0]}/MvcProjects/ProjectList?ProgramId={UI[1]}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        try:
            tab = page.get_by_text("Application Corrections Required", exact=False).first
            tab.click()
            page.wait_for_timeout(3000)
            row_link = page.locator("table tbody tr a").first
            row_link.click()
            page.wait_for_load_state("domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)
            print("landing page URL:", page.url)

            comm_table = page.locator("table", has_text="Communications").first
            html = comm_table.evaluate("el => el.outerHTML")
            print("communications table HTML (first 4000 chars):")
            print(html[:4000])

            view_links = comm_table.locator("a").all()
            print(f"\n{len(view_links)} links in the communications table; hrefs:")
            for l in view_links[:6]:
                try:
                    print(" ", l.get_attribute("href"))
                except Exception as e:
                    print("  (failed to read href)", e)
        except Exception as e:
            print("landing-page investigation failed:", e)

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
