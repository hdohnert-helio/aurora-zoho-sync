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
        payload = capture_and_replay(page, UI[0], UI[1], length=2000)
        blocked_project_id = None
        if payload:
            rows = (payload.get("Data") or {}).get("ProjectListData", {}).get("data", [])
            if rows:
                print("row0 top-level keys:", list(rows[0].keys()))
                for k in ("ProjectId", "StatusId", "CanDelete", "CanAssign",
                          "QueuePosition", "Highlights"):
                    print(f"  row0[{k!r}] =", rows[0].get(k))
                # find a project whose status contains "Corrections Required" so
                # Q4 has a real blocked project to inspect.
                for r in rows:
                    fields = r.get("ProjectData") or []
                    status_val = next(
                        (f.get("Value") for f in fields if f.get("BuiltInFieldConstantId") == 2), "")
                    if status_val and "corrections required" in str(status_val).lower():
                        blocked_project_id = r.get("ProjectId")
                        proj_no = next(
                            (f.get("Value") for f in fields if f.get("BuiltInFieldConstantId") == 1), "?")
                        print(f"  found blocked project {proj_no} -> ProjectId={blocked_project_id!r} "
                              f"status={status_val!r}")
                        break
            else:
                print("no rows in reply")
        print()

        # --- Q4: landing page Communications table + View link hrefs ---
        # Navigate DIRECTLY using the ProjectId from Q1 -- no clicking needed.
        print("=== Q4: Communications table via direct landing-page URL ===")
        if blocked_project_id:
            url = (f"https://{UI[0]}/MvcProjects/LandingPage"
                   f"?ProgramId={UI[1]}&ProjectId={blocked_project_id}")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(6000)
                print("landing page URL:", page.url)
                print("page title:", page.title())

                idx = page.inner_text("body").find("Communications")
                print("'Communications' found in body text at index:", idx)
                if idx >= 0:
                    body_text = page.inner_text("body")
                    print("surrounding text:")
                    print(body_text[max(0, idx - 200): idx + 1500])

                n_tables = page.locator("table").count()
                print(f"\n{n_tables} <table> elements on the page")
                for i in range(min(n_tables, 8)):
                    t = page.locator("table").nth(i)
                    try:
                        txt = t.inner_text()[:300].replace("\n", " | ")
                        print(f"  table[{i}]: {txt}")
                    except Exception as e:
                        print(f"  table[{i}]: (failed: {e})")

                all_links = page.locator("a").all()
                view_like = [l for l in all_links
                             if "view" in (l.inner_text() or "").lower()
                             or "communication" in (l.get_attribute("href") or "").lower()]
                print(f"\n{len(all_links)} total <a> tags; {len(view_like)} look communication/view-related:")
                for l in view_like[:10]:
                    try:
                        print(" ", l.get_attribute("href"), "| text:", l.inner_text())
                    except Exception as e:
                        print("  (failed to read)", e)
            except Exception as e:
                print("landing-page investigation failed:", e)
        else:
            print("skipped -- no blocked project found in first page of rows")
        print()

        # --- Q3: Eversource Export to CSV ---
        print("=== Q3: Eversource Export to CSV ===")
        page.goto(f"https://{EVERSOURCE[0]}/MvcProjects/ProjectList?ProgramId={EVERSOURCE[1]}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(4000)
        try:
            # dismiss any lingering overlay before clicking
            page.keyboard.press("Escape")
            page.wait_for_timeout(500)
            btn = page.get_by_text("Export to CSV", exact=False).first
            btn.scroll_into_view_if_needed()
            try:
                with page.expect_download(timeout=40000) as dl_info:
                    btn.click(force=True)
                download = dl_info.value
                path = download.path()
                with open(path, "r", errors="replace") as f:
                    content = f.read()
                print("export downloaded ok, length:", len(content))
                print("first 2000 chars:")
                print(content[:2000])
            except Exception as e:
                print("no download event within 40s:", e)
                print("checking for a new tab/page instead...")
                pages_now = page.context.pages
                print(f"{len(pages_now)} open pages/tabs in context")
                for p2 in pages_now:
                    print("  page url:", p2.url)
        except Exception as e:
            print("export-to-csv failed:", e)

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
