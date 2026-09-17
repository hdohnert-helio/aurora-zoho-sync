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


def find_blocked_project(page, host, pid, status_substrings):
    payload = capture_and_replay(page, host, pid, length=2000)
    if not payload:
        return None, None
    rows = (payload.get("Data") or {}).get("ProjectListData", {}).get("data", [])
    print(f"  ({len(rows)} rows)")
    for r in rows:
        fields = r.get("ProjectData") or []
        status_val = next(
            (f.get("Value") for f in fields if f.get("BuiltInFieldConstantId") == 2), "")
        if status_val and any(s in str(status_val).lower() for s in status_substrings):
            proj_no = next(
                (f.get("Value") for f in fields if f.get("BuiltInFieldConstantId") == 1), "?")
            return r.get("ProjectId"), (proj_no, status_val)
    return None, None


def investigate_view_click(page, label, host, pid, project_id, proj_info):
    proj_no, status_val = proj_info
    print(f"--- {label}: {proj_no}, ProjectId={project_id!r}, status={status_val!r} ---")
    url = f"https://{host}/MvcProjects/LandingPage?ProgramId={pid}&ProjectId={project_id}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(6000)
        print("landing page URL:", page.url)

        for dismiss_label in ("Got it", "Close", "Dismiss", "×", "OK"):
            try:
                btn = page.get_by_text(dismiss_label, exact=False).first
                if btn.is_visible(timeout=1000):
                    btn.click(timeout=2000)
                    print(f"dismissed a modal via label {dismiss_label!r}")
                    page.wait_for_timeout(1000)
                    break
            except Exception:
                pass

        n_tables = page.locator("table").count()
        comm_table = None
        for i in range(n_tables):
            t = page.locator("table").nth(i)
            try:
                if "Subject" in t.inner_text() and "View" in t.inner_text():
                    comm_table = t
                    break
            except Exception:
                continue
        if comm_table is None:
            print("could not find the Communications table on this page")
            return

        view_el = comm_table.get_by_text("View", exact=True).first
        try:
            with page.context.expect_page(timeout=8000):
                view_el.click(force=True, timeout=8000)
            print("a new tab opened (unexpected given prior runs -- check its URL manually)")
        except Exception:
            pass
        page.wait_for_timeout(1500)

        n_dialogs = page.locator("[role='dialog'], .modal, .modal-content").count()
        mfa_seen = False
        for i in range(min(n_dialogs, 3)):
            d = page.locator("[role='dialog'], .modal, .modal-content").nth(i)
            try:
                txt = d.inner_text()[:300]
                if "Multi-Factor" in txt or "MFA" in txt:
                    mfa_seen = True
                print(f"  dialog[{i}]: {txt[:200].strip()!r}")
            except Exception:
                pass
        print(f"MFA-required modal seen: {mfa_seen}")
    except Exception as e:
        print(f"{label} investigation failed:", e)
    print()


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

        print("=== Q1/Q4/Q5: UI ===")
        pid, info = find_blocked_project(page, UI[0], UI[1], ["corrections required"])
        if pid:
            investigate_view_click(page, "UI", UI[0], UI[1], pid, info)
        else:
            print("no blocked UI project found in first page of rows")

        print("=== Q1/Q4/Q5: Eversource ===")
        pid2, info2 = find_blocked_project(
            page, EVERSOURCE[0], EVERSOURCE[1], ["customer action required"])
        if pid2:
            investigate_view_click(page, "EVERSOURCE", EVERSOURCE[0], EVERSOURCE[1], pid2, info2)
        else:
            print("no blocked Eversource project found in first page of rows")

        # Q3 (Eversource Export to CSV) was tried twice: the button is real and
        # clickable, but it fires neither a download event nor a new tab within
        # 40s -- inconclusive, and moot now that Q1 gives ProjectId directly.

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
