"""PowerClerk smoke test v3 — read-only.

v2 proved: login at uinet works (#UserName / #Password at
/MvcAccount/Login?ProgramId=...), 196 UI projects visible.

v2 failed on Eversource because it used a fresh browser context per program.
PowerClerk shares the session across programs, and esctinterconnect has no
public login page of its own — it bounces signed-out users to eversource.com.
So: log in ONCE, then visit every program in the same context.
"""
import os, pathlib, re, sys
from playwright.sync_api import sync_playwright

ART = pathlib.Path("artifacts"); ART.mkdir(exist_ok=True)
USER, PASS = os.environ.get("PC_USER", ""), os.environ.get("PC_PASS", "")

LOGIN_HOST, LOGIN_PID = "uinet.powerclerk.com", "MS7SMA7Q77CM"
PROGRAMS = [
    ("UI",         "uinet.powerclerk.com",            "MS7SMA7Q77CM"),
    ("Eversource", "esctinterconnect.powerclerk.com", "28X4MJXZUR42"),
]


def shot(page, name):
    try:
        page.screenshot(path=str(ART / f"{name}.png"), full_page=True)
    except Exception as e:
        print(f"  (screenshot {name} failed: {e})")


def main():
    if not USER or not PASS:
        print("FAIL: credentials missing."); return 1
    print(f"Credentials loaded. Username length={len(USER)}, password length={len(PASS)}.")

    ok = True
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1600, "height": 1200})
        page = ctx.new_page()

        # ---- one login for all programs ----
        print("\n=== login ===")
        page.goto(f"https://{LOGIN_HOST}/MvcAccount/Login?ProgramId={LOGIN_PID}",
                  wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(1500)
        if not page.locator("#UserName").count():
            print("  FAIL: no login form"); shot(page, "login-no-form"); return 1
        page.locator("#UserName").fill(USER)
        page.locator("#Password").fill(PASS)
        page.locator("#Password").press("Enter")
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)
        shot(page, "login-after")
        print(f"  landed: {page.url}")
        if "/MvcAccount/Login" in page.url:
            print("  FAIL: still on login page"); return 1
        print("  logged in")

        # ---- each program, same session ----
        for label, host, pid in PROGRAMS:
            print(f"\n=== {label} ===")
            page.goto(f"https://{host}/MvcProjects/ProjectList?ProgramId={pid}",
                      wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            shot(page, f"{label}-list")
            body = page.inner_text("body")
            m = re.search(r"([\d,]+)\s+Projects?\s+Found", body, re.I)
            if m:
                print(f"  SUCCESS: {m.group(1)} projects visible")
            else:
                ok = False
                print(f"  FAIL: no count. url={page.url} title={page.title()!r}")
                print("  body[:300]: " + body[:300].replace("\n", " | "))

        browser.close()

    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
