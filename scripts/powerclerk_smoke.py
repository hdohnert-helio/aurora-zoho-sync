"""PowerClerk smoke test v2 — read-only.

v1 findings: login form uses #UserName / #Password and lives at
/MvcAccount/Login?ProgramId=...  Going to a project list URL while signed out
redirects to that page for UI, but bounces to the utility's public website for
Eversource — so we navigate to the login page directly instead of relying on
the redirect.

Submits the form itself rather than hunting for a button ("Log In" appears more
than once on the page). Reports any validation error verbatim so a rejected
credential is obvious.

NOTE: only ONE login attempt per program per run, to avoid tripping any
account lockout.
"""
import os
import pathlib
import re
import sys

from playwright.sync_api import sync_playwright

ART = pathlib.Path("artifacts")
ART.mkdir(exist_ok=True)

USER = os.environ.get("PC_USER", "")
PASS = os.environ.get("PC_PASS", "")

PROGRAMS = [
    ("UI",         "uinet.powerclerk.com",           "MS7SMA7Q77CM"),
    ("Eversource", "esctinterconnect.powerclerk.com", "28X4MJXZUR42"),
]

ERROR_HINTS = ["invalid", "incorrect", "not recognized", "locked", "disabled",
               "try again", "does not match", "unsuccessful"]


def shot(page, name):
    try:
        page.screenshot(path=str(ART / f"{name}.png"), full_page=True)
    except Exception as e:
        print(f"  (screenshot {name} failed: {e})")


def check(page, label, list_url):
    body = page.inner_text("body")
    m = re.search(r"([\d,]+)\s+Projects?\s+Found", body, re.I)
    if m:
        print(f"  SUCCESS: {m.group(1)} projects visible")
        return True
    print(f"  FAIL: no 'Projects Found' count. url={page.url}")
    print(f"  title={page.title()!r}")
    for line in body.splitlines():
        s = line.strip()
        if s and any(h in s.lower() for h in ERROR_HINTS) and len(s) < 200:
            print(f"  page message: {s!r}")
    print("  body[:300]: " + body[:300].replace("\n", " | "))
    return False


def main():
    if not USER or not PASS:
        print("FAIL: credentials missing from environment.")
        return 1
    print(f"Credentials loaded. Username length={len(USER)}, password length={len(PASS)}.")

    ok = True
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for label, host, pid in PROGRAMS:
            print(f"\n=== {label} ===")
            ctx = browser.new_context(viewport={"width": 1600, "height": 1200})
            page = ctx.new_page()
            login_url = f"https://{host}/MvcAccount/Login?ProgramId={pid}"
            list_url = f"https://{host}/MvcProjects/ProjectList?ProgramId={pid}"

            page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            shot(page, f"{label}-01-login-page")
            print(f"  login page: {page.url}")

            user_field = page.locator("#UserName")
            if not user_field.count():
                print("  FAIL: no #UserName field on the login page")
                shot(page, f"{label}-02-no-form")
                ok = False
                ctx.close()
                continue

            user_field.fill(USER)
            page.locator("#Password").fill(PASS)
            shot(page, f"{label}-02-filled")

            # Submit the form itself — "Log In" text appears more than once.
            try:
                page.locator("#Password").press("Enter")
            except Exception:
                page.evaluate("document.querySelector('form').submit()")

            page.wait_for_load_state("domcontentloaded", timeout=60000)
            page.wait_for_timeout(3500)
            shot(page, f"{label}-03-after-login")
            print(f"  after login: {page.url}")

            if "/MvcAccount/Login" in page.url:
                print("  FAIL: still on the login page after submitting")
                body = page.inner_text("body")
                for line in body.splitlines():
                    s = line.strip()
                    if s and any(h in s.lower() for h in ERROR_HINTS) and len(s) < 200:
                        print(f"  page message: {s!r}")
                ok = False
                ctx.close()
                continue

            page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(3000)
            shot(page, f"{label}-04-project-list")
            if not check(page, label, list_url):
                ok = False
            ctx.close()

        browser.close()

    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
