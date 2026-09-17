"""PowerClerk smoke test — proves the credential chain works. Read-only.

Verifies: 1Password service account -> credentials -> Playwright login ->
project list is reachable for both UI and Eversource programs.

Writes nothing anywhere. Screenshots every step into artifacts/ so a failed
run tells us what the page actually looked like.
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
    ("UI",         "https://uinet.powerclerk.com/MvcProjects/ProjectList?ProgramId=MS7SMA7Q77CM"),
    ("Eversource", "https://esctinterconnect.powerclerk.com/MvcProjects/ProjectList?ProgramId=28X4MJXZUR42"),
]

# Candidate selectors — PowerClerk login form not yet mapped, so try several.
USER_SEL = ["#Email", "#UserName", "input[name='Email']", "input[name='UserName']",
            "input[type='email']", "input[name='username']"]
PASS_SEL = ["#Password", "input[name='Password']", "input[type='password']"]
SUBMIT_SEL = ["button[type='submit']", "input[type='submit']",
              "text=Sign In", "text=Log In", "text=Login"]


def first_visible(page, selectors):
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el.count() and el.is_visible():
                return el, sel
        except Exception:
            continue
    return None, None


def shot(page, name):
    try:
        page.screenshot(path=str(ART / f"{name}.png"), full_page=True)
    except Exception as e:
        print(f"  (screenshot {name} failed: {e})")


def main():
    if not USER or not PASS:
        print("FAIL: credentials not present in environment. "
              "Check the 1Password item path and the service account token.")
        return 1

    print(f"Credentials loaded. Username length={len(USER)}, password length={len(PASS)}.")

    ok = True
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1600, "height": 1200})
        page = ctx.new_page()

        for label, url in PROGRAMS:
            print(f"\n=== {label} ===")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)
            shot(page, f"{label}-01-landing")
            print(f"  landed on: {page.url}")

            # If we were bounced to a login page, sign in.
            u_el, u_sel = first_visible(page, USER_SEL)
            if u_el:
                print(f"  login form detected (user field: {u_sel})")
                p_el, p_sel = first_visible(page, PASS_SEL)
                if not p_el:
                    print("  FAIL: found username field but no password field.")
                    shot(page, f"{label}-02-no-password-field")
                    ok = False
                    continue
                u_el.fill(USER)
                p_el.fill(PASS)
                s_el, s_sel = first_visible(page, SUBMIT_SEL)
                if s_el:
                    print(f"  submitting via {s_sel}")
                    s_el.click()
                else:
                    print("  no submit button found; pressing Enter")
                    p_el.press("Enter")
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                page.wait_for_timeout(3000)
                shot(page, f"{label}-03-after-login")
                print(f"  after login: {page.url}")
                if page.url.rstrip("/") != url.rstrip("/"):
                    page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_timeout(2500)
            else:
                print("  no login form — already authenticated or different flow")

            shot(page, f"{label}-04-project-list")
            body = page.inner_text("body")
            m = re.search(r"([\d,]+)\s+Projects?\s+Found", body, re.I)
            if m:
                print(f"  SUCCESS: {m.group(1)} projects visible")
            else:
                print("  FAIL: could not find a 'Projects Found' count on the page")
                print("  first 400 chars of body:")
                print("  " + body[:400].replace("\n", " | "))
                ok = False

        browser.close()

    print("\nRESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
