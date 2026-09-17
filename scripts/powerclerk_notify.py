"""Zoho-only SMS digest of newly-blocked interconnection projects and PTO milestones.

Reads Zoho, sends at most one SMS (a digest, never one per project), and
stamps IC_Alert_Sent / IC_PTO_Alert_Sent. Never touches PowerClerk -- that is
powerclerk_sync.py's job, kept in a separate off-hours workflow so a scrape
outage doesn't also block this from reporting on it.

Message rules (CLAUDE.md):
  1. One SMS per run, a digest.
  2. Cap at 8 total new events (blocked + PTO combined). More than that means
     a scraper bug, not that many things happening overnight -- send a
     "check Canvas" message and stamp nothing, so the real alerts survive
     for the next run.
  3. Send nothing when there is nothing new. No "all clear" texts.
  4. If IC_Portal_Checked is stale on most active records, send a staleness
     warning instead of a digest -- the data isn't trustworthy.

IC_Alert_Sent / IC_PTO_Alert_Sent are stamped only after Twilio confirms the
send (never first -- a failed send that stamped would silently swallow the
alert forever), and only if every configured recipient's send succeeded.

PTO announcements (2026-09-17) require the IC_PTO_Alert_Sent field (datetime)
on Installs. If it doesn't exist yet, this script detects that at startup
and disables PTO announcements for the run without failing -- the blocker
digest keeps working either way. Once that field is created, run
--backfill-pto-alert-sent once before letting this run live: Utility_PTO is
being backfilled on every already-PTO'd install by powerclerk_sync.py's new
PTO-fill logic, and without a backfill they'd all announce at once the first
time this runs with the field present.
"""
import argparse
import datetime
import os
import sys

import requests

# main.py lives at the repo root; this script lives in scripts/.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import _send_sms  # noqa: E402  -- reuse the one Twilio client, don't write a new one

API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

INACTIVE_STAGES = ["Canceled", "Project Closeout"]
STALE_AFTER_HOURS = 36
MAX_DIGEST = 8
PTO_ALERT_FIELD = "IC_PTO_Alert_Sent"


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


def pto_field_available(token):
    """Whether IC_PTO_Alert_Sent actually exists in Zoho.

    Verified empirically (2026-09-17) that neither a GET with an unknown name
    in `fields=` nor a PUT naming an unknown field in `data` errors -- Zoho
    returns HTTP 200 / status "success" either way and just silently drops
    the unrecognized key (a PUT to a real record ID still legitimately
    updates Modified_Time, so it isn't even a no-op). So the only reliable
    test found is: fetch one real record directly by ID with no `fields`
    param (which returns every REAL field, nulls included) and check whether
    the key is present at all.
    """
    r = requests.get(f"{API_DOMAIN}/crm/v7/Installs", headers=zoho_headers(token),
                      params={"fields": "id", "per_page": 1}, timeout=30)
    if r.status_code != 200:
        return False
    data = r.json().get("data", [])
    if not data:
        return False
    rr = requests.get(f"{API_DOMAIN}/crm/v7/Installs/{data[0]['id']}",
                       headers=zoho_headers(token), timeout=30)
    if rr.status_code != 200:
        return False
    rec = rr.json().get("data", [{}])[0]
    return PTO_ALERT_FIELD in rec


def fetch_active_installs(token, include_pto_alert=False):
    criteria = "and".join(f"(Project_Stage:not_equal:{s})" for s in INACTIVE_STAGES)
    fields = ("id,Name,IC_Portal_Status,IC_Action_Required,IC_Alert_Sent,"
              "IC_Portal_Checked,Utility_PTO")
    if include_pto_alert:
        fields += f",{PTO_ALERT_FIELD}"
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
        r.raise_for_status()
        body = r.json()
        results.extend(body.get("data", []))
        if not body.get("info", {}).get("more_records"):
            break
        page += 1
    return results


def update_install(record_id, fields, token):
    body = {
        "data": [{"id": record_id, **fields}],
        "trigger": [],
        "skip_feature_execution": [{"name": "assignment_rules"}, {"name": "connected_workflows"}],
    }
    r = requests.put(f"{API_DOMAIN}/crm/v7/Installs", headers=zoho_headers(token),
                      json=body, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code} | {r.text[:300]}")
    result = r.json().get("data", [{}])[0]
    if result.get("status") != "success":
        raise RuntimeError(f"Zoho rejected update: {result}")


def _parse_checked(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(ts)
    except ValueError:
        return None


def staleness_check(installs):
    """Return a 'stale since <date>' message, or None if data is fresh enough to alert on."""
    if not installs:
        return None
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - datetime.timedelta(hours=STALE_AFTER_HOURS)
    checked_times = [_parse_checked(i.get("IC_Portal_Checked")) for i in installs]
    stale_count = sum(1 for t in checked_times if t is None or t < cutoff)
    if stale_count <= len(installs) / 2:
        return None
    fresh = [t for t in checked_times if t is not None]
    last_success = max(fresh).date().isoformat() if fresh else "unknown"
    return f"Portal sync stale since {last_success}"


def send_digest(text, recipients):
    """Send the same message to every recipient. Returns True only if ALL sends
    were confirmed by Twilio -- a partial failure must not suppress the retry."""
    if not recipients:
        print("  FAIL: no recipients configured (ALERT_SMS_TO empty)")
        return False
    all_ok = True
    for to in recipients:
        sid = _send_sms(to, text)
        print(f"  SMS to {to}: {'sent sid=' + sid if sid else 'FAILED'}")
        all_ok = all_ok and bool(sid)
    return all_ok


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                    help="Compute and print what would be sent/stamped. Sends and writes nothing.")
    p.add_argument("--backfill-alert-sent", action="store_true",
                    help="One-time maintenance: stamp IC_Alert_Sent=now on every record "
                         "currently at IC_Action_Required=true, then exit. No SMS. Run this "
                         "before the first live run so pre-existing blockers don't all fire "
                         "at once through the cap.")
    p.add_argument("--backfill-pto-alert-sent", action="store_true",
                    help="One-time maintenance: stamp IC_PTO_Alert_Sent=now on every record "
                         "that already has Utility_PTO set, then exit. No SMS. Run this once "
                         "after creating IC_PTO_Alert_Sent in Zoho, before letting PTO "
                         "announcements go live -- otherwise every already-PTO'd install "
                         "announces at once the first time this runs with the field present.")
    p.add_argument("--test-sms", metavar="TEXT",
                    help="Send TEXT verbatim to every configured recipient and exit. "
                         "Bypasses Zoho entirely -- for confirming Twilio delivery only.")
    return p.parse_args()


def main():
    args = parse_args()
    recipients = [t.strip() for t in os.environ.get("ALERT_SMS_TO", "").split(",") if t.strip()]

    if args.test_sms:
        print(f"test SMS to {recipients}: {args.test_sms!r}")
        ok = send_digest(args.test_sms, recipients)
        print("all recipients confirmed" if ok else "at least one recipient FAILED")
        return 0 if ok else 1

    token = get_zoho_token()
    pto_enabled = pto_field_available(token)
    if not pto_enabled:
        print(f"NOTE: {PTO_ALERT_FIELD} not found in Zoho -- PTO announcements disabled "
              f"for this run (blocker digest is unaffected). Create it to enable them.")

    if args.backfill_pto_alert_sent:
        if not pto_enabled:
            print(f"FAIL: {PTO_ALERT_FIELD} does not exist -- create it in Zoho first")
            return 1
        installs = fetch_active_installs(token, include_pto_alert=True)
        pto_done = [i for i in installs if (i.get("Utility_PTO") or "").strip()]
        print(f"{len(pto_done)} installs already have Utility_PTO set")
        if args.dry_run:
            for i in pto_done:
                print(f"  [dry-run] would stamp {PTO_ALERT_FIELD}: {i.get('Name')}")
            return 0
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        failed = 0
        for i in pto_done:
            try:
                update_install(i["id"], {PTO_ALERT_FIELD: now}, token)
                print(f"  stamped {i.get('Name')}")
            except Exception as e:
                failed += 1
                print(f"  FAIL stamping {i.get('Name')}: {e}")
        print(f"backfill done: {len(pto_done) - failed} stamped, {failed} failures")
        return 1 if failed else 0

    installs = fetch_active_installs(token, include_pto_alert=pto_enabled)
    print(f"{len(installs)} active installs in Zoho")

    if args.backfill_alert_sent:
        blocked = [i for i in installs if i.get("IC_Action_Required")]
        print(f"{len(blocked)} currently blocked (IC_Action_Required=true)")
        if args.dry_run:
            for i in blocked:
                print(f"  [dry-run] would stamp IC_Alert_Sent: {i.get('Name')}")
            return 0
        now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        failed = 0
        for i in blocked:
            try:
                update_install(i["id"], {"IC_Alert_Sent": now}, token)
                print(f"  stamped {i.get('Name')}")
            except Exception as e:
                failed += 1
                print(f"  FAIL stamping {i.get('Name')}: {e}")
        print(f"backfill done: {len(blocked) - failed} stamped, {failed} failures")
        return 1 if failed else 0

    stale_msg = staleness_check(installs)
    if stale_msg:
        print(f"staleness check FAILED: {stale_msg}")
        if args.dry_run:
            print(f"  [dry-run] would send: {stale_msg!r}")
            return 0
        send_digest(stale_msg, recipients)  # not stamped -- there's nothing to stamp
        return 0

    newly_blocked = [
        i for i in installs
        if i.get("IC_Action_Required") and not i.get("IC_Alert_Sent")
    ]
    print(f"{len(newly_blocked)} newly-blocked (action required, not yet alerted)")

    newly_pto = []
    if pto_enabled:
        newly_pto = [i for i in installs
                     if (i.get("Utility_PTO") or "").strip() and not i.get(PTO_ALERT_FIELD)]
        print(f"{len(newly_pto)} newly-PTO'd (not yet announced)")

    total = len(newly_blocked) + len(newly_pto)
    if total == 0:
        print("nothing new -- no SMS sent")
        return 0

    if total > MAX_DIGEST:
        msg = f"{total} new events -- check Canvas, likely a sync issue"
        print(msg)
        if args.dry_run:
            print(f"  [dry-run] would send: {msg!r}")
            return 0
        send_digest(msg, recipients)  # stamp nothing -- let real alerts survive for next run
        return 0

    parts = []
    if newly_blocked:
        lines = [f"{i.get('Name')}: {i.get('IC_Portal_Status') or '(no status)'}"
                  for i in newly_blocked]
        parts.append(f"{len(newly_blocked)} newly blocked:\n" + "\n".join(lines))
    if newly_pto:
        pto_lines = [f"{i.get('Name')}: PTO {i.get('Utility_PTO')}" for i in newly_pto]
        parts.append(f"{len(newly_pto)} PTO granted:\n" + "\n".join(pto_lines))
    msg = "\n\n".join(parts)

    if args.dry_run:
        print("=== DRY RUN -- would send this digest ===")
        print(msg)
        print(f"=== to: {recipients} ===")
        print(f"would then stamp IC_Alert_Sent on {len(newly_blocked)} record(s), "
              f"{PTO_ALERT_FIELD} on {len(newly_pto)} record(s)")
        return 0

    if not send_digest(msg, recipients):
        print("send not fully confirmed -- nothing stamped, will retry next run")
        return 1

    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    failed = 0
    for i in newly_blocked:
        try:
            update_install(i["id"], {"IC_Alert_Sent": now}, token)
        except Exception as e:
            failed += 1
            print(f"  FAIL stamping IC_Alert_Sent for {i.get('Name')}: {e}")
    for i in newly_pto:
        try:
            update_install(i["id"], {PTO_ALERT_FIELD: now}, token)
        except Exception as e:
            failed += 1
            print(f"  FAIL stamping {PTO_ALERT_FIELD} for {i.get('Name')}: {e}")

    print(f"done: digest sent, {total - failed} stamped, {failed} stamp failures")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
