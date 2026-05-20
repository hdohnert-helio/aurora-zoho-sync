"""
Interconnection status monitor.

Three-step loop per run:
  1. Pull IC watchlist from Zoho (Installs in active project stages)
  2. Search Gmail label:_INTERCONNECTIONS for matching emails per install
  3. Classify via keyword rules → update Utility_Status + IC_Project_Number + append Note
"""

import base64
import json
import logging
import os
import re

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# Project stages whose installs are in scope for IC monitoring
IC_ACTIVE_STAGES = [
    "Interconnection",
    "Permitting",
    "Installed",
    "Post-Install",
]

GMAIL_IMPERSONATE = os.getenv("GMAIL_IMPERSONATE_EMAIL", "installs@helio.solar")
GMAIL_LABEL = "_INTERCONNECTIONS"
GMAIL_LOOKBACK_DAYS = 45
GMAIL_MAX_RESULTS = 10

# ── Keyword classification rules ─────────────────────────────────────────────
# Each rule is (compiled_pattern, status_value, confidence).
# Rules are evaluated against the subject + body and checked in order.
# First match wins.

_IC_NUM_RE = re.compile(r"\b((?:INT|DER)-\d{4,8})\b", re.IGNORECASE)

# Emails matching any of these are silently skipped — no note, no update.
# Used for internal Helio workflow emails that land in the same mailbox.
_IGNORE_PATTERNS = [
    re.compile(r"plan set request.*(helio solar|delivered|completed)", re.I),
    re.compile(r"pe stamp request.*(helio solar|delivered|completed)", re.I),
    re.compile(r"electric bill", re.I),
]

_RULES = [
    # Contingent approval — must come before PTO because utility emails about
    # contingent approval often contain "permission to operate" language in the body
    (re.compile(r"contingent\s+approv.{0,60}upgrade|upgrade.{0,60}contingent\s+approv", re.I),
     "Contingent Approval (with Upgrade)", "high"),
    (re.compile(r"contingent\s+approv.{0,60}as.is|as.is.{0,60}contingent\s+approv", re.I),
     "Contingent Approval (As Is)", "high"),
    (re.compile(r"contingent\s+approv(al)?\s+to\s+interconnect", re.I),
     "Contingent Approval (As Is)", "high"),
    (re.compile(r"contingent\s+approv|contingent\s+interconnect", re.I),
     "Contingent Approval (As Is)", "medium"),

    # Terminal / green states
    # Meter Swap must come before PTO — Eversource meter change emails mention
    # "permission to operate" in the body (once meter is installed), which would
    # otherwise trigger a false PTO match.
    (re.compile(r"meter\s+(swap|change|set|install)", re.I),
     "Meter Swap", "high"),
    # UI (United Illuminating) uses "Approval to Energize"; Eversource uses "Permission to Operate"
    (re.compile(r"permission\s+to\s+operate|pto\s+granted|\bpto\b.*granted|approval\s+to\s+energize", re.I),
     "Permission to Operate", "high"),
    (re.compile(r"witness\s+test.*complet|witness\s+test.*pass", re.I),
     "Witness Test Complete", "high"),
    (re.compile(r"witness\s+test.*schedul|schedule.*witness\s+test", re.I),
     "Witness Test Schedule", "high"),
    (re.compile(r"waiting\s+for\s+town|municipal\s+approv|town\s+approv", re.I),
     "Waiting for Town Approval", "high"),

    # Fast track
    (re.compile(r"fast\s*track", re.I),
     "Fast Track", "high"),

    # ON HOLD — must come before RRES/validation rules to avoid false matches
    (re.compile(r"on\s+hold.{0,40}hea|hea.{0,40}on\s+hold", re.I),
     "Application On Hold - HEA", "high"),
    (re.compile(r"application\s+validation\s+on\s+hold|validation\s+on\s+hold|on\s+hold.*response\s+required", re.I),
     "App Signed by Client - IC On Hold", "high"),
    (re.compile(r"application\s+(validation\s+)?on\s+hold|placed\s+on\s+hold", re.I),
     "App Signed by Client - IC On Hold", "medium"),

    # Corrections received from customer = back in review after hold
    (re.compile(r"corrections\s+received", re.I),
     "Resubmitted - RRES Review", "high"),

    # Technical review
    (re.compile(r"resubmit.{0,40}technical\s+review|technical\s+review.{0,40}resubmit", re.I),
     "Resubmitted - Technical Review", "high"),
    (re.compile(r"technical\s+review", re.I),
     "Technical Review", "high"),

    # RRES review — specific enough to not match ON HOLD subjects
    (re.compile(r"resubmit.{0,40}rres|rres.{0,40}resubmit", re.I),
     "Resubmitted - RRES Review", "high"),
    (re.compile(r"rres\s+review|validation\s+complete|application\s+validation\s+complete", re.I),
     "RRES Review", "high"),

    # Submission / signature states
    (re.compile(r"disclosure\s+form.*interconnection|interconnection.*disclosure\s+form", re.I),
     "App Sent for Client Signature", "high"),
    (re.compile(r"sent\s+for.*signature|signature\s+request", re.I),
     "App Sent for Client Signature", "high"),
    (re.compile(r"application\s+receipt\b", re.I),
     "Signed App Submitted", "high"),
    (re.compile(r"res\s+customer\s+edu", re.I),
     "Signed App Submitted", "medium"),
    # DocuSign completion for IC-related documents
    (re.compile(r"document\s+.{0,80}(interconnection|renewable energy|tariff application).{0,80}has\s+been\s+completed", re.I),
     "Signed App Submitted", "high"),
    (re.compile(r"signed\s+app.*submitted|application.*submitted|app.*submitted", re.I),
     "Signed App Submitted", "high"),
]


_IGNORE = object()  # sentinel

def classify_email(install, subject, body):
    """
    Rule-based classifier. Returns a dict or the _IGNORE sentinel.
    Callers should skip the email entirely when _IGNORE is returned.
    """
    text = f"{subject} {body}"

    # Silently skip internal / irrelevant emails
    for pattern in _IGNORE_PATTERNS:
        if pattern.search(subject):
            return _IGNORE

    # Always try to extract IC project number
    ic_match = _IC_NUM_RE.search(text)
    ic_project_number = ic_match.group(1).upper() if ic_match else None

    new_status = None
    confidence = "low"
    note = ""

    for pattern, status, conf in _RULES:
        if pattern.search(text):
            new_status = status
            confidence = conf
            note = f'Matched rule for "{status}" on subject: {subject!r}'
            break

    if not new_status:
        note = f"No status rule matched. Subject: {subject!r}"

    return {
        "new_status": new_status,
        "ic_project_number": ic_project_number,
        "confidence": confidence,
        "note": note,
    }


# ── Zoho helpers ─────────────────────────────────────────────────────────────

def _zoho_headers(token):
    return {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type": "application/json",
    }


def fetch_ic_watchlist(token, api_domain):
    # Build OR criteria across all active stages
    criteria = "or".join(
        f"(Project_Stage:equals:{stage})" for stage in IC_ACTIVE_STAGES
    )
    fields = "id,Name,Site_Location,IC_Project_Number,Utility_Status,Utility_Provider"
    url = f"{api_domain}/crm/v2/Installs/search"

    results = []
    page = 1
    while True:
        resp = requests.get(
            url,
            headers=_zoho_headers(token),
            params={"criteria": criteria, "fields": fields, "page": page, "per_page": 200},
            timeout=30,
        )
        if resp.status_code == 204:  # no records
            break
        resp.raise_for_status()
        data = resp.json().get("data", [])
        results.extend(data)
        if not resp.json().get("info", {}).get("more_records"):
            break
        page += 1

    return results


def update_install_fields(install_id, fields, token, api_domain):
    resp = requests.put(
        f"{api_domain}/crm/v2/Installs/{install_id}",
        headers=_zoho_headers(token),
        json={"data": [fields]},
        timeout=30,
    )
    resp.raise_for_status()


def add_install_note(install_id, title, content, token, api_domain):
    resp = requests.post(
        f"{api_domain}/crm/v2/Installs/{install_id}/Notes",
        headers=_zoho_headers(token),
        json={"data": [{"Note_Title": title, "Note_Content": content}]},
        timeout=30,
    )
    resp.raise_for_status()


# ── Gmail helpers ─────────────────────────────────────────────────────────────

def _build_gmail_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/gmail.readonly"],
    )
    delegated = creds.with_subject(GMAIL_IMPERSONATE)
    return build("gmail", "v1", credentials=delegated, cache_discovery=False)


def _gmail_query_for_install(install):
    ic_num = (install.get("IC_Project_Number") or "").strip()
    if ic_num:
        return f'"{ic_num}" newer_than:{GMAIL_LOOKBACK_DAYS}d'
    address = (install.get("Site_Location") or "").strip()
    street = address.split(",")[0].strip() if address else ""
    if not street:
        return None
    return f'"{street}" newer_than:{GMAIL_LOOKBACK_DAYS}d'


def _decode_part(part):
    data = part.get("body", {}).get("data", "")
    if not data:
        return ""
    return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")


def _walk_payload(payload):
    mime = payload.get("mimeType", "")
    if mime == "text/plain":
        return _decode_part(payload)
    if mime == "text/html":
        return re.sub(r"<[^>]+>", " ", _decode_part(payload))
    for part in payload.get("parts", []):
        text = _walk_payload(part)
        if text:
            return text
    return ""


def _extract_subject_body(msg):
    headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
    subject = headers.get("Subject", "")
    body = _walk_payload(msg.get("payload", {}))
    return subject, body


def fetch_recent_emails_for_install(gmail, install):
    q = _gmail_query_for_install(install)
    name = install.get("Name", install.get("id"))
    if not q:
        logger.info(f"ic_monitor: no query built for {name} (missing address and IC number)")
        return []
    logger.info(f"ic_monitor: searching Gmail for {name!r} | query={q!r}")
    result = (
        gmail.users()
        .messages()
        .list(userId="me", q=q, maxResults=GMAIL_MAX_RESULTS)
        .execute()
    )
    messages = result.get("messages", [])
    logger.info(f"ic_monitor: {len(messages)} messages found for {name!r}")
    emails = []
    for m in messages:
        full = (
            gmail.users()
            .messages()
            .get(userId="me", id=m["id"], format="full")
            .execute()
        )
        subject, body = _extract_subject_body(full)
        emails.append({"id": m["id"], "subject": subject, "body": body})
    return emails


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_ic_monitor(get_zoho_token_fn):
    """
    Main entry point. Pass in get_zoho_access_token from main.py to avoid
    a circular import at module load time.
    """
    token = get_zoho_token_fn()
    if not token:
        logger.error("ic_monitor: failed to get Zoho token")
        return {"status": "failed", "reason": "no zoho token"}

    api_domain = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")

    watchlist = fetch_ic_watchlist(token, api_domain)
    logger.info(f"ic_monitor: {len(watchlist)} installs in watchlist")

    if not watchlist:
        return {"status": "ok", "watchlist": 0, "emails_processed": 0,
                "records_updated": 0, "flagged_for_review": 0}

    gmail = _build_gmail_service()
    logger.info(f"ic_monitor: searching Gmail as {GMAIL_IMPERSONATE}")
    emails_processed = 0
    records_updated = 0
    flagged_for_review = 0

    for install in watchlist:
        install_id = install["id"]
        name = install.get("Name", install_id)

        try:
            emails = fetch_recent_emails_for_install(gmail, install)
        except Exception:
            logger.exception(f"ic_monitor: Gmail fetch failed for {name}")
            continue

        if not emails:
            continue

        for email in emails:
            result = classify_email(install, email["subject"], email["body"])

            if result is _IGNORE:
                logger.info(f"ic_monitor: ignored internal email for {name} — {email['subject']!r}")
                continue

            emails_processed += 1
            confidence = result["confidence"]
            new_status = result["new_status"]
            ic_num = result["ic_project_number"]
            note_text = result["note"]

            if confidence == "low":
                try:
                    add_install_note(
                        install_id,
                        "IC Email – Needs Review",
                        f"Subject: {email['subject']}\n\n{note_text}",
                        token, api_domain,
                    )
                except Exception:
                    logger.exception(f"ic_monitor: note write failed for {name}")
                flagged_for_review += 1
                continue

            updates = {}
            if new_status and new_status != install.get("Utility_Status"):
                updates["Utility_Status"] = new_status
            if ic_num and not install.get("IC_Project_Number"):
                updates["IC_Project_Number"] = ic_num

            if updates:
                try:
                    update_install_fields(install_id, updates, token, api_domain)
                    if "Utility_Status" in updates:
                        install["Utility_Status"] = updates["Utility_Status"]
                    if "IC_Project_Number" in updates:
                        install["IC_Project_Number"] = updates["IC_Project_Number"]
                    records_updated += 1
                    logger.info(f"ic_monitor: updated {name} — {updates}")
                except Exception:
                    logger.exception(f"ic_monitor: field update failed for {name}")

            note_title = (
                f"IC Update – {new_status}" if new_status else "IC Email – No Change"
            )
            try:
                add_install_note(install_id, note_title, note_text, token, api_domain)
            except Exception:
                logger.exception(f"ic_monitor: note write failed for {name}")

    summary = {
        "status": "ok",
        "watchlist": len(watchlist),
        "emails_processed": emails_processed,
        "records_updated": records_updated,
        "flagged_for_review": flagged_for_review,
    }
    logger.info(f"ic_monitor: complete — {summary}")
    return summary


# ── Note cleanup ──────────────────────────────────────────────────────────────

# Patterns in note content that indicate a bad/noisy note to delete.
# Only keep patterns for emails that should NEVER produce a note (internal noise).
# Do NOT add patterns that match content from correctly classified utility emails —
# the note content always includes the subject line, so subject-based patterns
# will delete good notes too.
_CLEANUP_CONTENT_PATTERNS = [
    re.compile(r"Plan Set Request", re.I),
    re.compile(r"PE Stamp Request", re.I),
    re.compile(r"electric bill", re.I),
]


def clean_ic_notes(get_zoho_token_fn):
    """
    Delete IC monitor notes that were created in error:
    - "IC Email – Needs Review" notes for internal Helio workflow emails
    - "IC Update – RRES Review" notes that were actually ON HOLD misclassifications
    """
    token = get_zoho_token_fn()
    if not token:
        return {"status": "failed", "reason": "no zoho token"}

    api_domain = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")
    headers = _zoho_headers(token)

    # Fetch IC-related notes across all Install records, paginated
    deleted = 0
    skipped = 0
    page = 1

    while True:
        resp = requests.get(
            f"{api_domain}/crm/v2/Notes",
            headers=headers,
            params={
                "se_module": "Installs",
                "fields": "id,Note_Title,Note_Content",
                "criteria": "(Note_Title:starts_with:IC)",
                "page": page,
                "per_page": 200,
            },
            timeout=30,
        )
        if resp.status_code == 204:
            break
        resp.raise_for_status()
        notes = resp.json().get("data", [])

        for note in notes:
            content = (note.get("Note_Content") or "") + " " + (note.get("Note_Title") or "")
            if any(p.search(content) for p in _CLEANUP_CONTENT_PATTERNS):
                try:
                    del_resp = requests.delete(
                        f"{api_domain}/crm/v2/Notes/{note['id']}",
                        headers=headers,
                        timeout=30,
                    )
                    del_resp.raise_for_status()
                    logger.info(f"ic_cleanup: deleted note {note['id']} — {note.get('Note_Title')!r}")
                    deleted += 1
                except Exception:
                    logger.exception(f"ic_cleanup: failed to delete note {note['id']}")
            else:
                skipped += 1

        if not resp.json().get("info", {}).get("more_records"):
            break
        page += 1

    summary = {"status": "ok", "deleted": deleted, "kept": skipped}
    logger.info(f"ic_cleanup: complete — {summary}")
    return summary
