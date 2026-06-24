from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from urllib.parse import quote
import os
import re
import requests
import datetime
import json
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


app = FastAPI()

# ------------------------
# Internal: Create Initial Snapshot After Install Creation
# ------------------------
def _create_initial_snapshot_for_install(
    install_id,
    project_id,
    deal_id=None,
    access_token=None,
    headers=None,
    api_domain=None,
):
    """
    Core logic for creating an initial snapshot. Callable from both the HTTP
    endpoint below and the bulk backfill background task. Returns a dict with
    a "status" string identical to what the endpoint returns.

    If access_token / headers / api_domain are passed in, they're reused (saves
    per-call overhead in bulk loops). Otherwise this function acquires them.
    """
    if not install_id or not project_id:
        return {"status": "failed - missing install_id or project_id"}

    if not access_token:
        access_token = get_zoho_access_token()
    if not access_token:
        return {"status": "failed - no zoho token"}

    if not headers:
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    if not api_domain:
        api_domain = os.getenv("ZOHO_API_DOMAIN")

    # Pull Install record
    install_url = f"{api_domain}/crm/v2/Installs/{install_id}"
    install_response = requests.get(install_url, headers=headers)
    if install_response.status_code != 200:
        return {"status": "failed - install lookup error"}

    install_records = install_response.json().get("data", [])
    if not install_records:
        return {"status": "failed - install not found"}
    install_data = install_records[0]

    # If Active Snapshot already exists, do nothing
    if install_data.get("Active_Snapshot"):
        return {"status": "skipped - active snapshot already exists"}

    # Pull Aurora designs for project (with 429 retry)
    tenant_id = os.getenv("AURORA_TENANT_ID")
    designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"
    designs_response = _aurora_get_with_retry(designs_url)
    if designs_response.status_code != 200:
        return {
            "status": f"failed - aurora designs pull error ({designs_response.status_code})"
        }

    designs = designs_response.json().get("designs", [])
    sold_designs = [
        d for d in designs
        if (d.get("milestone") or {}).get("milestone") == "sold"
    ]

    if len(sold_designs) != 1:
        return {
            "status": f"failed - sold design count invalid ({len(sold_designs)})"
        }

    design_id = sold_designs[0].get("id")

    # Pull full design, pricing, and summary
    design_response = pull_design(design_id)
    pricing_response = pull_pricing(design_id)
    summary_response = pull_design_summary(design_id)

    if design_response.status_code != 200 or pricing_response.status_code != 200:
        return {"status": "failed - aurora design/pricing pull error"}

    design_root = design_response.json()
    design_json = design_root.get("design", design_root)

    pricing_root = pricing_response.json()
    pricing_json = pricing_root.get("pricing", pricing_root)

    summary_json = (
        summary_response.json().get("design", {})
        if summary_response.status_code == 200
        else {}
    )

    timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()
    snapshot_name = f"{project_id[:8]} | {design_id[:8]} | INITIAL SOLD | {timestamp_now}"
    aurora_design_url = f"https://v2.aurorasolar.com/projects/{project_id}/designs/{design_id}/cad"
    aurora_project_url = f"https://v2.aurorasolar.com/projects/{project_id}/overview/dashboard"

    pricing_fields = extract_pricing_fields(design_json, pricing_json, summary_json)

    snapshot_data = {
        "Name": snapshot_name,
        "Aurora_Project_ID": project_id,
        "Aurora_Design_ID": design_id,
        "Install": {"id": install_id},
        "Deal": {"id": deal_id} if deal_id else None,
        "Snapshot_Is_Active": True,
        "Processing_Status": "Initial Locked",
        "Webhook_Received_At": timestamp_now,
        "Aurora_Design_URL": aurora_design_url,
        "Aurora_Project_URL": aurora_project_url,
        **pricing_fields,
    }

    snapshot_create_response = create_snapshot(snapshot_data, access_token)
    if snapshot_create_response.status_code not in [200, 201, 202]:
        return {"status": "failed - snapshot creation error"}

    # Zoho can return HTTP 2xx but include per-record failures in the body
    # (DUPLICATE_DATA, INVALID_DATA, etc.). Inspect data[0].code before
    # assuming details.id exists.
    try:
        resp_json = snapshot_create_response.json()
    except ValueError:
        return {"status": "failed - snapshot creation: non-JSON response"}

    data_list = resp_json.get("data") or []
    if not data_list:
        return {"status": "failed - snapshot creation: empty data array"}

    first = data_list[0]
    if first.get("code") != "SUCCESS":
        code = first.get("code") or "UNKNOWN"
        message = first.get("message") or "no message"
        # Zoho returns field-level diagnostic info in `details` for INVALID_DATA
        # and similar responses (e.g. {"api_name": "Aurora_Design_ID",
        # "expected_data_type": "string"}). Log the whole thing to make these
        # debuggable from the Render logs.
        details = first.get("details")
        logger.warning(
            f"create_initial_snapshot: snapshot creation rejected | "
            f"install_id={install_id} project_id={project_id} "
            f"code={code} message={message} details={json.dumps(details)}"
        )
        return {"status": f"failed - snapshot creation: {code} ({message})"}

    snapshot_id = (first.get("details") or {}).get("id")
    if not snapshot_id:
        return {"status": "failed - snapshot creation: missing id in response"}

    # Pull LightReach IDs from Aurora financings on this design.
    lightreach_fields = extract_lightreach_install_fields(design_id)
    if lightreach_fields:
        logger.info(
            f"create_initial_snapshot: writing LightReach fields | "
            f"install_id={install_id} keys={list(lightreach_fields.keys())}"
        )

    # Update Install with Active Snapshot, Aurora Details mirror, and LightReach fields
    update_payload = {
        "data": [
            {
                "id": install_id,
                "Active_Snapshot": {"id": snapshot_id},
                **aurora_details_from_pricing(pricing_fields),
                **lightreach_fields,
            }
        ]
    }

    update_url = f"{api_domain}/crm/v2/Installs"
    update_response = requests.put(update_url, headers=headers, json=update_payload)
    if update_response.status_code not in [200, 202]:
        return {"status": "failed - install update error"}

    ok, code, msg = _zoho_update_ok(update_response)
    if not ok:
        logger.warning(
            f"create_initial_snapshot: install update rejected by Zoho | "
            f"install_id={install_id} code={code} message={msg}"
        )
        return {"status": f"failed - install update: {code} ({msg})"}

    repair = _verify_and_repair_pricing(
        install_id, pricing_fields, headers, api_domain,
        label="create_initial_snapshot:"
    )
    if repair not in ("ok", "no_pricing_data"):
        logger.info(f"create_initial_snapshot: pricing verify result={repair} install_id={install_id}")

    return {"status": "initial snapshot created", "snapshot_id": snapshot_id, "pricing_verify": repair}


@app.post("/internal/create-initial-snapshot")
async def create_initial_snapshot(request: Request):
    try:
        body = await request.json()
        return _create_initial_snapshot_for_install(
            install_id=body.get("install_id"),
            project_id=body.get("project_id"),
            deal_id=body.get("deal_id"),
        )
    except Exception:
        logger.exception("Unhandled exception in initial snapshot creation")
        return {"status": "failed - exception"}

# ------------------------
# Internal: Sync New Aurora Users Only (efficient — skips existing)
# ------------------------
@app.post("/internal/sync-aurora-users")
async def sync_aurora_users_new_only(request: Request):
    try:
        tenant_id = os.getenv("AURORA_TENANT_ID")

        # Step 1: Pull all Aurora users
        users_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/users"
        users_response = requests.get(users_url, headers=aurora_headers())
        if users_response.status_code != 200:
            logger.error(f"Aurora users pull failed | status={users_response.status_code}")
            return {"status": "failed - aurora users pull error"}

        aurora_users = users_response.json().get("users", [])
        aurora_by_email = {
            (u.get("email") or "").strip().lower(): u
            for u in aurora_users
            if (u.get("email") or "").strip()
        }
        logger.info(f"Pulled {len(aurora_by_email)} Aurora users")

        # Pull Aurora team and partner maps once (id -> name)
        team_map = get_aurora_team_map()
        partner_map = get_aurora_partner_map()
        logger.info(f"Loaded {len(team_map)} Aurora teams, {len(partner_map)} Aurora partners")

        # Step 2: Pull all existing Zoho Sales Rep emails (paginated)
        access_token = get_zoho_access_token()
        if not access_token:
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        zoho_headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        zoho_emails = set()
        page = 1
        while True:
            list_url = f"{api_domain}/crm/v2/Sales_Reps?fields=Email&page={page}&per_page=200"
            list_response = requests.get(list_url, headers=zoho_headers)
            if list_response.status_code == 204:
                break
            if list_response.status_code != 200:
                logger.error(f"Zoho Sales Reps list failed | status={list_response.status_code}")
                return {"status": "failed - zoho list error"}
            for rec in list_response.json().get("data", []):
                email = (rec.get("Email") or "").strip().lower()
                if email:
                    zoho_emails.add(email)
            if not list_response.json().get("info", {}).get("more_records"):
                break
            page += 1

        logger.info(f"Found {len(zoho_emails)} existing Zoho Sales Reps")

        # Step 3: Only process users not already in Zoho
        new_emails = set(aurora_by_email.keys()) - zoho_emails
        if not new_emails:
            logger.info("No new Aurora users to sync")
            return {"status": "completed", "new": 0, "failed": 0}

        logger.info(f"Syncing {len(new_emails)} new users")

        created = 0
        failed = 0

        for email in new_emails:
            user = aurora_by_email[email]
            user_id = user.get("id")
            first_name = (user.get("first_name") or "").strip()
            last_name = (user.get("last_name") or "").strip()

            user_detail_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/users/{user_id}"
            user_detail_response = requests.get(user_detail_url, headers=aurora_headers())
            if user_detail_response.status_code == 200:
                detail = user_detail_response.json().get("user", {})
            else:
                logger.warning(f"Could not fetch detail for {email} | status={user_detail_response.status_code}")
                detail = {}

            account_status = detail.get("account_status") or user.get("account_status")
            phone = (detail.get("phone") or "").strip() or None
            role_id = detail.get("role_id")
            raw_team_ids = detail.get("team_ids") or []
            raw_partner_ids = detail.get("partner_ids") or []
            team_ids_str = ", ".join(raw_team_ids) or None
            team_names = ", ".join(team_map.get(tid, tid) for tid in raw_team_ids) or None
            partner_ids_str = ", ".join(raw_partner_ids) or None
            partner_names = ", ".join(partner_map.get(pid, pid) for pid in raw_partner_ids) or None
            base_ppw_min = detail.get("base_price_per_watt_min")

            full_name = f"{first_name} {last_name}".strip() or email
            is_active = account_status == "active"

            record = {
                "Name": full_name,
                "Email": email,
                "Active": is_active,
                "Aurora_User_ID": user_id,
                "Aurora_Role_ID": role_id,
                "Aurora_Team_IDs": team_ids_str,
                "Aurora_Team_Names": team_names,
                "Aurora_Partner_IDs": partner_ids_str,
                "Aurora_Partner_Names": partner_names,
                "Aurora_Base_PPW_Min": base_ppw_min,
            }
            if phone:
                record["Phone"] = phone

            create_resp = requests.post(
                f"{api_domain}/crm/v2/Sales_Reps",
                headers=zoho_headers,
                json={"data": [record]},
            )
            resp_data = create_resp.json().get("data", [{}])[0] if create_resp.status_code in [200, 201, 202] else {}
            if create_resp.status_code not in [200, 201, 202] or resp_data.get("code") not in [None, "SUCCESS"]:
                logger.error(f"Failed to create {email} | status={create_resp.status_code} | body={create_resp.text}")
                failed += 1
            else:
                created += 1
                logger.info(f"Created Sales Rep: {full_name} ({email})")

        return {"status": "completed", "new": created, "failed": failed}

    except Exception:
        logger.exception("Unhandled exception in sync-aurora-users")
        return {"status": "failed - exception"}


# ------------------------
# Internal: Full Sync — Upserts All Aurora Users into Zoho Sales Reps
# ------------------------
@app.post("/internal/sync-aurora-users/full")
async def sync_aurora_users_full(request: Request):
    try:
        tenant_id = os.getenv("AURORA_TENANT_ID")
        users_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/users"
        users_response = requests.get(users_url, headers=aurora_headers())

        if users_response.status_code != 200:
            logger.error(f"Aurora users pull failed | status={users_response.status_code}")
            return {"status": "failed - aurora users pull error"}

        users = users_response.json().get("users", [])
        logger.info(f"Pulled {len(users)} users from Aurora")

        # Pull Aurora team and partner maps once (id -> name)
        team_map = get_aurora_team_map()
        partner_map = get_aurora_partner_map()
        logger.info(f"Loaded {len(team_map)} Aurora teams, {len(partner_map)} Aurora partners")

        access_token = get_zoho_access_token()
        if not access_token:
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        created = 0
        updated = 0
        skipped = 0

        for user in users:
            user_id = user.get("id")
            first_name = (user.get("first_name") or "").strip()
            last_name = (user.get("last_name") or "").strip()
            email = (user.get("email") or "").strip()

            if not email:
                skipped += 1
                continue

            # Fetch full user detail for additional fields
            user_detail_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/users/{user_id}"
            user_detail_response = requests.get(user_detail_url, headers=aurora_headers())

            if user_detail_response.status_code == 200:
                detail = user_detail_response.json().get("user", {})
            else:
                logger.warning(f"Could not fetch detail for user {email} | status={user_detail_response.status_code}")
                detail = {}

            logger.info(f"Aurora detail keys for {email}: {list(detail.keys())}")
            logger.info(f"Aurora team_ids raw for {email}: {detail.get('team_ids')}")

            account_status = detail.get("account_status") or user.get("account_status")
            phone = (detail.get("phone") or "").strip() or None
            role_id = detail.get("role_id")
            raw_team_ids = detail.get("team_ids") or []
            raw_partner_ids = detail.get("partner_ids") or []
            team_ids_str = ", ".join(raw_team_ids) or None
            team_names = ", ".join(team_map.get(tid, tid) for tid in raw_team_ids) or None
            partner_ids_str = ", ".join(raw_partner_ids) or None
            partner_names = ", ".join(partner_map.get(pid, pid) for pid in raw_partner_ids) or None
            base_ppw_min = detail.get("base_price_per_watt_min")

            full_name = f"{first_name} {last_name}".strip()
            is_active = account_status == "active"

            # Check if Sales Rep already exists by email
            search_url = f"{api_domain}/crm/v2/Sales_Reps/search?criteria=(Email:equals:{quote(email, safe='')})"
            search_response = requests.get(search_url, headers=headers)

            record = {
                "Name": full_name,
                "Email": email,
                "Active": is_active,
                "Aurora_User_ID": user_id,
                "Aurora_Role_ID": role_id,
                "Aurora_Team_IDs": team_ids_str,
                "Aurora_Team_Names": team_names,
                "Aurora_Partner_IDs": partner_ids_str,
                "Aurora_Partner_Names": partner_names,
                "Aurora_Base_PPW_Min": base_ppw_min,
            }

            if phone:
                record["Phone"] = phone

            if not full_name:
                full_name = email  # Zoho requires Name; fall back to email

            if search_response.status_code == 200 and search_response.json().get("data"):
                # Update existing record
                existing_id = search_response.json()["data"][0]["id"]
                record["id"] = existing_id
                record["Name"] = full_name
                update_url = f"{api_domain}/crm/v2/Sales_Reps"
                update_resp = requests.put(update_url, headers=headers, json={"data": [record]})
                resp_data = update_resp.json().get("data", [{}])[0] if update_resp.status_code in [200, 201, 202] else {}
                if update_resp.status_code not in [200, 201, 202] or resp_data.get("code") not in [None, "SUCCESS"]:
                    logger.error(f"Failed to update {email} | status={update_resp.status_code} | body={update_resp.text}")
                else:
                    updated += 1
                    logger.info(f"Updated Sales Rep: {full_name} ({email})")
            else:
                # Create new record
                record["Name"] = full_name
                create_url = f"{api_domain}/crm/v2/Sales_Reps"
                create_resp = requests.post(create_url, headers=headers, json={"data": [record]})
                resp_data = create_resp.json().get("data", [{}])[0] if create_resp.status_code in [200, 201, 202] else {}
                if create_resp.status_code not in [200, 201, 202] or resp_data.get("code") not in [None, "SUCCESS"]:
                    logger.error(f"Failed to create {email} | status={create_resp.status_code} | body={create_resp.text}")
                else:
                    created += 1
                    logger.info(f"Created Sales Rep: {full_name} ({email})")

        return {
            "status": "completed",
            "created": created,
            "updated": updated,
            "skipped": skipped,
        }

    except Exception:
        logger.exception("Unhandled exception in sync-aurora-users")
        return {"status": "failed - exception"}


# ------------------------
# Internal: Backfill LightReach fields onto an existing Install
# ------------------------
# POST /internal/backfill-lightreach { "install_id": "..." }
# Looks up the install's Aurora_Project_ID, finds the financing record on
# Aurora that has provider == "palmetto", and writes the resulting
# LightReach_* fields onto the Install. Use this for installs created before
# the Aurora→LightReach sync was wired in.
@app.post("/internal/backfill-lightreach")
async def backfill_lightreach(request: Request):
    try:
        body = await request.json()
        install_id = body.get("install_id")
        if not install_id:
            return {"status": "failed - missing install_id"}

        access_token = get_zoho_access_token()
        if not access_token:
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        # Pull the install to get its Aurora_Project_ID.
        install_resp = requests.get(f"{api_domain}/crm/v2/Installs/{install_id}", headers=headers)
        if install_resp.status_code != 200:
            return {"status": f"failed - install lookup error ({install_resp.status_code})"}
        records = install_resp.json().get("data", [])
        if not records:
            return {"status": "failed - install not found"}
        install = records[0]
        project_id = install.get("Aurora_Project_ID")
        if not project_id:
            return {"status": "failed - install has no Aurora_Project_ID"}

        # Walk every design under the project and pick the most-progressed
        # palmetto financing across the whole project (not just per-design).
        lightreach_fields = extract_lightreach_install_fields_for_project(project_id)
        if not lightreach_fields:
            return {"status": "no palmetto financing found on any design"}

        logger.info(
            f"backfill_lightreach: install_id={install_id} "
            f"keys={list(lightreach_fields.keys())}"
        )

        update_payload = {"data": [{"id": install_id, **lightreach_fields}]}
        update_resp = requests.put(
            f"{api_domain}/crm/v2/Installs", headers=headers, json=update_payload
        )
        if update_resp.status_code not in [200, 201, 202]:
            return {
                "status": "failed - install update error",
                "code": update_resp.status_code,
                "body": update_resp.text[:500],
            }

        return {"status": "ok", "fields_written": list(lightreach_fields.keys())}

    except Exception:
        logger.exception("Unhandled exception in backfill_lightreach")
        return {"status": "failed - exception"}


# ------------------------
# Internal: Bulk backfill LightReach fields across all Installs
# ------------------------
# POST /internal/backfill-lightreach-all
# Body (all optional):
#   { "force": false, "limit": 0, "dry_run": false }
#   - force=true: also re-process installs that already have LightReach_Account_ID
#   - limit=N:   only process up to N candidates (0 = no limit)
#   - dry_run:   count and log candidates but skip the actual writes
#
# Returns immediately with {"status": "started", "candidates": N}; the loop
# runs in a background task. Watch Render logs for progress and final summary.
@app.post("/internal/backfill-lightreach-all")
async def backfill_lightreach_all(request: Request, background_tasks: BackgroundTasks):
    try:
        raw = await request.body()
        body = json.loads(raw) if raw else {}
        force = bool(body.get("force"))
        limit = int(body.get("limit") or 0)
        dry_run = bool(body.get("dry_run"))

        background_tasks.add_task(
            _run_lightreach_backfill_all, force=force, limit=limit, dry_run=dry_run
        )
        return {
            "status": "started",
            "force": force,
            "limit": limit,
            "dry_run": dry_run,
            "note": "watch Render logs for progress; final summary logs as 'backfill_all complete'",
        }
    except Exception:
        logger.exception("Unhandled exception in backfill_lightreach_all")
        return {"status": "failed - exception"}


def _run_lightreach_backfill_all(force: bool, limit: int, dry_run: bool):
    """
    Background task: page through all Zoho Installs, and for each one with an
    Aurora_Project_ID (and, unless force, a missing LightReach_Account_ID),
    pull the LightReach fields from Aurora and write them to the Install.
    """
    try:
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("backfill_all: failed to obtain Zoho token")
            return

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        page = 1
        per_page = 200
        seen = 0
        skipped_no_project = 0
        skipped_already_set = 0
        attempted = 0
        succeeded = 0
        failed = 0
        no_match = 0

        logger.info(
            f"backfill_all: starting | force={force} limit={limit} dry_run={dry_run}"
        )

        while True:
            list_url = (
                f"{api_domain}/crm/v2/Installs"
                f"?fields=id,Aurora_Project_ID,LightReach_Account_ID"
                f"&page={page}&per_page={per_page}"
            )
            list_resp = requests.get(list_url, headers=headers)
            if list_resp.status_code == 401:
                # Token expired mid-run — refresh and retry once.
                access_token = get_zoho_access_token()
                headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
                list_resp = requests.get(list_url, headers=headers)
            if list_resp.status_code != 200:
                logger.error(
                    f"backfill_all: install pull failed | "
                    f"page={page} status={list_resp.status_code} body={list_resp.text[:300]}"
                )
                break

            payload = list_resp.json()
            records = payload.get("data") or []
            if not records:
                break

            for record in records:
                seen += 1

                if limit and attempted >= limit:
                    break

                install_id = record.get("id")
                project_id = record.get("Aurora_Project_ID")
                existing_account = record.get("LightReach_Account_ID")

                if not project_id:
                    skipped_no_project += 1
                    continue
                if existing_account and not force:
                    skipped_already_set += 1
                    continue

                attempted += 1
                try:
                    fields = extract_lightreach_install_fields_for_project(project_id)
                    if not fields:
                        no_match += 1
                        continue

                    if dry_run:
                        logger.info(
                            f"backfill_all [dry-run]: would update install_id={install_id} "
                            f"project_id={project_id} keys={list(fields.keys())}"
                        )
                        succeeded += 1
                        continue

                    update_payload = {"data": [{"id": install_id, **fields}]}
                    update_resp = requests.put(
                        f"{api_domain}/crm/v2/Installs",
                        headers=headers,
                        json=update_payload,
                    )
                    if update_resp.status_code == 401:
                        access_token = get_zoho_access_token()
                        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
                        update_resp = requests.put(
                            f"{api_domain}/crm/v2/Installs",
                            headers=headers,
                            json=update_payload,
                        )
                    if update_resp.status_code in [200, 201, 202]:
                        succeeded += 1
                        if succeeded % 25 == 0:
                            logger.info(
                                f"backfill_all: progress | seen={seen} attempted={attempted} "
                                f"succeeded={succeeded} no_match={no_match} failed={failed}"
                            )
                    else:
                        failed += 1
                        logger.warning(
                            f"backfill_all: update failed | install_id={install_id} "
                            f"status={update_resp.status_code} body={update_resp.text[:200]}"
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        f"backfill_all: exception on install_id={install_id} "
                        f"project_id={project_id}"
                    )

                # Be polite — small delay between Aurora-heavy iterations.
                time.sleep(0.5)

            if limit and attempted >= limit:
                break

            info = payload.get("info") or {}
            if not info.get("more_records"):
                break
            page += 1

        logger.info(
            f"backfill_all complete | seen={seen} attempted={attempted} "
            f"succeeded={succeeded} no_match={no_match} failed={failed} "
            f"skipped_no_project={skipped_no_project} skipped_already_set={skipped_already_set} "
            f"dry_run={dry_run}"
        )
    except Exception:
        logger.exception("Unhandled exception in _run_lightreach_backfill_all")


# ------------------------
# Internal: Sync pricing fields from Active Snapshot → Install
# ------------------------
# POST /internal/sync-pricing-from-snapshot { "install_id": "..." }
#
# Reads pricing fields directly from the install's Active_Snapshot record
# and writes them back to the Install. Use this to repair installs where
# pricing fields are null despite a valid snapshot existing — e.g. when
# the original install-update was silently rejected by Zoho.
@app.post("/internal/sync-pricing-from-snapshot")
async def sync_pricing_from_snapshot(request: Request):
    try:
        body = await request.json()
        install_id = body.get("install_id")
        if not install_id:
            return {"status": "failed - missing install_id"}

        result = _sync_pricing_from_snapshot_for_install(install_id)
        return result
    except Exception:
        logger.exception("Unhandled exception in sync_pricing_from_snapshot")
        return {"status": "failed - exception"}


# ------------------------
# Internal: Bulk sync pricing fields from Active Snapshot → Install
# ------------------------
# POST /internal/sync-pricing-from-snapshot-all
# Body (all optional):
#   { "force": false, "limit": 0, "dry_run": false }
#   - force=true:  re-write even installs that already have Final_System_Price
#   - limit=N:     only process up to N candidates (0 = no limit)
#   - dry_run:     log candidates but skip actual writes
#
# Returns immediately; runs in background. Watch Render logs for
# 'pricing_sync complete' for the final tally.
@app.post("/internal/sync-pricing-from-snapshot-all")
async def sync_pricing_from_snapshot_all(request: Request, background_tasks: BackgroundTasks):
    try:
        raw = await request.body()
        body = json.loads(raw) if raw else {}
        force = bool(body.get("force"))
        limit = int(body.get("limit") or 0)
        dry_run = bool(body.get("dry_run"))

        background_tasks.add_task(_run_pricing_sync_all, force=force, limit=limit, dry_run=dry_run)
        return {
            "status": "started",
            "force": force,
            "limit": limit,
            "dry_run": dry_run,
            "note": "watch Render logs for progress; final summary logs as 'pricing_sync complete'",
        }
    except Exception:
        logger.exception("Unhandled exception in sync_pricing_from_snapshot_all")
        return {"status": "failed - exception"}


# Pricing fields stored on the Snapshot module that mirror onto the Install.
_SNAPSHOT_PRICING_KEYS = [
    "Final_System_Price",
    "Price_Per_Watt",
    "Gross_Price_Per_Watt",
    "Base_Price",
    "Adders_Total",
    "Discounts_Total",
    "Consultant_Comp_PPW",
    "Helio_Lead_Fee_PPW",
    "Referral_Payout_PPW",
]

_HEA_SHEET_BASE = (
    "https://docs.google.com/spreadsheets/d/"
    "1BsEFP4rAmRjPJ9_49rjAEFHnoo12jH3oWdiQ6zKUHME"
    "/gviz/tq?tqx=out:csv&sheet="
)
# Cancelled tab is intentionally excluded — records there have no corresponding
# Zoho HEA status and should not overwrite any existing status.
_HEA_SHEET_TABS = [
    "Pending%20Confirmation",
    "Confirmed",
    "HEA%20Completed",
]

# Forward-only guard: never downgrade HEA status
_HEA_STATUS_RANK = {
    "Pending Confirmation": 1,
    "Scheduled with HEA Auditor": 2,
    "HEA Auditor Confirmed Date": 3,
    "HEA Completed ( < 3Yrs Old)": 4,
}

_HEA_CANCELLED_RE = re.compile(
    r"already had|no longer|cancel|barriered|unresponsive|not interested|"
    r"refused|other vendor|another vendor|wise use|solar cancel|home too new",
    re.I,
)


def _sync_pricing_from_snapshot_for_install(
    install_id,
    force=True,
    dry_run=False,
    access_token=None,
    headers=None,
    api_domain=None,
):
    """
    Core logic: read pricing from the install's Active_Snapshot and write to Install.
    Returns a dict with a "status" key.
    """
    if not install_id:
        return {"status": "failed - missing install_id"}

    if not access_token:
        access_token = get_zoho_access_token()
    if not access_token:
        return {"status": "failed - no zoho token"}
    if not headers:
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    if not api_domain:
        api_domain = os.getenv("ZOHO_API_DOMAIN")

    # Fetch the install to get its Active_Snapshot and current pricing state.
    install_resp = requests.get(f"{api_domain}/crm/v2/Installs/{install_id}", headers=headers)
    if install_resp.status_code != 200:
        return {"status": f"failed - install lookup error ({install_resp.status_code})"}
    records = install_resp.json().get("data", [])
    if not records:
        return {"status": "failed - install not found"}
    install = records[0]

    active_snapshot = install.get("Active_Snapshot")
    if not active_snapshot:
        return {"status": "skipped - no active snapshot"}
    snapshot_id = active_snapshot.get("id") if isinstance(active_snapshot, dict) else None
    if not snapshot_id:
        return {"status": "skipped - active snapshot has no id"}

    # Skip if already populated, unless force=True.
    if not force and install.get("Final_System_Price") not in (None, 0, 0.0):
        return {"status": "skipped - pricing already populated"}

    # Fetch the snapshot record to read its pricing fields.
    snap_resp = requests.get(
        f"{api_domain}/crm/v2/Aurora_Design_Snapshots/{snapshot_id}", headers=headers
    )
    if snap_resp.status_code != 200:
        return {"status": f"failed - snapshot lookup error ({snap_resp.status_code})"}
    snap_records = snap_resp.json().get("data", [])
    if not snap_records:
        return {"status": "failed - snapshot record not found"}
    snap = snap_records[0]

    pricing_update = {k: snap[k] for k in _SNAPSHOT_PRICING_KEYS if snap.get(k) is not None}

    if not pricing_update:
        return {"status": "skipped - snapshot has no pricing data"}

    logger.info(
        f"sync_pricing: install_id={install_id} snapshot_id={snapshot_id} "
        f"dry_run={dry_run} fields={list(pricing_update.keys())}"
    )

    if dry_run:
        return {"status": "dry-run", "would_write": pricing_update}

    update_resp = requests.put(
        f"{api_domain}/crm/v2/Installs",
        headers=headers,
        json={"data": [{"id": install_id, **pricing_update}]},
    )
    if update_resp.status_code not in [200, 201, 202]:
        return {"status": f"failed - install update error ({update_resp.status_code})"}

    ok, code, msg = _zoho_update_ok(update_resp)
    if not ok:
        logger.warning(
            f"sync_pricing: install update rejected | install_id={install_id} code={code} msg={msg}"
        )
        return {"status": f"failed - install update: {code} ({msg})"}

    return {"status": "ok", "fields_written": list(pricing_update.keys())}


def _run_pricing_sync_all(force: bool, limit: int, dry_run: bool):
    """
    Background task: page through all Installs that have an Active_Snapshot,
    and for each one with null/zero Final_System_Price (or all if force=True),
    write pricing fields from the snapshot record onto the Install.
    """
    try:
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("pricing_sync_all: failed to obtain Zoho token")
            return

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        page, per_page = 1, 200
        seen = attempted = updated = skipped = failed = 0

        logger.info(
            f"pricing_sync_all: starting | force={force} limit={limit} dry_run={dry_run}"
        )

        while True:
            url = (
                f"{api_domain}/crm/v2/Installs"
                f"?fields=id,Active_Snapshot,Final_System_Price"
                f"&page={page}&per_page={per_page}"
            )
            resp = requests.get(url, headers=headers)
            if resp.status_code != 200:
                logger.error(
                    f"pricing_sync_all: install page pull failed | "
                    f"page={page} status={resp.status_code}"
                )
                break

            data = resp.json()
            records = data.get("data") or []
            if not records:
                break

            for record in records:
                seen += 1
                if limit and seen > limit:
                    break

                install_id = record.get("id")
                if not record.get("Active_Snapshot"):
                    skipped += 1
                    continue
                existing_price = record.get("Final_System_Price")
                if not force and existing_price not in (None, 0, 0.0):
                    skipped += 1
                    continue

                attempted += 1
                try:
                    result = _sync_pricing_from_snapshot_for_install(
                        install_id,
                        force=force,
                        dry_run=dry_run,
                        access_token=access_token,
                        headers=headers,
                        api_domain=api_domain,
                    )
                    status = result.get("status", "")
                    if status == "ok":
                        updated += 1
                    elif status.startswith("skipped"):
                        skipped += 1
                    else:
                        failed += 1
                        logger.warning(
                            f"pricing_sync_all: failed | install_id={install_id} result={result}"
                        )

                    if attempted % 25 == 0:
                        logger.info(
                            f"pricing_sync_all: progress | seen={seen} attempted={attempted} "
                            f"updated={updated} skipped={skipped} failed={failed}"
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        f"pricing_sync_all: exception | install_id={install_id}"
                    )

            if limit and seen >= limit:
                break
            if not data.get("info", {}).get("more_records"):
                break
            page += 1

        logger.info(
            f"pricing_sync complete | seen={seen} attempted={attempted} "
            f"updated={updated} skipped={skipped} failed={failed} dry_run={dry_run}"
        )
    except Exception:
        logger.exception("Unhandled exception in _run_pricing_sync_all")


# ------------------------
# Internal: Bulk-create initial snapshots for installs missing them
# ------------------------
# POST /internal/backfill-snapshots-all
# Body (all optional):
#   { "limit": 0, "dry_run": false }
#   - limit=N:   only process up to N candidates (0 = no limit)
#   - dry_run:   log what would be created without actually doing it
#
# Returns immediately with {"status": "started"}; the loop runs in a background
# task. Watch Render logs for 'snapshot_backfill complete' for the final tally.
@app.post("/internal/backfill-snapshots-all")
async def backfill_snapshots_all(request: Request, background_tasks: BackgroundTasks):
    try:
        raw = await request.body()
        body = json.loads(raw) if raw else {}
        limit = int(body.get("limit") or 0)
        dry_run = bool(body.get("dry_run"))

        background_tasks.add_task(_run_snapshot_backfill_all, limit=limit, dry_run=dry_run)
        return {
            "status": "started",
            "limit": limit,
            "dry_run": dry_run,
            "note": "watch Render logs for progress; final summary logs as 'snapshot_backfill complete'",
        }
    except Exception:
        logger.exception("Unhandled exception in backfill_snapshots_all")
        return {"status": "failed - exception"}


def _run_snapshot_backfill_all(limit: int, dry_run: bool):
    """
    Page through every Zoho Install. For each one with Aurora_Project_ID set
    AND no Active_Snapshot, call _create_initial_snapshot_for_install. Logs
    progress every 25 successes and a summary at the end.

    Buckets the results:
      succeeded            – snapshot created cleanly
      sold_design_invalid  – Aurora returned 0 or 2+ sold designs (the most
                             common reason an install can't be auto-bootstrapped)
      failed               – any other failure (Aurora 5xx, Zoho update error,
                             unhandled exception, etc.)
      skipped_no_project   – install has no Aurora_Project_ID
      skipped_already_set  – install already has an Active_Snapshot
    """
    try:
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("snapshot_backfill: failed to obtain Zoho token")
            return

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        page = 1
        per_page = 200
        seen = 0
        skipped_no_project = 0
        skipped_already_set = 0
        attempted = 0
        succeeded = 0
        sold_design_invalid = 0
        failed = 0

        logger.info(f"snapshot_backfill: starting | limit={limit} dry_run={dry_run}")

        while True:
            list_url = (
                f"{api_domain}/crm/v2/Installs"
                f"?fields=id,Aurora_Project_ID,Active_Snapshot"
                f"&page={page}&per_page={per_page}"
            )
            list_resp = requests.get(list_url, headers=headers)
            if list_resp.status_code == 401:
                access_token = get_zoho_access_token()
                headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
                list_resp = requests.get(list_url, headers=headers)
            if list_resp.status_code != 200:
                logger.error(
                    f"snapshot_backfill: install pull failed | "
                    f"page={page} status={list_resp.status_code} body={list_resp.text[:300]}"
                )
                break

            payload = list_resp.json()
            records = payload.get("data") or []
            if not records:
                break

            for record in records:
                seen += 1
                if limit and attempted >= limit:
                    break

                install_id = record.get("id")
                project_id = record.get("Aurora_Project_ID")
                existing_snapshot = record.get("Active_Snapshot")

                if not project_id:
                    skipped_no_project += 1
                    continue
                if existing_snapshot:
                    skipped_already_set += 1
                    continue

                attempted += 1

                if dry_run:
                    logger.info(
                        f"snapshot_backfill [dry-run]: would create snapshot | "
                        f"install_id={install_id} project_id={project_id}"
                    )
                    succeeded += 1
                    continue

                try:
                    result = _create_initial_snapshot_for_install(
                        install_id=install_id,
                        project_id=project_id,
                        deal_id=None,
                        access_token=access_token,
                        headers=headers,
                        api_domain=api_domain,
                    )
                    status = (result or {}).get("status", "")

                    if "initial snapshot created" in status:
                        succeeded += 1
                        if succeeded % 25 == 0:
                            logger.info(
                                f"snapshot_backfill: progress | seen={seen} attempted={attempted} "
                                f"succeeded={succeeded} sold_design_invalid={sold_design_invalid} "
                                f"failed={failed}"
                            )
                    elif "sold design count invalid" in status:
                        sold_design_invalid += 1
                        logger.warning(
                            f"snapshot_backfill: sold count invalid | "
                            f"install_id={install_id} project_id={project_id} status={status}"
                        )
                    elif "skipped - active snapshot already exists" in status:
                        # Race — list said empty, but a snapshot got created in between.
                        skipped_already_set += 1
                    else:
                        failed += 1
                        logger.warning(
                            f"snapshot_backfill: failed | install_id={install_id} "
                            f"project_id={project_id} status={status}"
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        f"snapshot_backfill: exception | install_id={install_id} "
                        f"project_id={project_id}"
                    )

                # Be polite — small delay between Aurora-heavy iterations.
                time.sleep(0.5)

            if limit and attempted >= limit:
                break

            info = payload.get("info") or {}
            if not info.get("more_records"):
                break
            page += 1

        logger.info(
            f"snapshot_backfill complete | seen={seen} attempted={attempted} "
            f"succeeded={succeeded} sold_design_invalid={sold_design_invalid} "
            f"failed={failed} skipped_no_project={skipped_no_project} "
            f"skipped_already_set={skipped_already_set} dry_run={dry_run}"
        )
    except Exception:
        logger.exception("Unhandled exception in _run_snapshot_backfill_all")


# ------------------------
# Internal: Sync HEA status from Home Doctor Google Sheet → Zoho Installs
# ------------------------

def _normalize_phone(raw):
    """Strip non-digits, return last 10 digits."""
    digits = re.sub(r"\D", "", raw or "")
    return digits[-10:] if len(digits) >= 10 else digits


def _parse_hea_date(raw):
    """Parse date strings like '1/31/2025', '6/7/25'. Returns 'YYYY-MM-DD' or None."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _parse_hea_tab(url):
    """Download and parse one HEA sheet tab CSV. Returns a list of record dicts."""
    import csv
    import io

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.error(f"hea_sync: tab download failed | {url} | {exc}")
        return []

    reader = csv.reader(io.StringIO(resp.text))
    rows = list(reader)

    col = {}
    section = None
    records = []

    for row in rows:
        if not any(c.strip() for c in row):
            continue

        normalized = [re.sub(r'\s+', ' ', c).strip().lower() for c in row]

        # Detect header rows by presence of name columns
        if "first name" in normalized or ("first" in normalized and "last" in normalized):
            col = {v: i for i, v in enumerate(normalized) if v}
            if "apt date" in col:
                section = "active"
            elif "hes date" in col and "lead source" in col:
                section = "pending_cancelled"
            elif "hes date" in col:
                section = "completed"
            else:
                section = "confirmed"
            continue

        if not col or not section:
            continue

        def _get(*keys):
            for k in keys:
                idx = col.get(k)
                if idx is not None and idx < len(row):
                    v = row[idx].strip()
                    if v:
                        return v
            return ""

        phone = _normalize_phone(_get("phone"))
        if not phone or len(phone) < 10:
            continue

        first = _get("first name", "first")
        last = _get("last name", "last")
        city = _get("city")
        notes = _get("notes")

        # Drop cancelled / disqualified rows
        if _HEA_CANCELLED_RE.search(notes):
            continue

        # Determine HEA status from section + notes
        if section == "completed":
            hea_status = "HEA Completed ( < 3Yrs Old)"
            apt_date_raw = _get("hes date", "hes date/time")
        elif section == "pending_cancelled":
            hea_status = "Pending Confirmation"
            apt_date_raw = _get("apt date", "hes date/time", "hes date")
        elif "info confirmed" in notes.lower():
            hea_status = "HEA Auditor Confirmed Date"
            apt_date_raw = _get("apt date", "hes date/time", "hes date")
        else:
            hea_status = "Scheduled with HEA Auditor"
            apt_date_raw = _get("apt date", "hes date/time", "hes date")

        records.append({
            "phone": phone,
            "first_name": first,
            "last_name": last,
            "city": city,
            "hea_status": hea_status,
            "apt_date": _parse_hea_date(apt_date_raw),
        })

    return records


def _parse_hea_sheet():
    """Read all active HEA tabs and return combined records."""
    all_records = []
    for tab in _HEA_SHEET_TABS:
        tab_records = _parse_hea_tab(_HEA_SHEET_BASE + tab)
        logger.info(f"hea_sync: {tab} → {len(tab_records)} records")
        all_records.extend(tab_records)
    logger.info(f"hea_sync: parsed {len(all_records)} records total from {len(_HEA_SHEET_TABS)} tabs")
    return all_records


async def _run_hea_sync():
    """Core HEA sync logic. Reads the Home Doctor sheet and updates Zoho Installs."""
    access_token = get_zoho_access_token()
    if not access_token:
        logger.error("hea_sync: no zoho token")
        return {"status": "failed", "reason": "no zoho token"}

    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    api_domain = os.getenv("ZOHO_API_DOMAIN")

    records = _parse_hea_sheet()
    if not records:
        return {"status": "ok", "synced": 0, "not_found": 0, "skipped": 0, "total_parsed": 0}

    synced = 0
    skipped = 0
    not_found = 0
    not_found_names = []
    skipped_names = []

    for rec in records:
        phone = rec["phone"]

        # --- Lookup installs by phone (may return multiple for same customer) ---
        installs = []
        search_url = (
            f"{api_domain}/crm/v2/Installs/search"
            f"?criteria=(Primary_Phone:equals:{phone})"
        )
        r = requests.get(search_url, headers=headers)
        if r.status_code == 200:
            installs = r.json().get("data", [])

        # --- Fallback: last name + city ---
        if not installs and rec["last_name"] and rec["city"]:
            search_url2 = (
                f"{api_domain}/crm/v2/Installs/search"
                f"?criteria=((Name:contains:{rec['last_name']})"
                f"AND(Site_Location:contains:{rec['city']}))"
            )
            r2 = requests.get(search_url2, headers=headers)
            if r2.status_code == 200:
                installs = r2.json().get("data", [])

        if not installs:
            not_found += 1
            label = f"{rec['first_name']} {rec['last_name']} ({phone})"
            not_found_names.append(label)
            logger.info(f"hea_sync: no install found for {label}")
            continue

        new_status = rec["hea_status"]

        for install in installs:
            install_id = install["id"]

            # --- Forward-only guard: never downgrade status ---
            current_status = install.get("Home_Energy_Audit_Status") or ""
            current_rank = _HEA_STATUS_RANK.get(current_status, 0)
            new_rank = _HEA_STATUS_RANK.get(new_status, 0)
            if new_rank > 0 and new_rank < current_rank:
                label = f"{rec['first_name']} {rec['last_name']} ({phone}) [{current_status} → {new_status}]"
                logger.info(f"hea_sync: skipping downgrade for {install_id} | {label}")
                skipped_names.append(label)
                skipped += 1
                continue

            # --- Build update payload ---
            update = {
                "id": install_id,
                "Home_Energy_Audit_Status": new_status,
                "HEA_Audit_Company": "Home Doctor",
            }
            if rec["apt_date"]:
                if new_status == "HEA Completed ( < 3Yrs Old)":
                    update["Energy_Audit_Completed_On"] = rec["apt_date"]
                else:
                    update["Energy_Audit_Scheduled_For"] = rec["apt_date"] + "T12:00:00+00:00"

            put_r = requests.put(
                f"{api_domain}/crm/v2/Installs",
                headers=headers,
                json={"data": [update]},
            )
            if put_r.status_code in [200, 201, 202]:
                synced += 1
                logger.info(f"hea_sync: updated install {install_id} | {new_status}")
            else:
                skipped += 1
                logger.error(
                    f"hea_sync: update failed for {install_id} | "
                    f"{put_r.status_code} | {put_r.text}"
                )

    result = {
        "status": "ok",
        "synced": synced,
        "not_found": not_found,
        "not_found_names": not_found_names,
        "skipped": skipped,
        "skipped_names": skipped_names,
        "total_parsed": len(records),
    }
    logger.info(f"hea_sync: complete | {result}")
    return result


@app.post("/internal/sync-hea")
async def sync_hea(request: Request):
    """Sync HEA status from Home Doctor Google Sheet to Zoho Installs."""
    try:
        result = await _run_hea_sync()
        return result
    except Exception:
        logger.exception("Unhandled exception in sync_hea")
        return {"status": "failed", "reason": "exception"}


# ------------------------
# Google Calendar helpers (used by Zoho blueprint webhooks)
# ------------------------
# Service account JSON lives in env var GOOGLE_SERVICE_ACCOUNT_JSON.
# Domain-wide delegation is authorized in Workspace admin for the scope
# https://www.googleapis.com/auth/calendar.events, so the service account can
# act as any @helio.solar user. We impersonate `installs@helio.solar` (the
# Install Department mailbox) so events appear with that calendar/account as
# both creator and organizer.
GCAL_INSTALL_DEPT = "installs@helio.solar"
GCAL_DEFAULT_ATTENDEES = [
    "dhfargnoli@helio.solar",
    "rgoncalves@helio.solar",
    "wvargas@helio.solar",
]
GCAL_SITE_SURVEY_DURATION_MIN = 60
GCAL_DEFAULT_TIMEZONE = "America/New_York"


def _build_calendar_service(impersonate_email):
    """
    Build a Calendar API client that acts on behalf of `impersonate_email`.
    Returns None if GOOGLE_SERVICE_ACCOUNT_JSON isn't configured.
    """
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON env var is missing")
        return None
    try:
        info = json.loads(raw)
    except ValueError:
        logger.exception("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")
        return None
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/calendar.events"]
    )
    delegated = creds.with_subject(impersonate_email)
    return build("calendar", "v3", credentials=delegated, cache_discovery=False)


def _format_event_time(dt):
    """Render a datetime as '11am' / '1pm' / '1:30pm' for the event title."""
    h = dt.hour
    m = dt.minute
    suffix = "am" if h < 12 else "pm"
    h_12 = h % 12 or 12
    return f"{h_12}{suffix}" if m == 0 else f"{h_12}:{m:02d}{suffix}"


def _extract_city_from_address(address):
    """
    Pull the city out of a US-style street address.

    Examples:
      "66 Wilson St, Stamford, CT 06902, USA"        → "Stamford"
      "66 Wilson St, Apt 2, Stamford, CT 06902, USA" → "Stamford"
      "Stamford, CT 06902"                            → "Stamford"
    """
    if not address:
        return ""
    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    if not parts:
        return ""
    # The city is conventionally the segment immediately before the
    # state-and-zip segment. Identify state-and-zip by looking for a 5-digit zip.
    for i, p in enumerate(parts):
        if re.search(r"\b\d{5}(-\d{4})?\b", p):
            if i > 0:
                return parts[i - 1]
            break
    if len(parts) >= 3:
        return parts[-2]
    return parts[0]


# ------------------------
# Webhook: Zoho Blueprint — Site Survey Scheduled
# ------------------------
# Triggered by a Zoho CRM blueprint transition action. Body should contain:
#   { "install_id": "<zoho install record id>" }
# Pulls Survey_Scheduled_For, Site_Location, Name, and Primary_Phone from the
# Install record, then creates a 1-hour Google Calendar event on the
# installs@helio.solar calendar with the standard attendee list.
@app.post("/webhook/zoho/site-survey-scheduled")
async def site_survey_scheduled_webhook(request: Request):
    try:
        try:
            raw = await request.body()
        except Exception:
            raw = b""
        body_text = raw.decode("utf-8", errors="replace") if raw else ""
        try:
            body = json.loads(body_text) if body_text else {}
        except ValueError:
            body = {}
        logger.info(
            f"site-survey-scheduled: webhook received | "
            f"content_type={request.headers.get('content-type')!r} "
            f"raw_body={body_text[:500]!r} parsed_keys={list(body.keys())}"
        )

        install_id = body.get("install_id")
        if not install_id:
            logger.warning(
                f"site-survey-scheduled: missing install_id in body | body={body!r}"
            )
            return {"status": "failed - missing install_id", "body": body}

        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("site-survey-scheduled: failed to obtain Zoho token")
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        zoho_headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        install_resp = requests.get(
            f"{api_domain}/crm/v2/Installs/{install_id}", headers=zoho_headers
        )
        if install_resp.status_code != 200:
            logger.warning(
                f"site-survey-scheduled: install lookup failed | "
                f"install_id={install_id} status={install_resp.status_code} "
                f"body={install_resp.text[:300]}"
            )
            return {
                "status": f"failed - install lookup error ({install_resp.status_code})"
            }
        records = install_resp.json().get("data") or []
        if not records:
            logger.warning(
                f"site-survey-scheduled: install not found | install_id={install_id}"
            )
            return {"status": "failed - install not found"}
        install = records[0]

        name = install.get("Name") or "Customer"
        survey_for = install.get("Survey_Scheduled_For")
        site_location = install.get("Site_Location") or ""
        phone = install.get("Primary_Phone") or ""

        # Site_Surveyor may be a plain text field or a Zoho user/contact
        # lookup ({"id": ..., "name": "Walter Vargas"}). Handle both.
        surveyor_raw = install.get("Site_Surveyor")
        if isinstance(surveyor_raw, dict):
            surveyor_full = surveyor_raw.get("name") or ""
        else:
            surveyor_full = (surveyor_raw or "").strip()
        # Title shows just the first name (e.g. "Walter" rather than
        # "Walter Vargas") to match the existing manual event format.
        surveyor_first = surveyor_full.split()[0] if surveyor_full else ""

        if not survey_for:
            logger.warning(
                f"site-survey-scheduled: no Survey_Scheduled_For | install_id={install_id}"
            )
            return {"status": "failed - install has no Survey_Scheduled_For"}

        # Parse Zoho datetime. Zoho returns ISO 8601 — handle both 'Z' and offset forms.
        try:
            start_dt = datetime.datetime.fromisoformat(
                survey_for.replace("Z", "+00:00")
            )
        except ValueError:
            logger.exception(
                f"site-survey-scheduled: cannot parse Survey_Scheduled_For | "
                f"install_id={install_id} value={survey_for}"
            )
            return {"status": "failed - cannot parse Survey_Scheduled_For"}

        end_dt = start_dt + datetime.timedelta(minutes=GCAL_SITE_SURVEY_DURATION_MIN)

        time_str = _format_event_time(start_dt)
        city = _extract_city_from_address(site_location)

        # Title format:  "Helio SS: <Name> (<Surveyor>) <time> <city>"
        customer_with_surveyor = (
            f"{name} ({surveyor_first})" if surveyor_first else name
        )
        title_parts = [f"Helio SS: {customer_with_surveyor}"]
        if time_str:
            title_parts.append(time_str)
        if city:
            title_parts.append(city)
        title = " ".join(title_parts)

        description_lines = [name]
        if phone:
            description_lines.append(f"Phone # {phone}")
        description = "\n".join(description_lines)

        # Use the offset embedded in the parsed datetime when available,
        # otherwise fall back to the org's default timezone.
        if start_dt.tzinfo is not None:
            tz_string = GCAL_DEFAULT_TIMEZONE  # readable label; offset already in dateTime
            start_iso = start_dt.isoformat()
            end_iso = end_dt.isoformat()
        else:
            tz_string = GCAL_DEFAULT_TIMEZONE
            start_iso = start_dt.isoformat()
            end_iso = end_dt.isoformat()

        event = {
            "summary": title,
            "location": site_location,
            "description": description,
            "start": {"dateTime": start_iso, "timeZone": tz_string},
            "end": {"dateTime": end_iso, "timeZone": tz_string},
            "attendees": [{"email": e} for e in GCAL_DEFAULT_ATTENDEES],
            # "2" = Sage (light green) — matches the existing manual events
            # the team has been creating for Site Surveys.
            "colorId": "2",
        }

        calendar = _build_calendar_service(GCAL_INSTALL_DEPT)
        if calendar is None:
            return {"status": "failed - calendar service unavailable"}

        try:
            created = calendar.events().insert(
                calendarId=GCAL_INSTALL_DEPT,
                body=event,
                sendUpdates="all",  # emails attendees
            ).execute()
        except HttpError as e:
            logger.exception(
                f"site-survey-scheduled: calendar insert failed | "
                f"install_id={install_id} status={e.resp.status} content={e.content[:300]}"
            )
            return {
                "status": f"failed - calendar api error ({e.resp.status})",
                "detail": e.content.decode("utf-8", errors="replace")[:500],
            }
        except Exception:
            logger.exception(
                f"site-survey-scheduled: calendar insert exception | "
                f"install_id={install_id}"
            )
            return {"status": "failed - calendar api exception"}

        event_id = created.get("id")
        event_link = created.get("htmlLink")
        logger.info(
            f"site-survey-scheduled: event created | install_id={install_id} "
            f"event_id={event_id} title={title!r} link={event_link}"
        )

        return {
            "status": "ok",
            "event_id": event_id,
            "event_link": event_link,
            "title": title,
        }

    except Exception:
        logger.exception("Unhandled exception in site_survey_scheduled_webhook")
        return {"status": "failed - exception"}


# ------------------------
# Health Check
# ------------------------
@app.get("/")
def health_check():
    return {"status": "Aurora-Zoho Sync Service Running"}


# ------------------------
# Get Zoho Access Token
# ------------------------
def get_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": os.getenv("ZOHO_CLIENT_ID"),
        "client_secret": os.getenv("ZOHO_CLIENT_SECRET"),
        "refresh_token": os.getenv("ZOHO_REFRESH_TOKEN"),
    }

    response = requests.post(url, data=payload)
    return response.json().get("access_token")


# ------------------------
# Aurora API Helpers
# ------------------------
def extract_pricing_fields(design_json, pricing_json, summary_json):
    """
    Parses design, pricing, and summary JSON from Aurora into a flat dict
    of snapshot fields. Used by both the webhook and the initial snapshot endpoint.
    """
    fields = {}

    # --- System Size ---
    breakdown = pricing_json.get("system_price_breakdown", [])
    pricing_method = (pricing_json.get("pricing_method") or "").strip().lower()
    ppw = float(pricing_json.get("price_per_watt") or 0)
    base_price_for_size = 0.0
    for item in breakdown:
        if item.get("item_type") == "base_price":
            base_price_for_size = float(item.get("item_price") or 0)
            break

    if ("price per watt" in pricing_method) and ppw > 0 and base_price_for_size > 0:
        system_size_watts = int(round(base_price_for_size / ppw))
    else:
        max_qty = 0.0
        for item in breakdown:
            if item.get("item_type") in ["adders", "discounts"]:
                for sub in item.get("subcomponents", []):
                    qty = sub.get("quantity")
                    if qty is None:
                        continue
                    try:
                        qty_f = float(qty)
                    except (TypeError, ValueError):
                        continue
                    if qty_f >= 1000 and qty_f > max_qty:
                        max_qty = qty_f
        system_size_watts = int(round(max_qty)) if max_qty > 0 else 0

    fields["System_Size_STC_Watts"] = system_size_watts

    # --- Milestone / Design Metadata ---
    milestone = design_json.get("milestone", {})
    fields["Aurora_Milestone"] = milestone.get("milestone")
    fields["Aurora_Milestone_ID"] = milestone.get("id")
    fields["Aurora_Milestone_Notes"] = milestone.get("notes")
    fields["Aurora_Design_Name"] = design_json.get("name")

    milestone_time_raw = milestone.get("recorded_at")
    fields["Milestone_Recorded_At"] = (
        datetime.datetime.fromisoformat(milestone_time_raw.replace("Z", "+00:00"))
        .astimezone().replace(microsecond=0).isoformat()
        if milestone_time_raw else None
    )

    aurora_created_raw = design_json.get("created_at")
    fields["Aurora_Created_At"] = (
        datetime.datetime.fromisoformat(aurora_created_raw.replace("Z", "+00:00"))
        .astimezone().replace(microsecond=0).isoformat()
        if aurora_created_raw else None
    )

    # --- Pricing ---
    final_price = pricing_json.get("system_price")
    fields["Price_Per_Watt"] = pricing_json.get("price_per_watt")
    fields["Final_System_Price"] = round(float(final_price or 0), 2)
    fields["Gross_Price_Per_Watt"] = (
        round(float(final_price) / system_size_watts, 4)
        if system_size_watts and float(final_price or 0) > 0 else 0
    )

    base_price = 0.0
    total_adders = 0.0
    total_discounts = 0.0
    for item in breakdown:
        item_type = item.get("item_type")
        item_price = float(item.get("item_price", 0) or 0)
        if item_type == "base_price":
            base_price = round(item_price, 2)
        elif item_type == "adders":
            total_adders = round(item_price, 2)
        elif item_type == "discounts":
            total_discounts = round(item_price, 2)

    fields["Base_Price"] = base_price
    fields["Adders_Total"] = total_adders
    fields["Discounts_Total"] = total_discounts

    # --- Commission Adders ---
    consultant_comp_ppw = 0.0
    helio_lead_fee_ppw = 0.0
    referral_payout = 0.0
    es_upline_discount_ppw = 0.0
    evp_upline_discount_ppw = 0.0

    for adder in pricing_json.get("adders", []):
        name = (adder.get("adder_name") or "").strip()
        value = float(adder.get("adder_value") or 0)
        if name == "A - Consultant Comp":
            consultant_comp_ppw = value
        elif name == "A - Helio Provided Lead":
            helio_lead_fee_ppw = value
        elif name == "A - Referral Payout":
            referral_payout = value
        elif name == "A - COMP: ES Upline Discount":
            es_upline_discount_ppw = value
        elif name == "A - COMP: EVP Upline Discount":
            evp_upline_discount_ppw = value

    fields["Consultant_Comp_PPW"] = consultant_comp_ppw
    fields["Helio_Lead_Fee_PPW"] = helio_lead_fee_ppw
    fields["Referral_Payout"] = referral_payout
    fields["ES_Upline_Discount_PPW"] = es_upline_discount_ppw
    fields["EVP_Upline_Discount_PPW"] = evp_upline_discount_ppw

    # --- Adder / Discount Lists ---
    fields["Adder_Name_List"] = ", ".join(
        a.get("adder_name") for a in pricing_json.get("adders", []) if not a.get("is_discount")
    )
    fields["Discount_Name_List"] = ", ".join(
        a.get("adder_name") for a in pricing_json.get("adders", []) if a.get("is_discount")
    )

    adder_details = []
    discount_details = []
    for item in pricing_json.get("system_price_breakdown", []):
        item_type = item.get("item_type")
        if item_type in ["adders", "discounts"]:
            for sub in item.get("subcomponents", []):
                rec = {"name": sub.get("adder_name"), "quantity": sub.get("quantity"), "total": sub.get("item_price")}
                if item_type == "adders":
                    adder_details.append(rec)
                else:
                    discount_details.append(rec)

    fields["Adder_Details_JSON"] = json.dumps(adder_details)
    fields["Discount_Details_JSON"] = json.dumps(discount_details)

    # --- Equipment ---
    module_model = None
    module_count = 0
    inverter_model = None
    inverter_count = 0
    optimizer_count = 0

    for item in summary_json.get("bill_of_materials", []):
        ct = item.get("component_type")
        name = item.get("name")
        mfr = item.get("manufacturer_name", "")
        qty = int(float(item.get("quantity") or 0))
        full = f"{mfr} {name}".strip() if mfr else name
        if ct == "modules":
            module_model, module_count = full, qty
        elif ct in ("inverters", "microinverters", "string_inverters"):
            inverter_model, inverter_count = full, qty
        elif ct == "dc_optimizers":
            optimizer_count = qty

    if not inverter_model:
        for inv in summary_json.get("string_inverters", []):
            mfr = inv.get("manufacturer_name", "")
            name = inv.get("name")
            inverter_model = f"{mfr} {name}".strip() if mfr else name
            inverter_count = int(float(inv.get("quantity") or 0))
            break

    if not inverter_model:
        for component in pricing_json.get("pricing_by_component", []):
            ct = component.get("component_type")
            mfr = component.get("manufacturer_name", "")
            name = component.get("name")
            qty = int(float(component.get("quantity") or 0))
            full = f"{mfr} {name}".strip() if mfr else name
            if ct in ("inverters", "microinverters", "string_inverters"):
                inverter_model, inverter_count = full, qty
            elif ct == "dc_optimizers" and optimizer_count == 0:
                optimizer_count = qty

    if not module_model:
        for adder in pricing_json.get("adders", []):
            adder_name = (adder.get("adder_name") or "").strip()
            if adder_name.upper().startswith("A. EQUIP:"):
                module_model = adder_name[9:].strip().replace(" (TPO ONLY)", "").replace(" (TPO)", "").strip()
                break

    fields["Module_Model"] = module_model
    fields["Module_Count"] = module_count
    fields["Inverter_Model"] = inverter_model
    fields["Inverter_Count"] = inverter_count
    fields["Optimizer_Count"] = optimizer_count

    # --- Battery ---
    battery_model = None
    battery_count = 0
    battery_base_price = 0.0
    for component in pricing_json.get("pricing_by_component", []):
        if component.get("component_type") == "batteries":
            battery_model = component.get("name")
            battery_count = int(float(component.get("quantity") or 0))
            battery_base_price = float(component.get("price") or 0)

    fields["Battery_Model"] = battery_model
    fields["Battery_Count"] = battery_count
    fields["Battery_Base_Price"] = battery_base_price

    # --- Incentives ---
    solar_incentives_total = 0.0
    storage_incentives_total = 0.0
    incentive_names = [inc.get("name") for inc in pricing_json.get("incentives", []) if inc.get("name")]

    for item in pricing_json.get("system_price_breakdown", []):
        if item.get("item_type") == "incentives":
            solar_incentives_total = float(item.get("item_price") or 0)

    solar_price_before_incentives = 0.0
    for item in pricing_json.get("system_price_breakdown", []):
        if item.get("item_type") == "discounts":
            solar_price_before_incentives = float(item.get("cumulative_price") or 0)

    storage_price_before_incentives = 0.0
    for item in pricing_json.get("storage_system_price_breakdown", []):
        if item.get("item_type") == "discounts":
            storage_price_before_incentives = float(item.get("cumulative_price") or 0)
        if item.get("item_type") == "incentives":
            storage_incentives_total = float(item.get("item_price") or 0)

    fields["Solar_Incentives_Total"] = solar_incentives_total
    fields["Storage_Incentives_Total"] = storage_incentives_total
    fields["Incentives_Total"] = solar_incentives_total + storage_incentives_total
    fields["Incentive_Name_List"] = ", ".join(incentive_names)
    fields["Solar_System_Price_Before_Incentives"] = solar_price_before_incentives
    fields["Storage_System_Price_Before_Incentives"] = storage_price_before_incentives
    fields["Total_Price_Before_Incentives"] = solar_price_before_incentives + storage_price_before_incentives

    fields["Raw_Design_JSON"] = json.dumps(design_json)
    fields["Raw_Pricing_JSON"] = json.dumps(pricing_json)

    return fields


def get_aurora_team_map():
    """Returns a dict of {team_id: team_name} for the tenant."""
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/teams"
    response = requests.get(url, headers=aurora_headers())
    if response.status_code != 200:
        logger.warning(f"Could not fetch Aurora teams | status={response.status_code}")
        return {}
    return {t["id"]: t["name"] for t in response.json().get("teams", [])}


def get_aurora_partner_map():
    """Returns a dict of {partner_id: partner_name} for the tenant."""
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/partners"
    response = requests.get(url, headers=aurora_headers())
    if response.status_code != 200:
        logger.warning(f"Could not fetch Aurora partners | status={response.status_code}")
        return {}
    return {p["id"]: p["name"] for p in response.json().get("partners", [])}


def aurora_headers():
    return {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json",
    }


def pull_design(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}?include_layout=true"
    return requests.get(url, headers=aurora_headers())


def pull_design_summary(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/summary"
    return requests.get(url, headers=aurora_headers())


def pull_pricing(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"
    return requests.get(url, headers=aurora_headers())


def _aurora_get_with_retry(url, max_retries=5):
    """
    GET an Aurora URL with retry-on-429 backoff. Aurora's rate limit returns
    a Retry-After header; we honor it when present, otherwise exponential
    backoff starting at 1s. Returns the final Response object (which may
    still be 429 if every retry is exhausted).
    """
    backoff = 1.0
    resp = None
    for attempt in range(max_retries):
        resp = requests.get(url, headers=aurora_headers())
        if resp.status_code != 429:
            return resp
        retry_after = resp.headers.get("Retry-After")
        try:
            sleep_for = float(retry_after) if retry_after else backoff
        except (TypeError, ValueError):
            sleep_for = backoff
        logger.warning(
            f"Aurora 429 — sleeping {sleep_for:.1f}s before retry "
            f"(attempt {attempt + 1}/{max_retries}) | url={url}"
        )
        time.sleep(sleep_for)
        backoff = min(backoff * 2, 30.0)
    return resp


def _normalize_aurora_datetime(s):
    """
    Convert Aurora's 'YYYY-MM-DD HH:MM:SS UTC' string format to ISO 8601 so it
    parses cleanly into Zoho DateTime fields. Returns the original input on
    any parse failure so we never block an update on date formatting.
    """
    if not s or not isinstance(s, str):
        return s
    try:
        cleaned = s.replace(" UTC", "").strip()
        dt = datetime.datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=datetime.timezone.utc).isoformat()
    except (ValueError, TypeError):
        return s


def pull_financings(design_id):
    """List all financings for a design (palmetto, sungage, cash, etc.)."""
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/financings"
    return _aurora_get_with_retry(url)


def pull_financing(design_id, financing_id):
    """Retrieve the full record for one financing by ID."""
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/financings/{financing_id}"
    return _aurora_get_with_retry(url)


def extract_lightreach_install_fields(design_id):
    """
    Walk every financing on a design, find the LightReach (provider == "palmetto")
    one, and return a dict of Zoho Install field updates.

    Returns {} if the design has no LightReach financing.

    Field mapping (verified against the Aurora financing payload):
      financing.financier.external.consumer_id        -> LightReach_Account_ID
      financing.financier.external.request_id         -> LightReach_Request_ID
      financing.financier.external.quote_id           -> LightReach_Quote_ID
      financing.financier.external.provider_status    -> LightReach_Finance_Status
      financing.financier.external.contract_signed_at -> LightReach_Contract_Signed_At
      financing.financier.status                      -> LightReach_Application_Status
    """
    list_resp = pull_financings(design_id)
    if list_resp.status_code != 200:
        logger.warning(
            f"extract_lightreach_install_fields: list failed | "
            f"design_id={design_id} status={list_resp.status_code}"
        )
        return {}

    listed = list_resp.json()
    summaries = listed.get("financings") if isinstance(listed, dict) else listed
    if not summaries:
        return {}

    # Pull each financing's full record; keep the palmetto ones that have a consumer_id.
    palmetto_records = []
    for s in summaries:
        f_id = s.get("id") if isinstance(s, dict) else None
        if not f_id:
            continue
        full_resp = pull_financing(design_id, f_id)
        if full_resp.status_code != 200:
            continue
        full = full_resp.json().get("financing", full_resp.json())
        financier = full.get("financier") or {}
        if financier.get("provider") != "palmetto":
            continue
        external = financier.get("external") or {}
        if not external.get("consumer_id"):
            continue
        palmetto_records.append({"financier": financier, "external": external})

    if not palmetto_records:
        return {}

    # Multiple palmetto financings can exist (re-quotes). Prefer the most-progressed:
    # contract_signed_at > request_id > quote_id.
    def _progress_score(rec):
        ext = rec["external"]
        return (
            1 if ext.get("contract_signed_at") else 0,
            1 if ext.get("request_id") else 0,
            1 if ext.get("quote_id") else 0,
        )

    best = max(palmetto_records, key=_progress_score)
    external = best["external"]
    financier = best["financier"]

    fields = {}
    if external.get("consumer_id"):
        fields["LightReach_Account_ID"] = external["consumer_id"]
        fields["LightReach_Account_URL"] = f"https://palmetto.finance/accounts/{external['consumer_id']}"
    if external.get("request_id"):
        fields["LightReach_Request_ID"] = external["request_id"]
    if external.get("quote_id"):
        fields["LightReach_Quote_ID"] = external["quote_id"]
    if external.get("provider_status"):
        fields["LightReach_Finance_Status"] = external["provider_status"]
    if external.get("contract_signed_at"):
        fields["LightReach_Contract_Signed_At"] = _normalize_aurora_datetime(
            external["contract_signed_at"]
        )
    if financier.get("status"):
        fields["LightReach_Application_Status"] = financier["status"]
    return fields


def extract_lightreach_install_fields_for_project(project_id):
    """
    Walk every design under a project, gather every palmetto financing across
    all of them, and return the field dict for the most-progressed one. This
    is strictly better than picking the first design with a palmetto financing
    since LightReach state evolves on whichever design was the actual contract.

    Returns {} if no palmetto financing exists anywhere on the project.
    """
    tenant_id = os.getenv("AURORA_TENANT_ID")
    designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"
    designs_resp = _aurora_get_with_retry(designs_url)
    if designs_resp.status_code != 200:
        logger.warning(
            f"extract_lightreach_install_fields_for_project: designs pull failed | "
            f"project_id={project_id} status={designs_resp.status_code}"
        )
        return {}
    designs = designs_resp.json().get("designs", [])
    if not designs:
        return {}

    # Gather every palmetto financing across every design.
    all_palmetto = []
    for d in designs:
        d_id = d.get("id")
        if not d_id:
            continue
        list_resp = pull_financings(d_id)
        if list_resp.status_code != 200:
            continue
        listed = list_resp.json()
        summaries = listed.get("financings") if isinstance(listed, dict) else listed
        if not summaries:
            continue
        for s in summaries:
            f_id = s.get("id") if isinstance(s, dict) else None
            if not f_id:
                continue
            full_resp = pull_financing(d_id, f_id)
            if full_resp.status_code != 200:
                continue
            full = full_resp.json().get("financing", full_resp.json())
            financier = full.get("financier") or {}
            if financier.get("provider") != "palmetto":
                continue
            external = financier.get("external") or {}
            if not external.get("consumer_id"):
                continue
            all_palmetto.append({"financier": financier, "external": external})

    if not all_palmetto:
        return {}

    def _progress_score(rec):
        ext = rec["external"]
        return (
            1 if ext.get("contract_signed_at") else 0,
            1 if ext.get("request_id") else 0,
            1 if ext.get("quote_id") else 0,
        )

    best = max(all_palmetto, key=_progress_score)
    external = best["external"]
    financier = best["financier"]

    fields = {}
    if external.get("consumer_id"):
        fields["LightReach_Account_ID"] = external["consumer_id"]
        fields["LightReach_Account_URL"] = f"https://palmetto.finance/accounts/{external['consumer_id']}"
    if external.get("request_id"):
        fields["LightReach_Request_ID"] = external["request_id"]
    if external.get("quote_id"):
        fields["LightReach_Quote_ID"] = external["quote_id"]
    if external.get("provider_status"):
        fields["LightReach_Finance_Status"] = external["provider_status"]
    if external.get("contract_signed_at"):
        fields["LightReach_Contract_Signed_At"] = _normalize_aurora_datetime(
            external["contract_signed_at"]
        )
    if financier.get("status"):
        fields["LightReach_Application_Status"] = financier["status"]
    return fields


# ------------------------
# Find Install by Aurora Project ID
# ------------------------
def find_install(project_id, access_token):
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    api_domain = os.getenv("ZOHO_API_DOMAIN")
    url = f"{api_domain}/crm/v2/Installs/search?criteria=(Aurora_Project_ID:equals:{project_id})"

    return requests.get(url, headers=headers)


# ------------------------
# Aurora Details mirror fields
# ------------------------
def _verify_and_repair_pricing(install_id, pricing_fields, headers, api_domain, label=""):
    """
    After writing an install update that included aurora_details_from_pricing,
    re-read the install to confirm Final_System_Price was actually persisted.
    If it's still null/zero, do a second targeted PUT with only the pricing fields.

    Returns "ok" | "repaired" | "repair_failed" | "no_pricing_data"
    """
    pricing_payload = aurora_details_from_pricing(pricing_fields)
    if not pricing_payload or not any(v for v in pricing_payload.values() if v):
        return "no_pricing_data"

    try:
        verify_resp = requests.get(
            f"{api_domain}/crm/v2/Installs/{install_id}"
            f"?fields=id,Final_System_Price",
            headers=headers,
        )
        if verify_resp.status_code != 200:
            logger.warning(f"{label} pricing verify: install re-read failed ({verify_resp.status_code})")
            return "repair_failed"

        data = verify_resp.json().get("data") or []
        if not data:
            return "repair_failed"

        current_price = data[0].get("Final_System_Price")
        if current_price not in (None, 0, 0.0):
            return "ok"

        # Price is missing — retry with a dedicated write.
        logger.warning(f"{label} pricing verify: Final_System_Price missing after write, retrying")
        repair_resp = requests.put(
            f"{api_domain}/crm/v2/Installs",
            headers=headers,
            json={"data": [{"id": install_id, **pricing_payload}]},
        )
        ok, code, msg = _zoho_update_ok(repair_resp)
        if ok and repair_resp.status_code in [200, 201, 202]:
            logger.info(f"{label} pricing verify: repair write succeeded | install_id={install_id}")
            return "repaired"
        else:
            logger.warning(
                f"{label} pricing verify: repair write failed | "
                f"install_id={install_id} http={repair_resp.status_code} code={code} msg={msg}"
            )
            return "repair_failed"

    except Exception:
        logger.exception(f"{label} pricing verify: exception during verify/repair")
        return "repair_failed"


def _zoho_update_ok(response) -> tuple:
    """
    Inspect a Zoho CRM PUT/POST response body for per-record success.
    Returns (ok: bool, code: str, message: str).
    Zoho returns HTTP 200 even for per-record failures (INVALID_DATA, etc.),
    so checking HTTP status alone is insufficient.
    """
    try:
        first = (response.json().get("data") or [{}])[0]
        code = first.get("code") or "UNKNOWN"
        msg = first.get("message") or ""
        return code == "SUCCESS", code, msg
    except Exception:
        return True, "UNKNOWN", ""  # non-JSON body; assume OK if HTTP was 2xx


def aurora_details_from_pricing(pricing_fields):
    """Return the subset of snapshot pricing_fields to mirror onto the Install record."""
    keys = [
        "Final_System_Price",
        "Price_Per_Watt",
        "Gross_Price_Per_Watt",
        "Base_Price",
        "Adders_Total",
        "Discounts_Total",
        "Consultant_Comp_PPW",
        "Helio_Lead_Fee_PPW",
    ]
    result = {k: pricing_fields[k] for k in keys if k in pricing_fields}
    if "Referral_Payout" in pricing_fields:
        result["Referral_Payout_PPW"] = pricing_fields["Referral_Payout"]
    return result


# ------------------------
# Create Snapshot Record
# ------------------------
def create_snapshot(snapshot_data, access_token):
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

    api_domain = os.getenv("ZOHO_API_DOMAIN")
    url = f"{api_domain}/crm/v2/Aurora_Design_Snapshots"

    payload = {"data": [snapshot_data]}

    return requests.post(url, headers=headers, json=payload)


# ------------------------
# Webhook: Aurora — Milestone Created (auto-snapshot on "sold")
# ------------------------
# Aurora fires this webhook every time a milestone is created on a project.
# We filter to the `sold` stage server-side, look up the matching Zoho Install
# by Aurora_Project_ID, and call the same snapshot helper that
# /internal/create-initial-snapshot uses. This closes the timing gap that
# leaves Zoho-side workflow rules (which fire on Install creation) unable to
# reliably trigger snapshot creation when the rep marks the design "sold"
# *after* the Install record was already created in Zoho.
#
# Aurora's URL template style (from their docs):
#   https://your-app.example.com/webhook/aurora/milestone-created
#       ?project_id=<PROJECT_ID>&design_id=<DESIGN_ID>
#       &stage=<STAGE>&source=<SOURCE>
#
# Optional shared-secret auth: set AURORA_WEBHOOK_TOKEN in env, then include
# `&token=<value>` in the URL Aurora is configured to call. If the env var
# is unset, the endpoint is open (use only inside a trusted network).
@app.api_route("/webhook/aurora/milestone-created", methods=["GET", "POST"])
async def aurora_milestone_created_webhook(request: Request):
    try:
        # Aurora's URL template substitutes everything as query params, but
        # accept body fields as a fallback in case POST sends a JSON body.
        params = dict(request.query_params)
        body = {}
        if request.method == "POST":
            try:
                raw = await request.body()
                if raw:
                    body = json.loads(raw)
            except (ValueError, json.JSONDecodeError):
                body = {}

        def _pick(key):
            return params.get(key) or body.get(key)

        project_id = _pick("project_id")
        design_id = _pick("design_id")
        stage = _pick("stage")
        source = _pick("source")
        token = _pick("token")

        logger.info(
            f"Aurora milestone webhook | project_id={project_id} design_id={design_id} "
            f"stage={stage} source={source}"
        )

        # Optional token check
        expected_token = os.getenv("AURORA_WEBHOOK_TOKEN")
        if expected_token and token != expected_token:
            logger.warning("Aurora milestone webhook rejected — invalid token")
            raise HTTPException(status_code=401, detail="Unauthorized")

        if not project_id:
            return {"status": "ignored - missing project_id"}

        # Server-side filter: only act on "sold" milestones, even if Aurora's
        # filter happens to pass us something else.
        if (stage or "").lower() != "sold":
            return {"status": "ignored - stage is not sold", "stage": stage}

        # Look up the Zoho Install by Aurora_Project_ID.
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("Aurora milestone webhook: failed to obtain Zoho token")
            return {"status": "failed - no zoho token"}

        find_resp = find_install(project_id, access_token)
        if find_resp.status_code != 200:
            logger.warning(
                f"Aurora milestone webhook: install search failed | "
                f"project_id={project_id} status={find_resp.status_code}"
            )
            return {"status": "failed - install lookup error"}

        records = find_resp.json().get("data", []) or []
        if not records:
            logger.warning(
                f"Aurora milestone webhook: no install found | project_id={project_id}"
            )
            return {"status": "no install found for project"}

        install = records[0]
        install_id = install.get("id")

        # If the install has a Deal lookup, pass it through to the snapshot.
        deal = install.get("Deal") or {}
        deal_id = deal.get("id") if isinstance(deal, dict) else None

        result = _create_initial_snapshot_for_install(
            install_id=install_id,
            project_id=project_id,
            deal_id=deal_id,
        )
        logger.info(
            f"Aurora milestone webhook: result | install_id={install_id} "
            f"project_id={project_id} status={result.get('status')}"
        )
        return result

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unhandled exception in aurora_milestone_created_webhook")
        return {"status": "failed - exception"}


# ------------------------
# Webhook: LightReach (Palmetto) — Contract Signed & Status Events
# ------------------------
@app.post("/webhook/lightreach")
async def lightreach_webhook(request: Request):
    try:
        # Validate Palmetto-generated API key (sent as `apiKey` header)
        expected_key = os.getenv("LIGHTREACH_API_KEY")
        received_key = request.headers.get("apiKey")

        if expected_key and received_key != expected_key:
            logger.warning(
                f"LightReach webhook rejected — invalid apiKey | "
                f"headers={dict(request.headers)}"
            )
            raise HTTPException(status_code=401, detail="Unauthorized")

        body = await request.json()
        logger.info(f"LightReach webhook received | payload={json.dumps(body)}")

        # --- Extract fields from payload (flexible — log raw if structure changes) ---
        event_type = body.get("event") or body.get("eventType") or body.get("type") or "unknown"
        # LightReach's `accountId` is the primary stable identifier on every event.
        # We mirror it onto the Install as LightReach_Account_ID via Aurora's
        # financing.financier.external.consumer_id field.
        account_id = (
            body.get("accountId")
            or body.get("account_id")
            or (body.get("account") or {}).get("id")
        )
        quote_id = (
            body.get("quoteId")
            or body.get("quote_id")
            or (body.get("quote") or {}).get("id")
        )
        contact_id = (
            body.get("contactId")
            or body.get("contact_id")
            or body.get("alchemyContactId")
        )

        customer = body.get("customer") or body.get("homeowner") or body.get("applicant") or {}
        customer_email = (
            body.get("email")
            or customer.get("email")
            or ""
        ).strip().lower()

        signed_at = (
            body.get("signedAt")
            or body.get("signed_at")
            or body.get("contractSignedAt")
            or body.get("timestamp")
        )

        logger.info(
            f"LightReach event | type={event_type} account_id={account_id} "
            f"quote_id={quote_id} contact_id={contact_id} email={customer_email}"
        )

        # --- Find matching Zoho Install ---
        # Primary path: match by LightReach_Account_ID (populated by aurora-zoho-sync
        # from financing.financier.external.consumer_id). This is the steady state.
        # Fallbacks: email, then LightReach_Quote_ID, for installs not yet bootstrapped.
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("LightReach webhook: failed to obtain Zoho token")
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        install_id = None

        if account_id:
            search_url = (
                f"{api_domain}/crm/v2/Installs/search"
                f"?criteria=(LightReach_Account_ID:equals:{quote(account_id, safe='')})"
            )
            search_resp = requests.get(search_url, headers=headers)
            if search_resp.status_code == 200:
                records = search_resp.json().get("data", [])
                if records:
                    install_id = records[0].get("id")

        if not install_id and customer_email:
            search_url = (
                f"{api_domain}/crm/v2/Installs/search"
                f"?criteria=(Primary_Email:equals:{quote(customer_email, safe='')})"
            )
            search_resp = requests.get(search_url, headers=headers)
            if search_resp.status_code == 200:
                records = search_resp.json().get("data", [])
                if records:
                    install_id = records[0].get("id")

        if not install_id and quote_id:
            search_url = (
                f"{api_domain}/crm/v2/Installs/search"
                f"?criteria=(LightReach_Quote_ID:equals:{quote(quote_id, safe='')})"
            )
            search_resp = requests.get(search_url, headers=headers)
            if search_resp.status_code == 200:
                records = search_resp.json().get("data", [])
                if records:
                    install_id = records[0].get("id")

        if not install_id:
            logger.warning(
                f"LightReach webhook: no Install found | "
                f"account_id={account_id} email={customer_email} quote_id={quote_id}"
            )
            return {"status": "logged - no matching install"}

        logger.info(f"LightReach webhook: matched Install id={install_id}")

        # --- Build update fields ---
        timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()

        update_fields = {
            "id": install_id,
            "LightReach_Finance_Status": event_type,
            "LightReach_Last_Updated": timestamp_now,
            "LightReach_Raw_Payload": json.dumps(body),
        }
        if account_id:
            # Persist the link so future webhooks for this install short-circuit
            # to the LightReach_Account_ID branch above.
            update_fields["LightReach_Account_ID"] = account_id
            update_fields["LightReach_Account_URL"] = f"https://palmetto.finance/accounts/{account_id}"
        if quote_id:
            update_fields["LightReach_Quote_ID"] = quote_id
        if contact_id:
            update_fields["LightReach_Contact_ID"] = contact_id

        # --- Event-specific logic ---
        if event_type == "contractSigned":
            update_fields["LightReach_Contract_Status"] = "contractSigned"
            if signed_at:
                update_fields["LightReach_Contract_Signed_At"] = signed_at

        elif event_type == "applicationStatus":
            status = (
                body.get("status")
                or body.get("applicationStatus")
                or body.get("state")
                or event_type
            )
            update_fields["LightReach_Finance_Status"] = status

        elif event_type == "stipulationAdded":
            stip = body.get("stipulation") or body.get("requirement") or {}
            stip_name = (
                stip.get("name") or stip.get("description") or stip.get("type")
                or body.get("stipulationName") or body.get("stipulationDescription")
                or "Unknown stipulation"
            )
            update_fields["LightReach_Stipulation_Action_Needed"] = True
            update_fields["LightReach_Outstanding_Stipulations"] = stip_name
            logger.info(f"LightReach stipulation added: {stip_name}")

        elif event_type == "allStipulationsCleared":
            update_fields["LightReach_Stipulation_Action_Needed"] = False
            update_fields["LightReach_Outstanding_Stipulations"] = ""

        elif event_type in ("stipulationCleared", "requirementCompleted", "requirementStatusChanged"):
            stip = body.get("stipulation") or body.get("requirement") or {}
            stip_name = (
                stip.get("name") or stip.get("description")
                or body.get("stipulationName") or body.get("requirementName") or ""
            )
            if stip_name:
                logger.info(f"LightReach {event_type}: {stip_name}")

        elif event_type == "milestoneAchieved":
            milestone = (
                body.get("newMilestone")
                or body.get("milestone")
                or body.get("milestoneName")
                or body.get("name")
                or ""
            )
            if isinstance(milestone, dict):
                milestone = milestone.get("name") or milestone.get("type") or ""
            logger.info(f"LightReach milestone achieved: {milestone}")
            milestone_l = str(milestone).lower()
            if (
                "ntp" in milestone_l
                or "noticetoproceed" in milestone_l
                or "notice to proceed" in milestone_l
            ):
                update_fields["LightReach_NTP_Granted_At"] = timestamp_now
                logger.info(f"LightReach NTP granted for Install id={install_id}")

        update_resp = requests.put(
            f"{api_domain}/crm/v2/Installs",
            headers=headers,
            json={"data": [update_fields]},
        )

        if update_resp.status_code not in [200, 201, 202]:
            logger.error(
                f"LightReach webhook: Install update failed | "
                f"status={update_resp.status_code} | body={update_resp.text}"
            )
            return {"status": "failed - install update error"}

        logger.info(
            f"LightReach webhook: Install id={install_id} updated | "
            f"event={event_type} quote_id={quote_id}"
        )
        return {"status": "processed"}

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unhandled exception in LightReach webhook")
        return {"status": "failed - exception"}


@app.api_route("/webhook/aurora", methods=["GET", "POST"])
async def aurora_webhook(request: Request):
    try:
        # Validate secret
        expected_secret = os.getenv("AURORA_WEBHOOK_SECRET")
        received_secret = request.headers.get("X-Aurora-Webhook-Secret")

        if received_secret != expected_secret:
            raise HTTPException(status_code=401, detail="Unauthorized")

        params = dict(request.query_params)

        project_id = params.get("project_id")
        design_id = params.get("design_id")

        logger.info(f"Webhook received | project_id={project_id} design_id={design_id}")

        if not project_id or not design_id:
            logger.warning("Missing project_id or design_id")
            return {"status": "ignored - missing ids"}

        event_id = f"{project_id[:6]}-{design_id[:6]}"
        logger.info(f"[{event_id}] Processing milestone event")

        # ------------------------
        # Pull Aurora Data
        # ------------------------
        design_response = pull_design(design_id)
        pricing_response = pull_pricing(design_id)
        summary_response = pull_design_summary(design_id)

        logger.info(f"[{event_id}] Design pull status={design_response.status_code}")
        logger.info(f"[{event_id}] Pricing pull status={pricing_response.status_code}")

        if design_response.status_code != 200 or pricing_response.status_code != 200:
            logger.error(f"[{event_id}] Aurora pull failed")
            return {"status": "failed - aurora pull error"}

        design_root = design_response.json()
        design_json = design_root.get("design", design_root)

        pricing_root = pricing_response.json()
        pricing_json = pricing_root.get("pricing", pricing_root)

        summary_json = summary_response.json().get("design", {}) if summary_response.status_code == 200 else {}

        # Extract all pricing/equipment/milestone fields via shared helper
        pricing_fields = extract_pricing_fields(design_json, pricing_json, summary_json)
        system_size_watts = pricing_fields["System_Size_STC_Watts"]
        milestone_name = pricing_fields["Aurora_Milestone"]
        es_upline_discount_ppw = pricing_fields["ES_Upline_Discount_PPW"]
        evp_upline_discount_ppw = pricing_fields["EVP_Upline_Discount_PPW"]

        logger.info(f"[{event_id}] Resolved System Size (Watts)={system_size_watts}")



        # ------------------------
        # Zoho Token
        # ------------------------
        access_token = get_zoho_access_token()
        if not access_token:
            logger.error(f"[{event_id}] Failed to obtain Zoho access token")
            return {"status": "failed - no zoho token"}

        # ------------------------
        # Find Install
        # ------------------------
        install_response = find_install(project_id, access_token)

        if install_response.status_code == 204:
            # 204 = no records found — Install not yet created, graceful skip
            logger.info(
                f"[{event_id}] Install not yet created for project_id={project_id} | "
                f"milestone={milestone_name} | skipping (204)"
            )
            return {"status": "skipped - install not yet created"}

        if install_response.status_code != 200:
            logger.error(
                f"[{event_id}] Install search failed | "
                f"status={install_response.status_code} | "
                f"body={install_response.text}"
            )
            return {"status": "failed - install search error"}

        install_data = install_response.json().get("data")
        if not install_data:
            # Install doesn't exist yet — expected when a milestone fires before the
            # Install record is created in Zoho. The initial sold snapshot is handled
            # by /internal/create-initial-snapshot (triggered on Install creation).
            # Future milestone webhooks will find the install once it exists.
            logger.info(
                f"[{event_id}] Install not yet created for project_id={project_id} | "
                f"milestone={milestone_name} | skipping"
            )
            return {"status": "skipped - install not yet created"}

        install_record = install_data[0]
        install_id = install_record.get("id")

        opportunity = install_record.get("Opportunity")
        deal_id = opportunity.get("id") if opportunity else None

        # Decide what to do with this milestone snapshot:
        #   * "advancing" milestones (sold, installed, permission_to_operate)
        #     should become the install's Active_Snapshot — these reflect
        #     forward progress in the project lifecycle.
        #   * everything else (canceled_*, offer, etc.) just appends to the
        #     install's snapshot related list as historical record without
        #     touching Active_Snapshot.
        # On the *very first* sold milestone we additionally pull LightReach
        # IDs from Aurora's financings (one-time bootstrap). Subsequent
        # promotions don't refresh LightReach fields — the LightReach webhook
        # handler is the authoritative source for those after bootstrap.
        ADVANCING_MILESTONES = {
            "sold",
            "installed",
            "permission_to_operate",
            "permission to operate",
            "pto",
            "permissiontooperate",
        }
        milestone_lc = (milestone_name or "").lower().strip()
        is_advancing = milestone_lc in ADVANCING_MILESTONES

        existing_active_snapshot = install_record.get("Active_Snapshot")
        is_initial_sold = milestone_lc == "sold" and not existing_active_snapshot
        previous_active_snapshot_id = (
            existing_active_snapshot.get("id")
            if isinstance(existing_active_snapshot, dict)
            else None
        )

        # ------------------------
        # Pull Sales Org Redline from Install (Formula Field)
        # ------------------------
        try:
            sales_org_redline_ppw = float(install_record.get("Sales_Org_Redline_PPW") or 0)
        except (TypeError, ValueError):
            sales_org_redline_ppw = 0.0

        # ------------------------
        # Calculate Effective Redline At Sale
        # ------------------------
        redline_at_sale = (
            sales_org_redline_ppw
            + es_upline_discount_ppw
            + evp_upline_discount_ppw
        )

        # ------------------------
        # Snapshot Creation
        # ------------------------
        timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()

        snapshot_name = f"{project_id[:8]} | {design_id[:8]} | {milestone_name} | {timestamp_now}"

        aurora_design_url = f"https://v2.aurorasolar.com/projects/{project_id}/designs/{design_id}/cad"
        aurora_project_url = f"https://v2.aurorasolar.com/projects/{project_id}/overview/dashboard"

        snapshot_data = {
            "Name": snapshot_name,
            "Aurora_Project_ID": project_id,
            "Aurora_Design_ID": design_id,
            "Webhook_Received_At": timestamp_now,
            "Install": {"id": install_id},
            "Deal": {"id": deal_id} if deal_id else None,
            "Aurora_Design_URL": aurora_design_url,
            "Aurora_Project_URL": aurora_project_url,
            "Sales_Org_Redline_PPW": sales_org_redline_ppw,
            "Redline_At_Sale": redline_at_sale,
            "Processing_Status": "Initial Locked" if is_initial_sold else "Processed",
            "Snapshot_Is_Active": is_advancing,
            **pricing_fields,
        }

        snapshot_create_response = create_snapshot(snapshot_data, access_token)

        if snapshot_create_response.status_code not in [200, 201, 202]:
            logger.error(
                f"[{event_id}] Snapshot creation failed | "
                f"status={snapshot_create_response.status_code} | "
                f"body={snapshot_create_response.text}"
            )
            return {"status": "failed - snapshot creation error"}

        # Handle Zoho's per-record success/failure code in the body.
        try:
            create_resp_json = snapshot_create_response.json()
        except ValueError:
            create_resp_json = {}
        first_record = (create_resp_json.get("data") or [{}])[0]
        if first_record.get("code") and first_record.get("code") != "SUCCESS":
            logger.warning(
                f"[{event_id}] Snapshot creation rejected | "
                f"code={first_record.get('code')} message={first_record.get('message')} "
                f"details={json.dumps(first_record.get('details'))}"
            )
            return {"status": f"failed - snapshot creation: {first_record.get('code')}"}

        snapshot_id = (first_record.get("details") or {}).get("id")

        logger.info(
            f"[{event_id}] Snapshot created successfully | "
            f"status={snapshot_create_response.status_code} snapshot_id={snapshot_id} "
            f"milestone={milestone_lc} is_advancing={is_advancing} "
            f"is_initial_sold={is_initial_sold}"
        )

        # If this milestone advances the project (sold / installed / PTO),
        # promote the new snapshot to active. Also demote the previously-active
        # snapshot so Snapshot_Is_Active is mutually exclusive in practice. On
        # the very first sold, additionally bootstrap LightReach fields.
        if is_advancing and snapshot_id:
            api_domain = os.getenv("ZOHO_API_DOMAIN")
            zoho_headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

            # Demote the previously-active snapshot, if any and if it's not
            # the one we just created (it isn't — different ID).
            if previous_active_snapshot_id and previous_active_snapshot_id != snapshot_id:
                demote_payload = {
                    "data": [{
                        "id": previous_active_snapshot_id,
                        "Snapshot_Is_Active": False,
                    }]
                }
                demote_resp = requests.put(
                    f"{api_domain}/crm/v2/Aurora_Design_Snapshots",
                    headers=zoho_headers,
                    json=demote_payload,
                )
                if demote_resp.status_code in [200, 201, 202]:
                    logger.info(
                        f"[{event_id}] Previous active snapshot demoted | "
                        f"snapshot_id={previous_active_snapshot_id}"
                    )
                else:
                    logger.warning(
                        f"[{event_id}] Previous active snapshot demote failed | "
                        f"snapshot_id={previous_active_snapshot_id} "
                        f"status={demote_resp.status_code} body={demote_resp.text[:300]}"
                    )

            install_update = {
                "id": install_id,
                "Active_Snapshot": {"id": snapshot_id},
                **aurora_details_from_pricing(pricing_fields),
            }
            # LightReach bootstrap only on the very first sold — subsequent
            # promotions leave LightReach fields alone (they're maintained by
            # the LightReach webhook handler from then on).
            if is_initial_sold:
                lightreach_fields = extract_lightreach_install_fields(design_id)
                install_update.update(lightreach_fields)
            else:
                lightreach_fields = {}

            update_resp = requests.put(
                f"{api_domain}/crm/v2/Installs",
                headers=zoho_headers,
                json={"data": [install_update]},
            )
            if update_resp.status_code not in [200, 201, 202]:
                logger.warning(
                    f"[{event_id}] Install promotion update failed (HTTP) | "
                    f"status={update_resp.status_code} body={update_resp.text[:300]}"
                )
            else:
                ok, code, msg = _zoho_update_ok(update_resp)
                if ok:
                    logger.info(
                        f"[{event_id}] Install promoted with active snapshot | "
                        f"install_id={install_id} milestone={milestone_lc} "
                        f"lightreach_keys={list(lightreach_fields.keys())}"
                    )
                    repair = _verify_and_repair_pricing(
                        install_id, pricing_fields, zoho_headers, api_domain,
                        label=f"[{event_id}] milestone_webhook:"
                    )
                    if repair not in ("ok", "no_pricing_data"):
                        logger.info(
                            f"[{event_id}] pricing verify result={repair} install_id={install_id}"
                        )
                else:
                    logger.warning(
                        f"[{event_id}] Install promotion update rejected by Zoho | "
                        f"install_id={install_id} code={code} message={msg}"
                    )
                    repair = _verify_and_repair_pricing(
                        install_id, pricing_fields, zoho_headers, api_domain,
                        label=f"[{event_id}] milestone_webhook:"
                    )
                    logger.info(
                        f"[{event_id}] pricing repair after rejected update result={repair} install_id={install_id}"
                    )

        return {
            "status": "processed",
            "is_advancing": is_advancing,
            "is_initial_sold": is_initial_sold,
        }
    except Exception:
        logger.exception("Unhandled exception during webhook processing")


# ------------------------
# IC Monitor
# ------------------------

@app.post("/run-ic-monitor")
async def run_ic_monitor_endpoint(background_tasks: BackgroundTasks):
    from ic_monitor import run_ic_monitor
    background_tasks.add_task(run_ic_monitor, get_zoho_access_token)
    return {"status": "ic monitor started"}


@app.post("/clean-ic-notes")
async def clean_ic_notes_endpoint(background_tasks: BackgroundTasks):
    from ic_monitor import clean_ic_notes
    background_tasks.add_task(clean_ic_notes, get_zoho_access_token)
    return {"status": "ic note cleanup started"}

# ------------------------
# Commissions Data Endpoint
# ------------------------

@app.post("/commissions")
async def get_commissions(request: Request):
    """
    Accepts: {"project_ids": ["aurora-uuid-1", ...]}
    Returns commission-relevant pricing pulled fresh from Aurora for each project.

    Commission formula:
      base_ppw          = base_price / system_size_watts
      base_commission   = (base_ppw - 2.50) * system_size_watts
      consultant_comm   = consultant_comp_ppw * system_size_watts
      total_commission  = base_commission + consultant_commission
    """
    body = await request.json()
    project_ids = body.get("project_ids") or []
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids required")

    tenant_id = os.getenv("AURORA_TENANT_ID")
    BASE_PPW_FLOOR = 2.50
    results = []

    for project_id in project_ids:
        designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"
        designs_resp = _aurora_get_with_retry(designs_url)
        if designs_resp.status_code != 200:
            results.append({"project_id": project_id, "error": f"designs fetch failed ({designs_resp.status_code})"})
            continue

        designs = designs_resp.json().get("designs", [])
        if not designs:
            results.append({"project_id": project_id, "error": "no designs found"})
            continue

        sold_designs = [d for d in designs if (d.get("milestone") or {}).get("milestone") == "sold"]
        if len(sold_designs) != 1:
            results.append({"project_id": project_id, "error": f"expected 1 sold design, found {len(sold_designs)}"})
            continue
        design_id = sold_designs[0].get("id")

        pricing_resp = pull_pricing(design_id)
        if pricing_resp.status_code != 200:
            results.append({"project_id": project_id, "design_id": design_id, "error": f"pricing fetch failed ({pricing_resp.status_code})"})
            continue

        design_resp = pull_design(design_id)
        summary_resp = pull_design_summary(design_id)
        design_json = design_resp.json() if design_resp.status_code == 200 else {}
        pricing_raw = pricing_resp.json()
        pricing_json = pricing_raw.get("pricing") or pricing_raw
        summary_json = summary_resp.json() if summary_resp.status_code == 200 else {}

        fields = extract_pricing_fields(design_json, pricing_json, summary_json)

        system_size_watts = fields.get("System_Size_STC_Watts") or 0
        base_price = float(fields.get("Base_Price") or 0)
        consultant_comp_ppw = float(fields.get("Consultant_Comp_PPW") or 0)
        referral_payout_ppw = float(fields.get("Referral_Payout_PPW") or 0)
        helio_lead_fee_ppw = float(fields.get("Helio_Lead_Fee_PPW") or 0)
        adder_name_list = fields.get("Adder_Name_List") or ""
        adder_details_raw = fields.get("Adder_Details_JSON") or "[]"
        discounts_total = float(fields.get("Discounts_Total") or 0)
        discount_name_list = fields.get("Discount_Name_List") or ""
        discount_details_raw = fields.get("Discount_Details_JSON") or "[]"

        base_ppw = round(base_price / system_size_watts, 6) if system_size_watts else 0.0
        base_commission = round((base_ppw - BASE_PPW_FLOOR) * system_size_watts, 2) if system_size_watts else 0.0
        consultant_commission = round(consultant_comp_ppw * system_size_watts, 2)
        total_commission = round(base_commission + consultant_commission, 2)

        try:
            adder_details = json.loads(adder_details_raw) if isinstance(adder_details_raw, str) else adder_details_raw
        except (ValueError, TypeError):
            adder_details = []

        try:
            discount_details = json.loads(discount_details_raw) if isinstance(discount_details_raw, str) else discount_details_raw
        except (ValueError, TypeError):
            discount_details = []

        results.append({
            "project_id": project_id,
            "design_id": design_id,
            "system_size_watts": system_size_watts,
            "system_size_kw": round(system_size_watts / 1000, 3) if system_size_watts else 0,
            "base_price": base_price,
            "base_ppw": base_ppw,
            "consultant_comp_ppw": consultant_comp_ppw,
            "referral_payout_ppw": referral_payout_ppw,
            "helio_lead_fee_ppw": helio_lead_fee_ppw,
            "adder_name_list": adder_name_list,
            "adder_details": adder_details,
            "base_commission": base_commission,
            "consultant_commission": consultant_commission,
            "total_commission": total_commission,
            "discounts_total": discounts_total,
            "discount_name_list": discount_name_list,
            "discount_details": discount_details,
        })

    return {"results": results}


# ------------------------
# Debug: Raw Aurora Pricing
# ------------------------

@app.post("/commissions/debug-pricing")
async def debug_pricing(request: Request):
    """Returns raw Aurora pricing JSON for the first design of a project."""
    body = await request.json()
    project_id = body.get("project_id")
    if not project_id:
        raise HTTPException(status_code=400, detail="project_id required")

    tenant_id = os.getenv("AURORA_TENANT_ID")
    designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"
    designs_resp = _aurora_get_with_retry(designs_url)
    if designs_resp.status_code != 200:
        return {"error": f"designs fetch failed ({designs_resp.status_code})", "body": designs_resp.text}

    designs = designs_resp.json().get("designs", [])
    if not designs:
        return {"error": "no designs found", "raw": designs_resp.json()}

    sold_designs = [d for d in designs if (d.get("milestone") or {}).get("milestone") == "sold"]
    if len(sold_designs) != 1:
        return {"error": f"expected 1 sold design, found {len(sold_designs)}", "designs": [
            {"id": d.get("id"), "milestone": (d.get("milestone") or {}).get("milestone"), "updated_at": d.get("updated_at")}
            for d in designs
        ]}
    design_id = sold_designs[0].get("id")

    pricing_resp = pull_pricing(design_id)
    return {
        "design_id": design_id,
        "pricing_status": pricing_resp.status_code,
        "pricing_keys": list(pricing_resp.json().keys()) if pricing_resp.status_code == 200 else None,
        "pricing_raw": pricing_resp.json() if pricing_resp.status_code == 200 else pricing_resp.text,
    }


# ============================================================================
# Commission Run — Automated Sheet Output
# ============================================================================
#
# POST /commissions/run
#   Pulls all Zoho Installs with an Aurora_Project_ID created on or after
#   a cutoff date (default 2026-01-01), fetches fresh pricing from Aurora
#   for the sold design on each project, calculates commissions, and writes
#   a new tab to the master commission Google Sheet with live formulas.
#
# POST /webhook/zoho/project-intake
#   Triggered by a Zoho blueprint when a project moves to Project Intake.
#   Runs the same logic for that single project and appends a tab.
#
# Master sheet ID (create once, tabs accumulate per run):
COMMISSION_SHEET_ID = "1JUXFXJJFOpbzNAnl-UH1bN_HIoYvFQtbnuwSFYA8L5I"

# Impersonate this user when writing to Sheets (must be in the same Google Workspace).
SHEETS_IMPERSONATE_EMAIL = "hdohnert@helio.solar"

COMMISSION_PPW_FLOOR = 2.50


def _build_sheets_service():
    """
    Build a Sheets API v4 client using the service account directly (no impersonation).
    The sheet must be shared with the service account email as Editor.
    """
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        logger.error("GOOGLE_SERVICE_ACCOUNT_JSON env var is missing")
        return None
    try:
        info = json.loads(raw)
    except ValueError:
        logger.exception("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")
        return None
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _fetch_all_commission_projects(cutoff_date: str = "2026-01-01") -> list[dict]:
    """
    Pull all Zoho Installs with an Aurora_Project_ID created on or after
    cutoff_date. Returns list of dicts with keys: customer, project_id,
    zoho_record_id, aurora_project_id, rep, stage.
    """
    token = get_zoho_access_token()
    if not token:
        return []
    api_domain = os.getenv("ZOHO_API_DOMAIN")
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    fields = "Name,Project_ID,Aurora_Project_ID,Sales_Representative,Owner,Project_Stage,Project_Created_Date,Commissions_Paid,Commissions_Fully_Paid"

    criteria = f"(Project_Created_Date:greater_equal:{cutoff_date})"
    results = []
    page = 1
    while True:
        url = (
            f"{api_domain}/crm/v7/Installs/search"
            f"?criteria={criteria}&fields={fields}&page={page}&per_page=200"
        )
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            logger.error(f"_fetch_all_commission_projects: Zoho fetch failed status={resp.status_code} body={resp.text[:200]}")
            break
        data = resp.json().get("data") or []
        for r in data:
            aurora_id = (r.get("Aurora_Project_ID") or "").strip()
            if not aurora_id:
                continue
            # Skip projects that are fully paid off
            if r.get("Commissions_Fully_Paid"):
                continue
            owner_obj = r.get("Owner")
            owner_name = (owner_obj.get("name") or "").strip() if isinstance(owner_obj, dict) else ""
            results.append({
                "customer": (r.get("Name") or "").strip(),
                "project_id": (r.get("Project_ID") or "").strip(),
                "zoho_record_id": r.get("id") or "",
                "aurora_project_id": aurora_id,
                "rep": (r.get("Sales_Representative") or "").strip(),
                "owner": owner_name,
                "stage": (r.get("Project_Stage") or "").strip(),
                "created_date": (r.get("Project_Created_Date") or "").strip(),
                "commissions_paid": r.get("Commissions_Paid") or "",
            })
        info = resp.json().get("info") or {}
        if not info.get("more_records"):
            break
        page += 1
        if page > 50:
            break
    return results


def _get_commission_data_for_project(aurora_project_id: str) -> dict:
    """
    Pull fresh pricing from Aurora for the sold design on a project.
    Returns a flat dict of commission fields, or {"error": "..."} on failure.
    """
    tenant_id = os.getenv("AURORA_TENANT_ID")
    designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{aurora_project_id}/designs"
    designs_resp = _aurora_get_with_retry(designs_url)
    if designs_resp.status_code != 200:
        return {"error": f"designs fetch failed ({designs_resp.status_code})"}

    designs = designs_resp.json().get("designs", [])
    sold_designs = [d for d in designs if (d.get("milestone") or {}).get("milestone") == "sold"]
    if len(sold_designs) != 1:
        return {"error": f"expected 1 sold design, found {len(sold_designs)}"}

    design_id = sold_designs[0].get("id")
    pricing_resp = pull_pricing(design_id)
    if pricing_resp.status_code != 200:
        return {"error": f"pricing fetch failed ({pricing_resp.status_code})"}

    design_resp = pull_design(design_id)
    summary_resp = pull_design_summary(design_id)
    design_json = design_resp.json() if design_resp.status_code == 200 else {}
    pricing_raw = pricing_resp.json()
    pricing_json = pricing_raw.get("pricing") or pricing_raw
    summary_json = summary_resp.json() if summary_resp.status_code == 200 else {}

    fields = extract_pricing_fields(design_json, pricing_json, summary_json)

    system_size_watts = fields.get("System_Size_STC_Watts") or 0
    base_price = float(fields.get("Base_Price") or 0)
    consultant_comp_ppw = float(fields.get("Consultant_Comp_PPW") or 0)
    referral_payout_ppw = float(fields.get("Referral_Payout_PPW") or 0)
    helio_lead_fee_ppw = float(fields.get("Helio_Lead_Fee_PPW") or 0)
    adder_name_list = fields.get("Adder_Name_List") or ""

    # Flat referral/subcontractor amounts from adder_details
    adder_details_raw = fields.get("Adder_Details_JSON") or "[]"
    try:
        adder_details = json.loads(adder_details_raw) if isinstance(adder_details_raw, str) else adder_details_raw
    except (ValueError, TypeError):
        adder_details = []

    referral_flat = 0.0
    subcontractor_total = 0.0
    subcontractor_notes = []
    for adder in adder_details:
        name = (adder.get("name") or "").strip()
        total = float(adder.get("total") or 0)
        if name == "A - Referral Payout":
            referral_flat += total
        elif name.startswith("D. MISC:") and total > 0:
            subcontractor_total += total
            subcontractor_notes.append(f"{name.replace('D. MISC: ', '')} ${total:,.2f}")

    final_system_price = float(fields.get("Final_System_Price") or 0)

    return {
        "design_id": design_id,
        "system_size_watts": system_size_watts,
        "base_price": base_price,
        "final_system_price": final_system_price,
        "consultant_comp_ppw": consultant_comp_ppw,
        "referral_payout_ppw": referral_payout_ppw,
        "referral_flat": referral_flat,
        "helio_lead_fee_ppw": helio_lead_fee_ppw,
        "adder_name_list": adder_name_list,
        "subcontractor_total": subcontractor_total,
        "subcontractor_notes": " | ".join(subcontractor_notes) if subcontractor_notes else "",
    }


def _write_commission_tab(svc, tab_name: str, rows: list[dict]) -> None:
    """
    Add a new tab to COMMISSION_SHEET_ID and write commission data with
    live Sheets formulas for every calculated field.

    Column layout (A:P) — matches "Payroll [date]" format:
      A  Customer
      B  Install Owner (ES)
      C  Sales Rep
      D  EVP                      ← always "Fred Stevens"
      E  Project ID
      F  Aurora Project ID
      G  System Size (W)          ← raw value
      H  System Size (kW)         =G{r}/1000
      I  Base Price ($)           ← raw value
      J  Base Price Per Watt      =IFERROR(I{r}/G{r},0)
      K  PPW Floor                ← constant $2.50
      L  Base PPW - Floor         =J{r}-K{r}
      M  Base Commission          =L{r}*G{r}
      N  Consultant Comp PPW      ← raw value
      O  Consultant Commission    =N{r}*G{r}
      P  Total Comp on Deal       =M{r}+O{r}
    """
    sheets = svc.spreadsheets()

    # 1. Add new sheet tab
    # Delete existing tab with same name if present
    existing = sheets.get(spreadsheetId=COMMISSION_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheets.batchUpdate(
                spreadsheetId=COMMISSION_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break

    add_sheet_body = {
        "requests": [{
            "addSheet": {
                "properties": {"title": tab_name}
            }
        }]
    }
    resp = sheets.batchUpdate(spreadsheetId=COMMISSION_SHEET_ID, body=add_sheet_body).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # 2. Build header + data rows
    # Column layout (A=0 … T=19):
    #  A  Project Created Date
    #  B  Customer
    #  C  Install Owner (ES)
    #  D  Sales Rep
    #  E  EVP
    #  F  Project ID
    #  G  Aurora Project ID
    #  H  System Size (W)
    #  I  System Size (kW)      =H/1000
    #  J  Base Price ($)        $
    #  K  Base PPW ($/W)        $ =J/H
    #  L  PPW Floor             $ constant
    #  M  Base PPW - Floor      $ =K-L
    #  N  Base Commission       $ =M*H
    #  O  Consultant PPW        $
    #  P  Consultant Commission $ =O*H
    #  Q  Total Comp on Deal    $ =N+P
    #  R  Zoho Link
    #  S  Aurora Link
    #  T  Commissions Paid (%)
    headers = [
        "Project Created Date",
        "Customer", "Install Owner (ES)", "Sales Rep", "EVP",
        "Project ID", "Aurora Project ID",
        "System Size (W)", "System Size (kW)",
        "Base Price ($)", "Base Price Per Watt ($/W)", "PPW Floor",
        "Base PPW - Floor", "Base Commission",
        "Consultant Comp PPW ($/W)", "Consultant Commission",
        "Total Comp on Deal",
        "Commissions Paid (%)", "Remaining Commission", "Zoho Link", "Aurora Link",
    ]

    zoho_base = "https://crm.zoho.com/crm/heliosolar/tab/CustomModule6/"
    aurora_base = "https://v2.aurorasolar.com/projects/"

    value_rows = [headers]
    for i, row in enumerate(rows, start=2):  # row 1 = header, data starts at 2
        r = str(i)
        if "error" in row:
            value_rows.append([
                row.get("created_date", ""), row.get("customer", ""),
                row.get("owner", ""), row.get("rep", ""), "Fred Stevens",
                row.get("project_id", ""), row.get("aurora_project_id", ""),
                row.get("error", ""), "", "", "", "", "", "", "", "", "", "", "", "",
            ])
            continue

        d = row["data"]
        zoho_id = row.get("zoho_record_id", "")
        aurora_id = row.get("aurora_project_id", "")
        zoho_link = f'=HYPERLINK("{zoho_base}{zoho_id}","Zoho")' if zoho_id else ""
        aurora_link = f'=HYPERLINK("{aurora_base}{aurora_id}","Aurora")' if aurora_id else ""

        value_rows.append([
            row.get("created_date", ""),               # A — project created date
            row.get("customer", ""),                   # B — customer
            row.get("owner", ""),                      # C — install owner (ES)
            row.get("rep", ""),                        # D — sales rep
            "Fred Stevens",                            # E — EVP
            row.get("project_id", ""),                 # F — project ID
            aurora_id,                                 # G — aurora project ID
            d["system_size_watts"],                    # H — raw watts
            f"=H{r}/1000",                             # I — kW
            d["base_price"],                           # J — raw base price
            f"=IFERROR(J{r}/H{r},0)",                  # K — base PPW
            COMMISSION_PPW_FLOOR,                      # L — floor
            f"=K{r}-L{r}",                             # M — margin
            f"=M{r}*H{r}",                             # N — base commission
            d["consultant_comp_ppw"],                  # O — raw consultant PPW
            f"=O{r}*H{r}",                             # P — consultant commission
            f"=N{r}+P{r}",                             # Q — total comp on deal
            row.get("commissions_paid", ""),           # R — commissions paid %
            f"=Q{r}*(1-IFERROR(R{r}/100,0))",         # S — remaining commission
            zoho_link,                                 # T — Zoho link
            aurora_link,                               # U — Aurora link
        ])

    # 3. Write values (formulas go as USER_ENTERED so Sheets evaluates them)
    sheets.values().update(
        spreadsheetId=COMMISSION_SHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": value_rows},
    ).execute()

    # Dollar format columns: J(9), K(10), L(11), M(12), N(13), O(14), P(15), Q(16)
    dollar_fmt = {"numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'}}
    dollar_requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": dollar_fmt},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
        for col in [9, 10, 11, 12, 13, 14, 15, 16, 18]  # J through Q, plus S (Remaining)
    ]

    # 4. Format header bold, freeze row, apply dollar formats
    format_requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        *dollar_requests,
    ]
    sheets.batchUpdate(spreadsheetId=COMMISSION_SHEET_ID, body={"requests": format_requests}).execute()
    logger.info(f"_write_commission_tab: wrote {len(rows)} rows to tab '{tab_name}'")


def _run_commission_batch(projects: list[dict], tab_name: str) -> dict:
    """Core logic: fetch Aurora data for each project and write to Sheets."""
    svc = _build_sheets_service()
    if not svc:
        return {"status": "failed", "reason": "could not build Sheets service"}

    rows = []
    for p in projects:
        logger.info(f"commission_batch: fetching {p['aurora_project_id']} ({p['customer']})")
        data = _get_commission_data_for_project(p["aurora_project_id"])
        if "error" in data:
            rows.append({**p, "error": data["error"]})
        else:
            rows.append({**p, "data": data})

    succeeded = [r for r in rows if "error" not in r]
    failed = [r for r in rows if "error" in r]
    _write_commission_tab(svc, tab_name, succeeded)
    return {"status": "ok", "tab": tab_name, "succeeded": len(succeeded), "failed": len(failed),
            "failed_projects": [{"project_id": r.get("project_id"), "error": r.get("error")} for r in failed]}


def _run_commission_batch_task(cutoff: str, tab_name: str) -> None:
    """Background task body for /commissions/run."""
    try:
        projects = _fetch_all_commission_projects(cutoff_date=cutoff)
        if not projects:
            logger.warning(f"commission_run_task: no projects found for cutoff={cutoff}")
            return
        logger.info(f"commission_run_task: found {len(projects)} projects, writing tab '{tab_name}'")
        result = _run_commission_batch(projects, tab_name)
        logger.info(f"commission_run_task: done | {result}")
    except Exception:
        logger.exception(f"commission_run_task: unhandled exception for tab '{tab_name}'")


@app.post("/commissions/run")
async def commissions_run(request: Request):
    """
    On-demand / backfill. Runs synchronously — may be slow for large batches.
    Pulls all Zoho projects with an Aurora ID created since cutoff_date,
    fetches Aurora pricing for the sold design, and writes a new tab.
    Body (optional): {"cutoff_date": "2026-01-01"}
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    cutoff = (body.get("cutoff_date") or "2026-01-01") if isinstance(body, dict) else "2026-01-01"
    now_label = datetime.datetime.now(datetime.timezone.utc).strftime("%-m-%-d-%Y")
    tab_name = f"Payroll {now_label}"
    projects = _fetch_all_commission_projects(cutoff_date=cutoff)
    if not projects:
        return {"status": "no projects found", "cutoff_date": cutoff}
    try:
        result = _run_commission_batch(projects, tab_name)
        result["project_count"] = len(projects)
        result["cutoff_date"] = cutoff
        return result
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc(), "project_count": len(projects)}


@app.post("/commissions/run-sync")
async def commissions_run_sync(request: Request):
    """Synchronous commission run for debugging — returns full result. Slow."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    cutoff = (body.get("cutoff_date") or "2026-06-01") if isinstance(body, dict) else "2026-06-01"
    now_label = datetime.datetime.now(datetime.timezone.utc).strftime("%-m-%-d-%Y")
    tab_name = f"Payroll {now_label}"
    projects = _fetch_all_commission_projects(cutoff_date=cutoff)
    if not projects:
        return {"status": "no projects found", "cutoff_date": cutoff}
    try:
        result = _run_commission_batch(projects, tab_name)
        result["project_count"] = len(projects)
        return result
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc(), "project_count": len(projects)}


@app.post("/webhook/zoho/project-intake")
async def project_intake_webhook(request: Request):
    """
    Triggered by Zoho blueprint when a project moves to Project Intake.
    Expected body: {"install_id": "...", "project_id": "PROJ-XXXX", "customer": "..."}
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    install_id = body.get("install_id") or ""
    project_id = body.get("project_id") or ""
    customer = body.get("customer") or ""

    # Look up Aurora Project ID, owner, and rep from Zoho if not provided
    aurora_project_id = body.get("aurora_project_id") or ""
    owner = body.get("owner") or ""
    rep = body.get("rep") or ""
    if (not aurora_project_id or not owner) and install_id:
        token = get_zoho_access_token()
        api_domain = os.getenv("ZOHO_API_DOMAIN")
        r = requests.get(
            f"{api_domain}/crm/v2/Installs/{install_id}?fields=Aurora_Project_ID,Name,Project_ID,Sales_Representative,Owner,Project_Stage",
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
        )
        if r.status_code == 200:
            rec = (r.json().get("data") or [{}])[0]
            aurora_project_id = aurora_project_id or (rec.get("Aurora_Project_ID") or "").strip()
            if not customer:
                customer = (rec.get("Name") or "").strip()
            if not project_id:
                project_id = (rec.get("Project_ID") or "").strip()
            if not rep:
                rep = (rec.get("Sales_Representative") or "").strip()
            if not owner:
                owner_obj = rec.get("Owner")
                owner = (owner_obj.get("name") or "").strip() if isinstance(owner_obj, dict) else ""

    if not aurora_project_id:
        logger.warning(f"project_intake_webhook: no Aurora Project ID for install_id={install_id}")
        return {"status": "skipped - no Aurora Project ID"}

    project = {
        "customer": customer,
        "project_id": project_id,
        "zoho_record_id": install_id,
        "aurora_project_id": aurora_project_id,
        "rep": rep,
        "owner": owner,
        "stage": "Project Intake",
    }

    tab_name = f"{customer} — Project Intake"
    result = _run_commission_batch([project], tab_name)
    return result


@app.get("/commissions/debug-zoho")
async def debug_zoho():
    """Quick diagnostic: test Zoho token and list endpoint."""
    # Show what env vars are present (masked)
    client_id = os.getenv("ZOHO_CLIENT_ID")
    client_secret = os.getenv("ZOHO_CLIENT_SECRET")
    refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
    api_domain = os.getenv("ZOHO_API_DOMAIN")
    env_check = {
        "ZOHO_CLIENT_ID": "set" if client_id else "MISSING",
        "ZOHO_CLIENT_SECRET": "set" if client_secret else "MISSING",
        "ZOHO_REFRESH_TOKEN": "set" if refresh_token else "MISSING",
        "ZOHO_API_DOMAIN": api_domain or "MISSING",
    }

    # Try token exchange directly and show raw response
    token_resp = requests.post("https://accounts.zoho.com/oauth/v2/token", data={
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    })
    token_body = token_resp.json()
    token = token_body.get("access_token")
    if not token:
        return {"env": env_check, "token_status": token_resp.status_code, "token_error": token_body}

    url = f"{api_domain}/crm/v7/Installs/search?criteria=(Project_Created_Date:greater_equal:2026-01-01)&fields=Name,Project_ID,Aurora_Project_ID&per_page=3"
    resp = requests.get(url, headers={"Authorization": f"Zoho-oauthtoken {token}"})
    return {"env": env_check, "token": "ok", "search_status": resp.status_code, "body": resp.json()}


@app.get("/commissions/debug-sheets")
async def debug_sheets():
    """Write one test row to the master sheet and return any error."""
    try:
        svc = _build_sheets_service()
        if not svc:
            return {"error": "could not build sheets service - check GOOGLE_SERVICE_ACCOUNT_JSON"}

        sheets = svc.spreadsheets()

        # Try adding a test tab
        tab_name = "DEBUG TEST"
        try:
            add_resp = sheets.batchUpdate(
                spreadsheetId=COMMISSION_SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
            ).execute()
            sheet_id = add_resp["replies"][0]["addSheet"]["properties"]["sheetId"]
        except Exception as e:
            return {"error": f"addSheet failed: {e}"}

        # Try writing a test value
        try:
            sheets.values().update(
                spreadsheetId=COMMISSION_SHEET_ID,
                range=f"'{tab_name}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [["Hello", "=1+1", "=A1&\" world\""]]},
            ).execute()
        except Exception as e:
            return {"sheet_id": sheet_id, "error": f"values.update failed: {e}"}

        return {"status": "ok", "tab": tab_name, "sheet_id": sheet_id}
    except Exception as e:
        return {"error": f"unexpected: {e}"}


@app.get("/commissions/debug-run")
async def debug_run():
    """Synchronous single-project commission run — surfaces errors directly."""
    try:
        # Use Frank Fazzino as the test project
        project = {
            "customer": "Frank Fazzino",
            "project_id": "PROJ-1606",
            "zoho_record_id": "",
            "aurora_project_id": "644ce760-a6d4-43e8-a9e0-04d02400dc76",
            "rep": "Erik Williams",
            "owner": "Fred Stevens",
            "stage": "Test",
        }
        data = _get_commission_data_for_project(project["aurora_project_id"])
        if "error" in data:
            return {"step": "aurora_fetch", "error": data["error"]}

        svc = _build_sheets_service()
        if not svc:
            return {"step": "sheets_service", "error": "could not build service"}

        tab_name = "DEBUG RUN"
        try:
            _write_commission_tab(svc, tab_name, [{**project, "data": data}])
        except Exception as e:
            return {"step": "write_tab", "error": str(e), "aurora_data": data}

        return {"status": "ok", "tab": tab_name, "aurora_data": data}
    except Exception as e:
        return {"error": f"unexpected: {e}"}


# ============================================================================
# Cash Flow Run — Automated Pipeline Sheet Output
# ============================================================================
#
# POST /cashflow/run
#   Pulls all Zoho Installs that are installed (have Substantial_Completion date
#   OR are in Energized/PTO/Inspection/Project Closeout stage), created on or
#   after a cutoff date. Fetches Aurora pricing/subcontractor data, calculates
#   payment dates and amounts by finance type, and writes a Pipeline tab to
#   the cash flow Google Sheet.
#
CASHFLOW_SHEET_ID = "15diQy50zSxuYVl6VINDb-4HOnLuT1xa1J2QtZAS65rM"
CASHFLOW_MATERIALS_PPW = 1.26  # LR materials estimate $/W
CASHFLOW_LR_WARRANTY = 250.00  # LR warranty deduction from 20% final

CASHFLOW_INSTALLED_STAGES = {
    "Energized", "PTO", "Inspection", "Witness Test / PTO"
}

CASHFLOW_CT_GREEN_PPW = 0.25  # $0.25/W cost owed to CT Green Estates at final payment

# Lending statuses that indicate all payments have been received — exclude from cash flow
CASHFLOW_FULLY_PAID_STATUSES = {
    "LR - Activation Package Paid",
    "Cash - paid in full",
    "CF - Phase 2 Funded",
    "SG - PTO Package Paid",
    "SE- Final 1/3 Payment Funded",
}

# LR statuses where the 80% draw has already been received — show only 20% final
CASHFLOW_LR_DRAW_PAID_STATUSES = {
    "LR - Install Package Paid",
    "LR - Activation Package Submitted",
    "Cash - 20PCT deposit paid",
}

CASHFLOW_CASH_PROGRESS_PAID_STATUSES = {
    "Cash - 60PCT paid",
}

CASHFLOW_PIPELINE_STAGES = {
    "Sales Ops Review", "Project Intake", "Site Survey", "Engineering", "Plan Review",
    "Interconnection", "Permitting", "Procurement & Scheduling",
    "Active Installation",
}

# Days remaining to Substantial Completion from each pipeline stage (sequential model)
CASHFLOW_STAGE_DAYS_TO_SC = {
    "Sales Ops Review": 47,
    "Project Intake": 45,
    "Site Survey": 43,
    "Engineering": 40,
    "Plan Review": 38,
    "Interconnection": 38,
    "Permitting": 22,
    "Procurement & Scheduling": 12,
    "Active Installation": 5,
}


def _classify_finance_type(lending_status: str) -> str:
    s = (lending_status or "").strip().upper()
    if s.startswith("LR"):
        return "LR"
    if s.startswith("CASH"):
        return "CASH"
    if s.startswith("CF"):
        return "CF"
    if s in ("SG", "SO") or s.startswith("SG"):
        return "SG"
    if s in ("SE", "SMART E LOAN", "SMART E", "SMART E-LOAN") or s.startswith("SE"):
        return "SE"
    return s or "UNKNOWN"


def _next_monday_on_or_after(d: datetime.date) -> datetime.date:
    """Return d if already Monday, else the next Monday."""
    days = (7 - d.weekday()) % 7
    return d + datetime.timedelta(days=days)


def _fetch_all_cashflow_projects(cutoff_date: str = "2026-01-01") -> list[dict]:
    """
    Pull all Zoho Installs for cash flow pipeline. Filters to projects that
    are installed (have Substantial_Completion OR are in an installed stage),
    created on or after cutoff_date.
    """
    token = get_zoho_access_token()
    if not token:
        return []
    api_domain = os.getenv("ZOHO_API_DOMAIN")
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    fields = (
        "Name,Project_ID,Aurora_Project_ID,Sales_Representative,Owner,"
        "Project_Stage,Project_Created_Date,Substantial_Completion,"
        "Lending_Status,System_kW_DC,Base_Price,Price_Per_Watt,Utility_PTO"
    )
    criteria = f"(Project_Created_Date:greater_equal:{cutoff_date})"
    results = []
    page = 1
    while True:
        url = (
            f"{api_domain}/crm/v7/Installs/search"
            f"?criteria={criteria}&fields={fields}&page={page}&per_page=200"
        )
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            logger.error(
                f"_fetch_all_cashflow_projects: Zoho fetch failed "
                f"status={resp.status_code} body={resp.text[:200]}"
            )
            break
        data = resp.json().get("data") or []
        for r in data:
            aurora_id = (r.get("Aurora_Project_ID") or "").strip()
            if not aurora_id:
                continue
            stage = (r.get("Project_Stage") or "").strip()
            substantial_completion = (r.get("Substantial_Completion") or "").strip()
            is_installed = bool(substantial_completion) or stage in CASHFLOW_INSTALLED_STAGES
            is_pipeline = stage in CASHFLOW_PIPELINE_STAGES
            if not is_installed and not is_pipeline:
                continue
            if stage == "Project Closeout":
                continue
            lending_status = (r.get("Lending_Status") or "").strip()
            # Skip fully paid projects — all payments received, nothing pending
            if lending_status in CASHFLOW_FULLY_PAID_STATUSES:
                continue
            owner_obj = r.get("Owner")
            owner_name = owner_obj.get("name", "") if isinstance(owner_obj, dict) else ""
            results.append({
                "customer": (r.get("Name") or "").strip(),
                "project_id": (r.get("Project_ID") or "").strip(),
                "zoho_record_id": r.get("id") or "",
                "aurora_project_id": aurora_id,
                "rep": (r.get("Sales_Representative") or "").strip(),
                "owner": owner_name,
                "stage": stage,
                "created_date": (r.get("Project_Created_Date") or "").strip(),
                "substantial_completion": substantial_completion,
                "finance_type": _classify_finance_type(lending_status),
                "lending_status": lending_status,
                "system_kw_zoho": float(r.get("System_kW_DC") or 0),
                "base_price_zoho": float(r.get("Base_Price") or 0),
                "price_per_watt_zoho": float(r.get("Price_Per_Watt") or 0),
                "pto_date": (r.get("Utility_PTO") or "").strip(),
            })
        info = resp.json().get("info") or {}
        if not info.get("more_records"):
            break
        page += 1
        if page > 50:
            break
    return results


def _write_cashflow_tab(svc, tab_name: str, rows: list[dict]) -> None:
    """
    Write a Pipeline tab to CASHFLOW_SHEET_ID with per-project payment dates,
    amounts, materials, subcontractor costs, and commission payouts.

    Column layout (A:AA):
      A  Customer
      B  Project ID
      C  Finance Type
      D  Stage
      E  SC / Projected SC        (prefixed with ~ when projected from stage timing)
      F  kW
      G  Rev $/W
      H  Total Revenue            $
      I  Payment 1 Date           LR: 80% draw; Cash: 20% deposit; Loans: full
      J  Payment 1 Amt            $
      K  Payment 2 Date           LR: 20% final; Cash: 60% progress
      L  Payment 2 Amt            $
      M  Payment 3 Date           Cash: 20% final at Energized
      N  Payment 3 Amt            $
      O  Materials (est)          $ (LR only, at $1.26/W)
      P  Subcontractor Cost       $
      Q  Subcontractor Notes
      R  Referral Payout          $
      S  Total Commission         $
      T  Comm Payout 1 Date
      U  Comm Payout 1 Amt        $
      V  Comm Payout 2 Date
      W  Comm Payout 2 Amt        $
      X  Comm Payout 3 Date       (Cash only)
      Y  Comm Payout 3 Amt        $
      Z  Zoho Link
      AA Aurora Link
    """
    sheets = svc.spreadsheets()
    today = datetime.date.today()

    # Delete existing tab with same name
    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break

    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    headers = [
        "Customer", "Project ID", "Finance Type", "Stage",
        "SC / Projected SC", "kW", "Rev $/W", "Total Revenue",
        "Payment 1 Date", "Payment 1 Amt",
        "Payment 2 Date", "Payment 2 Amt",
        "Payment 3 Date", "Payment 3 Amt",
        "Materials (est)", "Subcontractor Cost", "Subcontractor Notes",
        "Referral Payout", "Total Commission",
        "Comm Payout 1 Date", "Comm Payout 1 Amt",
        "Comm Payout 2 Date", "Comm Payout 2 Amt",
        "Comm Payout 3 Date", "Comm Payout 3 Amt",
        "Zoho Link", "Aurora Link",
    ]

    zoho_base = "https://crm.zoho.com/crm/heliosolar/tab/CustomModule6/"
    aurora_base = "https://v2.aurorasolar.com/projects/"

    value_rows = [headers]
    for row in rows:
        zoho_id = row.get("zoho_record_id", "")
        aurora_id = row.get("aurora_project_id", "")
        zoho_link = f'=HYPERLINK("{zoho_base}{zoho_id}","Zoho")' if zoho_id else ""
        aurora_link = f'=HYPERLINK("{aurora_base}{aurora_id}","Aurora")' if aurora_id else ""

        finance_type = row.get("finance_type", "")
        lending_status = row.get("lending_status", "")
        stage = row.get("stage", "")
        sc_date_str = row.get("substantial_completion", "")
        created_date_str = row.get("created_date", "")
        d = row.get("data", {})

        # System size and pricing — prefer Aurora data, fall back to Zoho
        system_watts = d.get("system_size_watts") or int(row.get("system_kw_zoho", 0) * 1000)
        system_kw = round(system_watts / 1000, 3) if system_watts else row.get("system_kw_zoho", 0)
        base_price = d.get("base_price") or row.get("base_price_zoho", 0)
        contract_price = d.get("final_system_price") or base_price
        rev_ppw = (
            round(contract_price / system_watts, 4)
            if system_watts else row.get("price_per_watt_zoho", 0)
        )
        base_ppw = base_price / system_watts if system_watts else 0
        base_commission = max(0, (base_ppw - COMMISSION_PPW_FLOOR) * system_watts) if system_watts else 0
        consultant_comp_ppw = float(d.get("consultant_comp_ppw") or 0)
        total_commission = round(base_commission + consultant_comp_ppw * system_watts, 2)
        referral_flat = float(d.get("referral_flat") or 0)
        subcontractor_total = d.get("subcontractor_total", 0)
        subcontractor_notes = d.get("subcontractor_notes", "")
        materials_est = (
            round(system_watts * CASHFLOW_MATERIALS_PPW, 2)
            if system_watts and finance_type == "LR" else ""
        )
        # Determine effective SC date — actual if available, else project from stage timing
        is_projected_sc = False
        effective_sc_str = sc_date_str
        if not sc_date_str and stage in CASHFLOW_STAGE_DAYS_TO_SC:
            days_to_sc = CASHFLOW_STAGE_DAYS_TO_SC[stage]
            effective_sc_str = (today + datetime.timedelta(days=days_to_sc)).isoformat()
            is_projected_sc = True

        sc_display = f"~{effective_sc_str}" if is_projected_sc else (sc_date_str or "(no SC)")

        payment1_date = payment1_amt = ""
        payment2_date = payment2_amt = ""
        payment3_date = payment3_amt = ""
        comm_payout1_date = comm_payout1_amt = ""
        comm_payout2_date = comm_payout2_amt = ""
        comm_payout3_date = comm_payout3_amt = ""

        if finance_type == "LR" and effective_sc_str:
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                # 80% draw: SC + 14 → next Monday
                draw_date = _next_monday_on_or_after(sc + datetime.timedelta(days=14))
                # 20% final: SC + 33 → next Monday (Inspection 5 + Witness Test 14 + 14)
                final_date = _next_monday_on_or_after(sc + datetime.timedelta(days=33))
                mat = materials_est if isinstance(materials_est, (int, float)) else 0
                draw_amt = round(contract_price * 0.8 - mat, 2)
                final_amt = round(contract_price * 0.2 - CASHFLOW_LR_WARRANTY, 2)
                payment1_date = draw_date.isoformat()
                payment1_amt = draw_amt
                payment2_date = final_date.isoformat()
                payment2_amt = final_amt
                # Commissions: 80% of comp + 100% referral at draw; 20% of comp at final
                comm_payout1_date = draw_date.isoformat()
                comm_payout1_amt = round(total_commission * 0.8 + referral_flat, 2)
                comm_payout2_date = final_date.isoformat()
                comm_payout2_amt = round(total_commission * 0.2, 2)
            except (ValueError, TypeError):
                pass

            # 80% draw already received — clear Payment 1 and Comm Payout 1
            if lending_status in CASHFLOW_LR_DRAW_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""

        elif finance_type == "CASH" and effective_sc_str:
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                # Payment 1: 20% deposit received ~11 days after project creation
                if created_date_str:
                    created = datetime.date.fromisoformat(created_date_str)
                    deposit_received = created + datetime.timedelta(days=11)
                else:
                    deposit_received = today
                # Payment 2: 60% progress at Procurement & Scheduling (SC - 12 days) + 7
                progress_received = sc - datetime.timedelta(days=5)  # SC - 12 + 7
                # Payment 3: 20% final at Energized (SC + 19) + 7 = SC + 26
                final_received = sc + datetime.timedelta(days=26)
                payment1_date = deposit_received.isoformat()
                payment1_amt = round(contract_price * 0.2, 2)
                payment2_date = progress_received.isoformat()
                payment2_amt = round(contract_price * 0.6, 2)
                payment3_date = final_received.isoformat()
                payment3_amt = round(contract_price * 0.2, 2)
                # Commissions proportional; referral at final payment
                comm_payout1_date = deposit_received.isoformat()
                comm_payout1_amt = round(total_commission * 0.2, 2)
                comm_payout2_date = progress_received.isoformat()
                comm_payout2_amt = round(total_commission * 0.6, 2)
                comm_payout3_date = final_received.isoformat()
                comm_payout3_amt = round(total_commission * 0.2 + referral_flat, 2)
            except (ValueError, TypeError):
                pass
            # 20% deposit already collected — clear Payment 1
            if lending_status in CASHFLOW_LR_DRAW_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""
            # 60% progress already collected — clear Payment 1 and Payment 2
            if lending_status in CASHFLOW_CASH_PROGRESS_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""
                payment2_date = payment2_amt = ""
                comm_payout2_date = comm_payout2_amt = ""

        elif effective_sc_str:
            # CF, SG, SE, etc. — single payment at SC
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                payment1_date = sc.isoformat()
                payment1_amt = contract_price
                comm_payout1_date = sc.isoformat()
                comm_payout1_amt = round(total_commission + referral_flat, 2)
            except (ValueError, TypeError):
                pass

        # Apply manual payment date overrides (from Overrides tab)
        pov = row.get("payment_overrides", {})
        if pov.get("payment1"):
            payment1_date = pov["payment1"]
            comm_payout1_date = pov["payment1"]
        if pov.get("payment2"):
            payment2_date = pov["payment2"]
            comm_payout2_date = pov["payment2"]
        if pov.get("payment3"):
            payment3_date = pov["payment3"]
            comm_payout3_date = pov["payment3"]

        notes_col = subcontractor_notes

        value_rows.append([
            row.get("customer", ""),
            row.get("project_id", ""),
            finance_type,
            stage,
            sc_display,
            system_kw,
            rev_ppw,
            contract_price,
            payment1_date,
            payment1_amt,
            payment2_date,
            payment2_amt,
            payment3_date,
            payment3_amt,
            materials_est,
            subcontractor_total if subcontractor_total else "",
            notes_col,
            referral_flat if referral_flat else "",
            total_commission,
            comm_payout1_date,
            comm_payout1_amt,
            comm_payout2_date,
            comm_payout2_amt,
            comm_payout3_date,
            comm_payout3_amt,
            zoho_link,
            aurora_link,
        ])

    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": value_rows},
    ).execute()

    # Currency formatting (0-based col indices):
    # H=7, J=9, L=11, N=13, O=14, P=15, R=17, S=18, U=20, W=22, Y=24
    dollar_fmt = {"numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'}}
    dollar_requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": dollar_fmt},
                "fields": "userEnteredFormat.numberFormat",
            }
        }
        for col in [7, 9, 11, 13, 14, 15, 17, 18, 20, 22, 24]
    ]
    format_requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        *dollar_requests,
    ]
    sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": format_requests}
    ).execute()
    logger.info(f"_write_cashflow_tab: wrote {len(rows)} rows to tab '{tab_name}'")


CASHFLOW_WEEKLY_TAB = "Weekly Payments"
CASHFLOW_OVERRIDES_TAB = "Overrides"


def _ensure_overrides_tab(svc) -> None:
    """Create the Overrides tab with headers if it doesn't already exist."""
    sheets = svc.spreadsheets()
    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == CASHFLOW_OVERRIDES_TAB:
            return  # already exists

    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": CASHFLOW_OVERRIDES_TAB}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{CASHFLOW_OVERRIDES_TAB}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [
            ["Project ID", "Customer", "Payment 1 Date", "Payment 2 Date", "Payment 3 Date", "Notes"],
            ["# Example: PROJ-1234", "", "2026-08-01", "", "", "LR draw delayed — paid week of 8/1"],
        ]},
    ).execute()

    sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
        ]},
    ).execute()
    logger.info("_ensure_overrides_tab: created Overrides tab")


def _read_payment_overrides(svc) -> dict:
    """
    Read the Overrides tab and return a dict keyed by project_id:
      {project_id: {"payment1": "YYYY-MM-DD", "payment2": "YYYY-MM-DD", "payment3": "YYYY-MM-DD"}}
    Only populated keys are included. Rows starting with '#' are skipped.
    Columns: A=Project ID, B=Customer (ignored), C=Payment 1, D=Payment 2, E=Payment 3, F=Notes
    """
    sheets = svc.spreadsheets()
    try:
        data = sheets.values().get(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{CASHFLOW_OVERRIDES_TAB}'!A2:E200",
            valueRenderOption="FORMATTED_VALUE",
        ).execute().get("values", [])
    except Exception:
        return {}

    def valid_date(s):
        try:
            datetime.date.fromisoformat(s.strip())
            return s.strip()
        except (ValueError, AttributeError):
            return None

    overrides = {}
    for row in data:
        if not row:
            continue
        proj_id = row[0].strip() if row else ""
        if not proj_id or proj_id.startswith("#"):
            continue
        entry = {}
        if len(row) > 2 and valid_date(row[2]):
            entry["payment1"] = valid_date(row[2])
        if len(row) > 3 and valid_date(row[3]):
            entry["payment2"] = valid_date(row[3])
        if len(row) > 4 and valid_date(row[4]):
            entry["payment3"] = valid_date(row[4])
        if entry:
            overrides[proj_id] = entry
            logger.info(f"_read_payment_overrides: {proj_id} → {entry}")
    logger.info(f"_read_payment_overrides: loaded {len(overrides)} override(s)")
    return overrides


def _write_weekly_payments_tab(svc, rows: list[dict]) -> None:
    """
    Write (or overwrite) a 'Weekly Payments' tab that shows one row per
    payment event, sorted by payment date, so it's easy to see what's
    expected each week.

    Columns:
      A  Week Of          (Monday of the payment week, YYYY-MM-DD)
      B  Payment Date
      C  Customer
      D  Finance Type
      E  Payment Type     (e.g. "LR 80% Draw", "LR 20% Final", "Cash Progress 60%", …)
      F  Amount
      G  Commission Date
      H  Commission Amt
      I  Stage
      J  SC / Projected SC
      K  Project ID
      L  Zoho Link
    """
    sheets = svc.spreadsheets()
    today = datetime.date.today()

    tab_name = CASHFLOW_WEEKLY_TAB

    # Delete and recreate tab
    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break

    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    zoho_base = "https://crm.zoho.com/crm/heliosolar/tab/CustomModule6/"

    def week_of(date_str):
        """Return the Monday of the week containing date_str."""
        try:
            d = datetime.date.fromisoformat(date_str)
            return (d - datetime.timedelta(days=d.weekday())).isoformat()
        except (ValueError, TypeError):
            return ""

    event_rows = []

    for row in rows:
        finance_type = row.get("finance_type", "")
        lending_status = row.get("lending_status", "")
        stage = row.get("stage", "")
        sc_date_str = row.get("substantial_completion", "")
        created_date_str = row.get("created_date", "")
        d = row.get("data", {})
        customer = row.get("customer", "")
        project_id = row.get("project_id", "")
        zoho_id = row.get("zoho_record_id", "")
        zoho_link = f'=HYPERLINK("{zoho_base}{zoho_id}","Zoho")' if zoho_id else ""

        system_watts = d.get("system_size_watts") or int(row.get("system_kw_zoho", 0) * 1000)
        base_price = d.get("base_price") or row.get("base_price_zoho", 0)
        contract_price = d.get("final_system_price") or base_price
        base_ppw = base_price / system_watts if system_watts else 0
        base_commission = max(0, (base_ppw - COMMISSION_PPW_FLOOR) * system_watts) if system_watts else 0
        consultant_comp_ppw = float(d.get("consultant_comp_ppw") or 0)
        total_commission = round(base_commission + consultant_comp_ppw * system_watts, 2)
        referral_flat = float(d.get("referral_flat") or 0)
        materials_est = round(system_watts * CASHFLOW_MATERIALS_PPW, 2) if system_watts and finance_type == "LR" else 0

        is_projected_sc = False
        effective_sc_str = sc_date_str
        if not sc_date_str and stage in CASHFLOW_STAGE_DAYS_TO_SC:
            days_to_sc = CASHFLOW_STAGE_DAYS_TO_SC[stage]
            effective_sc_str = (today + datetime.timedelta(days=days_to_sc)).isoformat()
            is_projected_sc = True

        sc_display = f"~{effective_sc_str}" if is_projected_sc else (sc_date_str or "(no SC)")

        def add_event(pay_date, pay_type, pay_amt, comm_date="", comm_amt=""):
            if not pay_date:
                return
            event_rows.append([
                week_of(pay_date),
                pay_date,
                customer,
                finance_type,
                pay_type,
                pay_amt,
                comm_date,
                comm_amt,
                stage,
                sc_display,
                project_id,
                zoho_link,
            ])

        pov = row.get("payment_overrides", {})

        if finance_type == "LR" and effective_sc_str:
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                draw_date_str = pov.get("payment1") or _next_monday_on_or_after(sc + datetime.timedelta(days=14)).isoformat()
                final_date_str = pov.get("payment2") or _next_monday_on_or_after(sc + datetime.timedelta(days=33)).isoformat()
                mat = materials_est if isinstance(materials_est, (int, float)) else 0
                draw_amt = round(contract_price * 0.8 - mat, 2)
                final_amt = round(contract_price * 0.2 - CASHFLOW_LR_WARRANTY, 2)
                comm1_amt = round(total_commission * 0.8 + referral_flat, 2)
                comm2_amt = round(total_commission * 0.2, 2)

                if lending_status not in CASHFLOW_LR_DRAW_PAID_STATUSES:
                    add_event(draw_date_str, "LR 80% Draw", draw_amt, draw_date_str, comm1_amt)
                add_event(final_date_str, "LR 20% Final", final_amt, final_date_str, comm2_amt)
            except (ValueError, TypeError):
                pass

        elif finance_type == "CASH" and effective_sc_str:
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                if created_date_str:
                    created = datetime.date.fromisoformat(created_date_str)
                    deposit_date_str = pov.get("payment1") or (created + datetime.timedelta(days=11)).isoformat()
                else:
                    deposit_date_str = pov.get("payment1") or today.isoformat()
                progress_date_str = pov.get("payment2") or (sc - datetime.timedelta(days=5)).isoformat()
                final_date_str = pov.get("payment3") or (sc + datetime.timedelta(days=26)).isoformat()

                if lending_status not in CASHFLOW_LR_DRAW_PAID_STATUSES and lending_status not in CASHFLOW_CASH_PROGRESS_PAID_STATUSES:
                    add_event(deposit_date_str, "Cash 20% Deposit",
                              round(contract_price * 0.2, 2),
                              deposit_date_str, round(total_commission * 0.2, 2))
                if lending_status not in CASHFLOW_CASH_PROGRESS_PAID_STATUSES:
                    add_event(progress_date_str, "Cash 60% Progress",
                              round(contract_price * 0.6, 2),
                              progress_date_str, round(total_commission * 0.6, 2))
                add_event(final_date_str, "Cash 20% Final",
                          round(contract_price * 0.2, 2),
                          final_date_str, round(total_commission * 0.2 + referral_flat, 2))
            except (ValueError, TypeError):
                pass

        elif effective_sc_str:
            try:
                sc = datetime.date.fromisoformat(effective_sc_str)
                pay_date_str = pov.get("payment1") or sc.isoformat()
                add_event(pay_date_str, "Loan / Full Payment", contract_price,
                          pay_date_str, round(total_commission + referral_flat, 2))
            except (ValueError, TypeError):
                pass

    # Sort by payment date
    event_rows.sort(key=lambda r: r[1] if r[1] else "9999")

    headers = [
        "Week Of", "Payment Date", "Customer", "Finance Type", "Payment Type",
        "Amount", "Commission Date", "Commission Amt",
        "Stage", "SC / Projected SC", "Project ID", "Zoho Link",
    ]
    value_rows = [headers] + event_rows

    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": value_rows},
    ).execute()

    dollar_cols = [5, 7]  # 0-indexed: F=5, H=7
    dollar_fmt = {"numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'}}
    format_requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        *[
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": col,
                        "endColumnIndex": col + 1,
                    },
                    "cell": {"userEnteredFormat": dollar_fmt},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
            for col in dollar_cols
        ],
    ]
    sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": format_requests}
    ).execute()
    logger.info(f"_write_weekly_payments_tab: wrote {len(event_rows)} payment events")


def _compute_cashflow_row(row: dict, today: datetime.date, zoho_base: str, aurora_base: str):
    """
    Compute the Pipeline tab row values and weekly payment events for a single
    project row (with 'data' key containing Aurora fields and optional
    'payment_overrides' key).

    Returns (pipeline_row_list, pay_events_list).
    pay_events_list entries: [pay_date, customer, finance_type, pay_type,
                               pay_amt, comm_date, comm_amt, stage, sc_display,
                               project_id, zoho_link]  (11 fields, week_of prepended by caller)
    """
    zoho_id = row.get("zoho_record_id", "")
    aurora_id = row.get("aurora_project_id", "")
    zoho_link = f'=HYPERLINK("{zoho_base}{zoho_id}","Zoho")' if zoho_id else ""
    aurora_link = f'=HYPERLINK("{aurora_base}{aurora_id}","Aurora")' if aurora_id else ""

    finance_type = row.get("finance_type", "")
    lending_status = row.get("lending_status", "")
    stage = row.get("stage", "")
    sc_date_str = row.get("substantial_completion", "")
    created_date_str = row.get("created_date", "")
    d = row.get("data", {})
    pov = row.get("payment_overrides", {})
    customer = row.get("customer", "")
    project_id = row.get("project_id", "")

    system_watts = d.get("system_size_watts") or int(row.get("system_kw_zoho", 0) * 1000)
    system_kw = round(system_watts / 1000, 3) if system_watts else row.get("system_kw_zoho", 0)
    base_price = d.get("base_price") or row.get("base_price_zoho", 0)
    # Use final system price (base + adders - discounts) for payment amounts; fall back to base_price
    contract_price = d.get("final_system_price") or base_price
    rev_ppw = round(contract_price / system_watts, 4) if system_watts else row.get("price_per_watt_zoho", 0)
    base_ppw = base_price / system_watts if system_watts else 0
    base_commission = max(0, (base_ppw - COMMISSION_PPW_FLOOR) * system_watts) if system_watts else 0
    consultant_comp_ppw = float(d.get("consultant_comp_ppw") or 0)
    total_commission = round(base_commission + consultant_comp_ppw * system_watts, 2)
    referral_flat = float(d.get("referral_flat") or 0)
    subcontractor_total = d.get("subcontractor_total", 0)
    subcontractor_notes = d.get("subcontractor_notes", "")
    materials_est = round(system_watts * CASHFLOW_MATERIALS_PPW, 2) if system_watts and finance_type == "LR" else ""

    is_projected_sc = False
    effective_sc_str = sc_date_str
    if not sc_date_str and stage in CASHFLOW_STAGE_DAYS_TO_SC:
        effective_sc_str = (today + datetime.timedelta(days=CASHFLOW_STAGE_DAYS_TO_SC[stage])).isoformat()
        is_projected_sc = True
    sc_display = f"~{effective_sc_str}" if is_projected_sc else (sc_date_str or "(no SC)")

    payment1_date = payment1_amt = ""
    payment2_date = payment2_amt = ""
    payment3_date = payment3_amt = ""
    comm_payout1_date = comm_payout1_amt = ""
    comm_payout2_date = comm_payout2_amt = ""
    comm_payout3_date = comm_payout3_amt = ""

    if finance_type == "LR" and effective_sc_str:
        try:
            sc = datetime.date.fromisoformat(effective_sc_str)
            mat = materials_est if isinstance(materials_est, (int, float)) else 0
            payment1_date = _next_monday_on_or_after(sc + datetime.timedelta(days=14)).isoformat()
            payment1_amt = round(contract_price * 0.8 - mat, 2)
            payment2_date = _next_monday_on_or_after(sc + datetime.timedelta(days=33)).isoformat()
            payment2_amt = round(contract_price * 0.2 - CASHFLOW_LR_WARRANTY, 2)
            comm_payout1_date = payment1_date
            comm_payout1_amt = round(total_commission * 0.8 + referral_flat, 2)
            comm_payout2_date = payment2_date
            comm_payout2_amt = round(total_commission * 0.2, 2)
            if lending_status in CASHFLOW_LR_DRAW_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""
        except (ValueError, TypeError):
            pass

    elif finance_type == "CASH" and effective_sc_str:
        try:
            sc = datetime.date.fromisoformat(effective_sc_str)
            if created_date_str:
                created = datetime.date.fromisoformat(created_date_str)
                payment1_date = (created + datetime.timedelta(days=11)).isoformat()
            else:
                payment1_date = today.isoformat()
            payment2_date = (sc - datetime.timedelta(days=5)).isoformat()
            payment3_date = (sc + datetime.timedelta(days=26)).isoformat()
            payment1_amt = round(contract_price * 0.2, 2)
            payment2_amt = round(contract_price * 0.6, 2)
            payment3_amt = round(contract_price * 0.2, 2)
            comm_payout1_date = payment1_date
            comm_payout1_amt = round(total_commission * 0.2, 2)
            comm_payout2_date = payment2_date
            comm_payout2_amt = round(total_commission * 0.6, 2)
            comm_payout3_date = payment3_date
            comm_payout3_amt = round(total_commission * 0.2 + referral_flat, 2)
            # 20% deposit already collected — clear Payment 1
            if lending_status in CASHFLOW_LR_DRAW_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""
            # 60% progress already collected — clear Payment 1 and Payment 2
            if lending_status in CASHFLOW_CASH_PROGRESS_PAID_STATUSES:
                payment1_date = payment1_amt = ""
                comm_payout1_date = comm_payout1_amt = ""
                payment2_date = payment2_amt = ""
                comm_payout2_date = comm_payout2_amt = ""
        except (ValueError, TypeError):
            pass

    elif finance_type == "SE" and effective_sc_str:
        try:
            sc = datetime.date.fromisoformat(effective_sc_str)
            payment1_date = _next_monday_on_or_after(sc + datetime.timedelta(days=14)).isoformat()
            payment1_amt = round(contract_price * 0.33, 2)
            payment2_date = _next_monday_on_or_after(sc + datetime.timedelta(days=33)).isoformat()
            payment2_amt = round(contract_price * 0.33, 2)
            comm_payout1_date = payment1_date
            comm_payout1_amt = round(total_commission * 0.33 + referral_flat, 2)
            comm_payout2_date = payment2_date
            comm_payout2_amt = round(total_commission * 0.33, 2)
            # Payment 3: PTO + 14 days → next Monday; fall back to SC + 60 if no PTO
            pto_date_str = row.get("pto_date", "")
            if pto_date_str:
                pto = datetime.date.fromisoformat(pto_date_str)
                payment3_date = _next_monday_on_or_after(pto + datetime.timedelta(days=14)).isoformat()
            else:
                payment3_date = _next_monday_on_or_after(sc + datetime.timedelta(days=60)).isoformat()
            payment3_amt = round(contract_price * 0.34, 2)
            comm_payout3_date = payment3_date
            comm_payout3_amt = round(total_commission * 0.34, 2)
        except (ValueError, TypeError):
            pass

    elif effective_sc_str:
        try:
            sc = datetime.date.fromisoformat(effective_sc_str)
            payment1_date = sc.isoformat()
            payment1_amt = contract_price
            comm_payout1_date = sc.isoformat()
            comm_payout1_amt = round(total_commission + referral_flat, 2)
        except (ValueError, TypeError):
            pass

    # Apply manual payment date overrides
    if pov.get("payment1"):
        payment1_date = pov["payment1"]
        comm_payout1_date = pov["payment1"]
    if pov.get("payment2"):
        payment2_date = pov["payment2"]
        comm_payout2_date = pov["payment2"]
    if pov.get("payment3"):
        payment3_date = pov["payment3"]
        comm_payout3_date = pov["payment3"]

    # Cash materials cost: $1.26/W at 60% progress (Cash) or Payment 2 (SE) date
    cash_materials_date = cash_materials_amt = ""
    if finance_type in ("CASH", "SE") and system_watts and payment2_date:
        cash_materials_date = payment2_date
        cash_materials_amt = round(system_watts * CASHFLOW_MATERIALS_PPW, 2)

    # CT Green Estates cost: $0.25/W, due at final payment, only for pre-install jobs
    ct_green_date = ct_green_amt = ""
    is_pre_install = stage in CASHFLOW_PIPELINE_STAGES
    if is_pre_install and system_watts:
        # Final payment date by finance type
        if finance_type == "LR":
            final_date_for_ct = payment2_date
        elif finance_type in ("CASH", "SE"):
            final_date_for_ct = payment3_date
        else:
            final_date_for_ct = payment1_date
        if final_date_for_ct:
            ct_green_date = final_date_for_ct
            ct_green_amt = round(system_watts * CASHFLOW_CT_GREEN_PPW, 2)

    pipeline_row = [
        customer, project_id, finance_type, stage, sc_display,
        system_kw, rev_ppw, contract_price,
        payment1_date, payment1_amt,
        payment2_date, payment2_amt,
        payment3_date, payment3_amt,
        materials_est, subcontractor_total if subcontractor_total else "", subcontractor_notes,
        referral_flat if referral_flat else "", total_commission,
        comm_payout1_date, comm_payout1_amt,
        comm_payout2_date, comm_payout2_amt,
        comm_payout3_date, comm_payout3_amt,
        zoho_link, aurora_link,
        ct_green_date, ct_green_amt,
        cash_materials_date, cash_materials_amt,
    ]

    PAYMENT_TYPE_MAP = {
        ("LR",   0): "LR 80% Draw",
        ("LR",   1): "LR 20% Final",
        ("CASH", 0): "Cash 20% Deposit",
        ("CASH", 1): "Cash 60% Progress",
        ("CASH", 2): "Cash 20% Final",
        ("SE",   0): "SE Payment 1 (33%)",
        ("SE",   1): "SE Payment 2 (33%)",
        ("SE",   2): "SE Payment 3 (34%)",
    }
    pay_slots = [
        (payment1_date, payment1_amt, comm_payout1_date, comm_payout1_amt),
        (payment2_date, payment2_amt, comm_payout2_date, comm_payout2_amt),
        (payment3_date, payment3_amt, comm_payout3_date, comm_payout3_amt),
    ]
    pay_events = []
    for i, (pd, pa, cd, ca) in enumerate(pay_slots):
        if not pd or not pa:
            continue
        pay_type = PAYMENT_TYPE_MAP.get((finance_type, i), "Loan / Full Payment")
        pay_events.append([pd, customer, finance_type, pay_type, pa, cd, ca, stage, sc_display, project_id, zoho_link])

    if ct_green_date and ct_green_amt:
        pay_events.append([ct_green_date, customer, finance_type, "CT Green Estates", ct_green_amt,
                           "", "", stage, sc_display, project_id, zoho_link])

    if cash_materials_date and cash_materials_amt:
        pay_events.append([cash_materials_date, customer, finance_type, "Cash Materials", cash_materials_amt,
                           "", "", stage, sc_display, project_id, zoho_link])

    return pipeline_row, pay_events


def _write_weekly_payments_from_events(svc, weekly_events: list) -> None:
    """Write the Weekly Payments tab from pre-sorted event rows."""
    sheets = svc.spreadsheets()
    tab_name = CASHFLOW_WEEKLY_TAB

    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    weekly_sheet_id = None
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break

    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    ).execute()
    weekly_sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    headers = [
        "Week Of", "Payment Date", "Customer", "Finance Type", "Payment Type",
        "Amount", "Commission Date", "Commission Amt",
        "Stage", "SC / Projected SC", "Project ID", "Zoho Link",
    ]
    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [headers] + weekly_events},
    ).execute()

    dollar_fmt = {"numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'}}
    sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": weekly_sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                    "fields": "userEnteredFormat.textFormat.bold",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": weekly_sheet_id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            *[
                {
                    "repeatCell": {
                        "range": {"sheetId": weekly_sheet_id, "startRowIndex": 1,
                                  "startColumnIndex": col, "endColumnIndex": col + 1},
                        "cell": {"userEnteredFormat": dollar_fmt},
                        "fields": "userEnteredFormat.numberFormat",
                    }
                }
                for col in [5, 7]
            ],
        ]},
    ).execute()
    logger.info(f"_write_weekly_payments_from_events: wrote {len(weekly_events)} events")


CASHFLOW_README_TAB = "README"
CASHFLOW_SUMMARY_TAB = "Summary"


def _write_readme_tab(svc) -> None:
    """Overwrite column A of the README tab with current documentation.
    Columns B+ are left untouched so existing buttons are preserved."""
    readme_rows = [
        ["Helio Cash Flow Model — README"],
        [""],
        ["TABS:"],
        ["  • Pipeline [date] — auto-generated from Zoho + Aurora on each sync. One row per active project."],
        ["  • Cash Flow — 16-week weekly forecast. Pulls automatically from the latest Pipeline tab."],
        ["  • Weekly Payments — chronological list of all payment events (draws, finals, commissions, materials, CT Green)."],
        ["  • Overrides — manually override payment dates for specific projects by Project ID (YYYY-MM-DD format)."],
        ["  • Summary — totals and % of revenue for revenue, commissions, subs, materials, referral, and net."],
        [""],
        ["RUNNING A REFRESH:"],
        ["  • Click 'Run Cash Flow' (blue button) to pull fresh data from Zoho and Aurora, create a new dated Pipeline tab,"],
        ["    rebuild Weekly Payments and Summary, and update Cash Flow formulas. Takes ~2-3 minutes."],
        ["  • Click 'Run Overrides' (yellow button) to apply manual date overrides from the Overrides tab without"],
        ["    re-fetching all data. Use this for quick date tweaks between full syncs."],
        ["  • Note: Overrides are applied automatically during every full Cash Flow run — 'Run Overrides' is only"],
        ["    needed when you want to update dates without waiting for a full sync."],
        [""],
        ["PAYMENT LOGIC:"],
        ["  LR (Lightreach):"],
        ["    • 80% draw ~14 days post-SC (rounded to next Monday)"],
        ["      = Contract Price × 80% − (kW × 1000 × $1.26) for materials"],
        ["    • 20% final ~33 days post-SC (rounded to next Monday)"],
        ["      = Contract Price × 20% − $250 inverter warranty"],
        ["    • Commissions: 80% paid with 80% draw, 20% paid with 20% final"],
        [""],
        ["  CASH:"],
        ["    • 20% deposit ~11 days after contract date (rounded to next Monday)"],
        ["    • 60% progress ~5 days before SC (rounded to next Monday)"],
        ["    • 20% final ~26 days post-SC (rounded to next Monday)"],
        ["    • Commissions: proportional — 20% / 60% / 20%"],
        ["    • Zoho 'Cash - 20PCT deposit paid' → removes deposit from forecast"],
        ["    • Zoho 'Cash - 60PCT paid' → removes both deposit and progress from forecast"],
        [""],
        ["  SE (Smart E-Loan):"],
        ["    • Payment 1 (33%) ~14 days post-SC (rounded to next Monday)"],
        ["    • Payment 2 (33%) ~33 days post-SC (rounded to next Monday)"],
        ["    • Payment 3 (34%) ~14 days post-PTO, or SC+60 days if no PTO date yet"],
        ["    • Commissions: proportional — 33% / 33% / 34%"],
        [""],
        ["COSTS:"],
        ["  • LR Materials: $1.26/W deducted directly from the 80% draw amount"],
        ["  • Cash/SE Materials: $1.26/W cash outflow at the 60% progress / Payment 2 date"],
        ["  • CT Green Estates: $0.25/W cash outflow at final payment date (pre-install projects only)"],
        ["  • Subcontractor costs: pulled from Aurora project notes per project"],
        [""],
        ["PRICING:"],
        ["  • Payment amounts calculated on Final System Price (base + adders − discounts) from Aurora."],
        ["  • Commissions calculated on Base Price only, above the $2.50/W floor."],
        ["    Formula: max(0, (base_ppw − $2.50) × system_watts) + consultant $/W × system_watts"],
        [""],
        ["OVERRIDES TAB:"],
        ["  • Add a row: Project ID | Payment 1 Date | Payment 2 Date | Payment 3 Date | Notes"],
        ["  • Dates must be in YYYY-MM-DD format."],
        ["  • Overrides are applied automatically on every full sync — no separate step needed."],
        ["  • To remove an override, delete the row from the Overrides tab and re-run."],
    ]

    svc.spreadsheets().values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{CASHFLOW_README_TAB}'!A1:A{len(readme_rows)}",
        valueInputOption="RAW",
        body={"values": readme_rows},
    ).execute()
    logger.info("_write_readme_tab: README tab updated")


def _write_summary_tab(svc, pipeline_tab_name: str) -> None:
    """Create/replace a Summary tab with totals pulled from the Pipeline tab."""
    sheets = svc.spreadsheets()
    p = pipeline_tab_name

    # Delete existing Summary tab if present
    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == CASHFLOW_SUMMARY_TAB:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break

    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": CASHFLOW_SUMMARY_TAB, "gridProperties": {"columnCount": 3, "rowCount": 20}}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    # Formulas — Pipeline columns:
    #   H = Total Revenue, S = Total Commission, P = Subcontractor Cost
    #   O = Materials est (LR), AE = Cash Materials (CASH/SE), R = Referral Payout
    # Column C = % of revenue (blank for Revenue and Net Revenue rows)
    rows = [
        ["Helio Pipeline Summary", "", "", ""],
        ["", "", "", ""],
        ["Metric", "Amount", "% of Revenue", "Notes"],
        ["Total Revenue",       f"=SUMIF('{p}'!C2:C,\"<>\",'{p}'!H2:H)",  "",          "Contract price (base + adders - discounts)"],
        ["Total Commissions",   f"=SUMIF('{p}'!C2:C,\"<>\",'{p}'!S2:S)",  "=B5/B4",   "Rep + consultant commissions"],
        ["Total Subcontractor", f"=SUMIF('{p}'!C2:C,\"<>\",'{p}'!P2:P)",  "=B6/B4",   "Subcontractor payments"],
        ["Total Materials",     f"=SUMIF('{p}'!C2:C,\"<>\",'{p}'!O2:O)+SUMIF('{p}'!C2:C,\"<>\",'{p}'!AE2:AE)", "=B7/B4", "LR materials est + Cash/SE materials"],
        ["Total Referral",      f"=SUMIF('{p}'!C2:C,\"<>\",'{p}'!R2:R)",  "=B8/B4",   "Referral payouts"],
        ["", "", "", ""],
        ["Net Revenue",         f"=B4-B5-B6-B7-B8",                       "=B10/B4",  "Revenue minus all costs above"],
        ["", "", "", ""],
        ["Project Count",       f"=COUNTA('{p}'!A2:A)",                    "",         "Active pipeline projects"],
    ]

    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{CASHFLOW_SUMMARY_TAB}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()

    # Formatting
    format_requests = [
        # Title row bold + large
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 14}}},
            "fields": "userEnteredFormat.textFormat"}},
        # Header row bold + background
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 0, "endColumnIndex": 4},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True},
                     "backgroundColor": {"red": 0.22, "green": 0.46, "blue": 0.64}}},
            "fields": "userEnteredFormat.textFormat,userEnteredFormat.backgroundColor"}},
        # Currency format for column B (rows 4-12)
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 12, "startColumnIndex": 1, "endColumnIndex": 2},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}}},
            "fields": "userEnteredFormat.numberFormat"}},
        # Percentage format for column C (rows 4-12)
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 12, "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
            "fields": "userEnteredFormat.numberFormat"}},
        # Net Revenue bold
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 9, "endRowIndex": 10, "startColumnIndex": 0, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat"}},
        # Column widths
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 200}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 150}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 3},
            "properties": {"pixelSize": 120}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 3, "endIndex": 4},
            "properties": {"pixelSize": 320}, "fields": "pixelSize"}},
    ]
    sheets.batchUpdate(spreadsheetId=CASHFLOW_SHEET_ID, body={"requests": format_requests}).execute()
    logger.info("_write_summary_tab: Summary tab written")


def _run_cashflow_batch(projects: list[dict], tab_name: str) -> dict:
    """
    Fetch Aurora data one project at a time and stream rows directly to the
    Pipeline tab, keeping memory usage flat regardless of project count.
    Weekly payment events are accumulated as minimal 12-field tuples only.
    """
    import gc
    svc = _build_sheets_service()
    if not svc:
        return {"status": "failed", "reason": "could not build Sheets service"}

    _ensure_overrides_tab(svc)
    overrides = _read_payment_overrides(svc)

    # --- Set up Pipeline tab (headers + formatting shell) ---
    sheets = svc.spreadsheets()
    existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    for s in existing.get("sheets", []):
        if s["properties"]["title"] == tab_name:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"deleteSheet": {"sheetId": s["properties"]["sheetId"]}}]}
            ).execute()
            break
    resp = sheets.batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name, "gridProperties": {"columnCount": 32}}}}]}
    ).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

    pipeline_headers = [
        "Customer", "Project ID", "Finance Type", "Stage",
        "SC / Projected SC", "kW", "Rev $/W", "Total Revenue",
        "Payment 1 Date", "Payment 1 Amt",
        "Payment 2 Date", "Payment 2 Amt",
        "Payment 3 Date", "Payment 3 Amt",
        "Materials (est)", "Subcontractor Cost", "Subcontractor Notes",
        "Referral Payout", "Total Commission",
        "Comm Payout 1 Date", "Comm Payout 1 Amt",
        "Comm Payout 2 Date", "Comm Payout 2 Amt",
        "Comm Payout 3 Date", "Comm Payout 3 Amt",
        "Zoho Link", "Aurora Link",
        "CT Green Date", "CT Green Amt",
        "Cash Materials Date", "Cash Materials Amt",
    ]
    sheets.values().update(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{tab_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [pipeline_headers]},
    ).execute()

    zoho_base = "https://crm.zoho.com/crm/heliosolar/tab/CustomModule6/"
    aurora_base = "https://v2.aurorasolar.com/projects/"
    today = datetime.date.today()

    def week_of(date_str):
        try:
            d = datetime.date.fromisoformat(date_str)
            return (d - datetime.timedelta(days=d.weekday())).isoformat()
        except (ValueError, TypeError):
            return ""

    total = 0
    overrides_applied = 0
    weekly_events = []  # list of 12-field lists — small footprint

    for p in projects:
        logger.info(f"cashflow_batch: fetching {p['aurora_project_id']} ({p['customer']})")
        aurora_data = _get_commission_data_for_project(p["aurora_project_id"])
        if "error" in aurora_data:
            logger.info(f"cashflow_batch: skipping {p['customer']} — {aurora_data['error']}")
            gc.collect()
            continue

        proj_id = p.get("project_id", "")
        pov = overrides.get(proj_id, {})
        if pov:
            overrides_applied += 1

        # Combine project + aurora data into one temp row dict, process it,
        # then discard — never accumulate into a list of full rows
        row = {**p, "data": aurora_data, "payment_overrides": pov}
        pipeline_row, pay_events = _compute_cashflow_row(row, today, zoho_base, aurora_base)
        del row, aurora_data
        gc.collect()

        # Append pipeline row immediately
        sheets.values().append(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{tab_name}'!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [pipeline_row]},
        ).execute()

        # Accumulate only minimal weekly event tuples
        for evt in pay_events:
            weekly_events.append([week_of(evt[0])] + evt)

        total += 1

    # Apply Pipeline tab formatting
    dollar_fmt = {"numberFormat": {"type": "CURRENCY", "pattern": '"$"#,##0.00'}}
    format_requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        *[
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1,
                              "startColumnIndex": col, "endColumnIndex": col + 1},
                    "cell": {"userEnteredFormat": dollar_fmt},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
            for col in [7, 9, 11, 13, 14, 15, 17, 18, 20, 22, 24, 28, 30]
        ],
    ]
    sheets.batchUpdate(spreadsheetId=CASHFLOW_SHEET_ID, body={"requests": format_requests}).execute()

    # Write Weekly Payments tab
    weekly_events.sort(key=lambda r: r[1] if r[1] else "9999")
    _write_weekly_payments_from_events(svc, weekly_events)

    _write_summary_tab(svc, tab_name)
    _write_readme_tab(svc)

    formula_result = _update_cashflow_formulas(svc, tab_name)
    return {
        "status": "ok",
        "tab": tab_name,
        "total": total,
        "overrides_applied": overrides_applied,
        "formulas": formula_result,
    }


CASHFLOW_MAIN_TAB = "Cash Flow"


def _col_letter(idx):
    if idx < 26:
        return chr(65 + idx)
    return chr(65 + idx // 26 - 1) + chr(65 + idx % 26)


def _update_cashflow_formulas(svc, pipeline_tab_name: str) -> dict:
    """
    Rewrite the SUMPRODUCT formulas in the 'Cash Flow' tab to pull from
    the given pipeline_tab_name instead of the old Jobs tab.

    Rows updated (found by label in column A):
      • LR 80% Draws / Cash 60% Pre-Install  → Payment 1 (all) + Cash Payment 2 (60%)
      • LR 20% Finals / Cash 20% Finals       → LR Payment 2 + Cash Payment 3
      • Commissions (Payout 1)                → Comm Payout 1
      • Commissions (Payout 2)                → Comm Payout 2 + Comm Payout 3
    """
    sheets = svc.spreadsheets()

    # Read column A (up to row 60) to locate label rows
    col_a_vals = sheets.values().get(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{CASHFLOW_MAIN_TAB}'!A1:A70",
        valueRenderOption="FORMATTED_VALUE",
    ).execute().get("values", [])
    col_a = [r[0].strip() if r else "" for r in col_a_vals]

    def find_row(fragment):
        for i, v in enumerate(col_a):
            if fragment.lower() in v.lower():
                return i + 1  # 1-indexed sheet row
        return None

    row_draws        = find_row("LR 80% Draws")
    row_finals       = find_row("LR 20% Finals")
    row_comm1        = find_row("Commissions (Payout 1)")
    row_comm2        = find_row("Commissions (Payout 2)")
    row_ct_green     = find_row("CT Green Estates")
    row_cash_mat     = find_row("Materials (Cash Deals)")
    row_sub_ref      = find_row("Subcontractor + Referral")

    missing = [k for k, v in {"lr_draws": row_draws, "lr_finals": row_finals,
                               "comm1": row_comm1, "comm2": row_comm2}.items() if not v]
    if missing:
        return {"error": f"Could not find rows: {missing}"}

    # Read row 2 to discover week-date columns (D onward)
    row2 = sheets.values().get(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{CASHFLOW_MAIN_TAB}'!2:2",
        valueRenderOption="FORMATTED_VALUE",
    ).execute().get("values", [[]])[0]

    start_col = 3  # 0-indexed — column D
    end_col = start_col
    for i in range(start_col, len(row2)):
        if row2[i]:
            end_col = i
    num_weeks = end_col - start_col + 1

    # Read existing CT Green row values — skip any column that already has a value
    ct_green_existing = set()
    if row_ct_green:
        ct_row_data = sheets.values().get(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{CASHFLOW_MAIN_TAB}'!{row_ct_green}:{row_ct_green}",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [[]])[0]
        for i, val in enumerate(ct_row_data):
            # Only protect cells with actual numeric values (e.g. the $8,500)
            # Error cells (#REF!, etc.) return empty with UNFORMATTED_VALUE
            try:
                if val and float(val) != 0 and i >= start_col:
                    ct_green_existing.add(i)
            except (ValueError, TypeError):
                pass

    p = f"'{pipeline_tab_name}'"
    updates = []

    for w in range(num_weeks):
        c = _col_letter(start_col + w)

        # Payment 1 (LR 80% draw / SE 33% / loan) + Cash Payment 2 (60% progress)
        f_draws = (
            f"=SUMPRODUCT(ISNUMBER({p}!$I$2:$I$200)*({p}!$I$2:$I$200>={c}$2)*({p}!$I$2:$I$200<{c}$2+7)*ISNUMBER({p}!$J$2:$J$200)*({p}!$J$2:$J$200))"
            f"+SUMPRODUCT(ISNUMBER({p}!$K$2:$K$200)*({p}!$K$2:$K$200>={c}$2)*({p}!$K$2:$K$200<{c}$2+7)*(LEFT({p}!$C$2:$C$200,4)=\"CASH\")*ISNUMBER({p}!$L$2:$L$200)*({p}!$L$2:$L$200))"
        )
        # LR 20% final + Cash 20% final + SE Payment 2 (33%) + SE Payment 3 (34%)
        f_finals = (
            f"=SUMPRODUCT(ISNUMBER({p}!$K$2:$K$200)*({p}!$K$2:$K$200>={c}$2)*({p}!$K$2:$K$200<{c}$2+7)*({p}!$C$2:$C$200=\"LR\")*ISNUMBER({p}!$L$2:$L$200)*({p}!$L$2:$L$200))"
            f"+SUMPRODUCT(ISNUMBER({p}!$M$2:$M$200)*({p}!$M$2:$M$200>={c}$2)*({p}!$M$2:$M$200<{c}$2+7)*(LEFT({p}!$C$2:$C$200,4)=\"CASH\")*ISNUMBER({p}!$N$2:$N$200)*({p}!$N$2:$N$200))"
            f"+SUMPRODUCT(ISNUMBER({p}!$K$2:$K$200)*({p}!$K$2:$K$200>={c}$2)*({p}!$K$2:$K$200<{c}$2+7)*({p}!$C$2:$C$200=\"SE\")*ISNUMBER({p}!$L$2:$L$200)*({p}!$L$2:$L$200))"
            f"+SUMPRODUCT(ISNUMBER({p}!$M$2:$M$200)*({p}!$M$2:$M$200>={c}$2)*({p}!$M$2:$M$200<{c}$2+7)*({p}!$C$2:$C$200=\"SE\")*ISNUMBER({p}!$N$2:$N$200)*({p}!$N$2:$N$200))"
        )
        # Comm Payout 1
        f_comm1 = (
            f"=SUMPRODUCT(ISNUMBER({p}!$T$2:$T$200)*({p}!$T$2:$T$200>={c}$2)*({p}!$T$2:$T$200<{c}$2+7)*ISNUMBER({p}!$U$2:$U$200)*({p}!$U$2:$U$200))"
        )
        # Comm Payout 2 + Comm Payout 3
        f_comm2 = (
            f"=SUMPRODUCT(ISNUMBER({p}!$V$2:$V$200)*({p}!$V$2:$V$200>={c}$2)*({p}!$V$2:$V$200<{c}$2+7)*ISNUMBER({p}!$W$2:$W$200)*({p}!$W$2:$W$200))"
            f"+SUMPRODUCT(ISNUMBER({p}!$X$2:$X$200)*({p}!$X$2:$X$200>={c}$2)*({p}!$X$2:$X$200<{c}$2+7)*ISNUMBER({p}!$Y$2:$Y$200)*({p}!$Y$2:$Y$200))"
        )

        # CT Green Estates: $0.25/W at final payment for pre-install jobs (Pipeline tab col AB/AC)
        f_ct_green = (
            f"=SUMPRODUCT(ISNUMBER({p}!$AB$2:$AB$200)*({p}!$AB$2:$AB$200>={c}$2)*({p}!$AB$2:$AB$200<{c}$2+7)*ISNUMBER({p}!$AC$2:$AC$200)*({p}!$AC$2:$AC$200))"
        )

        for row, formula in [(row_draws, f_draws), (row_finals, f_finals),
                              (row_comm1, f_comm1), (row_comm2, f_comm2)]:
            updates.append({"range": f"'{CASHFLOW_MAIN_TAB}'!{c}{row}", "values": [[formula]]})

        # Cash materials: $1.26/W at 60% progress date (Pipeline tab col AD/AE)
        f_cash_mat = (
            f"=SUMPRODUCT(ISNUMBER({p}!$AD$2:$AD$200)*({p}!$AD$2:$AD$200>={c}$2)*({p}!$AD$2:$AD$200<{c}$2+7)*ISNUMBER({p}!$AE$2:$AE$200)*({p}!$AE$2:$AE$200))"
        )

        # Only write CT Green formula if cell is currently empty
        col_idx = start_col + w
        if row_ct_green and col_idx not in ct_green_existing:
            updates.append({"range": f"'{CASHFLOW_MAIN_TAB}'!{c}{row_ct_green}", "values": [[f_ct_green]]})

        if row_cash_mat:
            updates.append({"range": f"'{CASHFLOW_MAIN_TAB}'!{c}{row_cash_mat}", "values": [[f_cash_mat]]})

        # Subcontractor + Referral: Pipeline cols P (subcontractor) + R (referral)
        if row_sub_ref:
            f_sub_ref = (
                f"=SUMPRODUCT(ISNUMBER({p}!$I$2:$I$200)*({p}!$I$2:$I$200>={c}$2)*({p}!$I$2:$I$200<{c}$2+7)*ISNUMBER({p}!$P$2:$P$200)*({p}!$P$2:$P$200))"
                f"+SUMPRODUCT(ISNUMBER({p}!$M$2:$M$200)*({p}!$M$2:$M$200>={c}$2)*({p}!$M$2:$M$200<{c}$2+7)*(LEFT({p}!$C$2:$C$200,4)=\"CASH\")*ISNUMBER({p}!$P$2:$P$200)*({p}!$P$2:$P$200))"
                f"+SUMPRODUCT(ISNUMBER({p}!$I$2:$I$200)*({p}!$I$2:$I$200>={c}$2)*({p}!$I$2:$I$200<{c}$2+7)*ISNUMBER({p}!$R$2:$R$200)*({p}!$R$2:$R$200))"
            )
            updates.append({"range": f"'{CASHFLOW_MAIN_TAB}'!{c}{row_sub_ref}", "values": [[f_sub_ref]]})

    sheets.values().batchUpdate(
        spreadsheetId=CASHFLOW_SHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": updates},
    ).execute()

    logger.info(f"_update_cashflow_formulas: updated {len(updates)} cells → '{pipeline_tab_name}'")
    return {"status": "ok", "pipeline_tab": pipeline_tab_name, "cells_updated": len(updates)}


@app.post("/cashflow/run")
async def cashflow_run(request: Request):
    """
    Pull all installed Zoho projects since cutoff_date, fetch Aurora data,
    calculate payment dates/amounts by finance type, and write a Pipeline tab
    to the cash flow Google Sheet.

    LR: 80% draw ~14 days post-Substantial Completion (next Monday), 20%
        final 21 days later. Materials deducted at $1.26/W, $250 warranty
        deducted from final.
    CF/SG/SE/Smart E (loans): single payment = contract price on SC date.
    Cash: single payment = contract price on SC/PTO date.

    Body (optional): {"cutoff_date": "2025-06-01"}
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    cutoff = (body.get("cutoff_date") or "2025-06-01") if isinstance(body, dict) else "2025-06-01"
    now_label = datetime.datetime.now(datetime.timezone.utc).strftime("%-m-%-d-%Y")
    tab_name = f"Pipeline {now_label}"
    projects = _fetch_all_cashflow_projects(cutoff_date=cutoff)
    if not projects:
        return {"status": "no installed projects found", "cutoff_date": cutoff}
    try:
        result = _run_cashflow_batch(projects, tab_name)
        result["project_count"] = len(projects)
        result["cutoff_date"] = cutoff
        return result
    except Exception as e:
        import traceback
        return {
            "error": str(e),
            "traceback": traceback.format_exc(),
            "project_count": len(projects),
        }


def _find_current_pipeline_tab(svc) -> str | None:
    """Return the most recently created Pipeline tab name, or None."""
    sheets = svc.spreadsheets()
    meta = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
    pipeline_tabs = [
        s["properties"]["title"]
        for s in meta.get("sheets", [])
        if s["properties"]["title"].startswith("Pipeline ")
    ]
    if not pipeline_tabs:
        return None
    # Sort by sheet index (order in workbook) — most recent is last added
    tab_order = {
        s["properties"]["title"]: s["properties"]["index"]
        for s in meta.get("sheets", [])
    }
    return max(pipeline_tabs, key=lambda t: tab_order.get(t, 0))


def _apply_overrides_to_pipeline_tab(svc, tab_name: str, overrides: dict) -> dict:
    """
    Patch payment date and commission date cells in an existing Pipeline tab
    for any project listed in overrides.

    Pipeline columns (1-indexed):
      B=2  Project ID
      I=9  Payment 1 Date   T=20 Comm Payout 1 Date
      K=11 Payment 2 Date   V=22 Comm Payout 2 Date
      M=13 Payment 3 Date   X=24 Comm Payout 3 Date
    """
    sheets = svc.spreadsheets()

    # Read project IDs (col B) to find row numbers
    col_b = sheets.values().get(
        spreadsheetId=CASHFLOW_SHEET_ID,
        range=f"'{tab_name}'!B1:B200",
        valueRenderOption="FORMATTED_VALUE",
    ).execute().get("values", [])

    # Build {project_id: sheet_row_number (1-indexed)}
    proj_row = {}
    for i, cell in enumerate(col_b):
        val = cell[0].strip() if cell else ""
        if val and val != "Project ID":
            proj_row[val] = i + 1

    # payment key → (date col letter, comm col letter)
    PAY_COLS = {
        "payment1": ("I", "T"),
        "payment2": ("K", "V"),
        "payment3": ("M", "X"),
    }

    updates = []
    patched = []
    for proj_id, pov in overrides.items():
        row_num = proj_row.get(proj_id)
        if not row_num:
            logger.warning(f"apply_overrides: {proj_id} not found in {tab_name}")
            continue
        for key, (pay_col, comm_col) in PAY_COLS.items():
            if pov.get(key):
                date_val = pov[key]
                updates.append({"range": f"'{tab_name}'!{pay_col}{row_num}",  "values": [[date_val]]})
                updates.append({"range": f"'{tab_name}'!{comm_col}{row_num}", "values": [[date_val]]})
        patched.append(proj_id)

    if updates:
        sheets.values().batchUpdate(
            spreadsheetId=CASHFLOW_SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": updates},
        ).execute()

    logger.info(f"_apply_overrides_to_pipeline_tab: patched {len(patched)} project(s)")
    return {"patched": patched, "cells_updated": len(updates)}


@app.post("/cashflow/apply-overrides")
async def cashflow_apply_overrides():
    """
    Re-read the Overrides tab and patch payment dates in the current Pipeline tab,
    then regenerate Weekly Payments and refresh Cash Flow formulas.
    Does NOT re-pull from Zoho or Aurora — runs in a few seconds.
    """
    try:
        svc = _build_sheets_service()
        if not svc:
            return {"status": "failed", "reason": "could not build Sheets service"}

        tab_name = _find_current_pipeline_tab(svc)
        if not tab_name:
            return {"status": "failed", "reason": "no Pipeline tab found — run /cashflow/run first"}

        overrides = _read_payment_overrides(svc)
        if not overrides:
            return {"status": "ok", "tab": tab_name, "message": "no overrides found in Overrides tab"}

        patch_result = _apply_overrides_to_pipeline_tab(svc, tab_name, overrides)

        # Rebuild Weekly Payments from the now-patched Pipeline tab data
        # Read the full pipeline tab and reconstruct row dicts for _write_weekly_payments_tab
        sheets = svc.spreadsheets()
        raw = sheets.values().get(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{tab_name}'!A1:AA200",
            valueRenderOption="FORMATTED_VALUE",
        ).execute().get("values", [])

        if len(raw) < 2:
            return {"status": "ok", "tab": tab_name, "patch": patch_result, "weekly_payments": "no data"}

        headers = raw[0]
        def col(name):
            try:
                return headers.index(name)
            except ValueError:
                return None

        ci = {
            "customer":     col("Customer"),
            "project_id":   col("Project ID"),
            "finance_type": col("Finance Type"),
            "stage":        col("Stage"),
            "sc":           col("SC / Projected SC"),
            "pay1_date":    col("Payment 1 Date"),
            "pay1_amt":     col("Payment 1 Amt"),
            "pay2_date":    col("Payment 2 Date"),
            "pay2_amt":     col("Payment 2 Amt"),
            "pay3_date":    col("Payment 3 Date"),
            "pay3_amt":     col("Payment 3 Amt"),
            "comm1_date":   col("Comm Payout 1 Date"),
            "comm1_amt":    col("Comm Payout 1 Amt"),
            "comm2_date":   col("Comm Payout 2 Date"),
            "comm2_amt":    col("Comm Payout 2 Amt"),
            "comm3_date":   col("Comm Payout 3 Date"),
            "comm3_amt":    col("Comm Payout 3 Amt"),
            "zoho_link":    col("Zoho Link"),
        }

        def cell(row, key):
            idx = ci.get(key)
            if idx is None or idx >= len(row):
                return ""
            return row[idx]

        def week_of(date_str):
            try:
                d = datetime.date.fromisoformat(date_str)
                return (d - datetime.timedelta(days=d.weekday())).isoformat()
            except (ValueError, TypeError):
                return ""

        PAYMENT_TYPE_MAP = {
            ("LR",   "pay1"): "LR 80% Draw",
            ("LR",   "pay2"): "LR 20% Final",
            ("CASH", "pay1"): "Cash 20% Deposit",
            ("CASH", "pay2"): "Cash 60% Progress",
            ("CASH", "pay3"): "Cash 20% Final",
            ("SE",   "pay1"): "SE Payment 1 (33%)",
            ("SE",   "pay2"): "SE Payment 2 (33%)",
            ("SE",   "pay3"): "SE Payment 3 (34%)",
        }

        event_rows = []
        for row in raw[1:]:
            if not any(row):
                continue
            ft = cell(row, "finance_type")
            customer = cell(row, "customer")
            stage = cell(row, "stage")
            sc_display = cell(row, "sc")
            proj_id = cell(row, "project_id")
            zoho_link = cell(row, "zoho_link")

            for slot, pay_key, comm_key in [
                ("pay1", "pay1_date", "comm1_date"),
                ("pay2", "pay2_date", "comm2_date"),
                ("pay3", "pay3_date", "comm3_date"),
            ]:
                pay_date = cell(row, pay_key)
                pay_amt_raw = cell(row, pay_key.replace("_date", "_amt"))
                comm_date = cell(row, comm_key)
                comm_amt_raw = cell(row, comm_key.replace("_date", "_amt"))
                if not pay_date:
                    continue
                pay_type = PAYMENT_TYPE_MAP.get((ft, slot), "Loan / Full Payment")
                try:
                    pay_amt = float(str(pay_amt_raw).replace("$", "").replace(",", "")) if pay_amt_raw else ""
                except ValueError:
                    pay_amt = pay_amt_raw
                try:
                    comm_amt = float(str(comm_amt_raw).replace("$", "").replace(",", "")) if comm_amt_raw else ""
                except ValueError:
                    comm_amt = comm_amt_raw

                event_rows.append([
                    week_of(pay_date), pay_date, customer, ft, pay_type,
                    pay_amt, comm_date, comm_amt, stage, sc_display, proj_id, zoho_link,
                ])

        event_rows.sort(key=lambda r: r[1] if r[1] else "9999")

        weekly_tab = CASHFLOW_WEEKLY_TAB
        existing = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
        weekly_sheet_id = None
        for s in existing.get("sheets", []):
            if s["properties"]["title"] == weekly_tab:
                weekly_sheet_id = s["properties"]["sheetId"]
                break

        if weekly_sheet_id is None:
            resp = sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": [{"addSheet": {"properties": {"title": weekly_tab}}}]}
            ).execute()
            weekly_sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]

        weekly_headers = [
            "Week Of", "Payment Date", "Customer", "Finance Type", "Payment Type",
            "Amount", "Commission Date", "Commission Amt",
            "Stage", "SC / Projected SC", "Project ID", "Zoho Link",
        ]
        sheets.values().update(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{weekly_tab}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [weekly_headers] + event_rows},
        ).execute()

        formula_result = _update_cashflow_formulas(svc, tab_name)

        return {
            "status": "ok",
            "tab": tab_name,
            "overrides_applied": len(patch_result["patched"]),
            "projects_patched": patch_result["patched"],
            "weekly_payment_rows": len(event_rows),
            "formulas": formula_result,
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.post("/cashflow/reorganize-expenses")
async def cashflow_reorganize_expenses():
    """
    Reorganize the Cash Flow tab expense rows (CASH OUT section) into
    categorized groups with header rows. Safe to run multiple times —
    reads existing values and rewrites the section in place.
    """
    try:
        svc = _build_sheets_service()
        if not svc:
            return {"error": "could not build sheets service"}
        sheets = svc.spreadsheets()

        # Read full Cash Flow tab to find structure
        raw = sheets.values().get(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=f"'{CASHFLOW_MAIN_TAB}'!A1:ZZ200",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])

        # Find key structural rows
        def find_row_idx(fragment):
            for i, r in enumerate(raw):
                if r and fragment.lower() in str(r[0]).lower():
                    return i  # 0-indexed
            return None

        cash_out_idx = find_row_idx("CASH OUT")
        total_cash_out_idx = find_row_idx("Total Cash Out")

        if cash_out_idx is None or total_cash_out_idx is None:
            return {"error": "Could not find CASH OUT or Total Cash Out rows"}

        # Determine week columns by reading row 2 (0-indexed row 1)
        row2 = raw[1] if len(raw) > 1 else []
        start_col = 3  # column D (0-indexed)
        end_col = start_col
        for i in range(start_col, len(row2)):
            if row2[i]:
                end_col = i
        num_cols = end_col - start_col + 1  # number of week columns

        # Build a lookup: label → list of week values
        def get_vals(label_fragment):
            idx = find_row_idx(label_fragment)
            if idx is None:
                return [""] * num_cols
            r = raw[idx]
            vals = []
            for c in range(start_col, start_col + num_cols):
                vals.append(r[c] if c < len(r) else "")
            return vals

        # Collect existing expense values by label
        expense_data = {
            "Payroll":             get_vals("Payroll"),
            "Amex":                get_vals("Amex"),
            "Lowest Credit Card":  get_vals("Lowest Credit Card"),
            "Ink Card":            get_vals("Ink Card"),
            "Mulligan":            get_vals("Mulligan"),
            "SBA Loan":            get_vals("SBA Loan"),
            "QuickBooks Loan":     get_vals("QuickBooks Loan"),
            "QuickBooks 2nd Loan": get_vals("QuickBooks 2nd Loan"),
            "Auto Loans":          get_vals("Auto Loans"),
            "Soligent":            get_vals("Soligent"),
            "Greentech":           get_vals("Greentech"),
            "US Renewables":       get_vals("US Renewables"),
            "EW":                  get_vals("\tEW"),  # exact match via tab char fallback
            "F&M":                 get_vals("F&M"),
            "TK Properties":       get_vals("TK Properties"),
            "Aurora":              get_vals("Aurora"),
            "Zoho One":            get_vals("Zoho One"),
            "Squarespace":         get_vals("Squarespace"),
            "Google Workspace":    get_vals("Google Workspace"),
            "BBB":                 get_vals("BBB"),
            "Office365 Business":  get_vals("Office365"),
            "Sirius Radio":        get_vals("Sirius Radio"),
            "Site Capture":        get_vals("Site Capture"),
            "Adobe Services":      get_vals("Adobe Services"),
            "Energytoolbase.com":  get_vals("Energytoolbase"),
            "duns and bradstreet": get_vals("duns and bradstreet"),
            "Nav.com":             get_vals("Nav.com"),
            "zappier":             get_vals("zappier"),
            "Loom.com":            get_vals("Loom.com"),
            "chatgpt":             get_vals("chatgpt"),
            "Anthropic":           get_vals("Anthropic"),
            "50 Merritt Drive (Rent)": get_vals("50 Merritt"),
            "Virtual Mailboxes":   get_vals("Virtual Mailbox"),
            "AT&T":                get_vals("AT&T"),
            "Canon Printer":       get_vals("Canon Printer"),
            "SolarInsure":         get_vals("SolarInsure"),
            "Misc":                get_vals("Misc"),
        }

        # Also get EW without tab char fallback
        ew_idx = None
        for i, r in enumerate(raw):
            if r and str(r[0]).strip().upper() == "EW":
                ew_idx = i
                break
        if ew_idx is not None:
            r = raw[ew_idx]
            expense_data["EW"] = [r[c] if c < len(r) else "" for c in range(start_col, start_col + num_cols)]
        else:
            expense_data["EW"] = [""] * num_cols

        # Build new expense section rows
        # Each row: [label, "", "", (week values...)]
        # Header rows: [label] with empty data cols
        empty = [""] * num_cols

        def data_row(label, vals=None):
            return [label, "", ""] + (vals if vals else empty)

        def header_row(label):
            return [label, "", ""] + empty

        new_rows = []

        # 1. Payroll
        new_rows.append(data_row("Payroll", expense_data["Payroll"]))

        # 2. Debt Payments
        new_rows.append(header_row("  Debt Payments"))
        for label in ["Amex", "Lowest Credit Card", "Ink Card", "Mulligan", "SBA Loan",
                       "QuickBooks Loan", "QuickBooks 2nd Loan", "Auto Loans",
                       "Soligent", "Greentech", "US Renewables", "EW", "F&M", "TK Properties"]:
            new_rows.append(data_row(label, expense_data[label]))

        # 3. Subscriptions
        new_rows.append(header_row("  Subscriptions"))
        for label in ["Aurora", "Zoho One", "Squarespace", "Google Workspace", "BBB",
                       "Office365 Business", "Sirius Radio", "Site Capture", "Adobe Services",
                       "Energytoolbase.com", "duns and bradstreet", "Nav.com", "zappier",
                       "Loom.com", "chatgpt", "Anthropic"]:
            new_rows.append(data_row(label, expense_data[label]))

        # 4. Office Expenses
        new_rows.append(header_row("  Office Expenses"))
        for label in ["50 Merritt Drive (Rent)", "Virtual Mailboxes", "AT&T", "Canon Printer"]:
            new_rows.append(data_row(label, expense_data[label]))

        # 5. Project Expenses
        new_rows.append(header_row("  Project Expenses"))
        new_rows.append(data_row("CT Green Estates", empty))       # auto (formula)
        new_rows.append(data_row("SolarInsure", expense_data["SolarInsure"]))
        new_rows.append(data_row("Subcontractor + Referral", empty))  # auto (formula)
        new_rows.append(data_row("Materials (Cash Deals)", empty))  # auto (formula)

        # 6. Commissions
        new_rows.append(header_row("  Commissions"))
        new_rows.append(data_row("Commissions (Payout 1)", empty))  # auto (formula)
        new_rows.append(data_row("Commissions (Payout 2)", empty))  # auto (formula)

        # 7. Misc
        new_rows.append(data_row("Misc", expense_data["Misc"]))

        # Write the new expense section starting right after CASH OUT row
        # The first expense row is 2 rows after CASH OUT (CASH OUT row + 1 blank/header = expense start)
        # Find where Payroll currently lives to know the write start
        payroll_idx = find_row_idx("Payroll")
        if payroll_idx is None:
            # Fall back to 2 rows after CASH OUT
            write_start_idx = cash_out_idx + 2
        else:
            write_start_idx = payroll_idx

        write_start_row = write_start_idx + 1  # 1-indexed
        write_end_row = total_cash_out_idx  # write up to (not including) Total Cash Out

        # Pad / trim to exactly fit
        num_rows_available = write_end_row - write_start_row  # rows before Total Cash Out
        while len(new_rows) < num_rows_available:
            new_rows.append([""] * (3 + num_cols))

        # Write expense rows (overwrite existing)
        col_letter_end = _col_letter(start_col + num_cols)  # last week col letter
        write_range = f"'{CASHFLOW_MAIN_TAB}'!A{write_start_row}:{col_letter_end}{write_start_row + len(new_rows) - 1}"

        sheets.values().update(
            spreadsheetId=CASHFLOW_SHEET_ID,
            range=write_range,
            valueInputOption="USER_ENTERED",
            body={"values": new_rows},
        ).execute()

        # Get sheet ID for formatting
        sheet_meta = sheets.get(spreadsheetId=CASHFLOW_SHEET_ID).execute()
        cf_sheet_id = None
        for s in sheet_meta.get("sheets", []):
            if s["properties"]["title"] == CASHFLOW_MAIN_TAB:
                cf_sheet_id = s["properties"]["sheetId"]
                break

        # Format category header rows as bold + light gray background
        fmt_requests = []
        if cf_sheet_id is not None:
            header_labels = {"  Debt Payments", "  Subscriptions", "  Office Expenses",
                             "  Project Expenses", "  Commissions"}
            for i, row in enumerate(new_rows):
                if row and row[0] in header_labels:
                    sheet_row = write_start_row + i - 1  # 0-indexed
                    fmt_requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": cf_sheet_id,
                                "startRowIndex": sheet_row,
                                "endRowIndex": sheet_row + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "textFormat": {"bold": True},
                                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                                }
                            },
                            "fields": "userEnteredFormat.textFormat.bold,userEnteredFormat.backgroundColor",
                        }
                    })

        if fmt_requests:
            sheets.batchUpdate(
                spreadsheetId=CASHFLOW_SHEET_ID,
                body={"requests": fmt_requests},
            ).execute()

        # Re-run formulas to populate auto rows
        tab_name = None
        for s in sheet_meta.get("sheets", []):
            t = s["properties"]["title"]
            if t.startswith("Pipeline "):
                tab_name = t
        formula_result = _update_cashflow_formulas(svc, tab_name) if tab_name else {"skipped": "no Pipeline tab found"}

        return {
            "status": "ok",
            "rows_written": len(new_rows),
            "write_range": write_range,
            "formulas": formula_result,
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}

