from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from urllib.parse import quote
import os
import requests
import datetime
import json
import time

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

    # Update Install with Active Snapshot (and LightReach fields, if any)
    update_payload = {
        "data": [
            {
                "id": install_id,
                "Active_Snapshot": {"id": snapshot_id},
                **lightreach_fields,
            }
        ]
    }

    update_url = f"{api_domain}/crm/v2/Installs"
    update_response = requests.put(update_url, headers=headers, json=update_payload)
    if update_response.status_code not in [200, 202]:
        return {"status": "failed - install update error"}

    return {"status": "initial snapshot created", "snapshot_id": snapshot_id}


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
            milestone = body.get("milestone") or body.get("milestoneName") or body.get("name") or ""
            if isinstance(milestone, dict):
                milestone = milestone.get("name") or milestone.get("type") or ""
            logger.info(f"LightReach milestone achieved: {milestone}")
            if "ntp" in str(milestone).lower() or "notice to proceed" in str(milestone).lower():
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
            if update_resp.status_code in [200, 201, 202]:
                logger.info(
                    f"[{event_id}] Install promoted with active snapshot | "
                    f"install_id={install_id} milestone={milestone_lc} "
                    f"lightreach_keys={list(lightreach_fields.keys())}"
                )
            else:
                logger.warning(
                    f"[{event_id}] Install promotion update failed | "
                    f"status={update_resp.status_code} body={update_resp.text[:300]}"
                )

        return {
            "status": "processed",
            "is_advancing": is_advancing,
            "is_initial_sold": is_initial_sold,
        }
    except Exception:
        logger.exception("Unhandled exception during webhook processing")
        return {"status": "failed - exception"}