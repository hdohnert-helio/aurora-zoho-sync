from fastapi import FastAPI, Request, HTTPException
from urllib.parse import quote
import os
import requests
import datetime
import json

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
@app.post("/internal/create-initial-snapshot")
async def create_initial_snapshot(request: Request):
    try:
        body = await request.json()
        install_id = body.get("install_id")
        project_id = body.get("project_id")
        deal_id = body.get("deal_id")

        if not install_id or not project_id:
            return {"status": "failed - missing install_id or project_id"}

        access_token = get_zoho_access_token()
        if not access_token:
            return {"status": "failed - no zoho token"}

        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
        api_domain = os.getenv("ZOHO_API_DOMAIN")

        # Pull Install record
        install_url = f"{api_domain}/crm/v2/Installs/{install_id}"
        install_response = requests.get(install_url, headers=headers)

        if install_response.status_code != 200:
            return {"status": "failed - install lookup error"}

        install_data = install_response.json().get("data", [])[0]

        # If Active Snapshot already exists, do nothing
        if install_data.get("Active_Snapshot"):
            return {"status": "skipped - active snapshot already exists"}

        # Pull Aurora designs for project
        tenant_id = os.getenv("AURORA_TENANT_ID")
        designs_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"
        designs_response = requests.get(designs_url, headers=aurora_headers())

        if designs_response.status_code != 200:
            return {"status": "failed - aurora designs pull error"}

        designs = designs_response.json().get("designs", [])

        sold_designs = [
            d for d in designs
            if d.get("milestone", {}).get("milestone") == "sold"
        ]

        if len(sold_designs) != 1:
            return {"status": "failed - sold design count invalid"}

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

        summary_json = summary_response.json().get("design", {}) if summary_response.status_code == 200 else {}

        timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()
        snapshot_name = f"{project_id[:8]} | {design_id[:8]} | INITIAL SOLD | {timestamp_now}"

        aurora_design_url = f"https://v2.aurorasolar.com/projects/{project_id}/designs/{design_id}/cad"
        aurora_project_url = f"https://v2.aurorasolar.com/projects/{project_id}/overview/dashboard"

        # Extract all pricing/equipment fields using shared helper
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

        snapshot_id = snapshot_create_response.json()["data"][0]["details"]["id"]

        # Update Install with Active Snapshot
        update_payload = {
            "data": [
                {
                    "id": install_id,
                    "Active_Snapshot": {"id": snapshot_id}
                }
            ]
        }

        update_url = f"{api_domain}/crm/v2/Installs"
        update_response = requests.put(update_url, headers=headers, json=update_payload)

        if update_response.status_code not in [200, 202]:
            return {"status": "failed - install update error"}

        return {"status": "initial snapshot created"}

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
# Webhook: LightReach (Palmetto) — Contract Signed & Status Events
# ------------------------
@app.post("/webhook/lightreach")
async def lightreach_webhook(request: Request):
    try:
        # Validate Palmetto-generated API key (sent as `apiKey` header)
        expected_key = os.getenv("LIGHTREACH_API_KEY")
        received_key = request.headers.get("apiKey")

        if expected_key and received_key != expected_key:
            logger.warning(f"LightReach webhook rejected — invalid apiKey")
            raise HTTPException(status_code=401, detail="Unauthorized")

        body = await request.json()
        logger.info(f"LightReach webhook received | payload={json.dumps(body)}")

        # --- Extract fields from payload (flexible — log raw if structure changes) ---
        event_type = body.get("event") or body.get("eventType") or body.get("type") or "unknown"
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
            f"LightReach event | type={event_type} quote_id={quote_id} "
            f"contact_id={contact_id} email={customer_email}"
        )

        # --- Find matching Zoho Install by customer email ---
        if not customer_email:
            logger.warning("LightReach webhook: no customer email in payload — cannot match Install")
            return {"status": "logged - no email to match"}

        access_token = get_zoho_access_token()
        if not access_token:
            logger.error("LightReach webhook: failed to obtain Zoho token")
            return {"status": "failed - no zoho token"}

        api_domain = os.getenv("ZOHO_API_DOMAIN")
        headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}

        # Search Installs by customer email (field name: Primary_Email)
        search_url = (
            f"{api_domain}/crm/v2/Installs/search"
            f"?criteria=(Primary_Email:equals:{quote(customer_email, safe='')})"
        )
        search_resp = requests.get(search_url, headers=headers)

        if search_resp.status_code == 204:
            logger.warning(f"LightReach webhook: no Install found for email={customer_email}")
            return {"status": "logged - no matching install"}

        if search_resp.status_code != 200:
            logger.error(
                f"LightReach webhook: Install search failed | "
                f"status={search_resp.status_code} | body={search_resp.text}"
            )
            return {"status": "failed - install search error"}

        install_records = search_resp.json().get("data", [])
        if not install_records:
            logger.warning(f"LightReach webhook: no Install found for email={customer_email}")
            return {"status": "logged - no matching install"}

        install_id = install_records[0].get("id")
        logger.info(f"LightReach webhook: matched Install id={install_id} for email={customer_email}")

        # --- Update Install with LightReach data ---
        timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()

        update_fields = {
            "id": install_id,
            "LightReach_Contract_Status": event_type,
            "LightReach_Last_Updated": timestamp_now,
            "LightReach_Raw_Payload": json.dumps(body),
        }
        if quote_id:
            update_fields["LightReach_Quote_ID"] = quote_id
        if contact_id:
            update_fields["LightReach_Contact_ID"] = contact_id
        if signed_at:
            update_fields["LightReach_Contract_Signed_At"] = signed_at

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
            "Processing_Status": "Processed",
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
        else:
            logger.info(
                f"[{event_id}] Snapshot created successfully | "
                f"status={snapshot_create_response.status_code}"
            )
            return {"status": "processed"}
    except Exception:
        logger.exception("Unhandled exception during webhook processing")
        return {"status": "failed - exception"}