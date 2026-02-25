from fastapi import FastAPI, Request, HTTPException
import os
import requests
import datetime
import json

app = FastAPI()

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
def aurora_headers():
    return {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json",
    }


def pull_design(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    # Note: include_layout=true may still return a summary object in some tenants,
    # but we keep it as it can be helpful where supported.
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}?include_layout=true"
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
# Webhook Endpoint
@app.api_route("/webhook/aurora", methods=["GET", "POST"])
async def aurora_webhook(request: Request):

    # Validate secret
    expected_secret = os.getenv("AURORA_WEBHOOK_SECRET")
    received_secret = request.headers.get("X-Aurora-Webhook-Secret")

    if received_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")

    params = dict(request.query_params)

    project_id = params.get("project_id")
    design_id = params.get("design_id")

    print("Webhook received:", params)

    if not project_id or not design_id:
        return {"status": "ignored - missing ids"}

    print("Processing milestone event...")
    print("Project ID:", project_id)
    print("Design ID:", design_id)

    # ------------------------
    # Pull Aurora Data
    # ------------------------
    design_response = pull_design(design_id)
    pricing_response = pull_pricing(design_id)

    print("Design pull status:", design_response.status_code)
    print("Pricing pull status:", pricing_response.status_code)

    if design_response.status_code != 200 or pricing_response.status_code != 200:
        return {"status": "failed - aurora pull error"}

    design_root = design_response.json()
    design_json = design_root.get("design", design_root)

    pricing_root = pricing_response.json()
    pricing_json = pricing_root.get("pricing", pricing_root)

    # ------------------------
    # Extract System Size (Robust)
    # ------------------------
    system_size_watts = 0

    breakdown = pricing_json.get("system_price_breakdown", [])

    # Prefer authoritative calculation when pricing is "price per watt"
    pricing_method = (pricing_json.get("pricing_method") or "").strip().lower()
    ppw = float(pricing_json.get("price_per_watt") or 0)

    base_price_for_size = 0.0
    for item in breakdown:
        if item.get("item_type") == "base_price":
            base_price_for_size = float(item.get("item_price") or 0)
            break

    if ("price per watt" in pricing_method) and ppw > 0 and base_price_for_size > 0:
        # Example: 40260 / 3.05 = 13200 watts
        system_size_watts = int(round(base_price_for_size / ppw))
    else:
        # Fallback: infer from per-watt adders/discounts quantities (choose the largest)
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
                    # Filter out flat-quantity adders (often 1) and small non-size quantities
                    if qty_f >= 1000 and qty_f > max_qty:
                        max_qty = qty_f
        if max_qty > 0:
            system_size_watts = int(round(max_qty))

    print("Resolved System Size (Watts):", system_size_watts)

    # ------------------------
    # Extract Milestone Data
    # ------------------------
    milestone = design_json.get("milestone", {})
    milestone_name = milestone.get("milestone")
    milestone_id = milestone.get("id")

    milestone_time_raw = milestone.get("recorded_at")

    if milestone_time_raw:
        milestone_time = (
            datetime.datetime.fromisoformat(milestone_time_raw.replace("Z", "+00:00"))
            .astimezone()
            .replace(microsecond=0)
            .isoformat()
        )
    else:
        milestone_time = None

    aurora_design_name = design_json.get("name")

    aurora_created_raw = design_json.get("created_at")
    if aurora_created_raw:
        aurora_created_at = (
            datetime.datetime.fromisoformat(aurora_created_raw.replace("Z", "+00:00"))
            .astimezone()
            .replace(microsecond=0)
            .isoformat()
        )
    else:
        aurora_created_at = None


    # ------------------------
    # Extract Pricing Data
    # ------------------------
    price_per_watt = pricing_json.get("price_per_watt")
    final_price = pricing_json.get("system_price")

    gross_price_per_watt = (
        round(float(final_price) / system_size_watts, 4)
        if system_size_watts and float(final_price or 0) > 0
        else 0
    )

    breakdown = pricing_json.get("system_price_breakdown", [])

    base_price = 0.00
    total_adders = 0.00
    total_discounts = 0.00

    for item in breakdown:
        item_type = item.get("item_type")
        item_price = float(item.get("item_price", 0) or 0)

        if item_type == "base_price":
            base_price = round(item_price, 2)
        elif item_type == "adders":
            total_adders = round(item_price, 2)
        elif item_type == "discounts":
            total_discounts = round(item_price, 2)

    # ------------------------
    # Extract Commission-Related Adders
    # ------------------------
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


    adder_name_list = ", ".join(
        adder.get("adder_name")
        for adder in pricing_json.get("adders", [])
        if not adder.get("is_discount")
    )

    discount_name_list = ", ".join(
        adder.get("adder_name")
        for adder in pricing_json.get("adders", [])
        if adder.get("is_discount")
    )

    adder_details = []
    discount_details = []

    for item in pricing_json.get("system_price_breakdown", []):
        item_type = item.get("item_type")

        if item_type in ["adders", "discounts"]:
            for sub in item.get("subcomponents", []):
                record = {
                    "name": sub.get("adder_name"),
                    "quantity": sub.get("quantity"),
                    "total": sub.get("item_price"),
                }

                if item_type == "adders":
                    adder_details.append(record)
                elif item_type == "discounts":
                    discount_details.append(record)

    adder_details_json = json.dumps(adder_details)
    discount_details_json = json.dumps(discount_details)


    # ------------------------
    # Extract Equipment Details
    # ------------------------
    module_model = None
    module_count = 0
    inverter_model = None
    inverter_count = 0
    optimizer_count = 0

    for component in pricing_json.get("pricing_by_component", []):
        component_type = component.get("component_type")
        name = component.get("name")
        quantity = component.get("quantity")

        try:
            qty = int(float(quantity)) if quantity is not None else 0
        except (TypeError, ValueError):
            qty = 0

        if component_type == "modules":
            module_model = name
            module_count = qty
        elif component_type == "inverters":
            inverter_model = name
            inverter_count = qty
        elif component_type == "dc_optimizers":
            optimizer_count = qty


    # ------------------------
    # Zoho Token
    # ------------------------
    access_token = get_zoho_access_token()
    if not access_token:
        return {"status": "failed - no zoho token"}

    # ------------------------
    # Find Install
    # ------------------------
    install_response = find_install(project_id, access_token)

    if install_response.status_code != 200:
        return {"status": "failed - install search error"}

    install_data = install_response.json().get("data")
    if not install_data:
        return {"status": "failed - install not found"}

    install_record = install_data[0]
    install_id = install_record.get("id")

    opportunity = install_record.get("Opportunity")
    deal_id = opportunity.get("id") if opportunity else None

    # ------------------------
    # Snapshot Creation
    # ------------------------
    timestamp_now = datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()

    snapshot_name = f"{project_id[:8]} | {design_id[:8]} | {milestone_name} | {timestamp_now}"

    aurora_design_url = f"https://app.aurorasolar.com/projects/{project_id}/designs/{design_id}"

    snapshot_data = {
        "Name": snapshot_name,
        "Aurora_Project_ID": project_id,
        "Aurora_Design_ID": design_id,
        "Aurora_Milestone": milestone_name,
        "Aurora_Milestone_ID": milestone_id,
        "Aurora_Design_Name": aurora_design_name,
        "Aurora_Created_At": aurora_created_at,
        "Milestone_Recorded_At": milestone_time,
        "Webhook_Received_At": timestamp_now,
        "System_Size_STC_Watts": system_size_watts,
        "Price_Per_Watt": price_per_watt,
        "Gross_Price_Per_Watt": gross_price_per_watt,
        "Base_Price": base_price,
        "Adders_Total": total_adders,
        "Discounts_Total": total_discounts,
        "Adder_Name_List": adder_name_list,
        "Discount_Name_List": discount_name_list,
        "Adder_Details_JSON": adder_details_json,
        "Discount_Details_JSON": discount_details_json,
        "Consultant_Comp_PPW": consultant_comp_ppw,
        "Helio_Lead_Fee_PPW": helio_lead_fee_ppw,
        "Referral_Payout": referral_payout,
        "ES_Upline_Discount_PPW": es_upline_discount_ppw,
        "EVP_Upline_Discount_PPW": evp_upline_discount_ppw,
        "Module_Model": module_model,
        "Module_Count": module_count,
        "Inverter_Model": inverter_model,
        "Inverter_Count": inverter_count,
        "Optimizer_Count": optimizer_count,
        "Final_System_Price": round(float(final_price or 0), 2),
        "Install": install_id,
        "Deal": deal_id,
        "Aurora_Design_URL": aurora_design_url,
        "Raw_Design_JSON": json.dumps(design_json),
        "Raw_Pricing_JSON": json.dumps(pricing_json),
        "Processing_Status": "Processed",
    }

    snapshot_create_response = create_snapshot(snapshot_data, access_token)

    print("Snapshot create status:", snapshot_create_response.status_code)
    print("Snapshot create response:", snapshot_create_response.text)

    return {"status": "processed"}