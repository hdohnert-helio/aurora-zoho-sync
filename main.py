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
        "refresh_token": os.getenv("ZOHO_REFRESH_TOKEN")
    }

    response = requests.post(url, data=payload)
    return response.json().get("access_token")


# ------------------------
# Aurora API Helpers
# ------------------------
def aurora_headers():
    return {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }


def pull_design(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}"
    return requests.get(url, headers=aurora_headers())


def pull_pricing(design_id):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"
    return requests.get(url, headers=aurora_headers())


# ------------------------
# Find Install by Aurora Project ID
# ------------------------
def find_install(project_id, access_token):
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    api_domain = os.getenv("ZOHO_API_DOMAIN")
    url = f"{api_domain}/crm/v2/Installs/search?criteria=(Aurora_Project_ID:equals:{project_id})"

    return requests.get(url, headers=headers)


# ------------------------
# Create Snapshot Record
# ------------------------
def create_snapshot(snapshot_data, access_token):
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    api_domain = os.getenv("ZOHO_API_DOMAIN")
    url = f"{api_domain}/crm/v2/Aurora_Design_Snapshots"

    payload = {
        "data": [snapshot_data]
    }

    return requests.post(url, headers=headers, json=payload)


# ------------------------
# Webhook Endpoint
# ------------------------
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

    # Handle possible wrapped JSON
    design_root = design_response.json()
    design_json = design_root.get("design", design_root)

    pricing_root = pricing_response.json()
    pricing_json = pricing_root.get("pricing", pricing_root)

    # ------------------------
    # Extract Milestone Data
    # ------------------------
    milestone = design_json.get("milestone", {})
    milestone_name = milestone.get("milestone")

    milestone_time_raw = milestone.get("recorded_at")

    if milestone_time_raw:
        milestone_time = datetime.datetime.fromisoformat(
            milestone_time_raw.replace("Z", "+00:00")
        ).astimezone().replace(microsecond=0).isoformat()
    else:
        milestone_time = None

    # ------------------------
    # Extract Pricing Data
    # ------------------------
    system_size = design_json.get("system_size_stc")
    price_per_watt = pricing_json.get("price_per_watt")
    final_price = pricing_json.get("system_price")

    breakdown = pricing_json.get("system_price_breakdown", [])

    base_price = 0
    total_adders = 0
    total_discounts = 0

    for item in breakdown:
        item_type = item.get("item_type")
        item_price = item.get("item_price", 0)

        if item_type == "base_price":
            base_price = item_price
        elif item_type == "adders":
            total_adders = item_price
        elif item_type == "discounts":
            total_discounts = item_price

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

    snapshot_data = {
        "Name": snapshot_name,
        "Aurora_Project_ID": project_id,
        "Aurora_Design_ID": design_id,
        "Aurora_Milestone": milestone_name,
        "Milestone_Recorded_At": milestone_time,
        "Webhook_Received_At": timestamp_now,
        "System_Size_STC_Watts": system_size,
        "Price_Per_Watt": price_per_watt,
        "Base_Price": base_price,
        "Adders_Total": total_adders,
        "Discounts_Total": total_discounts,
        "Final_System_Price": final_price,
        "Install": install_id,
        "Deal": deal_id,
        "Raw_Design_JSON": json.dumps(design_json),
        "Raw_Pricing_JSON": json.dumps(pricing_json),
        "Processing_Status": "Processed"
    }

    snapshot_create_response = create_snapshot(snapshot_data, access_token)

    print("Snapshot create status:", snapshot_create_response.status_code)
    print("Snapshot create response:", snapshot_create_response.text)

    return {"status": "processed"}