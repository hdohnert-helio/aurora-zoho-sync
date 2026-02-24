from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
import os
import requests

app = FastAPI()

# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/")
def health_check():
    return {"status": "Aurora-Zoho Sync Service Running"}


# ============================================================
# ZOHO AUTH + TEST
# ============================================================

def get_zoho_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": os.getenv("ZOHO_CLIENT_ID"),
        "client_secret": os.getenv("ZOHO_CLIENT_SECRET"),
        "refresh_token": os.getenv("ZOHO_REFRESH_TOKEN")
    }

    response = requests.post(url, data=payload)
    response_json = response.json()

    return response_json.get("access_token")


@app.get("/zoho/test")
def test_zoho():
    access_token = get_zoho_access_token()

    if not access_token:
        return {"error": "Failed to get access token"}

    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    api_domain = os.getenv("ZOHO_API_DOMAIN")
    url = f"{api_domain}/crm/v2/Leads?per_page=1"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


# ============================================================
# AURORA HELPERS
# ============================================================

def aurora_headers():
    return {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }


# ============================================================
# AURORA TEST ENDPOINTS
# ============================================================

@app.get("/aurora/test")
def test_aurora():
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects"

    response = requests.get(url, headers=aurora_headers())

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


@app.get("/aurora/project/{project_id}")
def get_project(project_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}"

    response = requests.get(url, headers=aurora_headers())

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


@app.get("/aurora/design/{design_id}")
def get_design(design_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}"

    response = requests.get(url, headers=aurora_headers())

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


@app.get("/aurora/design/{design_id}/pricing")
def get_design_pricing(design_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")
    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"

    response = requests.get(url, headers=aurora_headers())

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


# ============================================================
# BACKGROUND PROCESSOR
# ============================================================

def process_milestone_event(params):
    try:
        print("Processing milestone event...")

        project_id = params.get("project_id")
        design_id = params.get("design_id")

        print(f"Project ID: {project_id}")
        print(f"Design ID: {design_id}")

        tenant_id = os.getenv("AURORA_TENANT_ID")

        if not tenant_id:
            print("ERROR: Missing AURORA_TENANT_ID")
            return

        if not design_id:
            print("ERROR: Missing design_id")
            return

        # Pull Design
        design_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}"
        design_response = requests.get(design_url, headers=aurora_headers())

        print("Design pull status:", design_response.status_code)

        # Pull Pricing
        pricing_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"
        pricing_response = requests.get(pricing_url, headers=aurora_headers())

        print("Pricing pull status:", pricing_response.status_code)

        if pricing_response.status_code == 200:
            print("Pricing data received successfully.")

    except Exception as e:
        print("ERROR in process_milestone_event:")
        print(str(e))


# ============================================================
# AURORA WEBHOOK ENDPOINT
# ============================================================
@app.api_route("/webhook/aurora", methods=["GET", "POST"])
async def aurora_webhook(request: Request):

    expected_secret = os.getenv("AURORA_WEBHOOK_SECRET")
    received_secret = request.headers.get("X-Aurora-Webhook-Secret")

    if received_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")

    params = dict(request.query_params)

    print("Webhook received:")
    print(params)

    try:
        print("Processing milestone event...")

        project_id = params.get("project_id")
        design_id = params.get("design_id")

        print(f"Project ID: {project_id}")
        print(f"Design ID: {design_id}")

        tenant_id = os.getenv("AURORA_TENANT_ID")

        # Pull design
        design_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}"
        design_response = requests.get(design_url, headers=aurora_headers())

        print("Design pull status:", design_response.status_code)

        # Pull pricing
        pricing_url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"
        pricing_response = requests.get(pricing_url, headers=aurora_headers())

        print("Pricing pull status:", pricing_response.status_code)

    except Exception as e:
        print("ERROR processing webhook:", str(e))

    return {"status": "accepted"}
