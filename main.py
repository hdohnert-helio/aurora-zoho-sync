from fastapi import FastAPI
import os
import requests

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
    response_json = response.json()

    return response_json.get("access_token")


# ------------------------
# Test Zoho Connection
# ------------------------
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
# ------------------------
# Test Aurora Connection
# ------------------------
@app.get("/aurora/test")
def test_aurora():
    tenant_id = os.getenv("AURORA_TENANT_ID")

    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }
@app.get("/aurora/project/{project_id}")
def get_project(project_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")

    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }
@app.get("/aurora/designs/{project_id}")
def get_designs(project_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")

    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/projects/{project_id}/designs"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }

@app.get("/aurora/design/{design_id}")
def get_design(design_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")

    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }

@app.get("/aurora/design/{design_id}/pricing")
def get_design_pricing(design_id: str):
    tenant_id = os.getenv("AURORA_TENANT_ID")

    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "Content-Type": "application/json"
    }

    url = f"https://api.aurorasolar.com/tenants/{tenant_id}/designs/{design_id}/pricing"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }


from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
import os
import requests

app = FastAPI()


# ------------------------
# Aurora Webhook Endpoint (TEST ONLY)
# ------------------------
@app.post("/webhook/aurora")
async def aurora_webhook(request: Request):
    # Validate header authentication
    expected_secret = os.getenv("AURORA_WEBHOOK_SECRET")
    received_secret = request.headers.get("X-Aurora-Webhook-Secret")

    if received_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Capture query parameters
    params = dict(request.query_params)

    print("Webhook received:")
    print(params)

    return {"status": "accepted"}