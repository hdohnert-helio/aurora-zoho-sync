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
    headers = {
        "Authorization": f"Bearer {os.getenv('AURORA_API_KEY')}",
        "X-Aurora-Tenant-Id": os.getenv("AURORA_TENANT_ID"),
        "Content-Type": "application/json"
    }

    url = "https://api.aurorasolar.com/projects?limit=1"

    response = requests.get(url, headers=headers)

    return {
        "status_code": response.status_code,
        "response": response.json()
    }