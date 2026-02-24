from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "Aurora-Zoho Sync Service Running"}

@app.get("/debug-env")
def debug_env():
    return {
        "all_env_keys": list(os.environ.keys())
    }

@app.get("/test-env")
def test_env():
    return {
        "zoho_client_id_exists": os.getenv("ZOHO_CLIENT_ID"),
        "aurora_api_key_exists": os.getenv("AURORA_API_KEY")
    }