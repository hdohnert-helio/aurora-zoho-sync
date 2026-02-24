from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "Aurora-Zoho Sync Service Running"}

@app.get("/test-env")
def test_env():
    return {
        "zoho_client_id_exists": bool(os.getenv("1000.IR5R4GI02T86RYPYQ2LAC1KVNVH1WF")),
        "aurora_api_key_exists": bool(os.getenv("sk_prod_4ff27baddd6bfbc1b2d02167"))
    }