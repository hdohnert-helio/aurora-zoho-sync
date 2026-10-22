"""
Probe Aurora's financings API to discover what LightReach metadata is exposed.

Goal: given a project (or design) we know was sent to LightReach via Aurora's
TPO integration, fetch every financing on every design and dump the full
response. The output reveals the actual field names that hold the LightReach
account ID / reference / quote ID / contract ID, so we can write the matcher
against real keys instead of guesses.

Run locally with the same env vars `aurora-zoho-sync` already uses:
    AURORA_API_KEY     – bearer token
    AURORA_TENANT_ID   – tenant uuid

Examples:
    python probe_aurora_financings.py --project-id <aurora_project_uuid>
    python probe_aurora_financings.py --design-id  <aurora_design_uuid>

Pick a customer whose LightReach webhook you've already seen in Render logs
so you can cross-reference the LightReach accountId we already know
(e.g. 68adfefb7c9de440576cfdea or 69d9065c445cb63fd5583f3f) against
whatever Aurora returns.
"""

import argparse
import json
import os
import sys
from typing import Optional

import requests

AURORA_BASE = "https://api.aurorasolar.com"


def aurora_headers() -> dict:
    api_key = os.getenv("AURORA_API_KEY")
    if not api_key:
        sys.exit("AURORA_API_KEY is not set in env")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def get_tenant_id() -> str:
    tenant = os.getenv("AURORA_TENANT_ID")
    if not tenant:
        sys.exit("AURORA_TENANT_ID is not set in env")
    return tenant


def get_json(url: str) -> Optional[dict]:
    """GET a URL with Aurora auth, return parsed JSON or None on 4xx/5xx."""
    print(f"\n→ GET {url}")
    resp = requests.get(url, headers=aurora_headers(), timeout=30)
    print(f"  status: {resp.status_code}")
    if resp.status_code >= 400:
        print(f"  body:   {resp.text[:500]}")
        return None
    try:
        return resp.json()
    except ValueError:
        print(f"  non-JSON body: {resp.text[:500]}")
        return None


def list_designs_for_project(tenant_id: str, project_id: str) -> list:
    """List every design under a project."""
    url = f"{AURORA_BASE}/tenants/{tenant_id}/projects/{project_id}/designs"
    data = get_json(url)
    if not data:
        return []
    # Aurora returns either {"designs": [...]} or a top-level list depending on the route.
    if isinstance(data, dict):
        return data.get("designs") or data.get("data") or []
    return data if isinstance(data, list) else []


def list_financings_for_design(tenant_id: str, design_id: str) -> list:
    """List every financing on a design."""
    url = f"{AURORA_BASE}/tenants/{tenant_id}/designs/{design_id}/financings"
    data = get_json(url)
    if not data:
        return []
    if isinstance(data, dict):
        return data.get("financings") or data.get("data") or []
    return data if isinstance(data, list) else []


def retrieve_financing(tenant_id: str, design_id: str, financing_id: str) -> Optional[dict]:
    """Retrieve the full financing record."""
    url = f"{AURORA_BASE}/tenants/{tenant_id}/designs/{design_id}/financings/{financing_id}"
    return get_json(url)


def dump(label: str, payload) -> None:
    print(f"\n========== {label} ==========")
    print(json.dumps(payload, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Aurora financings for LightReach IDs.")
    parser.add_argument("--project-id", help="Aurora project UUID")
    parser.add_argument("--design-id", help="Aurora design UUID")
    args = parser.parse_args()

    if not args.project_id and not args.design_id:
        parser.error("supply --project-id or --design-id")

    tenant_id = get_tenant_id()

    # Collect design IDs to inspect.
    design_ids: list[str] = []
    if args.design_id:
        design_ids.append(args.design_id)
    else:
        designs = list_designs_for_project(tenant_id, args.project_id)
        if not designs:
            sys.exit(f"No designs returned for project {args.project_id}")
        dump(f"designs (count={len(designs)})", designs)
        for d in designs:
            d_id = d.get("id") if isinstance(d, dict) else None
            if d_id:
                design_ids.append(d_id)

    # For each design, list and retrieve every financing.
    findings: list[dict] = []
    for d_id in design_ids:
        financings = list_financings_for_design(tenant_id, d_id)
        if not financings:
            print(f"\n(no financings on design {d_id})")
            continue
        dump(f"design {d_id} financings list (count={len(financings)})", financings)

        for f in financings:
            f_id = f.get("id") if isinstance(f, dict) else None
            if not f_id:
                continue
            full = retrieve_financing(tenant_id, d_id, f_id)
            if full:
                dump(f"design {d_id} / financing {f_id} (full)", full)
                findings.append({"design_id": d_id, "financing_id": f_id, "record": full})

    # Heuristic scan for anything that looks like a LightReach ID.
    print("\n========== heuristic ID scan ==========")
    targets = ("lightreach", "light_reach", "external", "provider", "account",
               "reference", "quote", "contract", "financier", "tpo")
    hits: list[tuple[str, str]] = []

    def walk(prefix: str, node) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                key_l = k.lower()
                path = f"{prefix}.{k}" if prefix else k
                if any(t in key_l for t in targets):
                    hits.append((path, repr(v)[:200]))
                walk(path, v)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(f"{prefix}[{i}]", v)

    for f in findings:
        walk("", f["record"])

    if hits:
        print(f"\nFound {len(hits)} candidate field(s):")
        for path, val in hits:
            print(f"  {path} = {val}")
    else:
        print("\nNo obvious LightReach-shaped fields found in retrieved records.")
        print("Check the dumps above manually — the relevant key may use unusual naming.")


if __name__ == "__main__":
    main()
