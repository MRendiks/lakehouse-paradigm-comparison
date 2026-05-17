import os
import json
import requests

def main():
    db_file = r"D:\project\lakehouse-paradigm-comparison\resources\token\databricks.json"
    gcp_file = r"D:\project\lakehouse-paradigm-comparison\resources\token\token_gcs.json"

    with open(db_file, "r") as f:
        db_config = json.load(f)

    token = db_config["databricks_token"]
    url = db_config["databricks_url"].rstrip("/")

    with open(gcp_file, "r") as f:
        gcp_sa = f.read()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 1. Create Scope
    print("Creating scope 'gcp_secrets'...")
    scope_data = {"scope": "gcp_secrets", "initial_manage_principal": "users"}
    r1 = requests.post(f"{url}/api/2.0/secrets/scopes/create", headers=headers, json=scope_data)
    if r1.status_code == 200:
        print("[OK] Scope created successfully.")
    elif "RESOURCE_ALREADY_EXISTS" in r1.text:
        print("[OK] Scope already exists.")
    else:
        print("[ERROR] Failed to create scope:", r1.text)

    # 2. Put Secret
    print("Uploading secret 'gcp_sa_key'...")
    secret_data = {
        "scope": "gcp_secrets",
        "key": "gcp_sa_key",
        "string_value": gcp_sa
    }
    r2 = requests.post(f"{url}/api/2.0/secrets/put", headers=headers, json=secret_data)
    if r2.status_code == 200:
        print("[OK] Secret uploaded successfully!")
    else:
        print("[ERROR] Failed to upload secret:", r2.text)

if __name__ == "__main__":
    main()
