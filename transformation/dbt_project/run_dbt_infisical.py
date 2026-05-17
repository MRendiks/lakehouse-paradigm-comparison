#!/usr/bin/env python
import os
import sys
import subprocess
import logging
from dotenv import load_dotenv
from infisical_sdk import InfisicalSDKClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("infisical_dbt_runner")

def run():
    # 1. Load Infisical credentials from ingestion/.env if not already in system environment
    parent_env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ingestion/.env"))
    if os.path.exists(parent_env_path):
        logger.info(f"Loading Infisical machine credentials from {parent_env_path}")
        load_dotenv(parent_env_path)
    
    project_id = os.getenv("INFISICAL_PROJECT_ID")
    machine_id = os.getenv("INFISICAL_MACHINE_ID")
    machine_secret = os.getenv("INFISICAL_MACHINE_SECRET")
    base_url = os.getenv("INFISICAL_URL", "https://app.infisical.com")
    
    if not all([project_id, machine_id, machine_secret]):
        logger.error("Missing required Infisical credentials: INFISICAL_PROJECT_ID, INFISICAL_MACHINE_ID, or INFISICAL_MACHINE_SECRET.")
        sys.exit(1)
        
    # 2. Authenticate with Infisical
    logger.info(f"Connecting to Infisical at {base_url}...")
    try:
        client = InfisicalSDKClient(host=base_url)
        client.auth.universal_auth.login(client_id=machine_id, client_secret=machine_secret)
        logger.info("Successfully authenticated via Universal Auth.")
    except Exception as e:
        logger.error(f"Failed to authenticate with Infisical: {e}")
        sys.exit(1)

    # 3. Retrieve Snowflake secrets
    env_slug = os.getenv("APP_ENV", "dev")
    logger.info(f"Fetching Snowflake secrets from project {project_id} (env: '{env_slug}')...")
    
    secrets_to_fetch = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    env_vars = os.environ.copy()
    
    for secret_name in secrets_to_fetch:
        try:
            secret = client.secrets.get_secret_by_name(
                secret_name=secret_name,
                project_id=project_id,
                environment_slug=env_slug,
                secret_path="/"
            )
            env_vars[secret_name] = secret.secretValue
        except Exception as e:
            logger.error(f"Failed to fetch secret '{secret_name}' from Infisical: {e}")
            sys.exit(1)
            
    logger.info("Successfully loaded Snowflake credentials into runtime environment.")

    # 4. Construct and execute the dbt command
    dbt_args = sys.argv[1:]
    if not dbt_args:
        dbt_args = ["run"]  # Default to run if no command provided
        
    dbt_command = ["dbt"] + dbt_args
    logger.info(f"Executing dbt command: {' '.join(dbt_command)}")
    
    try:
        # Use shell=True on Windows for command accessibility
        result = subprocess.run(dbt_command, env=env_vars, shell=True)
        sys.exit(result.returncode)
    except Exception as e:
        logger.error(f"Failed to run dbt subprocess: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
