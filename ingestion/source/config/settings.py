from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # ── Connection ──────────────────────────────────────────────────────────────
    INFISICAL_URL: str = "https://app.infisical.com"
    INFISICAL_MACHINE_ID: str = ""
    INFISICAL_MACHINE_SECRET: str = ""
    INFISICAL_PROJECT_ID: str = ""

    # app env
    APP_ENV: str = "dev"

    # ── Variables (Fetched dynamically from Infisical) ─────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = ""
    GCP_PROJECT_ID: str = ""
    GCP_SERVICE_ACCOUNT_JSON: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
