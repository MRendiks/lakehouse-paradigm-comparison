import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str
    gcp_project_id: str
    environment: str

    infisical_project_id: Optional[str] = None
    infisical_client_id: Optional[str] = None
    infisical_client_secret: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    def load_from_infisical(self):
        """Fetch secrets dynamically from Infisical Cloud and override local values."""
        if not (
            self.infisical_client_id
            and self.infisical_client_secret
            and self.infisical_project_id
        ):
            print(
                "INFO: Infisical credentials not complete in .env, using local configuration."
            )
            return

        try:
            from infisical_sdk import InfisicalSDKClient

            client = InfisicalSDKClient(host="https://app.infisical.com")

            client.auth.universal_auth.login(
                client_id=self.infisical_client_id,
                client_secret=self.infisical_client_secret,
            )

            def get_secret(name: str) -> str:
                secret = client.secrets.get_secret_by_name(
                    secret_name=name,
                    project_id=self.infisical_project_id,
                    environment_slug=self.environment,
                    secret_path="/",
                )
                return secret.secretValue

            try:
                self.kafka_bootstrap_servers = get_secret("KAFKA_BOOTSTRAP_SERVERS")
            except Exception:
                pass

            try:
                self.gcp_project_id = get_secret("GCP_PROJECT_ID")
            except Exception:
                pass

            print("Success load secret from Infisical Cloud.")

        except Exception as e:
            print(f"Warning: Failed load secret from Infisical Cloud: {e}")


settings = Settings()
settings.load_from_infisical()
