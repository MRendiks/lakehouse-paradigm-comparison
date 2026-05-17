import logging
from infisical_sdk import InfisicalSDKClient
from source.config.settings import settings

logger = logging.getLogger(__name__)


class InfisicalManager:
    def __init__(self, base_url: str):
        """
        Initialize connection to Infisical.
        Machine Identity credentials should never be hardcoded.
        """
        client_id = settings.INFISICAL_MACHINE_ID
        client_secret = settings.INFISICAL_MACHINE_SECRET
        self.project_id = settings.INFISICAL_PROJECT_ID

        if not client_id or not client_secret or not self.project_id:
            logger.error(
                "Infisical credentials or Project ID not found in environment variables."
            )
            raise ValueError(
                "Environment variables: INFISICAL_MACHINE_ID, INFISICAL_MACHINE_SECRET, or INFISICAL_PROJECT_ID are missing."
            )

        try:
            self.client = InfisicalSDKClient(host=base_url)

            self.client.auth.universal_auth.login(
                client_id=client_id, client_secret=client_secret
            )
            logger.info("Successfully authenticated to Infisical via Universal Auth.")

        except Exception as e:
            logger.error(f"Failed to authenticate to Infisical: {e}")
            raise

    def _get_secret_value(
        self, secret_name: str, environment_slug: str, default: str = None
    ) -> str:
        """Helper method to retrieve a single secret."""
        try:
            secret = self.client.secrets.get_secret_by_name(
                secret_name=secret_name,
                project_id=self.project_id,
                environment_slug=environment_slug,
                secret_path="/",
            )
            return secret.secretValue
        except Exception as e:
            if default is not None:
                logger.warning(
                    f"Secret '{secret_name}' not found, using default value."
                )
                return default
            logger.error(
                f"Secret '{secret_name}' is required but not found in Infisical."
            )
            raise

    def load_app_settings(self, environment_slug: str, settings_obj):
        """
        Fetch configuration from Infisical and update the settings object.
        """
        try:
            logger.info("Fetching configuration from Infisical...")

            settings_obj.KAFKA_BOOTSTRAP_SERVERS = self._get_secret_value(
                secret_name="KAFKA_BOOTSTRAP_SERVERS", environment_slug=environment_slug
            )
            settings_obj.GCP_PROJECT_ID = self._get_secret_value(
                secret_name="GCP_PROJECT_ID",
                environment_slug=environment_slug,
                default="",
            )
            settings_obj.GCP_SERVICE_ACCOUNT_JSON = self._get_secret_value(
                secret_name="GCP_SERVICE_ACCOUNT_JSON",
                environment_slug=environment_slug,
                default="",
            )

            logger.info("Successfully updated app settings from Infisical.")
        except Exception as e:
            logger.error(f"Failed to load app settings from Infisical: {e}")
            raise


def bootstrap_settings(env: str = "dev") -> None:
    """
    Resolve secrets at startup: Infisical first, fallback to .env.

    Strategy:
        1. If INFISICAL_MACHINE_ID, INFISICAL_MACHINE_SECRET, and INFISICAL_PROJECT_ID
           are all set → fetch secrets from Infisical and hydrate `settings`.
        2. Otherwise → skip. `settings` already holds values from .env via pydantic-settings.

    Args:
        env: Infisical environment slug matching the project config ('dev'|'staging'|'prod').
    """
    infisical_ready = all(
        [
            settings.INFISICAL_MACHINE_ID,
            settings.INFISICAL_MACHINE_SECRET,
            settings.INFISICAL_PROJECT_ID,
        ]
    )

    if infisical_ready:
        logger.info(
            f"Infisical credentials detected — fetching secrets for env='{env}'."
        )
        manager = InfisicalManager(base_url=settings.INFISICAL_URL)
        manager.load_app_settings(environment_slug=env, settings_obj=settings)
        print(settings)
    else:
        logger.info(
            "Infisical credentials not set — using values from .env / environment variables."
        )
