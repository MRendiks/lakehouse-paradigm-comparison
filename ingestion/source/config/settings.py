from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    gcp_project_id: str = "my-project-id"
    environment: str = "development"

    model_config = SettingsConfigDict(env_file=".env")

settings = Settings()
