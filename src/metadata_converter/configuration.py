# Dummy example for pydantic_settings

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel

class DatabaseSettings(BaseModel):
    host: str
    port: int = 5432

class Settings(BaseSettings):
    app_name: str = "My App"
    db: DatabaseSettings  # Nested settings

    model_config = SettingsConfigDict(
        toml_file="config.toml",  # Load from TOML file
        env_file=".env",           # Optional: Also load from .env
        env_file_encoding="utf-8",
    )