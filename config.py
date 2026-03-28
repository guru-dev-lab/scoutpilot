from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # AI
    anthropic_api_key: str = ""

    # Optional APIs
    serpapi_key: str = ""
    rapidapi_key: str = ""

    # Scraping
    scrape_interval_minutes: int = 10

    # Database
    database_path: str = "/data/scoutpilot.db"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Site access
    site_password: str = ""  # Set to require password; empty = open access

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
