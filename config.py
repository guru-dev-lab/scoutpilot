from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # AI
    anthropic_api_key: str = ""

    # Optional APIs
    serpapi_key: str = ""
    rapidapi_key: str = ""

    # Job source API keys (register for free at each provider)
    usajobs_api_key: str = ""       # https://developer.usajobs.gov/APIRequest/Index
    usajobs_email: str = ""         # Email used when registering at USAJobs
    jooble_api_key: str = ""        # https://jooble.org/api/about
    adzuna_app_id: str = ""         # https://developer.adzuna.com/
    adzuna_app_key: str = ""        # https://developer.adzuna.com/
    careerjet_affid: str = ""       # https://www.careerjet.com/partners/api
    findwork_token: str = ""        # https://findwork.dev/developers/

    # Scraping
    scrape_interval_minutes: int = 5

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
