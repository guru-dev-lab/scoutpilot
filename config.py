from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # AI
    anthropic_api_key: str = ""

    # Xhire Suite SSO — shared HS256 secret with the Xhire app (env JWT_SECRET).
    # When set, a valid Xhire JWT is accepted as an alternative to the password.
    # Empty = feature off (password-only, unchanged behavior).
    jwt_secret: str = ""

    # Xhire app base URL — used by the "Prep for this job" loop to create an
    # interview-prep session from a listing.
    xhire_base_url: str = "https://xhire.app"

    # Optional APIs
    serpapi_key: str = ""
    rapidapi_key: str = ""

    # Residential rotating proxy, e.g.
    #   http://USER__cr.us:PASS@gw.dataimpulse.com:823
    # Railway runs on datacenter IPs, which LinkedIn blocks outright — measured
    # 1 LinkedIn job ever. Through a US residential IP the same guest endpoint
    # returns 70 cards per request. Empty = no proxy, direct connection.
    #
    # Deliberately NOT applied to every source: Indeed already works direct
    # (JobSpy uses its internal API) and returns 403 + CAPTCHA through the
    # proxy, and DataImpulse blocks all .gov domains, which would kill USAJobs.
    proxy_url: str = ""

    # Job source API keys (register for free at each provider)
    usajobs_api_key: str = ""       # https://developer.usajobs.gov/APIRequest/Index
    usajobs_email: str = ""         # Email used when registering at USAJobs
    jooble_api_key: str = ""        # https://jooble.org/api/about
    adzuna_app_id: str = ""         # https://developer.adzuna.com/
    adzuna_app_key: str = ""        # https://developer.adzuna.com/
    careerjet_affid: str = ""       # https://www.careerjet.com/partners/api
    findwork_token: str = ""        # https://findwork.dev/developers/

    # US-only extraction: drop jobs whose location is clearly non-US (applied to
    # every source in insert_job). Set false to allow worldwide.
    us_only: bool = True

    # Relevance: jobs the AI classifier scores below this are hidden from the
    # board (clearly-wrong roles). Reversible — they're set status='hidden', not deleted.
    # Jobs scoring below this are marked hidden and vanish from the feed.
    #
    # History: 40 -> 25 on Aug 28, on the reasoning that the role-family fence
    # already hard-caps genuine mismatches at 22, so a 25+ row had passed the
    # fence and was plausible. That reasoning leaned on a fence that was leakier
    # than I thought — v2.32.0 found "Criminal Intelligence Analyst" scoring 100,
    # never mind 25. With the gate now requiring a whole role identity, the
    # owner's call (2026-09-02) is to stop spending attention on the weak band at
    # all: "the job board result is aweful". 50 is the floor for a solid match.
    # One number, reversible — the startup backfill re-applies it in both
    # directions on the next boot.
    relevance_hide_below: int = 50

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
