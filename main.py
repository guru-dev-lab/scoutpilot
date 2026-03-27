"""
ScoutPilot — Real-time job intelligence engine.
FastAPI app with background scheduler.
"""
import asyncio
import logging
import json
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import settings
from database import (
    init_db, get_jobs, get_job_count, update_job_status,
    update_job_scores, create_profile, get_profiles,
    update_profile, delete_profile, insert_job,
)
from scraper import run_scrape_cycle
from ai_engine import expand_title_ai, score_relevance_ai, score_trust_ai

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scoutpilot")

# Scheduler
scheduler = AsyncIOScheduler()
last_scrape_result = {"status": "idle", "timestamp": None}


async def scheduled_scrape():
    """Background scrape cycle."""
    global last_scrape_result
    try:
        profiles = await get_profiles()
        if not profiles:
            last_scrape_result = {
                "status": "no_profiles",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return

        logger.info(f"Starting scheduled scrape for {len(profiles)} profiles...")
        result = await run_scrape_cycle(profiles)

        # Score new jobs
        from database import get_jobs as _get_jobs
        new_jobs = await _get_jobs(hours=1, status="new", limit=100)
        for job in new_jobs:
            for profile in profiles:
                relevance = await score_relevance_ai(
                    job["title"], job.get("description", ""),
                    profile["title"], profile.get("expanded_titles", []),
                    profile.get("keywords", []), profile.get("excluded_keywords", []),
                )
                trust = await score_trust_ai(
                    job["title"], job.get("company_name", ""),
                    job.get("description", ""), job.get("salary_min", 0),
                    job.get("salary_max", 0), job.get("company_domain", ""),
                    job.get("source", ""),
                )
                await update_job_scores(job["id"], relevance, trust)

        last_scrape_result = {
            "status": "ok",
            "new_jobs": result["new_jobs"],
            "errors": result.get("errors", []),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(f"Scrape complete: {result['new_jobs']} new jobs")
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}")
        last_scrape_result = {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialized")

    scheduler.add_job(
        scheduled_scrape,
        "interval",
        minutes=settings.scrape_interval_minutes,
        id="scrape_cycle",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started (every {settings.scrape_interval_minutes} min)")
    yield
    scheduler.shutdown()


app = FastAPI(title="ScoutPilot", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
