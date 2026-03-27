"""
ScoutPilot — Real-time job intelligence engine.
FastAPI app with background scheduler.
"""
import asyncio
import logging
import json
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("scoutpilot")

scheduler = AsyncIOScheduler()
last_scrape_result = {"status": "idle", "timestamp": None}


async def scheduled_scrape():
    global last_scrape_result
    try:
        profiles = await get_profiles()
        if not profiles:
            last_scrape_result = {"status": "no_profiles", "timestamp": datetime.now(timezone.utc).isoformat()}
            return

        logger.info(f"Starting scheduled scrape for {len(profiles)} profiles...")
        result = await run_scrape_cycle(profiles)

        from database import get_jobs as _get_jobs
        new_jobs = await _get_jobs(hours=1, status="new", limit=100)
        for job in new_jobs:
            for profile in profiles:
                relevance = await score_relevance_ai(job["title"], job.get("description", ""), profile["title"], profile.get("expanded_titles", []), profile.get("keywords", []), profile.get("excluded_keywords", []))
                trust = await score_trust_ai(job["title"], job.get("company_name", ""), job.get("description", ""), job.get("salary_min", 0), job.get("salary_max", 0), job.get("company_domain", ""), job.get("source", ""))
                await update_job_scores(job["id"], relevance, trust)

        last_scrape_result = {"status": "ok", "new_jobs": result["new_jobs"], "errors": result.get("errors", []), "timestamp": datetime.now(timezone.utc).isoformat()}
        logger.info(f"Scrape complete: {result['new_jobs']} new jobs")
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}")
        last_scrape_result = {"status": "error", "error": str(e), "timestamp": datetime.now(timezone.utc).isoformat()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("Database initialized")
    scheduler.add_job(scheduled_scrape, "interval", minutes=settings.scrape_interval_minutes, id="scrape_cycle", replace_existing=True)
    scheduler.start()
    logger.info(f"Scheduler started (every {settings.scrape_interval_minutes} min)")
    yield
    scheduler.shutdown()


app = FastAPI(title="ScoutPilot", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/api/jobs")
async def api_get_jobs(hours: int = Query(24, ge=1, le=720), min_relevance: int = Query(0, ge=0, le=100), min_trust: int = Query(0, ge=0, le=100), source: str = "", status: str = "", sort_by: str = "first_seen_at", sort_dir: str = "DESC", limit: int = Query(200, ge=1, le=500), offset: int = Query(0, ge=0), search: str = ""):
    jobs = await get_jobs(hours=hours, min_relevance=min_relevance, min_trust=min_trust, source=source, status=status, sort_by=sort_by, sort_dir=sort_dir, limit=limit, offset=offset, search=search)
    stats = await get_job_count(hours)
    return {"jobs": jobs, "stats": stats}


@app.patch("/api/jobs/{job_id}/status")
async def api_update_status(job_id: int, status: str = "seen"):
    if status not in ("new", "seen", "applied", "hidden"): return JSONResponse({"error": "Invalid status"}, 400)
    await update_job_status(job_id, status)
    return {"ok": True}


@app.get("/api/profiles")
async def api_get_profiles():
    return await get_profiles()


@app.post("/api/profiles")
async def api_create_profile(request: Request):
    data = await request.json()
    if not data.get("title"): return JSONResponse({"error": "Title is required"}, 400)
    expanded = await expand_title_ai(data["title"])
    data["expanded_titles"] = expanded
    profile_id = await create_profile(data)
    return {"id": profile_id, "expanded_titles": expanded}


@app.put("/api/profiles/{profile_id}")
async def api_update_profile(profile_id: int, request: Request):
    data = await request.json()
    if data.get("title"):
        expanded = await expand_title_ai(data["title"])
        data["expanded_titles"] = expanded
    await update_profile(profile_id, data)
    return {"ok": True}


@app.delete("/api/profiles/{profile_id}")
async def api_delete_profile(profile_id: int):
    await delete_profile(profile_id)
    return {"ok": True}


@app.post("/api/scrape")
async def api_trigger_scrape():
    asyncio.create_task(scheduled_scrape())
    return {"status": "started", "message": "Scrape cycle triggered"}


@app.get("/api/status")
async def api_status():
    return {"scraper": last_scrape_result, "interval_minutes": settings.scrape_interval_minutes, "has_anthropic_key": bool(settings.anthropic_api_key), "has_serpapi_key": bool(settings.serpapi_key), "has_rapidapi_key": bool(settings.rapidapi_key)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
