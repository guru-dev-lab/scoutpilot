"""
ScoutPilot — Real-time job intelligence engine.
FastAPI app with background scheduler.
"""

# ──────────────────────────────────────────────
# Build Info — update with each deploy
# ──────────────────────────────────────────────
BUILD_VERSION = "1.0.1"
BUILD_DATE = "2026-04-06"
RECENT_CHANGES = [
    {"version": "1.0.1", "date": "2026-04-06", "status": "active", "change": "Scrape each site individually (Indeed/LinkedIn/Google) so one slow site doesn't block others, 2min timeout, better error logging"},
    {"version": "1.0.0", "date": "2026-04-06", "status": "active", "change": "Reliability — 60s timeout per scrape query (no more hanging), 5-day auto-archive, startup cleanup, reduced to 3 fast sites"},
    {"version": "0.9.9", "date": "2026-03-30", "status": "active", "change": "Fast scrape — 3 profiles per cycle, JobSpy only, fuzzy scoring (no AI calls), heuristic quality checks. Deep sweep handles full AI."},
    {"version": "0.9.7", "date": "2026-03-30", "status": "active", "change": "AI data quality — verifies remote vs hybrid vs onsite from descriptions, strips fake Direct Apply (Easy Apply / Indeed)"},
    {"version": "0.9.5", "date": "2026-03-30", "status": "active", "change": "Search overhaul — keywords searched standalone to find jobs by description, scoring checks descriptions not just titles, best-match scoring across profiles"},
    {"version": "0.9.3", "date": "2026-03-29", "status": "active", "change": "Keyword-powered search — profile keywords (MicroStrategy, Domo, etc.) now generate actual search queries, not just scoring"},
    {"version": "0.9.1", "date": "2026-03-29", "status": "active", "change": "AI engine live — dedup catches near-duplicates, auto-detects direct apply URLs, 5-min scrape interval"},
    {"version": "0.9.0", "date": "2026-03-28", "status": "active", "change": "Visual redesign — premium glass styling for stats and filters, refined search bar"},
    {"version": "0.8.4", "date": "2026-03-28", "status": "active", "change": "Compact layout — Smart Search beside stats, profiles managed in modal only"},
    {"version": "0.8.3", "date": "2026-03-28", "status": "active", "change": "Profile management panel with add/remove in one window"},
    {"version": "0.8.2", "date": "2026-03-28", "status": "active", "change": "Multi-skill filter — search and select multiple skill tags with OR logic"},
]  # Keep only last 5 entries
import asyncio
import logging
import json
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import hashlib
import secrets

import csv
import io

from fastapi import FastAPI, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import settings
from database import (
    init_db, get_jobs, get_job_count, update_job_status,
    update_job_scores, create_profile, get_profiles,
    update_profile, delete_profile, insert_job,
    init_archive_table, cleanup_old_jobs, get_retention_stats,
)
from scraper import run_scrape_cycle, scrape_jobspy
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
_scrape_running = False  # Lock to prevent overlapping cycles


async def scheduled_scrape():
    """Background scrape cycle — with overlap prevention."""
    global last_scrape_result, _scrape_running

    # Skip if a cycle is already running
    if _scrape_running:
        logger.info("[Scrape] Skipping — previous cycle still running")
        return
    _scrape_running = True

    try:
        profiles = await get_profiles()
        if not profiles:
            last_scrape_result = {
                "status": "no_profiles",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            return

        logger.info(f"Starting scheduled scrape for {len(profiles)} profiles...")
        last_scrape_result = {
            "status": "running",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        result = await run_scrape_cycle(profiles)

        # Score new jobs — use FAST fuzzy scoring (no AI calls in regular cycle)
        from database import get_jobs as _get_jobs, get_db as _get_db
        from ai_engine import score_relevance_fuzzy, extract_direct_link_ai
        new_jobs = await _get_jobs(hours=1, status="new", limit=100)

        # Build combined keyword/exclusion lists across all profiles
        all_profiles_data = []
        for profile in profiles:
            kws = profile.get("keywords", [])
            if isinstance(kws, str):
                kws = [k.strip() for k in kws.split(",") if k.strip()]
            excl = profile.get("excluded_keywords", [])
            if isinstance(excl, str):
                excl = [k.strip() for k in excl.split(",") if k.strip()]
            all_profiles_data.append({
                "title": profile["title"],
                "expanded": profile.get("expanded_titles", []),
                "keywords": kws,
                "excluded": excl,
            })

        for job in new_jobs:
            best_relevance = 0
            for pd in all_profiles_data:
                # Fast fuzzy scoring — no API calls, instant
                relevance = score_relevance_fuzzy(
                    job["title"], job.get("description", ""),
                    pd["title"], pd["expanded"],
                    pd["keywords"],
                )
                best_relevance = max(best_relevance, relevance)

            # Trust: use 50 as default (neutral) — deep sweep will refine with AI
            trust = 50
            await update_job_scores(job["id"], best_relevance, trust)

        # AI enhancements for new jobs (capped at 20 per cycle to keep it fast)
        # Light pass — heuristic-only quality checks (NO API calls)
        # Full AI scoring (relevance AI, trust AI, skills, work type, direct links) runs in deep sweep
        if new_jobs:
            from ai_engine import verify_direct_apply_ai
            quality_fixes = 0

            for job in new_jobs[:50]:
                # Heuristic direct apply check only — no Claude API calls
                try:
                    is_direct, clean_url = await verify_direct_apply_ai(
                        job.get("source_url", ""),
                        job.get("direct_apply_url", ""),
                        job.get("source", ""),
                    )
                    corrections = {}
                    if not is_direct and (job.get("direct_apply_url") or job.get("source_url")):
                        corrections["is_direct_apply"] = False
                        corrections["direct_apply_url"] = ""
                    elif is_direct and clean_url:
                        corrections["is_direct_apply"] = True
                        corrections["direct_apply_url"] = clean_url

                    if corrections:
                        db = await _get_db()
                        try:
                            sets = []
                            vals = []
                            for k, v in corrections.items():
                                sets.append(f"{k} = ?")
                                vals.append(v)
                            vals.append(job["id"])
                            await db.execute(
                                f"UPDATE jobs SET {', '.join(sets)} WHERE id = ?",
                                vals,
                            )
                            await db.commit()
                            quality_fixes += 1
                        finally:
                            await db.close()
                except Exception as e:
                    logger.debug(f"[AI Quality] Error for job {job['id']}: {e}")

            if quality_fixes:
                logger.info(f"[Quality] Fixed {quality_fixes} jobs (work type / direct apply corrections)")

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
    finally:
        _scrape_running = False


async def scheduled_deep_sweep():
    """Deep sweep — looks back 7 days to catch jobs missed by regular scrapes.
    Runs every 6 hours. Uses more results per query and wider time window."""
    global last_scrape_result, _scrape_running

    # Don't deep sweep while a regular scrape is running
    if _scrape_running:
        logger.info("[Deep Sweep] Skipping — regular scrape is running")
        return

    _scrape_running = True
    try:
        profiles = await get_profiles()
        if not profiles:
            return

        logger.info(f"[Deep Sweep] Starting 7-day lookback for {len(profiles)} profiles...")
        total_new = 0
        for profile in profiles:
            title = profile["title"]
            expanded = profile.get("expanded_titles", [])
            locations = profile.get("locations", [])
            profile_id = profile["id"]

            search_terms = [title] + [t for t in expanded if t.lower() != title.lower()]

            # Include keywords as standalone search terms for deep sweep too
            keywords = profile.get("keywords", [])
            if isinstance(keywords, str):
                keywords = [k.strip() for k in keywords.split(",") if k.strip()]
            for kw in keywords:
                if kw.lower() not in [s.lower() for s in search_terms]:
                    search_terms.append(kw)

            for term in search_terms[:5]:  # cap terms for deep sweep
                for loc in (locations if locations else [""]):
                    try:
                        new_jobs = await scrape_jobspy(
                            search_term=term,
                            location=loc,
                            results_wanted=50,
                            hours_old=168,  # 7 days
                            profile_id=profile_id,
                        )
                        total_new += len(new_jobs)
                    except Exception as e:
                        logger.error(f"[Deep Sweep] Error: {e}")

        # Score any new finds
        if total_new > 0:
            from database import get_jobs as _get_jobs
            new_jobs = await _get_jobs(hours=1, status="new", limit=200)
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

        logger.info(f"[Deep Sweep] Complete — {total_new} new jobs discovered")
        if total_new > 0:
            last_scrape_result = {
                "status": "ok",
                "new_jobs": total_new,
                "errors": [],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sweep": True,
            }
    except Exception as e:
        logger.error(f"[Deep Sweep] Failed: {e}")
    finally:
        _scrape_running = False


async def scheduled_cleanup():
    """Daily cleanup: archive old jobs and purge ancient archives."""
    try:
        result = await cleanup_old_jobs()
        logger.info(
            f"[Retention] Archived {result['archived']} jobs, "
            f"purged {result['purged']}. "
            f"Active: {result['active_jobs']}, Archived: {result['archived_jobs']}"
        )
    except Exception as e:
        logger.error(f"[Retention] Cleanup failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_archive_table()
    logger.info("Database initialized (with archive table)")

    # Run cleanup on startup to archive stale jobs immediately
    try:
        result = await cleanup_old_jobs()
        logger.info(
            f"[Startup Cleanup] Archived {result['archived']} jobs older than 5 days, "
            f"purged {result['purged']}. Active: {result['active_jobs']}"
        )
    except Exception as e:
        logger.error(f"[Startup Cleanup] Failed: {e}")

    scheduler.add_job(
        scheduled_scrape,
        "interval",
        minutes=settings.scrape_interval_minutes,
        id="scrape_cycle",
        replace_existing=True,
        next_run_time=datetime.now(timezone.utc),  # Run immediately on startup
        max_instances=1,  # Never overlap
        misfire_grace_time=60,  # Skip if missed by > 60s
    )
    scheduler.add_job(
        scheduled_deep_sweep,
        "interval",
        hours=6,
        id="deep_sweep",
        replace_existing=True,
    )
    scheduler.add_job(
        scheduled_cleanup,
        "cron",
        hour=3, minute=0,
        id="daily_cleanup",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started (scrape every {settings.scrape_interval_minutes} min, deep sweep every 6h, cleanup daily at 3 AM)")

    # Reprocess existing jobs to fix direct_apply and posted_at on startup
    asyncio.create_task(_reprocess_existing_jobs())

    yield
    scheduler.shutdown()


app = FastAPI(title="ScoutPilot", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ──────────────────────────────────────────────
# Password Protection
# ──────────────────────────────────────────────

# Generate a random session token on startup (changes each deploy = extra safe)
_AUTH_TOKEN = secrets.token_hex(32)

# Valid session tokens (in-memory, survives for the life of this process)
_valid_sessions: set[str] = set()

# Parse comma-separated passwords (supports multiple users)
_passwords: set[str] = {p.strip() for p in settings.site_password.split(",") if p.strip()}


def _make_session_id() -> str:
    """Create a new random session ID."""
    return secrets.token_hex(24)


class AuthMiddleware(BaseHTTPMiddleware):
    """Block all routes except /login when SITE_PASSWORD is set and user has no session."""

    OPEN_PATHS = {"/login", "/favicon.ico", "/healthz"}

    async def dispatch(self, request: Request, call_next):
        # If no password configured, let everything through
        if not _passwords:
            return await call_next(request)

        path = request.url.path

        # Always allow login page and static assets
        if path in self.OPEN_PATHS or path.startswith("/static"):
            return await call_next(request)

        # Check for valid session cookie
        session_id = request.cookies.get("sp_session")
        if session_id and session_id in _valid_sessions:
            return await call_next(request)

        # Not authenticated — redirect browser requests, block API calls
        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return RedirectResponse("/login", status_code=302)


app.add_middleware(AuthMiddleware)


@app.get("/healthz")
async def healthz():
    """Unauthenticated health check — shows DB path and job count."""
    import os
    from database import get_db, DB_PATH
    info = {"db_path": DB_PATH, "db_exists": os.path.exists(DB_PATH), "version": BUILD_VERSION}
    try:
        db = await get_db()
        row = await db.execute("SELECT COUNT(*) as cnt FROM jobs")
        result = await row.fetchone()
        info["job_count"] = result[0] if result else 0
        await db.close()
    except Exception as e:
        info["db_error"] = str(e)
    # Check if /data/ directory exists and list contents
    try:
        info["data_dir_exists"] = os.path.isdir("/data")
        if info["data_dir_exists"]:
            info["data_dir_contents"] = os.listdir("/data")
    except Exception as e:
        info["data_dir_error"] = str(e)
    return info


LOGIN_PAGE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ScoutPilot — Login</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<style>
  :root { --bg: #0f1117; --surface: #1a1d2e; --border: #2a2d3e; --text: #e2e8f0; --muted: #94a3b8; --accent: #818cf8; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
  .login-box { background: var(--surface); border: 1px solid var(--border); border-radius: 16px; padding: 40px 36px; width: 100%%; max-width: 380px; text-align: center; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
  h1 { font-size: 1.6rem; background: linear-gradient(90deg, #818cf8, #c084fc); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 6px; }
  .sub { color: var(--muted); font-size: 0.85rem; margin-bottom: 24px; }
  input[type=password] { width: 100%%; padding: 12px 16px; border-radius: 10px; border: 1px solid var(--border); background: var(--bg); color: var(--text); font-size: 1rem; outline: none; margin-bottom: 16px; transition: border 0.2s; }
  input[type=password]:focus { border-color: var(--accent); }
  button { width: 100%%; padding: 12px; border-radius: 10px; border: none; background: linear-gradient(135deg, #818cf8, #6366f1); color: #fff; font-size: 1rem; font-weight: 600; cursor: pointer; transition: opacity 0.2s; }
  button:hover { opacity: 0.9; }
  .error { color: #f87171; font-size: 0.8rem; margin-top: 12px; }
  .lock { font-size: 2.5rem; margin-bottom: 12px; }
</style>
</head>
<body>
<div class="login-box">
  <div class="lock">🔒</div>
  <h1>ScoutPilot</h1>
  <p class="sub">Enter the access password to continue</p>
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="Password" autofocus required>
    <button type="submit">Unlock</button>
  </form>
  {error}
</div>
</body>
</html>"""


@app.get("/login", response_class=HTMLResponse)
async def login_page():
    if not _passwords:
        return RedirectResponse("/", status_code=302)
    return HTMLResponse(LOGIN_PAGE_HTML.replace("{error}", ""))


@app.post("/login")
async def login_submit(password: str = Form(...)):
    if not _passwords:
        return RedirectResponse("/", status_code=302)

    if password in _passwords:
        session_id = _make_session_id()
        _valid_sessions.add(session_id)
        response = RedirectResponse("/", status_code=302)
        response.set_cookie(
            "sp_session", session_id,
            httponly=True, secure=True, samesite="lax",
            max_age=60 * 60 * 24 * 30,  # 30 days
        )
        return response

    html = LOGIN_PAGE_HTML.replace("{error}", '<p class="error">Wrong password. Try again.</p>')
    return HTMLResponse(html, status_code=401)


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("sp_session")
    return response


# ──────────────────────────────────────────────
# Dashboard
# ──────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


# ──────────────────────────────────────────────
# Jobs API
# ──────────────────────────────────────────────

@app.get("/api/jobs")
async def api_get_jobs(
    hours: int = Query(24, ge=1, le=720),
    posted_hours: int = Query(0, ge=0, le=720),
    min_relevance: int = Query(0, ge=0, le=100),
    min_trust: int = Query(0, ge=0, le=100),
    source: str = "",
    status: str = "",
    work_type: str = "",
    sort_by: str = "first_seen_at",
    sort_dir: str = "DESC",
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    search: str = "",
    direct_only: str = "",
    location: str = "",
    skill: str = "",
):
    try:
        # When searching, expand time window to search ALL jobs (not just last 24h)
        effective_hours = 720 if search.strip() else hours
        is_direct = direct_only in ("1", "true", "yes")
        jobs = await get_jobs(
            hours=effective_hours, posted_hours=posted_hours,
            min_relevance=min_relevance, min_trust=min_trust,
            source=source, status=status, work_type=work_type,
            sort_by=sort_by, sort_dir=sort_dir,
            limit=limit, offset=offset, search=search,
            direct_only=is_direct, location=location,
            skill=skill,
        )
        stats = await get_job_count(hours)
        return {"jobs": jobs, "stats": stats}
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"API /api/jobs error: {tb}")
        return JSONResponse({"error": str(e), "traceback": tb}, status_code=500)


@app.patch("/api/jobs/{job_id}/status")
async def api_update_status(job_id: int, status: str = "seen"):
    if status not in ("new", "viewed", "applied", "hidden", "saved"):
        return JSONResponse({"error": "Invalid status"}, 400)
    await update_job_status(job_id, status)
    return {"ok": True}


# ──────────────────────────────────────────────
# Search Profiles API
# ──────────────────────────────────────────────

@app.get("/api/profiles")
async def api_get_profiles():
    return await get_profiles()


@app.post("/api/profiles")
async def api_create_profile(request: Request):
    data = await request.json()
    if not data.get("title"):
        return JSONResponse({"error": "Title is required"}, 400)

    # Auto-expand titles with AI
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


# ──────────────────────────────────────────────
# Skills API
# ──────────────────────────────────────────────

@app.get("/api/skills")
async def api_top_skills():
    """Return the top skills across all jobs, sorted by frequency."""
    from database import get_db
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT skills FROM jobs WHERE skills IS NOT NULL AND skills != ''"
        )
        rows = await cursor.fetchall()
        counts: dict[str, int] = {}
        for row in rows:
            for skill in row[0].split(","):
                s = skill.strip()
                if s and s != "_none":
                    counts[s] = counts.get(s, 0) + 1
        # Sort by frequency descending
        top = sorted(counts.items(), key=lambda x: -x[1])
        return [{"skill": s, "count": c} for s, c in top[:50]]
    finally:
        await db.close()


# ──────────────────────────────────────────────
# Export
# ──────────────────────────────────────────────

@app.get("/api/export/csv")
async def api_export_csv(
    hours: int = Query(24, ge=1, le=720),
    search: str = "",
    status: str = "",
    work_type: str = "",
    source: str = "",
    location: str = "",
    direct_only: str = "",
    skill: str = "",
):
    """Export current filtered jobs as CSV."""
    effective_hours = 720 if search.strip() else hours
    is_direct = direct_only in ("1", "true", "yes")
    jobs = await get_jobs(
        hours=effective_hours, search=search, status=status,
        work_type=work_type, source=source, location=location,
        direct_only=is_direct, skill=skill, limit=500,
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Title", "Company", "Location", "Work Type", "Source",
                      "Posted", "Salary Min", "Salary Max", "Direct Apply",
                      "Skills", "Status", "Apply URL"])
    for j in jobs:
        writer.writerow([
            j.get("title", ""), j.get("company_name", ""),
            j.get("location", ""), j.get("work_type", ""),
            j.get("source", ""), j.get("posted_at", ""),
            j.get("salary_min", 0), j.get("salary_max", 0),
            "Yes" if j.get("is_direct_apply") else "No",
            j.get("skills", ""),
            j.get("status", ""),
            j.get("direct_apply_url") or j.get("source_url", ""),
        ])

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=scoutpilot-jobs.csv"},
    )


# ──────────────────────────────────────────────
# Manual Controls
# ──────────────────────────────────────────────

_background_tasks = set()  # prevent GC of background tasks

@app.post("/api/scrape")
async def api_trigger_scrape():
    """Manually trigger a scrape cycle."""
    if _scrape_running:
        return {"status": "already_running", "message": "A scrape cycle is already in progress"}
    task = asyncio.create_task(scheduled_scrape())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return {"status": "started", "message": "Scrape cycle triggered"}


@app.post("/api/reprocess")
async def api_reprocess_jobs():
    """Re-scan existing jobs to fix direct_apply detection and posted_at normalization."""
    asyncio.create_task(_reprocess_existing_jobs())
    return {"status": "started", "message": "Reprocessing jobs in background"}


async def _reprocess_existing_jobs():
    """Fix direct_apply and posted_at for all existing jobs."""
    from scraper import _is_direct_url, _normalize_posted_at
    from database import get_db

    db = await get_db()
    try:
        # Reset all direct_apply flags first so we re-evaluate cleanly
        await db.execute("UPDATE jobs SET is_direct_apply = 0, direct_apply_url = '' WHERE is_direct_apply = 1")
        await db.commit()

        cursor = await db.execute(
            "SELECT id, source_url, direct_apply_url, description, posted_at, is_direct_apply FROM jobs"
        )
        rows = await cursor.fetchall()
        fixed_direct = 0
        fixed_posted = 0

        for row in rows:
            row = dict(row)
            updates = {}

            # Fix direct apply detection — ONLY use structured URL fields
            # Never extract from description (leads to company homepages, not job posts)
            urls = []
            if row["source_url"]:
                urls.append(row["source_url"])

            # Check if any structured URL is a direct company link
            has_direct = False
            best_direct = ""
            for u in urls:
                if _is_direct_url(u):
                    has_direct = True
                    best_direct = u
                    break

            if has_direct and not row["is_direct_apply"]:
                updates["is_direct_apply"] = 1
                updates["direct_apply_url"] = best_direct
                fixed_direct += 1

            # Fix posted_at normalization
            if row["posted_at"]:
                normalized = _normalize_posted_at(row["posted_at"])
                if normalized and normalized != row["posted_at"]:
                    updates["posted_at"] = normalized
                    fixed_posted += 1

            if updates:
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [row["id"]]
                await db.execute(f"UPDATE jobs SET {set_clause} WHERE id = ?", values)

        await db.commit()
        logger.info(f"[Reprocess] Fixed {fixed_direct} direct-apply flags, {fixed_posted} posted_at dates out of {len(rows)} jobs")
    except Exception as e:
        logger.error(f"[Reprocess] Error: {e}")
    finally:
        await db.close()


# ──────────────────────────────────────────────
# Data Retention API
# ──────────────────────────────────────────────

@app.get("/api/retention")
async def api_retention_stats():
    """Get data retention stats (active vs archived jobs, age range)."""
    try:
        stats = await get_retention_stats()
        return stats
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/cleanup")
async def api_trigger_cleanup():
    """Manually trigger cleanup (archive stale + purge ancient)."""
    try:
        result = await cleanup_old_jobs()
        return {"status": "ok", **result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/status")
async def api_status():
    return {
        "scraper": last_scrape_result,
        "interval_minutes": settings.scrape_interval_minutes,
        "has_anthropic_key": bool(settings.anthropic_api_key),
        "has_serpapi_key": bool(settings.serpapi_key),
        "has_rapidapi_key": bool(settings.rapidapi_key),
        "build": {"version": BUILD_VERSION, "date": BUILD_DATE},
        "has_password": bool(_passwords),
    }


@app.get("/api/build")
async def api_build():
    """Build info and recent changelog."""
    return {
        "version": BUILD_VERSION,
        "date": BUILD_DATE,
        "changes": RECENT_CHANGES,
    }


# ──────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
