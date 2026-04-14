"""
ScoutPilot — Real-time job intelligence engine.
FastAPI app with background scheduler.
"""

# ──────────────────────────────────────────────
# Build Info — update with each deploy
# ──────────────────────────────────────────────
BUILD_VERSION = "1.9.2"
BUILD_DATE = "2026-04-13"
RECENT_CHANGES = [
    {"version": "1.9.2", "date": "2026-04-13", "status": "active", "change": "ATS EXPANSION: Added Workday (41 tenants — NVIDIA, Salesforce, Adobe, PayPal, Capital One, Walmart, Target, Boeing, Disney, Intel + more) and SmartRecruiters (Bosch, Visa, Experian, ServiceNow) adapters. Total 206 company boards across 5 ATS platforms. All ATS sources ship disabled by default — enable from Sources panel."},
    {"version": "1.9.1", "date": "2026-04-12", "status": "active", "change": "ATS COMPANY LIST EXPANDED: Mega-probed 300+ candidates → 159 verified (116 Greenhouse, 40 Ashby, 3 Lever). Admin CRUD endpoints for ats-companies."},
    {"version": "1.8.0", "date": "2026-04-12", "status": "active", "change": "SOURCE MANAGEMENT: Enable/disable any of the 14+ job sources from the dashboard. New 'Sources' button in header opens toggle UI. Disabled sources skip scraping entirely. Settings persist in database."},
    {"version": "1.6.0", "date": "2026-04-10", "status": "active", "change": "6 NEW SOURCES: USAJobs (gov engineering/analyst), Jooble (8M+ aggregator), Adzuna (massive aggregator), CareerJet (global), FindWork.dev (tech), JustRemote (RSS). Now 13 sources total. All fire every cycle for every profile."},
    {"version": "1.4.9", "date": "2026-04-10", "status": "active", "change": "Scraper reliability overhaul — JobSpy runs SEQUENTIAL with 3s delays (was hundreds of parallel calls causing IP bans), profiles run sequentially (not parallel), Remotive broad fetch + client filter (server search too strict), Jobicy list crash fixed, limited to 5 JobSpy terms/profile"},
    {"version": "1.4.3", "date": "2026-04-10", "status": "active", "change": "Source fixes verified — Jobicy: removed tag filter (was returning 0), Himalayas: removed q param (irrelevant results), both use broad client-side matching now. Glassdoor removed (403 confirmed). TheMuse 5 pages. Diagnostic endpoint added."},
    {"version": "1.4.2", "date": "2026-04-10", "status": "active", "change": "Source fixes — upgrade JobSpy 1.1.75→1.1.82 (Glassdoor/ZipRecruiter community fixes), Himalayas pagination+search (was requesting 50, API max=20), Jobicy/Himalayas/Arbeitnow throttled to 1 call/profile (was per-term = rate limited), Arbeitnow paginated"},
    {"version": "1.4.1", "date": "2026-04-10", "status": "active", "change": "Show ALL jobs — Work Type default 'All Types' (was 'Remote' hiding 80% of results), archive after 3 days (was 5), purge after 30 days"},
    {"version": "1.4.0", "date": "2026-04-09", "status": "active", "change": "FULL OVERHAUL — scrape EVERYTHING (no remote/onsite filter), all free APIs hit for ALL profiles, AI generates 25+ title variants, WeWorkRemotely RSS added, 50 results/query, 72h search window, 3-day default display, independent profile bots"},
    {"version": "1.3.2", "date": "2026-04-09", "status": "active", "change": "Max freshness — ALL search terms every cycle (not rotating 3), scrape every 5min, smarter status display showing new job counts"},
    {"version": "1.3.1", "date": "2026-04-09", "status": "active", "change": "Live feed — client-side sort, time group headers (Just Now/Today/Yesterday), slide-in animations, warm cards, auto-refresh 45s, scrape every 7min with 60% more results"},
    {"version": "1.3.0", "date": "2026-04-09", "status": "active", "change": "Fix sorting (newest posted first with fallback), restore freshness animations, slash API costs — wider fuzzy gate, shorter prompts, heuristic trust, deep sweep every 12h"},
    {"version": "1.1.0", "date": "2026-04-08", "status": "active", "change": "Smart title expansion — AI generates distinct role families (BI Analyst ≈ Data Analyst ≈ Reporting Analyst etc.), 5 terms/cycle, 15 term rotation, re-expands on every deploy"},
    {"version": "1.0.9", "date": "2026-04-08", "status": "active", "change": "MAX scraping — 7 sources (JobSpy 5 boards + Remotive + RemoteOK + Arbeitnow + TheMuse + SerpApi + JSearch), 3 terms/profile, 25 results, remote default"},
    {"version": "1.0.8", "date": "2026-04-08", "status": "active", "change": "Fix dead page — all AI calls now async (were blocking event loop, freezing API during scoring)"},
    {"version": "1.0.7", "date": "2026-04-07", "status": "active", "change": "Fix empty page — default filter widened to 30 days so existing jobs always show on load."},
    {"version": "1.0.6", "date": "2026-04-07", "status": "active", "change": "Pure AI scoring — no fuzzy gates, AI decides relevance for every new job. Removed background re-score that killed the page."},
    {"version": "1.0.5", "date": "2026-04-07", "status": "active", "change": "AI-powered relevance scoring — Haiku understands Data Analyst ≈ BI Analyst ≈ BI Developer, fuzzy as fast pre-filter"},
    {"version": "1.0.4", "date": "2026-04-07", "status": "active", "change": "Add 'remote' to search queries for remote-only profiles — Data Analyst/BI/Security now specifically search for remote jobs"},
    {"version": "1.0.3", "date": "2026-04-07", "status": "active", "change": "Tighter relevance scoring — keyword boosts only when title matches, default min relevance 85, re-score on startup"},
    {"version": "1.0.2", "date": "2026-04-07", "status": "active", "change": "All 10 profiles scraped every cycle (1 rotating search term each) — no more skipping Data Analyst for 20min"},
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
    init_source_settings, get_source_settings, update_source_setting,
    bulk_update_source_settings,
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

# Ring buffer for scraper logs — last 500 log entries visible at /api/debug/scrape-log
from collections import deque
_scrape_log = deque(maxlen=500)

class ScrapeLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            _scrape_log.append({"ts": record.created, "level": record.levelname, "msg": msg})
        except Exception:
            pass

_slh = ScrapeLogHandler()
_slh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
logging.getLogger("scoutpilot").addHandler(_slh)
logging.getLogger("scraper").addHandler(_slh)


async def scheduled_scrape(cycle_number: int = 1):
    """Background scrape cycle — with overlap prevention and per-source rate limiting."""
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
        result = await run_scrape_cycle(profiles, cycle_number=cycle_number)

        # Score new jobs — AI-powered relevance scoring
        # Uses Haiku for smart matching: understands "Data Analyst" ≈ "BI Analyst" ≈ "BI Developer"
        # Fuzzy runs first as fast pre-filter; AI only called when score is ambiguous (20-85)
        from database import get_jobs as _get_jobs, get_db as _get_db
        from ai_engine import score_relevance_ai, score_relevance_fuzzy, extract_direct_link_ai
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

        ai_scored = 0
        from ai_engine import score_relevance_fuzzy
        for job in new_jobs:
            # STEP 1: Find best-matching profile using FREE fuzzy scoring
            best_fuzzy = 0
            best_profile = all_profiles_data[0] if all_profiles_data else None
            for pd in all_profiles_data:
                fuzzy = score_relevance_fuzzy(
                    job["title"], job.get("description", ""),
                    pd["title"], pd["expanded"], pd["keywords"],
                )
                if fuzzy > best_fuzzy:
                    best_fuzzy = fuzzy
                    best_profile = pd

            # STEP 2: Only call AI for the BEST matching profile (not all 5)
            # Fuzzy pre-filter inside score_relevance_ai handles obvious cases
            if best_profile:
                relevance = await score_relevance_ai(
                    job["title"], job.get("description", ""),
                    best_profile["title"], best_profile["expanded"],
                    best_profile["keywords"], best_profile["excluded"],
                )
            else:
                relevance = best_fuzzy

            trust = 50
            await update_job_scores(job["id"], relevance, trust)
            ai_scored += 1

        if ai_scored:
            logger.info(f"[Scrape] AI-scored {ai_scored} new jobs")

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

        # Score any new finds — best-profile-only + fuzzy gate (same as regular scrape)
        if total_new > 0:
            from database import get_jobs as _get_jobs
            from ai_engine import score_relevance_fuzzy
            new_jobs = await _get_jobs(hours=1, status="new", limit=200)

            # Build profile data once
            all_pd = []
            for profile in profiles:
                kws = profile.get("keywords", [])
                if isinstance(kws, str):
                    kws = [k.strip() for k in kws.split(",") if k.strip()]
                excl = profile.get("excluded_keywords", [])
                if isinstance(excl, str):
                    excl = [k.strip() for k in excl.split(",") if k.strip()]
                all_pd.append({"title": profile["title"], "expanded": profile.get("expanded_titles", []), "keywords": kws, "excluded": excl})

            for job in new_jobs:
                # Find best profile with FREE fuzzy, then AI only for that one
                best_fuzzy = 0
                best_pd = all_pd[0] if all_pd else None
                for pd in all_pd:
                    f = score_relevance_fuzzy(job["title"], job.get("description", ""), pd["title"], pd["expanded"], pd["keywords"])
                    if f > best_fuzzy:
                        best_fuzzy = f
                        best_pd = pd

                if best_pd:
                    relevance = await score_relevance_ai(
                        job["title"], job.get("description", ""),
                        best_pd["title"], best_pd["expanded"],
                        best_pd["keywords"], best_pd["excluded"],
                    )
                else:
                    relevance = best_fuzzy

                trust = 50  # Heuristic trust — no API call
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
    await init_source_settings()
    logger.info("Database initialized (with archive table + source settings)")

    # Run cleanup on startup to archive stale jobs immediately
    try:
        result = await cleanup_old_jobs()
        logger.info(
            f"[Startup Cleanup] Archived {result['archived']} jobs older than 5 days, "
            f"purged {result['purged']}. Active: {result['active_jobs']}"
        )
    except Exception as e:
        logger.error(f"[Startup Cleanup] Failed: {e}")

    # One-time cleanup: remove jobs from old/deleted profiles (v1.8.1)
    try:
        from database import get_db
        db = await get_db()
        active_profiles = await get_profiles()
        active_ids = {p["id"] for p in active_profiles}
        if active_ids:
            placeholders = ",".join("?" for _ in active_ids)
            cursor = await db.execute(
                f"SELECT COUNT(*) FROM jobs WHERE search_profile_id IS NOT NULL AND search_profile_id NOT IN ({placeholders})",
                list(active_ids),
            )
            orphan_count = (await cursor.fetchone())[0]
            if orphan_count > 0:
                await db.execute(
                    f"DELETE FROM jobs WHERE search_profile_id IS NOT NULL AND search_profile_id NOT IN ({placeholders})",
                    list(active_ids),
                )
                await db.commit()
                logger.info(f"[Startup Cleanup] Removed {orphan_count} jobs from old/deleted profiles")
        await db.close()
    except Exception as e:
        logger.error(f"[Startup Cleanup] Old profile cleanup failed: {e}")

    # NOTE: removed startup score inflation (was forcing all jobs to 75)
    # Let real AI/fuzzy scores stand — filter handles visibility

    # Report which optional API keys are configured
    from config import settings as _cfg
    _keys = {
        "USAJOBS_API_KEY": bool(_cfg.usajobs_api_key),
        "JOOBLE_API_KEY": bool(_cfg.jooble_api_key),
        "ADZUNA_APP_ID/KEY": bool(_cfg.adzuna_app_id and _cfg.adzuna_app_key),
        "CAREERJET_AFFID": bool(_cfg.careerjet_affid),
        "FINDWORK_TOKEN": bool(_cfg.findwork_token),
        "SERPAPI_KEY": bool(_cfg.serpapi_key),
        "RAPIDAPI_KEY": bool(_cfg.rapidapi_key),
    }
    configured = [k for k, v in _keys.items() if v]
    missing = [k for k, v in _keys.items() if not v]
    logger.info(f"[Startup] API keys configured: {configured or 'NONE'}")
    if missing:
        logger.warning(f"[Startup] API keys MISSING (sources will be skipped): {missing}")

    # Continuous scrape loop — smart cooldowns based on what sources ran
    # Fast cycles (Remotive/RemoteOK/WWR only): 30s cooldown
    # JobSpy cycles: 90s cooldown (anti-bot needs breathing room)
    # Full cycles (all sources): 120s cooldown
    async def _continuous_scrape_loop():
        cycle_count = 0
        while True:
            cycle_count += 1
            ran_jobspy = (cycle_count % 3 == 1)
            ran_strict = (cycle_count % 5 == 1)
            logger.info(f"[Continuous] Starting cycle #{cycle_count}")
            try:
                await scheduled_scrape(cycle_number=cycle_count)
            except Exception as e:
                logger.error(f"[Continuous] Cycle #{cycle_count} crashed: {e}")
            # Smart cooldown: heavier cycles get more breathing room
            if ran_jobspy and ran_strict:
                cooldown = 120  # Full sweep — give all APIs time to recover
            elif ran_jobspy:
                cooldown = 90   # JobSpy hit — Indeed/LinkedIn need space
            else:
                cooldown = 30   # Fast APIs only — quick turnaround
            logger.info(f"[Continuous] Cycle #{cycle_count} done — {cooldown}s cooldown")
            await asyncio.sleep(cooldown)

    asyncio.create_task(_continuous_scrape_loop())

    # Keep deep sweep and cleanup on scheduler
    scheduler.add_job(
        scheduled_deep_sweep,
        "interval",
        hours=12,
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
    logger.info("[Scheduler] Continuous scrape loop started. Fast cycles every ~30s, JobSpy every 3rd (~90s cooldown), full sweep every 15th (~120s). Deep sweep 12h, cleanup 3AM.")

    # Re-expand ALL profile titles with latest AI prompt (25+ titles per profile)
    asyncio.create_task(_re_expand_profiles())

    # Reprocess existing jobs to fix direct_apply and posted_at on startup
    asyncio.create_task(_reprocess_existing_jobs())

    # One-time fix: clear fake posted_at where it was set to scrape time
    # LinkedIn doesn't give real post dates, so posted_at=first_seen_at is fake
    async def _fix_fake_posted_at():
        from database import get_db
        db = await get_db()
        try:
            result = await db.execute(
                "UPDATE jobs SET posted_at = '' WHERE posted_at = first_seen_at AND posted_at != ''"
            )
            await db.commit()
            logger.info(f"[Startup] Cleared {result.rowcount} fake posted_at timestamps")
        except Exception as e:
            logger.error(f"[Startup] Fix fake posted_at failed: {e}")
        finally:
            await db.close()
    asyncio.create_task(_fix_fake_posted_at())

    # Re-detect work_type for ALL jobs currently tagged as remote
    # Catches false positives from earlier detection logic
    async def _fix_work_types():
        from database import get_db
        from scraper import _detect_work_type
        db = await get_db()
        try:
            cursor = await db.execute(
                "SELECT id, title, location, description, source, work_type "
                "FROM jobs WHERE work_type = 'remote'"
            )
            rows = await cursor.fetchall()
            fixed = 0
            for row in rows:
                new_type = _detect_work_type({
                    "title": row[1] or "",
                    "location": row[2] or "",
                    "description": row[3] or "",
                    "source": row[4] or "",
                })
                if new_type != "remote":
                    await db.execute(
                        "UPDATE jobs SET work_type = ?, is_remote = 0 WHERE id = ?",
                        (new_type, row[0]),
                    )
                    fixed += 1
            await db.commit()
            logger.info(f"[Startup] Re-detected work_type: {fixed}/{len(rows)} jobs changed from remote → onsite/hybrid")
        except Exception as e:
            logger.error(f"[Startup] Fix work_type failed: {e}")
        finally:
            await db.close()
    asyncio.create_task(_fix_work_types())

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

    OPEN_PATHS = {"/login", "/favicon.ico", "/healthz", "/api/test-sources", "/api/debug/scrape-log", "/api/debug/sources", "/api/debug/outbound-ip", "/api/status"}

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


@app.get("/api/test-sources")
async def test_sources():
    """Test each scraper source from the server — returns HTTP status and sample data."""
    import httpx
    results = {}
    headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}

    async with httpx.AsyncClient(timeout=20, headers=headers) as client:
        # 1. Remotive
        try:
            r = await client.get("https://remotive.com/api/remote-jobs", params={"limit": 3})
            jobs = r.json().get("jobs", [])
            results["remotive"] = {"status": r.status_code, "jobs": len(jobs), "sample": jobs[0]["title"] if jobs else None}
        except Exception as e:
            results["remotive"] = {"error": str(e)}

        # 2. RemoteOK
        try:
            r = await client.get("https://remoteok.com/api")
            data = r.json()
            jobs = [j for j in data if isinstance(j, dict) and j.get("position")]
            results["remoteok"] = {"status": r.status_code, "jobs": len(jobs), "sample": jobs[0]["position"] if jobs else None}
        except Exception as e:
            results["remoteok"] = {"error": str(e)}

        # 3. Jobicy
        try:
            r = await client.get("https://jobicy.com/api/v2/remote-jobs", params={"count": 50})
            results["jobicy"] = {"status": r.status_code, "body_preview": r.text[:300]}
            if r.status_code == 200:
                data = r.json()
                jobs = data.get("jobs", [])
                results["jobicy"]["jobs"] = len(jobs)
                if jobs:
                    results["jobicy"]["sample"] = jobs[0].get("jobTitle")
        except Exception as e:
            results["jobicy"] = {"error": str(e)}

        # 4. Himalayas
        try:
            r = await client.get("https://himalayas.app/jobs/api", params={"limit": 20})
            results["himalayas"] = {"status": r.status_code, "body_preview": r.text[:300]}
            if r.status_code == 200:
                data = r.json()
                jobs = data.get("jobs", [])
                results["himalayas"]["jobs"] = len(jobs)
                if jobs:
                    results["himalayas"]["sample"] = jobs[0].get("title")
        except Exception as e:
            results["himalayas"] = {"error": str(e)}

        # 5. Arbeitnow
        try:
            r = await client.get("https://www.arbeitnow.com/api/job-board-api", params={"page": 1})
            results["arbeitnow"] = {"status": r.status_code, "body_preview": r.text[:300]}
            if r.status_code == 200:
                data = r.json()
                items = data.get("data", [])
                results["arbeitnow"]["jobs"] = len(items)
                if items:
                    results["arbeitnow"]["sample"] = items[0].get("title")
        except Exception as e:
            results["arbeitnow"] = {"error": str(e)}

        # 6. TheMuse
        try:
            r = await client.get("https://www.themuse.com/api/public/jobs", params={"page": 1, "category": "Data Science"})
            results["themuse"] = {"status": r.status_code}
            if r.status_code == 200:
                data = r.json()
                jobs = data.get("results", [])
                results["themuse"]["jobs"] = len(jobs)
                if jobs:
                    results["themuse"]["sample"] = jobs[0].get("name")
        except Exception as e:
            results["themuse"] = {"error": str(e)}

        # 7. WeWorkRemotely RSS
        try:
            r = await client.get("https://weworkremotely.com/categories/remote-programming-jobs.rss")
            results["weworkremotely"] = {"status": r.status_code, "content_length": len(r.text)}
        except Exception as e:
            results["weworkremotely"] = {"error": str(e)}

        # 8. Glassdoor (via JobSpy — just test if site is reachable)
        try:
            r = await client.get("https://www.glassdoor.com/", follow_redirects=True)
            results["glassdoor"] = {"status": r.status_code, "reachable": r.status_code < 400}
        except Exception as e:
            results["glassdoor"] = {"error": str(e)}

        # 9. ZipRecruiter
        try:
            r = await client.get("https://www.ziprecruiter.com/", follow_redirects=True)
            results["ziprecruiter"] = {"status": r.status_code, "reachable": r.status_code < 400}
        except Exception as e:
            results["ziprecruiter"] = {"error": str(e)}

    # Also get DB source counts
    try:
        from database import get_db
        db = await get_db()
        rows = await db.execute("""
            SELECT source, COUNT(*) as cnt,
                   MAX(first_seen_at) as newest_seen,
                   SUM(CASE WHEN first_seen_at > datetime('now', '-1 hour') THEN 1 ELSE 0 END) as last_hour
            FROM jobs
            GROUP BY source ORDER BY cnt DESC
        """)
        db_sources = [dict(r) for r in await rows.fetchall()]
        results["_db_source_counts"] = db_sources
    except Exception as e:
        results["_db_error"] = str(e)

    return results


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
    profile: str = "",
):
    try:
        # Parse profile — supports comma-separated IDs for multi-select
        profile_ids = []
        for chunk in profile.split(","):
            chunk = chunk.strip()
            if chunk.isdigit() and int(chunk) > 0:
                profile_ids.append(int(chunk))

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
            skill=skill, profile_ids=profile_ids,
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
    # Only auto-expand if title changed AND no manual expanded_titles provided
    if data.get("title") and "expanded_titles" not in data:
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
# Source Settings (Enable/Disable sources)
# ──────────────────────────────────────────────

@app.get("/api/sources")
async def api_get_sources():
    """Return all source settings with their enabled/disabled status."""
    sources = await get_source_settings()
    return sources


@app.put("/api/sources/{source_key}")
async def api_update_source(source_key: str, request: Request):
    """Enable or disable a single source."""
    data = await request.json()
    enabled = data.get("enabled", True)
    await update_source_setting(source_key, enabled)
    return {"ok": True, "source_key": source_key, "enabled": enabled}


@app.put("/api/sources")
async def api_bulk_update_sources(request: Request):
    """Bulk update source settings. Body: {sources: {source_key: bool, ...}}"""
    data = await request.json()
    settings_map = data.get("sources", {})
    if settings_map:
        await bulk_update_source_settings(settings_map)
    return {"ok": True, "updated": len(settings_map)}


# ──────────────────────────────────────────────
# Debug / Source Stats
# ──────────────────────────────────────────────

@app.get("/api/debug/sources")
async def api_debug_sources():
    """Show job counts per source — helps diagnose which scrapers are working."""
    from database import get_db
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT source, COUNT(*) as cnt, "
            "MIN(first_seen_at) as first_seen, MAX(first_seen_at) as last_seen "
            "FROM jobs GROUP BY source ORDER BY cnt DESC"
        )
        rows = await cursor.fetchall()
        sources = [
            {"source": r[0], "count": r[1], "first_seen": r[2], "last_seen": r[3]}
            for r in rows
        ]
        # Also check last 24h
        cursor2 = await db.execute(
            "SELECT source, COUNT(*) as cnt FROM jobs "
            "WHERE first_seen_at > datetime('now', '-24 hours') "
            "GROUP BY source ORDER BY cnt DESC"
        )
        rows2 = await cursor2.fetchall()
        recent = [{"source": r[0], "count": r[1]} for r in rows2]
        # Work type distribution
        cursor3 = await db.execute(
            "SELECT work_type, COUNT(*) as cnt FROM jobs GROUP BY work_type ORDER BY cnt DESC"
        )
        rows3 = await cursor3.fetchall()
        work_types = [{"work_type": r[0], "count": r[1]} for r in rows3]

        return {"all_time": sources, "last_24h": recent, "work_type_dist": work_types}
    finally:
        await db.close()


@app.get("/api/debug/scrape-log")
async def api_debug_scrape_log(filter: str = "", level: str = ""):
    """Show recent scraper log entries — helps diagnose what each source is doing.
    ?filter=USAJobs,Jooble — show only entries containing these strings
    ?level=WARNING,ERROR — show only these log levels
    """
    entries = list(_scrape_log)
    if filter:
        keywords = [k.strip().lower() for k in filter.split(",") if k.strip()]
        entries = [e for e in entries if any(kw in e["msg"].lower() for kw in keywords)]
    if level:
        levels = [l.strip().upper() for l in level.split(",") if l.strip()]
        entries = [e for e in entries if e["level"] in levels]
    return {"log": entries, "count": len(entries), "total_in_buffer": len(_scrape_log)}


@app.get("/api/debug/outbound-ip")
async def api_debug_outbound_ip():
    """Show the outbound IP address of this Railway instance.
    Use this to whitelist in services like CareerJet that require IP declaration.
    """
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.ipify.org?format=json")
            ip_data = resp.json()
            return {"outbound_ip": ip_data.get("ip", "unknown"), "note": "Railway Hobby plan IPs can change on redeploy. Check after each deploy."}
    except Exception as e:
        return {"error": str(e), "note": "Could not determine outbound IP"}


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
    task = asyncio.create_task(scheduled_scrape(cycle_number=1))  # Manual = full sweep (cycle 1 hits all sources)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return {"status": "started", "message": "Scrape cycle triggered"}


@app.post("/api/reprocess")
async def api_reprocess_jobs():
    """Re-scan existing jobs to fix direct_apply detection and posted_at normalization."""
    asyncio.create_task(_reprocess_existing_jobs())
    return {"status": "started", "message": "Reprocessing jobs in background"}


async def _re_expand_profiles():
    """Re-expand all profile titles with the latest AI prompt on each deploy.
    This ensures search terms stay up-to-date with the best role families."""
    try:
        await asyncio.sleep(5)  # Let the app fully start first
        profiles = await get_profiles()
        for profile in profiles:
            title = profile["title"]
            logger.info(f"[Startup] Re-expanding titles for '{title}'...")
            try:
                expanded = await expand_title_ai(title)
                if expanded and len(expanded) > 3:
                    await update_profile(profile["id"], {"expanded_titles": expanded})
                    logger.info(f"[Startup] '{title}' expanded to {len(expanded)} distinct role names")
                else:
                    logger.info(f"[Startup] '{title}' expansion returned too few results, keeping existing")
            except Exception as e:
                logger.error(f"[Startup] Failed to expand '{title}': {e}")
        logger.info("[Startup] Profile re-expansion complete")
    except Exception as e:
        logger.error(f"[Startup] Profile re-expansion failed: {e}")


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


@app.post("/api/admin/re-expand-titles")
async def api_re_expand_titles():
    """Admin: re-expand all profile titles using the current (tighter) AI prompt."""
    profiles = await get_profiles()
    results = []
    for p in profiles:
        try:
            expanded = await expand_title_ai(p["title"])
            await update_profile(p["id"], {"expanded_titles": expanded})
            results.append({"id": p["id"], "title": p["title"], "count": len(expanded)})
            logger.info(f"[Admin] Re-expanded '{p['title']}' -> {len(expanded)} titles")
        except Exception as e:
            results.append({"id": p["id"], "title": p["title"], "error": str(e)})
    return {"ok": True, "profiles": results}


@app.post("/api/admin/clear-all-jobs")
async def api_clear_all_jobs():
    """Admin: delete ALL jobs so we can start fresh (e.g. after country change)."""
    from database import get_db
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) FROM jobs")
        count = (await cursor.fetchone())[0]
        await db.execute("DELETE FROM jobs")
        await db.commit()
        logger.info(f"[Admin] Cleared all {count} jobs")
        return {"ok": True, "deleted": count}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        await db.close()


@app.get("/api/admin/ats-companies")
async def api_ats_companies_list():
    """List all configured ATS companies (for dashboard UI)."""
    try:
        from ats_scraper import load_companies
        companies = load_companies()
        from collections import Counter
        by_ats = dict(Counter(c.get("ats", "unknown") for c in companies))
        return {"ok": True, "count": len(companies), "by_ats": by_ats, "companies": companies}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/ats-companies")
async def api_ats_companies_add(request: Request):
    """Add a company to the ATS list. Body: {name, slug, ats}."""
    try:
        from ats_scraper import load_companies, save_companies
        body = await request.json()
        name = (body.get("name") or "").strip()
        slug = (body.get("slug") or "").strip()
        ats = (body.get("ats") or "").strip().lower()
        if not name or not slug or ats not in ("greenhouse", "lever", "ashby"):
            return JSONResponse(
                {"error": "name, slug, and ats (greenhouse|lever|ashby) are required"},
                status_code=400,
            )
        companies = load_companies()
        # Dedupe by (ats, slug)
        if any(c.get("ats") == ats and c.get("slug") == slug for c in companies):
            return JSONResponse({"error": f"{ats}/{slug} already exists"}, status_code=409)
        companies.append({"name": name, "slug": slug, "ats": ats})
        companies.sort(key=lambda c: (c.get("ats", ""), c.get("name", "")))
        if not save_companies(companies):
            return JSONResponse({"error": "failed to save"}, status_code=500)
        logger.info(f"[Admin] Added ATS company: {ats}/{slug} ({name})")
        return {"ok": True, "count": len(companies)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/admin/ats-companies/{ats}/{slug}")
async def api_ats_companies_delete(ats: str, slug: str):
    """Remove a company from the ATS list."""
    try:
        from ats_scraper import load_companies, save_companies
        companies = load_companies()
        before = len(companies)
        companies = [c for c in companies if not (c.get("ats") == ats and c.get("slug") == slug)]
        if len(companies) == before:
            return JSONResponse({"error": "not found"}, status_code=404)
        if not save_companies(companies):
            return JSONResponse({"error": "failed to save"}, status_code=500)
        logger.info(f"[Admin] Removed ATS company: {ats}/{slug}")
        return {"ok": True, "count": len(companies)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/status")
async def api_status():
    return {
        "scraper": last_scrape_result,
        "mode": "continuous",
        "cooldown_seconds": 60,
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
