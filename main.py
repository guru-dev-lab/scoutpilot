"""
ScoutPilot — Real-time job intelligence engine.
FastAPI app with background scheduler.
"""

# ──────────────────────────────────────────────
# Build Info — update with each deploy
# ──────────────────────────────────────────────
# Score at or below which score_relevance_fuzzy's role-family fence has
# rejected a job outright — different discipline, and no skill-signature
# evidence in the description to rescue it. Mirrors the cap inside
# ai_engine.score_relevance_fuzzy.
_FAMILY_FENCE_CAP = 22

# Only jobs whose fuzzy score lands between these bounds are worth an AI call.
# Outside the band the answer is already settled: below the low bound the
# role-family fence has rejected it, above the high bound the title is an
# unambiguous match. Mirrors the same gate score_relevance_ai() applies.
_AI_BAND_LOW = 25
_AI_BAND_HIGH = 75

BUILD_VERSION = "2.26.0"
BUILD_DATE = "2026-08-27"
RECENT_CHANGES = [
    {"version": "1.9.6", "date": "2026-04-13", "status": "active", "change": "SKILL SIGNATURE — description-based rescue for disguised roles. Plus on top of the family fence, not a replacement. Each profile now gets a one-time AI-generated 'skill signature' (foundation skills + toolkit + bonus signals) cached forever in the DB. At runtime, when the family fence would hard-cap a job at 22, the scorer first walks the JOB DESCRIPTION (zero AI cost) looking for signature matches. If it finds enough — e.g. SQL + Tableau + dashboards + KPIs in a Solutions Engineer description — it overrides the fence with a 60-100 score. This rescues legit-but-disguised roles: Solutions Engineer that's really a DA, Product Analyst that's really a DA, Business Systems Analyst + EDW, Growth Specialist with SQL/Looker. Built-in fallback signatures for 9 common roles (Data Analyst, BI Analyst, Data Engineer, Data Scientist, Software Engineer, DevOps, Security, Product Manager, UX Designer) so rescue works even before AI generates a custom one. New POST /api/admin/generate-signatures backfills existing profiles. Total cost: 1 AI call per profile (one-time), 0 AI calls per job. Direct mismatches (SWE / Web Dev / Marketing for a DA profile) still get capped at 22."},
    {"version": "1.9.5", "date": "2026-04-13", "status": "active", "change": "RELEVANCE HARDENING: Kills the 'Data Analyst filter showing QA Engineer / Web Developer / Marketing' class of leak. Three fixes. (1) AI title-expansion prompt is now STRICT — it forbids generic single-word variants (Developer, Engineer, Manager, Analyst, Designer, Specialist…) and cross-family matches (Data Analyst ≠ Software Engineer, UX Designer ≠ Frontend Dev). (2) New role-family fence in the fuzzy scorer — jobs whose title clearly belongs to a different family than the target are hard-capped at 22 regardless of keyword overlap. Families: data_analytics, data_engineering, data_science, software_engineering, devops_platform, security, design, product, marketing, sales_cs, qa, finance, hr, support. (3) Keywords (Python, SQL, Tableau, AWS) are NO LONGER sent to scrapers as standalone search queries — they bring back noisy SWE/QA/Marketing jobs that merely mention those tools. Keywords still count for relevance scoring. Plus partial_ratio only runs for multi-token targets ≥ 12 chars; old polluted expansions are sanitized on load; int() return for type safety. New POST /api/admin/rescore-all-jobs and /api/admin/re-expand-titles flush the existing noise."},
    {"version": "1.9.4", "date": "2026-04-13", "status": "active", "change": "THREE-PATH AUTO-DISCOVERY: Discovery now runs three paths in order. (1) URL extraction from direct_apply_url/source_url — now also captures JobSpy's job_url_direct field, which is the real employer ATS link for Indeed rows (JobSpy already resolved it during its scrape, we just weren't reading it). (2) HTML second-link fetch — for aggregator URLs (Indeed/LinkedIn/Glassdoor/SimplyHired/Wellfound/BuiltIn/etc), ScoutPilot GETs the listing page and regex-extracts any embedded ATS apply URL. (3) Name-based slug fuzzing — generates slug variants from unknown company names, probes each ATS, and fuzzy-matches the returned board name against the expected company (rapidfuzz threshold 70) to prevent false positives. Negative results cached in discovery_checked.json so repeat probes are free. The list now grows from Indeed/LinkedIn jobs too, not just direct-ATS jobs."},
    {"version": "1.9.3", "date": "2026-04-13", "status": "active", "change": "ATS AUTO-DISCOVERY: ScoutPilot now self-grows its company list. Every 6th scrape cycle (~30min) it scans recent job URLs from ALL sources (Indeed, LinkedIn, JobSpy, etc), extracts Greenhouse/Lever/Ashby/Workday/SmartRecruiters slugs via regex, verifies them against live ATS APIs, and auto-adds any new companies. Also exposes POST /api/admin/ats-discover for on-demand runs. The list compounds over time — every new company hiring through a supported ATS gets picked up automatically."},
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
from ai_engine import expand_title_ai, score_relevance_ai, score_trust_ai, score_trust_heuristic

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


def _build_profile_data(profiles: list[dict]) -> list[dict]:
    """Normalize profiles into the compact dicts the scorer/classifier use."""
    out = []
    for profile in profiles:
        kws = profile.get("keywords", [])
        if isinstance(kws, str):
            kws = [k.strip() for k in kws.split(",") if k.strip()]
        excl = profile.get("excluded_keywords", [])
        if isinstance(excl, str):
            excl = [k.strip() for k in excl.split(",") if k.strip()]
        out.append({
            "id": profile.get("id"),
            "title": profile["title"],
            "expanded": profile.get("expanded_titles", []),
            "keywords": kws,
            "excluded": excl,
            "signature": profile.get("skill_signature") or {},
        })
    return out


async def _classify_and_store(new_jobs: list[dict], profiles: list[dict]) -> tuple[int, int]:
    """v2.2.0 relevance gate — the single scoring path for both the regular cycle
    and the deep sweep.

    Groups unscored jobs by the profile that fetched them (or best fuzzy match),
    then classifies each batch of ~18 with ONE Haiku call (title-first, JD snippet
    to disambiguate) and HIDES clearly-wrong roles. Bounded cost (batched +
    once-per-job). Falls back to fuzzy per-job if AI is unavailable.
    Returns (scored_count, hidden_count).
    """
    from config import settings as _cfg
    from ai_engine import score_relevance_fuzzy, classify_jobs_batch, pop_ai_call_stats

    if not new_jobs:
        return (0, 0)

    all_pd = _build_profile_data(profiles)
    if not all_pd:
        return (0, 0)
    pdata_by_id = {pd["id"]: pd for pd in all_pd if pd.get("id") is not None}

    HIDE_BELOW = _cfg.relevance_hide_below
    BATCH = 18

    # Assign each job to a profile (the one that fetched it, else best fuzzy).
    groups: dict = {}
    for job in new_jobs:
        pd = pdata_by_id.get(job.get("search_profile_id"))
        if pd is None:
            best_f, pd = -1, all_pd[0]
            for cand in all_pd:
                f = score_relevance_fuzzy(
                    job["title"], job.get("description", ""),
                    cand["title"], cand["expanded"], cand["keywords"],
                    skill_signature=cand.get("signature"),
                )
                if f > best_f:
                    best_f, pd = f, cand
        groups.setdefault(pd["title"], (pd, []))[1].append(job)

    ai_scored = 0
    hidden_count = 0
    for _title, (pd, jobs_for_pd) in groups.items():
        for i in range(0, len(jobs_for_pd), BATCH):
            chunk = jobs_for_pd[i:i + BATCH]

            # Score with fuzzy FIRST, then send only the genuinely ambiguous
            # band to the classifier. Previously every job went to the AI, so a
            # pass cost one Haiku call per 18 jobs no matter how obvious they
            # were — which is why 12,352 of 30,977 jobs sat unscored, showing
            # the schema's DEFAULT 50 as if it were a real judgement and
            # sailing straight through a "relevance 50+" filter.
            # Below _AI_BAND_LOW the family fence has already rejected it;
            # above _AI_BAND_HIGH the title is an unambiguous match. The AI adds
            # nothing at either extreme, so skipping them cuts token spend and
            # lets the worker actually clear its backlog.
            fuzzy_by_id = {}
            for job in chunk:
                fuzzy_by_id[job["id"]] = score_relevance_fuzzy(
                    job["title"], job.get("description", ""),
                    pd["title"], pd["expanded"], pd["keywords"],
                    skill_signature=pd.get("signature"),
                )
            ambiguous = [j for j in chunk
                         if _AI_BAND_LOW <= fuzzy_by_id[j["id"]] <= _AI_BAND_HIGH]
            scores = {}
            if ambiguous:
                scores = await classify_jobs_batch(
                    pd["title"], pd["keywords"], pd["excluded"], ambiguous,
                )

            for job in chunk:
                fuzzy = fuzzy_by_id[job["id"]]
                if job["id"] in scores:
                    relevance = scores[job["id"]]
                    # The classifier was rating a "Senior DevOps Engineer" above
                    # 50 for a Data Analyst profile and putting it on the board.
                    # A fuzzy score at or below the fence cap means two things at
                    # once: different role family AND nothing in the description
                    # the skill signature recognises — the disguised-role rescue
                    # would have lifted it above the cap otherwise. That is
                    # stronger evidence than the classifier's opinion, so it wins.
                    if fuzzy <= _FAMILY_FENCE_CAP:
                        relevance = min(relevance, fuzzy)
                else:
                    relevance = fuzzy
                trust = score_trust_heuristic(
                    job["title"], job.get("company_name", ""),
                    job.get("description", ""), job.get("salary_min", 0),
                    job.get("salary_max", 0), job.get("company_domain", ""),
                    job.get("source", ""),
                )
                hide = relevance < HIDE_BELOW
                await update_job_scores(job["id"], relevance, trust, hide=hide)
                ai_scored += 1
                if hide:
                    hidden_count += 1

    if ai_scored:
        _stats = pop_ai_call_stats()
        logger.info(
            f"[Relevance] Classified {ai_scored} jobs | hid {hidden_count} off-target "
            f"| Haiku calls: {_stats['total']}"
        )
    return (ai_scored, hidden_count)


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
        from database import get_jobs as _get_jobs, get_db as _get_db, get_unscored_jobs
        from ai_engine import score_relevance_ai, score_relevance_fuzzy, extract_direct_link_ai
        # v2.1.0 COST FIX: only fetch jobs that have NEVER been scored.
        # Previously this used get_jobs(status='new', hours=72, limit=500), which
        # re-fetched the same jobs every 5-min cycle and re-ran Haiku on each for
        # up to 72h (~800x per job). scored_at makes AI scoring run once per job.
        new_jobs = await get_unscored_jobs(limit=400)

        # v2.2.0: AI batch relevance gate — classify + hide off-target jobs.
        await _classify_and_store(new_jobs, profiles)

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

            # v1.9.5: sanitize expansions and do NOT use keywords as standalone
            # search terms (they bring back noisy SWE/QA/Marketing results that
            # happen to mention "Python" or "SQL").
            from ai_engine import _sanitize_expansions
            clean_expanded = _sanitize_expansions(title, expanded)
            search_terms = [title] + [t for t in clean_expanded if t.lower() != title.lower()]

            for term in search_terms[:5]:  # cap terms for deep sweep
                for loc in (locations if locations else [""]):
                    try:
                        new_jobs = await scrape_jobspy(
                            search_term=term,
                            location=loc,
                            results_wanted=150,
                            hours_old=168,  # 7 days
                            profile_id=profile_id,
                        )
                        total_new += len(new_jobs)
                    except Exception as e:
                        logger.error(f"[Deep Sweep] Error: {e}")

        # Score any new finds — same AI batch relevance gate as the regular cycle.
        if total_new > 0:
            from database import get_unscored_jobs
            new_jobs = await get_unscored_jobs(limit=400)
            await _classify_and_store(new_jobs, profiles)

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
        # Deleting rows does not shrink a SQLite file — without this the DB
        # only ever grows and eventually fills the volume.
        try:
            from database import reclaim_space
            rec = await reclaim_space()
            logger.info(f"[Retention] reclaim: {rec.get('steps')} "
                        f"freed={rec.get('freed_mb')}MB")
        except Exception as e:
            logger.error(f"[Retention] reclaim_space failed: {e}")
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

    # Self-heal a full volume BEFORE anything tries to write. Once SQLite is
    # raising "database or disk is full" every scraper insert fails silently,
    # so this has to happen ahead of the workers, not on a daily timer.
    try:
        from database import emergency_reclaim, CRITICAL_FREE_MB
        er = await emergency_reclaim()
        if er.get("freed_mb"):
            logger.warning(
                f"[Storage] EMERGENCY RECLAIM: {er['start_free_mb']}MB -> "
                f"{er['end_free_mb']}MB free, db {er.get('start_db_mb')}MB -> "
                f"{er.get('end_db_mb')}MB. Steps: {er['steps']}")
        else:
            logger.info(f"[Storage] {er['steps']}")
    except Exception as e:
        logger.error(f"[Storage] emergency_reclaim failed: {e}")

    # Re-show jobs that only became hidden because the old threshold was
    # stricter. update_job_scores() writes status='hidden' at scrape time, so
    # lowering relevance_hide_below alone would apply to NEW jobs only and the
    # existing backlog would stay invisible.
    try:
        from config import settings as _s
        from database import get_db as _gdb
        _db = await _gdb()
        try:
            _cur = await _db.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='hidden' "
                "AND relevance_score >= ?", (_s.relevance_hide_below,))
            _n = (await _cur.fetchone())[0]
            if _n:
                await _db.execute(
                    "UPDATE jobs SET status='new' WHERE status='hidden' "
                    "AND relevance_score >= ?", (_s.relevance_hide_below,))
                await _db.commit()
                logger.warning(
                    f"[Retention] un-hid {_n} jobs now at/above the "
                    f"relevance threshold ({_s.relevance_hide_below})")
        finally:
            await _db.close()
    except Exception as e:
        logger.error(f"[Retention] unhide backfill failed: {e}")

    # One-time consistency repair: rows written before is_remote was derived
    # from work_type can disagree, and the card badge treats is_remote=1 as
    # Remote regardless of work_type — so a hybrid row rendered as Remote.
    try:
        from database import get_db as _gdb2
        _db2 = await _gdb2()
        try:
            _c = await _db2.execute(
                "SELECT COUNT(*) FROM jobs WHERE (work_type='remote') != (is_remote=1)")
            _n2 = (await _c.fetchone())[0]
            if _n2:
                await _db2.execute(
                    "UPDATE jobs SET is_remote = CASE WHEN work_type='remote' "
                    "THEN 1 ELSE 0 END WHERE (work_type='remote') != (is_remote=1)")
                await _db2.commit()
                logger.warning(f"[Integrity] realigned is_remote on {_n2} rows")
        finally:
            await _db2.close()
    except Exception as e:
        logger.error(f"[Integrity] is_remote realign failed: {e}")

    # REVERSING v2.18.0. Those three sources were dark on purpose, not by
    # accident: Himalayas puts the job itself behind account creation —
    # clicking Apply lands on himalayas.app/signup/talent?redirect=... — which
    # is exactly the kind of board to avoid. Treating "produced zero jobs" as a
    # bug without asking WHY it was off put signup-wall jobs back on the board.
    # Jobicy is the same shape, so it goes dark too.
    _SIGNUP_WALL_SOURCES = ("himalayas", "himalayas_rss", "jobicy", "jobicy_rss")
    try:
        from database import get_db as _gdb3
        _db3 = await _gdb3()
        try:
            _q = ",".join("?" for _ in _SIGNUP_WALL_SOURCES)
            _cur3 = await _db3.execute(
                f"UPDATE source_settings SET enabled = 0 "
                f"WHERE source_key IN ({_q}) AND enabled = 1", _SIGNUP_WALL_SOURCES)
            _off = _cur3.rowcount
            # Existing rows from those sources are dead ends — hide them.
            _cur4 = await _db3.execute(
                f"UPDATE jobs SET status = 'hidden' "
                f"WHERE source IN ({_q}) AND status = 'new'", _SIGNUP_WALL_SOURCES)
            _hid = _cur4.rowcount
            if _off or _hid:
                await _db3.commit()
                logger.warning(
                    f"[Sources] disabled {_off} signup-wall sources, "
                    f"hid {_hid} of their jobs")
        finally:
            await _db3.close()
    except Exception as e:
        logger.error(f"[Sources] signup-wall disable failed: {e}")

    # Re-apply the role-family fence to rows already scored. Scores are written
    # once (scored_at is set, so the Scoring worker skips them), which means a
    # scorer change only affects NEW jobs and the backlog keeps whatever the old
    # logic decided — that is why "Senior Software Engineer" was still sitting
    # in a Data Analyst feed after the fence was fixed. Fuzzy-only, so this
    # costs no API calls.
    try:
        from database import get_db as _gdb4, get_profiles as _gp4
        from ai_engine import score_relevance_fuzzy as _fz, _get_fallback_signature as _sig4
        _profs = await _gp4()
        if _profs:
            _pd = _build_profile_data(_profs)
            _by_id = {p["id"]: p for p in _pd if p.get("id") is not None}
            _db4 = await _gdb4()
            try:
                _cur4 = await _db4.execute(
                    "SELECT id, title, description, search_profile_id, relevance_score "
                    "FROM jobs WHERE status NOT IN ('saved','applied')")
                _rows = await _cur4.fetchall()
                _fixed = 0
                for _r in _rows:
                    _p = _by_id.get(_r["search_profile_id"]) or (_pd[0] if _pd else None)
                    if not _p:
                        continue
                    _f = _fz(_r["title"] or "", (_r["description"] or "")[:3000],
                             _p["title"], _p["expanded"], _p["keywords"],
                             skill_signature=_p.get("signature"))
                    # Only ever demote here: this pass exists to remove roles the
                    # fence now rejects, not to promote anything the AI scored low.
                    if _f <= _FAMILY_FENCE_CAP and (_r["relevance_score"] or 0) > _f:
                        await _db4.execute(
                            "UPDATE jobs SET relevance_score = ?, status = "
                            "CASE WHEN status IN ('saved','applied') THEN status "
                            "ELSE 'hidden' END WHERE id = ?", (_f, _r["id"]))
                        _fixed += 1
                if _fixed:
                    await _db4.commit()
                    logger.warning(
                        f"[Rescore] fence re-applied to {_fixed} previously-scored jobs")
            finally:
                await _db4.close()
    except Exception as e:
        logger.error(f"[Rescore] fence backfill failed: {e}")

    # Repair rows written before the entity decoding and the tightened remote
    # detection. Both are display-facing: a title reading "R&amp;D Finance" and
    # a Boston job badged Remote are visible defects, and existing rows are
    # never rewritten by re-scraping because dedup rejects them.
    try:
        from database import get_db as _gdb5, _clean_text as _ct
        from scraper import _detect_work_type as _dwt
        _db5 = await _gdb5()
        try:
            _cur = await _db5.execute(
                "SELECT id, title, company_name, location FROM jobs "
                "WHERE title LIKE '%&%;%' OR company_name LIKE '%&%;%' "
                "   OR location LIKE '%&%;%'")
            _ents = [dict(r) for r in await _cur.fetchall()]
            for _r in _ents:
                await _db5.execute(
                    "UPDATE jobs SET title=?, company_name=?, location=? WHERE id=?",
                    (_ct(_r["title"]), _ct(_r["company_name"]),
                     _ct(_r["location"]), _r["id"]))
            if _ents:
                await _db5.commit()
                logger.warning(f"[Repair] decoded HTML entities in {len(_ents)} rows")

            # Re-derive work_type where the label contradicts a concrete
            # location. Only ever demotes remote -> hybrid/onsite here.
            _cur = await _db5.execute(
                "SELECT id, title, location, description FROM jobs "
                "WHERE work_type='remote' AND location != '' "
                "  AND location NOT LIKE '%remote%' AND location NOT LIKE '%anywhere%' "
                "  AND location LIKE '%,%'")
            _susp = [dict(r) for r in await _cur.fetchall()]
            _demoted = 0
            for _r in _susp:
                _wt = _dwt({"title": _r["title"], "location": _r["location"],
                            "description": _r["description"] or ""})
                if _wt != "remote":
                    await _db5.execute(
                        "UPDATE jobs SET work_type=?, is_remote=0 WHERE id=?",
                        (_wt, _r["id"]))
                    _demoted += 1
            if _demoted:
                await _db5.commit()
                logger.warning(
                    f"[Repair] re-labelled {_demoted}/{len(_susp)} rows that claimed "
                    f"remote with a concrete location")
        finally:
            await _db5.close()
    except Exception as e:
        logger.error(f"[Repair] backfill failed: {e}")

    # Purge rows the previous US filter let through: open-ended locations
    # qualified by a non-US region ("Anywhere In South America"), European
    # cities that were not in the list (Cologne, Dusseldorf, Nuremberg), and
    # German-language postings identifiable by their (m/w/d) gender marker.
    try:
        from database import get_db as _gdb6, is_us_location as _isus, \
            looks_non_us_posting as _nonus
        _db6 = await _gdb6()
        try:
            _cur6 = await _db6.execute(
                "SELECT id, title, location FROM jobs WHERE status != 'hidden'")
            _rows6 = [dict(r) for r in await _cur6.fetchall()]
            _kill = [r["id"] for r in _rows6
                     if ((r["location"] or "").strip()
                         and not _isus(r["location"]))
                     or _nonus(r["title"] or "")]
            for _i in range(0, len(_kill), 400):
                _chunk = _kill[_i:_i + 400]
                await _db6.execute(
                    f"UPDATE jobs SET status='hidden' WHERE id IN "
                    f"({','.join('?' for _ in _chunk)})", _chunk)
            if _kill:
                await _db6.commit()
                logger.warning(f"[Repair] hid {len(_kill)} non-US rows")
        finally:
            await _db6.close()
    except Exception as e:
        logger.error(f"[Repair] non-US purge failed: {e}")

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
    # ── v2.3.0: Independent per-source workers ──
    # Instead of one loop where sources take turns behind a shared cooldown, each
    # source group runs in its OWN loop at its OWN cadence, all in parallel. They
    # all write to the shared DB (deduped on insert, WAL handles concurrent
    # writers); a separate Scoring worker classifies unscored jobs and hides
    # off-target ones. No source waits on any other.
    async def _for_each_profile(fn, *args):
        profiles = await get_profiles()
        for p in (profiles or []):
            try:
                await fn(p, *args)
            except Exception as e:
                logger.error(f"[Worker] {getattr(fn, '__name__', 'fn')} failed for {p.get('title')}: {e}")

    async def _worker(name: str, interval: int, body):
        await asyncio.sleep(3)  # small startup stagger
        while True:
            try:
                await body()
            except Exception as e:
                logger.error(f"[Worker:{name}] crashed: {e}")
            await asyncio.sleep(interval)

    # One independent worker per ATS platform — each rotates its own companies
    # at its own cadence, so a slow platform (Workday POST+pagination) never
    # holds up the fast ones (Greenhouse/Ashby).
    _ats_cycles = {}

    def _make_ats_body(platform: str):
        _ats_cycles[platform] = 0
        async def _body():
            from scraper import scrape_ats_for_profile
            _ats_cycles[platform] += 1
            await _for_each_profile(scrape_ats_for_profile, _ats_cycles[platform], [platform])
        return _body

    _jobspy_cycle = {"n": 0}

    async def _jobspy_body():
        from scraper import scrape_jobspy_for_profile
        _jobspy_cycle["n"] += 1
        # Pass the cycle number so the term window rotates across all
        # profile-correct titles over time (not just the first 8 every run).
        await _for_each_profile(scrape_jobspy_for_profile, _jobspy_cycle["n"])

    async def _light_body():
        from scraper import scrape_light_for_profile
        await _for_each_profile(scrape_light_for_profile)

    async def _linkedin_body():
        """LinkedIn via its public guest feed — ~2.9KB/job instead of JobSpy's
        ~200KB, so it can run continuously without draining proxy bandwidth.
        Rotates a small term window per cycle to widen coverage over time."""
        from scraper import scrape_linkedin_guest, _build_profile_terms
        from database import get_enabled_sources
        if "linkedin" not in await get_enabled_sources():
            return
        profiles = await get_profiles()
        if not profiles:
            return
        cycle = _linkedin_state["cycle"] = _linkedin_state["cycle"] + 1
        total = 0
        for profile in profiles:
            terms = _build_profile_terms(profile) or [profile["title"]]
            window = 6
            start = (cycle * window) % max(len(terms), 1)
            picked = [terms[(start + i) % len(terms)] for i in range(min(window, len(terms)))]
            for term in picked:
                # One pass per workplace type. The cards carry no remote hint and
                # no description, so querying them together made every LinkedIn
                # row default to onsite. Asking LinkedIn per type is the only way
                # to label remote/hybrid correctly.
                for wt in ("remote", "hybrid", "onsite"):
                    try:
                        # days=1: only postings from the last 24h, so deep
                        # pagination spends its pages on genuinely new jobs
                        # instead of re-walking the same week.
                        # pages=12 is a CEILING, not a cost — the scraper stops
                        # after two pages that add nothing new, so a quiet cycle
                        # still costs two pages.
                        r = await scrape_linkedin_guest(
                            term, "United States", profile["id"],
                            pages=12, days=1, work_type=wt)
                        total += len(r)
                    except Exception as e:
                        logger.error(f"[LinkedInGuest] '{term}'/{wt}: {e}")
                    await asyncio.sleep(1.5)
        logger.info(f"[LinkedInGuest] cycle {cycle}: +{total} new jobs")

    async def _enrich_body():
        from scraper import enrich_missing_descriptions
        await enrich_missing_descriptions(limit=12)

    async def _scoring_body():
        global last_scrape_result
        from database import get_unscored_jobs
        profiles = await get_profiles()
        if not profiles:
            return
        # 1500, was 400. Most jobs now resolve on fuzzy alone (no network call),
        # so a pass is far cheaper and the 12k unscored backlog can drain
        # instead of growing behind the ATS intake.
        jobs = await get_unscored_jobs(limit=1500)
        scored = 0
        if jobs:
            scored, _ = await _classify_and_store(jobs, profiles)
        # Heartbeat so /api/status reflects the live worker model.
        last_scrape_result = {
            "status": "running",
            "mode": "workers",
            "last_classified": scored,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # Rotates the LinkedIn guest-feed search-term window across cycles.
    _linkedin_state = {"cycle": 0}

    # Always-on AI discovery bot — grows the ATS roster by itself.
    _disc = {"keys": None}

    async def _seed_disc_keys():
        if _disc["keys"] is None:
            from ats_scraper import load_companies
            from database import get_discovered_companies
            keys = {(c["slug"].lower(), c["ats"].lower()) for c in load_companies()}
            for c in await get_discovered_companies():
                keys.add((c["slug"].lower(), c["ats"].lower()))
            _disc["keys"] = keys
            logger.info(f"[Discovery] seeded with {len(keys)} known companies")

    async def _discovery_body():
        from discovery import run_discovery_round
        from database import add_discovered_company, count_discovered_companies
        await _seed_disc_keys()
        found = await run_discovery_round(_disc["keys"])
        for c in found:
            await add_discovered_company(c)
        if found:
            total = await count_discovered_companies()
            sample = ", ".join(f"{c['name']}({c['ats']}:{c['jobs_seen']})" for c in found[:8])
            logger.info(f"[Discovery] +{len(found)} new companies (bot total {total}) — {sample}")

    async def _ats_harvest_body():
        """URL-harvest discovery: pull real ATS slugs out of jobs we already
        scraped. Free (no LLM), far higher yield than name-guessing. This was
        only reachable from the legacy run_scrape_cycle path, so in workers
        mode it never ran — that is why the roster grew so slowly."""
        from ats_discovery import discover_new_ats_companies
        stats = await discover_new_ats_companies()
        n = stats.get("verified_new", 0)
        if n:
            from database import count_discovered_companies
            total = await count_discovered_companies()
            sample = ", ".join(
                f"{a['ats']}:{a['name']}" for a in stats.get("added", [])[:8]
            )
            logger.info(
                f"[Discovery/Harvest] +{n} companies (bot total {total}) — {sample}"
            )
        else:
            logger.info(
                f"[Discovery/Harvest] 0 new "
                f"(scanned {stats.get('jobs_scanned',0)} jobs, "
                f"{stats.get('failed_verify',0)} failed verify)"
            )

    async def _workday_discovery_body():
        # Gentle, infrequent Workday discovery (shared IP with the Workday scraper).
        from discovery import run_workday_discovery_round
        from database import add_discovered_company, count_discovered_companies
        await _seed_disc_keys()
        found = await run_workday_discovery_round(_disc["keys"])
        for c in found:
            await add_discovered_company(c)
        if found:
            total = await count_discovered_companies()
            sample = ", ".join(f"{c['name']}(wd:{c['jobs_seen']})" for c in found[:6])
            logger.info(f"[Discovery/Workday] +{len(found)} new Workday employers (bot total {total}) — {sample}")

    # One worker per ATS platform. Fast public APIs now sweep ALL their companies
    # every run (no rotation), so intervals are sized to a full sweep + politeness.
    asyncio.create_task(_worker("ATS-greenhouse", 180, _make_ats_body("greenhouse")))     # ~770 companies/run
    asyncio.create_task(_worker("ATS-ashby", 150, _make_ats_body("ashby")))               # ~360
    asyncio.create_task(_worker("ATS-lever", 130, _make_ats_body("lever")))               # ~200
    asyncio.create_task(_worker("ATS-smartrecruiters", 150, _make_ats_body("smartrecruiters")))  # ~145
    asyncio.create_task(_worker("ATS-workday", 200, _make_ats_body("workday")))           # rotates (buckets=4), WAF-sensitive
    # Cloudflare-fronted platforms: low concurrency + long intervals. Workable
    # answered ~40 quick probes with a 24-hour 429 ban, so these are swept
    # gently (and via the residential proxy when one is configured).
    asyncio.create_task(_worker("ATS-workable", 600, _make_ats_body("workable")))
    asyncio.create_task(_worker("ATS-recruitee", 600, _make_ats_body("recruitee")))
    asyncio.create_task(_worker("ATS-breezy", 600, _make_ats_body("breezy")))
    # Non-ATS source groups + scoring + discovery:
    # 300s, was 120s. Every remote board in this group reports "inserted 0 new"
    # cycle after cycle (Remotive 19 found -> 0 new, TheMuse 100 -> 0) because
    # cross-source dedup already has those jobs. Polling them 3x/hour instead of
    # 30x frees request budget for the ATS sweep that is actually producing.
    asyncio.create_task(_worker("Light", 300, _light_body))
    asyncio.create_task(_worker("JobSpy", 420, _jobspy_body)) # LinkedIn/Indeed anti-bot: long interval
    asyncio.create_task(_worker("LinkedIn-Guest", 900, _linkedin_body))  # cheap public feed, USA only
    # Backfill descriptions on rows that arrived without one (LinkedIn's guest
    # feed has no description, and the skill-signature rescue — the mechanism
    # that catches a role hidden by its title — cannot run without one).
    # 30-min interval and a 12-row cap because each LinkedIn page is ~300KB
    # through the metered proxy; the queue drains and then idles.
    asyncio.create_task(_worker("Enrich", 1800, _enrich_body))
    asyncio.create_task(_worker("Scoring", 30, _scoring_body))# classify + hide, keeps up with inflow
    asyncio.create_task(_worker("Discovery", 900, _discovery_body)) # AI finds new companies, forever
    asyncio.create_task(_worker("Discovery-Workday", 5400, _workday_discovery_body)) # gentle, every 90min
    asyncio.create_task(_worker("Discovery-Harvest", 600, _ats_harvest_body))  # URL-harvest: free, high-yield roster growth
    logger.info("[Workers] Launched 10 workers — 5 ATS platforms + Light + JobSpy + Scoring + Discovery + Discovery-Workday")

    # Keep deep sweep and cleanup on scheduler
    scheduler.add_job(
        scheduled_deep_sweep,
        "interval",
        hours=12,
        id="deep_sweep",
        replace_existing=True,
    )
    # Hourly, not once at 3AM. Retention is 3 days with no archive step, and the
    # ATS layer now ingests onsite/hybrid too, so waiting a full day between
    # sweeps lets the volume pile up on a 454MB disk.
    scheduler.add_job(
        scheduled_cleanup,
        "interval",
        hours=1,
        id="hourly_cleanup",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("[Scheduler] Parallel source workers running. Deep sweep 12h, cleanup hourly.")

    # Self-healing title expansion — only re-expands profiles whose search terms
    # look polluted (heuristic seniority junk from a past AI outage). Clean
    # profiles are skipped, so this costs ~0 AI calls once healed.
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
                    # These rows are all currently work_type='remote', so preserve
                    # the platform's remote flag. This lets detection keep genuinely
                    # remote roles (whose stored text lacks the word "remote") and
                    # only downgrade the ones with a clear onsite signal — instead
                    # of blanket-demoting every LinkedIn job saved without a JD.
                    "is_remote": 1,
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


def _validate_xhire_jwt(token: str) -> bool:
    """Xhire Suite SSO — verify a JWT minted by the Xhire app using the shared
    HS256 secret. Returns True only for a valid, non-suspended token.

    Fully guarded: if the secret is unset or PyJWT isn't installed, returns
    False so the caller falls back to normal password auth. Never raises.
    """
    if not token or not settings.jwt_secret:
        return False
    try:
        import jwt as _pyjwt
        payload = _pyjwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        if payload.get("suspended"):
            return False
        return True
    except Exception:
        return False


class AuthMiddleware(BaseHTTPMiddleware):
    """Block all routes except /login when SITE_PASSWORD is set and user has no session."""

    OPEN_PATHS = {"/login", "/favicon.ico", "/healthz", "/api/test-sources", "/api/debug/scrape-log", "/api/debug/sources", "/api/debug/outbound-ip", "/api/debug/storage", "/api/debug/storage-reclaim", "/api/debug/pipeline", "/api/status"}

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

        # Xhire Suite SSO — accept a valid Xhire JWT (query param on first hit,
        # cookie, or Authorization: Bearer). On success, mint an sp_session and
        # set the cookie so subsequent requests use the normal session path.
        # This is purely additive: password login is unchanged, and if the
        # shared secret is unset this whole block is a no-op.
        xhire_tok = (
            request.query_params.get("xhire_token")
            or request.cookies.get("xhire_token")
            or (request.headers.get("authorization", "").replace("Bearer ", "")
                if request.headers.get("authorization", "").startswith("Bearer ") else "")
        )
        if xhire_tok and _validate_xhire_jwt(xhire_tok):
            new_sid = _make_session_id()
            _valid_sessions.add(new_sid)
            response = await call_next(request)
            response.set_cookie(
                "sp_session", new_sid,
                httponly=True, max_age=60 * 60 * 24 * 30, samesite="lax",
            )
            # Persist the Xhire token (httponly) so the "Prep for this job"
            # feature can call the Xhire API server-side on the user's behalf.
            response.set_cookie(
                "xhire_token", xhire_tok,
                httponly=True, max_age=60 * 60 * 24 * 30, samesite="lax",
            )
            return response

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
    sort_by: str = "first_seen_at",  # newest FOUND first; posted time breaks ties
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
        # Opt-IN filter. Job boards (LinkedIn, Indeed, Glassdoor) are wanted as
        # sources — what is not wanted is being forced to apply INSIDE them. So
        # they stay visible by default and carry a "Direct Apply" badge when the
        # link lands on the employer's own posting; the toggle narrows to those.
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

    # v1.9.6: also generate the skill signature so description-rescue
    # works on day one. One AI call per profile, cached forever.
    try:
        from ai_engine import generate_skill_signature_ai
        sig = await generate_skill_signature_ai(data["title"], data.get("keywords"))
        if sig:
            data["skill_signature"] = sig
    except Exception as e:
        logger.error(f"[Profile] Signature generation failed: {e}")

    profile_id = await create_profile(data)
    return {"id": profile_id, "expanded_titles": expanded, "skill_signature": data.get("skill_signature", {})}


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

@app.post("/api/prep-for-job")
async def api_prep_for_job(request: Request):
    """Xhire Suite — turn a ScoutPilot listing into a pre-filled Xhire
    interview-prep session. Server-side so the Xhire JWT stays httponly.

    Isolation-safe: this only touches Xhire over the network. Any failure
    (no token, Xhire down, error) returns a friendly JSON error and never
    affects ScoutPilot's own operation.
    """
    from database import get_job_by_id

    xhire_tok = request.cookies.get("xhire_token", "")
    if not xhire_tok or not _validate_xhire_jwt(xhire_tok):
        return JSONResponse(
            {"error": "not_linked",
             "message": "Open Job Scout from your Xhire launcher to prep interviews."},
            status_code=403,
        )

    try:
        body = await request.json()
    except Exception:
        body = {}
    job_id = body.get("job_id")
    if not job_id:
        return JSONResponse({"error": "missing_job_id"}, status_code=400)

    job = await get_job_by_id(int(job_id))
    if not job:
        return JSONResponse({"error": "job_not_found"}, status_code=404)

    payload = {
        "company": job.get("company_name", "") or "",
        "role": job.get("title", "") or "",
        "jd": job.get("description", "") or "",
        "resume": "",
    }

    try:
        import httpx
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                f"{settings.xhire_base_url.rstrip('/')}/api/sessions",
                json=payload,
                headers={"Authorization": f"Bearer {xhire_tok}"},
            )
        if r.status_code >= 400:
            # Surface Xhire's own message (e.g. plan limit) without crashing.
            try:
                msg = r.json().get("error", "Could not create prep session.")
            except Exception:
                msg = "Could not create prep session."
            return JSONResponse({"error": "xhire_error", "message": msg}, status_code=502)
        data = r.json()
        session_id = data.get("id")
        return {
            "ok": True,
            "session_id": session_id,
            "session_url": f"{settings.xhire_base_url.rstrip('/')}/launcher",
        }
    except Exception as e:
        logger.error(f"[Prep] Xhire session creation failed: {e}")
        return JSONResponse(
            {"error": "xhire_unreachable",
             "message": "Xhire is unreachable right now — try again shortly."},
            status_code=502,
        )


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


# Seniority / generic prefixes that a GOOD AI expansion never produces. Their
# presence means the profile's expanded_titles came from the heuristic fallback
# (generated during an AI outage) — junk search terms like "Head of Data Analyst"
# that return nothing and crowd out real variants (BI Analyst, Reporting Analyst).
_POLLUTION_PREFIXES = (
    "senior ", "sr ", "sr. ", "junior ", "jr ", "jr. ", "lead ", "staff ",
    "principal ", "head of ", "director ", "vp ", "chief ",
)


def _titles_polluted(expanded: list) -> bool:
    """True if a profile's expanded_titles look like heuristic junk and should
    be regenerated: too few, or containing seniority-prefixed variants."""
    clean = [t for t in (expanded or []) if isinstance(t, str) and t.strip()]
    if len(clean) < 4:
        return True
    for t in clean:
        tl = t.strip().lower()
        if any(tl.startswith(p) for p in _POLLUTION_PREFIXES):
            return True
    return False


async def _re_expand_profiles():
    """Self-healing title expansion. Runs on startup but only re-expands profiles
    whose expanded_titles look polluted (heuristic seniority junk) — so it fixes
    bad search terms without spending an AI call on already-clean profiles."""
    try:
        await asyncio.sleep(5)  # Let the app fully start first
        profiles = await get_profiles()
        for profile in profiles:
            title = profile["title"]
            if not _titles_polluted(profile.get("expanded_titles", [])):
                logger.info(f"[Startup] '{title}' titles already clean — skipping re-expansion")
                continue
            logger.info(f"[Startup] '{title}' titles polluted — re-expanding...")
            try:
                expanded = await expand_title_ai(title)
                if expanded and len(expanded) > 3:
                    await update_profile(profile["id"], {"expanded_titles": expanded})
                    logger.info(f"[Startup] '{title}' expanded to {len(expanded)} clean role names: {expanded}")
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

@app.get("/api/debug/pipeline")
async def api_debug_pipeline():
    """Where jobs actually end up: status split, hidden counts per source, score
    distribution, and the ATS roster size per platform. Open, aggregate counts
    only — this answers 'the DB has jobs but the page looks empty'."""
    try:
        from config import settings as _s
        from database import get_db, ATS_SOURCES
        from ats_scraper import load_companies_merged
        out: dict = {"hide_below": _s.relevance_hide_below}
        db = await get_db()
        try:
            async def rows(sql, args=()):
                cur = await db.execute(sql, args)
                return [dict(r) for r in await cur.fetchall()]

            out["by_status"] = await rows(
                "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status ORDER BY n DESC")
            out["by_source_status"] = await rows(
                "SELECT source, "
                "  SUM(CASE WHEN status='hidden' THEN 1 ELSE 0 END) AS hidden, "
                "  SUM(CASE WHEN status!='hidden' THEN 1 ELSE 0 END) AS visible, "
                "  COUNT(*) AS total "
                "FROM jobs GROUP BY source ORDER BY total DESC")
            out["score_buckets"] = await rows(
                "SELECT CASE "
                "  WHEN relevance_score IS NULL THEN 'unscored' "
                "  WHEN relevance_score < 25 THEN '00-24' "
                "  WHEN relevance_score < 40 THEN '25-39' "
                "  WHEN relevance_score < 60 THEN '40-59' "
                "  WHEN relevance_score < 80 THEN '60-79' "
                "  ELSE '80-100' END AS bucket, COUNT(*) AS n "
                "FROM jobs GROUP BY bucket ORDER BY bucket")
            out["visible_last_24h"] = await rows(
                "SELECT COUNT(*) AS n FROM jobs "
                "WHERE status != 'hidden' AND first_seen_at > datetime('now','-1 day')")
            # What the PAGE actually shows: the first 50 rows in default sort.
            # "Only Workday shows up" is a sort-order question, not a fetching
            # one, so measure the head of the feed rather than raw totals.
            out["feed_top50_by_source"] = await rows(
                "SELECT source, COUNT(*) AS n FROM ("
                "  SELECT source FROM jobs WHERE status != 'hidden' "
                "  ORDER BY CASE "
                "    WHEN posted_at != '' AND posted_at IS NOT NULL AND posted_at NOT LIKE '%T00:00:00%' "
                "      THEN datetime(posted_at) "
                "    WHEN posted_at != '' AND posted_at IS NOT NULL "
                "      THEN MIN(datetime(first_seen_at), datetime(posted_at, '+1 day')) "
                "    ELSE datetime(first_seen_at) END DESC, datetime(first_seen_at) DESC "
                "  LIMIT 50) GROUP BY source ORDER BY n DESC")
            # Posted-time quality per source decides who wins that ordering.
            out["posted_quality"] = await rows(
                "SELECT source, "
                "  SUM(CASE WHEN posted_at != '' AND posted_at IS NOT NULL "
                "       AND posted_at NOT LIKE '%T00:00:00%' THEN 1 ELSE 0 END) AS precise_time, "
                "  SUM(CASE WHEN posted_at LIKE '%T00:00:00%' THEN 1 ELSE 0 END) AS date_only, "
                "  SUM(CASE WHEN posted_at IS NULL OR posted_at='' THEN 1 ELSE 0 END) AS no_posted "
                "FROM jobs WHERE status != 'hidden' GROUP BY source ORDER BY source")
            # The question that matters: how many jobs actually survive the
            # filters the user browses with (work type + relevance), per
            # profile. Totals hide this — a big board can still show an empty
            # page if the relevance gate is eating everything.
            out["remote_by_relevance"] = await rows(
                "SELECT CASE WHEN relevance_score >= 70 THEN 'rel_70+' "
                "            WHEN relevance_score >= 50 THEN 'rel_50-69' "
                "            WHEN relevance_score >= 25 THEN 'rel_25-49' "
                "            ELSE 'rel_under25' END AS band, "
                "       work_type, COUNT(*) AS n "
                "FROM jobs WHERE status != 'hidden' "
                "GROUP BY band, work_type ORDER BY band, n DESC")
            out["visible_by_profile"] = await rows(
                "SELECT COALESCE(p.title,'(unassigned)') AS profile, j.work_type, "
                "       SUM(CASE WHEN j.relevance_score >= 50 THEN 1 ELSE 0 END) AS rel50plus, "
                "       COUNT(*) AS visible "
                "FROM jobs j LEFT JOIN search_profiles p ON p.id = j.search_profile_id "
                "WHERE j.status != 'hidden' "
                "GROUP BY profile, j.work_type ORDER BY visible DESC LIMIT 24")
            # Data-integrity checks: work_type must agree with is_remote, and
            # nothing non-US should have reached the table.
            out["work_type_mismatch"] = await rows(
                "SELECT work_type, is_remote, COUNT(*) AS n FROM jobs "
                "WHERE (work_type='remote') != (is_remote=1) "
                "GROUP BY work_type, is_remote ORDER BY n DESC")
            # Is the Scoring worker keeping up? An unscored row keeps the
            # schema default of 50 for both scores, which lands right on the
            # user's "relevance 50+" filter and looks like a real judgement.
            out["scoring_backlog"] = await rows(
                "SELECT SUM(CASE WHEN scored_at IS NULL OR scored_at='' THEN 1 ELSE 0 END) AS unscored, "
                "       SUM(CASE WHEN relevance_score = 50 AND trust_score = 50 THEN 1 ELSE 0 END) AS both_default, "
                "       COUNT(*) AS total FROM jobs")
            out["unscored_visible"] = await rows(
                "SELECT COUNT(*) AS n FROM jobs "
                "WHERE status != 'hidden' AND (scored_at IS NULL OR scored_at='')")
            # P2: is trust actually discriminating, or stuck at the default 50?
            out["trust_distribution"] = await rows(
                "SELECT CASE WHEN trust_score >= 80 THEN '80-100' "
                "            WHEN trust_score >= 60 THEN '60-79' "
                "            WHEN trust_score = 50 THEN 'exactly-50' "
                "            WHEN trust_score >= 40 THEN '40-49' "
                "            ELSE 'under40' END AS band, COUNT(*) AS n "
                "FROM jobs WHERE status != 'hidden' GROUP BY band ORDER BY n DESC")
            # P5: are the profiles' expanded titles wide enough?
            out["profiles"] = await rows(
                "SELECT id, title, "
                "  LENGTH(expanded_titles) AS expanded_len, "
                "  expanded_titles, keywords "
                "FROM search_profiles WHERE is_active = 1")
            # How many rows the Enrich worker can actually act on. A worker that
            # logs nothing is either idle by design or silently broken, and on
            # this codebase that has twice been the latter.
            out["enrich_queue"] = await rows(
                "SELECT source, "
                "  SUM(CASE WHEN relevance_score BETWEEN 25 AND 79 THEN 1 ELSE 0 END) AS in_band, "
                "  COUNT(*) AS no_desc_visible "
                "FROM jobs WHERE (description IS NULL OR description='') "
                "  AND status NOT IN ('hidden') GROUP BY source ORDER BY no_desc_visible DESC")
            # Look at the ACTUAL rows the user sees, ordered exactly as the feed
            # orders them. Aggregates cannot show a mangled title or a remote
            # label that contradicts its own location.
            out["sample_feed"] = await rows(
                "SELECT j.title, j.company_name, j.location, j.work_type, "
                "       j.is_remote, j.relevance_score, j.source, "
                "       COALESCE(p.title,'(none)') AS profile "
                "FROM jobs j LEFT JOIN search_profiles p ON p.id = j.search_profile_id "
                "WHERE j.status != 'hidden' "
                "ORDER BY datetime(j.first_seen_at) DESC LIMIT 40")
            # Titles still carrying raw HTML entities (&amp; &#39; &quot; ...).
            out["entity_titles"] = await rows(
                "SELECT COUNT(*) AS n FROM jobs "
                "WHERE title LIKE '%&amp;%' OR title LIKE '%&#%' OR title LIKE '%&quot;%' "
                "   OR title LIKE '%&lt;%' OR title LIKE '%&gt;%' OR title LIKE '%&nbsp;%'")
            out["entity_examples"] = await rows(
                "SELECT title, source FROM jobs "
                "WHERE title LIKE '%&amp;%' OR title LIKE '%&#%' LIMIT 8")
            # work_type=remote on a row whose location names a concrete city.
            out["suspect_remote"] = await rows(
                "SELECT COUNT(*) AS n FROM jobs "
                "WHERE status != 'hidden' AND work_type='remote' "
                "  AND location NOT LIKE '%remote%' AND location NOT LIKE '%anywhere%' "
                "  AND location != '' AND location LIKE '%,%'")
            out["suspect_remote_examples"] = await rows(
                "SELECT title, location, source FROM jobs "
                "WHERE status != 'hidden' AND work_type='remote' "
                "  AND location NOT LIKE '%remote%' AND location NOT LIKE '%anywhere%' "
                "  AND location != '' AND location LIKE '%,%' LIMIT 10")
            out["no_description"] = await rows(
                "SELECT source, COUNT(*) AS n FROM jobs "
                "WHERE (description IS NULL OR description='') GROUP BY source ORDER BY n DESC")
        finally:
            await db.close()

        companies = await load_companies_merged()
        per: dict = {}
        for c in companies:
            per[c.get("ats", "?")] = per.get(c.get("ats", "?"), 0) + 1
        out["ats_companies"] = dict(sorted(per.items(), key=lambda kv: -kv[1]))
        out["ats_companies_total"] = len(companies)
        out["ats_platforms_implemented"] = sorted(
            s["source_key"] for s in ATS_SOURCES)
        return out
    except Exception as e:
        logger.exception("[Debug] pipeline failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/debug/storage")
async def api_debug_storage():
    """Disk/SQLite space accounting. Open (like the other /api/debug routes) so
    volume pressure can be diagnosed without a login — it exposes sizes and row
    counts only, never job or user data."""
    try:
        from database import storage_stats
        return await storage_stats()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/debug/storage-reclaim")
async def api_debug_storage_reclaim():
    """Report what an emergency reclaim WOULD do (dry run, no writes)."""
    try:
        from database import emergency_reclaim
        return await emergency_reclaim(dry_run=True)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/emergency-reclaim")
async def api_emergency_reclaim():
    """Force the staged volume recovery immediately."""
    try:
        from database import emergency_reclaim
        return await emergency_reclaim()
    except Exception as e:
        logger.exception("[Admin] emergency-reclaim failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/admin/reclaim-space")
async def api_reclaim_space(force: str = ""):
    """Return dead SQLite pages to the filesystem (WAL truncate, then VACUUM
    when there is enough headroom)."""
    try:
        from database import reclaim_space
        return await reclaim_space(force_vacuum=force in ("1", "true", "yes"))
    except Exception as e:
        logger.exception("[Admin] reclaim-space failed")
        return JSONResponse({"error": str(e)}, status_code=500)


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


@app.post("/api/admin/generate-signatures")
async def api_generate_signatures(force: bool = False):
    """Admin: generate skill signatures for every profile that doesn't have
    one yet. One Haiku call per profile, cached in DB forever. Pass
    ?force=true to regenerate signatures even for profiles that already
    have one."""
    from ai_engine import generate_skill_signature_ai
    profiles = await get_profiles()
    results = []
    for p in profiles:
        existing = p.get("skill_signature") or {}
        if existing and not force:
            results.append({"id": p["id"], "title": p["title"], "status": "skipped (exists)"})
            continue
        try:
            sig = await generate_skill_signature_ai(p["title"], p.get("keywords"))
            await update_profile(p["id"], {"skill_signature": sig})
            results.append({
                "id": p["id"],
                "title": p["title"],
                "foundation": len(sig.get("foundation", [])),
                "toolkit": len(sig.get("toolkit", [])),
                "bonus": len(sig.get("bonus", [])),
            })
            logger.info(f"[Admin] Generated signature for '{p['title']}'")
        except Exception as e:
            results.append({"id": p["id"], "title": p["title"], "error": str(e)})
    return {"ok": True, "profiles": results}


@app.post("/api/admin/reclassify")
async def api_reclassify():
    """Admin: re-run the v2.2.0 AI relevance gate over the whole board.
    Classifies every active/hidden job against its profile (title + JD via
    batched Haiku) and hides the off-target ones — cleaning the current board
    in one pass. User-saved/applied jobs are left untouched."""
    from database import get_jobs_for_reclassify
    profiles = await get_profiles()
    if not profiles:
        return {"status": "no_profiles"}
    jobs = await get_jobs_for_reclassify()
    scored, hidden = await _classify_and_store(jobs, profiles)
    return {
        "status": "ok",
        "classified": scored,
        "hidden": hidden,
        "kept": scored - hidden,
    }


@app.post("/api/admin/rescore-all-jobs")
async def api_rescore_all_jobs(threshold: int = 40):
    """Admin: re-score every job in the DB against all profiles using the
    current (v1.9.5+) fuzzy scorer with role-family fence. Jobs that score
    below `threshold` against every profile are archived. This flushes the
    noise that was scored under the looser v1.9.4 and earlier rules."""
    from database import get_db
    from ai_engine import score_relevance_fuzzy, _sanitize_expansions
    profiles = await get_profiles()
    if not profiles:
        return {"error": "no profiles"}

    # Prebuild clean profile data (with skill signature for description rescue)
    from ai_engine import _get_fallback_signature
    pds = []
    for p in profiles:
        kws = p.get("keywords", [])
        if isinstance(kws, str):
            kws = [k.strip() for k in kws.split(",") if k.strip()]
        sig = p.get("skill_signature") or _get_fallback_signature(p["title"])
        pds.append({
            "title": p["title"],
            "expanded": _sanitize_expansions(p["title"], p.get("expanded_titles", []) or []),
            "keywords": kws,
            "signature": sig,
        })

    db = await get_db()
    try:
        cur = await db.execute("SELECT id, title, description FROM jobs WHERE status != 'archived'")
        rows = await cur.fetchall()
        rescored = 0
        archived = 0
        for row in rows:
            jid, jtitle, jdesc = row[0], row[1] or "", row[2] or ""
            best = 0
            for pd in pds:
                s = score_relevance_fuzzy(
                    jtitle, jdesc, pd["title"], pd["expanded"], pd["keywords"],
                    skill_signature=pd.get("signature"),
                )
                if s > best:
                    best = s
            await db.execute("UPDATE jobs SET relevance_score = ? WHERE id = ?", (best, jid))
            rescored += 1
            if best < threshold:
                await db.execute("UPDATE jobs SET status = 'archived' WHERE id = ?", (jid,))
                archived += 1
        await db.commit()
        return {"ok": True, "rescored": rescored, "archived": archived, "threshold": threshold}
    except Exception as e:
        logger.exception("[Admin] rescore failed")
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        await db.close()


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


@app.post("/api/admin/ats-discover")
async def api_ats_discover():
    """Trigger an on-demand ATS auto-discovery pass.

    Scans recent job URLs in the DB, extracts ATS slugs, verifies them
    against live APIs, and appends any newly-verified companies to the list.
    Returns stats about the run (scanned, candidates, new, failed).
    """
    try:
        from ats_discovery import discover_new_ats_companies
        stats = await discover_new_ats_companies()
        return {"ok": True, **stats}
    except Exception as e:
        logger.exception("[Admin] ats-discover failed")
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


@app.get("/api/discovery-stats")
async def api_discovery_stats():
    """Roster size = static file companies + companies found by the AI bot."""
    from collections import Counter
    try:
        from ats_scraper import load_companies
        from database import get_discovered_companies
        file_c = load_companies()
        disc_c = await get_discovered_companies()
        seen = {(c["slug"].lower(), c["ats"].lower()) for c in file_c}
        disc_new = [c for c in disc_c if (c["slug"].lower(), c["ats"].lower()) not in seen]
        by_ats = Counter(c["ats"] for c in file_c)
        for c in disc_new:
            by_ats[c["ats"]] += 1
        return {
            "total_companies": len(file_c) + len(disc_new),
            "base_companies": len(file_c),
            "discovered_by_ai": len(disc_new),
            "by_ats": dict(by_ats),
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
