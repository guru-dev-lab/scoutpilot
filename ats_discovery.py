"""
ATS auto-discovery — grow the company list automatically.

Reads URLs from jobs already in the database (from Indeed, LinkedIn, JobSpy,
and any existing source), matches them against known ATS URL patterns to
extract candidate slugs, dedupes against the current ``ats_companies.json``,
then verifies each new candidate against the live ATS API before adding it.

This is what makes ScoutPilot a self-growing job scraper — every new company
hiring through any of the 5 supported ATS platforms (Greenhouse, Lever, Ashby,
Workday, SmartRecruiters) eventually shows up here the moment they publish a
role to any other board we're already scraping.

Designed to be fully side-effect free on failure: if ANY step breaks, the
existing company list is left untouched.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Optional

import httpx

from database import get_db
from ats_scraper import COMPANIES_FILE, load_companies, save_companies, HTTP_HEADERS

logger = logging.getLogger("scoutpilot.ats.discovery")


# ─────────────────────────────────────────────────────────────────────────────
# URL pattern extractors
# Each returns either None or a candidate dict ready for verification.
# ─────────────────────────────────────────────────────────────────────────────

# Greenhouse: multiple URL shapes observed in the wild
#   https://boards.greenhouse.io/{slug}/jobs/{id}
#   https://boards.greenhouse.io/embed/job_app?for={slug}&token=...
#   https://job-boards.greenhouse.io/{slug}/jobs/...
#   https://{slug}.greenhouse.io/...
#   https://boards.eu.greenhouse.io/{slug}/jobs/...
_GREENHOUSE_PATTERNS = [
    re.compile(r"https?://boards(?:\.eu)?\.greenhouse\.io/([a-z0-9][a-z0-9_-]+)/", re.I),
    re.compile(r"https?://job-boards\.greenhouse\.io/([a-z0-9][a-z0-9_-]+)/", re.I),
    re.compile(r"https?://boards\.greenhouse\.io/embed/job_app\?.*?for=([a-z0-9][a-z0-9_-]+)", re.I),
    re.compile(r"https?://([a-z0-9][a-z0-9_-]+)\.greenhouse\.io/", re.I),
]

_GREENHOUSE_IGNORE_SLUGS = {"embed", "job_app", "boards", "job-boards", "www", "api", "eu"}


def extract_greenhouse(url: str) -> Optional[str]:
    if not url:
        return None
    for pat in _GREENHOUSE_PATTERNS:
        m = pat.search(url)
        if m:
            slug = m.group(1).lower()
            if slug not in _GREENHOUSE_IGNORE_SLUGS and len(slug) >= 2:
                return slug
    return None


# Lever: https://jobs.lever.co/{slug}/{uuid} or https://jobs.eu.lever.co/{slug}/...
_LEVER_PAT = re.compile(r"https?://jobs(?:\.eu)?\.lever\.co/([a-z0-9][a-z0-9_-]+)", re.I)


def extract_lever(url: str) -> Optional[str]:
    if not url:
        return None
    m = _LEVER_PAT.search(url)
    if m:
        return m.group(1).lower()
    return None


# Ashby: https://jobs.ashbyhq.com/{slug}/{id} or https://{slug}.ashbyhq.com/...
_ASHBY_PATTERNS = [
    re.compile(r"https?://jobs\.ashbyhq\.com/([a-z0-9][a-z0-9_.-]+)", re.I),
    re.compile(r"https?://([a-z0-9][a-z0-9_-]+)\.ashbyhq\.com/", re.I),
]


def extract_ashby(url: str) -> Optional[str]:
    if not url:
        return None
    for pat in _ASHBY_PATTERNS:
        m = pat.search(url)
        if m:
            slug = m.group(1).lower()
            if slug not in ("www", "api", "jobs"):
                return slug
    return None


# Workday: https://{tenant}.wd{N}.myworkdayjobs.com/en-US/{site}/job/...
#          https://{tenant}.wd{N}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/...
#          https://{tenant}.myworkdayjobs.com/en-US/{site}/...  (no wd prefix)
_WORKDAY_PAT_FULL = re.compile(
    r"https?://([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/(?:en-US/)?([A-Za-z0-9_.-]+)/",
    re.I,
)
_WORKDAY_PAT_NOWD = re.compile(
    r"https?://([a-z0-9-]+)\.myworkdayjobs\.com/(?:en-US/)?([A-Za-z0-9_.-]+)/",
    re.I,
)


def extract_workday(url: str) -> Optional[dict]:
    """Return {tenant, wd, site} dict or None."""
    if not url:
        return None
    m = _WORKDAY_PAT_FULL.search(url)
    if m:
        return {
            "tenant": m.group(1).lower(),
            "wd": m.group(2).lower(),
            "site": m.group(3),
        }
    m = _WORKDAY_PAT_NOWD.search(url)
    if m:
        # No wd# in URL — we'll need to probe wd1..wd12 to find it
        return {
            "tenant": m.group(1).lower(),
            "wd": None,
            "site": m.group(2),
        }
    return None


# SmartRecruiters: https://jobs.smartrecruiters.com/{slug}/{posting-id}
#                  https://careers.smartrecruiters.com/{slug}/...
_SMARTRECRUITERS_PAT = re.compile(
    r"https?://(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9_-]+)", re.I
)


def extract_smartrecruiters(url: str) -> Optional[str]:
    if not url:
        return None
    m = _SMARTRECRUITERS_PAT.search(url)
    if m:
        slug = m.group(1)
        if slug.lower() not in ("www", "api", "oneclick-ui"):
            return slug
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Slug extraction across all ATS — returns list of (ats, candidate_dict)
# ─────────────────────────────────────────────────────────────────────────────

def extract_candidates_from_url(url: str) -> list[tuple[str, dict]]:
    """Given a job URL, return [(ats, candidate)] for any matches."""
    if not url:
        return []
    out: list[tuple[str, dict]] = []

    if (s := extract_greenhouse(url)):
        out.append(("greenhouse", {"slug": s}))
    if (s := extract_lever(url)):
        out.append(("lever", {"slug": s}))
    if (s := extract_ashby(url)):
        out.append(("ashby", {"slug": s}))
    if (wd := extract_workday(url)):
        out.append(("workday", wd))
    if (s := extract_smartrecruiters(url)):
        out.append(("smartrecruiters", {"slug": s}))

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Per-platform live verification
# ─────────────────────────────────────────────────────────────────────────────

async def _verify_greenhouse(client: httpx.AsyncClient, slug: str) -> bool:
    try:
        r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
        if r.status_code != 200:
            return False
        d = r.json()
        return isinstance(d, dict) and len(d.get("jobs", [])) > 0
    except Exception:
        return False


async def _verify_lever(client: httpx.AsyncClient, slug: str) -> bool:
    try:
        r = await client.get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if r.status_code != 200:
            return False
        d = r.json()
        return isinstance(d, list) and len(d) > 0
    except Exception:
        return False


async def _verify_ashby(client: httpx.AsyncClient, slug: str) -> bool:
    try:
        r = await client.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        if r.status_code != 200:
            return False
        d = r.json()
        return isinstance(d, dict) and len(d.get("jobs", [])) > 0
    except Exception:
        return False


_WD_VARIANTS = ["wd1", "wd3", "wd5", "wd12", "wd2", "wd103"]


async def _verify_workday(client: httpx.AsyncClient, cand: dict) -> Optional[dict]:
    """Returns filled-in {tenant, wd, site, total} or None.

    If ``wd`` is None in the input, probes all known wd# subdomains to find
    the one that responds.
    """
    tenant = cand["tenant"]
    site = cand["site"]
    wds = [cand["wd"]] if cand.get("wd") else _WD_VARIANTS

    for wd in wds:
        try:
            url = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
            r = await client.post(
                url,
                json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
                timeout=15,
            )
            if r.status_code == 200:
                total = r.json().get("total", 0)
                if total > 0:
                    return {"tenant": tenant, "wd": wd, "site": site, "total": total}
        except Exception:
            continue
    return None


async def _verify_smartrecruiters(client: httpx.AsyncClient, slug: str) -> bool:
    try:
        r = await client.get(
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1"
        )
        if r.status_code != 200:
            return False
        d = r.json()
        return d.get("totalFound", 0) > 0
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Main discovery run
# ─────────────────────────────────────────────────────────────────────────────

# How many recent jobs to scan each discovery pass
JOBS_SCAN_LIMIT = 3000

# Hard cap on new companies added in a single pass (safety net)
MAX_NEW_PER_PASS = 50


async def _load_recent_job_urls(limit: int = JOBS_SCAN_LIMIT) -> list[tuple[str, str]]:
    """Read recent (company_name, url) tuples from the jobs table.

    Pulls both direct_apply_url and source_url so we don't miss ATS signals
    hidden inside either field.
    """
    out: list[tuple[str, str]] = []
    try:
        db = await get_db()
        try:
            async with db.execute(
                """SELECT company_name, direct_apply_url, source_url
                   FROM jobs
                   ORDER BY first_seen_at DESC
                   LIMIT ?""",
                (limit,),
            ) as cur:
                async for row in cur:
                    cn = row["company_name"] or ""
                    for url in (row["direct_apply_url"], row["source_url"]):
                        if url:
                            out.append((cn, url))
        finally:
            await db.close()
    except Exception as e:
        logger.warning(f"[Discovery] failed reading jobs: {e}")
    return out


async def discover_new_ats_companies() -> dict:
    """Run one discovery pass. Returns a dict with stats and any new additions.

    Flow:
      1. Pull recent job URLs from DB
      2. Extract ATS candidates via regex
      3. Dedupe against the on-disk ats_companies.json
      4. Verify each new candidate against the live ATS API (concurrent)
      5. Append verified ones to the list and persist atomically
    """
    stats = {
        "scanned_urls": 0,
        "candidates_found": 0,
        "already_known": 0,
        "new_verified": 0,
        "new_failed_verify": 0,
        "added": [],
        "errors": [],
    }

    try:
        rows = await _load_recent_job_urls()
        stats["scanned_urls"] = len(rows)

        # Extract candidates with a mapping back to the company name seen in DB
        # (so we can label the entry nicely when adding it)
        raw_candidates: dict[tuple[str, str], dict] = {}
        for company_name, url in rows:
            for ats, cand in extract_candidates_from_url(url):
                key = (ats, cand.get("slug") or f"{cand.get('tenant')}::{cand.get('site')}")
                if key not in raw_candidates:
                    entry = {"ats": ats, "name_hint": company_name, **cand}
                    raw_candidates[key] = entry
        stats["candidates_found"] = len(raw_candidates)

        # Dedupe against existing list
        existing = load_companies()
        existing_keys: set[tuple[str, str]] = set()
        for e in existing:
            ats = e.get("ats")
            if ats == "workday":
                # Workday dedupe key includes tenant + site
                existing_keys.add(
                    ("workday", f"{e.get('tenant')}::{e.get('site')}")
                )
            else:
                existing_keys.add((ats, (e.get("slug") or "").lower()))

        # Filter to unknown candidates only
        unknown: list[dict] = []
        for key, cand in raw_candidates.items():
            # Normalize the dedupe key for non-workday
            if cand["ats"] == "workday":
                lookup_key = ("workday", f"{cand['tenant']}::{cand['site']}")
            else:
                lookup_key = (cand["ats"], (cand.get("slug") or "").lower())
            if lookup_key in existing_keys:
                stats["already_known"] += 1
            else:
                unknown.append(cand)

        if not unknown:
            logger.info(f"[Discovery] nothing new to verify ({stats['already_known']} already known)")
            return stats

        logger.info(
            f"[Discovery] scanning {len(rows)} job URLs → "
            f"{stats['candidates_found']} candidates, "
            f"{stats['already_known']} known, "
            f"verifying {len(unknown)} new"
        )

        # Verify concurrently with a per-platform cap
        sem = asyncio.Semaphore(10)
        verified_new: list[dict] = []

        async with httpx.AsyncClient(
            timeout=15,
            headers=HTTP_HEADERS,
            follow_redirects=True,
        ) as client:

            async def one(cand):
                async with sem:
                    ats = cand["ats"]
                    try:
                        if ats == "greenhouse":
                            ok = await _verify_greenhouse(client, cand["slug"])
                            if ok:
                                return {
                                    "name": cand.get("name_hint") or cand["slug"],
                                    "slug": cand["slug"],
                                    "ats": "greenhouse",
                                }
                        elif ats == "lever":
                            ok = await _verify_lever(client, cand["slug"])
                            if ok:
                                return {
                                    "name": cand.get("name_hint") or cand["slug"],
                                    "slug": cand["slug"],
                                    "ats": "lever",
                                }
                        elif ats == "ashby":
                            ok = await _verify_ashby(client, cand["slug"])
                            if ok:
                                return {
                                    "name": cand.get("name_hint") or cand["slug"],
                                    "slug": cand["slug"],
                                    "ats": "ashby",
                                }
                        elif ats == "workday":
                            result = await _verify_workday(client, cand)
                            if result:
                                return {
                                    "name": cand.get("name_hint") or cand["tenant"],
                                    "slug": result["tenant"],
                                    "ats": "workday",
                                    "tenant": result["tenant"],
                                    "wd": result["wd"],
                                    "site": result["site"],
                                }
                        elif ats == "smartrecruiters":
                            ok = await _verify_smartrecruiters(client, cand["slug"])
                            if ok:
                                return {
                                    "name": cand.get("name_hint") or cand["slug"],
                                    "slug": cand["slug"],
                                    "ats": "smartrecruiters",
                                }
                    except Exception as e:
                        logger.debug(f"[Discovery] verify {ats} failed: {e}")
                    return None

            results = await asyncio.gather(*[one(c) for c in unknown])

        for r in results:
            if r:
                verified_new.append(r)
            else:
                stats["new_failed_verify"] += 1

        # Cap + persist
        if verified_new:
            verified_new = verified_new[:MAX_NEW_PER_PASS]
            combined = existing + verified_new
            # Final dedupe pass on combined
            seen: set = set()
            unique = []
            for e in combined:
                if e.get("ats") == "workday":
                    k = ("workday", f"{e.get('tenant')}::{e.get('site')}")
                else:
                    k = (e.get("ats"), (e.get("slug") or "").lower())
                if k not in seen:
                    seen.add(k)
                    unique.append(e)
            unique.sort(key=lambda x: (x.get("ats", ""), x.get("name", "")))

            if save_companies(unique):
                stats["new_verified"] = len(verified_new)
                stats["added"] = [
                    {"name": v["name"], "slug": v.get("slug"), "ats": v["ats"]}
                    for v in verified_new
                ]
                logger.info(
                    f"[Discovery] added {len(verified_new)} new ATS companies: "
                    + ", ".join(f"{v['ats']}:{v['name']}" for v in verified_new[:10])
                    + (" ..." if len(verified_new) > 10 else "")
                )
            else:
                stats["errors"].append("save_companies returned False")
        else:
            logger.info(f"[Discovery] 0 verified out of {len(unknown)} new candidates")

    except Exception as e:
        logger.error(f"[Discovery] crashed: {e}")
        stats["errors"].append(str(e))

    return stats
