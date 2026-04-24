"""
ATS scraper — direct fetches from company Applicant Tracking Systems.

Supports Greenhouse, Lever, and Ashby public job board APIs. These endpoints
return jobs directly from the company (no aggregator middleman), with real
direct-apply URLs. No API keys required.

Company lists live in sources/ats_companies.json and are hot-reloaded each
cycle. Companies are sliced into rotation buckets so we only hit a subset
per 5-min cycle, spreading load across ~30 minutes.

Ships fully isolated from the rest of scraper.py — failures here cannot
break existing sources.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Optional

import httpx

from database import insert_job, get_enabled_sources
from scraper import _is_direct_url, _is_blocked_company, _normalize_posted_at

logger = logging.getLogger("scoutpilot.ats")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

COMPANIES_FILE = Path(__file__).parent / "sources" / "ats_companies.json"

# How many rotation buckets. Full company list is covered every
# ROTATION_BUCKETS × cycle_interval minutes.
# v1.9.7: reduced from 6 → 3 so all 206 companies are covered every
# ~15 min instead of ~30 min. ATS APIs are free and fast (no anti-bot).
ROTATION_BUCKETS = 3

# Max concurrent HTTP fetches per ATS platform
# v1.9.7: bumped from 15 → 25 to match the faster rotation
PLATFORM_CONCURRENCY = 25

# Hard cap on inserts per ATS platform per cycle (safety net)
MAX_INSERTS_PER_PLATFORM_PER_CYCLE = 300

# Per-request timeout (seconds)
HTTP_TIMEOUT = 25

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 ScoutPilot/1.0 (job aggregator; +https://scoutpilot.app)",
    "Accept": "application/json",
}

# ─────────────────────────────────────────────────────────────────────────────
# US + remote detection
# ─────────────────────────────────────────────────────────────────────────────

# Strings that indicate the role is US-eligible (case-insensitive substring match)
_US_TOKENS = [
    " us", " us ", "u.s.", "u.s ", "usa", "united states", "america",
    "north america", "americas", "remote - us", "remote, us", "remote (us",
    "worldwide", "anywhere", "global",
]

# US state names and abbreviations (for locations like "Remote, NY")
_US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
}
_US_STATE_ABBR = {
    " al", " ak", " az", " ar", " ca", " co", " ct", " de", " fl", " ga",
    " hi", " id", " il", " in", " ia", " ks", " ky", " la", " me", " md",
    " ma", " mi", " mn", " ms", " mo", " mt", " ne", " nv", " nh", " nj",
    " nm", " ny", " nc", " nd", " oh", " ok", " or", " pa", " ri", " sc",
    " sd", " tn", " tx", " ut", " vt", " va", " wa", " wv", " wi", " wy",
    " dc",
}

# Tokens that clearly disqualify a location as NOT US
_NON_US_TOKENS = [
    "emea", "apac", "latam", "india", "pakistan", "bangladesh", "vietnam",
    "philippines", "indonesia", "malaysia", "thailand", "singapore",
    "hong kong", "taiwan", "japan", "korea", "china", "australia",
    "new zealand", "united kingdom", "uk only", "england", "scotland",
    "wales", "ireland", "germany", "france", "spain", "italy", "portugal",
    "netherlands", "belgium", "switzerland", "austria", "poland", "sweden",
    "norway", "finland", "denmark", "greece", "turkey", "israel", "uae",
    "saudi", "egypt", "south africa", "nigeria", "kenya", "brazil",
    "argentina", "chile", "colombia", "mexico", "canada only", "canada,",
]


def is_us_location(location: str) -> bool:
    """Heuristic: does this location string indicate a US-eligible role?

    Returns True for explicit US mentions, US states, Worldwide/Anywhere.
    Returns False if a non-US country/region is clearly named without US.
    """
    if not location:
        return False
    loc = f" {location.lower().strip()} "

    # Fast allow: worldwide/anywhere/global = US-eligible
    for tok in ("worldwide", "anywhere", "global"):
        if tok in loc:
            return True

    # Fast reject: clearly non-US with no US mention
    non_us_hit = any(tok in loc for tok in _NON_US_TOKENS)
    us_hit = any(tok in loc for tok in _US_TOKENS)
    if non_us_hit and not us_hit:
        return False

    if us_hit:
        return True

    # Check US state names
    if any(state in loc for state in _US_STATES):
        return True

    # Check US state abbreviations (requires space-bounded match)
    if any(abbr in loc for abbr in _US_STATE_ABBR):
        return True

    return False


def is_remote_string(s: str) -> bool:
    """Does a location/title/tag string signal 'remote'?"""
    if not s:
        return False
    sl = s.lower()
    return "remote" in sl or "anywhere" in sl or "work from home" in sl


# ─────────────────────────────────────────────────────────────────────────────
# Company list loader
# ─────────────────────────────────────────────────────────────────────────────

def load_companies() -> list[dict]:
    """Load the ATS company list from disk. Returns empty list on any failure."""
    try:
        if not COMPANIES_FILE.exists():
            logger.warning(f"[ATS] Company file not found: {COMPANIES_FILE}")
            return []
        with open(COMPANIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.error(f"[ATS] Company file is not a list")
            return []
        return [c for c in data if isinstance(c, dict) and "slug" in c and "ats" in c]
    except Exception as e:
        logger.error(f"[ATS] Failed to load company file: {e}")
        return []


def save_companies(companies: list[dict]) -> bool:
    """Persist the company list back to disk (for admin endpoint use)."""
    try:
        COMPANIES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(COMPANIES_FILE, "w", encoding="utf-8") as f:
            json.dump(companies, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"[ATS] Failed to save company file: {e}")
        return False


def get_rotation_slice(companies: list[dict], cycle_number: int, platform: str) -> list[dict]:
    """Return the subset of companies for this ATS platform to fetch this cycle.

    Uses (cycle_number % ROTATION_BUCKETS) as the bucket index, so every
    company gets fetched every ROTATION_BUCKETS cycles.
    """
    platform_companies = [c for c in companies if c.get("ats") == platform]
    if not platform_companies:
        return []
    bucket_idx = cycle_number % ROTATION_BUCKETS
    return [c for i, c in enumerate(platform_companies) if i % ROTATION_BUCKETS == bucket_idx]


# ─────────────────────────────────────────────────────────────────────────────
# Title matching (reuse same fuzzy matching approach as existing sources)
# ─────────────────────────────────────────────────────────────────────────────

def _title_matches_profile(title: str, search_terms: list[str]) -> bool:
    """True if any word from any search term appears in title."""
    if not search_terms:
        return True  # No terms = accept everything
    title_lower = title.lower()
    for term in search_terms:
        term_lower = term.lower().replace(" remote", "").strip()
        words = [w for w in term_lower.split() if len(w) > 2]
        if not words:
            continue
        if all(w in title_lower for w in words):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Greenhouse adapter
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_greenhouse(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Greenhouse company board."""
    slug = company["slug"]
    company_name = company.get("name", slug)

    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []  # Company doesn't use Greenhouse anymore
        if resp.status_code != 200:
            logger.warning(f"[Greenhouse:{slug}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[Greenhouse:{slug}] fetch error: {e}")
        return []

    jobs_raw = data.get("jobs", []) or []
    inserted: list[dict] = []

    for item in jobs_raw:
        try:
            title = (item.get("title") or "").strip()
            if not title:
                continue

            loc_name = ((item.get("location") or {}).get("name") or "").strip()

            # Remote + US filter
            if not is_remote_string(loc_name):
                continue
            if not is_us_location(loc_name):
                continue

            # Title relevance
            if not _title_matches_profile(title, search_terms):
                continue

            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("absolute_url") or ""
            if not apply_url:
                continue

            content_html = item.get("content") or ""
            # Greenhouse content is HTML-encoded HTML — decode + strip tags
            content_html = content_html.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
            clean_desc = re.sub(r"<[^>]+>", " ", content_html)
            clean_desc = re.sub(r"\s+", " ", clean_desc).strip()

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_name,
                "is_remote": True,
                "work_type": "remote",
                "description": clean_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "greenhouse",
                "source_url": apply_url,
                "direct_apply_url": apply_url,  # Greenhouse boards are direct apply
                "posted_at": _normalize_posted_at(item.get("updated_at") or item.get("first_published") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Greenhouse:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Greenhouse:{slug}] +{len(inserted)} new jobs ({len(jobs_raw)} total on board)")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Lever adapter
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_lever(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Lever company board."""
    slug = company["slug"]
    company_name = company.get("name", slug)

    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[Lever:{slug}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[Lever:{slug}] fetch error: {e}")
        return []

    if not isinstance(data, list):
        return []

    inserted: list[dict] = []

    for item in data:
        try:
            title = (item.get("text") or "").strip()
            if not title:
                continue

            workplace_type = (item.get("workplaceType") or "").lower()
            categories = item.get("categories") or {}
            loc = (categories.get("location") or "").strip()
            all_locations = categories.get("allLocations") or []

            # Remote: workplaceType == 'remote' OR location contains 'remote'
            is_remote = workplace_type == "remote" or is_remote_string(loc)
            if not is_remote:
                continue

            # US filter: check primary location and all secondary locations
            all_loc_str = " | ".join([loc] + list(all_locations))
            if not is_us_location(all_loc_str):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("applyUrl") or item.get("hostedUrl") or ""
            if not apply_url:
                continue

            desc = item.get("descriptionPlain") or ""
            additional = item.get("additionalPlain") or ""
            full_desc = (desc + "\n\n" + additional).strip()

            posted_ts = item.get("createdAt")
            if isinstance(posted_ts, (int, float)):
                from datetime import datetime as dt, timezone as tz
                posted_at = dt.fromtimestamp(posted_ts / 1000, tz=tz.utc).isoformat()
            else:
                posted_at = ""

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc or "Remote",
                "is_remote": True,
                "work_type": "remote",
                "description": full_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "lever",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": posted_at,
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Lever:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Lever:{slug}] +{len(inserted)} new jobs ({len(data)} total on board)")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Ashby adapter
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_ashby(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Ashby company board."""
    slug = company["slug"]
    company_name = company.get("name", slug)

    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[Ashby:{slug}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[Ashby:{slug}] fetch error: {e}")
        return []

    jobs_raw = data.get("jobs", []) or []
    inserted: list[dict] = []

    for item in jobs_raw:
        try:
            title = (item.get("title") or "").strip()
            if not title:
                continue

            if not item.get("isListed", True):
                continue

            workplace_type = (item.get("workplaceType") or "").lower()
            loc = (item.get("location") or "").strip()
            secondary = item.get("secondaryLocations") or []
            sec_locs = []
            for s in secondary:
                if isinstance(s, dict):
                    sec_locs.append(s.get("location", ""))
                elif isinstance(s, str):
                    sec_locs.append(s)

            # Strict remote: workplaceType must be 'remote' (Ashby's isRemote flag
            # is too loose — includes hybrids that can be done remotely)
            is_remote = workplace_type == "remote"
            if not is_remote:
                continue

            all_loc_str = " | ".join([loc] + sec_locs)
            if not is_us_location(all_loc_str):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("applyUrl") or item.get("jobUrl") or ""
            if not apply_url:
                continue

            desc = item.get("descriptionPlain") or ""

            # Extract comp (Ashby actually provides it!)
            salary_min = 0
            salary_max = 0
            comp = item.get("compensation") or {}
            if isinstance(comp, dict):
                summary = comp.get("compensationTierSummary") or ""
                # Parse strings like "$150K – $200K" or "150000 - 200000 USD"
                nums = re.findall(r"\$?(\d+(?:\.\d+)?)\s*[Kk]?", summary)
                if len(nums) >= 2:
                    def _to_int(s):
                        try:
                            v = float(s)
                            if "k" in summary.lower() or "K" in summary:
                                v *= 1000
                            return int(v)
                        except Exception:
                            return 0
                    salary_min = _to_int(nums[0])
                    salary_max = _to_int(nums[1])

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc or "Remote",
                "is_remote": True,
                "work_type": "remote",
                "description": desc[:10000],
                "salary_min": salary_min,
                "salary_max": salary_max,
                "source": "ashby",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(item.get("publishedAt") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Ashby:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Ashby:{slug}] +{len(inserted)} new jobs ({len(jobs_raw)} total on board)")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Workday adapter
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_workday(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a Workday tenant's public CxS jobs endpoint.

    Workday entries in ats_companies.json must include either:
      - ``workday_url``: full base (e.g. https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite)
      - OR the three parts ``tenant``, ``wd``, ``site``.

    The public list endpoint returns 'postings' with title, locationsText,
    externalPath, postedOn. We search with ``searchText='Remote'`` to narrow
    results to remote-eligible roles, then filter locationsText for US markers.
    """
    slug = company.get("slug") or company.get("tenant") or ""
    company_name = company.get("name", slug)

    # Build the base URL
    base = company.get("workday_url") or ""
    if not base:
        tenant = company.get("tenant")
        wd = company.get("wd")
        site = company.get("site")
        if not (tenant and wd and site):
            logger.warning(f"[Workday:{slug}] missing workday_url or (tenant,wd,site)")
            return []
        base = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}"

    list_url = base.rstrip("/") + "/jobs"

    # Derive the user-facing apply URL root (no /wday/cxs prefix)
    # e.g. https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite{externalPath}
    apply_root = ""
    m = re.match(r"(https?://[^/]+)/wday/cxs/([^/]+)/([^/]+)", base)
    if m:
        host = m.group(1)
        site_name = m.group(3)
        apply_root = f"{host}/en-US/{site_name}"

    postings_raw: list[dict] = []
    try:
        # Page through up to 3 × 20 = 60 most recent remote postings
        for offset in (0, 20, 40):
            resp = await client.post(
                list_url,
                json={
                    "appliedFacets": {},
                    "limit": 20,
                    "offset": offset,
                    "searchText": "Remote",
                },
            )
            if resp.status_code != 200:
                if offset == 0:
                    logger.warning(f"[Workday:{slug}] HTTP {resp.status_code}")
                break
            data = resp.json()
            batch = data.get("jobPostings", []) or []
            postings_raw.extend(batch)
            if len(batch) < 20:
                break
    except Exception as e:
        logger.warning(f"[Workday:{slug}] fetch error: {e}")
        return []

    inserted: list[dict] = []

    for item in postings_raw:
        try:
            title = (item.get("title") or "").strip()
            if not title:
                continue

            loc_text = (item.get("locationsText") or "").strip()
            loc_low = loc_text.lower()

            # Accept if any of: locationsText says 'remote'/'united states'/state,
            # OR it's a multi-location posting (then we assume US-based Workday
            # customers include US as one of the locations, since we already
            # searchText='Remote' filtered).
            is_multi_loc = "location" in loc_low and any(ch.isdigit() for ch in loc_low)
            has_remote = "remote" in loc_low or is_multi_loc
            if not has_remote:
                continue
            if not (is_multi_loc or is_us_location(loc_text)):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            ext_path = item.get("externalPath") or ""
            if not ext_path:
                continue
            apply_url = (apply_root + ext_path) if apply_root else ""
            if not apply_url:
                continue

            # Description isn't in the list endpoint — leave minimal but non-empty
            # so downstream systems don't drop it for blankness.
            desc = f"{title} at {company_name}. Apply directly on Workday."

            posted_on = (item.get("postedOn") or "").strip()

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_text or "Remote",
                "is_remote": True,
                "work_type": "remote",
                "description": desc,
                "salary_min": 0,
                "salary_max": 0,
                "source": "workday",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(posted_on),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Workday:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Workday:{slug}] +{len(inserted)} new jobs ({len(postings_raw)} remote postings on board)")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# SmartRecruiters adapter
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_smartrecruiters(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a SmartRecruiters company public posting API.

    Uses the public ``/v1/companies/{slug}/postings`` endpoint, filtered to
    ``country=us`` to cut down volume. Only items with ``location.remote=True``
    are kept.
    """
    slug = company["slug"]
    company_name = company.get("name", slug)

    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    params = {"country": "us", "limit": 100, "offset": 0}

    all_items: list[dict] = []
    try:
        for _ in range(3):  # up to 300 postings per company per cycle
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                if params["offset"] == 0:
                    logger.warning(f"[SmartRecruiters:{slug}] HTTP {resp.status_code}")
                break
            data = resp.json()
            batch = data.get("content", []) or []
            all_items.extend(batch)
            if len(batch) < params["limit"]:
                break
            params["offset"] += params["limit"]
    except Exception as e:
        logger.warning(f"[SmartRecruiters:{slug}] fetch error: {e}")
        return []

    inserted: list[dict] = []

    for item in all_items:
        try:
            title = (item.get("name") or "").strip()
            if not title:
                continue

            loc = item.get("location") or {}
            if not loc.get("remote"):
                continue

            country = (loc.get("country") or "").lower()
            full_loc = loc.get("fullLocation") or f"{loc.get('city','')}, {loc.get('region','')}"
            # Accept only if country is us OR full location is US-eligible
            if country != "us" and not is_us_location(full_loc):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("applyUrl") or item.get("postingUrl") or ""
            if not apply_url:
                continue

            desc = f"{title} at {company_name}. Remote role in {full_loc}."

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": full_loc or "Remote, US",
                "is_remote": True,
                "work_type": "remote",
                "description": desc,
                "salary_min": 0,
                "salary_max": 0,
                "source": "smartrecruiters",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(item.get("releasedDate") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[SmartRecruiters:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[SmartRecruiters:{slug}] +{len(inserted)} new jobs ({len(all_items)} US postings on board)")
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Platform dispatcher
# ─────────────────────────────────────────────────────────────────────────────

_PLATFORM_FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "workday": fetch_workday,
    "smartrecruiters": fetch_smartrecruiters,
}


async def _fetch_platform(
    platform: str,
    companies: list[dict],
    profile_id: Optional[int],
    search_terms: list[str],
) -> int:
    """Fetch all companies for one ATS platform with concurrency limit and cap."""
    if not companies:
        return 0

    fetcher = _PLATFORM_FETCHERS.get(platform)
    if not fetcher:
        return 0

    sem = asyncio.Semaphore(PLATFORM_CONCURRENCY)
    total_inserted = 0

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
        headers=HTTP_HEADERS,
        follow_redirects=True,
    ) as client:

        async def _one(company):
            async with sem:
                try:
                    return await fetcher(client, company, profile_id, search_terms)
                except Exception as e:
                    logger.warning(f"[{platform}:{company.get('slug')}] crashed: {e}")
                    return []

        results = await asyncio.gather(*[_one(c) for c in companies], return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                total_inserted += len(r)
                if total_inserted >= MAX_INSERTS_PER_PLATFORM_PER_CYCLE:
                    logger.warning(
                        f"[{platform}] hit per-cycle cap "
                        f"({MAX_INSERTS_PER_PLATFORM_PER_CYCLE}), stopping early"
                    )
                    break

    return total_inserted


async def scrape_all_ats(
    profile_id: Optional[int],
    search_terms: list[str],
    cycle_number: int,
) -> dict:
    """Run all enabled ATS platforms for one profile, one cycle.

    Returns {platform: insert_count} dict. Never raises — caller can rely on
    this function being safe. Any per-company failures are swallowed and logged.
    """
    results: dict[str, int] = {}
    try:
        enabled = await get_enabled_sources()
    except Exception as e:
        logger.error(f"[ATS] Failed to read enabled sources: {e}")
        return results

    active_platforms = [p for p in _PLATFORM_FETCHERS.keys() if p in enabled]
    if not active_platforms:
        return results

    companies = load_companies()
    if not companies:
        logger.info(f"[ATS] No companies configured — skipping")
        return results

    for platform in active_platforms:
        slice_ = get_rotation_slice(companies, cycle_number, platform)
        if not slice_:
            results[platform] = 0
            continue
        logger.info(
            f"[ATS] cycle#{cycle_number} {platform}: "
            f"fetching {len(slice_)} companies (bucket {cycle_number % ROTATION_BUCKETS + 1}/{ROTATION_BUCKETS})"
        )
        try:
            count = await _fetch_platform(platform, slice_, profile_id, search_terms)
            results[platform] = count
        except Exception as e:
            logger.error(f"[ATS] {platform} dispatcher crashed: {e}")
            results[platform] = 0

    return results
