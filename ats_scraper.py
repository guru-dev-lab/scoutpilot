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
import time
from pathlib import Path
from typing import Optional

import httpx

from config import settings
from database import insert_job, get_enabled_sources
from scraper import _is_direct_url, _is_blocked_company, _normalize_posted_at

logger = logging.getLogger("scoutpilot.ats")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

COMPANIES_FILE = Path(__file__).parent / "sources" / "ats_companies.json"

# Rotation buckets PER PLATFORM. Each platform has its own worker now, so the
# well-behaved public APIs sweep ALL their companies every run (buckets=1 = no
# rotation). Only Workday rotates, because its WAF rate-limits aggressively and
# we share the IP with discovery — so we spread its companies over a few runs.
ROTATION_BUCKETS = 1  # default for any platform not listed below
_PLATFORM_BUCKETS = {
    "greenhouse": 1,       # public API, no rotation — scrape all every run
    "ashby": 1,
    "lever": 1,
    "smartrecruiters": 1,
    "workday": 4,          # WAF-sensitive — cover the roster over 4 runs
}

# Max concurrent HTTP fetches per ATS platform
PLATFORM_CONCURRENCY = 32

# Per-platform overrides. Workable/Recruitee/Breezy sit behind Cloudflare and
# rate-limit far more aggressively than the open Greenhouse/Lever/Ashby APIs —
# Workable returned `retry-after: 86287` (24h) after roughly 40 quick requests.
_PLATFORM_CONCURRENCY = {
    "workable": 3,
    "recruitee": 4,
    "breezy": 4,
}

# Platforms to route through the residential proxy when one is configured, so a
# single datacenter IP cannot be banned for a day. Their payloads are compact
# JSON, so the bandwidth cost is negligible compared with rendered job pages.
_PROXIED_PLATFORMS = {"workable", "recruitee", "breezy"}

# Hard cap on inserts per ATS platform per cycle (safety net)
MAX_INSERTS_PER_PLATFORM_PER_CYCLE = 1200

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


# US location detection lives in database.py — ONE implementation. Two copies
# previously drifted: the ats_scraper one was corrected while database.py's
# still let "London" through insert_job's us_only gate. ATS boards can list any
# country, so an unrecognisable location must NOT be assumed US here.
from database import is_us_location as _db_is_us_location


def is_us_location(location: str) -> bool:
    """Strict US check for ATS company boards."""
    return _db_is_us_location(location, strict=True)


def is_remote_string(s: str) -> bool:
    """Does a location/title/tag string signal 'remote'?"""
    if not s:
        return False
    sl = s.lower()
    return "remote" in sl or "anywhere" in sl or "work from home" in sl


# ─────────────────────────────────────────────────────────────────────────────
# Company list loader
# ─────────────────────────────────────────────────────────────────────────────

def _derive_work_type(platform_field: str, location: str, title: str = "") -> tuple[str, bool]:
    """Return (work_type, is_remote) for an ATS row.

    Every ATS fetcher used to hardcode work_type="remote" because they only
    ever kept remote jobs. Now that onsite and hybrid rows are kept too, the
    label has to come from real data. Lever/Ashby/Workable publish an explicit
    workplace field; the rest only give a location string, so fall back to
    reading that. Never guess "remote" from silence — an unlabelled row with a
    city in it is onsite.
    """
    f = (platform_field or "").strip().lower().replace("_", "").replace("-", "")
    if f in ("remote", "fullyremote", "remotefirst"):
        return "remote", True
    if f in ("hybrid", "flexible"):
        return "hybrid", False
    if f in ("onsite", "inoffice", "office", "inperson"):
        return "onsite", False

    blob = f"{location} {title}".lower()
    if "hybrid" in blob:
        return "hybrid", False
    if is_remote_string(blob):
        return "remote", True
    return "onsite", False


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


async def load_companies_merged() -> list[dict]:
    """File roster + companies found by the discovery bot (DB), de-duped by
    (slug, ats). This is what the scraper uses so the roster grows persistently
    without a code deploy."""
    companies = load_companies()
    seen = {(c["slug"].lower(), c["ats"].lower()) for c in companies}
    try:
        from database import get_discovered_companies
        for c in await get_discovered_companies():
            key = (c["slug"].lower(), c["ats"].lower())
            if key not in seen:
                seen.add(key)
                companies.append(c)
    except Exception as e:
        logger.error(f"[ATS] Failed to merge discovered companies: {e}")
    return companies


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
    """Companies for this platform to fetch this run. buckets=1 (the fast public
    APIs) => the whole list every run, no rotation. buckets>1 (Workday) => spread
    over that many runs so we stay under its WAF limits."""
    platform_companies = [c for c in companies if c.get("ats") == platform]
    if not platform_companies:
        return []
    buckets = _PLATFORM_BUCKETS.get(platform, ROTATION_BUCKETS)
    if buckets <= 1:
        return platform_companies
    bucket_idx = cycle_number % buckets
    return [c for i, c in enumerate(platform_companies) if i % buckets == bucket_idx]


# ─────────────────────────────────────────────────────────────────────────────
# Title matching (reuse same fuzzy matching approach as existing sources)
# ─────────────────────────────────────────────────────────────────────────────

def _title_matches_profile(title: str, search_terms: list[str]) -> bool:
    """True if at least one core search word appears in title (ANY-word matching)."""
    if not search_terms:
        return True  # No terms = accept everything
    title_lower = title.lower()
    for term in search_terms:
        # ANY-word matching — at least one search word must appear
        search_words = [w.lower() for w in term.split() if len(w) > 2]  # skip tiny words like "a", "of"
        if not search_words:
            continue
        if any(w in title_lower for w in search_words):
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
            # Keep onsite and hybrid too — the remote-only gate here was
            # discarding most of every company board.
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

            # Greenhouse publishes no workplace field, so classify from the
            # location string and title. The description is deliberately not
            # used: it is full of boilerplate like "remote-friendly culture"
            # that would mislabel plenty of onsite roles as remote.
            _gh_wt, _gh_remote = _derive_work_type("", loc_name, title)

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_name,
                "is_remote": _gh_remote,
                "work_type": _gh_wt,
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

            # Lever publishes workplaceType (remote / hybrid / on-site) — use it
            # instead of dropping everything that is not remote.
            work_type, is_remote = _derive_work_type(workplace_type, loc, title)

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
                "is_remote": is_remote,
                "work_type": work_type,
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

            # Ashby publishes workplaceType (remote / hybrid / onsite). It used
            # to be used only to reject non-remote rows; now it labels them.
            work_type, is_remote = _derive_work_type(workplace_type, loc, title)

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
                "is_remote": is_remote,
                "work_type": work_type,
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
    externalPath, postedOn. We pull the whole board (no searchText) and
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
        # Page through up to 5 × 20 = 100 most recent remote postings
        for offset in range(0, 100, 20):
            resp = await client.post(
                list_url,
                json={
                    "appliedFacets": {},
                    "limit": 20,
                    "offset": offset,
                    # Was "Remote", which filtered the feed at the API and
                    # made this a remote-only source. Empty returns the whole
                    # board; the US gate and _derive_work_type sort it out.
                    "searchText": "",
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
            # Onsite and hybrid roles are kept now; the label comes from the
            # location text rather than this having been a remote-only feed.
            _wd_wt, _wd_remote = _derive_work_type("", loc_text, title)
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
                "location": loc_text or "",
                "is_remote": _wd_remote,
                "work_type": _wd_wt,
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
            _sr_remote_flag = bool(loc.get("remote"))

            country = (loc.get("country") or "").lower()
            full_loc = loc.get("fullLocation") or f"{loc.get('city','')}, {loc.get('region','')}"
            # Accept only if country is us OR full location is US-eligible
            if country != "us" and not is_us_location(full_loc):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            # SmartRecruiters postings no longer expose applyUrl/postingUrl.
            # Build the public apply URL from company identifier + posting id:
            #   https://jobs.smartrecruiters.com/{identifier}/{id}
            company_obj = item.get("company") or {}
            company_identifier = (company_obj.get("identifier") or slug or "").strip()
            posting_id = str(item.get("id") or "").strip()
            if not (company_identifier and posting_id):
                continue
            apply_url = f"https://jobs.smartrecruiters.com/{company_identifier}/{posting_id}"

            _sr_wt, _sr_remote = _derive_work_type(
                "remote" if _sr_remote_flag else "", full_loc, title)
            desc = f"{title} at {company_name}. {_sr_wt.title()} role in {full_loc}."

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": full_loc or "",
                "is_remote": _sr_remote,
                "work_type": _sr_wt,
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

async def fetch_workable(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Workable board.

    Workable is the largest ATS we were not reading (~27k companies). The v3
    endpoint is a POST returning {total, results, nextPage}. Each row carries a
    structured location.countryCode and a `workplace` field
    (remote/hybrid/on_site), so the US gate and work-type come from real fields
    instead of being guessed from free text.
    """
    slug = company["slug"]
    company_name = company.get("name", slug)
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"

    items: list[dict] = []
    token = None
    try:
        for _ in range(5):  # page cap — these boards are small
            payload = {"token": token} if token else {}
            resp = await client.post(url, json=payload)
            if resp.status_code == 404:
                return []
            if resp.status_code != 200:
                logger.warning(f"[Workable:{slug}] HTTP {resp.status_code}")
                break
            data = resp.json()
            items.extend(data.get("results") or [])
            token = data.get("nextPage")
            if not token:
                break
    except Exception as e:
        logger.warning(f"[Workable:{slug}] fetch error: {e}")
        return []

    inserted: list[dict] = []
    for item in items:
        try:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            if (item.get("state") or "published") != "published":
                continue
            if item.get("isInternal"):
                continue

            loc = item.get("location") or {}
            locs = item.get("locations") or [loc]
            codes = {(l.get("countryCode") or "").upper() for l in locs if l}
            loc_str = ", ".join(
                p for p in (loc.get("city"), loc.get("region"), loc.get("country")) if p
            )
            workplace = (item.get("workplace") or "").lower()
            is_remote = bool(item.get("remote")) or workplace == "remote"

            # US gate: accept if any listed location is US. A remote role with
            # no US location is NOT assumed to be US.
            if "US" not in codes and not (is_remote and is_us_location(loc_str)):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            shortcode = item.get("shortcode")
            if not shortcode:
                continue
            apply_url = f"https://apply.workable.com/{slug}/j/{shortcode}/"

            work_type = ("remote" if is_remote
                         else "hybrid" if workplace == "hybrid"
                         else "onsite")

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_str or ("Remote, US" if is_remote else ""),
                "is_remote": is_remote,
                "work_type": work_type,
                "description": "",  # not in the list payload
                "salary_min": 0,
                "salary_max": 0,
                "source": "workable",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(item.get("published") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }
            if await insert_job(job):
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Workable:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Workable:{slug}] +{len(inserted)} new jobs ({len(items)} on board)")
    return inserted


async def fetch_recruitee(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Recruitee board (public offers endpoint)."""
    slug = company["slug"]
    company_name = company.get("name", slug)
    url = f"https://{slug}.recruitee.com/api/offers/"

    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[Recruitee:{slug}] HTTP {resp.status_code}")
            return []
        offers = (resp.json() or {}).get("offers") or []
    except Exception as e:
        logger.warning(f"[Recruitee:{slug}] fetch error: {e}")
        return []

    inserted: list[dict] = []
    for item in offers:
        try:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            if (item.get("status") or "published") != "published":
                continue

            country = (item.get("country_code") or "").upper()
            loc_str = ", ".join(
                p for p in (item.get("city"),
                            item.get("state_name") or item.get("state_code"),
                            item.get("country")) if p
            )
            _rc_wt, is_remote = _derive_work_type(
                "remote" if item.get("remote") else "", loc_str, title)
            if country != "US" and not (is_remote and is_us_location(loc_str)):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("careers_url") or item.get("careers_apply_url") or ""
            if not apply_url:
                continue

            desc = re.sub(r"<[^>]+>", " ", str(item.get("description") or ""))
            desc = re.sub(r"\s+", " ", desc).strip()

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_str or ("Remote, US" if is_remote else ""),
                "is_remote": is_remote,
                "work_type": _rc_wt,
                "description": desc[:4000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "recruitee",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(item.get("published_at") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }
            if await insert_job(job):
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Recruitee:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Recruitee:{slug}] +{len(inserted)} new jobs ({len(offers)} on board)")
    return inserted


async def fetch_breezy(
    client: httpx.AsyncClient,
    company: dict,
    profile_id: Optional[int],
    search_terms: list[str],
) -> list[dict]:
    """Fetch jobs from a single Breezy HR board (public /json endpoint)."""
    slug = company["slug"]
    company_name = company.get("name", slug)
    url = f"https://{slug}.breezy.hr/json"

    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[Breezy:{slug}] HTTP {resp.status_code}")
            return []
        items = resp.json()
    except Exception as e:
        logger.warning(f"[Breezy:{slug}] fetch error: {e}")
        return []

    if not isinstance(items, list):
        return []

    inserted: list[dict] = []
    for item in items:
        try:
            title = (item.get("name") or "").strip()
            if not title:
                continue

            loc = item.get("location") or {}
            country_obj = loc.get("country") or {}
            country = str(country_obj.get("id") or country_obj.get("name") or "")
            state = (loc.get("state") or {}).get("name") or ""
            loc_str = ", ".join(
                p for p in ((loc.get("city") or "").strip(), state, country) if p
            )
            _bz_wt, is_remote = _derive_work_type(
                "remote" if loc.get("is_remote") else "", loc_str, title)

            us_country = country.upper() in ("US", "USA", "UNITED STATES")
            if not us_country and not is_us_location(loc_str):
                continue

            if not _title_matches_profile(title, search_terms):
                continue
            if _is_blocked_company(company_name):
                continue

            apply_url = item.get("url") or ""
            if not apply_url:
                friendly = item.get("friendly_id") or item.get("id")
                if not friendly:
                    continue
                apply_url = f"https://{slug}.breezy.hr/p/{friendly}"

            desc = re.sub(r"<[^>]+>", " ", str(item.get("description") or ""))
            desc = re.sub(r"\s+", " ", desc).strip()

            job = {
                "title": title,
                "company_name": company_name,
                "company_domain": "",
                "location": loc_str or ("Remote, US" if is_remote else ""),
                "is_remote": is_remote,
                "work_type": _bz_wt,
                "description": desc[:4000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "breezy",
                "source_url": apply_url,
                "direct_apply_url": apply_url,
                "posted_at": _normalize_posted_at(item.get("published_date") or ""),
                "is_direct_apply": True,
                "search_profile_id": profile_id,
            }
            if await insert_job(job):
                inserted.append(job)
        except Exception as e:
            logger.debug(f"[Breezy:{slug}] item skip: {e}")
            continue

    if inserted:
        logger.info(f"[Breezy:{slug}] +{len(inserted)} new jobs ({len(items)} on board)")
    return inserted


_PLATFORM_FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "workday": fetch_workday,
    "smartrecruiters": fetch_smartrecruiters,
    "workable": fetch_workable,
    "recruitee": fetch_recruitee,
    "breezy": fetch_breezy,
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

    sem = asyncio.Semaphore(_PLATFORM_CONCURRENCY.get(platform, PLATFORM_CONCURRENCY))
    total_inserted = 0

    # Cloudflare-fronted platforms ban a single IP hard: Workable answered ~40
    # probe requests with HTTP 429 and `retry-after: 86287` — a 24-hour block.
    # One datacenter IP cannot sweep them. Their payloads are small JSON (a few
    # KB per board, not rendered pages), so routing them through the rotating
    # residential proxy costs almost no bandwidth and avoids the ban entirely.
    # Without a proxy configured they still run, just slowly and few at a time.
    client_kwargs: dict = {
        "timeout": HTTP_TIMEOUT,
        "headers": HTTP_HEADERS,
        "follow_redirects": True,
    }
    if platform in _PROXIED_PLATFORMS and settings.proxy_url:
        client_kwargs["proxy"] = settings.proxy_url
        logger.info(f"[{platform}] routing via residential proxy")

    async with httpx.AsyncClient(**client_kwargs) as client:

        async def _one(company):
            async with sem:
                try:
                    return await fetcher(client, company, profile_id, search_terms)
                except Exception as e:
                    logger.warning(f"[{platform}:{company.get('slug')}] crashed: {e}")
                    return []

        results = await asyncio.gather(*[_one(c) for c in companies], return_exceptions=True)
        # A platform that sweeps hundreds of boards and inserts nothing is the
        # signature of a silent failure, not a quiet day — fetch_greenhouse once
        # raised NameError on every single item into a per-item debug handler
        # and returned 0 for every board without a word in the logs.
        _ok = sum(1 for r in results if isinstance(r, list))
        _crashed = sum(1 for r in results if isinstance(r, Exception))
        if _crashed:
            logger.warning(
                f"[{platform}] {_crashed}/{len(companies)} companies raised")
        for r in results:
            if isinstance(r, list):
                total_inserted += len(r)
                if total_inserted >= MAX_INSERTS_PER_PLATFORM_PER_CYCLE:
                    logger.warning(
                        f"[{platform}] hit per-cycle cap "
                        f"({MAX_INSERTS_PER_PLATFORM_PER_CYCLE}), stopping early"
                    )
                    break

    if companies and total_inserted == 0:
        logger.warning(
            f"[{platform}] swept {len(companies)} companies and inserted 0 — "
            f"expected for a quiet cycle, but check for a silent fetcher error "
            f"if it persists")
    return total_inserted


async def scrape_all_ats(
    profile_id: Optional[int],
    search_terms: list[str],
    cycle_number: int,
    platforms: Optional[list[str]] = None,
    shard: int = 0,
    shards: int = 1,
) -> dict:
    """Run ATS platforms for one profile, one cycle.

    `platforms` (optional) restricts to specific platforms (e.g. ["greenhouse"]),
    which lets each platform run as its own independent worker.
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
    if platforms is not None:
        active_platforms = [p for p in active_platforms if p in platforms]
    if not active_platforms:
        return results

    companies = await load_companies_merged()
    if not companies:
        logger.info(f"[ATS] No companies configured — skipping")
        return results

    for platform in active_platforms:
        slice_ = get_rotation_slice(companies, cycle_number, platform)
        # Sharding: one worker per slice so a 1,693-company roster is swept by
        # several workers in parallel instead of one sequentially. Time to
        # discovery drops by roughly the shard count.
        if shards > 1:
            slice_ = [c for i, c in enumerate(slice_) if i % shards == shard]
        if not slice_:
            results[platform] = 0
            continue
        logger.info(
            f"[ATS] cycle#{cycle_number} {platform}: "
            f"fetching {len(slice_)} companies"
            + (f" [shard {shard + 1}/{shards}]" if shards > 1 else "")
        )
        try:
            count = await _fetch_platform(platform, slice_, profile_id, search_terms)
            results[platform] = count
        except Exception as e:
            logger.error(f"[ATS] {platform} dispatcher crashed: {e}")
            results[platform] = 0

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Description backfill for ATS rows that arrive without one
# ─────────────────────────────────────────────────────────────────────────────
#
# Two of the eight platforms hand back a job LIST that carries no description
# at all, and the code was reading a field that is never present:
#   - Workable: the v3 list payload has no description key. `fetch_workable`
#     literally sets `"description": ""`. All 199 rows were blind.
#   - Breezy: `{slug}.breezy.hr/json` returns
#     ['company','department','friendly_id','id','location','locations','name',
#      'published_date','salary','type','url'] — no description key, so
#     `item.get("description")` was always "". All 287 rows were blind.
#
# A blind row cannot be scored on its content, which means the skill-signature
# rescue — the whole mechanism for catching a role whose TITLE hides it — can
# never fire for it. These are direct-apply employer jobs, the most valuable
# kind on the board, so they are exactly the rows worth a second request.
#
# Verified live 2026-09-02:
#   Workable  GET  apply.workable.com/api/v1/accounts/{slug}/jobs/{shortcode}
#             -> 200, ~6KB JSON with description + requirements + benefits.
#             (the v3 per-job path 404s — v1 is the one that works)
#   Breezy    GET  the job page; it embeds a JSON-LD JobPosting whose
#             `description` was 3814 chars on the probe. Parsing the standard
#             schema.org block is far steadier than chasing Breezy's CSS.

_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)


def _jsonld_job_description(html: str) -> str:
    """Pull the description out of a page's schema.org JobPosting block."""
    for m in _JSONLD_RE.finditer(html):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        for node in (data if isinstance(data, list) else [data]):
            if not isinstance(node, dict):
                continue
            if node.get("@type") != "JobPosting":
                continue
            desc = node.get("description") or ""
            if desc:
                return desc
    return ""


def _plain(html_or_text: str) -> str:
    """Strip markup and collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", str(html_or_text or ""))
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'")
    return re.sub(r"\s+", " ", text).strip()


def _workable_shortcode(url: str) -> str:
    """apply.workable.com/{slug}/j/{SHORTCODE}/ -> SHORTCODE."""
    m = re.search(r"apply\.workable\.com/([^/]+)/j/([A-Za-z0-9]+)", url or "")
    return m.group(2) if m else ""


def _workable_slug(url: str) -> str:
    m = re.search(r"apply\.workable\.com/([^/]+)/j/", url or "")
    return m.group(1) if m else ""


async def _enrich_workable_row(client: httpx.AsyncClient, url: str) -> str:
    slug, shortcode = _workable_slug(url), _workable_shortcode(url)
    if not slug or not shortcode:
        return ""
    api = f"https://apply.workable.com/api/v1/accounts/{slug}/jobs/{shortcode}"
    resp = await client.get(api)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    data = resp.json()
    # Workable splits the posting across three fields; the requirements block is
    # where the skills the scorer matches on actually live, so join all three.
    parts = [data.get("description"), data.get("requirements"), data.get("benefits")]
    return _plain("\n\n".join(p for p in parts if p))


# Breezy does NOT emit JSON-LD on every board — probed 2026-09-02, one Duolingo
# posting carried a JobPosting block (3814 chars) and the very next posting
# carried none at all. Every page does render the body into
# `<div class="description">`, so that div is the fallback.
#
# Two traps, both hit on the first attempt:
#  1. `\bdescription\b` also matches INSIDE `class="container position-description"`
#     — a hyphen is a word boundary — and that outer container opens 2KB earlier
#     with the breadcrumbs and a Google Maps iframe in it. The class has to be
#     matched as a whole token in a space-separated list.
#  2. A `</div>` terminator regex stops at the first nested close. The body is
#     arbitrary employer HTML, so the close has to be found by counting depth.
_BREEZY_DESC_OPEN_RE = re.compile(
    r'<div[^>]*\sclass="(?:[^"]*\s)?description(?:\s[^"]*)?"[^>]*>',
    re.I,
)
_DIV_TAG_RE = re.compile(r'<(/?)div\b[^>]*>', re.I)


def _div_inner_html(html: str, open_match: re.Match) -> str:
    """Return the inner HTML of a div by counting nested open/close tags."""
    depth = 1
    pos = open_match.end()
    for tag in _DIV_TAG_RE.finditer(html, pos):
        depth += -1 if tag.group(1) else 1
        if depth == 0:
            return html[pos:tag.start()]
    return html[pos:]


async def _enrich_breezy_row(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    text = _plain(_jsonld_job_description(resp.text))
    if len(text) < 80:
        m = _BREEZY_DESC_OPEN_RE.search(resp.text)
        if m:
            text = _plain(_div_inner_html(resp.text, m))
    return text


_ATS_ENRICHERS = {
    "workable": _enrich_workable_row,
    "breezy": _enrich_breezy_row,
}


async def enrich_ats_descriptions(limit: int = 60) -> int:
    """Backfill descriptions on visible Workable/Breezy rows that lack one.

    Deliberately mirrors the LinkedIn enricher in scraper.py: only rows still
    visible, only rows whose score sits in the band where a description can
    change the verdict, a hard per-pass cap, and `scored_at = NULL` on write so
    the Scoring worker re-judges the row WITH the description.

    The cap is far more generous than LinkedIn's 12 because these payloads are
    ~6KB (Workable JSON) and ~22KB (Breezy page) rather than a ~300KB LinkedIn
    render, and Breezy is not behind Cloudflare so it needs no proxy.
    """
    from database import get_db, MAX_DESCRIPTION_CHARS, _write_lock

    platforms = tuple(_ATS_ENRICHERS)
    placeholders = ",".join("?" for _ in platforms)
    db = await get_db()
    try:
        # Deliberately NOT restricted to visible rows, unlike the LinkedIn
        # enricher. Raising the relevance floor to 50 created a catch-22 for
        # these two platforms: the row arrives with no description, is therefore
        # scored on its title alone, that puts it under the floor, it gets
        # hidden — and a visible-only queue would then never fetch the very
        # description that could rescue it. A Workable payload is ~6KB and a
        # Breezy page ~22KB unproxied, so reaching into the hidden band costs
        # little.
        #
        # No lower score bound either, which is the opposite of the LinkedIn
        # enricher. A sub-25 score normally means the gate or the family fence
        # rejected the role structurally — but that verdict was reached on a BARE
        # TITLE, because these platforms never sent a description. The
        # skill-signature rescue exists precisely to catch a role its title
        # hides, and it cannot run on a row that has no text. Refusing to fetch
        # the description because the description-less score is low is circular.
        # Measured: of 480 description-less Workable and Breezy rows, only 29 sat
        # in the old 25-95 window — the band was excluding 94% of the queue.
        # 480 one-time fetches of a 6KB and a 22KB payload is affordable; the
        # upper bound stays because above 95 there is nothing left to prove.
        cur = await db.execute(
            "SELECT id, source, source_url FROM jobs "
            "WHERE (description IS NULL OR description = '') "
            f"  AND source IN ({placeholders}) "
            "  AND status != 'archived' "
            "  AND relevance_score <= 95 "
            "ORDER BY first_seen_at DESC LIMIT ?",
            (*platforms, limit),
        )
        rows = [dict(r) for r in await cur.fetchall()]
    finally:
        await db.close()

    if not rows:
        # Say so out loud. A silent 0 is indistinguishable from a worker that
        # never ran — the failure mode that hid fetch_greenhouse for weeks.
        logger.info("[Enrich-ATS] no candidate rows "
                    f"({'/'.join(platforms)} + no description + relevance <= 95)")
        return 0
    logger.info(f"[Enrich-ATS] {len(rows)} candidates this pass")

    # Workable is Cloudflare-fronted and already proxied for the sweep; Breezy
    # is not, and a residential hop would only cost bandwidth. Two clients.
    # 12s, not the sweep's 25s. These are single small payloads, and a pass that
    # cannot finish tells us nothing: the worker logged "80 candidates" twice and
    # never once logged a result, so every failure mode was invisible.
    ENRICH_TIMEOUT = 12
    base: dict = {"timeout": ENRICH_TIMEOUT, "headers": HTTP_HEADERS,
                  "follow_redirects": True}
    proxied = dict(base)
    if settings.proxy_url:
        proxied["proxy"] = settings.proxy_url

    updated = 0
    reasons: dict[str, int] = {"http": 0, "empty": 0, "too_short": 0,
                               "error": 0, "no_url": 0}
    per_source: dict[str, int] = {}

    # Fetch concurrently, per platform. The first version walked the queue
    # strictly one row at a time with a 0.6s sleep between each; a pass of 80
    # rows never finished, because a Workable fetch goes through the rotating
    # residential proxy and takes seconds. The live worker logged "80 candidates
    # this pass" and no completion line 7 minutes later.
    #
    # Concurrency is per platform and deliberately low for Workable: its
    # Cloudflare answered ~40 quick probes with `retry-after: 86287`, a 24-hour
    # IP ban. Breezy is neither proxied nor Cloudflare-fronted, so it can go
    # wider. These match the sweep's own _PLATFORM_CONCURRENCY.
    _ENRICH_CONCURRENCY = {"workable": 3, "breezy": 4}
    sems = {plat: asyncio.Semaphore(n) for plat, n in _ENRICH_CONCURRENCY.items()}
    attempted: dict[str, int] = {}
    slowest: dict[str, float] = {}
    # started counts coroutines that got as far as the semaphore; attempted
    # counts those that got THROUGH it and issued a request. The gap between
    # them is the whole diagnosis: 80 started and 10 attempted means requests
    # are hanging and holding slots, while 10 started means the event loop
    # itself is starved and the coroutines never ran at all.
    started: dict[str, int] = {}

    async def _one(row, direct_client, proxy_client):
        """Fetch one description. Does NOT write — see the note below."""
        source = row.get("source") or ""
        started[source] = started.get(source, 0) + 1
        url = row.get("source_url") or ""
        enricher = _ATS_ENRICHERS.get(source)
        if not url or not enricher:
            reasons["no_url"] += 1
            return None
        sem = sems.get(source)
        client = proxy_client if source in _PROXIED_PLATFORMS else direct_client
        async with (sem or asyncio.Semaphore(1)):
            attempted[source] = attempted.get(source, 0) + 1
            _t = time.monotonic()
            try:
                text = await enricher(client, url)
                _dt = round(time.monotonic() - _t, 1)
                if _dt > slowest.get(source, 0):
                    slowest[source] = _dt
                if not text:
                    reasons["empty"] += 1
                    if reasons["empty"] == 1:
                        logger.warning(
                            f"[Enrich-ATS] {source} job {row['id']} returned no "
                            f"description text (url={url[:90]}) — payload shape "
                            "may have changed")
                    return None
                if len(text) < 80:
                    reasons["too_short"] += 1
                    return None
                return (row["id"], source, text)
            except Exception as e:
                reasons["error"] += 1
                logger.debug(f"[Enrich-ATS] {source} job {row['id']}: {e}")
                return None
            finally:
                # Politeness delay held INSIDE the semaphore, so it throttles a
                # platform's own request rate without stalling the whole pass.
                await asyncio.sleep(0.4)

    # Hard deadline on the whole pass. Without it a slow platform silently eats
    # the worker's interval and the pass never reports at all — which is exactly
    # what happened, twice.
    PASS_DEADLINE = 240
    t0 = time.monotonic()
    timed_out = False
    fetched: list = []
    async with httpx.AsyncClient(**base) as direct_client, \
               httpx.AsyncClient(**proxied) as proxy_client:
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    *[_one(r, direct_client, proxy_client) for r in rows],
                    return_exceptions=True),
                timeout=PASS_DEADLINE)
            fetched = [r for r in results if isinstance(r, tuple)]
        except asyncio.TimeoutError:
            timed_out = True
    fetch_secs = time.monotonic() - t0

    # WRITE AFTER THE FETCHES, IN ONE LOCK ACQUISITION.
    #
    # The write used to happen inline, still holding the platform semaphore. But
    # _write_lock() is process-wide and ~18 ATS insert workers queue on it, so a
    # coroutine that had finished its 8-second fetch then sat on its semaphore
    # slot waiting its turn to write. Measured: a 20-row pass issued 9 requests
    # in 242s — all 20 coroutines started, so the event loop was fine and the
    # slots were simply blocked — while the slowest completed fetch was 8.2s.
    # Network throughput was never the problem; write-lock contention was, and
    # it was being paid per row.
    async with _write_lock():
        wdb = await get_db()
        try:
            for jid, source, text in fetched:
                # status is restored to 'new' alongside scored_at, not just
                # cleared: get_unscored_jobs() filters on status='new', so a
                # hidden row with scored_at NULL is invisible to the Scoring
                # worker and would sit with a fresh description and a stale
                # score forever. The next scoring pass re-hides it if the
                # description does not in fact rescue it.
                await wdb.execute(
                    "UPDATE jobs SET description = ?, scored_at = NULL, "
                    "status = CASE WHEN status = 'hidden' THEN 'new' "
                    "ELSE status END WHERE id = ?",
                    (text[:MAX_DESCRIPTION_CHARS], jid),
                )
                updated += 1
                per_source[source] = per_source.get(source, 0) + 1
            await wdb.commit()
        finally:
            await wdb.close()
    elapsed = time.monotonic() - t0

    # Always report, including on the timeout path, and always include the
    # per-source timing. "Updated none" and "never finished" are different
    # failures and the old logging could not tell them apart.
    logger.warning(
        f"[Enrich-ATS] {updated}/{len(rows)} descriptions added in {elapsed:.0f}s"
        f" (fetch {fetch_secs:.0f}s)"
        f"{' (HIT THE ' + str(PASS_DEADLINE) + 's DEADLINE)' if timed_out else ''}"
        f" — by source {per_source or '{}'}, "
        f"started {dict(started)}, attempted {dict(attempted)}, "
        f"slowest {dict(slowest)}, reasons {reasons}")
    return updated
