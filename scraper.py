"""
Job scraper engine — pulls from multiple sources and normalizes into a common format.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from jobspy import scrape_jobs

from config import settings
from database import insert_job

logger = logging.getLogger("scoutpilot.scraper")

# ============================================================================
# JOB BOARD / STAFFING AGENCY BLOCKLIST
# These companies are reposters, not real employers. Filter them out.
# ============================================================================
BLOCKED_COMPANIES = {
    # Job boards / aggregators masquerading as employers
    "lensa", "dice", "monster", "careerbuilder", "ziprecruiter", "snagajob",
    "simplyhired", "jobget", "talent.com", "adzuna", "jooble", "ladders",
    "getwork", "livecareer", "resume-library", "resume library",
    "jobot", "cybercoders", "harnham", "crossover", "toptal",
    "upwork", "hired", "vettery", "triplebyte", "wellfound",
    # Known spam / fake job reposters
    "recruitics", "appcast", "joveo", "pandologic", "radancy",
    "talentify", "recruitology", "jobalign", "nexxt", "recruitics",
    "zippia", "salary.com", "comparably", "payscale",
    # Staffing farms that repost everything
    "insight global", "robert half", "teksystems", "kelly services",
    "adecco", "manpower", "manpowergroup", "randstad", "allegis group",
    "hays", "kforce", "modis", "aston carter", "astoncarter",
    "beacon hill staffing", "beacon hill", "aerotek",
    "brilliant staffing", "artech", "mastech", "genesis10",
    "collabera", "wipro", "infosys", "tata consultancy", "cognizant",
    "hcl technologies", "tech mahindra", "mphasis",
}


def _is_blocked_company(company_name: str) -> bool:
    """Check if a company is on the blocklist."""
    if not company_name:
        return False
    name = company_name.lower().strip()
    # Direct match
    if name in BLOCKED_COMPANIES:
        return True
    # Partial match (company name contains a blocked term)
    for blocked in BLOCKED_COMPANIES:
        if blocked in name:
            return True
    return False


# ============================================================================
# WORK TYPE DETECTION (Remote / Hybrid / Onsite)
# ============================================================================
def _detect_work_type(row: dict) -> str:
    """
    Detect work arrangement from job data.
    Returns: 'remote', 'hybrid', or 'onsite'
    """
    title = str(row.get("title", "")).lower()
    location = str(row.get("location", "")).lower()
    description = str(row.get("description", ""))[:3000].lower()

    # Check explicit remote flag first
    if row.get("is_remote"):
        return "remote"

    # Build combined text for pattern matching
    text = f"{title} {location} {description}"

    # Hybrid patterns (check first — hybrid is more specific than remote)
    hybrid_patterns = [
        r'\bhybrid\b', r'\bhybrid[\s\-]?remote\b', r'\bremote[\s\-]?hybrid\b',
        r'\b\d+\s*days?\s*(in[\s\-]?office|on[\s\-]?site|onsite)\b',
        r'\bflex(ible)?\s*(work|schedule|hybrid)\b',
        r'\bin[\s\-]?office\s*\d+\s*days?\b',
    ]
    for pat in hybrid_patterns:
        if re.search(pat, text):
            return "hybrid"

    # Remote patterns
    remote_patterns = [
        r'\bremote\b', r'\bwork\s*from\s*home\b', r'\bwfh\b',
        r'\bfully[\s\-]?remote\b', r'\b100%\s*remote\b',
        r'\btelecommute\b', r'\btelework\b', r'\bdistributed\b',
        r'\banywhere\b',
    ]
    for pat in remote_patterns:
        if re.search(pat, text):
            # Double-check it's not actually hybrid disguised as remote
            if re.search(r'\bnot\s+fully\s+remote\b|\bnot\s+100%\s+remote\b', text):
                return "hybrid"
            return "remote"

    # Default to onsite
    return "onsite"


def _normalize_job(row: dict, source: str, profile_id: Optional[int] = None) -> dict:
    """Normalize a scraped job into our DB schema format."""
    title = str(row.get("title", "")).strip()
    company = str(row.get("company_name", row.get("company", ""))).strip()
    location = str(row.get("location", "")).strip()
    description = str(row.get("description", "")).strip()

    # --- BLOCK FAKE REPOSTERS ---
    if _is_blocked_company(company):
        return None  # Signal to skip this job

    # Determine direct apply
    job_url = str(row.get("job_url", row.get("link", row.get("url", "")))).strip()
    company_url = str(row.get("company_url", "")).strip()

    # If the apply link goes to the company's own domain, it's direct
    is_direct = False
    direct_url = ""
    if job_url:
        # Check if it's a company career page (not linkedin/indeed/glassdoor/ziprecruiter)
        aggregator_domains = ["linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com", "google.com"]
        is_aggregator = any(d in job_url.lower() for d in aggregator_domains)
        if not is_aggregator:
            is_direct = True
            direct_url = job_url

    # Parse salary
    salary_min = 0
    salary_max = 0
    if row.get("min_amount"):
        try:
            salary_min = int(float(row["min_amount"]))
        except (ValueError, TypeError):
            pass
    if row.get("max_amount"):
        try:
            salary_max = int(float(row["max_amount"]))
        except (ValueError, TypeError):
            pass

    # Work type detection (remote / hybrid / onsite)
    work_type = _detect_work_type(row)
    is_remote = work_type == "remote"

    # Posted date
    posted_at = str(row.get("date_posted", row.get("posted_at", ""))).strip()

    # Company domain
    domain = ""
    if company_url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(company_url if company_url.startswith("http") else f"https://{company_url}")
            domain = parsed.netloc.replace("www.", "")
        except Exception:
            pass

    return {
        "title": title,
        "company_name": company,
        "company_domain": domain,
        "location": location,
        "is_remote": is_remote,
        "work_type": work_type,
        "description": description[:10000],  # cap description length
        "salary_min": salary_min,
        "salary_max": salary_max,
        "source": source,
        "source_url": job_url,
        "direct_apply_url": direct_url,
        "posted_at": posted_at,
        "is_direct_apply": is_direct,
        "search_profile_id": profile_id,
    }


async def scrape_jobspy(
    search_term: str,
    location: str = "",
    results_wanted: int = 50,
    hours_old: int = 24,
    profile_id: Optional[int] = None,
    sites: Optional[list[str]] = None,
) -> list[dict]:
    """
    Scrape jobs using JobSpy (LinkedIn, Indeed, Glassdoor, Google, ZipRecruiter).
    Runs in a thread since JobSpy is synchronous.
    """
    if sites is None:
        sites = ["indeed", "linkedin", "glassdoor", "google", "zip_recruiter"]

    logger.info(f"[JobSpy] Searching: '{search_term}' | location: '{location}' | sites: {sites}")

    def _scrape():
        try:
            kwargs = {
                "site_name": sites,
                "search_term": search_term,
                "results_wanted": results_wanted,
                "hours_old": hours_old,
                "country_indeed": "USA",
            }
            if location:
                kwargs["location"] = location

            results = scrape_jobs(**kwargs)
            return results
        except Exception as e:
            logger.error(f"[JobSpy] Error: {e}")
            return None

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, _scrape)

    if df is None or df.empty:
        logger.info("[JobSpy] No results returned")
        return []

    jobs = []
    inserted = 0
    blocked = 0
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        # Handle NaN values
        for k, v in row_dict.items():
            if isinstance(v, float) and str(v) == 'nan':
                row_dict[k] = ""

        source = str(row_dict.get("site", "jobspy")).lower()
        job = _normalize_job(row_dict, source, profile_id)

        # Skip blocked companies
        if job is None:
            blocked += 1
            continue

        if not job["title"] or not job["source_url"]:
            continue

        was_inserted = await insert_job(job)
        if was_inserted:
            inserted += 1
            jobs.append(job)

    logger.info(f"[JobSpy] Found {len(df)} results, blocked {blocked}, inserted {inserted} new jobs")
    return jobs


async def scrape_serpapi(
    search_term: str,
    location: str = "",
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape Google Jobs via SerpApi."""
    if not settings.serpapi_key:
        return []

    logger.info(f"[SerpApi] Searching: '{search_term}'")

    params = {
        "engine": "google_jobs",
        "q": search_term,
        "api_key": settings.serpapi_key,
        "chips": "date_posted:today",  # Only today's jobs
    }
    if location:
        params["location"] = location

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get("https://serpapi.com/search", params=params)
            data = resp.json()

        for item in data.get("jobs_results", []):
            company_name = item.get("company_name", "")

            # Skip blocked companies
            if _is_blocked_company(company_name):
                continue

            job_url = ""
            direct_url = ""
            is_direct = False

            # Check apply options for direct links
            for option in item.get("apply_options", []):
                link = option.get("link", "")
                if link:
                    aggregator_domains = ["linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com"]
                    is_agg = any(d in link.lower() for d in aggregator_domains)
                    if not is_agg:
                        direct_url = link
                        is_direct = True
                        break
                    if not job_url:
                        job_url = link

            if not job_url and not direct_url:
                continue

            # Detect work type
            work_type = _detect_work_type(item)

            job = {
                "title": item.get("title", ""),
                "company_name": company_name,
                "company_domain": "",
                "location": item.get("location", ""),
                "is_remote": work_type == "remote",
                "work_type": work_type,
                "description": item.get("description", "")[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "google_jobs",
                "source_url": direct_url or job_url,
                "direct_apply_url": direct_url,
                "posted_at": item.get("detected_extensions", {}).get("posted_at", ""),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[SerpApi] Found {len(data.get('jobs_results', []))} results, inserted {len(jobs)} new")
    except Exception as e:
        logger.error(f"[SerpApi] Error: {e}")

    return jobs


async def scrape_jsearch(
    search_term: str,
    location: str = "",
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape via JSearch (RapidAPI)."""
    if not settings.rapidapi_key:
        return []

    logger.info(f"[JSearch] Searching: '{search_term}'")

    query = search_term
    if location:
        query += f" in {location}"

    headers = {
        "X-RapidAPI-Key": settings.rapidapi_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    params = {
        "query": query,
        "page": "1",
        "num_pages": "1",
        "date_posted": "today",
    }

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://jsearch.p.rapidapi.com/search",
                headers=headers,
                params=params,
            )
            data = resp.json()

        for item in data.get("data", []):
            company_name = item.get("employer_name", "")

            # Skip blocked companies
            if _is_blocked_company(company_name):
                continue

            job_url = item.get("job_apply_link", "")
            is_direct = item.get("job_apply_is_direct", False)

            # Detect work type
            work_type = _detect_work_type({
                "title": item.get("job_title", ""),
                "location": f"{item.get('job_city', '')}, {item.get('job_state', '')}",
                "description": item.get("job_description", ""),
                "is_remote": item.get("job_is_remote", False),
            })

            job = {
                "title": item.get("job_title", ""),
                "company_name": company_name,
                "company_domain": item.get("employer_website", "").replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/"),
                "location": f"{item.get('job_city', '')}, {item.get('job_state', '')}".strip(", "),
                "is_remote": work_type == "remote",
                "work_type": work_type,
                "description": item.get("job_description", "")[:10000],
                "salary_min": int(item.get("job_min_salary", 0) or 0),
                "salary_max": int(item.get("job_max_salary", 0) or 0),
                "source": "jsearch",
                "source_url": job_url,
                "direct_apply_url": job_url if is_direct else "",
                "posted_at": item.get("job_posted_at_datetime_utc", ""),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[JSearch] Found {len(data.get('data', []))} results, inserted {len(jobs)} new")
    except Exception as e:
        logger.error(f"[JSearch] Error: {e}")

    return jobs


async def run_scrape_cycle(profiles: list[dict]) -> dict:
    """
    Run a full scrape cycle for all active profiles.
    Returns summary stats.
    """
    total_new = 0
    errors = []

    for profile in profiles:
        title = profile["title"]
        expanded = profile.get("expanded_titles", [])
        locations = profile.get("locations", [])
        profile_id = profile["id"]
        hours = profile.get("freshness_hours", 24)

        # Build search queries from title + expanded titles
        search_terms = [title] + [t for t in expanded if t.lower() != title.lower()]

        for term in search_terms[:5]:  # cap at 5 variants per cycle
            for loc in (locations if locations else [""]):
                try:
                    # JobSpy (primary)
                    new_jobs = await scrape_jobspy(
                        search_term=term,
                        location=loc,
                        results_wanted=30,
                        hours_old=hours,
                        profile_id=profile_id,
                    )
                    total_new += len(new_jobs)

                    # SerpApi (secondary)
                    new_jobs = await scrape_serpapi(term, loc, profile_id)
                    total_new += len(new_jobs)

                    # JSearch (tertiary)
                    new_jobs = await scrape_jsearch(term, loc, profile_id)
                    total_new += len(new_jobs)

                except Exception as e:
                    err_msg = f"Error scraping '{term}' in '{loc}': {e}"
                    logger.error(err_msg)
                    errors.append(err_msg)

    return {
        "new_jobs": total_new,
        "errors": errors,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
