"""
Job scraper engine — pulls from multiple sources and normalizes into a common format.
Aggressively finds direct company application URLs.
"""
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from jobspy import scrape_jobs

from config import settings
from database import insert_job

logger = logging.getLogger("scoutpilot.scraper")


def _normalize_posted_at(raw: str) -> str:
    """Normalize posted_at to ISO 8601 datetime string.
    Handles: ISO datetime, YYYY-MM-DD, relative strings like '3 days ago', 'X hours ago'.
    Returns empty string if unparseable."""
    if not raw or raw == "None":
        return ""
    raw = raw.strip()

    # Already ISO datetime (e.g. 2026-03-27T12:00:00Z or 2026-03-27T12:00:00+00:00)
    if re.match(r"^\d{4}-\d{2}-\d{2}T", raw):
        return raw

    # YYYY-MM-DD format — treat as start of that day UTC
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw + "T00:00:00+00:00"

    # Relative time: "X hours/days/minutes ago"
    m = re.match(r"(\d+)\s*(minute|min|hour|hr|day|week|month)s?\s*ago", raw, re.IGNORECASE)
    if m:
        from datetime import timedelta
        num = int(m.group(1))
        unit = m.group(2).lower()
        now = datetime.now(timezone.utc)
        if unit in ("minute", "min"):
            dt = now - timedelta(minutes=num)
        elif unit in ("hour", "hr"):
            dt = now - timedelta(hours=num)
        elif unit == "day":
            dt = now - timedelta(days=num)
        elif unit == "week":
            dt = now - timedelta(weeks=num)
        elif unit == "month":
            dt = now - timedelta(days=num * 30)
        else:
            return ""
        return dt.isoformat()

    # "just posted", "today"
    if raw.lower() in ("just posted", "just now", "today"):
        return datetime.now(timezone.utc).isoformat()

    # "yesterday"
    if raw.lower() == "yesterday":
        from datetime import timedelta
        return (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

    return ""


# Known aggregator domains — links from these are NOT direct apply
AGGREGATOR_DOMAINS = {
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "google.com", "monster.com", "careerbuilder.com", "simplyhired.com",
    "dice.com", "snagajob.com", "talent.com", "adzuna.com", "jooble.org",
    "getwork.com", "livecareer.com", "resume-library.com", "jobget.com",
    "wellfound.com", "lever.co", "greenhouse.io", "workday.com",
    "smartrecruiters.com", "icims.com", "myworkdayjobs.com",
    "jobs.lever.co", "boards.greenhouse.io",
    # AI job sites / aggregator clones — same as us, not real employers
    "otta.com", "builtin.com", "cord.co", "hired.com", "triplebyte.com",
    "angel.co", "remotive.com", "weworkremotely.com", "flexjobs.com",
    "remote.co", "himalayas.app", "jobgether.com", "4dayweek.io",
    "nodesk.co", "workingnomads.com", "remoteleaf.com", "jobspresso.co",
    "pangian.com", "remoteok.com", "skipthedrive.com", "virtualvocations.com",
    "lensa.com", "jobright.ai", "teal.com", "sonara.ai", "lazyapply.com",
    "jobscan.co", "huntr.co", "careerflow.ai",
}

# ATS domains that ARE direct apply (company uses these as their career page)
ATS_DIRECT_DOMAINS = {
    "lever.co", "greenhouse.io", "workday.com", "myworkdayjobs.com",
    "smartrecruiters.com", "icims.com", "jobvite.com", "ashbyhq.com",
    "bamboohr.com", "recruitee.com", "workable.com", "breezy.hr",
    "jazz.co", "jazzhr.com", "applicantstack.com", "paylocity.com",
    "ultipro.com", "ceridian.com", "taleo.net", "successfactors.com",
    "apply.workable.com", "jobs.ashbyhq.com", "careers.smartrecruiters.com",
}


def _is_direct_url(url: str) -> bool:
    """
    Determine if a URL is a direct company application link to a SPECIFIC JOB.
    Returns True for company career pages and ATS-hosted job pages with a job path.
    Returns False for aggregator job boards, LinkedIn EasyApply, and bare homepages.
    """
    if not url:
        return False
    try:
        parsed = urlparse(url.lower())
        domain = parsed.netloc.replace("www.", "")
        path = parsed.path.rstrip("/")

        # NEVER treat LinkedIn as direct apply (EasyApply loops back to LinkedIn)
        if "linkedin.com" in domain:
            return False

        # Check if it's a known aggregator (NOT direct)
        for agg in AGGREGATOR_DOMAINS:
            if domain == agg or domain.endswith("." + agg):
                # BUT check if it's an ATS that companies use for their own careers
                for ats in ATS_DIRECT_DOMAINS:
                    if domain == ats or domain.endswith("." + ats):
                        return True
                return False

        # Reject bare homepages — a URL like "company.com" or "company.com/"
        # is NOT a job application link. Need at least a path with content.
        if not path or path == "/":
            return False

        # Reject very short paths that are likely top-level pages, not job posts
        # e.g. /careers, /jobs, /about — these are category pages not specific jobs
        bare_pages = {"/careers", "/jobs", "/about", "/contact", "/team",
                      "/hiring", "/work-with-us", "/join-us", "/openings"}
        if path in bare_pages:
            return False

        # If it has a meaningful path, it's likely a direct company link
        return True
    except Exception:
        return False


def _find_best_direct_url(urls: list[str]) -> tuple[str, str, bool]:
    """
    Given a list of URLs from a job posting, find the best direct apply URL.
    Returns: (best_url, direct_url, is_direct)
    """
    direct_urls = []
    aggregator_urls = []

    for url in urls:
        if not url:
            continue
        if _is_direct_url(url):
            direct_urls.append(url)
        else:
            aggregator_urls.append(url)

    if direct_urls:
        # Prefer the first direct URL (usually the most relevant)
        return (direct_urls[0], direct_urls[0], True)
    elif aggregator_urls:
        return (aggregator_urls[0], "", False)
    else:
        return ("", "", False)

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
    # AI job sites / aggregator clones (same as us — not real employers)
    "otta", "cord", "jobright", "jobright.ai", "sonara", "lazyapply",
    "teal", "jobscan", "huntr", "careerflow", "jobgether", "remotive",
    "himalayas", "flexjobs", "pangian", "remoteok", "remote ok",
    "weworkremotely", "we work remotely", "nodesk", "jobspresso",
    "builtin", "built in", "4dayweek", "skipthedrive", "virtualvocations",
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

    # Determine direct apply — use smart URL detection
    job_url = str(row.get("job_url", row.get("link", row.get("url", "")))).strip()
    company_url = str(row.get("company_url", "")).strip()

    # Collect all available URLs and find the best direct one
    # ONLY use structured URL fields from the scraper — never extract from description
    # (description URLs lead to company homepages, benefit pages, etc. — not job postings)
    candidate_urls = [u for u in [job_url, company_url] if u]

    # Also check for apply_link / apply_url fields some sources provide
    for key in ("apply_link", "apply_url", "application_url", "job_apply_link"):
        val = str(row.get(key, "")).strip()
        if val and val not in candidate_urls:
            candidate_urls.append(val)

    # Check structured apply options (SerpApi provides these)
    for key in ("apply_options", "company_url_direct", "company_careers_url"):
        val = row.get(key)
        if isinstance(val, list):
            for item in val:
                u = item.get("link", "") if isinstance(item, dict) else str(item)
                if u and u not in candidate_urls:
                    candidate_urls.append(u)
        elif isinstance(val, str) and val.strip() and val.strip() not in candidate_urls:
            candidate_urls.append(val.strip())

    best_url, direct_url, is_direct = _find_best_direct_url(candidate_urls)
    if best_url:
        job_url = best_url

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

    # Posted date — normalize to ISO format
    posted_at = _normalize_posted_at(str(row.get("date_posted", row.get("posted_at", ""))))

    # Company domain
    domain = ""
    if company_url:
        try:
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
        sites = ["indeed", "linkedin", "google", "glassdoor", "zip_recruiter"]

    logger.info(f"[JobSpy] Searching: '{search_term}' | location: '{location}' | sites: {sites}")

    def _scrape():
        try:
            kwargs = {
                "site_name": sites,
                "search_term": search_term,
                "results_wanted": results_wanted,
                "hours_old": hours_old,
                "country_indeed": "USA",
                "linkedin_fetch_description": True,
                "description_format": "markdown",
                "verbose": 0,
            }
            if location:
                kwargs["location"] = location

            results = scrape_jobs(**kwargs)
            logger.info(f"[JobSpy] Raw results for '{search_term}': {len(results) if results is not None and not results.empty else 0} rows")
            return results
        except Exception as e:
            logger.error(f"[JobSpy] Error scraping '{search_term}': {e}")
            import traceback
            logger.error(f"[JobSpy] Traceback: {traceback.format_exc()}")
            return None

    loop = asyncio.get_event_loop()
    try:
        df = await asyncio.wait_for(
            loop.run_in_executor(None, _scrape),
            timeout=120,  # 2 min per query — JobSpy hits multiple sites
        )
    except asyncio.TimeoutError:
        logger.warning(f"[JobSpy] TIMEOUT after 120s for '{search_term}' — skipping")
        return []

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

    direct_count = sum(1 for j in jobs if j.get("is_direct_apply"))
    logger.info(f"[JobSpy] Found {len(df)} results, blocked {blocked}, inserted {inserted} new ({direct_count} direct apply)")
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

            # Collect ALL apply option URLs and find the best direct one
            all_urls = []
            for option in item.get("apply_options", []):
                link = option.get("link", "")
                if link:
                    all_urls.append(link)

            # Also check detected_extensions for a share_link
            share_link = item.get("detected_extensions", {}).get("share_link", "")
            if share_link:
                all_urls.append(share_link)

            job_url, direct_url, is_direct = _find_best_direct_url(all_urls)

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
                "source_url": job_url or direct_url,
                "direct_apply_url": direct_url,
                "posted_at": _normalize_posted_at(item.get("detected_extensions", {}).get("posted_at", "")),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        direct_count = sum(1 for j in jobs if j.get("is_direct_apply"))
        logger.info(f"[SerpApi] Found {len(data.get('jobs_results', []))} results, inserted {len(jobs)} new ({direct_count} direct apply)")
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

            job_apply_link = item.get("job_apply_link", "")
            # JSearch provides is_direct flag but let's verify with our own logic too
            jsearch_direct = item.get("job_apply_is_direct", False)
            is_direct = jsearch_direct or _is_direct_url(job_apply_link)
            job_url = job_apply_link

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
                "posted_at": _normalize_posted_at(item.get("job_posted_at_datetime_utc", "")),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        direct_count = sum(1 for j in jobs if j.get("is_direct_apply"))
        logger.info(f"[JSearch] Found {len(data.get('data', []))} results, inserted {len(jobs)} new ({direct_count} direct apply)")
    except Exception as e:
        logger.error(f"[JSearch] Error: {e}")

    return jobs


async def scrape_remotive(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape remote jobs from Remotive API (free, no key needed)."""
    logger.info(f"[Remotive] Searching: '{search_term}'")

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://remotive.com/api/remote-jobs",
                params={"search": search_term, "limit": 30},
            )
            data = resp.json()

        for item in data.get("jobs", []):
            company = item.get("company_name", "")
            if _is_blocked_company(company):
                continue

            apply_url = item.get("url", "")
            is_direct = _is_direct_url(apply_url)
            description = item.get("description", "")
            # Remotive returns HTML descriptions — strip tags for plain text
            clean_desc = re.sub(r"<[^>]+>", " ", description)
            clean_desc = re.sub(r"\s+", " ", clean_desc).strip()

            location = item.get("candidate_required_location", "Remote")
            posted_at = _normalize_posted_at(item.get("publication_date", ""))

            # Detect work type
            work_type = _detect_work_type({
                "title": item.get("title", ""),
                "location": location,
                "description": clean_desc[:3000],
                "is_remote": True,
            })

            job = {
                "title": item.get("title", ""),
                "company_name": company,
                "company_domain": "",
                "location": location,
                "is_remote": True,
                "work_type": work_type,
                "description": clean_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "remotive",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": posted_at,
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        direct_count = sum(1 for j in jobs if j.get("is_direct_apply"))
        logger.info(f"[Remotive] Found {len(data.get('jobs', []))} results, inserted {len(jobs)} new ({direct_count} direct apply)")
    except Exception as e:
        logger.error(f"[Remotive] Error: {e}")

    return jobs


async def scrape_themuse(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape jobs from The Muse API (free, no key needed)."""
    logger.info(f"[TheMuse] Searching: '{search_term}'")

    jobs = []
    try:
        # The Muse categories aren't exact search — use page param
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://www.themuse.com/api/public/jobs",
                params={"page": 1, "descending": "true"},
            )
            data = resp.json()

        search_lower = search_term.lower()
        for item in data.get("results", []):
            title = item.get("name", "")
            company_data = item.get("company", {})
            company = company_data.get("name", "") if isinstance(company_data, dict) else ""

            if _is_blocked_company(company):
                continue

            # Filter by search term match in title
            if search_lower not in title.lower():
                continue

            # Get locations
            locs = item.get("locations", [])
            loc_str = ", ".join(loc.get("name", "") for loc in locs if isinstance(loc, dict)) if locs else ""

            # Description (HTML → plain text)
            description = item.get("contents", "")
            clean_desc = re.sub(r"<[^>]+>", " ", description)
            clean_desc = re.sub(r"\s+", " ", clean_desc).strip()

            apply_url = item.get("refs", {}).get("landing_page", "")
            is_direct = _is_direct_url(apply_url)

            posted_at = _normalize_posted_at(item.get("publication_date", ""))

            work_type = _detect_work_type({
                "title": title,
                "location": loc_str,
                "description": clean_desc[:3000],
            })

            job = {
                "title": title,
                "company_name": company,
                "company_domain": "",
                "location": loc_str,
                "is_remote": work_type == "remote",
                "work_type": work_type,
                "description": clean_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "themuse",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": posted_at,
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        direct_count = sum(1 for j in jobs if j.get("is_direct_apply"))
        logger.info(f"[TheMuse] Found {len(data.get('results', []))} results, inserted {len(jobs)} new ({direct_count} direct apply)")
    except Exception as e:
        logger.error(f"[TheMuse] Error: {e}")

    return jobs


async def scrape_remoteok(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape remote jobs from RemoteOK (free JSON API, no key needed)."""
    logger.info(f"[RemoteOK] Searching: '{search_term}'")

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://remoteok.com/api",
                headers={"User-Agent": "ScoutPilot/1.0"},
            )
            data = resp.json()

        # RemoteOK returns all jobs — filter by search term
        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        for item in data:
            if not isinstance(item, dict) or not item.get("position"):
                continue

            title = item.get("position", "")
            company = item.get("company", "")
            description = item.get("description", "")

            # Match: any search word in title or tags
            title_lower = title.lower()
            tags = " ".join(item.get("tags", [])).lower() if item.get("tags") else ""
            combined = f"{title_lower} {tags}"
            if not any(w in combined for w in search_words):
                continue

            if _is_blocked_company(company):
                continue

            apply_url = item.get("url", "")
            if apply_url and not apply_url.startswith("http"):
                apply_url = f"https://remoteok.com{apply_url}"
            is_direct = _is_direct_url(apply_url)
            posted_at = _normalize_posted_at(item.get("date", ""))

            job = {
                "title": title,
                "company_name": company,
                "company_domain": "",
                "location": item.get("location", "Remote"),
                "is_remote": True,
                "work_type": "remote",
                "description": re.sub(r"<[^>]+>", " ", description).strip()[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "remoteok",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": posted_at,
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[RemoteOK] Matched {len(jobs)} new jobs for '{search_term}'")
    except Exception as e:
        logger.error(f"[RemoteOK] Error: {e}")

    return jobs


async def scrape_arbeitnow(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape jobs from Arbeitnow (free API, remote-friendly, no key needed)."""
    logger.info(f"[Arbeitnow] Searching: '{search_term}'")

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://www.arbeitnow.com/api/job-board-api",
            )
            data = resp.json()

        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        for item in data.get("data", []):
            title = item.get("title", "")
            company = item.get("company_name", "")

            title_lower = title.lower()
            tags = " ".join(item.get("tags", [])).lower() if item.get("tags") else ""
            combined = f"{title_lower} {tags}"
            if not any(w in combined for w in search_words):
                continue

            if _is_blocked_company(company):
                continue

            apply_url = item.get("url", "")
            is_direct = _is_direct_url(apply_url)
            is_remote = item.get("remote", False)
            description = item.get("description", "")
            clean_desc = re.sub(r"<[^>]+>", " ", description).strip()

            job = {
                "title": title,
                "company_name": company,
                "company_domain": "",
                "location": item.get("location", ""),
                "is_remote": is_remote,
                "work_type": "remote" if is_remote else _detect_work_type({"title": title, "location": item.get("location", ""), "description": clean_desc}),
                "description": clean_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "arbeitnow",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": _normalize_posted_at(item.get("created_at", "")),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[Arbeitnow] Matched {len(jobs)} new jobs for '{search_term}'")
    except Exception as e:
        logger.error(f"[Arbeitnow] Error: {e}")

    return jobs


_term_offset = {}  # Track which search term each profile is on


async def _scrape_one_profile(profile: dict) -> dict:
    """Scrape ALL sources for ONE profile — all terms and sources run concurrently."""
    global _term_offset
    title = profile["title"]
    expanded = profile.get("expanded_titles", [])
    locations = profile.get("locations", [])
    profile_id = profile["id"]
    hours = profile.get("freshness_hours", 24)

    # Build search queries from title + expanded titles
    search_terms = [title] + [t for t in expanded if t.lower() != title.lower()]

    # Add keyword-based search terms
    keywords = profile.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    for kw in keywords:
        if kw.lower() not in [s.lower() for s in search_terms]:
            search_terms.append(kw)

    is_remote_only = profile.get("remote_only", 0)

    # Pick 5 rotating terms per cycle for maximum coverage
    all_terms = search_terms[:15]
    pid = profile_id or 0
    term_idx = _term_offset.get(pid, 0) % len(all_terms) if all_terms else 0
    terms_this_cycle = []
    for i in range(min(5, len(all_terms))):
        idx = (term_idx + i) % len(all_terms)
        terms_this_cycle.append(all_terms[idx])
    _term_offset[pid] = term_idx + len(terms_this_cycle)

    # ── Build ALL scrape tasks for this profile (run concurrently) ──
    tasks = []

    # Each JobSpy site runs as a SEPARATE task so one site failing doesn't kill others
    JOBSPY_SITES = ["indeed", "linkedin", "google", "glassdoor", "zip_recruiter"]

    for term in terms_this_cycle:
        effective_term = f"{term} remote" if is_remote_only else term

        # JobSpy — each site is its own independent task
        for site in JOBSPY_SITES:
            for loc in (locations if locations else [""]):
                tasks.append((f"JobSpy/{site}", scrape_jobspy(
                    search_term=effective_term, location=loc,
                    results_wanted=15, hours_old=hours, profile_id=profile_id,
                    sites=[site],
                )))

        # Remotive + RemoteOK — only for remote profiles
        if is_remote_only:
            tasks.append(("Remotive", scrape_remotive(term, profile_id)))
            tasks.append(("RemoteOK", scrape_remoteok(term, profile_id)))

        # Arbeitnow — all profiles
        tasks.append(("Arbeitnow", scrape_arbeitnow(term, profile_id)))

    # TheMuse — 1 call per profile
    tasks.append(("TheMuse", scrape_themuse(title, profile_id)))

    # SerpApi (if key)
    if settings.serpapi_key:
        eff = f"{title} remote" if is_remote_only else title
        tasks.append(("SerpApi", scrape_serpapi(eff, locations[0] if locations else "", profile_id)))

    # JSearch (if key)
    if settings.rapidapi_key:
        eff = f"{title} remote" if is_remote_only else title
        tasks.append(("JSearch", scrape_jsearch(eff, locations[0] if locations else "", profile_id)))

    # ── Fire all tasks concurrently ──
    total_new = 0
    errors = []
    results = await asyncio.gather(*[t[1] for t in tasks], return_exceptions=True)

    for (source_name, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            err = f"{source_name} '{title}': {result}"
            errors.append(err)
            logger.error(f"[Scrape] {err}")
        elif isinstance(result, list):
            total_new += len(result)

    logger.info(f"[Scrape] Profile '{title}' done — {len(tasks)} parallel tasks, {total_new} new jobs, terms={terms_this_cycle}")
    return {"new_jobs": total_new, "errors": errors}


async def run_scrape_cycle(profiles: list[dict]) -> dict:
    """
    Run a FULL scrape cycle — ALL profiles × ALL sources × ALL terms CONCURRENTLY.
    Every profile runs in parallel. Within each profile, every source runs in parallel.
    A cycle that used to take 10+ minutes now finishes in ~2 minutes.
    """
    logger.info(f"[Scrape] PARALLEL cycle: {len(profiles)} profiles × all sources")

    # Fire ALL profiles at the same time
    profile_results = await asyncio.gather(
        *[_scrape_one_profile(p) for p in profiles],
        return_exceptions=True,
    )

    total_new = 0
    all_errors = []
    for i, result in enumerate(profile_results):
        if isinstance(result, Exception):
            err = f"Profile '{profiles[i]['title']}' crashed: {result}"
            all_errors.append(err)
            logger.error(f"[Scrape] {err}")
        elif isinstance(result, dict):
            total_new += result.get("new_jobs", 0)
            all_errors.extend(result.get("errors", []))

    logger.info(f"[Scrape] PARALLEL cycle done: {total_new} new jobs total, {len(all_errors)} errors")
    return {
        "new_jobs": total_new,
        "errors": all_errors,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
