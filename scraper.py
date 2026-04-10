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
    "teal", "jobscan", "huntr", "careerflow", "jobgether",
    "flexjobs", "pangian",
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

    IMPORTANT: LinkedIn/JobSpy is_remote flag is UNRELIABLE — it marks many
    onsite jobs as remote.  We ignore the flag entirely and rely on text
    analysis with a tiered approach:
      - Title/location: trusted (one mention of 'remote' is enough)
      - Description: requires STRONG signals (not just a passing mention)
    """
    title = str(row.get("title", "")).lower()
    location = str(row.get("location", "")).lower()
    description = str(row.get("description", ""))[:3000].lower()
    source = str(row.get("source", row.get("site", ""))).lower()

    title_loc = f"{title} {location}"
    text = f"{title} {location} {description}"

    # ── ONSITE OVERRIDE: strong onsite signals beat everything ──
    onsite_override = [
        r'\bon[\s\-]?site\s+only\b', r'\bno\s+remote\b',
        r'\bmust\s+(be\s+)?on[\s\-]?site\b', r'\bnot\s+remote\b',
        r'\bin[\s\-]?office\s+only\b', r'\bon[\s\-]?site\s+required\b',
        r'\bmust\s+work\s+(in|from)\s+(the\s+)?office\b',
        r'\bno\s+work\s*from\s*home\b', r'\bnot\s+a\s+remote\b',
    ]
    for pat in onsite_override:
        if re.search(pat, text):
            return "onsite"

    # ── HYBRID patterns (check before remote — hybrid is more specific) ──
    hybrid_patterns = [
        r'\bhybrid\b', r'\bhybrid[\s\-]?remote\b', r'\bremote[\s\-]?hybrid\b',
        r'\b\d+\s*days?\s*(in[\s\-]?office|on[\s\-]?site|onsite)\b',
        r'\bin[\s\-]?office\s*\d+\s*days?\b',
    ]
    for pat in hybrid_patterns:
        if re.search(pat, text):
            return "hybrid"

    # ── FALSE-POSITIVE filter: "remote" meaning physical locations, not WFH ──
    # Must run BEFORE remote detection to reject these
    remote_noise = [
        r'\bremote\s+project', r'\bremote\s+site', r'\bremote\s+location',
        r'\bremote\s+area', r'\bremote\s+region', r'\bremote\s+field',
        r'\bremote\s+facilit', r'\bremote\s+camp', r'\bremote\s+communit',
        r'\bremote\s+sensing', r'\bremote\s+monitor', r'\bremote\s+control',
        r'\bremote\s+support', r'\bremote\s+access', r'\bremote\s+diagnos',
        r'\bremote\s+troubleshoot', r'\bremote\s+install',
    ]
    title_has_noise = any(re.search(pat, title) for pat in remote_noise)

    # ── REMOTE: title or location says remote → trust it ──
    # But ONLY if "remote" appears as a work-arrangement term, not a physical descriptor
    title_loc_remote_strong = [
        r'\bfully[\s\-]?remote\b', r'\b100%\s*remote\b',
        r'\bwork\s*from\s*home\b', r'\bwfh\b',
        r'\btelecommute\b', r'\btelework\b', r'\banywhere\b',
        r'\(remote\)', r'\bremote\s*[-/|]\s*\w',  # "(Remote)" or "Remote / US"
    ]
    for pat in title_loc_remote_strong:
        if re.search(pat, title_loc):
            return "remote"

    # Plain \bremote\b in title/location — only if no noise patterns present
    if not title_has_noise and re.search(r'\bremote\b', title_loc):
        # Extra check: "remote" right next to physical-place words is noise
        if not re.search(r'\bremote\s+(project|site|location|area|field|facilit|camp|install)', title_loc):
            return "remote"

    # ── REMOTE: description only → require STRONG signals ──
    # A single "\bremote\b" in a long description is often noise
    # ("remote support", "remote teams", onsite job that "offers no remote")
    strong_desc_remote = [
        r'\bfully[\s\-]?remote\b', r'\b100%\s*remote\b',
        r'\bwork\s*from\s*home\b', r'\bwfh\b',
        r'\btelecommute\b', r'\btelework\b',
        r'\bremote[\s\-]?first\b', r'\bremote[\s\-]?friendly\b',
        r'\bremote\s+position\b', r'\bremote\s+role\b',
        r'\bremote\s+job\b', r'\bremote\s+opportunity\b',
        r'\bthis\s+(is\s+a\s+)?remote\b',
        r'\bopen\s+to\s+remote\b', r'\bremote\s+eligible\b',
        r'\bdesignated\s+as\s+.{0,20}remote\b',
    ]
    for pat in strong_desc_remote:
        if re.search(pat, description):
            return "remote"

    # "work remotely" / "working remotely" — only if it's a positive statement
    # Skip if negated: "no option to work remotely", "not able to work remotely"
    if re.search(r'\b(work|working)\s+remotely\b', description):
        # Check for negation within 30 chars before the match
        m = re.search(r'(.{0,30})\b(work|working)\s+remotely\b', description)
        if m:
            prefix = m.group(1).lower()
            negators = ['no ', 'not ', 'cannot ', "can't ", 'unable ', 'without ',
                        'occasional', 'sometimes', 'may ', 'might ', 'option to']
            if not any(neg in prefix for neg in negators):
                return "remote"

    # ── REMOTE-ONLY sources: if the source IS a remote job board, trust it ──
    remote_sources = {"remotive", "remoteok", "weworkremotely", "jobicy", "himalayas"}
    if source in remote_sources:
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
                "verbose": 2,  # Maximum verbosity to diagnose failures
            }
            if location:
                kwargs["location"] = location

            logger.info(f"[JobSpy] Starting: term='{search_term}' loc='{location}' sites={sites}")
            results = scrape_jobs(**kwargs)
            count = len(results) if results is not None and not results.empty else 0
            if count == 0:
                logger.warning(f"[JobSpy] EMPTY for '{search_term}' @ '{location}' sites={sites} — possible rate limit or IP block")
            else:
                logger.info(f"[JobSpy] Raw results for '{search_term}': {count} rows")
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
        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            # Broad fetch first — Remotive's server-side search is too strict
            # for niche terms. Fetch latest 100 jobs and filter client-side.
            resp = await client.get(
                "https://remotive.com/api/remote-jobs",
                params={"limit": 100},
            )
            if resp.status_code != 200:
                logger.error(f"[Remotive] HTTP {resp.status_code}: {resp.text[:200]}")
                return []
            data = resp.json()

        for item in data.get("jobs", []):
            # Client-side filter — any search word in title or category
            item_title = (item.get("title", "") or "").lower()
            item_category = (item.get("category", "") or "").lower()
            combined_rem = f"{item_title} {item_category}"
            if not any(w in combined_rem for w in search_words):
                continue
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
        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        # Fetch multiple pages to maximize matches
        all_results = []
        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            for page in range(1, 6):  # 5 pages for more coverage
                resp = await client.get(
                    "https://www.themuse.com/api/public/jobs",
                    params={"page": page, "descending": "true"},
                )
                if resp.status_code != 200:
                    logger.warning(f"[TheMuse] HTTP {resp.status_code} on page {page}")
                    break
                page_data = resp.json()
                results = page_data.get("results", [])
                if not results:
                    break
                all_results.extend(results)

        search_lower = search_term.lower()
        search_words = search_lower.replace(" remote", "").strip().split()

        for item in all_results:
            title = item.get("name", "")
            company_data = item.get("company", {})
            company = company_data.get("name", "") if isinstance(company_data, dict) else ""

            if _is_blocked_company(company):
                continue

            # Match: any search word in title (more flexible than exact match)
            title_lower = title.lower()
            if not any(w in title_lower for w in search_words):
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
        logger.info(f"[TheMuse] Found {len(all_results)} results, inserted {len(jobs)} new ({direct_count} direct apply)")
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
        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        all_items = []
        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            # Paginate up to 3 pages
            for page_num in range(1, 4):
                try:
                    resp = await client.get(
                        "https://www.arbeitnow.com/api/job-board-api",
                        params={"page": page_num},
                    )
                    if resp.status_code != 200:
                        logger.error(f"[Arbeitnow] HTTP {resp.status_code} on page {page_num}")
                        break
                    data = resp.json()
                    page_items = data.get("data", [])
                    if not page_items:
                        break
                    all_items.extend(page_items)
                except Exception as e:
                    logger.warning(f"[Arbeitnow] Page {page_num} error: {e}")
                    break

        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        for item in all_items:
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


async def scrape_jobicy(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape remote jobs from Jobicy (free API, no key needed)."""
    logger.info(f"[Jobicy] Searching: '{search_term}'")

    jobs = []
    try:
        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        # Jobicy: broad fetch, no tag/geo filters (both too restrictive, return 0)
        # Their API returns the latest 50 remote jobs — filter client-side
        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            resp = await client.get(
                "https://jobicy.com/api/v2/remote-jobs",
                params={"count": 50},
            )
            if resp.status_code != 200:
                logger.error(f"[Jobicy] HTTP {resp.status_code}: {resp.text[:200]}")
                return []
            data = resp.json()

        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        for item in data.get("jobs", []):
            title = item.get("jobTitle", "")
            company = item.get("companyName", "")

            if _is_blocked_company(company):
                continue

            # Relevance: any search word in title OR job industry/type
            title_lower = title.lower()
            job_type = (item.get("jobType", "") or "").lower()
            raw_industry = item.get("jobIndustry", "") or ""
            if isinstance(raw_industry, list):
                industry = " ".join(str(i) for i in raw_industry).lower()
            else:
                industry = str(raw_industry).lower()
            combined = f"{title_lower} {job_type} {industry}"
            if not any(w in combined for w in search_words):
                continue

            apply_url = item.get("url", "")
            is_direct = _is_direct_url(apply_url)
            geo = item.get("jobGeo", "Remote")
            description = item.get("jobDescription", "")
            clean_desc = re.sub(r"<[^>]+>", " ", description).strip()

            salary_min = 0
            salary_max = 0
            try:
                salary_min = int(float(item.get("annualSalaryMin", 0) or 0))
                salary_max = int(float(item.get("annualSalaryMax", 0) or 0))
            except (ValueError, TypeError):
                pass

            job = {
                "title": title,
                "company_name": company,
                "company_domain": "",
                "location": geo,
                "is_remote": True,
                "work_type": "remote",
                "description": clean_desc[:10000],
                "salary_min": salary_min,
                "salary_max": salary_max,
                "source": "jobicy",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": _normalize_posted_at(item.get("pubDate", "")),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[Jobicy] Matched {len(jobs)} new jobs for '{search_term}'")
    except Exception as e:
        logger.error(f"[Jobicy] Error: {e}")

    return jobs


async def scrape_himalayas(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape remote jobs from Himalayas (free API, no key needed).
    API max is 20 per request — paginate with offset to get more."""
    logger.info(f"[Himalayas] Searching: '{search_term}'")

    jobs = []
    try:
        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()
        all_items = []

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            # Browse endpoint — paginate 20/page up to 5 pages, filter client-side
            # The 'q' param returns irrelevant results (tested: "data analyst" → "Campus Ambassador")
            for page_offset in range(0, 100, 20):
                try:
                    resp = await client.get(
                        "https://himalayas.app/jobs/api",
                        params={"limit": 20, "offset": page_offset},
                    )
                    if resp.status_code == 429:
                        logger.warning(f"[Himalayas] Rate limited at offset {page_offset}")
                        break
                    if resp.status_code != 200:
                        logger.error(f"[Himalayas] HTTP {resp.status_code} at offset {page_offset}")
                        break
                    data = resp.json()
                    page_jobs = data.get("jobs", [])
                    if not page_jobs:
                        break
                    all_items.extend(page_jobs)
                except Exception as e:
                    logger.warning(f"[Himalayas] Page error at offset {page_offset}: {e}")
                    break

        for item in all_items:
            title = item.get("title", "")
            company = item.get("companyName", item.get("company_name", ""))

            if _is_blocked_company(company):
                continue

            title_lower = title.lower()
            categories = " ".join(item.get("categories", [])).lower() if item.get("categories") else ""
            combined_him = f"{title_lower} {categories}"
            if not any(w in combined_him for w in search_words):
                continue

            apply_url = item.get("applicationUrl", item.get("url", ""))
            if not apply_url:
                continue
            is_direct = _is_direct_url(apply_url)
            description = item.get("description", "")
            clean_desc = re.sub(r"<[^>]+>", " ", description).strip()

            job = {
                "title": title,
                "company_name": company,
                "company_domain": "",
                "location": item.get("location", "Remote"),
                "is_remote": True,
                "work_type": "remote",
                "description": clean_desc[:10000],
                "salary_min": 0,
                "salary_max": 0,
                "source": "himalayas",
                "source_url": apply_url,
                "direct_apply_url": apply_url if is_direct else "",
                "posted_at": _normalize_posted_at(item.get("pubDate", item.get("created_at", ""))),
                "is_direct_apply": is_direct,
                "search_profile_id": profile_id,
            }

            was_inserted = await insert_job(job)
            if was_inserted:
                jobs.append(job)

        logger.info(f"[Himalayas] Fetched {len(all_items)} total, matched {len(jobs)} new jobs for '{search_term}'")
    except Exception as e:
        logger.error(f"[Himalayas] Error: {e}")

    return jobs


async def scrape_weworkremotely(
    search_term: str,
    profile_id: Optional[int] = None,
) -> list[dict]:
    """Scrape remote jobs from WeWorkRemotely RSS feed (free, no key needed)."""
    logger.info(f"[WWR] Searching: '{search_term}'")

    jobs = []
    try:
        import feedparser

        headers = {"User-Agent": "ScoutPilot/1.0 (job search aggregator)"}
        # WWR has category-based RSS feeds — use the main programming feed
        rss_urls = [
            "https://weworkremotely.com/categories/remote-programming-jobs.rss",
            "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
            "https://weworkremotely.com/categories/remote-data-jobs.rss",
            "https://weworkremotely.com/categories/remote-business-exec-management-jobs.rss",
            "https://weworkremotely.com/categories/remote-finance-legal-jobs.rss",
        ]

        search_lower = search_term.lower().replace(" remote", "").strip()
        search_words = search_lower.split()

        async with httpx.AsyncClient(timeout=30, headers=headers) as client:
            for rss_url in rss_urls:
                try:
                    resp = await client.get(rss_url)
                    if resp.status_code != 200:
                        continue
                    feed = feedparser.parse(resp.text)

                    for entry in feed.entries:
                        title = entry.get("title", "")
                        # WWR titles often have format "Company: Job Title"
                        parts = title.split(":", 1)
                        if len(parts) == 2:
                            company = parts[0].strip()
                            job_title = parts[1].strip()
                        else:
                            company = ""
                            job_title = title

                        # Match: any search word in title
                        title_lower = job_title.lower()
                        if not any(w in title_lower for w in search_words):
                            continue

                        if _is_blocked_company(company):
                            continue

                        apply_url = entry.get("link", "")
                        description = entry.get("summary", entry.get("description", ""))
                        clean_desc = re.sub(r"<[^>]+>", " ", description).strip()
                        posted_at = _normalize_posted_at(entry.get("published", ""))

                        job = {
                            "title": job_title,
                            "company_name": company,
                            "company_domain": "",
                            "location": "Remote",
                            "is_remote": True,
                            "work_type": "remote",
                            "description": clean_desc[:10000],
                            "salary_min": 0,
                            "salary_max": 0,
                            "source": "weworkremotely",
                            "source_url": apply_url,
                            "direct_apply_url": apply_url if _is_direct_url(apply_url) else "",
                            "posted_at": posted_at,
                            "is_direct_apply": _is_direct_url(apply_url),
                            "search_profile_id": profile_id,
                        }

                        was_inserted = await insert_job(job)
                        if was_inserted:
                            jobs.append(job)
                except Exception as e:
                    logger.debug(f"[WWR] RSS error for {rss_url}: {e}")

        logger.info(f"[WWR] Matched {len(jobs)} new jobs for '{search_term}'")
    except ImportError:
        logger.warning("[WWR] feedparser not installed — skipping")
    except Exception as e:
        logger.error(f"[WWR] Error: {e}")

    return jobs


async def _scrape_one_profile(profile: dict, cycle_number: int = 1) -> dict:
    """Scrape sources for ONE profile — frequency based on source rate limits.

    Source tiers (based on rate limit behavior):
      EVERY cycle:  Remotive, RemoteOK, WWR (public APIs, generous limits)
      Every 3rd:    JobSpy main (Indeed+LinkedIn — aggressive anti-bot)
      Every 3rd:    JobSpy extra (Google+ZipRecruiter — moderate limits)
      Every 5th:    Jobicy (blocks >1x/hour), Himalayas (429 risk), TheMuse, Arbeitnow
    """
    title = profile["title"]
    expanded = profile.get("expanded_titles", [])
    locations = profile.get("locations", [])
    profile_id = profile["id"]
    # Build search queries from title + expanded titles
    search_terms = [title] + [t for t in expanded if t.lower() != title.lower()]

    # Add keyword-based search terms
    keywords = profile.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    for kw in keywords:
        if kw.lower() not in [s.lower() for s in search_terms]:
            search_terms.append(kw)

    # Search ALL terms every cycle — they run concurrently so it's fast
    # More terms = more chances to find freshly posted jobs
    all_terms = search_terms[:35]  # Allow up to 35 expanded titles (was 20 — too low for analyst roles)
    terms_this_cycle = all_terms if all_terms else [title]

    # ── Build ALL scrape tasks for this profile (run concurrently) ──
    # STRATEGY: Scrape EVERYTHING — do NOT filter by remote/onsite at scrape time.
    # Let the UI filters handle work type. This catches 5x more jobs.
    tasks = []

    # ── Source frequency tiers (based on rate limit behavior) ──
    # TIER 1 — EVERY cycle: public APIs with generous/no rate limits
    # TIER 2 — Every 3rd cycle: JobSpy (Indeed/LinkedIn/Google/Zip) — anti-bot, IP throttle
    # TIER 3 — Every 5th cycle: Strict rate-limiters (Jobicy, Himalayas, TheMuse, Arbeitnow)
    run_jobspy = (cycle_number % 3 == 1)      # Cycles 1, 4, 7, 10...
    run_strict = (cycle_number % 5 == 1)      # Cycles 1, 6, 11, 16...

    MAIN_SITES = ["indeed", "linkedin"]
    EXTRA_SITES = ["google", "zip_recruiter"]

    # For remote-only profiles with no locations, use broad US search
    remote_only = profile.get("remote_only", 0)
    effective_locations = locations if locations else (["USA"] if remote_only else [""])

    # ── LIGHT TASKS: Remotive, RemoteOK, WWR, Jobicy etc. — safe to run in parallel ──
    light_tasks = []

    for term in terms_this_cycle:
        # TIER 1: Remotive + RemoteOK — every cycle (generous limits)
        light_tasks.append(("Remotive", scrape_remotive(term, profile_id)))
        light_tasks.append(("RemoteOK", scrape_remoteok(term, profile_id)))

    # TIER 3: Strict rate-limited APIs — every 5th cycle
    if run_strict:
        light_tasks.append(("Jobicy", scrape_jobicy(title, profile_id)))
        light_tasks.append(("Himalayas", scrape_himalayas(title, profile_id)))
        light_tasks.append(("Arbeitnow", scrape_arbeitnow(title, profile_id)))
        light_tasks.append(("TheMuse", scrape_themuse(title, profile_id)))

    # TIER 1: WeWorkRemotely RSS — every cycle
    light_tasks.append(("WWR", scrape_weworkremotely(title, profile_id)))

    # SerpApi (if key) — every 3rd cycle
    if settings.serpapi_key and run_jobspy:
        light_tasks.append(("SerpApi", scrape_serpapi(title, locations[0] if locations else "", profile_id)))

    # JSearch (if key)
    if settings.rapidapi_key:
        light_tasks.append(("JSearch", scrape_jsearch(title, locations[0] if locations else "", profile_id)))

    # Fire light tasks in parallel (they're fast APIs with no scraping)
    total_new = 0
    errors = []
    if light_tasks:
        light_results = await asyncio.gather(*[t[1] for t in light_tasks], return_exceptions=True)
        for (source_name, _), result in zip(light_tasks, light_results):
            if isinstance(result, Exception):
                errors.append(f"{source_name} '{title}': {result}")
                logger.error(f"[Scrape] {source_name} '{title}': {result}")
            elif isinstance(result, list):
                total_new += len(result)

    # ── HEAVY TASKS: JobSpy (Indeed/LinkedIn) — SEQUENTIAL with delays ──
    # Indeed/LinkedIn aggressively block IPs that send too many requests.
    # Running these sequentially with delays prevents rate limiting.
    if run_jobspy:
        # Pick a SMALL subset of terms to avoid hammering (top 5 most relevant)
        jobspy_terms = terms_this_cycle[:5]
        for term in jobspy_terms:
            effective_term = f"{term} remote" if remote_only else term
            for loc in effective_locations:
                try:
                    result = await scrape_jobspy(
                        search_term=effective_term, location=loc,
                        results_wanted=50, hours_old=72, profile_id=profile_id,
                        sites=MAIN_SITES,
                    )
                    total_new += len(result) if isinstance(result, list) else 0
                except Exception as e:
                    errors.append(f"JobSpy/main '{term}': {e}")
                    logger.error(f"[Scrape] JobSpy/main '{term}': {e}")
                # Delay between JobSpy calls to avoid rate limiting
                await asyncio.sleep(3)

            # Extra boards (Google, ZipRecruiter) — less aggressive
            for loc in effective_locations:
                try:
                    result = await scrape_jobspy(
                        search_term=effective_term, location=loc,
                        results_wanted=30, hours_old=72, profile_id=profile_id,
                        sites=EXTRA_SITES,
                    )
                    total_new += len(result) if isinstance(result, list) else 0
                except Exception as e:
                    errors.append(f"JobSpy/extra '{term}': {e}")
                    logger.error(f"[Scrape] JobSpy/extra '{term}': {e}")
                await asyncio.sleep(2)

    logger.info(f"[Scrape] Profile '{title}' done — {total_new} new jobs, {len(errors)} errors")
    return {"new_jobs": total_new, "errors": errors}


async def run_scrape_cycle(profiles: list[dict], cycle_number: int = 1) -> dict:
    """
    Run a scrape cycle — profiles run SEQUENTIALLY to avoid IP blocking.

    Cycle 1: ALL sources (full sweep)
    Cycle 2-3: Fast sources only (Remotive, RemoteOK, WWR)
    Cycle 4: JobSpy + fast sources
    Cycle 5: Fast sources only
    Cycle 6: ALL sources again (JobSpy + strict APIs + fast)
    ...and so on.
    """
    jobspy_on = (cycle_number % 3 == 1)
    strict_on = (cycle_number % 5 == 1)
    tier_label = "ALL sources" if (jobspy_on and strict_on) else ("JobSpy + fast" if jobspy_on else ("strict + fast" if strict_on else "fast only"))
    logger.info(f"[Scrape] Cycle #{cycle_number} ({tier_label}): {len(profiles)} profiles — SEQUENTIAL")

    total_new = 0
    all_errors = []

    for profile in profiles:
        title = profile.get("title", "?")
        try:
            result = await _scrape_one_profile(profile, cycle_number)
            if isinstance(result, dict):
                total_new += result.get("new_jobs", 0)
                all_errors.extend(result.get("errors", []))
        except Exception as e:
            err = f"Profile '{title}' crashed: {e}"
            all_errors.append(err)
            logger.error(f"[Scrape] {err}")
        # Brief pause between profiles to spread load
        await asyncio.sleep(2)

    logger.info(f"[Scrape] Sequential cycle done: {total_new} new jobs total, {len(all_errors)} errors")
    return {
        "new_jobs": total_new,
        "errors": all_errors,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
