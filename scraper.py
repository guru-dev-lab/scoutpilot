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


def _normalize_job(row: dict, source: str, profile_id: Optional[int] = None) -> dict:
    """Normalize a scraped job into our DB schema format."""
    title = str(row.get("title", "")).strip()
    company = str(row.get("company_name", row.get("company", ""))).strip()
    location = str(row.get("location", "")).strip()
    description = str(row.get("description", "")).strip()

    job_url = str(row.get("job_url", row.get("link", row.get("url", "")))).strip()
    company_url = str(row.get("company_url", "")).strip()

    is_direct = False
    direct_url = ""
    if job_url:
        aggregator_domains = ["linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com", "google.com"]
        is_aggregator = any(d in job_url.lower() for d in aggregator_domains)
        if not is_aggregator:
            is_direct = True
            direct_url = job_url

    salary_min = 0
    salary_max = 0
    if row.get("min_amount"):
        try: salary_min = int(float(row["min_amount"]))
        except (ValueError, TypeError): pass
    if row.get("max_amount"):
        try: salary_max = int(float(row["max_amount"]))
        except (ValueError, TypeError): pass

    is_remote = False
    if row.get("is_remote"): is_remote = True
    elif "remote" in location.lower() or "remote" in title.lower(): is_remote = True

    posted_at = str(row.get("date_posted", row.get("posted_at", ""))).strip()

    domain = ""
    if company_url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(company_url if company_url.startswith("http") else f"https://{company_url}")
            domain = parsed.netloc.replace("www.", "")
        except Exception: pass

    return {
        "title": title,
        "company_name": company,
        "company_domain": domain,
        "location": location,
        "is_remote": is_remote,
        "description": description[:10000],
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
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        for k, v in row_dict.items():
            if isinstance(v, float) and str(v) == 'nan': row_dict[k] = ""

        source = str(row_dict.get("site", "jobspy")).lower()
        job = _normalize_job(row_dict, source, profile_id)

        if not job["title"] or not job["source_url"]: continue

        was_inserted = await insert_job(job)
        if was_inserted:
            inserted += 1
            jobs.append(job)

    logger.info(f"[JobSpy] Found {len(df)} results, inserted {inserted} new jobs")
    return jobs


async def scrape_serpapi(
    search_term: str,
    location: str = "",
    profile_id: Optional[int] = None,
) -> list[dict]:
    if not settings.serpapi_key: return []

    logger.info(f"[SerpApi] Searching: '{search_term}'")

    params = {
        "engine": "google_jobs",
        "q": search_term,
        "api_key": settings.serpapi_key,
        "chips": "date_posted:today",
    }
    if location: params["location"] = location

    jobs = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get("https://serpapi.com/search", params=params)
            data = resp.json()

        for item in data.get("jobs_results", []):
            job_url = ""
            direct_url = ""
            is_direct = False

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

            if not job_url and not direct_url: continue

            job = {
                "title": item.get("title", ""),
                "company_name": item.get("company_name", ""),
                "company_domain": "",
                "location": item.get("location", ""),
                "is_remote": "remote" in item.get("location", "").lower(),
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
            if was_inserted: jobs.append(job)

        logger.info(f"[SerpApi] Found {len(data.get('jobs_results', []))} results, inserted {len(jobs)} new")
    except Exception as e:
        logger.error(f"[SerpApi] Error: {e}")

    return jobs


async def scrape_jsearch(
    search_term: str,
    location: str = "",
    profile_id: Optional[int] = None,
) -> list[dict]:
    if not settings.rapidapi_key: return []

    logger.info(f"[JSearch] Searching: '{search_term}'")

    query = search_term
    if location: query += f" in {location}"

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
            job_url = item.get("job_apply_link", "")
            is_direct = item.get("job_apply_is_direct", False)

            job = {
                "title": item.get("job_title", ""),
                "company_name": item.get("employer_name", ""),
                "company_domain": item.get("employer_website", "").replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/"),
                "location": f"{item.get('job_city', '')}, {item.get('job_state', '')}".strip(", "),
                "is_remote": item.get("job_is_remote", False),
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
            if was_inserted: jobs.append(job)

        logger.info(f"[JSearch] Found {len(data.get('data', []))} results, inserted {len(jobs)} new")
    except Exception as e:
        logger.error(f"[JSearch] Error: {e}")

    return jobs


async def run_scrape_cycle(profiles: list[dict]) -> dict:
    total_new = 0
    errors = []

    for profile in profiles:
        title = profile["title"]
        expanded = profile.get("expanded_titles", [])
        locations = profile.get("locations", [])
        profile_id = profile["id"]
        hours = profile.get("freshness_hours", 24)

        search_terms = [title] + [t for t in expanded if t.lower() != title.lower()]

        for term in search_terms[:5]:
            for loc in (locations if locations else [""]):
                try:
                    new_jobs = await scrape_jobspy(search_term=term, location=loc, results_wanted=30, hours_old=hours, profile_id=profile_id)
                    total_new += len(new_jobs)
                    new_jobs = await scrape_serpapi(term, loc, profile_id)
                    total_new += len(new_jobs)
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
