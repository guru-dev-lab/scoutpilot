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
# Company-name-based slug fuzzing
#
# Why this exists: most jobs we scrape come from Indeed and LinkedIn, which
# return URLs like ``indeed.com/viewjob?jk=...`` and
# ``linkedin.com/jobs/view/12345`` that contain zero ATS slug info. To catch
# ATS companies behind those sources we have to work from the company_name
# field alone — generate plausible slug variants and probe each ATS API.
# ─────────────────────────────────────────────────────────────────────────────

_NAME_SUFFIX_RE = re.compile(
    r"\b(inc|llc|ltd|limited|corp|corporation|company|co|group|holdings?|labs?|"
    r"technologies|technology|tech|systems|solutions|software|services|"
    r"international|plc|the)\b",
    re.I,
)

# Slug fragments too generic to bother probing standalone
_GENERIC_SOLO_WORDS = {
    "and", "or", "the", "of", "for", "with", "at", "in", "on", "by",
    "to", "from", "as", "a", "an",
}


def slug_variants_from_name(company_name: str) -> list[str]:
    """Generate plausible ATS slug variants for a company name.

    Returns at most ~5 variants ordered by likelihood. Empty list if nothing
    reasonable can be derived.
    """
    if not company_name:
        return []
    name = company_name.strip()
    name = re.sub(r"[,.;:!?]+$", "", name)
    clean = name.replace("&", " and ").replace("/", " ").replace("+", " ")
    clean = _NAME_SUFFIX_RE.sub(" ", clean)
    clean = re.sub(r"[^\w\s-]", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip().lower()
    words = [w for w in clean.split() if w and w not in _GENERIC_SOLO_WORDS]
    if not words:
        return []

    out: list[str] = []
    concat = "".join(words)
    dashed = "-".join(words)
    first = words[0]
    first_two = (words[0] + words[1]) if len(words) >= 2 else ""
    caps = "".join(w.capitalize() for w in words)

    for v in (concat, dashed, first_two, first, caps):
        if v and 2 <= len(v) <= 40 and v not in out:
            out.append(v)
    return out


# Persisted cache of (ats, normalized_company_name) we've already probed
# and come up empty on, so we don't re-waste calls every pass.
_CHECK_CACHE_FILE = COMPANIES_FILE.parent / "discovery_checked.json"


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _load_check_cache() -> set[tuple[str, str]]:
    try:
        if _CHECK_CACHE_FILE.exists():
            data = json.loads(_CHECK_CACHE_FILE.read_text())
            return {tuple(x) for x in data if isinstance(x, list) and len(x) == 2}
    except Exception as e:
        logger.warning(f"[Discovery] couldn't load check cache: {e}")
    return set()


def _save_check_cache(cache: set[tuple[str, str]]) -> None:
    try:
        _CHECK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CHECK_CACHE_FILE.write_text(json.dumps([list(t) for t in cache]))
    except Exception as e:
        logger.warning(f"[Discovery] couldn't save check cache: {e}")


# Fuzzy-match helper for name verification (uses rapidfuzz which is already
# a dependency for the existing dedup logic).
try:
    from rapidfuzz import fuzz as _fuzz
    def _name_match_score(a: str, b: str) -> int:
        if not a or not b:
            return 0
        return int(_fuzz.token_set_ratio(a.lower(), b.lower()))
except Exception:
    def _name_match_score(a: str, b: str) -> int:
        # Fall back to a simple substring heuristic
        if not a or not b:
            return 0
        a, b = a.lower(), b.lower()
        return 100 if (a in b or b in a) else 0


_NAME_MATCH_THRESHOLD = 70


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


# Name-verifying variants: fetch the board's official name (when available)
# and fuzzy-match it against the expected company name. Protects us from
# false positives like slug="capital" matching a random startup when the real
# company is "Capital One".

async def _verify_greenhouse_with_name(
    client: httpx.AsyncClient, slug: str, expected_name: str
) -> Optional[str]:
    try:
        r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if r.status_code != 200:
            return None
        d = r.json()
        board_name = (d.get("name") or "").strip()
        if not board_name:
            return None
        if _name_match_score(board_name, expected_name) < _NAME_MATCH_THRESHOLD:
            return None
        # Also require at least one job
        r2 = await client.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        )
        if r2.status_code != 200:
            return None
        if len(r2.json().get("jobs", [])) < 1:
            return None
        return board_name
    except Exception:
        return None


async def _verify_smartrecruiters_with_name(
    client: httpx.AsyncClient, slug: str, expected_name: str
) -> Optional[str]:
    try:
        r = await client.get(
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1"
        )
        if r.status_code != 200:
            return None
        d = r.json()
        content = d.get("content") or []
        if not content:
            return None
        board_name = (content[0].get("company") or {}).get("name", "")
        if not board_name:
            return None
        if _name_match_score(board_name, expected_name) < _NAME_MATCH_THRESHOLD:
            return None
        return board_name
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main discovery run
# ─────────────────────────────────────────────────────────────────────────────

# How many recent jobs to scan each discovery pass
JOBS_SCAN_LIMIT = 3000

# Hard cap on new companies added in a single pass (safety net)
MAX_NEW_PER_PASS = 50


# ─────────────────────────────────────────────────────────────────────────────
# "Second link" resolver — fetch Indeed/LinkedIn pages and pull the real
# ATS URL out of the rendered HTML.
#
# Most jobs we scrape come from Indeed/LinkedIn, whose URLs contain no ATS
# slug. But the rendered job page HTML *does* — it includes the
# "Apply on company site" href, Indeed's data-indeed-apply-joburl attribute,
# or LinkedIn's companyApplyUrl inside a JSON blob. We fetch the page once,
# scan the HTML for any known ATS URL pattern, and feed whatever we find
# back into the regular URL extractor.
# ─────────────────────────────────────────────────────────────────────────────

# Broad regex that catches ANY occurrence of the 5 supported ATS domains
# anywhere in a blob of text/HTML (href, JSON, data-attrs — we don't care).
_ATS_URL_IN_HTML = re.compile(
    r"""https?://
        (?:
            (?:boards(?:\.eu)?|job-boards)\.greenhouse\.io/[a-z0-9_\-]+
            | [a-z0-9_\-]+\.greenhouse\.io
            | jobs(?:\.eu)?\.lever\.co/[a-z0-9_\-]+
            | jobs\.ashbyhq\.com/[a-z0-9_.\-]+
            | [a-z0-9_\-]+\.ashbyhq\.com
            | [a-z0-9\-]+\.wd\d+\.myworkdayjobs\.com/(?:en-US/)?[A-Za-z0-9_.\-]+
            | (?:jobs|careers)\.smartrecruiters\.com/[A-Za-z0-9_\-]+
        )
    """,
    re.I | re.VERBOSE,
)

# Browser-ish headers to squeeze past light bot checks
_HTML_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
}

# Per-pass cap on how many pages we fetch (this is where the real cost is)
MAX_HTML_FETCHES_PER_PASS = 200

# Which source URLs are worth fetching (domains that don't expose the ATS
# URL in their API responses, so we only learn it from the rendered page)
_HTML_FETCH_HOSTS = (
    "indeed.com",
    "linkedin.com",
    "simplyhired.com",
    "glassdoor.com",
    "ziprecruiter.com",
    "careerbuilder.com",
    "monster.com",
    "dice.com",
    "themuse.com",
    "builtin.com",
    "wellfound.com",
)


def _is_fetch_candidate(url: str) -> bool:
    if not url:
        return False
    url_low = url.lower()
    return any(host in url_low for host in _HTML_FETCH_HOSTS)


async def _fetch_page_and_extract_ats(
    client: httpx.AsyncClient, url: str
) -> list[str]:
    """Fetch a Indeed/LinkedIn-style page and return any ATS URLs found in
    the HTML body. Failures return an empty list silently."""
    try:
        r = await client.get(url, headers=_HTML_FETCH_HEADERS, timeout=12)
        if r.status_code >= 400:
            return []
        text = r.text or ""
        if not text:
            return []
        # Cap scanned text length for perf (some pages are megs of minified JS)
        if len(text) > 500_000:
            text = text[:500_000]
        matches = _ATS_URL_IN_HTML.findall(text)
        # Some matches come back as partial groups when the regex has
        # alternation branches; normalize by re-running a finditer and
        # pulling full matches.
        full = []
        for m in _ATS_URL_IN_HTML.finditer(text):
            full.append(m.group(0))
        # Dedupe while preserving order
        seen = set()
        uniq = []
        for u in full:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        return uniq
    except Exception:
        return []


async def _load_recent_jobs(limit: int = JOBS_SCAN_LIMIT) -> list[dict]:
    """Read recent job rows from the DB with the fields discovery cares about."""
    out: list[dict] = []
    try:
        db = await get_db()
        try:
            async with db.execute(
                """SELECT company_name, direct_apply_url, source_url, source
                   FROM jobs
                   ORDER BY first_seen_at DESC
                   LIMIT ?""",
                (limit,),
            ) as cur:
                async for row in cur:
                    out.append({
                        "company_name": row["company_name"] or "",
                        "direct_apply_url": row["direct_apply_url"] or "",
                        "source_url": row["source_url"] or "",
                        "source": row["source"] or "",
                    })
        finally:
            await db.close()
    except Exception as e:
        logger.warning(f"[Discovery] failed reading jobs: {e}")
    return out


def _dedupe_key(entry: dict) -> tuple[str, str]:
    """Canonical dedupe key for an ats company entry."""
    ats = entry.get("ats") or ""
    if ats == "workday":
        return ("workday", f"{entry.get('tenant')}::{entry.get('site')}")
    return (ats, (entry.get("slug") or "").lower())


async def _verify_candidate(
    client: httpx.AsyncClient, cand: dict
) -> Optional[dict]:
    """Verify one candidate against its ATS API. Returns the final
    companies.json entry on success, or None on failure.

    For Greenhouse and SmartRecruiters we also verify the board's official
    name fuzzy-matches the expected company name, to guard against false
    positives when fuzzing from names like 'Capital' that might collide with
    a random smaller company.
    """
    ats = cand["ats"]
    expected_name = (cand.get("name_hint") or "").strip()
    require_name_match = bool(cand.get("strict_name"))  # set by name-fuzz path

    try:
        if ats == "greenhouse":
            slug = cand["slug"]
            if require_name_match and expected_name:
                matched = await _verify_greenhouse_with_name(client, slug, expected_name)
                if not matched:
                    return None
                return {"name": expected_name, "slug": slug, "ats": "greenhouse"}
            if await _verify_greenhouse(client, slug):
                return {"name": expected_name or slug, "slug": slug, "ats": "greenhouse"}

        elif ats == "lever":
            slug = cand["slug"]
            if await _verify_lever(client, slug):
                return {"name": expected_name or slug, "slug": slug, "ats": "lever"}

        elif ats == "ashby":
            slug = cand["slug"]
            if await _verify_ashby(client, slug):
                return {"name": expected_name or slug, "slug": slug, "ats": "ashby"}

        elif ats == "workday":
            result = await _verify_workday(client, cand)
            if result:
                return {
                    "name": expected_name or result["tenant"],
                    "slug": result["tenant"],
                    "ats": "workday",
                    "tenant": result["tenant"],
                    "wd": result["wd"],
                    "site": result["site"],
                }

        elif ats == "smartrecruiters":
            slug = cand["slug"]
            if require_name_match and expected_name:
                matched = await _verify_smartrecruiters_with_name(client, slug, expected_name)
                if not matched:
                    return None
                return {"name": expected_name, "slug": slug, "ats": "smartrecruiters"}
            if await _verify_smartrecruiters(client, slug):
                return {"name": expected_name or slug, "slug": slug, "ats": "smartrecruiters"}

    except Exception as e:
        logger.debug(f"[Discovery] verify {ats} failed: {e}")
    return None


async def discover_new_ats_companies() -> dict:
    """Run one discovery pass. Three parallel paths:

      1. URL extraction — for jobs whose source URL already contains an ATS
         slug (RSS feeds, direct ATS sources).
      2. HTML fetch ("second link") — for Indeed/LinkedIn/Glassdoor/etc jobs,
         fetch the rendered page and extract any ATS URL that appears in the
         HTML (apply button href, data attributes, JSON blobs).
      3. Company-name fuzzing — generate plausible slug variants from the
         company_name field and probe each ATS API, with fuzzy board-name
         verification on Greenhouse/SmartRecruiters to reject false positives.

    All three feed into the same verification + persistence pipeline.
    Returns stats dict.
    """
    stats = {
        "jobs_scanned": 0,
        "url_candidates": 0,
        "html_fetched": 0,
        "html_candidates": 0,
        "name_variants_tried": 0,
        "already_known": 0,
        "verified_new": 0,
        "failed_verify": 0,
        "added": [],
        "errors": [],
    }

    try:
        jobs = await _load_recent_jobs()
        stats["jobs_scanned"] = len(jobs)
        if not jobs:
            return stats

        existing = load_companies()
        existing_keys: set[tuple[str, str]] = {_dedupe_key(e) for e in existing}
        check_cache = _load_check_cache()

        # Collected candidates: keyed by dedupe_key, holding the richest info
        candidates: dict[tuple[str, str], dict] = {}

        def _offer(cand: dict, name_hint: str = "", strict_name: bool = False):
            """Add a candidate if not already known or already collected."""
            cand.setdefault("ats", cand.get("ats"))
            if name_hint and not cand.get("name_hint"):
                cand["name_hint"] = name_hint
            if strict_name:
                cand["strict_name"] = True
            key = _dedupe_key({"ats": cand["ats"], **cand})
            if key in existing_keys:
                stats["already_known"] += 1
                return
            if key not in candidates:
                candidates[key] = cand

        # ── PATH 1: URL extraction from known URL fields ──
        for j in jobs:
            for url in (j["direct_apply_url"], j["source_url"]):
                for ats, cand in extract_candidates_from_url(url):
                    _offer({"ats": ats, **cand}, name_hint=j["company_name"])
        stats["url_candidates"] = len(candidates)

        # ── PATH 2: HTML fetch for Indeed/LinkedIn/etc ──
        # Pick up to MAX_HTML_FETCHES_PER_PASS unique URLs worth fetching.
        html_urls: list[tuple[str, str]] = []
        seen_urls: set = set()
        for j in jobs:
            for url in (j["source_url"], j["direct_apply_url"]):
                if _is_fetch_candidate(url) and url not in seen_urls:
                    seen_urls.add(url)
                    html_urls.append((j["company_name"], url))
                    if len(html_urls) >= MAX_HTML_FETCHES_PER_PASS:
                        break
            if len(html_urls) >= MAX_HTML_FETCHES_PER_PASS:
                break

        if html_urls:
            fetch_sem = asyncio.Semaphore(10)
            async with httpx.AsyncClient(
                timeout=12,
                follow_redirects=True,
                headers=_HTML_FETCH_HEADERS,
            ) as html_client:

                async def fetch_one(cn_url):
                    cn, url = cn_url
                    async with fetch_sem:
                        urls = await _fetch_page_and_extract_ats(html_client, url)
                        return cn, urls

                fetch_results = await asyncio.gather(
                    *[fetch_one(x) for x in html_urls],
                    return_exceptions=True,
                )

            html_candidate_count = 0
            for res in fetch_results:
                if isinstance(res, Exception) or not res:
                    continue
                cn, urls = res
                if urls:
                    stats["html_fetched"] += 1
                for u in urls:
                    for ats, cand in extract_candidates_from_url(u):
                        _offer({"ats": ats, **cand}, name_hint=cn)
                        html_candidate_count += 1
            stats["html_candidates"] = html_candidate_count
        stats["html_fetched"] = min(stats["html_fetched"], len(html_urls))

        # ── PATH 3: Company-name slug fuzzing ──
        # Collect unique company names NOT already matched via paths 1/2.
        # Skip ones already in check_cache to avoid re-probing failures.
        seen_names: set[str] = set()
        ordered_names: list[str] = []
        already_have_names: set[str] = set()
        for ev in existing:
            if ev.get("name"):
                already_have_names.add(_normalize_name(ev["name"]))

        for j in jobs:
            cn = j["company_name"].strip()
            if not cn:
                continue
            norm = _normalize_name(cn)
            if not norm or norm in seen_names:
                continue
            seen_names.add(norm)
            if norm in already_have_names:
                continue
            ordered_names.append(cn)

        # Build name-based candidates. For each company, pick its top-1
        # slug variant per ATS. Cache checked (ats, normname) so we don't
        # waste calls next pass.
        name_platforms = ("greenhouse", "lever", "ashby", "smartrecruiters")
        name_candidates_added = 0
        for cn in ordered_names:
            variants = slug_variants_from_name(cn)
            if not variants:
                continue
            primary = variants[0]
            norm = _normalize_name(cn)
            for ats in name_platforms:
                if (ats, norm) in check_cache:
                    continue
                _offer(
                    {"ats": ats, "slug": primary},
                    name_hint=cn,
                    strict_name=True,
                )
                name_candidates_added += 1
            # Workday: try concat-lowercase tenant
            wd_tenant = re.sub(r"[^a-z0-9]", "", cn.lower())
            if wd_tenant and 3 <= len(wd_tenant) <= 30 and ("workday", norm) not in check_cache:
                _offer(
                    {"ats": "workday", "tenant": wd_tenant, "wd": None, "site": None},
                    name_hint=cn,
                    strict_name=True,
                )
                name_candidates_added += 1
        stats["name_variants_tried"] = name_candidates_added

        if not candidates:
            logger.info(
                f"[Discovery] nothing new — scanned {len(jobs)} jobs, "
                f"{stats['already_known']} already known"
            )
            return stats

        logger.info(
            f"[Discovery] scanned={len(jobs)} urls={stats['url_candidates']} "
            f"html_pages={len(html_urls)} html_found={stats['html_candidates']} "
            f"name_probes={stats['name_variants_tried']} "
            f"unique_candidates={len(candidates)} known={stats['already_known']}"
        )

        # ── Verification ──
        sem = asyncio.Semaphore(15)
        async with httpx.AsyncClient(
            timeout=15, headers=HTTP_HEADERS, follow_redirects=True
        ) as client:

            async def one(cand):
                async with sem:
                    return await _verify_candidate(client, cand)

            results = await asyncio.gather(*[one(c) for c in candidates.values()])

        verified_new: list[dict] = []
        newly_checked: set[tuple[str, str]] = set()
        for cand, r in zip(candidates.values(), results):
            if r:
                verified_new.append(r)
            else:
                stats["failed_verify"] += 1
                # Only cache negatives for name-fuzz attempts; URL/HTML paths
                # may succeed later when the board opens up.
                if cand.get("strict_name") and cand.get("name_hint"):
                    newly_checked.add(
                        (cand["ats"], _normalize_name(cand["name_hint"]))
                    )

        # Persist negative cache growth
        if newly_checked:
            _save_check_cache(check_cache | newly_checked)

        if verified_new:
            verified_new = verified_new[:MAX_NEW_PER_PASS]
            combined = existing + verified_new
            seen: set = set()
            unique = []
            for e in combined:
                k = _dedupe_key(e)
                if k not in seen:
                    seen.add(k)
                    unique.append(e)
            unique.sort(key=lambda x: (x.get("ats", ""), x.get("name", "")))

            if save_companies(unique):
                stats["verified_new"] = len(verified_new)
                stats["added"] = [
                    {"name": v["name"], "slug": v.get("slug"), "ats": v["ats"]}
                    for v in verified_new
                ]
                logger.info(
                    f"[Discovery] +{len(verified_new)} new ATS companies: "
                    + ", ".join(f"{v['ats']}:{v['name']}" for v in verified_new[:10])
                    + (" ..." if len(verified_new) > 10 else "")
                )
            else:
                stats["errors"].append("save_companies returned False")
        else:
            logger.info(f"[Discovery] 0 verified out of {len(candidates)} candidates")

    except Exception as e:
        logger.error(f"[Discovery] crashed: {e}")
        stats["errors"].append(str(e))

    return stats
