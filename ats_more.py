"""
More ATS platforms — UKG (UltiPro), Oracle Cloud Recruiting, ADP WorkforceNow,
Rippling, BambooHR, Jobvite and iCIMS. Same contract as ats_scraper's
fetchers: ``fetch(client, company, profile_id, search_terms) -> list[dict]``
of inserted jobs, never raises.

Owner's instruction, 22 Sep 2026: "You need to be scrapping them too thats why
we have workers for them too.. whatever you need to do" — and, on verifying
from his Mac: "not here.. on the website and railway and all". So nothing here
was probed from a laptop. Each fetcher is written to the platform's public
board API as documented in the wild, parses defensively, and LOGS THE SHAPE of
the first response it sees per platform (top-level keys + first item's keys),
so the Railway log is where the shape is confirmed. ``/api/debug/ats-probe``
runs any fetcher for one company with inserts switched off and returns what it
parsed — the verification channel that lives on Railway, not here.

Roster entries reuse the discovered_companies columns Workday already needed:
  ukg      slug=org code (IVE1000IVEC)  tenant=host (recruiting2.ultipro.com)  site=board GUID
  oracle   slug=host/site               tenant=host (jpmc.fa.oraclecloud.com)  site=CX_1001
  adp      slug=cid (GUID)              tenant=host (workforcenow.adp.com)     site=ccId
  icims    slug=host (careers-x.icims.com)
  rippling / bamboohr / jobvite   slug only
"""
from __future__ import annotations

import contextvars
import html as _html
import logging
import re
import time
from typing import Optional

import httpx

logger = logging.getLogger("scoutpilot.ats")

# Set by the probe route: parse everything, insert nothing.
DRY_RUN: contextvars.ContextVar[bool] = contextvars.ContextVar("ats_dry_run", default=False)

_SHAPE_LOGGED: set[str] = set()


def _log_shape(platform: str, top, item) -> None:
    """Once per platform per process: what the API actually returned."""
    if platform in _SHAPE_LOGGED:
        return
    _SHAPE_LOGGED.add(platform)
    try:
        tk = sorted(top.keys())[:20] if isinstance(top, dict) else type(top).__name__
        ik = sorted(item.keys())[:40] if isinstance(item, dict) else type(item).__name__
        logger.warning(f"[{platform}] SHAPE top={tk} item={ik}")
    except Exception:
        pass


def _h():
    """ats_scraper's helpers, imported lazily (it imports this module last)."""
    import ats_scraper as a
    from scraper import _is_blocked_company, _normalize_posted_at
    from database import insert_job
    return a, _is_blocked_company, _normalize_posted_at, insert_job


def _strip(s) -> str:
    s = _html.unescape(re.sub(r"<[^>]+>", " ", str(s or "")))
    return re.sub(r"\s+", " ", s).strip()


def _epoch_or_str(v) -> str:
    """UKG dates arrive as '/Date(1690000000000)/'; pass epoch millis through."""
    if v is None:
        return ""
    s = str(v)
    m = re.search(r"/Date\((\d+)", s)
    return m.group(1) if m else s


async def _emit(job: dict, inserted: list) -> None:
    a, _blk, _np, insert_job = _h()
    if DRY_RUN.get():
        inserted.append(job)
        return
    if await insert_job(job):
        inserted.append(job)


def _row(source: str, title: str, company_name: str, location: str, url: str,
         posted, desc: str, profile_id, platform_field: str = "",
         country: str = "") -> Optional[dict]:
    a, _is_blocked_company, _normalize_posted_at, _ins = _h()
    if not title or not url:
        return None
    wt, is_remote = a._derive_work_type(platform_field, location, title, desc[:3000])
    us = (country or "").strip().upper() in ("US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA")
    if not us and location and not a.is_us_location(location):
        return None   # "Remote, International" in the UK is still not a US job
    if not us and not location and not is_remote:
        # No country, no location, no remote flag — the platform said nothing
        # about where this job is; keep it only when the title says remote.
        if "remote" not in title.lower():
            return None
    if _is_blocked_company(company_name):
        return None
    return {
        "title": title,
        "company_name": company_name,
        "company_domain": "",
        "location": location or ("Remote, US" if is_remote else ""),
        "is_remote": is_remote,
        "work_type": wt,
        "description": desc[:4000],
        "salary_min": 0,
        "salary_max": 0,
        "source": source,
        "source_url": url,
        "direct_apply_url": url,
        "posted_at": _normalize_posted_at(str(posted or "")),
        "is_direct_apply": True,
        "search_profile_id": profile_id,
    }


# ── UKG Pro Recruiting (recruiting.ultipro.com / recruiting2.ultipro.com) ───

_UKG_BODY = {
    "opportunitySearch": {
        "Top": 100, "Skip": 0, "QueryString": "",
        "OrderBy": [{"Value": "postedDateDesc", "PropertyName": "PostedDate", "Ascending": False}],
        "Filters": [],
    },
    "matchCriteria": {
        "PreferredJobs": [], "Educations": [], "LicenseAndCertifications": [],
        "Skills": [], "hasNoLicenses": False, "SkippedSkills": [],
    },
}


def ukg_urls(c: dict) -> tuple[str, str]:
    host = c.get("tenant") or "recruiting2.ultipro.com"
    org, board = c.get("slug") or "", c.get("site") or ""
    base = f"https://{host}/{org}/JobBoard/{board}"
    return base, f"{base}/JobBoardView/LoadSearchResults"


async def fetch_ukg(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    name = company.get("name") or company.get("slug") or ""
    page, api = ukg_urls(company)
    if not company.get("site"):
        logger.warning(f"[ukg:{company.get('slug')}] no board id")
        return []
    inserted: list[dict] = []
    try:
        resp = await client.post(api, json=_UKG_BODY,
                                 headers={"X-Requested-With": "XMLHttpRequest",
                                          "Accept": "application/json"})
        if resp.status_code != 200:
            logger.warning(f"[ukg:{company.get('slug')}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[ukg:{company.get('slug')}] fetch error: {e}")
        return []
    items = data.get("opportunities") if isinstance(data, dict) else None
    if not isinstance(items, list):
        _log_shape("ukg", data, None)
        return []
    if items:
        _log_shape("ukg", data, items[0])
    for it in items:
        try:
            title = _strip(it.get("Title"))
            if not a._title_matches_profile(title, search_terms):
                continue
            locs = it.get("Locations") or []
            parts, country = [], ""
            for l in locs if isinstance(locs, list) else []:
                if not isinstance(l, dict):
                    continue
                addr = l.get("Address") or {}
                city = addr.get("City") or ""
                st = (addr.get("State") or {}).get("Code") or (addr.get("State") or {}).get("Name") or ""
                country = country or (addr.get("Country") or {}).get("Code") or ""
                txt = l.get("LocalizedName") or ", ".join(p for p in (city, st) if p)
                if txt and txt not in parts:
                    parts.append(txt)
            loc = "; ".join(parts[:3])
            # Railway SHAPE log, 22 Sep: the item carries JobLocationType
            # (the platform's own workplace field) — trust it first.
            jlt = str(it.get("JobLocationType") or "").lower()
            remote_flag = ("remote" if ("remote" in jlt or it.get("IsRemote") or "remote" in loc.lower())
                           else "hybrid" if "hybrid" in jlt else "")
            oid = it.get("Id") or it.get("OpportunityId")
            url = f"{page}/OpportunityDetail?opportunityId={oid}" if oid else ""
            desc = _strip(it.get("BriefDescription") or it.get("Description") or "")
            job = _row("ukg", title, name, loc, url, _epoch_or_str(it.get("PostedDate")),
                       desc, profile_id, remote_flag, country)
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[ukg:{company.get('slug')}] item skip: {e}")
    if inserted:
        logger.info(f"[ukg:{company.get('slug')}] +{len(inserted)} new jobs ({len(items)} on board)")
    return inserted


# ── Oracle Cloud Recruiting (Candidate Experience) ─────────────────────────

def oracle_urls(c: dict) -> tuple[str, str]:
    host, site = c.get("tenant") or "", c.get("site") or "CX_1"
    page = f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/requisitions"
    api = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
           f"?onlyData=true&expand=requisitionList.secondaryLocations"
           f"&finder=findReqs;siteNumber={site},limit=100,offset=0,sortBy=POSTING_DATES_DESC")
    return page, api


async def fetch_oracle(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    name = company.get("name") or company.get("slug") or ""
    host, site = company.get("tenant") or "", company.get("site") or "CX_1"
    if not host:
        return []
    inserted: list[dict] = []
    reqs: list[dict] = []
    # Was the newest 200 of the whole site, unsearched; big Oracle tenants
    # hold thousands. Search with the profile titles (finder keyword=) and
    # page each until a page has no matching title, like fetch_workday.
    from urllib.parse import quote
    queries = [t for t in (search_terms or []) if t.strip()][:a.WORKDAY_QUERIES] or [""]
    seen_ids: set = set()
    try:
        for q in queries:
            kw = f",keyword={quote(q)}" if q else ""
            for offset in range(0, 500, 100):
                api = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
                       f"?onlyData=true&expand=requisitionList.secondaryLocations"
                       f"&finder=findReqs;siteNumber={site}{kw},limit=100,offset={offset},sortBy=RELEVANCY")
                resp = await client.get(api, headers={"Accept": "application/json"})
                if resp.status_code != 200:
                    if offset == 0:
                        logger.warning(f"[oracle:{company.get('slug')}] HTTP {resp.status_code} q={q!r}")
                    break
                data = resp.json()
                items = data.get("items") if isinstance(data, dict) else None
                if not items:
                    _log_shape("oracle", data, None)
                    break
                rl = (items[0] or {}).get("requisitionList") or []
                if rl:
                    _log_shape("oracle", items[0], rl[0])
                hits = 0
                for r in rl:
                    if not isinstance(r, dict) or r.get("Id") in seen_ids:
                        continue
                    seen_ids.add(r.get("Id"))
                    reqs.append(r)
                    if a._title_matches_profile(_strip(r.get("Title")), search_terms):
                        hits += 1
                if len(rl) < 100 or hits == 0:
                    break
    except Exception as e:
        logger.warning(f"[oracle:{company.get('slug')}] fetch error: {e}")
        if not reqs:
            return []
    for r in reqs:
        try:
            title = _strip(r.get("Title"))
            if not a._title_matches_profile(title, search_terms):
                continue
            loc = _strip(r.get("PrimaryLocation") or "")
            country = str(r.get("PrimaryLocationCountry") or "")
            wp = str(r.get("WorkplaceType") or r.get("WorkplaceTypeCode") or "").lower()
            platform_field = ("remote" if "remote" in wp else "hybrid" if "hybrid" in wp
                              else "onsite" if ("on-site" in wp or "onsite" in wp or "on site" in wp) else "")
            rid = r.get("Id")
            url = f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{rid}" if rid else ""
            desc = _strip(r.get("ShortDescriptionStr") or r.get("ExternalDescriptionStr") or "")
            job = _row("oracle", title, name, loc, url, r.get("PostedDate"), desc,
                       profile_id, platform_field, country)
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[oracle:{company.get('slug')}] item skip: {e}")
    if inserted:
        logger.info(f"[oracle:{company.get('slug')}] +{len(inserted)} new jobs ({len(reqs)} on board)")
    return inserted


# ── ADP WorkforceNow career center ─────────────────────────────────────────

def adp_urls(c: dict) -> tuple[str, str]:
    host = c.get("tenant") or "workforcenow.adp.com"
    cid, cc = c.get("slug") or "", c.get("site") or "19000101_000001"
    page = f"https://{host}/mascsr/default/mdf/recruitment/recruitment.html?cid={cid}&ccId={cc}&lang=en_US"
    api = (f"https://{host}/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions"
           f"?cid={cid}&ccId={cc}&lang=en_US&locale=en_US&$top=100&$skip=0")
    return page, api


async def fetch_adp(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    name = company.get("name") or company.get("slug") or ""
    host = company.get("tenant") or "workforcenow.adp.com"
    cid, cc = company.get("slug") or "", company.get("site") or "19000101_000001"
    if not cid:
        return []
    inserted: list[dict] = []
    reqs: list[dict] = []
    try:
        for skip in (0, 100):
            api = (f"https://{host}/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions"
                   f"?cid={cid}&timeStamp={int(time.time() * 1000)}&lang=en_US&ccId={cc}&locale=en_US"
                   f"&$top=100&$skip={skip}")
            resp = await client.get(api, headers={"Accept": "application/json"})
            if resp.status_code != 200:
                if skip == 0:
                    logger.warning(f"[adp:{cid[:8]}] HTTP {resp.status_code}")
                break
            data = resp.json()
            items = data.get("jobRequisitions") if isinstance(data, dict) else None
            if not isinstance(items, list):
                _log_shape("adp", data, None)
                break
            if items:
                _log_shape("adp", data, items[0])
            reqs.extend(items)
            if len(items) < 100:
                break
    except Exception as e:
        logger.warning(f"[adp:{cid[:8]}] fetch error: {e}")
        return []
    for r in reqs:
        try:
            title = _strip(r.get("requisitionTitle") or "")
            if not a._title_matches_profile(title, search_terms):
                continue
            parts, country = [], ""
            for l in r.get("requisitionLocations") or []:
                if not isinstance(l, dict):
                    continue
                addr = l.get("address") or {}
                city = addr.get("cityName") or ""
                st = (addr.get("countrySubdivisionLevel1") or {}).get("codeValue") or ""
                country = country or addr.get("countryCode") or ""
                txt = ", ".join(p for p in (city, st) if p) or (l.get("nameCode") or {}).get("shortName") or ""
                if txt and txt not in parts:
                    parts.append(txt)
            loc = "; ".join(parts[:3])
            rid = r.get("itemID") or (r.get("customFieldGroup") or {}).get("itemID")
            url = (f"https://{host}/mascsr/default/mdf/recruitment/recruitment.html"
                   f"?cid={cid}&ccId={cc}&jobId={rid}&lang=en_US") if rid else ""
            desc = _strip(r.get("requisitionDescription") or r.get("jobDescription") or "")
            job = _row("adp", title, name, loc, url, r.get("postDate"), desc, profile_id, "", country)
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[adp:{cid[:8]}] item skip: {e}")
    if inserted:
        logger.info(f"[adp:{cid[:8]}] +{len(inserted)} new jobs ({len(reqs)} on board)")
    return inserted


# ── Rippling ATS ───────────────────────────────────────────────────────────

def rippling_urls(c: dict) -> tuple[str, str]:
    slug = c.get("slug") or ""
    return f"https://ats.rippling.com/{slug}/jobs", f"https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs"


async def fetch_rippling(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    slug = company.get("slug") or ""
    name = company.get("name") or slug
    page, api = rippling_urls(company)
    inserted: list[dict] = []
    try:
        resp = await client.get(api, headers={"Accept": "application/json"})
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[rippling:{slug}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[rippling:{slug}] fetch error: {e}")
        return []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items") or data.get("results") or data.get("jobs")
    else:
        items = None
    if not isinstance(items, list):
        _log_shape("rippling", data, None)
        return []
    if items:
        _log_shape("rippling", data, items[0])
    for it in items:
        try:
            title = _strip(it.get("name") or it.get("title"))
            if not a._title_matches_profile(title, search_terms):
                continue
            wl = it.get("workLocation") or {}
            if isinstance(wl, list):
                wl = wl[0] if wl else {}
            loc = _strip(wl.get("label") or ", ".join(p for p in (wl.get("city"), wl.get("state")) if p))
            country = str(wl.get("country") or "")
            wp = str(it.get("workplaceType") or it.get("locationType") or "").lower()
            platform_field = "remote" if "remote" in wp else "hybrid" if "hybrid" in wp else ""
            url = it.get("url") or (f"{page}/{it.get('id')}" if it.get("id") else "")
            desc = _strip(it.get("description") or "")
            job = _row("rippling", title, name, loc, url, it.get("publishedAt") or it.get("createdAt"),
                       desc, profile_id, platform_field, country)
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[rippling:{slug}] item skip: {e}")
    if inserted:
        logger.info(f"[rippling:{slug}] +{len(inserted)} new jobs ({len(items)} on board)")
    return inserted


# ── BambooHR ───────────────────────────────────────────────────────────────

def bamboohr_urls(c: dict) -> tuple[str, str]:
    slug = c.get("slug") or ""
    return f"https://{slug}.bamboohr.com/careers", f"https://{slug}.bamboohr.com/careers/list"


async def fetch_bamboohr(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    slug = company.get("slug") or ""
    name = company.get("name") or slug
    page, api = bamboohr_urls(company)
    inserted: list[dict] = []
    try:
        resp = await client.get(api, headers={"Accept": "application/json"})
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[bamboohr:{slug}] HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"[bamboohr:{slug}] fetch error: {e}")
        return []
    items = data.get("result") if isinstance(data, dict) else data
    if not isinstance(items, list):
        _log_shape("bamboohr", data, None)
        return []
    if items:
        _log_shape("bamboohr", data, items[0])
    for it in items:
        try:
            title = _strip(it.get("jobOpeningName") or it.get("title"))
            if not a._title_matches_profile(title, search_terms):
                continue
            l = it.get("location") or {}
            if isinstance(l, str):
                loc, country = l, ""
            else:
                loc = ", ".join(p for p in ((l.get("city") or ""), (l.get("state") or "")) if p)
                country = str(l.get("country") or "")
            wp = str(it.get("locationType") or it.get("workplaceType") or "").lower()
            platform_field = ("remote" if (it.get("isRemote") or "remote" in wp) else
                              "hybrid" if "hybrid" in wp else "")
            jid = it.get("id")
            url = f"{page}/{jid}" if jid else ""
            job = _row("bamboohr", title, name, loc, url,
                       it.get("datePosted") or it.get("dateOpened") or "", "", profile_id,
                       platform_field, country)
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[bamboohr:{slug}] item skip: {e}")
    if inserted:
        logger.info(f"[bamboohr:{slug}] +{len(inserted)} new jobs ({len(items)} on board)")
    return inserted


# ── Jobvite (server-rendered job list) ─────────────────────────────────────

def jobvite_urls(c: dict) -> tuple[str, str]:
    slug = c.get("slug") or ""
    return f"https://jobs.jobvite.com/{slug}", f"https://jobs.jobvite.com/{slug}"


_JV_LINK = re.compile(
    r'<a[^>]+href="(?:https?://jobs\.jobvite\.com)?/([a-z0-9][a-z0-9_-]*)/job/([A-Za-z0-9]+)"[^>]*>(.*?)</a>',
    re.I | re.S)
_JV_LOC = re.compile(r'jv-job-list-location[^>]*>(.*?)</(?:td|div|span)>', re.I | re.S)
_JV_NAME = re.compile(r'jv-job-list-name[^>]*>(.*?)</(?:td|div|span)>', re.I | re.S)


async def fetch_jobvite(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    slug = company.get("slug") or ""
    name = company.get("name") or slug
    page, _api = jobvite_urls(company)
    inserted: list[dict] = []
    try:
        resp = await client.get(page)
        if resp.status_code == 404:
            return []
        if resp.status_code != 200:
            logger.warning(f"[jobvite:{slug}] HTTP {resp.status_code}")
            return []
        html = resp.text
    except Exception as e:
        logger.warning(f"[jobvite:{slug}] fetch error: {e}")
        return []
    # One <tr> per job on the jv-job-list table; the location sits in its own
    # cell somewhere after the link, not necessarily right after it.
    # Railway snippet, 22 Sep: <li class="row"><a href="/slug/job/ID" class=
    # "flex-row"><div class="jv-job-list-name">Title</div><div class="ml-auto
    # jv-job-type">Full-Time</div><div class="ml2 jv-job-list-location">United
    # Kingdom</div></a></li> — the whole row is the anchor.
    rows: list[tuple[str, str, str, str]] = []
    for m in _JV_LINK.finditer(html):
        inner = m.group(3)
        nm = _JV_NAME.search(inner)
        lm = _JV_LOC.search(inner)
        rows.append((m.group(1), m.group(2), nm.group(1) if nm else inner, lm.group(1) if lm else ""))
    if "jobvite" not in _SHAPE_LOGGED:
        _SHAPE_LOGGED.add("jobvite")
        i = html.find("/job/")
        snip = re.sub(r"\s+", " ", html[max(0, i - 300): i + 500]) if i >= 0 else html[:400]
        logger.warning(f"[jobvite] SHAPE rows={len(rows)} bytes={len(html)} "
                       f"has_list_class={'jv-job-list' in html} snippet={snip!r}")
    seen = set()
    for _slug, jid, title_html, loc_html in rows:
        try:
            if jid in seen:
                continue
            seen.add(jid)
            title = _strip(title_html)
            if not title or not a._title_matches_profile(title, search_terms):
                continue
            loc = _strip(loc_html)
            url = f"https://jobs.jobvite.com/{slug}/job/{jid}"
            job = _row("jobvite", title, name, loc, url, "", "", profile_id, "", "")
            if job:
                await _emit(job, inserted)
        except Exception as e:
            logger.debug(f"[jobvite:{slug}] item skip: {e}")
    if inserted:
        logger.info(f"[jobvite:{slug}] +{len(inserted)} new jobs ({len(rows)} on board)")
    return inserted


# ── iCIMS (server-rendered search results) ─────────────────────────────────

def icims_urls(c: dict) -> tuple[str, str]:
    host = c.get("slug") or ""
    return f"https://{host}/jobs/search?ss=1", f"https://{host}/jobs/search?ss=1&searchRelation=keyword_all&in_iframe=1"


# Any <a> whose attributes mention iCIMS_Anchor, in any attribute order, with
# an absolute OR relative href — the Railway sweep fetched three pages per
# host (so the anchors were there) and parsed nothing with the stricter form.
_ICIMS_A = re.compile(r"<a\b([^>]*)>(.*?)</a>", re.I | re.S)
_ICIMS_HREF = re.compile(r'href="([^"]*?/jobs/(\d+)/[^"]*)"', re.I)
_ICIMS_H = re.compile(r"<h[23][^>]*>(.*?)</h[23]>", re.I | re.S)
_ICIMS_LOC = re.compile(r"Job Locations?\s*(?:</dt>|</span>|</div>)\s*<(?:dd|div|span)[^>]*>(.*?)</(?:dd|div|span)>", re.I | re.S)


async def fetch_icims(client: httpx.AsyncClient, company: dict, profile_id, search_terms: list[str]) -> list[dict]:
    a = _h()[0]
    host = company.get("slug") or ""
    name = company.get("name") or host.split(".")[0].replace("careers-", "").replace("uscareers-", "")
    inserted: list[dict] = []
    pages_html: list[str] = []
    try:
        for pr in range(0, 3):
            url = f"https://{host}/jobs/search?ss=1&searchRelation=keyword_all&in_iframe=1&pr={pr}"
            resp = await client.get(url)
            if pr == 0 and "icims-first" not in _SHAPE_LOGGED:
                _SHAPE_LOGGED.add("icims-first")
                body = re.sub(r"\s+", " ", resp.text or "")
                i = body.find("iCIMS_Anchor")
                snip = body[max(0, i - 200): i + 600] if i >= 0 else body[:600]
                logger.warning(f"[icims] FIRST host={host} status={resp.status_code} "
                               f"bytes={len(resp.text or '')} anchors={resp.text.count('iCIMS_Anchor')} "
                               f"snippet={snip!r}")
            if resp.status_code != 200:
                if pr == 0:
                    logger.warning(f"[icims:{host}] HTTP {resp.status_code}")
                break
            pages_html.append(resp.text)
            if "iCIMS_Anchor" not in resp.text:
                break
    except Exception as e:
        logger.warning(f"[icims:{host}] fetch error: {e}")
        return []
    n_rows = 0
    seen = set()
    for html in pages_html:
        for am in _ICIMS_A.finditer(html):
            attrs, inner = am.group(1), am.group(2)
            if "iCIMS_Anchor" not in attrs:
                continue
            hm_ = _ICIMS_HREF.search(attrs)
            if not hm_:
                continue
            n_rows += 1
            try:
                url, jid = hm_.group(1), hm_.group(2)
                if url.startswith("/"):
                    url = f"https://{host}{url}"
                if jid in seen:
                    continue
                seen.add(jid)
                tail = html[am.end(): am.end() + 4000]
                hm = _ICIMS_H.search(inner) or _ICIMS_H.search(tail[:2000])
                title = _strip(hm.group(1) if hm else inner)
                if not title or not a._title_matches_profile(title, search_terms):
                    continue
                lm = _ICIMS_LOC.search(inner) or _ICIMS_LOC.search(tail)
                loc = _strip(lm.group(1)) if lm else ""
                job = _row("icims", title, name,
                           loc.replace("US-", "").replace("-", ", ") if loc else "",
                           url.split("?")[0], "", "", profile_id,
                           "remote" if "remote" in loc.lower() else "",
                           "US" if loc.upper().startswith("US") else "")
                if job:
                    await _emit(job, inserted)
            except Exception as e:
                logger.debug(f"[icims:{host}] item skip: {e}")
    if "icims" not in _SHAPE_LOGGED:
        _SHAPE_LOGGED.add("icims")
        logger.warning(f"[icims] SHAPE pages={len(pages_html)} rows={n_rows} "
                       f"bytes={sum(len(h) for h in pages_html)}")
    if inserted:
        logger.info(f"[icims:{host}] +{len(inserted)} new jobs ({n_rows} rows)")
    return inserted


FETCHERS = {
    "ukg": fetch_ukg,
    "oracle": fetch_oracle,
    "adp": fetch_adp,
    "rippling": fetch_rippling,
    "bamboohr": fetch_bamboohr,
    "jobvite": fetch_jobvite,
    "icims": fetch_icims,
}

URLS = {
    "ukg": ukg_urls,
    "oracle": oracle_urls,
    "adp": adp_urls,
    "rippling": rippling_urls,
    "bamboohr": bamboohr_urls,
    "jobvite": jobvite_urls,
    "icims": icims_urls,
}

PLATFORMS = tuple(FETCHERS.keys())

# Slug-only platforms whose slug can be guessed from a company name
NAME_FUZZ_PLATFORMS = ("rippling", "bamboohr", "jobvite")


async def probe(company: dict, search_terms: Optional[list[str]] = None) -> dict:
    """Run one fetcher with inserts OFF and report what it parsed. This is the
    Railway-side verification for every platform in this module."""
    import ats_scraper as a
    ats = (company.get("ats") or "").lower()
    fn = FETCHERS.get(ats)
    if not fn:
        return {"ok": False, "error": f"unknown ats {ats!r}", "platforms": list(PLATFORMS)}
    token = DRY_RUN.set(True)
    try:
        async with httpx.AsyncClient(timeout=a.HTTP_TIMEOUT, headers=a.HTTP_HEADERS,
                                     follow_redirects=True) as client:
            rows = await fn(client, company, None, search_terms or [])
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        DRY_RUN.reset(token)
    page, api = URLS[ats](company)
    return {
        "ok": True, "ats": ats, "company": company, "page": page, "api": api,
        "parsed": len(rows),
        "sample": [{k: r.get(k) for k in ("title", "location", "work_type", "posted_at", "source_url")}
                   for r in rows[:8]],
    }


async def verify(client: httpx.AsyncClient, cand: dict) -> bool:
    """A board exists if its fetcher parses at least one row with no title filter."""
    fn = FETCHERS.get((cand.get("ats") or "").lower())
    if not fn:
        return False
    token = DRY_RUN.set(True)
    try:
        rows = await fn(client, cand, None, [])
        return len(rows) > 0
    except Exception:
        return False
    finally:
        DRY_RUN.reset(token)
