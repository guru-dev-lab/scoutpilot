"""Always-on AI company discovery.

Each round: Claude Haiku generates real company names for a rotating
industry×region context, then we verify each live against the ATS APIs
(Greenhouse / Ashby / Lever / SmartRecruiters) and keep the ones with an
active board. Verified companies are persisted to the DB (discovered_companies)
and merged into the scrape roster automatically — the roster grows by itself.
"""
import asyncio
import logging
import re
from typing import Optional

import httpx

from config import settings

logger = logging.getLogger("scoutpilot.discovery")

# Rotating discovery contexts (industry × region). The bot walks this list so it
# keeps probing fresh niches before repeating. Extend freely.
_INDUSTRIES = [
    "B2B SaaS", "fintech", "healthtech", "biotech", "developer tools",
    "artificial intelligence / ML", "cybersecurity", "e-commerce", "logistics & supply chain",
    "climate / clean energy", "robotics", "gaming studios", "edtech", "insurtech",
    "proptech / real estate tech", "marketing & adtech", "data & analytics",
    "hardware / IoT", "crypto / web3", "consumer apps", "HR tech", "legal tech",
    "manufacturing", "aerospace & defense", "medical devices", "telecom",
    "professional services", "staffing & recruiting agencies", "digital agencies",
    "consulting firms", "banks & credit unions", "retail & CPG brands",
]
_REGIONS = [
    "the United States", "the United Kingdom", "Canada", "Germany", "France",
    "the Netherlands", "the Nordics", "Spain", "Australia & New Zealand",
    "India", "Singapore & SE Asia", "Ireland", "Israel", "the UAE / Middle East",
    "Brazil & Latin America", "Poland & Eastern Europe", "Japan", "Africa",
]
_CONTEXTS = [f"{ind} companies in {reg}" for reg in _REGIONS for ind in _INDUSTRIES]
_ctx_idx = 0


def _next_contexts(n: int) -> list[str]:
    global _ctx_idx
    out = []
    for _ in range(n):
        out.append(_CONTEXTS[_ctx_idx % len(_CONTEXTS)])
        _ctx_idx += 1
    return out


async def generate_candidates_ai(context: str, count: int = 35) -> list[str]:
    """Ask Haiku for real, lesser-known companies in a context. Names only."""
    if not settings.anthropic_api_key:
        return []
    try:
        import anthropic
        from ai_engine import note_ai_call
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        note_ai_call("other")
        resp = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=900,
            messages=[{
                "role": "user",
                "content": (
                    f"List {count} real, currently-operating {context}.\n"
                    "Prefer lesser-known mid-market companies and startups — NOT the "
                    "famous giants everyone knows. They should be companies likely to "
                    "post jobs on a modern applicant-tracking system.\n"
                    "Return ONLY the company names, one per line. No numbering, no notes."
                ),
            }],
        )
        text = resp.content[0].text
        names = []
        for line in text.splitlines():
            nm = line.strip().lstrip("-•*0123456789. \t").strip()
            if 2 <= len(nm) <= 60:
                names.append(nm)
        return names[:count]
    except Exception as e:
        logger.error(f"[Discovery] AI candidate gen failed: {e}")
        return []


def _slug_variants(name: str) -> list[str]:
    base = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    if not base:
        return []
    stop = {"the", "inc", "llc", "ltd", "co", "corp", "corporation", "company",
            "group", "holdings", "technologies", "technology", "labs", "software",
            "solutions", "systems", "global", "international"}
    core = [w for w in base if w not in stop] or base
    variants = {
        "".join(base), "-".join(base),
        "".join(core), "-".join(core),
        base[0],
    }
    return [v for v in variants if 2 <= len(v) <= 40]


_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


async def _check_greenhouse(client, slug) -> int:
    try:
        r = await client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
        if r.status_code == 200:
            n = len(r.json().get("jobs", []) or [])
            return n
    except Exception:
        pass
    return 0


async def _check_lever(client, slug) -> int:
    try:
        r = await client.get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if r.status_code == 200:
            d = r.json()
            if isinstance(d, list):
                return len(d)
    except Exception:
        pass
    return 0


async def _check_ashby(client, slug) -> int:
    try:
        body = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": slug},
            "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id } } }",
        }
        r = await client.post(
            "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams",
            json=body,
        )
        if r.status_code == 200:
            jb = (r.json().get("data") or {}).get("jobBoard")
            if jb:
                return len(jb.get("jobPostings", []) or [])
    except Exception:
        pass
    return 0


async def _check_smartrecruiters(client, slug) -> int:
    try:
        r = await client.get(
            f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=10"
        )
        if r.status_code == 200:
            return int(r.json().get("totalFound", 0) or 0)
    except Exception:
        pass
    return 0


_CHECKS = [
    ("greenhouse", _check_greenhouse),
    ("ashby", _check_ashby),
    ("lever", _check_lever),
    ("smartrecruiters", _check_smartrecruiters),
]


async def _verify_name(client, sem, name, existing) -> Optional[dict]:
    for slug in _slug_variants(name):
        for ats, fn in _CHECKS:
            if (slug.lower(), ats) in existing:
                continue
            async with sem:
                n = await fn(client, slug)
            if n > 0:
                return {"name": name, "slug": slug, "ats": ats, "jobs_seen": n}
    return None


async def run_discovery_round(existing_keys: set, contexts_per_round: int = 2,
                              per_context: int = 35) -> list[dict]:
    """One discovery pass. Returns NEW verified companies (also mutates
    existing_keys so we don't re-check them). Never raises."""
    if not settings.anthropic_api_key:
        return []
    try:
        contexts = _next_contexts(contexts_per_round)
        names = []
        for ctx in contexts:
            names += await generate_candidates_ai(ctx, per_context)
        # de-dup candidate names
        seen, uniq = set(), []
        for nm in names:
            k = nm.lower()
            if k not in seen:
                seen.add(k)
                uniq.append(nm)
        if not uniq:
            return []

        sem = asyncio.Semaphore(15)
        found = []
        async with httpx.AsyncClient(timeout=12, headers=_UA, follow_redirects=True) as client:
            results = await asyncio.gather(
                *[_verify_name(client, sem, nm, existing_keys) for nm in uniq],
                return_exceptions=True,
            )
        for r in results:
            if isinstance(r, dict):
                key = (r["slug"].lower(), r["ats"])
                if key not in existing_keys:
                    existing_keys.add(key)
                    found.append(r)
        logger.info(f"[Discovery] contexts={contexts} tested={len(uniq)} found={len(found)}")
        return found
    except Exception as e:
        logger.error(f"[Discovery] round failed: {e}")
        return []
