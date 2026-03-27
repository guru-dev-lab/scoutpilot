"""
AI scoring engine — title expansion, relevance scoring, and fake job detection.
Works with or without an API key (falls back to heuristics).
"""
import json
import logging
import re
from typing import Optional

from rapidfuzz import fuzz

from config import settings

logger = logging.getLogger("scoutpilot.ai")

# ──────────────────────────────────────────────
# Title Expansion (with or without AI)
# ──────────────────────────────────────────────

async def expand_title_ai(title: str) -> list[str]:
    """Use Claude to generate all possible title variants for a role."""
    if not settings.anthropic_api_key:
        return expand_title_heuristic(title)

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": f"""Given the job title "{title}", generate ALL possible alternative titles that describe
the same or very similar role. Include:
- Abbreviations (e.g., PM for Product Manager)
- Seniority variations (Senior, Lead, Head of, Principal, Staff, Director of)
- Alternate naming conventions across companies
- Related titles that would have 80%+ overlap in responsibilities

Return ONLY a JSON array of strings. No explanation. Example:
["Product Manager", "PM", "Senior Product Manager", "Product Lead"]

Title to expand: "{title}"
""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            titles = json.loads(match.group())
            return [t.strip() for t in titles if t.strip()]
    except Exception as e:
        logger.error(f"[AI] Title expansion failed: {e}")

    return expand_title_heuristic(title)


def expand_title_heuristic(title: str) -> list[str]:
    """Fallback title expansion without AI."""
    variants = [title]
    lower = title.lower()

    abbrevs = {
        "product manager": ["PM", "Product Lead", "Product Owner"],
        "software engineer": ["SWE", "Software Developer", "Developer", "Programmer"],
        "data scientist": ["DS", "ML Engineer", "Machine Learning Engineer"],
        "data engineer": ["DE", "Data Platform Engineer", "Analytics Engineer"],
        "project manager": ["Program Manager", "TPM", "Technical Program Manager"],
        "ux designer": ["UI Designer", "UX/UI Designer", "Product Designer", "Interaction Designer"],
        "devops engineer": ["SRE", "Site Reliability Engineer", "Platform Engineer", "Infrastructure Engineer"],
        "frontend engineer": ["Frontend Developer", "UI Engineer", "React Developer", "Web Developer"],
        "backend engineer": ["Backend Developer", "Server Engineer", "API Developer"],
        "full stack engineer": ["Full Stack Developer", "Fullstack Engineer", "Web Developer"],
        "marketing manager": ["Growth Manager", "Digital Marketing Manager", "Marketing Lead"],
        "sales representative": ["Account Executive", "AE", "Sales Associate", "BDR", "SDR"],
        "business analyst": ["BA", "Business Intelligence Analyst", "BI Analyst"],
    }

    for key, alts in abbrevs.items():
        if key in lower:
            variants.extend(alts)

    base = title
    for prefix in ["Senior ", "Sr. ", "Lead ", "Staff ", "Principal ", "Junior ", "Jr. "]:
        if title.startswith(prefix):
            base = title[len(prefix):]
            break

    if base != title:
        variants.append(base)

    for prefix in ["Senior", "Lead", "Staff", "Principal", "Head of"]:
        v = f"{prefix} {base}"
        if v != title:
            variants.append(v)

    return list(set(variants))


# ──────────────────────────────────────────────
# Relevance Scoring
# ──────────────────────────────────────────────

async def score_relevance_ai(
    job_title: str,
    job_description: str,
    target_title: str,
    expanded_titles: list[str],
    keywords: list[str] = [],
    excluded_keywords: list[str] = [],
) -> int:
    """Use Claude to score how relevant a job is to the target role (0-100)."""

    desc_lower = job_description.lower()
    for kw in excluded_keywords:
        if kw.lower() in desc_lower or kw.lower() in job_title.lower():
            return 5

    fuzzy_score = score_relevance_fuzzy(job_title, target_title, expanded_titles, keywords)

    if fuzzy_score > 85 or fuzzy_score < 20:
        return fuzzy_score

    if not settings.anthropic_api_key:
        return fuzzy_score

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=50,
            messages=[{
                "role": "user",
                "content": f"""Score how relevant this job is to someone looking for a "{target_title}" role.

Job Title: {job_title}
Job Description (first 1000 chars): {job_description[:1000]}

Score 0-100 where:
- 90-100: Perfect match, same role
- 70-89: Very similar role, transferable
- 40-69: Some overlap but different focus
- 0-39: Not relevant

Return ONLY the number. Nothing else.""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\d+', text)
        if match:
            return min(100, max(0, int(match.group())))
    except Exception as e:
        logger.error(f"[AI] Relevance scoring failed: {e}")

    return fuzzy_score


def score_relevance_fuzzy(
    job_title: str,
    target_title: str,
    expanded_titles: list[str],
    keywords: list[str] = [],
) -> int:
    """Fast fuzzy matching score without AI."""
    best_score = 0

    all_targets = [target_title] + expanded_titles
    for target in all_targets:
        s = fuzz.token_sort_ratio(job_title.lower(), target.lower())
        best_score = max(best_score, s)

        s2 = fuzz.partial_ratio(job_title.lower(), target.lower())
        best_score = max(best_score, int(s2 * 0.9))

    title_lower = job_title.lower()
    for kw in keywords:
        if kw.lower() in title_lower:
            best_score = min(100, best_score + 10)

    return min(100, best_score)


# ──────────────────────────────────────────────
# Fake Job Detection
# ──────────────────────────────────────────────

async def score_trust_ai(
    title: str,
    company: str,
    description: str,
    salary_min: int = 0,
    salary_max: int = 0,
    company_domain: str = "",
    source: str = "",
) -> int:
    """Score how trustworthy/legitimate a job posting is (0-100)."""

    trust = score_trust_heuristic(title, company, description, salary_min, salary_max, company_domain, source)

    if 30 <= trust <= 70 and settings.anthropic_api_key:
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=50,
                messages=[{
                    "role": "user",
                    "content": f"""Is this job posting legitimate or likely fake/spam? Score 0-100 (100 = definitely real).

Title: {title}
Company: {company}
Company Domain: {company_domain or 'none'}
Description (first 500 chars): {description[:500]}
Salary: {salary_min}-{salary_max if salary_max else 'not listed'}

Red flags to check: vague description, no real company, unrealistic salary, MLM/scam language, "urgently hiring" spam.
Return ONLY the number.""",
                }],
            )
            text = response.content[0].text.strip()
            match = re.search(r'\d+', text)
            if match:
                ai_trust = min(100, max(0, int(match.group())))
                trust = int(0.4 * trust + 0.6 * ai_trust)
        except Exception as e:
            logger.error(f"[AI] Trust scoring failed: {e}")

    return trust


def score_trust_heuristic(
    title: str,
    company: str,
    description: str,
    salary_min: int = 0,
    salary_max: int = 0,
    company_domain: str = "",
    source: str = "",
) -> int:
    """Fast trust scoring without AI."""
    score = 70

    desc_lower = description.lower()
    title_lower = title.lower()

    if company_domain: score += 10
    if len(description) > 500: score += 5
    if source in ("google_jobs", "linkedin"): score += 5

    spam_phrases = [
        "work from home", "no experience needed", "unlimited earning",
        "be your own boss", "earn up to", "weekly pay", "immediate start",
        "urgently hiring", "make money", "commission only",
        "mlm", "network marketing", "independent contractor opportunity",
    ]
    for phrase in spam_phrases:
        if phrase in desc_lower: score -= 15

    if not company or company.lower() in ("confidential", "company", "hiring", "staffing"): score -= 20
    if salary_max > 500000: score -= 20
    if salary_min > 0 and salary_max > 0 and salary_max > salary_min * 5: score -= 15
    if len(description) < 100: score -= 15
    if title == title.upper() and len(title) > 10: score -= 10
    if description.count("!") > 5: score -= 10
    if re.search(r'@gmail\.com|@yahoo\.com|@hotmail\.com', desc_lower): score -= 20

    return max(0, min(100, score))


# ──────────────────────────────────────────────
# Score new jobs in batch
# ──────────────────────────────────────────────

async def score_jobs(jobs: list[dict], profile: dict) -> list[dict]:
    """Score a batch of jobs for relevance and trust."""
    target_title = profile["title"]
    expanded = profile.get("expanded_titles", [])
    keywords = profile.get("keywords", [])
    excluded = profile.get("excluded_keywords", [])

    scored = []
    for job in jobs:
        relevance = await score_relevance_ai(
            job["title"], job.get("description", ""),
            target_title, expanded, keywords, excluded,
        )
        trust = await score_trust_ai(
            job["title"], job.get("company_name", ""),
            job.get("description", ""), job.get("salary_min", 0),
            job.get("salary_max", 0), job.get("company_domain", ""),
            job.get("source", ""),
        )
        job["relevance_score"] = relevance
        job["trust_score"] = trust
        scored.append(job)

    return scored
