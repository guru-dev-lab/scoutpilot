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
    """Use Claude AI to score how relevant a job is to the target role (0-100).
    AI is the SOLE scorer — no fuzzy gates. Understands role families natively."""

    # Quick exclude check — blocked keywords = instant reject
    desc_lower = job_description.lower()
    title_lower = job_title.lower()
    for kw in excluded_keywords:
        if kw.lower() in desc_lower or kw.lower() in title_lower:
            return 5

    # Fall back to fuzzy only if no API key
    if not settings.anthropic_api_key:
        return score_relevance_fuzzy(job_title, job_description, target_title, expanded_titles, keywords)

    try:
        import anthropic

        kw_str = ", ".join(keywords[:10]) if keywords else "none"
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{
                "role": "user",
                "content": f"""Does this job match the "{target_title}" profile? Tools/skills they want: {kw_str}

Job: {job_title}
Description: {job_description[:600]}

Score 0-100. Related roles = HIGH score:
Data Analyst ≈ BI Analyst ≈ BI Developer ≈ Analytics Engineer ≈ Reporting Analyst ≈ Business Intelligence
Civil Engineer ≈ Structural Engineer ≈ Infrastructure Engineer
Security Engineer ≈ SOC Analyst ≈ InfoSec Engineer

90-100 = same role family or exact match
75-89 = closely related, overlapping skills
40-74 = some overlap, different path
0-39 = not relevant

Return ONLY the number.""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\d+', text)
        if match:
            return min(100, max(0, int(match.group())))
    except Exception as e:
        logger.error(f"[AI] Relevance scoring failed: {e}")

    # Fallback to fuzzy if AI call fails
    return score_relevance_fuzzy(job_title, job_description, target_title, expanded_titles, keywords)


def score_relevance_fuzzy(
    job_title: str,
    job_description: str,
    target_title: str,
    expanded_titles: list[str],
    keywords: list[str] = [],
) -> int:
    """
    Fast fuzzy matching score — checks title similarity then keyword boosts.

    Scoring tiers:
      - token_sort_ratio:  full weight (good for reordered words)
      - partial_ratio:     0.75× weight (penalize partial-only matches)
      - keyword boost:     only if base title score >= 40 (prevents generic
        keywords like "SQL" from inflating completely unrelated jobs)
    """
    best_score = 0
    job_lower = job_title.lower()

    # --- Title matching ---
    all_targets = [target_title] + expanded_titles
    for target in all_targets:
        target_lower = target.lower()

        # Exact substring — if "Data Analyst" appears verbatim in the job title,
        # that's a strong signal even if the title has extra words like salary/location
        if target_lower in job_lower:
            # Score based on how much of the job title the target covers
            coverage = len(target_lower) / max(len(job_lower), 1)
            # "Data Analyst" in "Data Analyst" = 100, in "Senior Data Analyst" = 93
            # "Data Analyst" in "Data Analyst (Annotation) | $30/hr Remote" = 90
            # "Manager" in "Millwork Installation Project Manager" = only ~80
            substr_score = int(85 + coverage * 15)
            best_score = max(best_score, min(100, substr_score))

        # Full token sort — best for rearranged words ("Sr Data Analyst" ≈ "Data Analyst Sr")
        s = fuzz.token_sort_ratio(job_lower, target_lower)
        best_score = max(best_score, s)

        # Partial ratio — catches near-matches but weighted down
        s2 = fuzz.partial_ratio(job_lower, target_lower)
        best_score = max(best_score, int(s2 * 0.75))

    base_title_score = best_score

    # --- Keyword boost (only if title already has a reasonable match) ---
    # This prevents generic keywords ("SQL", "Excel", "CRM") from inflating
    # scores for completely unrelated jobs like "Customer Success Manager"
    if base_title_score >= 40:
        title_lower = job_lower
        desc_lower = job_description.lower() if job_description else ""
        keyword_boost = 0
        for kw in keywords:
            kw_lower = kw.lower().strip()
            if not kw_lower:
                continue
            if kw_lower in title_lower:
                keyword_boost += 12
            elif kw_lower in desc_lower:
                keyword_boost += 5
        # Cap total keyword boost at 20 points
        best_score = min(100, best_score + min(keyword_boost, 20))

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

# ──────────────────────────────────────────────
# AI-Powered Dedup (catch duplicates fuzzy matching misses)
# ──────────────────────────────────────────────

async def ai_is_duplicate(
    job_a_title: str, job_a_company: str,
    job_b_title: str, job_b_company: str,
    fuzzy_score: int,
) -> bool:
    """Use Claude to determine if two jobs are really the same posting.
    Only called when fuzzy score is borderline (70-87)."""
    if not settings.anthropic_api_key:
        return False  # Can't check without AI — let it through

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            messages=[{
                "role": "user",
                "content": f"""Are these two job postings the same position? Answer only YES or NO.

Job A: "{job_a_title}" at "{job_a_company}"
Job B: "{job_b_title}" at "{job_b_company}"

Fuzzy similarity: {fuzzy_score}%
Consider: abbreviation differences (Sr vs Senior), minor wording changes, same company = likely same job.""",
            }],
        )
        text = response.content[0].text.strip().upper()
        is_dup = text.startswith("YES")
        if is_dup:
            logger.info(f"[AI Dedup] Confirmed duplicate: '{job_a_title}' @ {job_a_company} ≈ '{job_b_title}' @ {job_b_company}")
        return is_dup
    except Exception as e:
        logger.error(f"[AI Dedup] Failed: {e}")
        return False


# ──────────────────────────────────────────────
# AI-Powered Direct Job Link Detection
# ──────────────────────────────────────────────

async def extract_direct_link_ai(
    description: str,
    company_name: str,
    company_domain: str = "",
    source_url: str = "",
) -> str:
    """Use Claude to extract the actual company career page URL from a job description.
    Returns the URL if found, empty string otherwise."""
    if not settings.anthropic_api_key:
        return ""

    # Only try if description has URLs in it
    url_pattern = re.compile(r'https?://[^\s<>"\')\]]+')
    urls_in_desc = url_pattern.findall(description[:3000])
    if not urls_in_desc:
        return ""

    # Filter out obviously non-career URLs
    career_hints = []
    non_career = {"linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
                  "google.com", "facebook.com", "twitter.com", "instagram.com",
                  "youtube.com", "github.com", "bit.ly", "tinyurl.com"}
    for url in urls_in_desc:
        domain = url.split("/")[2].lower() if len(url.split("/")) > 2 else ""
        if not any(nc in domain for nc in non_career):
            career_hints.append(url)

    if not career_hints:
        return ""

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": f"""From these URLs found in a job posting for "{company_name}", identify the direct application URL — the link that goes to the company's own career page or job application form (NOT a job board like LinkedIn/Indeed).

URLs found: {career_hints[:10]}
Company domain: {company_domain or 'unknown'}
Source URL: {source_url}

Rules:
- Return ONLY the best direct application URL, nothing else
- Look for: company career pages, workday/greenhouse/lever/ashby/smartrecruiters links
- If no direct application URL exists, return "NONE"
- Do NOT return job board URLs""",
            }],
        )
        text = response.content[0].text.strip()
        if text and text.upper() != "NONE" and text.startswith("http"):
            # Clean up — remove trailing punctuation
            clean_url = re.sub(r'[.,;:!?\s]+$', '', text)
            logger.info(f"[AI Direct Link] Found: {clean_url} for {company_name}")
            return clean_url
    except Exception as e:
        logger.error(f"[AI Direct Link] Failed: {e}")

    return ""


# ──────────────────────────────────────────────
# AI-Powered Skill Extraction
# ──────────────────────────────────────────────

# Known skills from our SKILL_DB (keep in sync with skills.py)
_KNOWN_SKILLS = {
    "Python", "JavaScript", "TypeScript", "Java", "C#", "C++", "Go", "Rust", "Ruby", "PHP",
    "Swift", "Kotlin", "Scala", "R", "SQL", "Power BI", "Tableau", "Excel", "Snowflake",
    "BigQuery", "Databricks", "Spark", "Hadoop", "Kafka", "Airflow", "dbt", "Looker",
    "Redshift", "PostgreSQL", "MongoDB", "Redis", "Elasticsearch", "MySQL", "Oracle DB", "ETL",
    "AWS", "Azure", "GCP", "Salesforce", "Docker", "Kubernetes", "Terraform", "Jenkins",
    "GitHub Actions", "React", "Angular", "Vue.js", "Node.js", "Django", "Flask", "Spring",
    "FastAPI", "GraphQL", "REST API", "TensorFlow", "PyTorch", "Scikit-learn", "Pandas",
    "NumPy", "OpenAI", "LangChain", "RAG", "Machine Learning", "Deep Learning", "NLP",
    "Computer Vision", "LLM", "GenAI", "Prompt Engineering", "Data Warehouse", "Data Lake",
    "Data Pipeline", "CI/CD", "Agile", "Scrum", "JIRA", "Confluence", "Git", "Linux",
    "Figma", "Sketch", "Adobe XD", "Photoshop", "Illustrator", "Sass", "Tailwind",
    "Next.js", "Express.js", "Spring Boot", "Microservices", "CCPA", "GDPR", "SOX",
    "HIPAA", "PCI", "ServiceNow", "SAP", "Workday", "Amplitude", "Mixpanel", "Segment",
}


async def extract_skills_ai(title: str, description: str) -> str:
    """Use Claude to extract skills/technologies from a job posting.
    Returns comma-separated skill names matching our known skill set.
    Only called when regex extraction found nothing."""
    if not settings.anthropic_api_key:
        return ""

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{
                "role": "user",
                "content": f"""Extract technical skills and tools mentioned in this job posting.

Title: {title}
Description (first 2000 chars): {description[:2000]}

Return ONLY a JSON array of skill/tool names. Pick from these known skills when possible:
Python, JavaScript, TypeScript, Java, SQL, Power BI, Tableau, Excel, AWS, Azure, GCP,
Docker, Kubernetes, React, Node.js, TensorFlow, PyTorch, Machine Learning, Data Warehouse,
ETL, Airflow, Spark, Kafka, Agile, Scrum, JIRA, Git, CI/CD, REST API, GraphQL,
Salesforce, SAP, ServiceNow, Figma, LLM, GenAI, Prompt Engineering, etc.

If you find skills not in this list but clearly technical, include them too.
Return [] if no technical skills are mentioned. No explanation.""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            skills = json.loads(match.group())
            # Filter to our known skill set and clean up
            valid = []
            for s in skills:
                s = s.strip()
                if s in _KNOWN_SKILLS:
                    valid.append(s)
                else:
                    # Fuzzy match against known skills
                    for known in _KNOWN_SKILLS:
                        if s.lower() == known.lower():
                            valid.append(known)
                            break
            if valid:
                result = ",".join(list(dict.fromkeys(valid)))  # dedup preserving order
                logger.info(f"[AI Skills] Found {len(valid)} skills for '{title}': {result}")
                return result
    except Exception as e:
        logger.error(f"[AI Skills] Failed: {e}")

    return ""


async def score_jobs(jobs: list[dict], profile: dict) -> list[dict]:
    """Score a batch of jobs for relevance and trust."""
    target_title = profile["title"]
    expanded = profile.get("expanded_titles", [])
    keywords = profile.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    excluded = profile.get("excluded_keywords", [])
    if isinstance(excluded, str):
        excluded = [k.strip() for k in excluded.split(",") if k.strip()]

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


# ============================================================================
# AI DATA QUALITY VERIFICATION
# ============================================================================

async def verify_work_type_ai(
    title: str,
    description: str,
    location: str,
    current_type: str,
) -> str:
    """Use Claude to verify the actual work arrangement from the job description.
    Returns: 'remote', 'hybrid', or 'onsite'."""
    if not settings.anthropic_api_key or not description:
        return current_type
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            messages=[{
                "role": "user",
                "content": f"""What is the ACTUAL work arrangement for this job? Answer REMOTE, HYBRID, or ONSITE only.

Title: {title}
Location: {location}
Description (first 1500 chars): {description[:1500]}

Rules:
- REMOTE = 100% work from home, no office requirement
- HYBRID = mix of remote and in-office (e.g. "3 days in office", "flexible")
- ONSITE = must be in office full time
- If location says a specific city with no mention of remote, it's ONSITE
- "Remote" in title but description says "must be in office 2 days" = HYBRID
- Look for clues like "on-site", "in-person", "office-based", "relocation"

Answer with ONE word only: REMOTE, HYBRID, or ONSITE""",
            }],
        )
        text = response.content[0].text.strip().upper()
        if "REMOTE" in text and "HYBRID" not in text:
            result = "remote"
        elif "HYBRID" in text:
            result = "hybrid"
        elif "ONSITE" in text or "ON-SITE" in text or "ON SITE" in text:
            result = "onsite"
        else:
            return current_type

        if result != current_type:
            logger.info(f"[AI Verify] Work type corrected: '{title}' {current_type} → {result}")
        return result
    except Exception as e:
        logger.error(f"[AI Verify] Work type check failed: {e}")
        return current_type


async def verify_direct_apply_ai(
    source_url: str,
    direct_apply_url: str,
    source: str,
) -> tuple[bool, str]:
    """Verify if a job link is actually a direct company application, not Easy Apply or Apply on Indeed.
    Returns: (is_direct, cleaned_url)"""
    # Quick heuristic checks — no AI needed for obvious cases
    url_lower = (direct_apply_url or source_url or "").lower()

    # These are NEVER direct apply
    easy_apply_indicators = [
        "linkedin.com/jobs/",
        "linkedin.com/job/",
        "indeed.com/viewjob",
        "indeed.com/jobs",
        "indeed.com/rc/",
        "glassdoor.com/job-listing",
        "glassdoor.com/partner",
        "ziprecruiter.com/",
        "google.com/search",
        "monster.com/",
        "careerbuilder.com/",
        "dice.com/",
        "snagajob.com/",
    ]

    for indicator in easy_apply_indicators:
        if indicator in url_lower:
            # This is a job board link, NOT direct apply
            return (False, "")

    # These ARE direct apply (company career portals / ATS systems)
    direct_apply_domains = [
        "greenhouse.io", "lever.co", "ashbylabs.com", "ashbyhq.com",
        "workday.com", "myworkdayjobs.com", "smartrecruiters.com",
        "icims.com", "ultipro.com", "breezy.hr", "bamboohr.com",
        "jobvite.com", "jazz.co", "recruitee.com", "workable.com",
        "applytojob.com", "careers-page.com", "successfactors.com",
        "taleo.net", "oracle.com/careers", "phenom.com",
    ]

    for domain in direct_apply_domains:
        if domain in url_lower:
            return (True, direct_apply_url or source_url)

    # If we have a direct_apply_url that's a company domain, keep it
    if direct_apply_url and not any(ind in direct_apply_url.lower() for ind in easy_apply_indicators):
        return (True, direct_apply_url)

    return (False, "")


async def verify_job_quality_ai(
    title: str,
    description: str,
    location: str,
    current_work_type: str,
    source_url: str,
    direct_apply_url: str,
    source: str,
) -> dict:
    """Run all data quality checks on a job. Returns corrections dict."""
    corrections = {}

    # 1. Verify work type
    verified_type = await verify_work_type_ai(title, description, location, current_work_type)
    if verified_type != current_work_type:
        corrections["work_type"] = verified_type
        corrections["is_remote"] = verified_type == "remote"

    # 2. Verify direct apply
    is_direct, clean_url = await verify_direct_apply_ai(source_url, direct_apply_url, source)
    if not is_direct and (direct_apply_url or source_url):
        # Was marked direct but shouldn't be
        corrections["is_direct_apply"] = False
        corrections["direct_apply_url"] = ""
    elif is_direct and clean_url:
        corrections["is_direct_apply"] = True
        corrections["direct_apply_url"] = clean_url

    return corrections
