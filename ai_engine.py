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

# Generic role words that are NEVER acceptable as standalone variants —
# they match far too broadly under partial/substring scoring.
_GENERIC_ROLE_BLOCKLIST = {
    "developer", "engineer", "programmer", "designer", "manager", "analyst",
    "specialist", "coordinator", "consultant", "associate", "assistant",
    "director", "lead", "head", "officer", "representative", "executive",
    "administrator", "technician", "architect", "operator", "advisor",
    "generalist", "intern", "trainee", "writer", "editor", "producer",
    "strategist", "planner", "scientist", "researcher", "agent",
}

# Known safe short abbreviations that we DO allow through the filter.
_ALLOWED_ABBREVS = {
    "pm", "tpm", "bi", "ba", "qa", "swe", "sde", "sre", "de", "ds", "ml",
    "ae", "bdr", "sdr", "cx", "ux", "ui", "ceo", "cto", "cfo", "coo", "cio",
    "csm", "pmm", "gm",
}


def _sanitize_expansions(title: str, expansions: list[str]) -> list[str]:
    """Drop variants that would explode fuzzy scoring — single generic words,
    ultra-short tokens, or anything in the generic-role blocklist.
    This prevents "Developer" or "Engineer" from matching every SWE job
    when the target is "Data Analyst"."""
    title_lower = title.lower().strip()
    out: list[str] = []
    seen: set[str] = set()
    for v in expansions:
        if not isinstance(v, str):
            continue
        vs = v.strip()
        if not vs or vs.lower() == title_lower:
            continue
        vl = vs.lower()
        if vl in seen:
            continue
        # Block single-word generic role names (Developer, Engineer, Manager…)
        word_count = len(vl.split())
        if word_count == 1:
            if vl in _GENERIC_ROLE_BLOCKLIST:
                continue
            # Allow known abbrevs (BI, PM, SWE…) otherwise require length ≥ 8
            if vl not in _ALLOWED_ABBREVS and len(vl) < 8:
                continue
        # Block ultra-short multi-word variants
        if len(vl) < 4:
            continue
        seen.add(vl)
        out.append(vs)
    return out


async def expand_title_ai(title: str) -> list[str]:
    """Use Claude to generate all possible title variants for a role."""
    if not settings.anthropic_api_key:
        return _sanitize_expansions(title, expand_title_heuristic(title))

    try:
        import anthropic

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": f"""List job title variants that are ESSENTIALLY THE SAME ROLE as "{title}". Be STRICT — someone searching for "{title}" should see every variant as an obviously-equivalent role, not just "related."

HARD RULES (breaking any of these = failure):
- NEVER include generic single-word roles: "Developer", "Engineer", "Programmer", "Designer", "Manager", "Analyst", "Specialist", "Coordinator", "Consultant", "Director". These match everything and poison the search.
- NEVER include a different role family. Examples of DIFFERENT families that must NOT be mixed:
    * Data/Analytics (Data Analyst, BI Analyst, Reporting Analyst) ≠ Engineering (Software Engineer, Web Developer, Full Stack)
    * Data/Analytics ≠ Marketing/Sales/Customer Success
    * Data Analyst ≠ Data Engineer (engineer builds pipelines; analyst queries them)
    * DevOps/SRE ≠ Software Engineer ≠ Security Engineer
    * UX Designer ≠ Frontend Developer
- NEVER include broader parent roles ("Analyst" for "Data Analyst", "Engineer" for "Backend Engineer")
- NEVER include tangential roles ("Business Analyst" for "Data Analyst" — BA does requirements, DA does SQL/dashboards)
- NEVER include industry prefixes ("Fintech DevOps", "Healthcare Data Analyst")
- NEVER include tool/stack titles ("Tableau Analyst", "Terraform Engineer", "React Developer")
- NEVER include seniority prefixes (Sr, Jr, Staff, Principal, Lead, Head of)
- Every variant must be at least 2 words, OR a well-known industry abbreviation (BI, PM, TPM, SWE, SRE, QA, UX, DS, ML, AE, BDR, SDR).

For reference, good expansions look like:
  "Data Analyst" → ["BI Analyst", "Business Intelligence Analyst", "Reporting Analyst", "Analytics Analyst", "Data Reporting Analyst"]
  "DevOps Engineer" → ["Site Reliability Engineer", "SRE", "Platform Engineer", "Infrastructure Engineer", "Cloud Operations Engineer"]
  "Product Manager" → ["PM", "Product Owner", "Technical Product Manager", "Group Product Manager", "Associate Product Manager"]

Return 5-15 titles. Quality over quantity — better 5 tight variants than 20 loose ones. JSON array only, no explanation.""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            titles = json.loads(match.group())
            clean = [t.strip() for t in titles if isinstance(t, str) and t.strip()]
            return _sanitize_expansions(title, clean)
    except Exception as e:
        logger.error(f"[AI] Title expansion failed: {e}")

    return _sanitize_expansions(title, expand_title_heuristic(title))


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
    skill_signature: dict | None = None,
) -> int:
    """Smart relevance scoring — fuzzy pre-filter saves API calls.

    Strategy:
      - Fuzzy score < 25  → obvious mismatch, return fuzzy score (NO API call)
      - Fuzzy score > 85  → obvious match, return fuzzy score (NO API call)
      - Fuzzy score 25-85 → ambiguous, ask AI to decide (API call)

    This skips ~70-80% of API calls while keeping AI for the hard cases."""

    # Quick exclude check — blocked keywords = instant reject
    desc_lower = job_description.lower()
    title_lower = job_title.lower()
    for kw in excluded_keywords:
        if kw.lower() in desc_lower or kw.lower() in title_lower:
            return 5

    # Always run fuzzy first (free, instant) — now with skill-signature rescue
    fuzzy = score_relevance_fuzzy(
        job_title, job_description, target_title, expanded_titles, keywords,
        skill_signature=skill_signature,
    )

    # Fall back to fuzzy only if no API key
    if not settings.anthropic_api_key:
        return fuzzy

    # COST GATE: skip AI for obvious cases (wider gate = fewer API calls)
    if fuzzy < 35:
        return fuzzy  # obvious mismatch — NO API call
    if fuzzy > 75:
        return fuzzy  # strong match — NO API call

    try:
        import anthropic

        kw_str = ", ".join(keywords[:10]) if keywords else "none"
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=5,
            messages=[{
                "role": "user",
                "content": f"""Rate 0-100 match: "{job_title}" for a "{target_title}" role. Skills: {kw_str}
Desc: {job_description[:400]}
90-100=same role family, 75-89=closely related, 40-74=some overlap, 0-39=not relevant.
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
    return score_relevance_fuzzy(
        job_title, job_description, target_title, expanded_titles, keywords,
        skill_signature=skill_signature,
    )


# ──────────────────────────────────────────────
# Role families — used as a hard fence so "Data Analyst" never matches
# "Software Engineer" jobs regardless of what fuzzy thinks.
# ──────────────────────────────────────────────
_ROLE_FAMILIES: dict[str, set[str]] = {
    "data_analytics": {
        "analyst", "analytics", "analysis", "bi", "business intelligence",
        "reporting", "insight", "insights", "tableau", "power bi", "powerbi",
        "looker", "data visualization", "dashboard",
    },
    "data_engineering": {
        "data engineer", "analytics engineer", "etl", "data pipeline",
        "data platform", "data infrastructure",
    },
    "data_science": {
        "data scientist", "machine learning", "ml engineer", "ai engineer",
        "deep learning", "nlp", "computer vision", "mlops",
    },
    "software_engineering": {
        "software engineer", "software developer", "swe", "programmer",
        "backend", "back-end", "frontend", "front-end", "full stack",
        "fullstack", "full-stack", "web developer", "mobile developer",
        "ios developer", "android developer", "application developer",
        "application engineer", "engineering manager",
    },
    "devops_platform": {
        "devops", "sre", "site reliability", "platform engineer",
        "infrastructure engineer", "cloud engineer", "systems engineer",
        "release engineer",
    },
    "security": {
        "security engineer", "security analyst", "security operations",
        "infosec", "cybersecurity", "cyber security", "soc analyst",
        "penetration tester", "red team", "blue team",
    },
    "design": {
        "ux designer", "ui designer", "product designer", "visual designer",
        "graphic designer", "interaction designer", "ux/ui",
    },
    "product": {
        "product manager", "product owner", "tpm", "program manager",
        "product lead",
    },
    "marketing": {
        "marketing", "growth", "seo", "content marketing", "brand",
        "demand generation", "social media", "copywriter",
    },
    "sales_cs": {
        "sales", "account executive", "account manager", "business development",
        "bdr", "sdr", "customer success", "customer experience",
        "solutions engineer", "sales engineer", "engagement manager",
    },
    "qa": {
        "qa engineer", "quality assurance", "test engineer", "sdet",
        "automation engineer",
    },
    "ops_pm": {
        "project manager", "operations manager", "scrum master",
        "chief of staff",
    },
    "finance": {
        "accountant", "financial analyst", "controller", "auditor",
        "bookkeeper", "tax ",
    },
    "hr": {
        "recruiter", "talent acquisition", "hr ", "human resources",
        "people operations", "people ops",
    },
    "support": {
        "support specialist", "technical support", "help desk", "helpdesk",
        "customer support",
    },
    "implementation": {
        "implementation specialist", "implementation consultant",
        "onboarding specialist", "deployment engineer",
    },
}

# Families that are compatible enough to not penalize each other.
_FAMILY_ADJACENCY: dict[str, set[str]] = {
    "data_analytics": {"data_analytics", "data_science"},
    "data_engineering": {"data_engineering", "data_analytics", "data_science"},
    "data_science": {"data_science", "data_analytics", "data_engineering"},
    "software_engineering": {"software_engineering", "devops_platform"},
    "devops_platform": {"devops_platform", "software_engineering", "security"},
    "security": {"security", "devops_platform"},
    "design": {"design", "product"},
    "product": {"product", "design"},
    "marketing": {"marketing", "sales_cs"},
    "sales_cs": {"sales_cs", "marketing"},
    "qa": {"qa", "software_engineering"},
    "ops_pm": {"ops_pm", "product"},
    "finance": {"finance"},
    "hr": {"hr"},
    "support": {"support"},
    "implementation": {"implementation", "support", "sales_cs"},
}


def _detect_family(text: str) -> Optional[str]:
    """Return the first role family that appears in `text` (already lowercased)."""
    # Order matters — check most-specific signals first.
    priority = [
        "data_engineering", "data_science", "data_analytics",
        "security", "devops_platform", "software_engineering",
        "qa", "design", "product", "ops_pm",
        "implementation", "support",
        "marketing", "sales_cs",
        "finance", "hr",
    ]
    for fam in priority:
        for marker in _ROLE_FAMILIES.get(fam, set()):
            if marker in text:
                return fam
    return None


def score_relevance_fuzzy(
    job_title: str,
    job_description: str,
    target_title: str,
    expanded_titles: list[str],
    keywords: list[str] = [],
    skill_signature: dict | None = None,
) -> int:
    """
    Fuzzy matching score with role-family fence.

    Key behaviors:
      - Substring match requires multi-token targets (blocks single-word
        generic variants like "Developer" matching every SWE job)
      - token_set_ratio as the primary comparator (resilient to word order
        and junk like city names, salary, parentheticals)
      - partial_ratio is only consulted for multi-token targets ≥ 12 chars,
        preventing explosive false positives
      - Role-family fence: if job title is clearly in a different family
        than the target (e.g. Data Analyst target vs. Software Engineer job),
        the score is hard-capped at 22 regardless of what fuzzy thinks.
      - Keyword boost only fires when base title score ≥ 50.
    """
    job_lower = job_title.lower()
    target_lower = target_title.lower()

    # Pre-sanitize expansions so bad data from old DB rows can't poison scoring
    clean_expanded = _sanitize_expansions(target_title, expanded_titles or [])
    all_targets = [target_title] + clean_expanded

    best_score = 0
    for target in all_targets:
        tl = target.lower().strip()
        if not tl:
            continue
        tok_count = len(tl.split())

        # ── Substring match — only for multi-token targets ────────────────
        if tok_count >= 2 and tl in job_lower:
            coverage = len(tl) / max(len(job_lower), 1)
            substr_score = int(85 + coverage * 15)
            best_score = max(best_score, min(100, substr_score))

        # ── token_set_ratio — resilient to word order and extra noise ─────
        # (unlike partial_ratio, it won't 100-score "Developer" vs "SWE")
        s_set = fuzz.token_set_ratio(job_lower, tl)
        # Penalize when the target is a single token (more false positives)
        if tok_count == 1:
            s_set = int(s_set * 0.55)
        best_score = max(best_score, s_set)

        # token_sort_ratio — catches reordered variants
        s_sort = fuzz.token_sort_ratio(job_lower, tl)
        if tok_count == 1:
            s_sort = int(s_sort * 0.55)
        best_score = max(best_score, s_sort)

        # partial_ratio — ONLY for safe, multi-token, long-enough targets
        if tok_count >= 2 and len(tl) >= 12:
            s_part = fuzz.partial_ratio(job_lower, tl)
            best_score = max(best_score, int(s_part * 0.70))

    base_title_score = best_score

    # ── Role-family fence ────────────────────────────────────────────────
    # If the job title is clearly a different role family than the target
    # (e.g. target=Data Analyst, job=Software Engineer), cap the score
    # regardless of what fuzzy/keywords say. This is the hard fix for the
    # "Data Analyst filter showing QA Engineer / Web Developer / Marketing"
    # class of leak.
    target_family = _detect_family(target_lower)
    for exp in clean_expanded:
        if target_family:
            break
        target_family = _detect_family(exp.lower())
    job_family = _detect_family(job_lower)

    fence_capped = False
    if target_family and job_family and job_family != target_family:
        allowed = _FAMILY_ADJACENCY.get(target_family, {target_family})
        if job_family not in allowed:
            # Hard cap — different family, not adjacent. No amount of
            # keyword overlap should rescue this job.
            base_title_score = min(base_title_score, 22)
            best_score = base_title_score
            fence_capped = True

    # ── v1.9.6: Skill-signature description rescue ───────────────────────
    # If the family fence just capped this job at 22, OR if the title-only
    # score is weak (< 50), check whether the description matches the
    # profile's skill signature. A strong signature hit means this job is
    # the role-in-disguise (Solutions Engineer → DA, Product Analyst → DA,
    # Business Systems Analyst + EDW → DA). In that case we OVERRIDE the
    # title-based score with the signature-based one, no AI call needed.
    #
    # If no signature is provided on the call, fall back to the built-in
    # signature for the target role (so the rescue still works for older
    # profile rows that don't have skill_signature populated yet).
    if (fence_capped or best_score < 50) and job_description:
        sig = skill_signature
        if not sig:
            sig = _get_fallback_signature(target_title)
        if sig:
            sig_score, _ = score_signature_match(job_title, job_description, sig)
            if sig_score >= 60:
                # Description clearly identifies this as the role —
                # override whatever the title-based scoring said.
                best_score = max(best_score, sig_score)
                base_title_score = best_score

    # ── Keyword boost — only when title is ALREADY a reasonable match ────
    # Raised threshold from 40 → 50 so we don't rescue borderline noise.
    if base_title_score >= 50:
        desc_lower = job_description.lower() if job_description else ""
        keyword_boost = 0
        for kw in keywords:
            kw_lower = kw.lower().strip()
            if not kw_lower or len(kw_lower) < 3:
                continue
            if kw_lower in job_lower:
                keyword_boost += 10
            elif kw_lower in desc_lower:
                keyword_boost += 4
        best_score = min(100, best_score + min(keyword_boost, 15))

    return int(min(100, best_score))


# ══════════════════════════════════════════════════════════════════════
# v1.9.6 — SKILL SIGNATURE: description-based rescue for disguised roles
# ══════════════════════════════════════════════════════════════════════
#
# The family fence above is great at killing obvious mismatches but it
# also blocks legit-but-disguised roles ("Solutions Engineer" that's
# really 80% data work, "Product Analyst" that's really a Data Analyst,
# "Business Systems Analyst" with EDW/SQL/Tableau in the description).
#
# Skill signature is a per-profile JSON object generated ONCE by Claude
# Haiku when the profile is created. At runtime the scorer walks the
# job description with PURE PYTHON (zero AI calls per job) looking for
# foundation + toolkit hits. If the signature triggers strongly, the
# scorer overrides the family-fence cap with a description-based score.
#
# Cost: 1 AI call per profile (one-time), 0 AI calls per job. Runs
# in microseconds against thousands of jobs per cycle.
# ══════════════════════════════════════════════════════════════════════

# Hardcoded fallback signatures for common roles, used when AI is
# unavailable or hasn't generated one yet. These give us a working
# rescue path immediately, even before the first AI call.
_FALLBACK_SIGNATURES: dict[str, dict] = {
    "data analyst": {
        "foundation": ["sql", "bigquery", "snowflake", "redshift", "postgres", "mysql", "athena"],
        "toolkit": ["tableau", "power bi", "powerbi", "looker", "domo", "qlik", "thoughtspot",
                    "microstrategy", "mode", "metabase", "excel", "google sheets"],
        "bonus": ["dashboard", "dashboards", "reporting", "kpi", "kpis", "stakeholder",
                  "ad-hoc analysis", "ad hoc analysis", "a/b test", "ab test", "cohort",
                  "funnel", "etl", "data visualization", "self-service analytics", "insights"],
    },
    "business intelligence analyst": {
        "foundation": ["sql", "bigquery", "snowflake", "redshift", "postgres", "athena"],
        "toolkit": ["tableau", "power bi", "powerbi", "looker", "domo", "qlik", "thoughtspot",
                    "microstrategy", "ssrs", "ssas", "ssis"],
        "bonus": ["dashboard", "dashboards", "kpi", "reporting", "data warehouse",
                  "olap", "cube", "etl", "executive reporting", "data mart"],
    },
    "data engineer": {
        "foundation": ["airflow", "spark", "dbt", "kafka", "flink", "beam"],
        "toolkit": ["snowflake", "bigquery", "redshift", "databricks", "s3", "gcs", "emr",
                    "glue", "kinesis", "kafka", "pubsub"],
        "bonus": ["pipeline", "pipelines", "etl", "elt", "orchestration", "dag",
                  "data lake", "data warehouse", "data platform", "ingestion"],
    },
    "data scientist": {
        "foundation": ["python", "r ", "scikit-learn", "tensorflow", "pytorch", "xgboost"],
        "toolkit": ["jupyter", "pandas", "numpy", "spark", "mlflow", "kubeflow", "sagemaker"],
        "bonus": ["machine learning", "deep learning", "regression", "classification",
                  "clustering", "nlp", "computer vision", "feature engineering",
                  "model training", "experimentation", "hypothesis testing"],
    },
    "software engineer": {
        "foundation": ["python", "java", "javascript", "typescript", "go", "rust", "c++", "ruby"],
        "toolkit": ["react", "node", "django", "flask", "spring", "rails", "express",
                    "kubernetes", "docker", "aws", "gcp", "azure"],
        "bonus": ["api", "microservices", "rest", "graphql", "ci/cd", "code review",
                  "system design", "distributed", "production", "scalable"],
    },
    "devops engineer": {
        "foundation": ["terraform", "ansible", "kubernetes", "docker", "helm"],
        "toolkit": ["aws", "gcp", "azure", "jenkins", "gitlab ci", "github actions",
                    "argocd", "prometheus", "grafana", "datadog", "elk"],
        "bonus": ["ci/cd", "infrastructure as code", "iac", "sre", "reliability",
                  "incident", "on-call", "monitoring", "observability", "deployment"],
    },
    "security engineer": {
        "foundation": ["siem", "splunk", "crowdstrike", "wiz", "tenable", "burp"],
        "toolkit": ["python", "bash", "wireshark", "nmap", "metasploit", "okta", "iam"],
        "bonus": ["soc", "incident response", "threat hunting", "penetration test",
                  "vulnerability", "compliance", "soc 2", "iso 27001", "pci"],
    },
    "product manager": {
        "foundation": ["roadmap", "user research", "prioritization", "okrs"],
        "toolkit": ["jira", "linear", "figma", "amplitude", "mixpanel", "looker", "tableau"],
        "bonus": ["stakeholder", "discovery", "go-to-market", "gtm", "launch",
                  "feature", "user story", "sprint", "agile", "metrics"],
    },
    "ux designer": {
        "foundation": ["figma", "sketch", "adobe xd", "invision", "axure"],
        "toolkit": ["wireframe", "prototype", "mockup", "design system", "user research"],
        "bonus": ["accessibility", "wcag", "user testing", "usability", "user flow",
                  "interaction design", "visual design", "personas"],
    },
}


def _get_fallback_signature(title: str) -> dict:
    """Return a built-in signature for common titles, or {} if unknown."""
    tl = title.lower().strip()
    # Exact match first
    if tl in _FALLBACK_SIGNATURES:
        return _FALLBACK_SIGNATURES[tl]
    # Fuzzy match against fallback keys
    best_key = None
    best_score = 0
    for key in _FALLBACK_SIGNATURES.keys():
        s = fuzz.token_set_ratio(tl, key)
        if s > best_score:
            best_score = s
            best_key = key
    if best_score >= 80 and best_key:
        return _FALLBACK_SIGNATURES[best_key]
    return {}


async def generate_skill_signature_ai(title: str, keywords: list[str] | None = None) -> dict:
    """One-time Haiku call per profile: generate a {foundation, toolkit, bonus}
    signature describing the SKILLS and TOOLS that mark a job description as
    matching this role. Result is cached on the profile row forever.

    This is the only AI call in the whole signature pipeline — runtime
    scoring is pure Python.
    """
    fallback = _get_fallback_signature(title)
    if not settings.anthropic_api_key:
        return fallback

    kw_hint = ""
    if keywords:
        kw_hint = f"\nThe user already lists these skills as important: {', '.join(keywords[:20])}"

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{
                "role": "user",
                "content": f"""For the job role "{title}", list the skills and tools that, when found together in a job description, identify it as this role REGARDLESS of what the job title says.{kw_hint}

Return a JSON object with three lists:
- "foundation": 5-12 core technical skills/languages/data stores. These are the "must have" items — at least one must appear in the description for the job to plausibly be this role. Examples for Data Analyst: SQL, BigQuery, Snowflake, Redshift, Postgres.
- "toolkit": 5-15 tools/libraries/frameworks specific to the day-to-day work. At least one must appear alongside a foundation item. Examples for Data Analyst: Tableau, Power BI, Looker, Domo, Excel, Mode, Metabase.
- "bonus": 8-20 supporting concepts, methodologies, or weaker signals that strengthen the match. Examples for Data Analyst: dashboards, KPIs, stakeholder reporting, ad-hoc analysis, A/B testing, cohort analysis, funnel.

Rules:
- Use lowercase strings.
- Use the most common substring people would actually write (e.g. "power bi" not "Microsoft Power BI Desktop").
- foundation + toolkit together should be enough to identify the role from a description alone, even if the title is something misleading like "Solutions Engineer" or "Business Systems Analyst".
- Keep items generic enough to match how real job descriptions are written.
- NO seniority words, NO company names, NO management terms.

Return ONLY the JSON object, no commentary.""",
            }],
        )
        text = response.content[0].text.strip()
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            obj = json.loads(match.group())
            sig = {
                "foundation": [s.lower().strip() for s in obj.get("foundation", []) if isinstance(s, str) and s.strip()],
                "toolkit":    [s.lower().strip() for s in obj.get("toolkit", [])    if isinstance(s, str) and s.strip()],
                "bonus":      [s.lower().strip() for s in obj.get("bonus", [])      if isinstance(s, str) and s.strip()],
            }
            # Merge with fallback so we always have something even if AI returned thin lists
            if fallback:
                for bucket in ("foundation", "toolkit", "bonus"):
                    seen = set(sig[bucket])
                    for item in fallback.get(bucket, []):
                        if item not in seen:
                            sig[bucket].append(item)
                            seen.add(item)
            logger.info(f"[Signature] Generated for '{title}': "
                        f"{len(sig['foundation'])} foundation, {len(sig['toolkit'])} toolkit, {len(sig['bonus'])} bonus")
            return sig
    except Exception as e:
        logger.error(f"[Signature] AI generation failed for '{title}': {e}")

    return fallback


def score_signature_match(job_title: str, job_description: str, signature: dict) -> tuple[int, dict]:
    """ZERO-AI runtime scorer. Walks the description looking for signature
    matches and returns (score 0-100, breakdown dict).

    Scoring rule:
      - 0 foundation hits         → 0          (not this role at all)
      - 1 foundation + 0 toolkit  → 25         (weak — could be anything)
      - 1 foundation + 1 toolkit  → 60         (looks like the role)
      - 2+ foundation + 1 toolkit → 70
      - 1 foundation + 2+ toolkit → 75
      - 2+ foundation + 2+ toolkit→ 80
      - + bonus signals (each)    → +2 each, capped at +15
      - title contains a foundation/toolkit word → +5

    A score of ≥ 60 is enough to bypass the family fence in the main scorer.
    """
    if not signature or not isinstance(signature, dict):
        return 0, {}

    foundation = signature.get("foundation") or []
    toolkit = signature.get("toolkit") or []
    bonus = signature.get("bonus") or []

    # Combine title + description for searching — title-mentions get bonus.
    title_lower = (job_title or "").lower()
    desc_lower = (job_description or "").lower()
    blob = title_lower + " \n " + desc_lower

    def count_hits(items):
        hit_list = []
        for item in items:
            it = item.lower().strip()
            if not it:
                continue
            # Word-boundary-ish check — avoid matching "java" inside "javascript".
            # Cheap heuristic: require non-alnum on both sides OR start/end of blob.
            idx = blob.find(it)
            if idx < 0:
                continue
            # Verify boundary
            left_ok = idx == 0 or not blob[idx - 1].isalnum()
            end = idx + len(it)
            right_ok = end >= len(blob) or not blob[end].isalnum()
            if left_ok and right_ok:
                hit_list.append(it)
        return hit_list

    found_hits = count_hits(foundation)
    tool_hits = count_hits(toolkit)
    bonus_hits = count_hits(bonus)

    n_found = len(found_hits)
    n_tool = len(tool_hits)
    n_bonus = len(bonus_hits)

    # Base score from foundation × toolkit combination
    if n_found == 0:
        base = 0
    elif n_tool == 0:
        base = 25
    elif n_found == 1 and n_tool == 1:
        base = 60
    elif n_found >= 2 and n_tool == 1:
        base = 70
    elif n_found == 1 and n_tool >= 2:
        base = 75
    else:  # n_found >= 2 and n_tool >= 2
        base = 80

    # Bonus signals — each adds +2, capped at +15
    base += min(n_bonus * 2, 15)

    # Title also contains a foundation/toolkit term → +5
    title_has_signal = any(
        h in title_lower for h in (found_hits + tool_hits)
    )
    if title_has_signal:
        base += 5

    score = int(min(100, base))
    return score, {
        "foundation_hits": found_hits,
        "toolkit_hits": tool_hits,
        "bonus_hits": bonus_hits,
        "base": base,
    }


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

            client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
            response = await client.messages.create(
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

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
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

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
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

        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{
                "role": "user",
                "content": f"""Extract skills from: "{title}" — {description[:1000]}
JSON array only. Use known names: Python, SQL, Power BI, Tableau, Excel, AWS, Azure, etc.""",
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
    """Score a batch of jobs for relevance and trust.
    Uses AI for relevance (with fuzzy gate) but heuristic-only for trust (no API calls)."""
    target_title = profile["title"]
    expanded = profile.get("expanded_titles", [])
    keywords = profile.get("keywords", [])
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    excluded = profile.get("excluded_keywords", [])
    if isinstance(excluded, str):
        excluded = [k.strip() for k in excluded.split(",") if k.strip()]
    sig = profile.get("skill_signature") or _get_fallback_signature(target_title)

    scored = []
    for job in jobs:
        relevance = await score_relevance_ai(
            job["title"], job.get("description", ""),
            target_title, expanded, keywords, excluded,
            skill_signature=sig,
        )
        # Heuristic trust only — NO API call (saves ~50% of Anthropic costs)
        trust = score_trust_heuristic(
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
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        response = await client.messages.create(
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
