import aiosqlite
import asyncio
import hashlib
import json
import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional
from rapidfuzz import fuzz
from config import settings
from skills import extract_skills

logger = logging.getLogger(__name__)

# Fuzzy dedup threshold — 88+ means titles are near-identical
FUZZY_TITLE_THRESHOLD = 88

# Ensure the database directory exists (Railway volume must be mounted)
_db_dir = os.path.dirname(settings.database_path)
if _db_dir and not os.path.isdir(_db_dir):
    logger.warning(f"Database directory {_db_dir} does not exist — creating it")
    os.makedirs(_db_dir, exist_ok=True)

DB_PATH = settings.database_path
logger.info(f"Using database at: {DB_PATH}")


async def get_db():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=30000")  # Wait up to 30s for locks (many parallel source workers)
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db():
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS search_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                expanded_titles TEXT DEFAULT '[]',
                keywords TEXT DEFAULT '[]',
                excluded_keywords TEXT DEFAULT '[]',
                locations TEXT DEFAULT '[]',
                remote_only INTEGER DEFAULT 0,
                min_salary INTEGER DEFAULT 0,
                freshness_hours INTEGER DEFAULT 24,
                min_relevance INTEGER DEFAULT 0,
                min_trust INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                company_name TEXT DEFAULT '',
                company_domain TEXT DEFAULT '',
                location TEXT DEFAULT '',
                is_remote INTEGER DEFAULT 0,
                work_type TEXT DEFAULT 'onsite',
                description TEXT DEFAULT '',
                salary_min INTEGER DEFAULT 0,
                salary_max INTEGER DEFAULT 0,
                source TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                direct_apply_url TEXT DEFAULT '',
                posted_at TEXT DEFAULT '',
                first_seen_at TEXT DEFAULT (datetime('now')),
                relevance_score INTEGER DEFAULT 50,
                trust_score INTEGER DEFAULT 50,
                is_direct_apply INTEGER DEFAULT 0,
                skills TEXT DEFAULT '',
                status TEXT DEFAULT 'new',
                search_profile_id INTEGER,
                FOREIGN KEY (search_profile_id) REFERENCES search_profiles(id)
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_first_seen ON jobs(first_seen_at DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_relevance ON jobs(relevance_score DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_trust ON jobs(trust_score DESC);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);

            CREATE TABLE IF NOT EXISTS source_settings (
                source_key TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                category TEXT DEFAULT 'api',
                requires_key TEXT DEFAULT '',
                updated_at TEXT DEFAULT (datetime('now'))
            );

            -- Companies found by the always-on AI discovery bot. Merged with the
            -- static sources/ats_companies.json at scrape time so the roster grows
            -- persistently without needing a code deploy.
            CREATE TABLE IF NOT EXISTS discovered_companies (
                slug TEXT NOT NULL,
                ats TEXT NOT NULL,
                name TEXT DEFAULT '',
                tenant TEXT DEFAULT '',
                wd TEXT DEFAULT '',
                site TEXT DEFAULT '',
                jobs_seen INTEGER DEFAULT 0,
                added_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (slug, ats)
            );
        """)
        await db.commit()

        # Migration: add work_type column if missing (for existing DBs)
        try:
            await db.execute("SELECT work_type FROM jobs LIMIT 1")
        except Exception:
            await db.execute("ALTER TABLE jobs ADD COLUMN work_type TEXT DEFAULT 'onsite'")
            # Backfill existing rows: set work_type based on is_remote
            await db.execute("UPDATE jobs SET work_type = 'remote' WHERE is_remote = 1")
            await db.commit()

        # Create work_type index (after migration ensures column exists)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_work_type ON jobs(work_type)")
        await db.commit()

        # Migration: add skills column if missing
        try:
            await db.execute("SELECT skills FROM jobs LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding skills column to jobs table")
            await db.execute("ALTER TABLE jobs ADD COLUMN skills TEXT DEFAULT ''")
            await db.commit()

        # Migration: add hash_cross column for cross-source dedup
        try:
            await db.execute("SELECT hash_cross FROM jobs LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding hash_cross column for cross-source dedup")
            await db.execute("ALTER TABLE jobs ADD COLUMN hash_cross TEXT DEFAULT ''")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hash_cross ON jobs(hash_cross)")
            await db.commit()

        # Migration: add source_url index for URL dedup
        await db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source_url ON jobs(source_url)")
        await db.commit()

        # Migration: add applied_at timestamp for tracking when user applied
        try:
            await db.execute("SELECT applied_at FROM jobs LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding applied_at column to jobs table")
            await db.execute("ALTER TABLE jobs ADD COLUMN applied_at TEXT DEFAULT ''")
            await db.commit()

        # Migration: add salary_period column if missing (v2.0.0)
        try:
            await db.execute("SELECT salary_period FROM jobs LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding salary_period column to jobs table")
            await db.execute("ALTER TABLE jobs ADD COLUMN salary_period TEXT DEFAULT 'yearly'")
            await db.commit()

        # Migration: add scored_at column if missing (v2.1.0)
        # CRITICAL COST FIX: without this, scored jobs kept status='new' and were
        # re-scored through Haiku every 5-minute cycle for up to 72h (~800x per job).
        # scored_at marks a job as already-scored so AI relevance runs exactly ONCE.
        try:
            await db.execute("SELECT scored_at FROM jobs LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding scored_at column to jobs table")
            await db.execute("ALTER TABLE jobs ADD COLUMN scored_at TEXT DEFAULT ''")
            await db.commit()

        # Migration: add skill_signature column to search_profiles (v1.9.6)
        # Stores a JSON object: {"foundation": [...], "toolkit": [...], "bonus": [...]}
        # Used by the description-based rescue scorer so disguised roles
        # (Solutions Engineer that's really a DA, Product Analyst that's
        # really a DA, etc.) can be matched without per-job AI calls.
        try:
            await db.execute("SELECT skill_signature FROM search_profiles LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding skill_signature column to search_profiles")
            await db.execute("ALTER TABLE search_profiles ADD COLUMN skill_signature TEXT DEFAULT '{}'")
            await db.commit()

        # Backfill: extract skills for ALL jobs missing skills (one pass)
        cursor = await db.execute(
            "SELECT id, title, description FROM jobs WHERE skills IS NULL OR skills = ''"
        )
        backfill_rows = await cursor.fetchall()
        if backfill_rows:
            tagged = 0
            for row in backfill_rows:
                skills = extract_skills(row[1] or "", row[2] or "")
                # Use "_none" sentinel so this row is never re-selected
                await db.execute("UPDATE jobs SET skills = ? WHERE id = ?",
                                 (skills if skills else "_none", row[0]))
                if skills:
                    tagged += 1
            await db.commit()
            logger.info(f"[Backfill] Complete — processed {len(backfill_rows)} jobs ({tagged} had skills)")

    finally:
        await db.close()


def _normalize_text(text: str) -> str:
    """Normalize text for fuzzy comparison — expand abbreviations, strip noise."""
    t = text.lower().strip()
    # Common title abbreviations → full form
    swaps = {
        r"\bsr\.?\b": "senior", r"\bjr\.?\b": "junior", r"\bmgr\.?\b": "manager",
        r"\beng\.?\b": "engineer", r"\bdev\.?\b": "developer", r"\badmin\.?\b": "administrator",
        r"\bassoc\.?\b": "associate", r"\bdir\.?\b": "director", r"\bvp\b": "vice president",
        r"\bii\b": "2", r"\biii\b": "3", r"\biv\b": "4",
    }
    for pat, repl in swaps.items():
        t = re.sub(pat, repl, t)
    # Strip trailing dots, dashes, extra whitespace
    t = re.sub(r"[.\-/]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _normalize_company(name: str) -> str:
    """Normalize company name for matching."""
    t = name.lower().strip()
    # Remove common suffixes
    t = re.sub(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings)\b", "", t)
    t = re.sub(r"[,.\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def make_job_hash(company: str, title: str, location: str) -> str:
    raw = f"{company.lower().strip()}|{title.lower().strip()}|{location.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# US-only filtering (central — applied to EVERY source via insert_job)
# ─────────────────────────────────────────────────────────────────────────────
_US_TOKENS = [
    " us", " us ", "u.s.", "u.s ", "usa", "united states", "america",
    "north america", "americas", "remote - us", "remote, us", "remote (us",
    "us-remote", "us remote", "worldwide", "anywhere", "global",
]
_US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
}
_US_STATE_ABBR = {
    " al", " ak", " az", " ar", " ca", " co", " ct", " de", " fl", " ga",
    " hi", " id", " il", " in", " ia", " ks", " ky", " la", " me", " md",
    " ma", " mi", " mn", " ms", " mo", " mt", " ne", " nv", " nh", " nj",
    " nm", " ny", " nc", " nd", " oh", " ok", " or", " pa", " ri", " sc",
    " sd", " tn", " tx", " ut", " vt", " va", " wa", " wv", " wi", " wy", " dc",
}
# Clearly-foreign country/region/city tokens (expanded — was missing many)
_NON_US_TOKENS = [
    "emea", "apac", "latam", "india", "pakistan", "bangladesh", "vietnam",
    "philippines", "indonesia", "malaysia", "thailand", "singapore",
    "hong kong", "taiwan", "japan", "korea", "china", "australia",
    "new zealand", "united kingdom", "uk only", " uk", "u.k", "england", "scotland",
    "wales", "ireland", "germany", "france", "spain", "italy", "portugal",
    "netherlands", "belgium", "switzerland", "austria", "poland", "sweden",
    "norway", "finland", "denmark", "greece", "turkey", "israel", "uae",
    "saudi", "egypt", "south africa", "nigeria", "kenya", "brazil",
    "argentina", "chile", "colombia", "mexico", "canada only", "canada,",
    # additions:
    "costa rica", "panama", "guatemala", "honduras", "nicaragua",
    "el salvador", "dominican", "ecuador", "bolivia", "uruguay", "paraguay",
    "venezuela", "peru", "ukraine", "romania", "bulgaria", "serbia",
    "croatia", "slovenia", "slovakia", "czech", "czechia", "hungary",
    "lithuania", "latvia", "estonia", "russia", "belarus", "armenia",
    "kazakhstan", "morocco", "ghana", "tunisia", "algeria", "qatar",
    "kuwait", "bahrain", "oman", "jordan", "lebanon", "sri lanka", "nepal",
    "cambodia", "myanmar", "mongolia", "iceland", "luxembourg", "malta",
    "cyprus", "kyiv", "lviv", "kiev", "bengaluru", "bangalore",
    "canada", "toronto", "vancouver", "montreal", "ottawa", "calgary",
    "edmonton", "deutschland", "österreich", "schweiz", "mumbai", "delhi",
    "hyderabad", "pune", "chennai", "gurgaon", "noida",
]


# Word-boundary matchers. insert_job()'s US gate used bare substring tests,
# which both let foreign cities through and rejected real US ones.
_DB_US_ABBR_RE = re.compile(
    r"(?:^|[,\-/(]|\s)\s*(" + "|".join(
        a.strip() for a in sorted(_US_STATE_ABBR)) + r")(?=$|[\s,.)/;])",
    re.IGNORECASE,
)

_DB_NON_US_CITIES = [
    "jakarta", "bangalore", "bengaluru", "mumbai", "delhi", "hyderabad",
    "chennai", "pune", "berlin", "munich", "hamburg", "frankfurt", "chisinau",
    "toronto", "vancouver", "montreal", "london", "manchester", "dublin",
    "paris", "madrid", "barcelona", "lisbon", "amsterdam", "brussels",
    "zurich", "vienna", "warsaw", "prague", "stockholm", "oslo", "helsinki",
    "copenhagen", "athens", "istanbul", "dubai", "tel aviv", "cairo", "lagos",
    "nairobi", "johannesburg", "cape town", "sao paulo", "buenos aires",
    "santiago", "bogota", "lima", "mexico city", "guadalajara", "manila",
    "bangkok", "hanoi", "kuala lumpur", "seoul", "tokyo", "osaka", "beijing",
    "shanghai", "shenzhen", "taipei", "sydney", "melbourne", "auckland",
    "wellington", "karachi", "lahore", "dhaka", "colombo", "gdansk", "krakow",
]

_DB_NON_US_CITY_RE = re.compile(
    r"(?<![a-z])(" + "|".join(re.escape(c) for c in
                              sorted(_DB_NON_US_CITIES, key=len, reverse=True))
    + r")(?![a-z])",
    re.IGNORECASE,
)


def is_us_location(location: str) -> bool:
    """US-eligibility check, biased to KEEP (drop only clearly-foreign roles).

    - Empty / unknown location → keep (can't determine; don't over-drop).
    - Any US signal (US / state / Worldwide / Anywhere) → keep.
    - A clearly-foreign country/region/city with no US signal → DROP.
    - Otherwise (bare 'Remote', a US city with no state, anything unrecognized)
      → keep. This avoids wrongly dropping US jobs listed as just a city.
    """
    if not location:
        return True
    loc = f" {location.strip().lower()} "
    # 1. Strong US signals win outright (covers "North America (US, Canada)").
    if any(tok in loc for tok in _US_TOKENS):
        return True
    if any(state in loc for state in _US_STATES):
        return True
    # 2. Clearly foreign → drop. MUST run before the loose state-abbreviation
    #    check below, which otherwise false-matches ' ca' in 'canada', ' in' in
    #    'india', ' co' in 'colombia', etc.
    if any(tok in loc for tok in _NON_US_TOKENS):
        return False
    # 3. A known non-US city vetoes, and must be checked BEFORE state
    #    abbreviations: "Jakarta, ID" and "Berlin, DE" would otherwise match
    #    Idaho and Delaware, and "London" used to reach the lenient default
    #    below and land on the board despite us_only being on.
    if _DB_NON_US_CITY_RE.search(loc):
        return False
    # 4. US state abbreviations, word-boundary anchored. A bare substring test
    #    matched " la" inside "Lagos, NG".
    if _DB_US_ABBR_RE.search(loc):
        return True
    # 5. No signal either way → keep (bare "Remote", lone US city, unknown).
    return True


# SQLite in WAL mode allows many readers but only ONE writer. This app runs
# ~13 concurrent workers (5+3 ATS platforms, Light, JobSpy, LinkedIn, Scoring,
# two discovery bots), and every one of them opens its own connection. When
# several tried to write at once they queued on busy_timeout and eventually
# raised "database is locked", which crashed whole worker passes — the
# Discovery worker died that way in production. Serialising writes in-process
# removes the contention entirely: nothing waits on the OS lock any more.
_WRITE_LOCK: Optional[asyncio.Lock] = None


def _write_lock() -> asyncio.Lock:
    global _WRITE_LOCK
    if _WRITE_LOCK is None:
        _WRITE_LOCK = asyncio.Lock()
    return _WRITE_LOCK


async def insert_job(job_data: dict) -> bool:
    """Insert a job if it doesn't already exist. Serialised against other writers."""
    async with _write_lock():
        return await _insert_job_unlocked(job_data)


async def _insert_job_unlocked(job_data: dict) -> bool:
    """Insert a job if it doesn't already exist (exact hash + fuzzy title + URL check). Returns True if inserted."""
    # US-only gate — applies to EVERY source. Skip clearly non-US locations;
    # keep empty/unknown location (can't determine origin).
    if getattr(settings, "us_only", True):
        _loc = (job_data.get("location") or "").strip()
        if _loc and not is_us_location(_loc):
            return False
    h = make_job_hash(
        job_data.get("company_name", ""),
        job_data.get("title", ""),
        job_data.get("location", ""),
    )
    db = await get_db()
    try:
        # 1. Exact hash match — fastest check
        existing = await db.execute("SELECT id FROM jobs WHERE hash = ?", (h,))
        if await existing.fetchone():
            return False

        # 1b. Source URL dedup — same URL from different scrape cycles
        source_url = job_data.get("source_url", "")
        if source_url:
            existing_url = await db.execute(
                "SELECT id FROM jobs WHERE source_url = ? LIMIT 1", (source_url,)
            )
            if await existing_url.fetchone():
                return False

        # 1c. Cross-source dedup — same company + normalized title (ignore location diffs)
        company_norm_hash = _normalize_company(job_data.get("company_name", ""))
        title_norm_hash = _normalize_text(job_data.get("title", ""))
        if company_norm_hash and title_norm_hash:
            cross_hash = hashlib.md5(f"{company_norm_hash}|{title_norm_hash}".encode()).hexdigest()
            existing_cross = await db.execute(
                "SELECT id FROM jobs WHERE hash_cross = ? LIMIT 1", (cross_hash,)
            )
            if await existing_cross.fetchone():
                return False
        else:
            cross_hash = None

        # 2. Fuzzy dedup — catch "Sr Data Analyst" vs "Senior Data Analyst" etc.
        company_norm = _normalize_company(job_data.get("company_name", ""))
        title_norm = _normalize_text(job_data.get("title", ""))
        if company_norm and title_norm:
            # Pull recent jobs from this company (use LIKE for loose company match)
            cursor = await db.execute(
                "SELECT id, title, company_name FROM jobs WHERE LOWER(company_name) LIKE ? LIMIT 50",
                (f"%{company_norm[:20]}%",),
            )
            similar_jobs = await cursor.fetchall()
            for row in similar_jobs:
                existing_title = _normalize_text(row[1] or "")
                score = fuzz.token_sort_ratio(title_norm, existing_title)
                if score >= FUZZY_TITLE_THRESHOLD:
                    logger.debug(
                        f"[Dedup] Fuzzy match ({score}%): '{job_data.get('title')}' ≈ '{row[1]}' — skipped"
                    )
                    return False
                # Borderline fuzzy (70-87): was calling AI to confirm — DISABLED to cut costs
                # Fuzzy dedup at 87+ is reliable enough; false negatives are acceptable
                # if 70 <= score < FUZZY_TITLE_THRESHOLD:
                #     ... AI dedup removed ...

        now = datetime.now(timezone.utc).isoformat()
        # If no posted_at from source, leave empty — DON'T fake it with scrape time
        # first_seen_at always has the real scrape time for sorting
        posted_at = job_data.get("posted_at", "") or ""
        skills = extract_skills(job_data.get("title", ""), job_data.get("description", "")) or "_none"

        # v2.0.0: Extract salary from description if not provided by source
        salary_period = job_data.get("salary_period", "yearly")
        if not job_data.get("salary_min") and not job_data.get("salary_max"):
            from salary_extractor import extract_salary_regex
            salary_info = extract_salary_regex(job_data.get("description", ""))
            if salary_info:
                job_data["salary_min"] = salary_info["min"]
                job_data["salary_max"] = salary_info["max"]
                salary_period = salary_info["period"]

        await db.execute(
            """INSERT INTO jobs (hash, hash_cross, title, company_name, company_domain, location,
               is_remote, work_type, description, salary_min, salary_max, salary_period, source, source_url,
               direct_apply_url, posted_at, first_seen_at, relevance_score, trust_score,
               is_direct_apply, skills, status, search_profile_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                h,
                cross_hash or "",
                job_data.get("title", ""),
                job_data.get("company_name", ""),
                job_data.get("company_domain", ""),
                job_data.get("location", ""),
                1 if job_data.get("is_remote") else 0,
                job_data.get("work_type", "onsite"),
                (job_data.get("description") or "")[:MAX_DESCRIPTION_CHARS],
                job_data.get("salary_min", 0),
                job_data.get("salary_max", 0),
                salary_period,
                job_data.get("source", ""),
                job_data.get("source_url", ""),
                job_data.get("direct_apply_url", ""),
                posted_at,
                now,
                job_data.get("relevance_score", 50),
                job_data.get("trust_score", 50),
                1 if job_data.get("is_direct_apply") else 0,
                skills,
                "new",
                job_data.get("search_profile_id"),
            ),
        )
        await db.commit()
        return True
    finally:
        await db.close()


async def get_jobs(
    hours: int = 24,
    posted_hours: int = 0,
    min_relevance: int = 0,
    min_trust: int = 0,
    source: str = "",
    status: str = "",
    work_type: str = "",
    # Default: newest FOUND first. A job we just discovered is new information
    # even if it was posted a while ago, and burying fresh discoveries under
    # posted-time ordering is what made the board look stale. "posted_at" stays
    # available and applies the true-posted-time ordering below.
    sort_by: str = "first_seen_at",
    sort_dir: str = "DESC",
    limit: int = 200,
    offset: int = 0,
    search: str = "",
    direct_only: bool = False,
    location: str = "",
    skill: str = "",
    profile_ids: list = None,
) -> list[dict]:
    db = await get_db()
    try:
        conditions = []
        params = []

        if profile_ids:
            placeholders = ",".join("?" for _ in profile_ids)
            conditions.append(f"search_profile_id IN ({placeholders})")
            params.extend(profile_ids)
        else:
            # Ghost profile fix: when no specific profile filter is set,
            # only show jobs from active profiles (soft-deleted profiles excluded)
            conditions.append(
                "(search_profile_id IS NULL OR search_profile_id IN "
                "(SELECT id FROM search_profiles WHERE is_active = 1))"
            )

        if hours > 0:
            conditions.append(
                "first_seen_at >= datetime('now', ?)"
            )
            params.append(f"-{hours} hours")

        # Filter by actual posted time (from the job board)
        # posted_at can be ISO datetime, YYYY-MM-DD, or relative text
        # Only filter if value looks like a valid datetime (contains digits and dashes)
        if posted_hours > 0:
            conditions.append(
                "posted_at != '' AND posted_at IS NOT NULL AND posted_at LIKE '____-__-%' AND posted_at >= datetime('now', ?)"
            )
            params.append(f"-{posted_hours} hours")

        if min_relevance > 0:
            conditions.append("relevance_score >= ?")
            params.append(min_relevance)

        if min_trust > 0:
            conditions.append("trust_score >= ?")
            params.append(min_trust)

        if source:
            conditions.append("source = ?")
            params.append(source)

        if status:
            if status == "new":
                conditions.append("status = 'new'")
            else:
                conditions.append("status = ?")
                params.append(status)
        else:
            # By default, exclude hidden jobs
            conditions.append("status != 'hidden'")

        if work_type:
            conditions.append("work_type = ?")
            params.append(work_type)

        if direct_only:
            conditions.append("is_direct_apply = 1")

        if location:
            loc_words = location.strip().split()
            for lw in loc_words:
                conditions.append("location LIKE ?")
                params.append(f"%{lw}%")

        if skill:
            # Filter by skill tags (OR logic — job must have ANY selected skill)
            skill_parts = [sk.strip() for sk in skill.split(",") if sk.strip()]
            if skill_parts:
                or_clauses = []
                for sk in skill_parts:
                    or_clauses.append("(',' || skills || ',') LIKE ?")
                    params.append(f"%,{sk},%")
                conditions.append("(" + " OR ".join(or_clauses) + ")")

        if search:
            # Search title and company only — description matching is too noisy
            # (a Java Developer job mentioning "data" in its desc would match "Data Analyst")
            # Relevance scores already account for description-level matching
            words = search.strip().split()
            word_conditions = []
            for word in words:
                w = f"%{word}%"
                word_conditions.append("(title LIKE ? OR company_name LIKE ?)")
                params.extend([w, w])
            if word_conditions:
                conditions.append("(" + " AND ".join(word_conditions) + ")")

        where = " AND ".join(conditions) if conditions else "1=1"

        allowed_sorts = {
            "first_seen_at", "relevance_score", "trust_score",
            "salary_max", "company_name", "title", "posted_at"
        }
        if sort_by not in allowed_sorts:
            sort_by = "first_seen_at"
        sort_dir = "ASC" if sort_dir.upper() == "ASC" else "DESC"

        # Sort by ACTUAL POSTED time, newest first. Two things matter here:
        #  1. Normalize with datetime() so the ordering doesn't compare mismatched
        #     string formats — posted_at is ISO ("2026-07-31T06:00:00", 'T'
        #     separator) while first_seen_at is SQLite's space format
        #     ("2026-07-31 13:00:00"). Raw string ORDER BY put ' ' before 'T',
        #     scrambling the order (a job found now sorting above one posted
        #     hours earlier). datetime() canonicalizes both.
        #  2. Use the posted DATE even when it's date-only — do NOT fall back to
        #     first_seen_at (scrape time) for those. Otherwise a job posted days
        #     ago but scraped just now looks brand new and jumps to the top. Only
        #     when there is no posted_at at all do we use first_seen_at.
        # first_seen_at is the secondary key so same-posted-time jobs show
        # newest-found first.
        if sort_by == "posted_at":
            # Effective time, newest first:
            #  - precise posted_at (has a real clock time) → use it directly.
            #  - date-only posted_at (Indeed etc., stored as ...T00:00:00) → we
            #    don't know the hour, so estimate with first_seen_at (when we
            #    found it) BUT cap it at the posting day + 1. That keeps a job
            #    posted TODAY ranked fresh (found-time), while a job posted days
            #    ago but scraped just now is capped to its posted date and sinks.
            #  - no posted_at at all → first_seen_at.
            # datetime() normalizes ISO 'T' vs space formats so ordering is right.
            order_expr = (
                "CASE "
                "WHEN posted_at != '' AND posted_at IS NOT NULL AND posted_at NOT LIKE '%T00:00:00%' "
                "  THEN datetime(posted_at) "
                "WHEN posted_at != '' AND posted_at IS NOT NULL "
                "  THEN MIN(datetime(first_seen_at), datetime(posted_at, '+1 day')) "
                "ELSE datetime(first_seen_at) "
                f"END {sort_dir}, datetime(first_seen_at) {sort_dir}"
            )
        elif sort_by == "first_seen_at":
            # "Newest found" — nothing we just discovered gets buried. A job
            # posted a while ago but found seconds ago is new information to the
            # user, so fetch time leads and posted time only breaks ties.
            order_expr = (f"datetime(first_seen_at) {sort_dir}, "
                          f"datetime(NULLIF(posted_at,'')) {sort_dir}")
        else:
            order_expr = f"{sort_by} {sort_dir}"

        query = f"""
            SELECT * FROM jobs
            WHERE {where}
            ORDER BY {order_expr}
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def get_job_count(hours: int = 24) -> dict:
    db = await get_db()
    try:
        # Filtered stats (for the current time window)
        cursor = await db.execute(
            """SELECT
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END), 0) as new_count,
                COALESCE(SUM(CASE WHEN status = 'viewed' THEN 1 ELSE 0 END), 0) as viewed_count,
                COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0) as applied_count,
                COALESCE(SUM(CASE WHEN status = 'saved' THEN 1 ELSE 0 END), 0) as saved_count,
                COALESCE(SUM(CASE WHEN status = 'hidden' THEN 1 ELSE 0 END), 0) as hidden_count,
                COALESCE(SUM(CASE WHEN is_direct_apply = 1 THEN 1 ELSE 0 END), 0) as direct_count
            FROM jobs WHERE first_seen_at >= datetime('now', ?)""",
            (f"-{hours} hours",),
        )
        row = await cursor.fetchone()
        result = dict(row) if row else {"total": 0, "new_count": 0, "viewed_count": 0, "applied_count": 0, "saved_count": 0, "hidden_count": 0, "direct_count": 0}

        # All-time total (never goes down)
        cursor2 = await db.execute("SELECT COUNT(*) as all_total FROM jobs")
        row2 = await cursor2.fetchone()
        result["all_total"] = row2["all_total"] if row2 else 0

        return result
    finally:
        await db.close()


async def update_job_status(job_id: int, status: str):
    """Serialised against the other writers — see _write_lock()."""
    async with _write_lock():
        return await _update_job_status_unlocked(job_id, status)


async def _update_job_status_unlocked(job_id: int, status: str):
    db = await get_db()
    try:
        if status == "applied":
            await db.execute(
                "UPDATE jobs SET status = ?, applied_at = datetime('now') WHERE id = ?",
                (status, job_id),
            )
        else:
            await db.execute(
                "UPDATE jobs SET status = ?, applied_at = '' WHERE id = ?",
                (status, job_id),
            )
        await db.commit()
    finally:
        await db.close()


async def update_job_scores(job_id: int, relevance: int, trust: int, hide: bool = False):
    """Serialised against the other writers — see _write_lock()."""
    async with _write_lock():
        return await _update_job_scores_unlocked(job_id, relevance, trust, hide)


async def _update_job_scores_unlocked(job_id: int, relevance: int, trust: int, hide: bool = False):
    db = await get_db()
    try:
        # Stamp scored_at so this job is never re-scored through AI again.
        # This is the core cost fix — see get_unscored_jobs().
        # hide=True: also set status='hidden' so a clearly-wrong role drops off
        # the board (reversible — status only, not deleted). Never re-hide a job
        # the user has already acted on (saved/applied).
        if hide:
            # Below the relevance cutoff → hide, unless the user already acted on it.
            await db.execute(
                "UPDATE jobs SET relevance_score = ?, trust_score = ?, "
                "scored_at = datetime('now'), "
                "status = CASE WHEN status IN ('saved','applied') THEN status ELSE 'hidden' END "
                "WHERE id = ?",
                (relevance, trust, job_id),
            )
        else:
            # At/above cutoff → restore an auto-hidden job back to 'new' (so a
            # re-classify that now likes a job un-hides it). No-op for jobs that
            # aren't hidden. Never touches saved/applied/viewed.
            await db.execute(
                "UPDATE jobs SET relevance_score = ?, trust_score = ?, "
                "scored_at = datetime('now'), "
                "status = CASE WHEN status = 'hidden' THEN 'new' ELSE status END "
                "WHERE id = ?",
                (relevance, trust, job_id),
            )
        await db.commit()
    finally:
        await db.close()


async def get_unscored_jobs(limit: int = 400) -> list[dict]:
    """Return jobs that have NEVER been scored (scored_at empty/null).

    This replaces the old `get_jobs(status='new')` scoring query, which
    re-fetched the same up-to-500 jobs every cycle and re-ran Haiku on each
    one for up to 72h. With scored_at, each job is AI-scored exactly once.
    """
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT * FROM jobs
            WHERE (scored_at IS NULL OR scored_at = '')
              AND status = 'new'
            ORDER BY first_seen_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_jobs_for_reclassify(limit: int = 6000) -> list[dict]:
    """All jobs eligible for re-classification — excludes user-owned states
    (saved/applied) and archived. Includes currently-hidden jobs so a re-run can
    restore ones that now score well."""
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM jobs WHERE status IN ('new','viewed','hidden') "
            "ORDER BY first_seen_at DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_discovered_companies() -> list[dict]:
    """Companies found by the discovery bot, in the shape the ATS scraper expects."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT slug, ats, name, tenant, wd, site FROM discovered_companies"
        )
        rows = await cursor.fetchall()
        out = []
        for r in rows:
            c = {"slug": r["slug"], "ats": r["ats"], "name": r["name"] or r["slug"]}
            if r["ats"] == "workday":
                c["tenant"] = r["tenant"]
                c["wd"] = r["wd"]
                c["site"] = r["site"]
            out.append(c)
        return out
    finally:
        await db.close()


async def add_discovered_company(c: dict) -> bool:
    """Serialised against the other writers — see _write_lock()."""
    async with _write_lock():
        return await _add_discovered_company_unlocked(c)


async def _add_discovered_company_unlocked(c: dict) -> bool:
    """Insert a newly-verified company (idempotent on slug+ats)."""
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO discovered_companies "
            "(slug, ats, name, tenant, wd, site, jobs_seen) VALUES (?,?,?,?,?,?,?)",
            (c["slug"], c["ats"], c.get("name", ""), c.get("tenant", ""),
             c.get("wd", ""), c.get("site", ""), int(c.get("jobs_seen", 0))),
        )
        await db.commit()
        return True
    finally:
        await db.close()


async def count_discovered_companies() -> int:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) AS n FROM discovered_companies")
        row = await cursor.fetchone()
        return int(row["n"]) if row else 0
    finally:
        await db.close()


async def get_job_by_id(job_id: int) -> Optional[dict]:
    """Fetch a single job row by id (used by the 'Prep for this job' loop)."""
    db = await get_db()
    try:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


# --- Search Profiles ---

async def create_profile(data: dict) -> int:
    db = await get_db()
    try:
        sig = data.get("skill_signature") or {}
        if not isinstance(sig, dict):
            sig = {}
        cursor = await db.execute(
            """INSERT INTO search_profiles (title, expanded_titles, keywords, excluded_keywords,
               locations, remote_only, min_salary, freshness_hours, min_relevance, min_trust,
               skill_signature)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["title"],
                json.dumps(data.get("expanded_titles", [])),
                json.dumps(data.get("keywords", [])),
                json.dumps(data.get("excluded_keywords", [])),
                json.dumps(data.get("locations", [])),
                1 if data.get("remote_only") else 0,
                data.get("min_salary", 0),
                data.get("freshness_hours", 24),
                data.get("min_relevance", 0),
                data.get("min_trust", 0),
                json.dumps(sig),
            ),
        )
        await db.commit()
        return cursor.lastrowid
    finally:
        await db.close()


async def get_profiles() -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM search_profiles WHERE is_active = 1")
        rows = await cursor.fetchall()
        profiles = []
        for row in rows:
            p = dict(row)
            p["expanded_titles"] = json.loads(p["expanded_titles"])
            p["keywords"] = json.loads(p["keywords"])
            p["excluded_keywords"] = json.loads(p["excluded_keywords"])
            p["locations"] = json.loads(p["locations"])
            # v1.9.6: parse skill_signature if present
            try:
                p["skill_signature"] = json.loads(p.get("skill_signature") or "{}")
            except Exception:
                p["skill_signature"] = {}
            profiles.append(p)
        return profiles
    finally:
        await db.close()


async def update_profile(profile_id: int, data: dict):
    db = await get_db()
    try:
        # Build dynamic UPDATE — only set fields that were provided
        sets = []
        vals = []
        field_map = {
            "title": ("title", lambda v: v),
            "expanded_titles": ("expanded_titles", lambda v: json.dumps(v) if isinstance(v, list) else v),
            "keywords": ("keywords", lambda v: json.dumps(v) if isinstance(v, list) else v),
            "excluded_keywords": ("excluded_keywords", lambda v: json.dumps(v) if isinstance(v, list) else v),
            "locations": ("locations", lambda v: json.dumps(v) if isinstance(v, list) else v),
            "remote_only": ("remote_only", lambda v: 1 if v else 0),
            "min_salary": ("min_salary", lambda v: v),
            "freshness_hours": ("freshness_hours", lambda v: v),
            "min_relevance": ("min_relevance", lambda v: v),
            "min_trust": ("min_trust", lambda v: v),
            "skill_signature": ("skill_signature", lambda v: json.dumps(v) if isinstance(v, dict) else (v or "{}")),
        }
        for key, (col, transform) in field_map.items():
            if key in data:
                sets.append(f"{col} = ?")
                vals.append(transform(data[key]))

        if not sets:
            return  # nothing to update

        vals.append(profile_id)
        await db.execute(
            f"UPDATE search_profiles SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        await db.commit()
    finally:
        await db.close()


async def delete_profile(profile_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE search_profiles SET is_active = 0 WHERE id = ?", (profile_id,))
        # Hard-delete the profile's jobs so they don't linger as ghost data
        await db.execute("DELETE FROM jobs WHERE search_profile_id = ?", (profile_id,))
        await db.commit()
    finally:
        await db.close()


# --- Data Retention / Cleanup ---

# Hot board, not a backlog: anything older than this is deleted outright. There
# is no archive step any more — copying rows to jobs_archive only moved the disk
# cost instead of releasing it.
RETENTION_DAYS = 3
# Kept as aliases so any older caller/import keeps working.
ARCHIVE_AFTER_DAYS = RETENTION_DAYS
PURGE_AFTER_DAYS = RETENTION_DAYS


async def init_archive_table():
    """Create the archive table (same schema as jobs) if it doesn't exist."""
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs_archive (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                title TEXT DEFAULT '',
                company_name TEXT DEFAULT '',
                company_domain TEXT DEFAULT '',
                location TEXT DEFAULT '',
                is_remote INTEGER DEFAULT 0,
                work_type TEXT DEFAULT 'onsite',
                description TEXT DEFAULT '',
                salary_min INTEGER DEFAULT 0,
                salary_max INTEGER DEFAULT 0,
                source TEXT DEFAULT '',
                source_url TEXT DEFAULT '',
                direct_apply_url TEXT DEFAULT '',
                posted_at TEXT DEFAULT '',
                first_seen_at TEXT DEFAULT '',
                relevance_score INTEGER DEFAULT 50,
                trust_score INTEGER DEFAULT 50,
                is_direct_apply INTEGER DEFAULT 0,
                status TEXT DEFAULT 'new',
                search_profile_id INTEGER,
                archived_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_archive_first_seen ON jobs_archive(first_seen_at);
            CREATE INDEX IF NOT EXISTS idx_archive_archived_at ON jobs_archive(archived_at);
        """)
        await db.commit()

        # Migration: add salary_period column to archive table if missing (v2.0.0)
        try:
            await db.execute("SELECT salary_period FROM jobs_archive LIMIT 1")
        except Exception:
            logger.info("[Migration] Adding salary_period column to jobs_archive table")
            await db.execute("ALTER TABLE jobs_archive ADD COLUMN salary_period TEXT DEFAULT 'yearly'")
            await db.commit()
    finally:
        await db.close()


async def cleanup_old_jobs() -> dict:
    """Delete jobs older than RETENTION_DAYS. No archive step.

    Jobs the user has SAVED or APPLIED to are never deleted — losing an
    application history to a disk cleanup would be far worse than the few KB it
    costs to keep. Everything else is a stale listing on a hot board.
    """
    db = await get_db()
    try:
        cutoff = f"-{RETENTION_DAYS} days"
        keep = ("saved", "applied")

        cur = await db.execute(
            "SELECT COUNT(*) FROM jobs WHERE first_seen_at < datetime('now', ?) "
            "AND status NOT IN (?, ?)",
            (cutoff, *keep),
        )
        stale = (await cur.fetchone())[0]

        if stale:
            await db.execute(
                "DELETE FROM jobs WHERE first_seen_at < datetime('now', ?) "
                "AND status NOT IN (?, ?)",
                (cutoff, *keep),
            )

        # Drain anything the old archive-then-purge flow left behind.
        leftover = 0
        try:
            cur = await db.execute("SELECT COUNT(*) FROM jobs_archive")
            leftover = (await cur.fetchone())[0]
            if leftover:
                await db.execute("DELETE FROM jobs_archive")
        except Exception:
            leftover = 0

        await db.commit()

        active = (await (await db.execute("SELECT COUNT(*) FROM jobs")).fetchone())[0]
        kept = (await (await db.execute(
            "SELECT COUNT(*) FROM jobs WHERE status IN (?, ?)", keep)).fetchone())[0]

        return {
            "archived": 0,          # kept for the existing log line
            "purged": stale + leftover,
            "deleted": stale,
            "archive_drained": leftover,
            "protected_saved_applied": kept,
            "active_jobs": active,
            "archived_jobs": 0,
        }
    finally:
        await db.close()


# Below this much free space the volume is effectively full: SQLite starts
# raising "database or disk is full" and inserts begin failing.
CRITICAL_FREE_MB = 60
# Descriptions dominate row size. The scorer only ever reads the first ~3k
# chars, so anything beyond this is stored for nothing.
MAX_DESCRIPTION_CHARS = 4000


async def _compact_via_tmp(log: list) -> bool:
    """Last resort when the volume has no room for an in-place VACUUM.

    VACUUM INTO writes a compacted copy to the container's own disk (/tmp is a
    different filesystem from the mounted volume). The copy is verified, and
    only then is the bloated original replaced — nothing is removed until the
    replacement passes an integrity check AND a row-count match.
    """
    import shutil, tempfile
    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, "scoutpilot_compact.db")
    for leftover in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
        try:
            os.remove(leftover)
        except OSError:
            pass

    # 1. Size the job; make sure the scratch filesystem can actually hold it.
    src = await aiosqlite.connect(DB_PATH)
    try:
        page_size = (await (await src.execute("PRAGMA page_size")).fetchone())[0]
        page_count = (await (await src.execute("PRAGMA page_count")).fetchone())[0]
        freelist = (await (await src.execute("PRAGMA freelist_count")).fetchone())[0]
        live_bytes = page_size * max(page_count - freelist, 0)
        expected_jobs = (await (await src.execute("SELECT COUNT(*) FROM jobs")).fetchone())[0]

        tmp_free = shutil.disk_usage(tmp_dir).free
        if tmp_free < live_bytes * 1.3:
            log.append(f"compact-via-tmp skipped — {tmp_dir} has "
                       f"{tmp_free/1e6:.0f}MB, needs ~{live_bytes*1.3/1e6:.0f}MB")
            return False

        log.append(f"compacting {live_bytes/1e6:.0f}MB of live data into {tmp_dir}")
        await src.execute("VACUUM INTO ?", (tmp_path,))
        await src.commit()
    finally:
        await src.close()

    # 2. Verify the copy BEFORE touching the original.
    chk = await aiosqlite.connect(tmp_path)
    try:
        integrity = (await (await chk.execute("PRAGMA integrity_check")).fetchone())[0]
        copy_jobs = (await (await chk.execute("SELECT COUNT(*) FROM jobs")).fetchone())[0]
    finally:
        await chk.close()

    if integrity != "ok" or copy_jobs != expected_jobs:
        log.append(f"compact ABORTED — integrity={integrity}, "
                   f"jobs {copy_jobs} != {expected_jobs}; original untouched")
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return False

    log.append(f"verified copy: integrity=ok, jobs={copy_jobs}, "
               f"{os.path.getsize(tmp_path)/1e6:.0f}MB")

    # 3. Free the volume, then move the verified copy into place.
    for suffix in ("-wal", "-shm", ""):
        try:
            os.remove(DB_PATH + suffix)
        except OSError:
            pass
    shutil.move(tmp_path, DB_PATH)
    log.append(f"replaced database — now {os.path.getsize(DB_PATH)/1e6:.0f}MB")
    return True


async def emergency_reclaim(dry_run: bool = False) -> dict:
    """Recover a volume that is too full for a normal VACUUM to even run.

    Ordered cheapest-and-safest first, re-checking free space after each step
    and stopping as soon as the volume is healthy again:

      1. Truncate the WAL.
      2. Purge archived jobs past the retention window.
      3. Trim over-long descriptions (the bulk of row size).
      4. Purge the archive entirely.
      5. VACUUM, which needs roughly the compacted size free — which is why it
         has to come last, after the earlier steps have made room.
    """
    import shutil
    d = os.path.dirname(DB_PATH) or "."

    def free_mb() -> float:
        return shutil.disk_usage(d).free / 1e6

    log: list[str] = []
    start_free = free_mb()
    start_size = os.path.getsize(DB_PATH) / 1e6 if os.path.exists(DB_PATH) else 0
    result = {"start_free_mb": round(start_free, 1),
              "start_db_mb": round(start_size, 1),
              "steps": log, "dry_run": dry_run}

    if start_free >= CRITICAL_FREE_MB:
        log.append(f"healthy — {start_free:.1f}MB free, nothing to do")
        result["end_free_mb"] = round(start_free, 1)
        return result

    if dry_run:
        log.append("dry run — would purge archive, trim descriptions, vacuum")
        return result

    db = await get_db()
    try:
        async def scalar(sql, args=()):
            return (await (await db.execute(sql, args)).fetchone())[0]

        # 1. WAL truncate — frees whatever the write-ahead log is holding.
        try:
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await db.commit()
            log.append(f"wal_checkpoint(TRUNCATE) -> {free_mb():.1f}MB free")
            # TRUNCATE silently no-ops while another connection holds a read
            # lock, and this app runs ~10 concurrent workers. Cycling the
            # journal mode removes the WAL file outright; WAL is restored after.
            if free_mb() < CRITICAL_FREE_MB:
                await db.execute("PRAGMA journal_mode=DELETE")
                await db.commit()
                await db.execute("PRAGMA journal_mode=WAL")
                await db.commit()
                log.append(f"journal_mode cycle -> {free_mb():.1f}MB free")
        except Exception as e:
            log.append(f"wal_checkpoint failed: {e}")

        # 2. Purge archive past the retention window.
        try:
            n = await scalar(
                "SELECT COUNT(*) FROM jobs_archive WHERE archived_at < datetime('now', ?)",
                (f"-{PURGE_AFTER_DAYS} days",))
            if n:
                await db.execute(
                    "DELETE FROM jobs_archive WHERE archived_at < datetime('now', ?)",
                    (f"-{PURGE_AFTER_DAYS} days",))
                await db.commit()
                log.append(f"purged {n} archived jobs older than {PURGE_AFTER_DAYS}d")
        except Exception as e:
            log.append(f"retention purge failed: {e}")

        # 3. Trim over-long descriptions in both tables.
        try:
            for table in ("jobs", "jobs_archive"):
                try:
                    n = await scalar(
                        f"SELECT COUNT(*) FROM {table} WHERE LENGTH(description) > ?",
                        (MAX_DESCRIPTION_CHARS,))
                    if n:
                        await db.execute(
                            f"UPDATE {table} SET description = substr(description, 1, ?) "
                            f"WHERE LENGTH(description) > ?",
                            (MAX_DESCRIPTION_CHARS, MAX_DESCRIPTION_CHARS))
                        await db.commit()
                        log.append(f"trimmed {n} {table} descriptions to {MAX_DESCRIPTION_CHARS} chars")
                except Exception as e:
                    log.append(f"trim {table} failed: {e}")
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await db.commit()
        except Exception as e:
            log.append(f"trim phase failed: {e}")

        # 4. Still critical -> drop the archive entirely. Archived rows are
        #    stale listings on a hot board; live `jobs` is never touched.
        try:
            if free_mb() < CRITICAL_FREE_MB:
                n = await scalar("SELECT COUNT(*) FROM jobs_archive")
                if n:
                    await db.execute("DELETE FROM jobs_archive")
                    await db.commit()
                    await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    await db.commit()
                    log.append(f"emergency: dropped all {n} archived jobs")
        except Exception as e:
            log.append(f"archive drop failed: {e}")
    finally:
        await db.close()

    # 5. VACUUM last — the steps above are what make room for it.
    try:
        db_size = os.path.getsize(DB_PATH)
        # Freed pages are reused by the rebuild, so the compacted file is much
        # smaller than the current one; require headroom against live content.
        vdb = await aiosqlite.connect(DB_PATH)
        try:
            page_size = (await (await vdb.execute("PRAGMA page_size")).fetchone())[0]
            page_count = (await (await vdb.execute("PRAGMA page_count")).fetchone())[0]
            freelist = (await (await vdb.execute("PRAGMA freelist_count")).fetchone())[0]
            live_bytes = page_size * max(page_count - freelist, 0)
            need = live_bytes * 1.1
            if free_mb() * 1e6 > need:
                await vdb.execute("VACUUM")
                await vdb.commit()
                log.append(f"VACUUM ok (needed ~{need/1e6:.0f}MB)")
            else:
                log.append(f"in-place VACUUM needs ~{need/1e6:.0f}MB but only "
                           f"{free_mb():.0f}MB free — compacting via temp disk")
                await vdb.close()
                await _compact_via_tmp(log)
                vdb = await aiosqlite.connect(DB_PATH)
        finally:
            await vdb.close()
        result["end_db_mb"] = round(os.path.getsize(DB_PATH) / 1e6, 1)
        result["shrunk_mb"] = round(start_size - result["end_db_mb"], 1)
    except Exception as e:
        log.append(f"vacuum phase failed: {e}")

    result["end_free_mb"] = round(free_mb(), 1)
    result["freed_mb"] = round(result["end_free_mb"] - start_free, 1)
    return result


async def storage_stats() -> dict:
    """Disk + SQLite space accounting for the volume the DB lives on."""
    import shutil
    out: dict = {}
    try:
        d = os.path.dirname(DB_PATH) or "."
        total, used, free = shutil.disk_usage(d)
        out["disk"] = {
            "mount": d,
            "total_mb": round(total / 1e6, 1),
            "used_mb": round(used / 1e6, 1),
            "free_mb": round(free / 1e6, 1),
            "pct_used": round(used / total * 100, 1) if total else None,
        }
    except Exception as e:
        out["disk"] = {"error": str(e)}

    files = {}
    for suffix in ("", "-wal", "-shm"):
        try:
            files[os.path.basename(DB_PATH) + suffix] = round(
                os.path.getsize(DB_PATH + suffix) / 1e6, 1)
        except OSError:
            pass
    out["files_mb"] = files

    db = await get_db()
    try:
        async def one(sql):
            return (await (await db.execute(sql)).fetchone())[0]
        page_size = await one("PRAGMA page_size")
        page_count = await one("PRAGMA page_count")
        freelist = await one("PRAGMA freelist_count")
        out["sqlite"] = {
            "page_size": page_size,
            "page_count": page_count,
            "freelist_pages": freelist,
            "logical_mb": round(page_size * page_count / 1e6, 1),
            # Space already deleted but never returned to the filesystem.
            "reclaimable_mb": round(page_size * freelist / 1e6, 1),
            "auto_vacuum": await one("PRAGMA auto_vacuum"),
        }
        counts = {}
        for t in ("jobs", "jobs_archive", "discovered_companies"):
            try:
                counts[t] = await one(f"SELECT COUNT(*) FROM {t}")
            except Exception:
                counts[t] = None
        out["rows"] = counts
        try:
            out["description_mb"] = round(
                (await one("SELECT COALESCE(SUM(LENGTH(description)),0) FROM jobs")
                 + await one("SELECT COALESCE(SUM(LENGTH(description)),0) FROM jobs_archive")
                 ) / 1e6, 1)
        except Exception:
            pass
        out["retention"] = {
            "archive_after_days": ARCHIVE_AFTER_DAYS,
            "purge_after_days": PURGE_AFTER_DAYS,
        }
    finally:
        await db.close()
    return out


async def reclaim_space(force_vacuum: bool = False) -> dict:
    """Return dead space to the filesystem, cheapest step first.

    A full VACUUM rebuilds the database and needs free disk roughly equal to
    the DB size. On a volume that is already nearly full that would fail, so it
    only runs when there is comfortable headroom (or force_vacuum=True).
    """
    import shutil
    result: dict = {"steps": [], "errors": []}
    before = {}
    try:
        before = {k: os.path.getsize(DB_PATH + k) for k in ("", "-wal")
                  if os.path.exists(DB_PATH + k)}
    except OSError:
        pass

    db = await get_db()
    try:
        # 1. Checkpoint + truncate the WAL. Cheap, no extra space needed.
        try:
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await db.commit()
            result["steps"].append("wal_checkpoint(TRUNCATE)")
        except Exception as e:
            result["errors"].append(f"wal_checkpoint: {e}")

        # 2. Incremental vacuum, only meaningful when auto_vacuum=INCREMENTAL.
        try:
            av = (await (await db.execute("PRAGMA auto_vacuum")).fetchone())[0]
            if av == 2:
                await db.execute("PRAGMA incremental_vacuum")
                await db.commit()
                result["steps"].append("incremental_vacuum")
            result["auto_vacuum"] = av
        except Exception as e:
            result["errors"].append(f"incremental_vacuum: {e}")
    finally:
        await db.close()

    # 3. Full VACUUM only with headroom — it needs ~1x the DB size free.
    try:
        d = os.path.dirname(DB_PATH) or "."
        free = shutil.disk_usage(d).free
        db_size = os.path.getsize(DB_PATH)
        result["free_mb"] = round(free / 1e6, 1)
        result["db_mb"] = round(db_size / 1e6, 1)
        if force_vacuum or free > db_size * 1.2:
            vdb = await aiosqlite.connect(DB_PATH)
            try:
                await vdb.execute("VACUUM")
                await vdb.commit()
                result["steps"].append("VACUUM")
            finally:
                await vdb.close()
        else:
            result["steps"].append(
                f"VACUUM SKIPPED — needs ~{round(db_size*1.2/1e6)}MB free, "
                f"only {round(free/1e6)}MB available")
    except Exception as e:
        result["errors"].append(f"vacuum: {e}")

    try:
        after = {k: os.path.getsize(DB_PATH + k) for k in ("", "-wal")
                 if os.path.exists(DB_PATH + k)}
        result["freed_mb"] = round(
            (sum(before.values()) - sum(after.values())) / 1e6, 1)
    except OSError:
        pass
    return result


# --- Source Settings ---

# Master list of all sources with their metadata
ALL_SOURCES = [
    {"source_key": "indeed",        "display_name": "Indeed",          "category": "jobspy",  "requires_key": ""},
    {"source_key": "linkedin",      "display_name": "LinkedIn",        "category": "jobspy",  "requires_key": ""},
    {"source_key": "google",        "display_name": "Google Jobs",     "category": "api",     "requires_key": "SERPAPI_KEY"},
    {"source_key": "jsearch",       "display_name": "JSearch",         "category": "api",     "requires_key": "RAPIDAPI_KEY"},
    {"source_key": "jooble",        "display_name": "Jooble",          "category": "api",     "requires_key": "JOOBLE_API_KEY"},
    {"source_key": "adzuna",        "display_name": "Adzuna",          "category": "api",     "requires_key": "ADZUNA_APP_ID"},
    {"source_key": "careerjet",     "display_name": "CareerJet",       "category": "api",     "requires_key": "CAREERJET_AFFID"},
    {"source_key": "findwork",      "display_name": "FindWork",        "category": "api",     "requires_key": "FINDWORK_TOKEN"},
    {"source_key": "usajobs",       "display_name": "USAJobs",         "category": "api",     "requires_key": "USAJOBS_API_KEY"},
    {"source_key": "remotive",      "display_name": "Remotive",        "category": "free",    "requires_key": ""},
    {"source_key": "remoteok",      "display_name": "RemoteOK",        "category": "free",    "requires_key": ""},
    {"source_key": "weworkremotely","display_name": "WeWorkRemotely",  "category": "free",    "requires_key": ""},
    {"source_key": "themuse",       "display_name": "TheMuse",         "category": "free",    "requires_key": ""},
    {"source_key": "jobicy",        "display_name": "Jobicy",          "category": "free",    "requires_key": ""},
    {"source_key": "himalayas",     "display_name": "Himalayas",       "category": "free",    "requires_key": ""},
    {"source_key": "arbeitnow",     "display_name": "Arbeitnow",       "category": "free",    "requires_key": ""},
    {"source_key": "jobicy_rss",    "display_name": "Jobicy RSS",      "category": "rss",     "requires_key": ""},
    {"source_key": "himalayas_rss", "display_name": "Himalayas RSS",   "category": "rss",     "requires_key": ""},
]

# ATS sources — enabled by default (free, no API keys needed)
ATS_SOURCES = [
    {"source_key": "greenhouse",      "display_name": "Greenhouse (ATS)",      "category": "ats", "requires_key": ""},
    {"source_key": "lever",           "display_name": "Lever (ATS)",           "category": "ats", "requires_key": ""},
    {"source_key": "ashby",           "display_name": "Ashby (ATS)",           "category": "ats", "requires_key": ""},
    {"source_key": "workday",         "display_name": "Workday (ATS)",         "category": "ats", "requires_key": ""},
    {"source_key": "smartrecruiters", "display_name": "SmartRecruiters (ATS)", "category": "ats", "requires_key": ""},
    {"source_key": "workable",        "display_name": "Workable (ATS)",        "category": "ats", "requires_key": ""},
    {"source_key": "recruitee",       "display_name": "Recruitee (ATS)",       "category": "ats", "requires_key": ""},
    {"source_key": "breezy",          "display_name": "Breezy HR (ATS)",       "category": "ats", "requires_key": ""},
]


async def init_source_settings():
    """Seed the source_settings table with all known sources (skip existing rows)."""
    db = await get_db()
    try:
        for src in ALL_SOURCES:
            await db.execute(
                """INSERT OR IGNORE INTO source_settings (source_key, display_name, enabled, category, requires_key)
                   VALUES (?, ?, 1, ?, ?)""",
                (src["source_key"], src["display_name"], src["category"], src["requires_key"]),
            )
        # ATS sources: enabled by default (free, no API keys needed)
        for src in ATS_SOURCES:
            await db.execute(
                """INSERT OR IGNORE INTO source_settings (source_key, display_name, enabled, category, requires_key)
                   VALUES (?, ?, 1, ?, ?)""",
                (src["source_key"], src["display_name"], src["category"], src["requires_key"]),
            )
        await db.commit()
    finally:
        await db.close()


async def get_source_settings() -> list[dict]:
    """Return all source settings sorted by category then name."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM source_settings ORDER BY category, display_name"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def is_source_enabled(source_key: str) -> bool:
    """Check if a source is enabled. Returns True if not found (default on)."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT enabled FROM source_settings WHERE source_key = ?", (source_key,)
        )
        row = await cursor.fetchone()
        return bool(row["enabled"]) if row else True
    finally:
        await db.close()


async def get_enabled_sources() -> set[str]:
    """Return a set of all enabled source keys for fast lookup."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT source_key FROM source_settings WHERE enabled = 1"
        )
        rows = await cursor.fetchall()
        return {row["source_key"] for row in rows}
    finally:
        await db.close()


async def update_source_setting(source_key: str, enabled: bool):
    """Enable or disable a source."""
    db = await get_db()
    try:
        await db.execute(
            "UPDATE source_settings SET enabled = ?, updated_at = datetime('now') WHERE source_key = ?",
            (1 if enabled else 0, source_key),
        )
        await db.commit()
    finally:
        await db.close()


async def bulk_update_source_settings(settings_map: dict[str, bool]):
    """Update multiple source settings at once. settings_map = {source_key: enabled}."""
    db = await get_db()
    try:
        for source_key, enabled in settings_map.items():
            await db.execute(
                "UPDATE source_settings SET enabled = ?, updated_at = datetime('now') WHERE source_key = ?",
                (1 if enabled else 0, source_key),
            )
        await db.commit()
    finally:
        await db.close()


async def get_retention_stats() -> dict:
    """Get current data retention stats."""
    db = await get_db()
    try:
        active = (await (await db.execute("SELECT COUNT(*) FROM jobs")).fetchone())[0]

        # Archive table may not exist yet
        try:
            archived = (await (await db.execute("SELECT COUNT(*) FROM jobs_archive")).fetchone())[0]
        except Exception:
            archived = 0

        oldest = await (await db.execute("SELECT MIN(first_seen_at) FROM jobs")).fetchone()
        newest = await (await db.execute("SELECT MAX(first_seen_at) FROM jobs")).fetchone()

        return {
            "active_jobs": active,
            "archived_jobs": archived,
            "oldest_active": oldest[0] if oldest else None,
            "newest_active": newest[0] if newest else None,
            "archive_after_days": ARCHIVE_AFTER_DAYS,
            "purge_after_days": PURGE_AFTER_DAYS,
        }
    finally:
        await db.close()
