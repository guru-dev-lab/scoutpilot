import aiosqlite
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


async def insert_job(job_data: dict) -> bool:
    """Insert a job if it doesn't already exist (exact hash + fuzzy title check). Returns True if inserted."""
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
                # Borderline fuzzy (70-87): ask AI to confirm if it's a duplicate
                if 70 <= score < FUZZY_TITLE_THRESHOLD:
                    try:
                        from ai_engine import ai_is_duplicate
                        is_dup = await ai_is_duplicate(
                            job_data.get("title", ""), job_data.get("company_name", ""),
                            row[1] or "", row[2] or "", score,
                        )
                        if is_dup:
                            return False
                    except Exception as e:
                        logger.debug(f"[Dedup] AI check failed: {e}")

        now = datetime.now(timezone.utc).isoformat()
        # If no posted_at from source, fall back to now (scrape time)
        posted_at = job_data.get("posted_at", "") or now
        skills = extract_skills(job_data.get("title", ""), job_data.get("description", "")) or "_none"
        await db.execute(
            """INSERT INTO jobs (hash, title, company_name, company_domain, location,
               is_remote, work_type, description, salary_min, salary_max, source, source_url,
               direct_apply_url, posted_at, first_seen_at, relevance_score, trust_score,
               is_direct_apply, skills, status, search_profile_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                h,
                job_data.get("title", ""),
                job_data.get("company_name", ""),
                job_data.get("company_domain", ""),
                job_data.get("location", ""),
                1 if job_data.get("is_remote") else 0,
                job_data.get("work_type", "onsite"),
                job_data.get("description", ""),
                job_data.get("salary_min", 0),
                job_data.get("salary_max", 0),
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
    sort_by: str = "first_seen_at",
    sort_dir: str = "DESC",
    limit: int = 200,
    offset: int = 0,
    search: str = "",
    direct_only: bool = False,
    location: str = "",
    skill: str = "",
) -> list[dict]:
    db = await get_db()
    try:
        conditions = []
        params = []

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

        # For posted_at sort, use COALESCE to fall back to first_seen_at
        # so jobs without a posted date don't float to the top
        if sort_by == "posted_at":
            order_expr = f"COALESCE(NULLIF(posted_at, ''), first_seen_at) {sort_dir}"
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
    db = await get_db()
    try:
        await db.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
        await db.commit()
    finally:
        await db.close()


async def update_job_scores(job_id: int, relevance: int, trust: int):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE jobs SET relevance_score = ?, trust_score = ? WHERE id = ?",
            (relevance, trust, job_id),
        )
        await db.commit()
    finally:
        await db.close()


# --- Search Profiles ---

async def create_profile(data: dict) -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO search_profiles (title, expanded_titles, keywords, excluded_keywords,
               locations, remote_only, min_salary, freshness_hours, min_relevance, min_trust)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            profiles.append(p)
        return profiles
    finally:
        await db.close()


async def update_profile(profile_id: int, data: dict):
    db = await get_db()
    try:
        await db.execute(
            """UPDATE search_profiles SET title=?, expanded_titles=?, keywords=?,
               excluded_keywords=?, locations=?, remote_only=?, min_salary=?,
               freshness_hours=?, min_relevance=?, min_trust=?
               WHERE id=?""",
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
                profile_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_profile(profile_id: int):
    db = await get_db()
    try:
        await db.execute("UPDATE search_profiles SET is_active = 0 WHERE id = ?", (profile_id,))
        await db.commit()
    finally:
        await db.close()


# --- Data Retention / Cleanup ---

ARCHIVE_AFTER_DAYS = 5    # Move jobs older than 5 days to archive
PURGE_AFTER_DAYS = 90     # Delete archived jobs older than this


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
    finally:
        await db.close()


async def cleanup_old_jobs() -> dict:
    """Move stale jobs to archive and purge ancient archived jobs.
    Returns stats on what was moved/deleted."""
    db = await get_db()
    try:
        # 1. Count what we're about to archive
        cursor = await db.execute(
            "SELECT COUNT(*) FROM jobs WHERE first_seen_at < datetime('now', ?)",
            (f"-{ARCHIVE_AFTER_DAYS} days",),
        )
        archive_count = (await cursor.fetchone())[0]

        # 2. Move old jobs to archive (INSERT OR IGNORE to skip dupes)
        if archive_count > 0:
            await db.execute(
                """INSERT OR IGNORE INTO jobs_archive
                   (id, hash, title, company_name, company_domain, location,
                    is_remote, work_type, description, salary_min, salary_max,
                    source, source_url, direct_apply_url, posted_at, first_seen_at,
                    relevance_score, trust_score, is_direct_apply, status, search_profile_id)
                   SELECT id, hash, title, company_name, company_domain, location,
                          is_remote, work_type, description, salary_min, salary_max,
                          source, source_url, direct_apply_url, posted_at, first_seen_at,
                          relevance_score, trust_score, is_direct_apply, status, search_profile_id
                   FROM jobs WHERE first_seen_at < datetime('now', ?)""",
                (f"-{ARCHIVE_AFTER_DAYS} days",),
            )
            # 3. Delete them from active table
            await db.execute(
                "DELETE FROM jobs WHERE first_seen_at < datetime('now', ?)",
                (f"-{ARCHIVE_AFTER_DAYS} days",),
            )

        # 4. Purge ancient archives
        cursor = await db.execute(
            "SELECT COUNT(*) FROM jobs_archive WHERE archived_at < datetime('now', ?)",
            (f"-{PURGE_AFTER_DAYS} days",),
        )
        purge_count = (await cursor.fetchone())[0]

        if purge_count > 0:
            await db.execute(
                "DELETE FROM jobs_archive WHERE archived_at < datetime('now', ?)",
                (f"-{PURGE_AFTER_DAYS} days",),
            )

        await db.commit()

        # 5. Get current table sizes
        active = (await (await db.execute("SELECT COUNT(*) FROM jobs")).fetchone())[0]
        archived = (await (await db.execute("SELECT COUNT(*) FROM jobs_archive")).fetchone())[0]

        return {
            "archived": archive_count,
            "purged": purge_count,
            "active_jobs": active,
            "archived_jobs": archived,
        }
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
