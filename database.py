import aiosqlite
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional
from config import settings

DB_PATH = settings.database_path


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

    finally:
        await db.close()


def make_job_hash(company: str, title: str, location: str) -> str:
    raw = f"{company.lower().strip()}|{title.lower().strip()}|{location.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


async def insert_job(job_data: dict) -> bool:
    """Insert a job if it doesn't already exist. Returns True if inserted."""
    h = make_job_hash(
        job_data.get("company_name", ""),
        job_data.get("title", ""),
        job_data.get("location", ""),
    )
    db = await get_db()
    try:
        existing = await db.execute("SELECT id FROM jobs WHERE hash = ?", (h,))
        if await existing.fetchone():
            return False

        now = datetime.now(timezone.utc).isoformat()
        await db.execute(
            """INSERT INTO jobs (hash, title, company_name, company_domain, location,
               is_remote, work_type, description, salary_min, salary_max, source, source_url,
               direct_apply_url, posted_at, first_seen_at, relevance_score, trust_score,
               is_direct_apply, status, search_profile_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                job_data.get("posted_at", ""),
                now,
                job_data.get("relevance_score", 50),
                job_data.get("trust_score", 50),
                1 if job_data.get("is_direct_apply") else 0,
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
        if posted_hours > 0:
            conditions.append(
                "posted_at != '' AND posted_at >= datetime('now', ?)"
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
            conditions.append("status = ?")
            params.append(status)

        if work_type:
            conditions.append("work_type = ?")
            params.append(work_type)

        if direct_only:
            conditions.append("is_direct_apply = 1")

        if search:
            # Split search into words for multi-term matching
            # Each word must appear somewhere in title, company, or description
            words = search.strip().split()
            word_conditions = []
            for word in words:
                w = f"%{word}%"
                word_conditions.append("(title LIKE ? OR company_name LIKE ? OR description LIKE ?)")
                params.extend([w, w, w])
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

        query = f"""
            SELECT * FROM jobs
            WHERE {where}
            ORDER BY {sort_by} {sort_dir}
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
        cursor = await db.execute(
            """SELECT
                COUNT(*) as total,
                COALESCE(SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END), 0) as new_count,
                COALESCE(SUM(CASE WHEN status = 'viewed' THEN 1 ELSE 0 END), 0) as viewed_count,
                COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0) as applied_count,
                COALESCE(SUM(CASE WHEN status = 'hidden' THEN 1 ELSE 0 END), 0) as hidden_count,
                COALESCE(SUM(CASE WHEN is_direct_apply = 1 THEN 1 ELSE 0 END), 0) as direct_count
            FROM jobs WHERE first_seen_at >= datetime('now', ?)""",
            (f"-{hours} hours",),
        )
        row = await cursor.fetchone()
        return dict(row) if row else {"total": 0, "new_count": 0, "viewed_count": 0, "applied_count": 0, "hidden_count": 0, "direct_count": 0}
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
