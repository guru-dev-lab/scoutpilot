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
    finally:
        await db.close()


def make_job_hash(company: str, title: str, location: str) -> str:
    raw = f"{company.lower().strip()}|{title.lower().strip()}|{location.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


async def insert_job(job_data: dict) -> bool:
    h = make_job_hash(job_data.get("company_name", ""), job_data.get("title", ""), job_data.get("location", ""))
    db = await get_db()
    try:
        existing = await db.execute("SELECT id FROM jobs WHERE hash = ?", (h,))
        if await existing.fetchone(): return False
        now = datetime.now(timezone.utc).isoformat()
        await db.execute("INSERT INTO jobs (hash, title, company_name, company_domain, location, is_remote, description, salary_min, salary_max, source, source_url, direct_apply_url, posted_at, first_seen_at, relevance_score, trust_score, is_direct_apply, status, search_profile_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (h, job_data.get("title", ""), job_data.get("company_name", ""), job_data.get("company_domain", ""), job_data.get("location", ""), 1 if job_data.get("is_remote") else 0, job_data.get("description", ""), job_data.get("salary_min", 0), job_data.get("salary_max", 0), job_data.get("source", ""), job_data.get("source_url", ""), job_data.get("direct_apply_url", ""), job_data.get("posted_at", ""), now, job_data.get("relevance_score", 50), job_data.get("trust_score", 50), 1 if job_data.get("is_direct_apply") else 0, "new", job_data.get("search_profile_id")))
        await db.commit()
        return True
    finally: await db.close()


async def get_jobs(hours=24, min_relevance=0, min_trust=0, source="", status="", sort_by="first_seen_at", sort_dir="DESC", limit=200, offset=0, search=""):
    db = await get_db()
    try:
        conditions, params = [], []
        if hours > 0: conditions.append("first_seen_at >= datetime('now', ?)"); params.append(f"-{hours} hours")
        if min_relevance > 0: conditions.append("relevance_score >= ?"); params.append(min_relevance)
        if min_trust > 0: conditions.append("trust_score >= ?"); params.append(min_trust)
        if source: conditions.append("source = ?"); params.append(source)
        if status: conditions.append("status = ?"); params.append(status)
        if search: conditions.append("(title LIKE ? OR company_name LIKE ? OR description LIKE ?)"); s = f"%{search}%"; params.extend([s, s, s])
        where = " AND ".join(conditions) if conditions else "1=1"
        allowed = {"first_seen_at", "relevance_score", "trust_score", "salary_max", "company_name", "title", "posted_at"}
        if sort_by not in allowed: sort_by = "first_seen_at"
        sort_dir = "ASC" if sort_dir.upper() == "ASC" else "DESC"
        params.extend([limit, offset])
        cursor = await db.execute(f"SELECT * FROM jobs WHERE {where} ORDER BY {sort_by} {sort_dir} LIMIT ? OFFSET ?", params)
        return [dict(row) for row in await cursor.fetchall()]
    finally: await db.close()


async def get_job_count(hours=24):
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) as total, COALESCE(SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END), 0) as new_count, COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0) as applied_count, COALESCE(SUM CASE THAN status = 'hidden' THEN 1 ELSE 0 END), 0) as hidden_count FROM jobs WHERE first_seen_at >= datetime('now', ?)", (f"-{hours} hours",))
        row = await cursor.fetchone()
        return dict(row) if row else {"total": 0, "new_count": 0, "applied_count": 0, "hidden_count": 0}
    finally: await db.close()


async def update_job_status(job_id, status):
    db = await get_db()
    try: await db.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id)); await db.commit()
    finally: await db.close()


async def update_job_scores(job_id, relevance, trust):
    db = await get_db()
    try: await db.execute("UPDATE jobs SET relevance_score = ?, trust_score = ? WHERE id = ?", (relevance, trust, job_id)); await db.commit()
    finally: await db.close()


async def create_profile(data):
    db = await get_db()
    try:
        cursor = await db.execute("INSERT INTO search_profiles (title, expanded_titles, keywords, excluded_keywords, locations, remote_only, min_salary, freshness_hours, min_relevance, min_trust) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (data["title"], json.dumps(data.get("expanded_titles", [])), json.dumps(data.get("keywords", [])), json.dumps(data.get("excluded_keywords", [])), json.dumps(data.get("locations", [])), 1 if data.get("remote_only") else 0, data.get("min_salary", 0), data.get("freshness_hours", 24), data.get("min_relevance", 0), data.get("min_trust", 0)))
        await db.commit()
        return cursor.lastrowid
    finally: await db.close()


async def get_profiles():
    db = await get_db()
    try:
        cursor = await db.execute("SELECT * FROM search_profiles WHERE is_active = 1")
        profiles = []
        for row in await cursor.fetchall():
            p = dict(row)
            for k in ["expanded_titles", "keywords", "excluded_keywords", "locations"]: p[k] = json.loads(p[k])
            profiles.append(p)
        return profiles
    finally: await db.close()


async def update_profile(profile_id, data):
    db = await get_db()
    try:
        await db.execute("UPDATE search_profiles SET title=?, expanded_titles=?, keywords=?, excluded_keywords=?, locations=?, remote_only=?, min_salary=?, freshness_hours=?, min_relevance=?, min_trust=? WHERE id=?", (data["title"], json.dumps(data.get("expanded_titles", [])), json.dumps(data.get("keywords", [])), json.dumps(data.get("excluded_keywords", [])), json.dumps(data.get("locations", [])), 1 if data.get("remote_only") else 0, data.get("min_salary", 0), data.get("freshness_hours", 24), data.get("min_relevance", 0), data.get("min_trust", 0), profile_id))
        await db.commit()
    finally: await db.close()


async def delete_profile(profile_id):
    db = await get_db()
    try: await db.execute("UPDATE search_profiles SET is_active = 0 WHERE id = ?", (profile_id,)); await db.commit()
    finally: await db.close()
