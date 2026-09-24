"""The board window is measured from the POSTED date, falling back to when we
scraped it. Run: DATABASE_PATH=/tmp/sp/f.db PYTHONPATH=$PWD .venv/bin/python tests/test_fresh_window.py"""
import asyncio
from datetime import datetime, timedelta, timezone
import database as d

now = datetime.now(timezone.utc)
iso = lambda dt: dt.isoformat()
CASES = [  # title, posted_at, first_seen_at, shown in a 48h window?
    ("Posted yesterday, scraped now", iso(now - timedelta(days=1)), iso(now), True),
    ("Posted 30 days ago, scraped now", iso(now - timedelta(days=30)), iso(now), False),
    ("Workday date-only today", (now.strftime("%Y-%m-%d") + "T00:00:00"), iso(now), True),
    ("No posted date, scraped 1 day ago", "", iso(now - timedelta(days=1)), True),
    ("No posted date, scraped 5 days ago", "", iso(now - timedelta(days=5)), False),
    ("Posted 47h ago Z format", (now - timedelta(hours=47)).strftime("%Y-%m-%dT%H:%M:%SZ"), iso(now), True),
    ("Posted 49h ago", iso(now - timedelta(hours=49)), iso(now), False),
]

async def main():
    await d.init_db()
    db = await d.get_db()
    for i, (t, p, f, _) in enumerate(CASES):
        await db.execute(
            "INSERT INTO jobs (hash, title, company_name, location, work_type, source, source_url, "
            "posted_at, first_seen_at, relevance_score, status, scored_at) VALUES "
            "(?,?,?,?,?,?,?,?,?,?,?,?)",
            (f"h{i}", t, "Acme", "Remote, US", "remote", "greenhouse", f"u{i}", p, f, 90, "new", iso(now)))
    await db.commit(); await db.close()
    shown = {j["title"] for j in await d.get_jobs(hours=48, limit=100)}
    bad = [(t, want) for t, _, _, want in CASES if (t in shown) != want]
    assert not bad, bad
    print(f"OK: {len(shown)}/{len(CASES)} shown, every case as expected")

asyncio.run(main())
