"""insert_job refuses a known row BEFORE the write lock and still inserts new
rows. Run: DATABASE_PATH=/tmp/sp/t.db PYTHONPATH=$PWD .venv/bin/python tests/test_insert_precheck.py"""
import asyncio
import database as d

JOB = {"title": "Senior Data Analyst", "company_name": "Acme Health", "location": "Remote, US",
       "work_type": "remote", "description": "SQL and Tableau dashboards.", "source": "greenhouse",
       "source_url": "https://boards.greenhouse.io/acme/jobs/1", "search_profile_id": None}


async def main():
    await d.init_db()
    assert await d.insert_job(dict(JOB)) is True, "new row must insert"
    acq = d._LOCK_STATS["acq"]
    assert await d.insert_job(dict(JOB)) is False, "same row must be refused"
    assert d._LOCK_STATS["acq"] == acq, "a known row must not take the write lock"
    same_url = dict(JOB, title="Sr. Data Analyst (Remote)")
    assert await d.insert_job(same_url) is False, "same URL must be refused"
    other = dict(JOB, title="Business Intelligence Analyst",
                 source_url="https://boards.greenhouse.io/acme/jobs/2")
    assert await d.insert_job(other) is True, "a different job must still insert"
    print("OK: known rows refused before the lock; new rows insert")

asyncio.run(main())
