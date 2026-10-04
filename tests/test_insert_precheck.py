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
    # 3 Oct 2026: the lock was busy 95% of the time refusing rows the cheap check let through.
    acq = d._LOCK_STATS["acq"]
    non_us = dict(JOB, title="Data Analyst (m/w/d)", location="Berlin, Germany", source_url="https://boards.greenhouse.io/acme/jobs/3")
    assert await d.insert_job(non_us) is False and d._LOCK_STATS["acq"] == acq, "a non-US row is refused BEFORE the lock"
    cross = dict(JOB, title="Senior Data Analyst", location="Austin, TX", source_url="https://jobs.lever.co/acme/9")
    assert await d.insert_job(cross) is False and d._LOCK_STATS["acq"] == acq, "same company+title from another source is refused BEFORE the lock"
    assert d._READER is not None, "the pre-check reuses one read connection instead of opening one per row"
    r1 = d._READER; await d._already_have(dict(JOB)); assert d._READER is r1, "reader connection is reused across calls"
    await d.close_reader(); assert d._READER is None, "shutdown closes the reader (its thread would keep the process alive)"
    print("OK: known / non-US / cross-source rows refused before the lock on one shared reader; new rows insert")

asyncio.run(main())
