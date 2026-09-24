"""Workable goes through the widget API (v3 is 429 from Railway). Run:
DATABASE_PATH=/tmp/sp/w.db PYTHONPATH=$PWD .venv/bin/python tests/test_workable_widget.py"""
import asyncio, httpx
import ats_scraper as a

WIDGET = {"name": "Acme", "description": "x", "jobs": [
    {"title": "Senior Data Analyst", "shortcode": "AB12", "telecommuting": True, "country": "United States",
     "state": "Texas", "city": "Austin", "locations": [{"countryCode": "US", "city": "Austin"}],
     "published_on": "2026-09-23", "application_url": "https://apply.workable.com/j/AB12/apply",
     "url": "https://apply.workable.com/j/AB12", "description": "<p>SQL &amp; Tableau dashboards</p>"},
    {"title": "Data Analyst", "shortcode": "UK1", "telecommuting": True, "country": "United Kingdom",
     "locations": [{"countryCode": "GB"}], "published_on": "2026-09-23", "url": "https://apply.workable.com/j/UK1"},
    {"title": "Office Manager", "shortcode": "OM1", "country": "United States",
     "locations": [{"countryCode": "US"}], "url": "https://apply.workable.com/j/OM1"},
]}

def handler(req):
    if "/api/v1/widget/accounts/" in str(req.url):
        return httpx.Response(200, json=WIDGET)
    return httpx.Response(429, text="<html>blocked</html>")

got = []
async def fake_insert(job):
    got.append(job); return True
a.insert_job = fake_insert

async def main():
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        rows = await a.fetch_workable(c, {"slug": "acme", "name": "Acme"}, 1, ["Data Analyst"])
    assert [r["title"] for r in rows] == ["Senior Data Analyst"], [r["title"] for r in rows]
    r = rows[0]
    assert r["work_type"] == "remote" and "SQL & Tableau" in r["description"], r
    assert r["posted_at"].startswith("2026-09-23"), r["posted_at"]
    print("OK: widget rows parsed, non-US and off-profile dropped, remote + description kept")

asyncio.run(main())
