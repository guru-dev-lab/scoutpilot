"""GovernmentJobs cards (real shape, captured on Railway 24 Sep). Only New-labelled
cards are taken; paging stops on a page with none. Run:
DATABASE_PATH=/tmp/sp/g.db PYTHONPATH=$PWD .venv/bin/python tests/test_govjobs.py"""
import asyncio, httpx
import scraper as s

CARD = '''<li class="job-item" data-job-id="{id}-0" data-job-isfeatured="False" role="presentation">
  <div class="job-item-container">{label}
    <div class="row-fluid job-tile-header"><h3>
      <a aria-label="x" class="job-details-link" href="/jobs/{id}-0/slug">{title}</a></h3></div>
    <div class="primaryInfo job-organization">{org}</div>
    <div class="primaryInfo"><span class="job-location">{loc}</span></div>
    <div class="primaryInfo">
        Full-Time  | $105,180.00 - $134,239.32 Annually  | Closes in 2 weeks
    </div>
  </div>
</li>'''
NEW = '<div class="row-fluid label-row"><span class="label new-job-label">New</span></div>'
PAGE1 = "<ul>" + CARD.format(id=1, label=NEW, title="Data Analyst (Telework)", org="King County", loc="Seattle, WA") \
      + CARD.format(id=2, label="", title="Senior Business Analyst", org="State of Michigan", loc="Lansing, MI") + "</ul>"
PAGE2 = "<ul>" + CARD.format(id=3, label="", title="Old Analyst", org="City", loc="Austin, TX") + "</ul>"
calls = []

class FakeClient:
    def __init__(self, *a, **k): pass
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False
    async def get(self, url):
        calls.append(url)
        return httpx.Response(200, text=PAGE1 if "page=1" in url else PAGE2)

got = []
async def fake_insert(j):
    got.append(j); return True
s.insert_job = fake_insert
s.httpx.AsyncClient = FakeClient

rows = asyncio.run(s.scrape_governmentjobs("data analyst", 1))
assert [r["title"] for r in rows] == ["Data Analyst (Telework)"], rows
r = rows[0]
assert r["work_type"] == "remote" and r["company_name"] == "King County" and r["location"] == "Seattle, WA"
assert (r["salary_min"], r["salary_max"]) == (105180, 134239), (r["salary_min"], r["salary_max"])
assert r["source_url"] == "https://www.governmentjobs.com/jobs/1-0/slug"
assert len(calls) == 2, calls   # page 2 had no New card -> stop
print("OK: New-only, telework->remote, pay parsed, stops when New labels stop")
