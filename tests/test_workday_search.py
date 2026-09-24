"""fetch_workday must SEARCH the board with the profile's titles and page each
query, not read the first 100 postings. Run:
DATABASE_PATH=/tmp/wd.db PYTHONPATH=$PWD .venv/bin/python tests/test_workday_search.py
"""
import asyncio
import ats_scraper as a

# A 2,000-posting board: 1,900 unrelated jobs first, then 100 data analyst jobs.
BOARD = ([{"title": f"Trade Sales Representative {i}", "externalPath": f"/job/x/{i}",
           "locationsText": "Vietnam - Can Tho", "postedOn": "Posted Today"} for i in range(1900)]
         + [{"title": f"Senior Data Analyst {i}", "externalPath": f"/job/d/{i}",
             "locationsText": "US - Remote", "postedOn": "Posted Today"} for i in range(100)])


class FakeResp:
    status_code = 200
    def __init__(self, d): self._d = d
    def json(self): return self._d


class FakeClient:
    def __init__(self): self.calls = []
    async def post(self, url, json):
        self.calls.append(json)
        q = (json.get("searchText") or "").lower()
        # Workday-style: relevance-ranked search — rows sharing a word first.
        rows = BOARD if not q else sorted(
            [r for r in BOARD if any(w in r["title"].lower() for w in q.split())],
            key=lambda r: -sum(w in r["title"].lower() for w in q.split()))
        o = json["offset"]
        return FakeResp({"total": len(rows), "jobPostings": rows[o:o + json["limit"]]})


inserted = []
async def fake_insert(job):
    inserted.append(job)
    return True
a.insert_job = fake_insert

client = FakeClient()
company = {"slug": "abbott", "name": "Abbott",
           "workday_url": "https://abbott.wd5.myworkdayjobs.com/wday/cxs/abbott/AbbottCareers"}
asyncio.run(a.fetch_workday(client, company, 1, ["Data Analyst", "BI Analyst"]))

titles = [j["title"] for j in inserted]
assert len(titles) == 100 and all("Data Analyst" in t for t in titles), (len(titles), titles[:3])
assert all(c["searchText"] for c in client.calls), "every call must carry a profile title"
assert len(client.calls) <= a.WORKDAY_QUERIES * a.WORKDAY_PAGES_PER_QUERY
assert inserted[0]["work_type"] == "remote"
print(f"OK: {len(titles)} analyst rows from a 2,000-row board in {len(client.calls)} calls")
