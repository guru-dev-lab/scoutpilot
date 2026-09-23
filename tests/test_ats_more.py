"""ats_more: link extractors + fetchers against synthetic payloads (no network).

Run: DATABASE_PATH=/tmp/x.db PYTHONPATH=$PWD .venv/bin/python tests/test_ats_more.py
The REAL shapes are confirmed on Railway: each fetcher logs '[<ats>] SHAPE ...'
on its first response and /api/debug/ats-probe returns what it parsed.
"""
import asyncio, json, os, sys
os.environ.setdefault("DATABASE_PATH", "/tmp/scoutpilot-test.db")
import httpx
import ats_more as M
from ats_discovery import extract_candidates_from_url

fails = []
def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond: fails.append(msg)

# ── extractors ──
ex = dict(extract_candidates_from_url("https://recruiting2.ultipro.com/IVE1000IVEC/JobBoard/ec3e4530-b925-46c7-8da5-602b6abf81e7/OpportunityDetail?opportunityId=9ce13ddb"))
check(ex.get("ukg") == {"slug": "IVE1000IVEC", "tenant": "recruiting2.ultipro.com", "site": "ec3e4530-b925-46c7-8da5-602b6abf81e7"}, f"ukg extract {ex.get('ukg')}")
ex = dict(extract_candidates_from_url("https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions/job/210574007/"))
check(ex.get("oracle") == {"slug": "jpmc.fa.oraclecloud.com/CX_1001", "tenant": "jpmc.fa.oraclecloud.com", "site": "CX_1001"}, f"oracle extract {ex.get('oracle')}")
ex = dict(extract_candidates_from_url("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=c968050c-15a9-468b-acaf-b26b765aff76&ccId=1586553167877902_1890&jobId=557480"))
check(ex.get("adp") == {"slug": "c968050c-15a9-468b-acaf-b26b765aff76", "tenant": "workforcenow.adp.com", "site": "1586553167877902_1890"}, f"adp extract {ex.get('adp')}")
ex = dict(extract_candidates_from_url("https://ats.rippling.com/vouch-inc/jobs/66a79ddf-42fd"))
check(ex.get("rippling") == {"slug": "vouch-inc"}, f"rippling extract {ex.get('rippling')}")
ex = dict(extract_candidates_from_url("https://acme.bamboohr.com/careers/123"))
check(ex.get("bamboohr") == {"slug": "acme"}, f"bamboohr extract {ex.get('bamboohr')}")
ex = dict(extract_candidates_from_url("https://jobs.jobvite.com/pulsepoint/job/ojtlyfwp"))
check(ex.get("jobvite") == {"slug": "pulsepoint"}, f"jobvite extract {ex.get('jobvite')}")
ex = dict(extract_candidates_from_url("https://careers-cotiviti.icims.com/jobs/16642/data-analyst/job"))
check(ex.get("icims") == {"slug": "careers-cotiviti.icims.com"}, f"icims extract {ex.get('icims')}")
check(not dict(extract_candidates_from_url("https://www.icims.com/jobs/whatever")).get("icims"), "icims ignores www.icims.com")

# ── fetchers on synthetic payloads (the shapes each API is documented to return) ──
PAYLOADS = {
 "ukg": json.dumps({"opportunities": [
    {"Id": "abc", "Title": "Senior Data Analyst", "PostedDate": "/Date(1758500000000)/",
     "Locations": [{"LocalizedName": "Austin, TX", "Address": {"City": "Austin", "State": {"Code": "TX"}, "Country": {"Code": "USA"}}}],
     "BriefDescription": "SQL and Tableau"},
    {"Id": "def", "Title": "Data Analyst (Remote)", "PostedDate": "/Date(1758500000000)/", "Locations": []},
    {"Id": "ghi", "Title": "Data Analyst", "Locations": [{"LocalizedName": "Toronto, ON", "Address": {"Country": {"Code": "CAN"}}}]},
 ], "totalCount": 3}),
 "oracle": json.dumps({"items": [{"TotalJobsCount": 2, "requisitionList": [
    {"Id": "1", "Title": "Business Intelligence Analyst", "PostedDate": "2026-09-20", "PrimaryLocation": "Columbus, OH, United States", "PrimaryLocationCountry": "US", "WorkplaceType": "Hybrid", "ShortDescriptionStr": "x"},
    {"Id": "2", "Title": "Data Analyst", "PostedDate": "2026-09-19", "PrimaryLocation": "Mumbai, India", "PrimaryLocationCountry": "IN"},
 ]}]}),
 "adp": json.dumps({"jobRequisitions": [
    {"itemID": "946250", "requisitionTitle": "Data Analyst", "postDate": "2026-09-18",
     "requisitionLocations": [{"nameCode": {"shortName": "HQ"}, "address": {"cityName": "Denver", "countrySubdivisionLevel1": {"codeValue": "CO"}, "countryCode": "US"}}]},
 ], "meta": {"totalNumber": 1}}),
 "rippling": json.dumps([
    {"id": "11316e9a", "name": "Data Analyst", "url": "https://ats.rippling.com/rippling/jobs/11316e9a", "workLocation": {"label": "San Francisco, CA", "country": "US"}, "workplaceType": "REMOTE"},
 ]),
 "bamboohr": json.dumps({"result": [
    {"id": 55, "jobOpeningName": "Reporting Analyst", "location": {"city": "Remote", "state": "", "country": "United States"}, "isRemote": True},
 ]}),
 "jobvite": '<div class="jv-job-list"><ul><li class="row"><a href="/pulsepoint/job/ojtlyfwp" class="flex-row"><div class="jv-job-list-name"> Sr. Data Analyst, Customer Reporting (Remote) </div><div class="ml-auto jv-job-type">Full-Time</div><div class="ml2 jv-job-list-location"> New York, NY </div></a></li><li class="row"><a href="/pulsepoint/job/ovz1zfwo" class="flex-row"><div class="jv-job-list-name"> BI Engineer, SRE (Remote, International) </div><div class="ml2 jv-job-list-location"> United Kingdom </div></a></li></ul></div>',
 "icims": '<div class="row"><a class="iCIMS_Anchor" href="https://careers-cotiviti.icims.com/jobs/16642/data-analyst/job?mode=job&iis=x"><h3>Data Analyst</h3></a><dl><dt>Job Locations</dt><dd>US-Remote</dd></dl></div>',
}

def handler(req: httpx.Request) -> httpx.Response:
    u = str(req.url)
    key = ("ukg" if "ultipro" in u else "oracle" if "oraclecloud" in u else "adp" if "adp.com" in u
           else "rippling" if "rippling" in u else "bamboohr" if "bamboohr" in u
           else "jobvite" if "jobvite" in u else "icims" if "icims" in u else None)
    if key in ("jobvite", "icims"):
        return httpx.Response(200, text=PAYLOADS[key], headers={"content-type": "text/html"})
    return httpx.Response(200, text=PAYLOADS[key], headers={"content-type": "application/json"})

COMPANIES = {
 "ukg": {"ats": "ukg", "slug": "IVE1000IVEC", "tenant": "recruiting2.ultipro.com", "site": "ec3e4530-b925-46c7-8da5-602b6abf81e7", "name": "Ive"},
 "oracle": {"ats": "oracle", "slug": "jpmc.fa.oraclecloud.com/CX_1001", "tenant": "jpmc.fa.oraclecloud.com", "site": "CX_1001", "name": "JPMC"},
 "adp": {"ats": "adp", "slug": "566efbd3-0446-4f82-a192-39a723b842bb", "tenant": "workforcenow.adp.com", "site": "19000101_000001", "name": "ADP co"},
 "rippling": {"ats": "rippling", "slug": "rippling", "name": "Rippling"},
 "bamboohr": {"ats": "bamboohr", "slug": "acme", "name": "Acme"},
 "jobvite": {"ats": "jobvite", "slug": "pulsepoint", "name": "PulsePoint"},
 "icims": {"ats": "icims", "slug": "careers-cotiviti.icims.com", "name": "Cotiviti"},
}
EXPECT = {"ukg": 2, "oracle": 1, "adp": 1, "rippling": 1, "bamboohr": 1, "jobvite": 1, "icims": 1}

async def run():
    token = M.DRY_RUN.set(True)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            for ats, fn in M.FETCHERS.items():
                rows = await fn(client, COMPANIES[ats], None, ["Data Analyst", "Business Intelligence Analyst", "Reporting Analyst"])
                check(len(rows) == EXPECT[ats], f"{ats}: parsed {len(rows)} rows (want {EXPECT[ats]}) -> {[ (r['title'], r['location'], r['work_type']) for r in rows]}")
                for r in rows:
                    check(r["source"] == ats and r["source_url"].startswith("https://") and r["is_direct_apply"], f"{ats}: row shape {r['source_url']}")
            # URLs for the public list
            from ats_scraper import company_pages
            for ats, c in COMPANIES.items():
                p = company_pages(c)
                check(bool(p and p["page"].startswith("https://") and p["api"].startswith("https://")), f"{ats}: company_pages {p and p['page']}")
    finally:
        M.DRY_RUN.reset(token)

asyncio.run(run())
print()
print("FAILED", len(fails)) if fails else print("all ats_more cases pass")
sys.exit(1 if fails else 0)
