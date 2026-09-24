"""JazzHR board parse (real markup from Railway, 24 Sep). Run:
DATABASE_PATH=/tmp/sp/j.db PYTHONPATH=$PWD .venv/bin/python tests/test_jazzhr.py"""
import asyncio, httpx
import ats_more as m

HTML = """<ul class='list-group'>
                                                    <li class="list-group-item">
                                <h3 class='list-group-item-heading'>
                                    <a href="https://smartlightanalytics.applytojob.com/apply/RVElPTFWQh/Business-Analyst-Data-Science">
                                        Business Analyst- Data Science                                    </a>
                                </h3>
                                <ul class='list-inline list-group-item-text'>
                                    <li><i class='fa fa-map-marker'></i>Remote</li>
                                                                    </ul>
                            </li>
                                                    <li class="list-group-item">
                                <h3 class='list-group-item-heading'>
                                    <a href="https://smartlightanalytics.applytojob.com/apply/KoGi7xaYTU/Senior-Data-Analyst">
                                        Senior Data Analyst                                    </a>
                                </h3>
                                <ul class='list-inline list-group-item-text'>
                                    <li><i class='fa fa-map-marker'></i>Austin, TX</li>
                                    <li><i class='fa fa-sitemap'></i>Analytics</li>
                                                                    </ul>
                            </li>
                                                    <li class="list-group-item">
                                <h3 class='list-group-item-heading'>
                                    <a href="https://smartlightanalytics.applytojob.com/apply/oESqBNZCsE/Director-Of-Account-Management">
                                        Director of Account Management                                    </a>
                                </h3>
                                <ul class='list-inline list-group-item-text'>
                                    <li><i class='fa fa-map-marker'></i>Remote</li>
                                    </ul>
                            </li>
</ul>"""

def handler(req):
    return httpx.Response(200, text=HTML)

async def main():
    tok = m.DRY_RUN.set(True)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
            rows = await m.fetch_jazzhr(c, {"slug": "smartlightanalytics", "name": "SmartLight"}, 1,
                                        ["Data Analyst", "Business Analyst"])
    finally:
        m.DRY_RUN.reset(tok)
    got = {r["title"]: (r["work_type"], r["location"]) for r in rows}
    assert got == {"Business Analyst- Data Science": ("remote", "Remote, US"),
                   "Senior Data Analyst": ("onsite", "Austin, TX")}, got
    print("OK: JazzHR titles, locations and Remote parsed; off-profile dropped")

asyncio.run(main())
