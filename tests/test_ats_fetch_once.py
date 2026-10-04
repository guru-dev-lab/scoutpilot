"""Every ATS board is fetched ONCE per sweep, not once per profile. The sweep
runs with the union of all active profiles' terms; the Scoring worker's
_home_profile re-homes a row the fetching profile would cap.
Run: PYTHONPATH=$PWD .venv/bin/python tests/test_ats_fetch_once.py"""
import asyncio, re
import scraper, ats_scraper

P1 = {"id": 1, "title": "Data Analyst", "expanded_titles": ["Analytics Analyst"]}
P2 = {"id": 2, "title": "Business Intelligence Analyst", "expanded_titles": ["BI Analyst"]}
calls = []

async def fake_scrape_all_ats(profile_id, search_terms, cycle_number, platforms=None, shard=0, shards=1):
    calls.append({"profile_id": profile_id, "terms": list(search_terms), "platforms": platforms, "shard": shard})
    return {"greenhouse": 0}

async def main():
    ats_scraper.scrape_all_ats = fake_scrape_all_ats
    n = await scraper.scrape_ats_all_profiles([P1, P2], 7, ["greenhouse"], 1, 4)
    assert len(calls) == 1, f"one sweep for all profiles, got {len(calls)}"
    t = [x.lower() for x in calls[0]["terms"]]
    assert "data analyst" in t and "business intelligence analyst" in t and "bi analyst" in t, f"union of every profile's terms, got {t}"
    assert calls[0]["profile_id"] == 1 and calls[0]["shard"] == 1 and calls[0]["platforms"] == ["greenhouse"]
    assert n == 0
    assert await scraper.scrape_ats_all_profiles([], 1, ["ashby"]) == 0 and len(calls) == 1, "no profiles → no fetch"
    src = open("main.py", encoding="utf-8").read()
    _i = src.index("def _make_ats_body("); body = src[_i:src.index("return _body", _i)]
    assert "scrape_ats_all_profiles(" in body and "_for_each_profile(" not in body, "the ATS worker body sweeps once for all profiles"
    assert "def _home_profile(" in src, "re-homing must exist for a union sweep to be safe"
    print("OK: one fetch per board per sweep, union terms, re-homing present")

asyncio.run(main())
