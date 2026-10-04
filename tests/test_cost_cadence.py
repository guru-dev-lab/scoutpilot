"""Cost guard (3 Oct 2026): the box was 1.17 vCPU / 5.5 GB for ~1 visible job an hour.
Concurrency and sweep cadence must stay at the measured-sane values.
Run: PYTHONPATH=$PWD .venv/bin/python tests/test_cost_cadence.py"""
import re
import ats_scraper as a
src = open("main.py", encoding="utf-8").read()
assert a.PLATFORM_CONCURRENCY <= 8, f"per-platform concurrency {a.PLATFORM_CONCURRENCY} (32 held 5 GB of in-flight boards)"
def interval(name):
    m = re.search(r'_worker\(\s*f?"%s[^"]*",\s*(\d+)' % re.escape(name), src)
    assert m, f"worker {name} not found"; return int(m.group(1))
assert interval("ATS-greenhouse") >= 600, "greenhouse sweep every ≥10 min (was 120 s, 0 inserted per sweep)"
assert interval("ATS-ashby") >= 600 and interval("ATS-lever") >= 600 and interval("ATS-smartrecruiters") >= 600
assert interval("ATS-workday") >= 900
assert interval("JobSpy") >= 900, "Indeed via JobSpy: 1,000 rows per pass, ~5 new — every 15 min is plenty"
m = re.search(r'\("ukg", (\d+)\), \("oracle", (\d+)\)', src); assert m and int(m.group(1)) >= 900 and int(m.group(2)) >= 900, "more-platform sweeps ≥15 min"
assert "sweep done" in open("ats_scraper.py", encoding="utf-8").read() and "unchanged" in open("ats_scraper.py", encoding="utf-8").read().split("sweep done")[1][:300], "sweep-done line reports unchanged boards"
# 4 Oct: after v2.55.0 the box idles at 0.03 vCPU; what is left is bursts — the 6 h full re-read and workers on short clocks.
assert a.BOARD_PRINT_TTL >= 24 * 3600, "full re-read of every board at most once a day (ETag 304s catch real changes in between)"
assert interval("Scoring") >= 60, "scoring pass every minute is plenty for ~10 rows an hour"
assert interval("Workday-Seed") >= 3600, "live Workday tenant guessing (12 probes a pass) hourly, not every 3 min"
assert interval("Light") >= 900 and interval("Enrich-ATS") >= 900, "light sources + ATS enrich every 15 min"
assert interval("JobSpy") >= 1800, "Indeed via JobSpy every 30 min (1,000 rows a pass, ~5 new)"
print("OK: cadence + concurrency at cost-sane values")
