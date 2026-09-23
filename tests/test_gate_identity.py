"""Replay of the live feed head against the modifier gate (v2.41.0).

Run:  PYTHONPATH=$PWD .venv/bin/python tests/test_gate_identity.py
Optionally pass a path to an older ai_engine.py to print a before/after table.

The rows come from /api/debug/pipeline sample_feed on 22 Sep 2026 — the
titles the owner was actually seeing under his Business Intelligence profile —
plus titles that MUST keep passing. A title that a person would never call a
BI/data role must land at or under 22; a real one must stay at or above 50.
"""
import importlib.util
import json
import sys

BI = {
    "title": "Business Intelligence Analyst",
    "expanded": ["BI Analyst", "Business Analytics Analyst", "Analytics Analyst",
                 "Data Analytics Analyst", "Reporting Analyst", "Business Analyst (Data)",
                 "Intelligence Analyst", "Data Intelligence Analyst", "BI Developer",
                 "Analytics Developer"],
    "keywords": ["Power BI", "Tableau", "SQL", "Snowflake", "dbt", "Looker", "Excel", "Python",
                 "DAX", "SSRS", "SSIS", "ETL", "Data Warehouse", "Redshift", "BigQuery",
                 "Databricks", "MicroStrategy", "Domo", "Qlik", "SAP BI"],
}
DA = {
    "title": "Data Analyst",
    "expanded": ["BI Analyst", "Business Intelligence Analyst", "Reporting Analyst",
                 "Analytics Analyst", "Data Reporting Analyst", "Insights Analyst",
                 "Business Analyst (Data)", "Data Analytics Analyst"],
    "keywords": ["SQL", "Python", "Power BI", "Tableau", "Excel", "Snowflake", "dbt", "Looker",
                 "BigQuery", "Redshift", "Databricks", "Pandas", "R", "ETL", "Data Pipeline",
                 "A/B Testing", "Statistical Analysis", "Google Analytics", "Jupyter", "Airflow"],
}

# (title, must_pass) — from the live sample feed and the owner's complaint.
JUNK = [
    "Relationship Banker Business Specialist",
    "Customer Service Rep(3134) - 1560-2 Business Center Dr.",
    "Assistant Manager(3134) - 1560-2 Business Center Dr.",
    "Sr Data Scientist",
    "Danaher Business System Leader Manager, Hardware",
    "Business Manager-Enterprise Arbitration, Enterprise Consumer Product",
    "Data and AI Solution Leader",
    "Criminal Intelligence Analyst",
    "Board Certified Behavior Analyst",
    "Registered Nurse",
    "Business Development Representative",
    "Data Entry Clerk",
    "Data Center Technician",
    "Business Office Manager",
]
REAL = [
    "Business Intelligence (BI) Developer I - QUICKSIGHT",
    "Business Intelligence Analyst",
    "Senior Business Intelligence Analyst",
    "Finance and BI Analyst",
    "Business Intelligence Architect",
    "BI Developer",
    "Senior Analyst, Business Intelligence",
    "Data Analytics Analyst II",
    "Reporting Analyst",
    "Analytics Analyst",
    "Business Analytics Analyst",
]
# Fetched by the BI sweep, but a Data Analyst title: the BI gate now caps it,
# and _home_profile must move it to the Data Analyst profile instead of hiding it.
REHOME = [("Data Analyst Senior Manager - DHA", "Data Analyst"),
          ("Senior Data Analyst", "Data Analyst")]
REAL_DA = ["Data Analyst", "Senior Data Analyst", "Data Analyst II", "Insights Analyst",
           "Data Reporting Analyst"]


def load(path):
    spec = importlib.util.spec_from_file_location("ai_engine_x", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def score(m, prof, title):
    return m.score_relevance_fuzzy(title, "", prof["title"], prof["expanded"], prof["keywords"])


def main():
    new = load("ai_engine.py")
    old = load(sys.argv[1]) if len(sys.argv) > 1 else None
    fails = []
    print(f"{'before':>6} {'after':>6}  title")
    for prof, rows, must_pass in ((BI, JUNK, False), (BI, REAL, True), (DA, REAL_DA, True),
                                  (DA, JUNK, False)):
        print(f"--- {prof['title']} / {'must pass' if must_pass else 'must be capped'}")
        for t in rows:
            a = score(new, prof, t)
            b = score(old, prof, t) if old else None
            ok = a >= 50 if must_pass else a <= 22
            if not ok:
                fails.append((prof["title"], t, a))
            print(f"{'' if b is None else b:>6} {a:>6}  {t}{'' if ok else '   <-- FAIL'}")
    print("--- re-homing (fetched under BI, belongs to DA)")
    import os
    os.environ.setdefault("DATABASE_PATH", "/tmp/scoutpilot-test.db")
    import main as app_main
    pds = [dict(BI, id=14), dict(DA, id=15)]
    for t, want in REHOME:
        home, sc, moved = app_main._home_profile(t, "", pds[0], pds, 30)
        ok = moved and home["title"] == want and sc >= 50
        if not ok:
            fails.append(("rehome", t, sc))
        print(f"{'':>6} {sc:>6}  {t} -> {home['title']} (moved={moved}){'' if ok else '   <-- FAIL'}")
    # and a title no profile wants must stay put, low, not moved
    home, sc, moved = app_main._home_profile("Registered Nurse", "", pds[0], pds, 30)
    print(f"{'':>6} {sc:>6}  Registered Nurse -> {home['title']} (moved={moved})")
    if moved or sc > 22:
        fails.append(("rehome", "Registered Nurse", sc))
    print()
    if fails:
        print(f"FAILED {len(fails)}: {json.dumps(fails)}")
        sys.exit(1)
    print("all gate cases pass")


if __name__ == "__main__":
    main()
