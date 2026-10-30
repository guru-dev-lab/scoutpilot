"""Fault 108 (relevance): an accounting job whose title happens to carry
"Reporting Analyst" scored 100 against the BI profile. The description's duties
now decide; zero tokens (regex only). Profiles copied from the live board,
29 Oct 2026 (/api/debug/pipeline).
Run: DATABASE_PATH=/tmp/sp/t.db PYTHONPATH=$PWD .venv/bin/python tests/test_accounting_duties.py"""
from ai_engine import score_relevance_fuzzy

BI = ("Business Intelligence Analyst",
      ["BI Analyst", "Business Analytics Analyst", "Analytics Analyst", "Data Analytics Analyst",
       "Reporting Analyst", "Business Analyst (Data)", "Intelligence Analyst", "Data Intelligence Analyst",
       "BI Developer", "Analytics Developer"],
      ["Power BI", "Tableau", "SQL", "Snowflake", "dbt", "Looker", "Excel", "Python", "DAX", "SSRS",
       "SSIS", "ETL", "Data Warehouse", "Redshift", "BigQuery", "Databricks", "MicroStrategy", "Domo",
       "Qlik", "SAP BI"])

COST_REPORT = ("Ascension is hiring. Prepares Medicare and Medicaid cost reports for our hospitals, "
               "reviews the general ledger and trial balance, reconciles accounts, supports audits by the "
               "fiscal intermediary and calculates reimbursement settlements. Prepares journal entries. "
               "Bachelor's in accounting, CPA preferred. Advanced Excel; SQL a plus.")
FIN_REPORTING = ("Prepare statutory financial statements under GAAP, support month-end close, "
                 "reconcile balance sheet accounts, draft 10-Q and 10-K schedules. CPA preferred. Excel.")
BI_JD = ("Build Power BI dashboards and SQL data models for finance; partner on month-end reporting, "
         "reconcile data between the warehouse and the general ledger, automate ETL.")

CASES = [
    # accounting duties -> capped
    ("Senior Reimbursement Analyst - Medicare Cost Reporting", COST_REPORT, "<=22"),
    ("Cost Reporting Analyst", COST_REPORT, "<=22"),
    ("Reporting Analyst", COST_REPORT, "<=22"),
    ("Financial Reporting Analyst", FIN_REPORTING, "<=22"),
    ("STAT Financial Reporting Analyst", FIN_REPORTING, "<=22"),
    # BI duties, even in a finance team -> not capped
    ("Financial Reporting Analyst", BI_JD, ">=80"),
    ("Reporting Analyst", BI_JD, ">=80"),
    # the title itself names data work -> the duty rule does not judge it
    ("Business Intelligence Analyst", COST_REPORT, ">=80"),
    ("Finance and BI Analyst", FIN_REPORTING, ">=80"),
    # no description -> title words only, as before
    ("Reporting Analyst", "", ">=80"),
]

bad = []
for title, desc, want in CASES:
    s = score_relevance_fuzzy(title, desc, BI[0], BI[1], BI[2])
    ok = s <= int(want[2:]) if want.startswith("<=") else s >= int(want[2:])
    print(f"{'ok ' if ok else 'BAD'} {s:3d} {want:5} {title} | {desc[:40]!r}")
    if not ok:
        bad.append(title)
assert not bad, bad
print(f"OK: {len(CASES)} cases; accounting duties cap a 'Reporting Analyst' title, BI duties and data titles do not")
