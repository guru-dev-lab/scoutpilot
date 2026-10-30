"""Fault 114: the employer's own text beats an aggregator's Remote flag.
Through insert_job (every source passes there) and the boot re-derive
(_fix_work_types), which used to read Fidelity's "does not apply to fully remote
roles" as proof of remote.
Run: DATABASE_PATH=/tmp/sp/t.db PYTHONPATH=$PWD .venv/bin/python tests/test_remote_contradiction.py"""
import asyncio
import database as d

# Fidelity's real boilerplate (fmr.wd1.myworkdayjobs.com, 29 Oct 2026).
FIDELITY = ("The Role: Business Analyst supporting Workplace Investing. "
            "Fidelity's Onsite Working Model Fidelity is transitioning to a full-time onsite working model "
            "through a phased rollout across regions and roles. Currently, some roles and locations require "
            "100% onsite presence, while others require less. Onsite expectations are likely to evolve as "
            "the rollout continues. This transition does not apply to fully remote roles.")
# Toast puts this in every posting, its genuinely remote roles included.
TOAST_MODEL = ("Our approach to hybrid working: we embrace a hybrid work model that fosters in-person "
               "collaboration while valuing individual needs.")

CASES = [
    # (title, location, description, expected)
    ("Business Analyst", "Smithfield, RI", FIDELITY, "onsite"),        # the reported row
    ("Business Analyst", "Smithfield, RI", "This role is onsite at our Smithfield office.", "onsite"),
    ("Data Analyst", "Boston, MA", "This is a hybrid role requiring two days per week in the office.", "hybrid"),
    ("Data Analyst", "Austin, TX", "You will be expected to be in office 3 days a week.", "hybrid"),
    ("Data Analyst", "Chicago, IL", "Work Location: Hybrid. SQL, Tableau.", "hybrid"),
    ("Data Analyst (Remote)", "Remote, US", "This position is hybrid, 3 days in office.", "hybrid"),
    # stays remote
    ("Data Analyst", "Remote, US", TOAST_MODEL, "remote"),             # company model vs a Remote location
    ("Account Executive", "San Francisco, CA",
     "This is a fully remote sales opportunity based out of a home office. " + TOAST_MODEL, "remote"),
    ("Business Analyst", "Remote", FIDELITY + " This role is fully remote.", "remote"),
    ("BI Analyst", "Remote, US", "Remote sensing data, SQL, Power BI dashboards.", "remote"),
    ("BI Analyst", "Denver, CO", "SQL, Power BI dashboards for finance.", "remote"),  # silence keeps the label
]


async def run():
    await d.init_db()
    bad = []
    for i, (t, loc, desc, want) in enumerate(CASES):
        got = d.remote_contradiction(t, loc, desc) or "remote"
        if got != want:
            bad.append(f"rule: {t} | {loc} -> {got}, want {want}")
        job = {"title": t, "company_name": f"Co{i}", "location": loc, "description": desc,
               "work_type": "remote", "is_remote": True, "source": "indeed",
               "source_url": f"https://example.com/{i}", "search_profile_id": None}
        assert await d.insert_job(job), t
    db = await d.get_db()
    cur = await db.execute("SELECT title, location, work_type, is_remote FROM jobs ORDER BY id")
    stored = [tuple(r) for r in await cur.fetchall()]
    await db.close()
    for (t, loc, _, want), (st, sl, wt, rem) in zip(CASES, stored):
        if wt != want or rem != (1 if want == "remote" else 0):
            bad.append(f"insert: {t} | {loc} stored {wt}/{rem}, want {want}")

    # Boot re-derive must not put Fidelity back to remote.
    db = await d.get_db()
    await db.execute("UPDATE jobs SET work_type='remote', is_remote=1 WHERE location='Smithfield, RI'")
    await db.commit()
    await db.close()
    await d.close_reader()
    import main as m
    await m._fix_work_types()
    db = await d.get_db()
    cur = await db.execute("SELECT DISTINCT work_type, is_remote FROM jobs WHERE location='Smithfield, RI'")
    after = [tuple(r) for r in await cur.fetchall()]
    await db.close()
    if after != [("onsite", 0)]:
        bad.append(f"boot re-derive left Fidelity as {after}")
    assert not bad, "\n".join(bad)
    print(f"OK: {len(CASES)} cases: onsite/hybrid text beats the remote flag at insert and at boot; "
          "company-wide model sentences and employer remote statements keep remote")


asyncio.run(run())
