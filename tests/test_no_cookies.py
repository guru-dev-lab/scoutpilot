"""Sweep/discovery clients must not keep cookies (a growing jar pinned the
box at one core, 24 Sep). Run: PYTHONPATH=$PWD .venv/bin/python tests/test_no_cookies.py"""
import asyncio, httpx
import ats_scraper as a

def handler(req):
    return httpx.Response(200, json={"ok": 1}, headers=[
        ("set-cookie", f"sess{req.url.host}=x; Path=/"),
        ("set-cookie", "PLAY_SESSION=y; Path=/")])

async def main():
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                 cookies=a.no_cookie_jar()) as c:
        for i in range(50):
            r = await c.get(f"https://host{i}.example.com/jobs")
            assert r.status_code == 200 and r.json() == {"ok": 1}
        assert len(c.cookies.jar) == 0, f"jar kept {len(c.cookies.jar)} cookies"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        for i in range(50):
            await c.get(f"https://host{i}.example.com/jobs")
        kept = len(c.cookies.jar)
    print(f"OK: no-cookie client kept 0; a default client kept {kept}")

asyncio.run(main())
