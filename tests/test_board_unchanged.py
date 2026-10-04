"""An ATS board that has not changed since the last sweep is not parsed again.
BoardClient (ats_scraper) remembers each board's ETag + content print; the next
GET sends If-None-Match, and a 304 — or identical bytes from a server without
ETags — comes back as an EMPTY payload of the same shape, so the fetcher's item
loop runs on nothing and insert_job is never called.
Run: PYTHONPATH=$PWD .venv/bin/python tests/test_board_unchanged.py"""
import asyncio, time
import httpx
import ats_scraper as a

BODY = b'{"jobs":[{"title":"Data Analyst"}]}'


def make_server(etag_support: bool):
    state = {"calls": 0, "body": BODY}
    def handler(req: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        tag = 'W/"' + str(hash(state["body"]) & 0xffff) + '"'
        if etag_support and req.headers.get("If-None-Match") == tag:
            return httpx.Response(304, headers={"ETag": tag})
        hdrs = {"ETag": tag} if etag_support else {}
        return httpx.Response(200, content=state["body"], headers=hdrs)
    return state, handler


async def main():
    a._BOARD_STATE.clear()
    # 1. ETag server: second GET → 304 → empty dict, not parsed, counted
    st, h = make_server(True)
    async with a.BoardClient(transport=httpx.MockTransport(h)) as c:
        r1 = await c.get("https://api.example/board/acme")
        assert r1.json()["jobs"][0]["title"] == "Data Analyst", "first fetch is the real body"
        r2 = await c.get("https://api.example/board/acme")
        assert r2.status_code == 200 and r2.json() == {}, f"unchanged board must read as an empty payload, got {r2.content!r}"
        assert c.unchanged == 1, "client counts unchanged boards"
        assert st["calls"] == 2, "the server was asked twice (second with If-None-Match)"
        # board changes → full body again
        st["body"] = b'{"jobs":[{"title":"BI Analyst"}]}'
        r3 = await c.get("https://api.example/board/acme")
        assert r3.json()["jobs"][0]["title"] == "BI Analyst", "a changed board is parsed in full"
        assert c.unchanged == 1
    # 2. No-ETag server: identical bytes → empty payload (content print)
    a._BOARD_STATE.clear()
    st, h = make_server(False)
    async with a.BoardClient(transport=httpx.MockTransport(h)) as c:
        assert (await c.get("https://api.example/board/b")).json() != {}
        assert (await c.get("https://api.example/board/b")).json() == {}, "same bytes → empty"
        assert c.unchanged == 1
    # 3. shape is kept: list boards come back as [], HTML as ''
    a._BOARD_STATE.clear()
    async with a.BoardClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b'[{"id":1}]'))) as c:
        await c.get("https://api.example/list"); assert (await c.get("https://api.example/list")).json() == []
    async with a.BoardClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b'<html>jobs</html>'))) as c:
        await c.get("https://api.example/page"); r = await c.get("https://api.example/page")
        assert r.text == "" and r.status_code == 200, "HTML board unchanged → empty text"
    # 4. POST bodies are part of the key (Workday/UKG search per title)
    a._BOARD_STATE.clear()
    async with a.BoardClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b'{"jobPostings":[1]}'))) as c:
        await c.post("https://wd/jobs", json={"searchText": "Data Analyst"})
        r = await c.post("https://wd/jobs", json={"searchText": "BI Analyst"})
        assert r.json() == {"jobPostings": [1]}, "a different search body is a different board page"
        r = await c.post("https://wd/jobs", json={"searchText": "Data Analyst"})
        assert r.json() == {}, "same search body unchanged → empty"
    # 5. a print older than the TTL forces a full read (nothing can stay invisible forever)
    a._BOARD_STATE.clear()
    async with a.BoardClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=BODY))) as c:
        await c.get("https://api.example/ttl")
        k = next(iter(a._BOARD_STATE)); a._BOARD_STATE[k]["ts"] = time.monotonic() - a.BOARD_PRINT_TTL - 1
        assert (await c.get("https://api.example/ttl")).json() != {}, "expired print → full parse"
    # 6. errors never look like 'unchanged'
    a._BOARD_STATE.clear()
    async with a.BoardClient(transport=httpx.MockTransport(lambda r: httpx.Response(500, content=b'oops'))) as c:
        await c.get("https://api.example/err"); r = await c.get("https://api.example/err")
        assert r.status_code == 500 and c.unchanged == 0, "non-200 responses are passed through untouched"
    print("OK: unchanged boards are skipped (ETag 304 + content print), shapes kept, TTL forces a full read")

asyncio.run(main())
