import asyncio
import time

from crawler.fetcher import AsyncCrawler

async def test_valid_url():
    print("\n=== Test: valid URL ===")
    crawler = AsyncCrawler()
    result = await crawler.fetch_urls(["https://example.com"])
    await crawler.close()

    data = result["https://example.com"]

    if "text" in data and data.get("status") == 200 and len(data["text"]) > 0:
        print("PASS: valid URL loads correctly")
    else:
        print("FAIL: valid URL test failed")


async def test_invalid_url():
    print("\n=== Test: invalid URL ===")
    crawler = AsyncCrawler()
    result = await crawler.fetch_urls(["https://nonexistent.domain.test"])
    await crawler.close()

    data = result["https://nonexistent.domain.test"]

    if "error" in data:
        print("PASS: invalid URL handled correctly")
    else:
        print("FAIL: invalid URL test failed")


async def test_timeout():
    crawler = AsyncCrawler(max_concurrent=2)
    await crawler._create_session()

    result = await crawler.fetch_url("https://nonexistent.domain.test")
    assert "error" in result

    await crawler.close()

async def test_parallel_vs_sequential():
    print("\n=== Test: parallel vs sequential ===")

    urls = [
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
    ]

                             
    crawler_seq = AsyncCrawler(max_concurrent=1)
    start = time.time()
    await crawler_seq.fetch_urls(urls)
    seq_time = time.time() - start
    await crawler_seq.close()

                         
    crawler_par = AsyncCrawler(max_concurrent=3)
    start = time.time()
    await crawler_par.fetch_urls(urls)
    par_time = time.time() - start
    await crawler_par.close()

    print(f"Sequential: {seq_time:.2f}s, Parallel: {par_time:.2f}s")

    if seq_time > par_time * 2:
        print("PASS: parallel requests are significantly faster")
    else:
        print("FAIL: parallel test failed")


async def main():
    print("=== Running built-in tests for Day 1 ===")

    await test_valid_url()
    await test_invalid_url()
    await test_timeout()
    await test_parallel_vs_sequential()

    print("\n=== ALL TESTS FINISHED ===")


if __name__ == "__main__":
    asyncio.run(main())