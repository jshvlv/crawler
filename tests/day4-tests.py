import asyncio
import time

                                                                     
from crawler.rate_limiter import RateLimiter, RobotsParser
from crawler.fetcher import AsyncCrawler


async def test_rate_limiter_single_domain():
    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)                        
    t0 = time.time()
    await limiter.acquire("example.com")                                
    await limiter.acquire("example.com")                      
    await limiter.acquire("example.com")             
    elapsed = time.time() - t0
                                                                                
    assert elapsed >= 0.9, f"Rate limiter too fast: {elapsed:.2f}s"
    print("PASS: rate limiting single domain")


async def test_rate_limiter_multi_domain():
    limiter = RateLimiter(requests_per_second=1.0, per_domain=True)               
    t0 = time.time()

    async def hit(domain: str):
        await limiter.acquire(domain)

    await asyncio.gather(hit("a.com"), hit("b.com"))
    elapsed = time.time() - t0
                                                                                        
    assert elapsed < 0.5, f"Per-domain limiter serialized different domains: {elapsed:.2f}s"
    print("PASS: rate limiting multi domain")


async def test_robots_parser_can_fetch_and_delay():
    parser = RobotsParser(user_agent="TestBot")

                                           
    from urllib.robotparser import RobotFileParser

    rp = RobotFileParser()
    rp.parse(
        [
            "User-agent: TestBot",
            "Disallow: /private",
            "Crawl-delay: 2",
            "",
            "User-agent: *",
            "Disallow:",
        ]
    )
    domain = "example.com"
    parser._cache[domain] = rp                                    

               
    allowed = await parser.can_fetch("https://example.com/public", session=None, user_agent="TestBot")
    blocked = await parser.can_fetch("https://example.com/private/data", session=None, user_agent="TestBot")
    assert allowed is True
    assert blocked is False

                 
    delay = await parser.get_crawl_delay("https://example.com/", session=None, user_agent="TestBot")
    assert delay == 2.0, f"Expected crawl delay 2, got {delay}"
    print("PASS: robots parser can_fetch & crawl-delay")


async def test_crawler_respects_robots_block():
    crawler = AsyncCrawler(respect_robots=True, verify_ssl=False)

                                               
    from urllib.robotparser import RobotFileParser

    rp = RobotFileParser()
    rp.parse(["User-agent: *", "Disallow: /"])
    crawler.robots_parser._cache[crawler._get_domain("https://example.com")] = rp

    allowed = await crawler._is_allowed_by_robots("https://example.com/allowed")
    assert allowed is False, "URL должен быть заблокирован"

    await crawler.close()
    print("PASS: crawler blocks disallowed URLs")


async def main():
    await test_rate_limiter_single_domain()
    await test_rate_limiter_multi_domain()
    await test_robots_parser_can_fetch_and_delay()
    await test_crawler_respects_robots_block()
    print("\n=== ALL DAY 4 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())




