"""
Тесты для Дня 4: Rate limiting и robots.txt.

Покрываем:
- rate limiting для одного домена
- rate limiting для разных доменов
- парсинг/блокировку robots.txt
- применение crawl-delay
"""
import asyncio
import time

# Импортируем локальные модули; при запуске добавьте PYTHONPATH=./src
from crawler.rate_limiter import RateLimiter, RobotsParser
from crawler.fetcher import AsyncCrawler


async def test_rate_limiter_single_domain():
    """Проверяем, что для одного домена соблюдается интервал запросов."""
    limiter = RateLimiter(requests_per_second=2.0, per_domain=True)  # 0.5s между запросами
    t0 = time.time()
    await limiter.acquire("example.com")  # первый запрос — без ожидания
    await limiter.acquire("example.com")  # должен ждать ~0.5s
    await limiter.acquire("example.com")  # ещё ~0.5s
    elapsed = time.time() - t0
    # Ожидаем, что суммарная задержка >= примерно 1.0 секунды (две паузы по 0.5)
    assert elapsed >= 0.9, f"Rate limiter too fast: {elapsed:.2f}s"
    print("PASS: rate limiting single domain")


async def test_rate_limiter_multi_domain():
    """Проверяем, что разные домены лимитируются отдельно (per_domain=True)."""
    limiter = RateLimiter(requests_per_second=1.0, per_domain=True)  # 1s на домен
    t0 = time.time()

    async def hit(domain: str):
        await limiter.acquire(domain)

    await asyncio.gather(hit("a.com"), hit("b.com"))
    elapsed = time.time() - t0
    # Поскольку домены разные, ожидание не должно суммироваться (должно быть близко к 0)
    assert elapsed < 0.5, f"Per-domain limiter serialized different domains: {elapsed:.2f}s"
    print("PASS: rate limiting multi domain")


async def test_robots_parser_can_fetch_and_delay():
    """Проверяем can_fetch и crawl-delay без сетевых запросов (заполняем кэш вручную)."""
    parser = RobotsParser(user_agent="TestBot")

    # Создаём парсер и кладём в кэш вручную
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
    parser._cache[domain] = rp  # заполняем кэш напрямую для теста

    # can_fetch
    allowed = await parser.can_fetch("https://example.com/public", session=None, user_agent="TestBot")
    blocked = await parser.can_fetch("https://example.com/private/data", session=None, user_agent="TestBot")
    assert allowed is True
    assert blocked is False

    # crawl-delay
    delay = await parser.get_crawl_delay("https://example.com/", session=None, user_agent="TestBot")
    assert delay == 2.0, f"Expected crawl delay 2, got {delay}"
    print("PASS: robots parser can_fetch & crawl-delay")


async def test_crawler_respects_robots_block():
    """Краулер должен блокировать URL, запрещённые robots.txt."""
    crawler = AsyncCrawler(respect_robots=True, verify_ssl=False)

    # Подменяем кэш robots, чтобы запретить всё
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




