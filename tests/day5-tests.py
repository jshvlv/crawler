"""
Тесты для Дня 5: обработка ошибок и повторы с backoff.

Проверяем:
- Повторы при TransientError с успешным завершением
- Отсутствие повторов при PermanentError
- Экспоненциальный backoff (по количеству задержек)
- Circuit breaker (открывается после порога)
"""
import asyncio
import time

from crawler.retry import RetryStrategy, TransientError, PermanentError, NetworkError
from crawler.fetcher import AsyncCrawler


async def test_retry_transient_success():
    """TransientError повторяется и в итоге завершается успехом."""
    attempts = {"count": 0}

    async def flaky(attempt: int):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise TransientError("temporary")
        return "ok"

    retry = RetryStrategy(max_retries=5, backoff_factor=1.5, base_delay=0.1, retry_on=[TransientError])
    result = await retry.execute_with_retry(flaky)
    assert result == "ok"
    assert attempts["count"] == 3, f"Expected 3 attempts, got {attempts['count']}"
    assert retry.retry_successes == 1, "Should register one successful retry"
    print("PASS: transient retries succeed")


async def test_no_retry_on_permanent():
    """PermanentError не повторяется."""
    attempts = {"count": 0}

    async def always_fail(attempt: int):
        attempts["count"] += 1
        raise PermanentError("perm")

    retry = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.1, retry_on=[TransientError, NetworkError])
    t0 = time.time()
    try:
        await retry.execute_with_retry(always_fail)
    except PermanentError:
        pass
    else:
        assert False, "PermanentError should be raised"

    elapsed = time.time() - t0
    assert attempts["count"] == 1, "Permanent errors should not be retried"
    assert elapsed < 0.2, "Should not wait on permanent errors"
    print("PASS: permanent errors are not retried")


async def test_backoff_delays_count():
    """Проверяем, что накапливаются задержки (экспоненциальный рост числа задержек)."""
    async def fail_all(attempt: int):
        raise TransientError("always")

    retry = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.05, retry_on=[TransientError])
    try:
        await retry.execute_with_retry(fail_all)
    except TransientError:
        pass
    else:
        assert False, "Should raise after exhausting retries"

    # Должны быть записи задержек по количеству повторов (max_retries)
    assert len(retry.retry_delays) == retry.max_retries, "Backoff delays should be recorded for each retry"
    # Проверяем экспоненциальный рост (каждая следующая >= предыдущей)
    for prev, nxt in zip(retry.retry_delays, retry.retry_delays[1:]):
        assert nxt >= prev, "Backoff delays should be non-decreasing"
    print("PASS: backoff delays recorded and non-decreasing")


async def test_circuit_breaker_opens():
    """Проверяем открытие circuit breaker после порога ошибок."""
    crawler = AsyncCrawler(
        max_concurrent=1,
        respect_robots=False,
        verify_ssl=False,
        circuit_breaker_threshold=2,
        circuit_breaker_cooldown=1.0,
    )
    domain = "example.com"
    crawler._record_failure(domain)
    assert not crawler._circuit_is_open(domain), "Breaker should stay closed after first failure"
    crawler._record_failure(domain)
    assert crawler._circuit_is_open(domain), "Breaker should open after threshold"
    await crawler.close()
    print("PASS: circuit breaker opens after threshold")


async def main():
    await test_retry_transient_success()
    await test_no_retry_on_permanent()
    await test_backoff_delays_count()
    await test_circuit_breaker_opens()
    print("\n=== ALL DAY 5 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())




