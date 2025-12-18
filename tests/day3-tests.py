"""
Тесты для Дня 3: Управление конкурентностью и очередями.

Проверяет:
- Очередь с приоритетами
- Ограничение глубины обхода
- Фильтрацию URL
- Отсутствие дубликатов в visited_urls
"""
import asyncio
from crawler.queue_manager import CrawlerQueue, SemaphoreManager
from crawler.fetcher import AsyncCrawler


async def test_queue_priorities():
    """
    Тест: Очередь корректно обрабатывает приоритеты.
    
    Проверяет, что URL с более высоким приоритетом обрабатываются первыми.
    """
    print("\n=== Test: Queue Priorities ===")
    
    queue = CrawlerQueue()
    
    # Добавляем URL с разными приоритетами
    queue.add_url("http://example.com/low1", priority=0)      # Низкий приоритет
    queue.add_url("http://example.com/high1", priority=10)     # Высокий приоритет
    queue.add_url("http://example.com/low2", priority=0)       # Низкий приоритет
    queue.add_url("http://example.com/high2", priority=10)    # Высокий приоритет
    queue.add_url("http://example.com/medium", priority=5)     # Средний приоритет
    
    # Получаем URL из очереди - должны идти в порядке приоритета
    urls_retrieved = []
    while True:
        url = await queue.get_next()
        if url is None:
            break
        urls_retrieved.append(url)
    
    # Проверяем порядок: сначала высокий приоритет (10), потом средний (5), потом низкий (0)
    print(f"Retrieved URLs: {urls_retrieved}")
    
    # Первые два должны быть с приоритетом 10
    assert "high1" in urls_retrieved[0] or "high2" in urls_retrieved[0], "High priority should come first"
    assert "high1" in urls_retrieved[1] or "high2" in urls_retrieved[1], "High priority should come first"
    
    # Средний должен быть третьим
    assert "medium" in urls_retrieved[2], "Medium priority should come third"
    
    # Низкие приоритеты должны быть последними
    assert "low1" in urls_retrieved[3] or "low2" in urls_retrieved[3], "Low priority should come last"
    assert "low1" in urls_retrieved[4] or "low2" in urls_retrieved[4], "Low priority should come last"
    
    print("PASS: Queue priorities work correctly")


async def test_queue_no_duplicates():
    """
    Тест: Очередь не добавляет дубликаты.
    
    Проверяет, что один и тот же URL не добавляется дважды.
    """
    print("\n=== Test: Queue No Duplicates ===")
    
    queue = CrawlerQueue()
    
    # Добавляем один и тот же URL дважды
    result1 = queue.add_url("http://example.com/page")
    result2 = queue.add_url("http://example.com/page")
    
    # Первое добавление должно быть успешным, второе - нет
    assert result1 is True, "First add should succeed"
    assert result2 is False, "Second add should fail (duplicate)"
    
    # Проверяем статистику - должен быть только один URL в очереди
    stats = queue.get_stats()
    assert stats["queued"] == 1, f"Should have 1 URL in queue, got {stats['queued']}"
    
    print("PASS: Duplicates are prevented")


async def test_queue_mark_processed_and_failed():
    """
    Тест: Очередь корректно помечает URL как обработанные и неудачные.
    
    Проверяет методы mark_processed и mark_failed.
    """
    print("\n=== Test: Mark Processed and Failed ===")
    
    queue = CrawlerQueue()
    
    # Добавляем URL
    queue.add_url("http://example.com/success")
    queue.add_url("http://example.com/fail")
    
    # Помечаем один как обработанный, другой как неудачный
    queue.mark_processed("http://example.com/success", {"title": "Test"})
    queue.mark_failed("http://example.com/fail", "Connection error")
    
    # Проверяем статистику
    stats = queue.get_stats()
    assert stats["processed"] == 1, f"Should have 1 processed, got {stats['processed']}"
    assert stats["failed"] == 1, f"Should have 1 failed, got {stats['failed']}"
    
    # Проверяем, что URL помечены как посещённые
    assert queue.is_visited("http://example.com/success"), "Success URL should be marked as visited"
    assert queue.is_visited("http://example.com/fail"), "Failed URL should be marked as visited"
    
    # Проверяем данные
    processed = queue.get_processed_urls()
    failed = queue.get_failed_urls()
    
    assert "http://example.com/success" in processed, "Success URL should be in processed"
    assert "http://example.com/fail" in failed, "Failed URL should be in failed"
    assert failed["http://example.com/fail"] == "Connection error", "Error message should match"
    
    print("PASS: Mark processed and failed work correctly")


async def test_semaphore_manager():
    """
    Тест: SemaphoreManager корректно управляет семафорами.
    
    Проверяет глобальное ограничение и ограничение по доменам.
    """
    print("\n=== Test: Semaphore Manager ===")
    
    manager = SemaphoreManager(global_limit=2, per_domain_limit=1)
    
    # Получаем семафоры для разных доменов
    await manager.acquire("example.com")
    await manager.acquire("test.com")
    
    # Проверяем статистику активных задач
    stats = manager.get_active_stats()
    assert stats["global_active"] == 2, f"Should have 2 global active, got {stats['global_active']}"
    assert stats["per_domain"]["example.com"] == 1, "Should have 1 active for example.com"
    assert stats["per_domain"]["test.com"] == 1, "Should have 1 active for test.com"
    
    # Освобождаем семафоры
    manager.release("example.com")
    manager.release("test.com")
    
    # Проверяем, что счётчики обнулились
    stats = manager.get_active_stats()
    assert stats["global_active"] == 0, "Global active should be 0 after release"
    
    print("PASS: Semaphore manager works correctly")


async def test_crawler_max_depth():
    """
    Тест: Краулер корректно ограничивает глубину обхода.
    
    Проверяет, что URL с глубиной больше max_depth не обрабатываются.
    """
    print("\n=== Test: Crawler Max Depth ===")
    
    # Создаём краулер с ограничением глубины
    crawler = AsyncCrawler(
        max_concurrent=2,
        max_depth=1,  # Только 1 уровень глубины
        verify_ssl=False,
    )
    
    # Извлекаем домен для проверки фильтра
    start_url = "https://example.com"
    start_domain = crawler._get_domain(start_url)
    
    # Тестируем фильтр глубины
    # URL с глубиной 0 (стартовый) - должен проходить
    assert crawler._should_process_url("https://example.com/page1", 0, {start_domain}) is True, "Depth 0 should pass"
    
    # URL с глубиной 1 - должен проходить (максимум)
    assert crawler._should_process_url("https://example.com/page2", 1, {start_domain}) is True, "Depth 1 should pass"
    
    # URL с глубиной 2 - должен быть отклонён (превышает max_depth=1)
    assert crawler._should_process_url("https://example.com/page3", 2, {start_domain}) is False, "Depth 2 should be rejected"
    
    await crawler.close()
    
    print("PASS: Max depth limitation works correctly")


async def test_crawler_url_filtering():
    """
    Тест: Краулер корректно фильтрует URL.
    
    Проверяет фильтры same_domain_only, exclude_patterns, include_patterns.
    """
    print("\n=== Test: URL Filtering ===")
    
    start_url = "https://example.com"
    start_domain = "example.com"
    
    # Тест 1: same_domain_only
    crawler1 = AsyncCrawler(
        max_concurrent=2,
        same_domain_only=True,
        verify_ssl=False,
    )
    
    # URL того же домена - должен проходить
    assert crawler1._should_process_url("https://example.com/page", 0, {start_domain}) is True, "Same domain should pass"
    
    # URL другого домена - должен быть отклонён
    assert crawler1._should_process_url("https://other.com/page", 0, {start_domain}) is False, "Other domain should be rejected"
    
    await crawler1.close()
    
    # Тест 2: exclude_patterns
    crawler2 = AsyncCrawler(
        max_concurrent=2,
        exclude_patterns=[r"\.pdf$", r"/admin/"],
        verify_ssl=False,
    )
    
    # URL без исключающих паттернов - должен проходить
    assert crawler2._should_process_url("https://example.com/page", 0, {start_domain}) is True, "Normal URL should pass"
    
    # URL с исключающим паттерном - должен быть отклонён
    assert crawler2._should_process_url("https://example.com/file.pdf", 0, {start_domain}) is False, "PDF should be excluded"
    assert crawler2._should_process_url("https://example.com/admin/", 0, {start_domain}) is False, "Admin should be excluded"
    
    await crawler2.close()
    
    # Тест 3: include_patterns
    crawler3 = AsyncCrawler(
        max_concurrent=2,
        include_patterns=[r"/blog/", r"/news/"],
        verify_ssl=False,
    )
    
    # URL с включающим паттерном - должен проходить
    assert crawler3._should_process_url("https://example.com/blog/post", 0, {start_domain}) is True, "Blog URL should pass"
    assert crawler3._should_process_url("https://example.com/news/article", 0, {start_domain}) is True, "News URL should pass"
    
    # URL без включающего паттерна - должен быть отклонён
    assert crawler3._should_process_url("https://example.com/page", 0, {start_domain}) is False, "Non-matching URL should be rejected"
    
    await crawler3.close()
    
    print("PASS: URL filtering works correctly")


async def test_crawler_no_duplicates():
    """
    Тест: Краулер не обрабатывает дубликаты.
    
    Проверяет, что один и тот же URL не добавляется в очередь дважды.
    """
    print("\n=== Test: No Duplicates in Crawler ===")
    
    crawler = AsyncCrawler(
        max_concurrent=2,
        verify_ssl=False,
    )
    
    # Добавляем URL в очередь
    url1 = "https://example.com/page"
    url2 = "https://example.com/page"  # Дубликат (после нормализации)
    url3 = "https://example.com/page/"  # Дубликат (trailing slash)
    url4 = "https://example.com/page#anchor"  # Дубликат (фрагмент)
    
    # Все эти URL должны нормализоваться в один
    normalized1 = crawler._normalize_url(url1)
    normalized2 = crawler._normalize_url(url2)
    normalized3 = crawler._normalize_url(url3)
    normalized4 = crawler._normalize_url(url4)
    
    # Проверяем, что все нормализуются одинаково
    assert normalized1 == normalized2 == normalized3 == normalized4, "URLs should normalize to the same value"
    
    # Добавляем в очередь - только первый должен быть добавлен
    result1 = crawler.queue.add_url(url1)
    result2 = crawler.queue.add_url(url2)
    result3 = crawler.queue.add_url(url3)
    result4 = crawler.queue.add_url(url4)
    
    assert result1 is True, "First URL should be added"
    assert result2 is False, "Duplicate should not be added"
    assert result3 is False, "Duplicate (trailing slash) should not be added"
    assert result4 is False, "Duplicate (fragment) should not be added"
    
    # Проверяем статистику - должен быть только один URL
    stats = crawler.queue.get_stats()
    assert stats["queued"] == 1, f"Should have 1 URL in queue, got {stats['queued']}"
    
    await crawler.close()
    
    print("PASS: Duplicates are prevented in crawler")


async def main():
    """
    Запускает все тесты для Дня 3.
    """
    print("=== Running Day 3 Tests ===")
    
    await test_queue_priorities()
    await test_queue_no_duplicates()
    await test_queue_mark_processed_and_failed()
    await test_semaphore_manager()
    await test_crawler_max_depth()
    await test_crawler_url_filtering()
    await test_crawler_no_duplicates()
    
    print("\n=== ALL DAY 3 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())






