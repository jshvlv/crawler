import asyncio
from crawler.queue_manager import CrawlerQueue, SemaphoreManager
from crawler.fetcher import AsyncCrawler


async def test_queue_priorities():
    print("\n=== Test: Queue Priorities ===")
    
    queue = CrawlerQueue()
    
                                          
    queue.add_url("http://example.com/low1", priority=0)                        
    queue.add_url("http://example.com/high1", priority=10)                        
    queue.add_url("http://example.com/low2", priority=0)                         
    queue.add_url("http://example.com/high2", priority=10)                       
    queue.add_url("http://example.com/medium", priority=5)                        
    
                                                                
    urls_retrieved = []
    while True:
        url = await queue.get_next()
        if url is None:
            break
        urls_retrieved.append(url)
    
                                                                                            
    print(f"Retrieved URLs: {urls_retrieved}")
    
                                             
    assert "high1" in urls_retrieved[0] or "high2" in urls_retrieved[0], "High priority should come first"
    assert "high1" in urls_retrieved[1] or "high2" in urls_retrieved[1], "High priority should come first"
    
                                 
    assert "medium" in urls_retrieved[2], "Medium priority should come third"
    
                                              
    assert "low1" in urls_retrieved[3] or "low2" in urls_retrieved[3], "Low priority should come last"
    assert "low1" in urls_retrieved[4] or "low2" in urls_retrieved[4], "Low priority should come last"
    
    print("PASS: Queue priorities work correctly")


async def test_queue_no_duplicates():
    print("\n=== Test: Queue No Duplicates ===")
    
    queue = CrawlerQueue()
    
                                        
    result1 = queue.add_url("http://example.com/page")
    result2 = queue.add_url("http://example.com/page")
    
                                                          
    assert result1 is True, "First add should succeed"
    assert result2 is False, "Second add should fail (duplicate)"
    
                                                                  
    stats = queue.get_stats()
    assert stats["queued"] == 1, f"Should have 1 URL in queue, got {stats['queued']}"
    
    print("PASS: Duplicates are prevented")


async def test_queue_mark_processed_and_failed():
    print("\n=== Test: Mark Processed and Failed ===")
    
    queue = CrawlerQueue()
    
                   
    queue.add_url("http://example.com/success")
    queue.add_url("http://example.com/fail")
    
                                                          
    queue.mark_processed("http://example.com/success", {"title": "Test"})
    queue.mark_failed("http://example.com/fail", "Connection error")
    
                          
    stats = queue.get_stats()
    assert stats["processed"] == 1, f"Should have 1 processed, got {stats['processed']}"
    assert stats["failed"] == 1, f"Should have 1 failed, got {stats['failed']}"
    
                                                
    assert queue.is_visited("http://example.com/success"), "Success URL should be marked as visited"
    assert queue.is_visited("http://example.com/fail"), "Failed URL should be marked as visited"
    
                      
    processed = queue.get_processed_urls()
    failed = queue.get_failed_urls()
    
    assert "http://example.com/success" in processed, "Success URL should be in processed"
    assert "http://example.com/fail" in failed, "Failed URL should be in failed"
    assert failed["http://example.com/fail"] == "Connection error", "Error message should match"
    
    print("PASS: Mark processed and failed work correctly")


async def test_semaphore_manager():
    print("\n=== Test: Semaphore Manager ===")
    
    manager = SemaphoreManager(global_limit=2, per_domain_limit=1)
    
                                          
    await manager.acquire("example.com")
    await manager.acquire("test.com")
    
                                         
    stats = manager.get_active_stats()
    assert stats["global_active"] == 2, f"Should have 2 global active, got {stats['global_active']}"
    assert stats["per_domain"]["example.com"] == 1, "Should have 1 active for example.com"
    assert stats["per_domain"]["test.com"] == 1, "Should have 1 active for test.com"
    
                          
    manager.release("example.com")
    manager.release("test.com")
    
                                        
    stats = manager.get_active_stats()
    assert stats["global_active"] == 0, "Global active should be 0 after release"
    
    print("PASS: Semaphore manager works correctly")


async def test_crawler_max_depth():
    print("\n=== Test: Crawler Max Depth ===")
    
                                            
    crawler = AsyncCrawler(
        max_concurrent=2,
        max_depth=1,                            
        verify_ssl=False,
    )
    
                                          
    start_url = "https://example.com"
    start_domain = crawler._get_domain(start_url)
    
                              
                                                     
    assert crawler._should_process_url("https://example.com/page1", 0, {start_domain}) is True, "Depth 0 should pass"
    
                                                    
    assert crawler._should_process_url("https://example.com/page2", 1, {start_domain}) is True, "Depth 1 should pass"
    
                                                                     
    assert crawler._should_process_url("https://example.com/page3", 2, {start_domain}) is False, "Depth 2 should be rejected"
    
    await crawler.close()
    
    print("PASS: Max depth limitation works correctly")


async def test_crawler_url_filtering():
    print("\n=== Test: URL Filtering ===")
    
    start_url = "https://example.com"
    start_domain = "example.com"
    
                              
    crawler1 = AsyncCrawler(
        max_concurrent=2,
        same_domain_only=True,
        verify_ssl=False,
    )
    
                                           
    assert crawler1._should_process_url("https://example.com/page", 0, {start_domain}) is True, "Same domain should pass"
    
                                               
    assert crawler1._should_process_url("https://other.com/page", 0, {start_domain}) is False, "Other domain should be rejected"
    
    await crawler1.close()
    
                              
    crawler2 = AsyncCrawler(
        max_concurrent=2,
        exclude_patterns=[r"\.pdf$", r"/admin/"],
        verify_ssl=False,
    )
    
                                                      
    assert crawler2._should_process_url("https://example.com/page", 0, {start_domain}) is True, "Normal URL should pass"
    
                                                        
    assert crawler2._should_process_url("https://example.com/file.pdf", 0, {start_domain}) is False, "PDF should be excluded"
    assert crawler2._should_process_url("https://example.com/admin/", 0, {start_domain}) is False, "Admin should be excluded"
    
    await crawler2.close()
    
                              
    crawler3 = AsyncCrawler(
        max_concurrent=2,
        include_patterns=[r"/blog/", r"/news/"],
        verify_ssl=False,
    )
    
                                                   
    assert crawler3._should_process_url("https://example.com/blog/post", 0, {start_domain}) is True, "Blog URL should pass"
    assert crawler3._should_process_url("https://example.com/news/article", 0, {start_domain}) is True, "News URL should pass"
    
                                                         
    assert crawler3._should_process_url("https://example.com/page", 0, {start_domain}) is False, "Non-matching URL should be rejected"
    
    await crawler3.close()
    
    print("PASS: URL filtering works correctly")


async def test_crawler_no_duplicates():
    print("\n=== Test: No Duplicates in Crawler ===")
    
    crawler = AsyncCrawler(
        max_concurrent=2,
        verify_ssl=False,
    )
    
                             
    url1 = "https://example.com/page"
    url2 = "https://example.com/page"                                 
    url3 = "https://example.com/page/"                             
    url4 = "https://example.com/page#anchor"                       
    
                                               
    normalized1 = crawler._normalize_url(url1)
    normalized2 = crawler._normalize_url(url2)
    normalized3 = crawler._normalize_url(url3)
    normalized4 = crawler._normalize_url(url4)
    
                                                
    assert normalized1 == normalized2 == normalized3 == normalized4, "URLs should normalize to the same value"
    
                                                              
    result1 = crawler.queue.add_url(url1)
    result2 = crawler.queue.add_url(url2)
    result3 = crawler.queue.add_url(url3)
    result4 = crawler.queue.add_url(url4)
    
    assert result1 is True, "First URL should be added"
    assert result2 is False, "Duplicate should not be added"
    assert result3 is False, "Duplicate (trailing slash) should not be added"
    assert result4 is False, "Duplicate (fragment) should not be added"
    
                                                        
    stats = crawler.queue.get_stats()
    assert stats["queued"] == 1, f"Should have 1 URL in queue, got {stats['queued']}"
    
    await crawler.close()
    
    print("PASS: Duplicates are prevented in crawler")


async def main():
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






