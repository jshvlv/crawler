import asyncio
import logging
import re
import ssl
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse, urljoin

import aiohttp
from aiohttp import ClientError, ClientResponseError, ClientConnectionError

from crawler.parser import HTMLParser
from crawler.queue_manager import CrawlerQueue, SemaphoreManager
from crawler.rate_limiter import RateLimiter, RobotsParser
from crawler.retry import (
    RetryStrategy,
    TransientError,
    PermanentError,
    NetworkError,
    ParseError,
)
from crawler.storage import DataStorage


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class AsyncCrawler:
    
    def __init__(
        self,
        max_concurrent: int = 10,
        verify_ssl: bool = True,
        max_depth: int = 3,
        same_domain_only: bool = False,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        per_domain_limit: int = 2,
        requests_per_second: float = 1.0,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        respect_robots: bool = True,
        user_agent: str = "AsyncCrawler/1.0",
        retry_max_retries: int = 3,
        retry_backoff_factor: float = 2.0,
        retry_base_delay: float = 0.5,
        connect_timeout: float = 5.0,
        read_timeout: float = 10.0,
        total_timeout: float = 15.0,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_cooldown: float = 30.0,
        storage: Optional[DataStorage] = None,
    ):
        self.max_workers = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)                                             
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parser = HTMLParser()
        self.verify_ssl = verify_ssl
        self.user_agent = user_agent
                                                       
        self.timeouts = {
            "connect": connect_timeout,
            "read": read_timeout,
            "total": total_timeout,
            "connect_step": 0.5,                                                 
            "read_step": 1.0,                               
            "total_step": 1.0,                               
        }
        
                          
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        
                                               
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit
        )
                                    
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True,
            min_delay=min_delay,
            jitter=jitter,
            backoff_base=0.0,
        )
        self.robots_parser = RobotsParser(user_agent=user_agent)
        self.respect_robots = respect_robots
        self.blocked_count = 0                                        
                           
        self.retry_strategy = RetryStrategy(
            max_retries=retry_max_retries,
            backoff_factor=retry_backoff_factor,
            base_delay=retry_base_delay,
            retry_on=[TransientError, NetworkError],
        )
                                                             
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_cooldown = circuit_breaker_cooldown
        self.circuit_state: Dict[str, Dict[str, float]] = {}                                                   
                           
        self.error_stats: Dict[str, int] = {
            "transient": 0,
            "network": 0,
            "permanent": 0,
            "parse": 0,
            "other": 0,
        }
        self.permanent_urls: Dict[str, str] = {}                        
        
                                
        self.visited_urls: Set[str] = set()                                 
        self.url_depths: Dict[str, int] = {}                                   
        
                    
        self.start_time: Optional[float] = None
        self.last_progress_time: float = 0
        
                                        
        self.storage: Optional[DataStorage] = storage

    async def _create_session(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
                                                                               
        if not self.verify_ssl:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": self.user_agent},
            )
        else:
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": self.user_agent},
            )

    async def fetch_url(self, url: str) -> Dict[str, Any]:
        if self.session is None:
            await self._create_session()

        domain = self._get_domain(url)

                                                                      
        if self._circuit_is_open(domain):
            self.logger.warning(f"Circuit breaker open for {domain}, skipping {url}")
            return {"url": url, "error": "circuit_open"}

                                                 
        if self.respect_robots:
            allowed = await self._is_allowed_by_robots(url)
            if not allowed:
                self.blocked_count += 1
                self.logger.warning(f"Blocked by robots.txt: {url}")
                return {"url": url, "error": "blocked_by_robots"}

        self.logger.info(f"Start fetch: {url}")

        async def attempt_request(attempt: int) -> Dict[str, Any]:
            return await self._request_once(url, domain, attempt)

        try:
            result = await self.retry_strategy.execute_with_retry(attempt_request)
                                           
            self._record_success(domain)
            return result
        except Exception as e:                
                                             
            self._bump_error_stats(e, url)
                                                                                     
            self._record_failure(domain)
            self.logger.error(f"Request failed for {url}: {e}")
            return {"url": url, "error": str(e)}

    async def fetch_urls(self, urls: List[str]) -> Dict[str, Any]:
        if self.session is None:
            await self._create_session()

        tasks = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(*tasks)
        return {url: html for url, html in zip(urls, results)}

    async def fetch_and_parse(self, url: str) -> Dict[str, Any]:
        html_data = await self.fetch_url(url)
        if "error" in html_data:
            return html_data
        
                     
        parsed = await self.parser.parse_html(html_data["text"], url)
        
                                             
        parsed["crawled_at"] = datetime.now().isoformat()
        parsed["status_code"] = html_data.get("status", 0)
        parsed["content_type"] = html_data.get("content_type", "text/html")
        
        return parsed

    async def _request_once(self, url: str, domain: str, attempt: int) -> Dict[str, Any]:
                                                 
        await self.rate_limiter.acquire(domain)

                                                                               
        crawl_delay = 0.0
        if self.respect_robots:
            crawl_delay = await self.robots_parser.get_crawl_delay(url, self.session, self.user_agent)
        if crawl_delay > 0:
            await asyncio.sleep(crawl_delay)

                                                   
        timeout = self._compute_timeout(attempt)

        try:
            async with self.semaphore:
                async with self.session.get(url, timeout=timeout) as response:
                    content_type = response.headers.get("Content-Type", "")
                    status = response.status
                    text = await response.text()

                                                 
                    if status in (429, 503, 500):
                        raise TransientError(f"HTTP {status}")
                    if status in (404, 403, 401):
                        raise PermanentError(f"HTTP {status}")
                                                       
                    if 400 <= status < 500:
                        raise PermanentError(f"HTTP {status}")

                    self.logger.info(f"Success: {url} [{status}] (attempt {attempt})")
                    self.rate_limiter.record_success(domain)
                    return {"url": url, "status": status, "text": text, "content_type": content_type,}

        except asyncio.TimeoutError as e:
            self.rate_limiter.record_error(domain)
            raise TransientError(f"Timeout: {e}") from e
        except ClientConnectionError as e:
            self.rate_limiter.record_error(domain)
            raise NetworkError(f"Connection error: {e}") from e
        except ClientResponseError as e:
            self.rate_limiter.record_error(domain)
                                                                                   
            if e.status in (429, 503, 500):
                raise TransientError(f"HTTP {e.status}") from e
            if e.status in (404, 403, 401):
                raise PermanentError(f"HTTP {e.status}") from e
            raise PermanentError(f"HTTP {e.status}") from e
        except ClientError as e:
            self.rate_limiter.record_error(domain)
            raise NetworkError(f"Client error: {e}") from e
        except Exception as e:                
            self.rate_limiter.record_error(domain)
            raise

    def _get_domain(self, url: str) -> str:
        try:
            parsed = urlparse(url)
                                                      
            domain = parsed.netloc or parsed.path.split('/')[0]
                                     
            if ':' in domain:
                domain = domain.split(':')[0]
            return domain.lower()                                             
        except Exception:
                                                                       
            return url.lower()

    def _should_process_url(self, url: str, current_depth: int, start_domains: Set[str]) -> bool:
                                                                           
        if current_depth >= self.max_depth:
            return False
        
                                             
        normalized = self._normalize_url(url)
        if normalized in self.visited_urls or self.queue.is_visited(normalized):
            return False
        
                                                                          
        if self.same_domain_only:
            url_domain = self._get_domain(url)
                                                                        
            if url_domain not in start_domains:
                return False
        
                                                                                           
        for pattern in self.exclude_patterns:
            if pattern.search(url):
                return False
        
                                                                                                        
        if self.include_patterns:
            matches = any(pattern.search(url) for pattern in self.include_patterns)
            if not matches:
                return False
        
        return True

    def _compute_timeout(self, attempt: int) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self.timeouts["total"] + self.timeouts["total_step"] * attempt,
            connect=self.timeouts["connect"] + self.timeouts["connect_step"] * attempt,
            sock_read=self.timeouts["read"] + self.timeouts["read_step"] * attempt,
        )

    def _circuit_is_open(self, domain: str) -> bool:
        state = self.circuit_state.get(domain)
        if not state:
            return False
        return time.time() < state.get("open_until", 0)

    def _record_failure(self, domain: str) -> None:
        state = self.circuit_state.setdefault(domain, {"fail_count": 0, "open_until": 0})
        state["fail_count"] += 1
        if state["fail_count"] >= self.circuit_breaker_threshold:
            state["open_until"] = time.time() + self.circuit_breaker_cooldown
            self.logger.warning(
                f"Circuit breaker OPEN for {domain} for {self.circuit_breaker_cooldown}s "
                f"(failures: {state['fail_count']})"
            )

    def _record_success(self, domain: str) -> None:
        if domain in self.circuit_state:
            self.circuit_state[domain]["fail_count"] = 0
            self.circuit_state[domain]["open_until"] = 0

    def _bump_error_stats(self, err: Exception, url: str) -> None:
        if isinstance(err, TransientError):
            self.error_stats["transient"] += 1
        elif isinstance(err, NetworkError):
            self.error_stats["network"] += 1
        elif isinstance(err, PermanentError):
            self.error_stats["permanent"] += 1
            self.permanent_urls[url] = str(err)
        elif isinstance(err, ParseError):
            self.error_stats["parse"] += 1
        else:
            self.error_stats["other"] += 1

    async def _is_allowed_by_robots(self, url: str) -> bool:
        if self.session is None:
            await self._create_session()
        return await self.robots_parser.can_fetch(url, self.session, self.user_agent)

    def _normalize_url(self, url: str) -> str:
                                        
        if '#' in url:
            url = url.split('#')[0]
        
                                                       
        if url.endswith('/') and len(url) > 1:
            url = url.rstrip('/')
        
        return url

    def _print_progress(self, stats: Dict, elapsed: float) -> None:
                                                          
        speed = stats["processed"] / elapsed if elapsed > 0 else 0
                                                                         
        avg_delay = (sum(self.rate_limiter.delays) / len(self.rate_limiter.delays)) if self.rate_limiter.delays else 0.0
        errors_total = sum(self.error_stats.values())
        
                                     
        progress_msg = (
            f"Progress: Processed={stats['processed']} | "
            f"Queued={stats['queued']} | "
            f"Failed={stats['failed']} | "
            f"Blocked={self.blocked_count} | "
            f"Errors={errors_total} | "
            f"Speed={speed:.2f} pages/sec | "
            f"Avg delay={avg_delay:.2f}s | "
            f"Time={elapsed:.1f}s"
        )
        
                                                                 
        print(f"\r{progress_msg}", end="", flush=True)

    async def crawl(
        self,
        start_urls: List[str],
        max_pages: int = 100,
        same_domain_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
                                                                 
        if same_domain_only is not None:
            self.same_domain_only = same_domain_only
        
                                                          
        start_domains = {self._get_domain(url) for url in start_urls}
        
                                    
        if self.session is None:
            await self._create_session()
        
                                                                        
        for url in start_urls:
            normalized = self._normalize_url(url)
            if self.queue.add_url(normalized, priority=0):
                self.url_depths[normalized] = 0                                 
        
                                              
        self.start_time = time.time()
        self.last_progress_time = time.time()
        
                                                   
        tasks = []
        
                                 
        while True:
                                               
            url = await self.queue.get_next()
            
                                                                
            if url is None:
                                                     
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
                
                                                                  
                url = await self.queue.get_next()
                if url is None:
                    break
            
                                                     
            stats = self.queue.get_stats()
            if stats["processed"] >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
                                           
            current_depth = self.url_depths.get(url, 0)
            
                                         
            self.visited_urls.add(url)
            
                                                       
            domain = self._get_domain(url)
            
                                              
            async def process_url(url: str, depth: int, domain: str) -> None:
                try:
                                                                 
                    await self.semaphore_manager.acquire(domain)
                    
                    try:
                                                     
                        parsed_data = await self.fetch_and_parse(url)
                        
                                             
                        if "error" in parsed_data:
                                                                 
                            error_msg = parsed_data.get("error", "unknown_error")
                            self.queue.mark_failed(url, error_msg)
                        else:
                                                                     
                            self.queue.mark_processed(url, parsed_data)
                            
                                                                         
                            if self.storage:
                                try:
                                    await self.storage.save(parsed_data)
                                except Exception as e:
                                                                                                 
                                    self.logger.error(f"Error saving data for {url}: {e}", exc_info=True)
                            
                                                          
                            links = parsed_data.get("links", [])
                            
                                                                  
                            for link in links:
                                                    
                                normalized_link = self._normalize_url(link)
                                
                                                                             
                                if self._should_process_url(normalized_link, depth + 1, start_domains):
                                                                                         
                                                                        
                                    priority = -(depth + 1)
                                    if self.queue.add_url(normalized_link, priority=priority):
                                                                          
                                        self.url_depths[normalized_link] = depth + 1
                                        self.logger.debug(f"Added to queue: {normalized_link} (depth: {depth + 1})")
                    
                    finally:
                                                                               
                        self.semaphore_manager.release(domain)
                        
                                                             
                        current_time = time.time()
                        if current_time - self.last_progress_time >= 0.5:
                            stats = self.queue.get_stats()
                            elapsed = current_time - (self.start_time or current_time)
                            self._print_progress(stats, elapsed)
                            self.last_progress_time = current_time
                
                except Exception as e:
                                                     
                    self.logger.error(f"Error processing {url}: {e}", exc_info=True)
                    self.queue.mark_failed(url, str(e))
                                                          
                    try:
                        self.semaphore_manager.release(domain)
                    except Exception:
                        pass
            
                                                                    
            task = asyncio.create_task(process_url(url, current_depth, domain))
            tasks.append(task)
            
                                                                    
                                                            
            if len(tasks) >= self.max_workers * 2:
                                                      
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                                                      
                tasks = list(pending)
        
                                               
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
                                   
        final_stats = self.queue.get_stats()
        elapsed = time.time() - (self.start_time or time.time())
        self._print_progress(final_stats, elapsed)
        print()                                
        
                             
        processed_urls = self.queue.get_processed_urls()
        failed_urls = self.queue.get_failed_urls()
        
        return {
            "processed": processed_urls,
            "failed": failed_urls,
            "stats": final_stats,
            "elapsed_time": elapsed,
        }

    async def close(self) -> None:
                               
        if self.session:
            await self.session.close()
        
                                                   
        if self.storage:
            try:
                await self.storage.close()
            except Exception as e:
                self.logger.error(f"Error closing storage: {e}", exc_info=True)


async def demo_sequential_vs_parallel() -> None:
    urls = [
        "https://example.com",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/get",
        "https://httpbin.org/uuid",
        "https://httpbin.org/status/404",
        "https://httpbin.org/status/500",
    ]

    crawler_seq = AsyncCrawler(max_concurrent=1)
    start = time.time()
    seq_results = await crawler_seq.fetch_urls(urls)
    seq_time = time.time() - start
    await crawler_seq.close()

    crawler_par = AsyncCrawler(max_concurrent=3)
    start = time.time()
    par_results = await crawler_par.fetch_urls(urls)
    par_time = time.time() - start
    await crawler_par.close()

    print("=== Sequential results ===")
    for url, data in seq_results.items():
        status = data.get("status") or data.get("error")
        print(f"{url} -> {status}")
    print(f"Sequential time: {seq_time:.2f}s")

    print("\n=== Parallel results ===")
    for url, data in par_results.items():
        status = data.get("status") or data.get("error")
        print(f"{url} -> {status}")
    print(f"Parallel time: {par_time:.2f}s")

    speedup = seq_time / par_time if par_time > 0 else float("inf")
    print(f"\nSpeedup: {speedup:.2f}x")


async def main():
    import json
    import os
    from crawler.storage import JSONStorage, CSVStorage, SQLiteStorage
    
                                           
    json_storage = JSONStorage("demo_results.json", buffer_size=5)
    csv_storage = CSVStorage("demo_results.csv", buffer_size=5)
    db_storage = SQLiteStorage("demo_results.db", batch_size=5)
    
                                       
    await db_storage.init_db()
    
                                          
    crawler = AsyncCrawler(
        max_concurrent=3,                                              
        verify_ssl=False,                                                      
        max_depth=2,                                                       
        same_domain_only=True,                                        
        per_domain_limit=2,                                                          
        storage=json_storage,                                  
    )
    
                              
    start_urls = ["https://petstore.swagger.io/"]
    
    print("=== Starting crawl with storage ===")
    print(f"Start URLs: {start_urls}")
    print(f"Max depth: {crawler.max_depth}")
    print(f"Max pages: 10")
    print(f"Storage: JSON (demo_results.json)")
    print()
    
                        
    results = await crawler.crawl(
        start_urls=start_urls,
        max_pages=10,
        same_domain_only=True,
    )
    
                                                   
    await crawler.close()
    
                                               
    print("\n=== Saving to CSV and SQLite ===")
    processed = results.get("processed", {})
    
                     
    for url, data in list(processed.items())[:5]:                    
        await csv_storage.save(data)
    await csv_storage.close()
    print(f"CSV saved: {os.path.abspath('demo_results.csv')}")
    
                        
    for url, data in list(processed.items())[:5]:                    
        await db_storage.save(data)
    
                                                                                                    
    db_stats = await db_storage.get_stats()
    print(f"SQLite stats: {db_stats}")
    
                              
    await db_storage.close()
    print(f"SQLite saved: {os.path.abspath('demo_results.db')}")

                        
    print("\n=== Crawl Results ===")
    print(f"Processed pages: {len(results['processed'])}")
    print(f"Failed pages: {len(results['failed'])}")
    print(f"Total time: {results['elapsed_time']:.2f}s")
    print(f"Stats: {results['stats']}")
    
                                     
    summary = []
    for url, data in results['processed'].items():
        item = {
            "url": url,
            "title": data.get("title", ""),
            "text_length": len(data.get("text", "")),
            "text": data.get("text", ""),
            "links_count": len(data.get("links", [])),
            "images_count": len(data.get("images", [])),
            "depth": crawler.url_depths.get(url, 0),
        }
        summary.append(item)
    
                                 
    with open("demo_results.json", "w", encoding="utf-8") as f:
        json.dump({
            "summary": summary,
            "stats": results['stats'],
            "failed": results['failed'],
        }, f, ensure_ascii=False, indent=2)
    print("Results saved to: demo_results.json")

                                          
    print("\n=== Sample Pages ===")
    for i, (url, data) in enumerate(list(results['processed'].items())[:5]):
        print(f"\n{i+1}. {url}")
        print(f"   Title: {data.get('title', 'N/A')[:50]}")
        print(f"   Links: {len(data.get('links', []))}")
        print(f"   Depth: {crawler.url_depths.get(url, 0)}")
        print(f"   Text: {data['text']}")


if __name__ == "__main__":
    asyncio.run(main())