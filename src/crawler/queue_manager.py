import asyncio
import logging
from collections import defaultdict
from typing import Optional, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class CrawlerQueue:
    
    def __init__(self):
                                                        
                                                                     
        self._priority_queues: Dict[int, list] = defaultdict(list)
        
                                                                        
        self._queued_urls: set = set()
        
                                                             
        self._processed_count: int = 0
        self._failed_count: int = 0
        
                                                    
        self._processed_urls: Dict[str, Dict] = {}
        
                                                       
        self._failed_urls: Dict[str, str] = {}
        
                                                                 
        self._lock = asyncio.Lock()
    
    def add_url(self, url: str, priority: int = 0) -> bool:
                                                                                       
        normalized_url = self._normalize_url(url)
        
                                               
        if normalized_url in self._queued_urls:
            return False
        
                                                         
        self._priority_queues[priority].append(normalized_url)
        self._queued_urls.add(normalized_url)
        
        logger.debug(f"Added URL to queue: {normalized_url} (priority: {priority})")
        return True
    
    def _normalize_url(self, url: str) -> str:
                                        
        if '#' in url:
            url = url.split('#')[0]
        
                                                       
        if url.endswith('/') and len(url) > 1:
            url = url.rstrip('/')
        
        normalized_url = url
        logger.debug(f"Normalized URL: {normalized_url}")

        return normalized_url
    
    async def get_next(self) -> Optional[str]:
        async with self._lock:                                     
                                                 
            if not self._queued_urls:
                return None
            
                                                            
            if not self._priority_queues:
                return None
            
            max_priority = max(self._priority_queues.keys())

                                                                           
            if self._priority_queues[max_priority]:
                url = self._priority_queues[max_priority].pop(0)
                self._queued_urls.remove(url)
                
                                                              
                if not self._priority_queues[max_priority]:
                    del self._priority_queues[max_priority]
                
                logger.debug(f"Retrieved URL from queue: {url} (priority: {max_priority})")
                return url
        
        return None
    
    def mark_processed(self, url: str, result: Optional[Dict] = None) -> None:
        normalized_url = self._normalize_url(url)
        self._processed_count += 1
        
        if result:
            self._processed_urls[normalized_url] = result
        else:
            self._processed_urls[normalized_url] = {"status": "processed"}
        
        logger.debug(f"Marked as processed: {normalized_url}")
    
    def mark_failed(self, url: str, error: str) -> None:
        normalized_url = self._normalize_url(url)
        self._failed_count += 1
        self._failed_urls[normalized_url] = error
        
        logger.debug(f"Marked as failed: {normalized_url} - {error}")
    
    def get_stats(self) -> Dict:
        queued_count = len(self._queued_urls)
        
        return {
            "queued": queued_count,
            "processed": self._processed_count,
            "failed": self._failed_count,
            "total": queued_count + self._processed_count + self._failed_count,
            "processed_urls": len(self._processed_urls),
            "failed_urls": len(self._failed_urls),
        }
    
    def is_visited(self, url: str) -> bool:
        normalized_url = self._normalize_url(url)
        return normalized_url in self._processed_urls or normalized_url in self._failed_urls
    
    def get_processed_urls(self) -> Dict[str, Dict]:
        return self._processed_urls.copy()
    
    def get_failed_urls(self) -> Dict[str, str]:
        return self._failed_urls.copy()


class SemaphoreManager:
    
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 2):
                                                          
        self._global_semaphore = asyncio.Semaphore(global_limit)
        
                                                                  
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        
                                 
        self._per_domain_limit = per_domain_limit
        
                                                                     
        self._lock = asyncio.Lock()
        
                                 
        self._active_global = 0
        self._active_per_domain: Dict[str, int] = defaultdict(int)
    
    def _get_domain(self, url: str) -> str:
        try:
            parsed = urlparse(url)
                                                      
            domain = parsed.netloc or parsed.path.split('/')[0]
                                     
            if ':' in domain:
                domain = domain.split(':')[0]
            return domain
        except Exception:
                                                                       
            return url
    
    async def acquire(self, domain: str) -> None:
                                                                 
        await self._global_semaphore.acquire()
        self._active_global += 1
        
                                                 
        async with self._lock:
            if domain not in self._domain_semaphores:
                                                                       
                self._domain_semaphores[domain] = asyncio.Semaphore(self._per_domain_limit)
        
                                                                             
        domain_semaphore = self._domain_semaphores[domain]
        await domain_semaphore.acquire()
        self._active_per_domain[domain] += 1
        
        logger.debug(f"Acquired semaphores for domain: {domain} (global: {self._active_global}, domain: {self._active_per_domain[domain]})")
    
    def release(self, domain: str) -> None:
                                    
        if domain in self._domain_semaphores:
            self._domain_semaphores[domain].release()
            self._active_per_domain[domain] = max(0, self._active_per_domain[domain] - 1)
        
                                        
        self._global_semaphore.release()
        self._active_global = max(0, self._active_global - 1)
        
        logger.debug(f"Released semaphores for domain: {domain} (global: {self._active_global}, domain: {self._active_per_domain.get(domain, 0)})")
    
    def get_active_stats(self) -> Dict:
        return {
            "global_active": self._active_global,
            "per_domain": dict(self._active_per_domain),
        }
