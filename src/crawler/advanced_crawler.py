import asyncio
import copy
import logging
import logging.handlers
from typing import Dict, List, Optional

from crawler.config import ConfigLoader
from crawler.fetcher import AsyncCrawler
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage import DataStorage, JSONStorage, CSVStorage, SQLiteStorage

logger = logging.getLogger(__name__)


class AdvancedCrawler:
    
    def __init__(self, config: Optional[Dict] = None):
                                 
                                                                        
                                                           
                                                                                              
        self.config = copy.deepcopy(config) if config is not None else copy.deepcopy(ConfigLoader.DEFAULT_CONFIG)
        
                                 
        self._setup_logging()
        
                            
        self.stats = CrawlerStats()
        
                                  
        self.storage: Optional[DataStorage] = self._create_storage()
        
                                  
        crawler_config = self.config.get("crawler", {})
        filters_config = self.config.get("filters", {})
        self.crawler = AsyncCrawler(
            max_concurrent=crawler_config.get("max_concurrent", 10),
            verify_ssl=crawler_config.get("verify_ssl", True),
            max_depth=crawler_config.get("max_depth", 3),
            same_domain_only=crawler_config.get("same_domain_only", False),
            exclude_patterns=filters_config.get("exclude_patterns", []),
            include_patterns=filters_config.get("include_patterns", []),
            per_domain_limit=crawler_config.get("per_domain_limit", 2),
            requests_per_second=crawler_config.get("requests_per_second", 1.0),
            min_delay=crawler_config.get("min_delay", 0.0),
            jitter=crawler_config.get("jitter", 0.0),
            respect_robots=crawler_config.get("respect_robots", True),
            user_agent=crawler_config.get("user_agent", "AsyncCrawler/1.0"),
            retry_max_retries=crawler_config.get("retry_max_retries", 3),
            retry_backoff_factor=crawler_config.get("retry_backoff_factor", 2.0),
            retry_base_delay=crawler_config.get("retry_base_delay", 0.5),
            connect_timeout=crawler_config.get("connect_timeout", 5.0),
            read_timeout=crawler_config.get("read_timeout", 10.0),
            total_timeout=crawler_config.get("total_timeout", 15.0),
            circuit_breaker_threshold=crawler_config.get("circuit_breaker_threshold", 5),
            circuit_breaker_cooldown=crawler_config.get("circuit_breaker_cooldown", 30.0),
            storage=self.storage,
        )
        
                                
        self.sitemap_parser = SitemapParser()
        
        logger.info("AdvancedCrawler initialized")
    
    @classmethod
    def from_config(cls, config_path: str) -> "AdvancedCrawler":
        config = ConfigLoader.load_from_file(config_path)
        return cls(config=config)
    
    def _setup_logging(self) -> None:
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO").upper())
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        log_file = log_config.get("file")
        
                                 
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
                                       
        root_logger.handlers.clear()
        
                            
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
                                             
        if log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,         
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(log_format)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            logger.info(f"Logging to file: {log_file}")
    
    def _create_storage(self) -> Optional[DataStorage]:
        storage_config = self.config.get("storage", {})
        storage_type = storage_config.get("type")
        
        if not storage_type or storage_type.lower() == "none":
            return None
        
        storage_type = storage_type.lower()
        
        if storage_type == "json":
            json_config = storage_config.get("json", {})
            return JSONStorage(
                filename=json_config.get("filename", "results.json"),
                buffer_size=json_config.get("buffer_size", 10),
            )
        elif storage_type == "csv":
            csv_config = storage_config.get("csv", {})
            return CSVStorage(
                filename=csv_config.get("filename", "results.csv"),
                buffer_size=csv_config.get("buffer_size", 10),
            )
        elif storage_type == "sqlite":
            sqlite_config = storage_config.get("sqlite", {})
            storage = SQLiteStorage(
                db_path=sqlite_config.get("filename", "results.db"),
                batch_size=sqlite_config.get("batch_size", 50),
            )
                                                                      
            return storage
        else:
            logger.warning(f"Unknown storage type: {storage_type}")
            return None
    
    async def _discover_urls_from_sitemap(self, base_urls: List[str]) -> List[str]:
        urls = []
        
                                                       
        if self.crawler.session is None:
            await self.crawler._create_session()
        
                                         
        for base_url in base_urls:
            try:
                                       
                sitemap_urls = await self.sitemap_parser.discover_sitemap_urls(
                    base_url,
                    self.crawler.session
                )
                urls.extend(sitemap_urls)
            except Exception as e:
                logger.error(f"Error discovering sitemap for {base_url}: {e}")
        
                                                    
        sitemap_urls_config = self.config.get("urls", {}).get("sitemap_urls", [])
        for sitemap_url in sitemap_urls_config:
            try:
                sitemap_urls = await self.sitemap_parser.fetch_sitemap(
                    sitemap_url,
                    self.crawler.session
                )
                urls.extend(sitemap_urls)
            except Exception as e:
                logger.error(f"Error fetching sitemap {sitemap_url}: {e}")
        
        logger.info(f"Discovered {len(urls)} URLs from sitemap")
        return urls
    
    async def crawl(self, start_urls: Optional[List[str]] = None, max_pages: Optional[int] = None) -> Dict:
                                  
        self.stats.start()
        
                                
        if start_urls is None:
                                                                      
            start_urls = list(self.config.get("urls", {}).get("start_urls", []))
        else:
                                                                                         
            start_urls = list(start_urls)
        
                                                                       
        use_sitemap = self.config.get("urls", {}).get("use_sitemap", False)
        if use_sitemap:
            sitemap_urls = await self._discover_urls_from_sitemap(start_urls)
            if sitemap_urls:
                                                                           
                start_urls.extend(list(sitemap_urls))
                logger.info(f"Added {len(sitemap_urls)} URLs from sitemap")
        
                                                           
        if max_pages is None:
            max_pages = self.config.get("crawler", {}).get("max_pages", 100)
        
                                                         
        if isinstance(self.storage, SQLiteStorage):
            await self.storage.init_db()
        
                            
        logger.info(f"Starting crawl with {len(start_urls)} start URLs, max_pages={max_pages}")
        results = await self.crawler.crawl(
            start_urls=start_urls,
            max_pages=max_pages,
        )
        
                                                    
        processed = results.get("processed", {})
        failed = results.get("failed", {})
        
        for url, data in processed.items():
            status_code = data.get("status_code", 200)
            self.stats.add_page(url, "success", 0.0)                                         
            self.stats.add_status_code(status_code)
        
        for url, error in failed.items():
            self.stats.add_page(url, "failed", 0.0, error)
        
                                  
        self.stats.stop()
        
        logger.info(f"Crawl completed: {len(processed)} successful, {len(failed)} failed")
        return results
    
    def get_stats(self) -> Dict:
        return self.stats.get_stats()
    
    def export_to_json(self, filename: Optional[str] = None) -> None:
        if filename is None:
            filename = self.config.get("output", {}).get("stats_json", "stats.json")
        self.stats.export_to_json(filename)
    
    def export_to_html_report(self, filename: Optional[str] = None) -> None:
        if filename is None:
            filename = self.config.get("output", {}).get("stats_html", "report.html")
        self.stats.export_to_html_report(filename)
    
    async def close(self) -> None:
        await self.crawler.close()
        logger.info("AdvancedCrawler closed")

