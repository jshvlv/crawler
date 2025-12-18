"""
Главный класс AdvancedCrawler - объединяет все компоненты краулера.

Интегрирует:
- AsyncCrawler (основной краулер)
- SitemapParser (парсинг sitemap)
- CrawlerStats (статистика)
- DataStorage (сохранение данных)
- Конфигурацию
- Логирование
"""
import asyncio
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
    """
    Продвинутый краулер с полной интеграцией всех компонентов.
    
    Объединяет:
    - AsyncCrawler для обхода сайтов
    - SitemapParser для загрузки URL из sitemap
    - CrawlerStats для сбора статистики
    - DataStorage для сохранения данных
    - Конфигурацию из файла
    - Структурированное логирование
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Инициализирует AdvancedCrawler.
        
        Args:
            config: Словарь с конфигурацией (если None, используются значения по умолчанию)
        """
        # Загружаем конфигурацию
        self.config = config or ConfigLoader.DEFAULT_CONFIG
        
        # Настраиваем логирование
        self._setup_logging()
        
        # Создаём статистику
        self.stats = CrawlerStats()
        
        # Создаём хранилище данных
        self.storage: Optional[DataStorage] = self._create_storage()
        
        # Создаём основной краулер
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
        
        # Создаём парсер sitemap
        self.sitemap_parser = SitemapParser()
        
        logger.info("AdvancedCrawler initialized")
    
    @classmethod
    def from_config(cls, config_path: str) -> "AdvancedCrawler":
        """
        Создаёт AdvancedCrawler из конфигурационного файла.
        
        Args:
            config_path: Путь к YAML конфигурационному файлу
        
        Returns:
            Экземпляр AdvancedCrawler с загруженной конфигурацией
        """
        config = ConfigLoader.load_from_file(config_path)
        return cls(config=config)
    
    def _setup_logging(self) -> None:
        """Настраивает логирование согласно конфигурации."""
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO").upper())
        log_format = log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        log_file = log_config.get("file")
        
        # Настраиваем root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Удаляем существующие handlers
        root_logger.handlers.clear()
        
        # Консольный handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(log_format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # Файловый handler (если указан файл)
        if log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(log_format)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            logger.info(f"Logging to file: {log_file}")
    
    def _create_storage(self) -> Optional[DataStorage]:
        """
        Создаёт хранилище данных согласно конфигурации.
        
        Returns:
            Объект DataStorage или None
        """
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
            # Инициализируем БД асинхронно (нужно будет вызвать await)
            return storage
        else:
            logger.warning(f"Unknown storage type: {storage_type}")
            return None
    
    async def _discover_urls_from_sitemap(self, base_urls: List[str]) -> List[str]:
        """
        Обнаруживает URL из sitemap для указанных базовых URL.
        
        Args:
            base_urls: Список базовых URL для поиска sitemap
        
        Returns:
            Список найденных URL из sitemap
        """
        urls = []
        
        # Создаём временную сессию для загрузки sitemap
        if self.crawler.session is None:
            await self.crawler._create_session()
        
        # Обрабатываем каждый базовый URL
        for base_url in base_urls:
            try:
                # Пробуем найти sitemap
                sitemap_urls = await self.sitemap_parser.discover_sitemap_urls(
                    base_url,
                    self.crawler.session
                )
                urls.extend(sitemap_urls)
            except Exception as e:
                logger.error(f"Error discovering sitemap for {base_url}: {e}")
        
        # Также проверяем явно указанные sitemap URL
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
        """
        Запускает краулинг с полной интеграцией всех компонентов.
        
        Args:
            start_urls: Стартовые URL (если None, берутся из конфигурации)
            max_pages: Максимальное количество страниц (если None, берётся из конфигурации)
        
        Returns:
            Словарь с результатами краулинга
        """
        # Начинаем сбор статистики
        self.stats.start()
        
        # Получаем стартовые URL
        if start_urls is None:
            start_urls = self.config.get("urls", {}).get("start_urls", [])
        
        # Если включено использование sitemap, загружаем URL из sitemap
        use_sitemap = self.config.get("urls", {}).get("use_sitemap", False)
        if use_sitemap:
            sitemap_urls = await self._discover_urls_from_sitemap(start_urls)
            if sitemap_urls:
                start_urls.extend(sitemap_urls)
                logger.info(f"Added {len(sitemap_urls)} URLs from sitemap")
        
        # Получаем max_pages из конфигурации если не указан
        if max_pages is None:
            max_pages = self.config.get("crawler", {}).get("max_pages", 100)
        
        # Инициализируем SQLite storage если используется
        if isinstance(self.storage, SQLiteStorage):
            await self.storage.init_db()
        
        # Запускаем краулинг
        logger.info(f"Starting crawl with {len(start_urls)} start URLs, max_pages={max_pages}")
        results = await self.crawler.crawl(
            start_urls=start_urls,
            max_pages=max_pages,
        )
        
        # Обновляем статистику на основе результатов
        processed = results.get("processed", {})
        failed = results.get("failed", {})
        
        for url, data in processed.items():
            status_code = data.get("status_code", 200)
            self.stats.add_page(url, "success", 0.0)  # Время не отслеживается в AsyncCrawler
            self.stats.add_status_code(status_code)
        
        for url, error in failed.items():
            self.stats.add_page(url, "failed", 0.0, error)
        
        # Останавливаем статистику
        self.stats.stop()
        
        logger.info(f"Crawl completed: {len(processed)} successful, {len(failed)} failed")
        return results
    
    def get_stats(self) -> Dict:
        """
        Возвращает статистику краулера.
        
        Returns:
            Словарь со статистикой
        """
        return self.stats.get_stats()
    
    def export_to_json(self, filename: Optional[str] = None) -> None:
        """
        Экспортирует статистику в JSON.
        
        Args:
            filename: Путь к файлу (если None, берётся из конфигурации)
        """
        if filename is None:
            filename = self.config.get("output", {}).get("stats_json", "stats.json")
        self.stats.export_to_json(filename)
    
    def export_to_html_report(self, filename: Optional[str] = None) -> None:
        """
        Создаёт HTML отчёт со статистикой.
        
        Args:
            filename: Путь к файлу (если None, берётся из конфигурации)
        """
        if filename is None:
            filename = self.config.get("output", {}).get("stats_html", "report.html")
        self.stats.export_to_html_report(filename)
    
    async def close(self) -> None:
        """Закрывает все ресурсы: краулер и хранилище."""
        await self.crawler.close()
        logger.info("AdvancedCrawler closed")

