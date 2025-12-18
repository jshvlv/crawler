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
    """
    Расширенный асинхронный краулер с поддержкой очередей, контроля глубины и фильтрации URL.
    
    Поддерживает:
    - Управление очередью URL с приоритетами
    - Контроль конкурентности через семафоры (глобально и по доменам)
    - Ограничение глубины обхода
    - Фильтрацию URL (по домену, паттернам)
    - Отслеживание посещённых URL
    - Статистику и прогресс в реальном времени
    """
    
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
        """
        Инициализирует краулер с настройками.
        
        Args:
            max_concurrent: Максимальное количество одновременных запросов
            verify_ssl: Проверять ли SSL сертификаты
            max_depth: Максимальная глубина обхода (0 = только стартовые URL)
            same_domain_only: Обрабатывать только URL того же домена, что и стартовые
            exclude_patterns: Список паттернов для исключения URL (regex)
            include_patterns: Список паттернов для включения URL (regex)
            per_domain_limit: Максимальное количество одновременных запросов к одному домену
            requests_per_second: Лимит запросов в секунду (rate limiting)
            min_delay: Минимальная задержка между запросами
            jitter: Случайная добавка к задержке (сек) для "человеческого" поведения
            respect_robots: Учитывать ли robots.txt
            user_agent: User-Agent для запросов и robots.txt
            retry_max_retries: Максимум повторов для временных ошибок
            retry_backoff_factor: Множитель backoff между попытками
            retry_base_delay: Базовая задержка перед повтором
            connect_timeout/read_timeout/total_timeout: таймауты aiohttp
            circuit_breaker_threshold: сколько подряд ошибок до блокировки домена
            circuit_breaker_cooldown: на сколько секунд блокировать домен
            storage: Объект для сохранения данных (JSONStorage, CSVStorage, SQLiteStorage)
        """
        self.max_workers = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)  # Старый семафор для обратной совместимости
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.parser = HTMLParser()
        self.verify_ssl = verify_ssl
        self.user_agent = user_agent
        # Таймауты: будем увеличивать с каждой попыткой
        self.timeouts = {
            "connect": connect_timeout,
            "read": read_timeout,
            "total": total_timeout,
            "connect_step": 0.5,  # увеличение connect-таймаута на каждую попытку
            "read_step": 1.0,     # увеличение read-таймаута
            "total_step": 1.0,    # увеличение total-таймаута
        }
        
        # Настройки обхода
        self.max_depth = max_depth
        self.same_domain_only = same_domain_only
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        
        # Управление очередью и конкурентностью
        self.queue = CrawlerQueue()
        self.semaphore_manager = SemaphoreManager(
            global_limit=max_concurrent,
            per_domain_limit=per_domain_limit
        )
        # Rate limiting и robots.txt
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True,
            min_delay=min_delay,
            jitter=jitter,
            backoff_base=0.0,
        )
        self.robots_parser = RobotsParser(user_agent=user_agent)
        self.respect_robots = respect_robots
        self.blocked_count = 0  # сколько URL заблокировано robots.txt
        # Повторы и backoff
        self.retry_strategy = RetryStrategy(
            max_retries=retry_max_retries,
            backoff_factor=retry_backoff_factor,
            base_delay=retry_base_delay,
            retry_on=[TransientError, NetworkError],
        )
        # Circuit breaker: блокируем домен при частых ошибках
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.circuit_breaker_cooldown = circuit_breaker_cooldown
        self.circuit_state: Dict[str, Dict[str, float]] = {}  # {domain: {"fail_count": int, "open_until": ts}}
        # Статистика ошибок
        self.error_stats: Dict[str, int] = {
            "transient": 0,
            "network": 0,
            "permanent": 0,
            "parse": 0,
            "other": 0,
        }
        self.permanent_urls: Dict[str, str] = {}  # url -> error message
        
        # Отслеживание состояния
        self.visited_urls: Set[str] = set()  # Множество всех посещённых URL
        self.url_depths: Dict[str, int] = {}  # Словарь глубины для каждого URL
        
        # Статистика
        self.start_time: Optional[float] = None
        self.last_progress_time: float = 0
        
        # Хранилище данных (опционально)
        self.storage: Optional[DataStorage] = storage

    async def _create_session(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        # Настройка SSL: если verify_ssl=False, отключаем проверку сертификатов
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

        # Circuit breaker: если домен заблокирован из-за частых ошибок
        if self._circuit_is_open(domain):
            self.logger.warning(f"Circuit breaker open for {domain}, skipping {url}")
            return {"url": url, "error": "circuit_open"}

        # Проверяем robots.txt, если это включено
        if self.respect_robots:
            allowed = await self._is_allowed_by_robots(url)
            if not allowed:
                self.blocked_count += 1
                self.logger.warning(f"Blocked by robots.txt: {url}")
                return {"url": url, "error": "blocked_by_robots"}

        self.logger.info(f"Start fetch: {url}")

        async def attempt_request(attempt: int) -> Dict[str, Any]:
            """Функция, которая будет вызываться с повторами."""
            return await self._request_once(url, domain, attempt)

        try:
            result = await self.retry_strategy.execute_with_retry(attempt_request)
            # Сбрасываем breaker при успехе
            self._record_success(domain)
            return result
        except Exception as e:  # noqa: BLE001
            # Классифицируем и считаем ошибки
            self._bump_error_stats(e, url)
            # Для постоянных ошибок — не повторять, для остальных — retries исчерпаны
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
        """
        Загружает и парсит страницу, добавляя метаданные для сохранения.
        
        Args:
            url: URL для загрузки и парсинга
        
        Returns:
            Словарь с распарсенными данными страницы
        """
        html_data = await self.fetch_url(url)
        if "error" in html_data:
            return html_data
        
        # Парсим HTML
        parsed = await self.parser.parse_html(html_data["text"], url)
        
        # Добавляем метаданные для сохранения
        parsed["crawled_at"] = datetime.now().isoformat()
        parsed["status_code"] = html_data.get("status", 0)
        parsed["content_type"] = html_data.get("content_type", "text/html")
        
        return parsed

    async def _request_once(self, url: str, domain: str, attempt: int) -> Dict[str, Any]:
        """
        Выполняет один HTTP-запрос с учётом rate limiting, crawl-delay и таймаутов.
        Бросает классифицированные ошибки для стратегии повторов.
        """
        # Rate limiting — ожидание перед запросом
        await self.rate_limiter.acquire(domain)

        # Дополнительная задержка из Crawl-delay (если robots.txt дал задержку)
        crawl_delay = 0.0
        if self.respect_robots:
            crawl_delay = await self.robots_parser.get_crawl_delay(url, self.session, self.user_agent)
        if crawl_delay > 0:
            await asyncio.sleep(crawl_delay)

        # Динамический таймаут с ростом по попыткам
        timeout = self._compute_timeout(attempt)

        try:
            async with self.semaphore:
                async with self.session.get(url, timeout=timeout) as response:
                    content_type = response.headers.get("Content-Type", "")
                    status = response.status
                    text = await response.text()

                    # Классификация HTTP статусов
                    if status in (429, 503, 500):
                        raise TransientError(f"HTTP {status}")
                    if status in (404, 403, 401):
                        raise PermanentError(f"HTTP {status}")
                    # Остальные 4xx считаем постоянными
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
            # aiohttp уже классифицирует, но поднимаем как Permanent/Transient выше
            if e.status in (429, 503, 500):
                raise TransientError(f"HTTP {e.status}") from e
            if e.status in (404, 403, 401):
                raise PermanentError(f"HTTP {e.status}") from e
            raise PermanentError(f"HTTP {e.status}") from e
        except ClientError as e:
            self.rate_limiter.record_error(domain)
            raise NetworkError(f"Client error: {e}") from e
        except Exception as e:  # noqa: BLE001
            self.rate_limiter.record_error(domain)
            raise

    def _get_domain(self, url: str) -> str:
        """
        Извлекает домен из URL для проверки фильтра same_domain_only.
        
        Args:
            url: URL для анализа
        
        Returns:
            Домен (например, "example.com")
        """
        try:
            parsed = urlparse(url)
            # Берём netloc (домен с портом, если есть)
            domain = parsed.netloc or parsed.path.split('/')[0]
            # Убираем порт, если есть
            if ':' in domain:
                domain = domain.split(':')[0]
            return domain.lower()  # Приводим к нижнему регистру для сравнения
        except Exception:
            # Если не удалось распарсить, возвращаем весь URL как домен
            return url.lower()

    def _should_process_url(self, url: str, current_depth: int, start_domains: Set[str]) -> bool:
        """
        Проверяет, нужно ли обрабатывать URL на основе фильтров и глубины.
        
        Args:
            url: URL для проверки
            current_depth: Текущая глубина обхода
            start_domains: Множество доменов стартовых URL
        
        Returns:
            True если URL нужно обработать, False если пропустить
        """
        # Проверка глубины: если превышена максимальная глубина, пропускаем
        if current_depth >= self.max_depth:
            return False
        
        # Проверка, не был ли URL уже посещён
        normalized = self._normalize_url(url)
        if normalized in self.visited_urls or self.queue.is_visited(normalized):
            return False
        
        # Проверка фильтра same_domain_only: если включен, проверяем домен
        if self.same_domain_only:
            url_domain = self._get_domain(url)
            # Проверяем, совпадает ли домен с одним из стартовых доменов
            if url_domain not in start_domains:
                return False
        
        # Проверка exclude_patterns: если URL совпадает с исключающим паттерном, пропускаем
        for pattern in self.exclude_patterns:
            if pattern.search(url):
                return False
        
        # Проверка include_patterns: если список не пуст, URL должен совпадать хотя бы с одним паттерном
        if self.include_patterns:
            matches = any(pattern.search(url) for pattern in self.include_patterns)
            if not matches:
                return False
        
        return True

    def _compute_timeout(self, attempt: int) -> aiohttp.ClientTimeout:
        """
        Строит объект таймаутов с увеличением на каждую попытку.
        attempt: номер попытки (0 — первая).
        """
        return aiohttp.ClientTimeout(
            total=self.timeouts["total"] + self.timeouts["total_step"] * attempt,
            connect=self.timeouts["connect"] + self.timeouts["connect_step"] * attempt,
            sock_read=self.timeouts["read"] + self.timeouts["read_step"] * attempt,
        )

    def _circuit_is_open(self, domain: str) -> bool:
        """Проверяет, открыт ли circuit breaker для домена."""
        state = self.circuit_state.get(domain)
        if not state:
            return False
        return time.time() < state.get("open_until", 0)

    def _record_failure(self, domain: str) -> None:
        """
        Увеличивает счётчик ошибок для домена.
        Если превышен порог, открываем circuit breaker на cooldown.
        """
        state = self.circuit_state.setdefault(domain, {"fail_count": 0, "open_until": 0})
        state["fail_count"] += 1
        if state["fail_count"] >= self.circuit_breaker_threshold:
            state["open_until"] = time.time() + self.circuit_breaker_cooldown
            self.logger.warning(
                f"Circuit breaker OPEN for {domain} for {self.circuit_breaker_cooldown}s "
                f"(failures: {state['fail_count']})"
            )

    def _record_success(self, domain: str) -> None:
        """Сбрасывает счётчик ошибок для домена (закрывает breaker)."""
        if domain in self.circuit_state:
            self.circuit_state[domain]["fail_count"] = 0
            self.circuit_state[domain]["open_until"] = 0

    def _bump_error_stats(self, err: Exception, url: str) -> None:
        """Обновляет статистику ошибок и помечает постоянные ошибки."""
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
        """
        Проверяет через robots.txt, можно ли заходить на URL.
        Если robots.txt недоступен — считаем, что можно.
        """
        if self.session is None:
            await self._create_session()
        return await self.robots_parser.can_fetch(url, self.session, self.user_agent)

    def _normalize_url(self, url: str) -> str:
        """
        Нормализует URL: убирает фрагменты и trailing slash.
        
        Args:
            url: Исходный URL
        
        Returns:
            Нормализованный URL
        """
        # Убираем фрагмент (всё после #)
        if '#' in url:
            url = url.split('#')[0]
        
        # Убираем trailing slash (кроме корневого пути)
        if url.endswith('/') and len(url) > 1:
            url = url.rstrip('/')
        
        return url

    def _print_progress(self, stats: Dict, elapsed: float) -> None:
        """
        Выводит прогресс краулинга в реальном времени.
        
        Args:
            stats: Статистика из очереди
            elapsed: Прошедшее время в секундах
        """
        # Вычисляем скорость обработки (страниц в секунду)
        speed = stats["processed"] / elapsed if elapsed > 0 else 0
        # Текущая средняя задержка между запросами по данным rate limiter
        avg_delay = (sum(self.rate_limiter.delays) / len(self.rate_limiter.delays)) if self.rate_limiter.delays else 0.0
        errors_total = sum(self.error_stats.values())
        
        # Форматируем вывод прогресса
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
        
        # Выводим в консоль (используем \r для перезаписи строки)
        print(f"\r{progress_msg}", end="", flush=True)

    async def crawl(
        self,
        start_urls: List[str],
        max_pages: int = 100,
        same_domain_only: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        Основной метод краулинга: обходит сайт начиная с указанных URL.
        
        Процесс:
        1. Добавляет стартовые URL в очередь с глубиной 0
        2. Пока очередь не пуста и не достигнут max_pages:
           - Получает следующий URL из очереди
           - Получает семафоры (глобальный и для домена)
           - Загружает и парсит страницу
           - Извлекает ссылки из страницы
           - Для каждой ссылки проверяет фильтры и глубину
           - Добавляет подходящие ссылки в очередь с увеличенной глубиной
           - Освобождает семафоры
           - Обновляет статистику и выводит прогресс
        3. Возвращает словарь с результатами
        
        Args:
            start_urls: Список стартовых URL для обхода
            max_pages: Максимальное количество страниц для обработки
            same_domain_only: Переопределяет настройку из конструктора (опционально)
        
        Returns:
            Словарь с результатами:
            - processed: список обработанных URL с данными
            - failed: словарь неудачных URL с ошибками
            - stats: финальная статистика
        """
        # Переопределяем настройку same_domain_only, если указана
        if same_domain_only is not None:
            self.same_domain_only = same_domain_only
        
        # Извлекаем домены из стартовых URL для фильтрации
        start_domains = {self._get_domain(url) for url in start_urls}
        
        # Инициализируем сессию HTTP
        if self.session is None:
            await self._create_session()
        
        # Добавляем стартовые URL в очередь с приоритетом 0 и глубиной 0
        for url in start_urls:
            normalized = self._normalize_url(url)
            if self.queue.add_url(normalized, priority=0):
                self.url_depths[normalized] = 0  # Стартовые URL имеют глубину 0
        
        # Засекаем время начала для статистики
        self.start_time = time.time()
        self.last_progress_time = time.time()
        
        # Список задач для параллельного выполнения
        tasks = []
        
        # Основной цикл краулинга
        while True:
            # Получаем следующий URL из очереди
            url = await self.queue.get_next()
            
            # Если очередь пуста и нет активных задач, завершаем
            if url is None:
                # Ждём завершения всех активных задач
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
                
                # Проверяем очередь ещё раз после завершения задач
                url = await self.queue.get_next()
                if url is None:
                    break
            
            # Проверяем, не превышен ли лимит страниц
            stats = self.queue.get_stats()
            if stats["processed"] >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break
            
            # Получаем глубину текущего URL
            current_depth = self.url_depths.get(url, 0)
            
            # Помечаем URL как посещённый
            self.visited_urls.add(url)
            
            # Извлекаем домен для управления семафорами
            domain = self._get_domain(url)
            
            # Создаём задачу для обработки URL
            async def process_url(url: str, depth: int, domain: str) -> None:
                """
                Внутренняя функция для обработки одного URL.
                
                Args:
                    url: URL для обработки
                    depth: Глубина обхода
                    domain: Домен URL
                """
                try:
                    # Получаем семафоры (глобальный и для домена)
                    await self.semaphore_manager.acquire(domain)
                    
                    try:
                        # Загружаем и парсим страницу
                        parsed_data = await self.fetch_and_parse(url)
                        
                        # Проверяем результат
                        if "error" in parsed_data:
                            # Если ошибка, помечаем как неудачный
                            error_msg = parsed_data.get("error", "unknown_error")
                            self.queue.mark_failed(url, error_msg)
                        else:
                            # Если успешно, помечаем как обработанный
                            self.queue.mark_processed(url, parsed_data)
                            
                            # Сохраняем данные в хранилище (если указано)
                            if self.storage:
                                try:
                                    await self.storage.save(parsed_data)
                                except Exception as e:
                                    # Логируем ошибку сохранения, но не прерываем работу краулера
                                    self.logger.error(f"Error saving data for {url}: {e}", exc_info=True)
                            
                            # Извлекаем ссылки из страницы
                            links = parsed_data.get("links", [])
                            
                            # Обрабатываем каждую найденную ссылку
                            for link in links:
                                # Нормализуем ссылку
                                normalized_link = self._normalize_url(link)
                                
                                # Проверяем, нужно ли обрабатывать эту ссылку
                                if self._should_process_url(normalized_link, depth + 1, start_domains):
                                    # Добавляем в очередь с приоритетом, обратным глубине
                                    # (меньшая глубина = выше приоритет)
                                    priority = -(depth + 1)
                                    if self.queue.add_url(normalized_link, priority=priority):
                                        # Сохраняем глубину для нового URL
                                        self.url_depths[normalized_link] = depth + 1
                                        self.logger.debug(f"Added to queue: {normalized_link} (depth: {depth + 1})")
                    
                    finally:
                        # Освобождаем семафоры в любом случае (даже при ошибке)
                        self.semaphore_manager.release(domain)
                        
                        # Выводим прогресс каждые 0.5 секунды
                        current_time = time.time()
                        if current_time - self.last_progress_time >= 0.5:
                            stats = self.queue.get_stats()
                            elapsed = current_time - (self.start_time or current_time)
                            self._print_progress(stats, elapsed)
                            self.last_progress_time = current_time
                
                except Exception as e:
                    # Обрабатываем неожиданные ошибки
                    self.logger.error(f"Error processing {url}: {e}", exc_info=True)
                    self.queue.mark_failed(url, str(e))
                    # Освобождаем семафоры даже при ошибке
                    try:
                        self.semaphore_manager.release(domain)
                    except Exception:
                        pass
            
            # Добавляем задачу в список для параллельного выполнения
            task = asyncio.create_task(process_url(url, current_depth, domain))
            tasks.append(task)
            
            # Ограничиваем количество одновременно выполняемых задач
            # (чтобы не создавать слишком много задач сразу)
            if len(tasks) >= self.max_workers * 2:
                # Ждём завершения хотя бы одной задачи
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # Удаляем завершённые задачи из списка
                tasks = list(pending)
        
        # Ждём завершения всех оставшихся задач
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Финальный вывод прогресса
        final_stats = self.queue.get_stats()
        elapsed = time.time() - (self.start_time or time.time())
        self._print_progress(final_stats, elapsed)
        print()  # Новая строка после прогресса
        
        # Формируем результат
        processed_urls = self.queue.get_processed_urls()
        failed_urls = self.queue.get_failed_urls()
        
        return {
            "processed": processed_urls,
            "failed": failed_urls,
            "stats": final_stats,
            "elapsed_time": elapsed,
        }

    async def close(self) -> None:
        """
        Закрывает все ресурсы: HTTP сессию и хранилище данных.
        """
        # Закрываем HTTP сессию
        if self.session:
            await self.session.close()
        
        # Закрываем хранилище данных (если указано)
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
    """
    Демонстрация краулера с сохранением данных в разные форматы.
    
    Показывает:
    - Обход сайта с ограничением глубины
    - Сохранение в JSON, CSV и SQLite
    - Статистику по сохранённым данным
    """
    import json
    import os
    from crawler.storage import JSONStorage, CSVStorage, SQLiteStorage
    
    # Создаём хранилища для разных форматов
    json_storage = JSONStorage("demo_results.json", buffer_size=5)
    csv_storage = CSVStorage("demo_results.csv", buffer_size=5)
    db_storage = SQLiteStorage("demo_results.db", batch_size=5)
    
    # Инициализируем SQLite базу данных
    await db_storage.init_db()
    
    # Настройки краулера с JSON хранилищем
    crawler = AsyncCrawler(
        max_concurrent=3,           # Максимум 3 одновременных запросов
        verify_ssl=False,           # Отключаем проверку SSL (для тестирования)
        max_depth=2,                # Максимальная глубина обхода: 2 уровня
        same_domain_only=True,      # Обрабатывать только тот же домен
        per_domain_limit=2,         # Максимум 2 запроса к одному домену одновременно
        storage=json_storage,       # Используем JSON хранилище
    )
    
    # Стартовые URL для обхода
    start_urls = ["https://petstore.swagger.io/"]
    
    print("=== Starting crawl with storage ===")
    print(f"Start URLs: {start_urls}")
    print(f"Max depth: {crawler.max_depth}")
    print(f"Max pages: 10")
    print(f"Storage: JSON (demo_results.json)")
    print()
    
    # Запускаем краулинг
    results = await crawler.crawl(
        start_urls=start_urls,
        max_pages=10,
        same_domain_only=True,
    )
    
    # Закрываем краулер (это также закроет storage)
    await crawler.close()
    
    # Демонстрируем сохранение в другие форматы
    print("\n=== Saving to CSV and SQLite ===")
    processed = results.get("processed", {})
    
    # Сохраняем в CSV
    for url, data in list(processed.items())[:5]:  # Первые 5 страниц
        await csv_storage.save(data)
    await csv_storage.close()
    print(f"CSV saved: {os.path.abspath('demo_results.csv')}")
    
    # Сохраняем в SQLite
    for url, data in list(processed.items())[:5]:  # Первые 5 страниц
        await db_storage.save(data)
    
    # Получаем статистику из SQLite ПЕРЕД закрытием (или можно после - метод переоткроет соединение)
    db_stats = await db_storage.get_stats()
    print(f"SQLite stats: {db_stats}")
    
    # Закрываем SQLite storage
    await db_storage.close()
    print(f"SQLite saved: {os.path.abspath('demo_results.db')}")

    # Выводим результаты
    print("\n=== Crawl Results ===")
    print(f"Processed pages: {len(results['processed'])}")
    print(f"Failed pages: {len(results['failed'])}")
    print(f"Total time: {results['elapsed_time']:.2f}s")
    print(f"Stats: {results['stats']}")
    
    # Формируем сводку для сохранения
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
    
    # Сохраняем результаты в JSON
    with open("demo_results.json", "w", encoding="utf-8") as f:
        json.dump({
            "summary": summary,
            "stats": results['stats'],
            "failed": results['failed'],
        }, f, ensure_ascii=False, indent=2)
    print("Results saved to: demo_results.json")

    # Выводим примеры обработанных страниц
    print("\n=== Sample Pages ===")
    for i, (url, data) in enumerate(list(results['processed'].items())[:5]):
        print(f"\n{i+1}. {url}")
        print(f"   Title: {data.get('title', 'N/A')[:50]}")
        print(f"   Links: {len(data.get('links', []))}")
        print(f"   Depth: {crawler.url_depths.get(url, 0)}")
        print(f"   Text: {data['text']}")


if __name__ == "__main__":
    asyncio.run(main())