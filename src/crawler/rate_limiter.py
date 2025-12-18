"""
Ограничение скорости (rate limiting) и работа с robots.txt.

Содержит:
- RateLimiter: контролирует частоту запросов (глобально или по доменам), поддерживает jitter и backoff.
- RobotsParser: загружает и кэширует robots.txt, проверяет разрешение на обход.
"""
import asyncio
import logging
import random
import time
from typing import Dict, Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Простой асинхронный rate limiter.
    
    Поддерживает:
    - per-domain лимиты (separate buckets)
    - глобальный лимит (один bucket на всё)
    - минимальную задержку между запросами
    - jitter (случайная добавка к задержке для "человеческого" поведения)
    - экспоненциальный backoff при ошибках
    """

    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        backoff_base: float = 0.0,
        backoff_factor: float = 2.0,
        backoff_max: float = 5.0,
    ):
        self.per_domain = per_domain
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.min_delay = min_delay
        self.jitter = jitter
        self.backoff_base = backoff_base
        self.backoff_factor = backoff_factor
        self.backoff_max = backoff_max

        # Временные метки последнего запроса: глобально или по доменам
        self._last_call: Dict[str, float] = {}
        # Счётчики ошибок для backoff
        self._error_counts: Dict[str, int] = {}
        # Блокировка для потокобезопасности
        self._lock = asyncio.Lock()
        # Статистика задержек
        self.delays: list[float] = []

    def _bucket_key(self, domain: Optional[str]) -> str:
        """Возвращает ключ ведра (bucket): домен или 'global'."""
        if self.per_domain and domain:
            return domain
        return "global"

    def record_error(self, domain: Optional[str]) -> None:
        """Увеличивает счётчик ошибок для backoff."""
        key = self._bucket_key(domain)
        self._error_counts[key] = self._error_counts.get(key, 0) + 1

    def record_success(self, domain: Optional[str]) -> None:
        """Сбрасывает счётчик ошибок при успешном запросе."""
        key = self._bucket_key(domain)
        self._error_counts[key] = 0

    def _compute_sleep(self, key: str) -> float:
        """
        Считает, сколько нужно подождать до следующего разрешения.
        Учитывает базовый интервал, минимальную задержку, jitter и backoff.
        """
        now = time.time()
        last = self._last_call.get(key, 0.0)

        # Базовая задержка: интервал между запросами
        wait_for_rate = self.interval - (now - last)

        # Минимальная задержка
        wait_for_min = self.min_delay - (now - last)

        # Берём максимальную из двух (чтобы соблюсти оба ограничения)
        wait_time = max(0.0, wait_for_rate, wait_for_min)

        # Jitter: случайная добавка до self.jitter секунд
        if self.jitter > 0:
            wait_time += random.uniform(0, self.jitter)

        # Backoff: если были ошибки, увеличиваем задержку
        error_count = self._error_counts.get(key, 0)
        if self.backoff_base > 0 and error_count > 0:
            backoff_delay = min(
                self.backoff_base * (self.backoff_factor ** (error_count - 1)),
                self.backoff_max,
            )
            wait_time = max(wait_time, backoff_delay)

        return wait_time

    async def acquire(self, domain: Optional[str] = None) -> None:
        """
        Ожидает разрешения на запрос.
        Вызывайте перед выполнением HTTP-запроса.
        """
        key = self._bucket_key(domain)
        async with self._lock:
            sleep_time = self._compute_sleep(key)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            # фиксируем время запроса
            self._last_call[key] = time.time()
            # сохраняем статистику задержек
            self.delays.append(sleep_time)


class RobotsParser:
    """
    Загрузка и кэширование robots.txt.
    
    Использует стандартный RobotFileParser для проверки Disallow/Allow и Crawl-delay.
    """

    def __init__(self, user_agent: str = "*"):
        self.user_agent = user_agent
        # Кэш: домен -> RobotFileParser
        self._cache: Dict[str, RobotFileParser] = {}
        # Блокировка для конкурентного доступа к кэшу
        self._lock = asyncio.Lock()

    def _get_domain(self, url: str) -> str:
        """Достаёт домен из URL."""
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        if ":" in domain:
            domain = domain.split(":")[0]
        return domain.lower()

    async def fetch_robots(self, base_url: str, session: Optional[aiohttp.ClientSession]) -> Optional[RobotFileParser]:
        """
        Загружает robots.txt для домена и кладёт в кэш.
        Если загрузка не удалась, возвращает None (в таком случае считаем, что разрешено).
        """
        domain = self._get_domain(base_url)
        async with self._lock:
            if domain in self._cache:
                return self._cache[domain]

        # Если нет сессии (например, в тестах), не пытаемся идти в сеть
        if session is None:
            return None

        robots_url = f"{base_url.rstrip('/')}/robots.txt"
        try:
            async with session.get(robots_url) as resp:
                if resp.status >= 400:
                    logger.info(f"robots.txt not found or forbidden for {domain}: {resp.status}")
                    return None
                content = await resp.text()
        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
            return None

        parser = RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(content.splitlines())

        async with self._lock:
            self._cache[domain] = parser

        return parser

    async def can_fetch(self, url: str, session: Optional[aiohttp.ClientSession], user_agent: Optional[str] = None) -> bool:
        """
        Проверяет, разрешён ли URL согласно robots.txt.
        Если robots.txt не доступен — считает, что можно.
        """
        domain = self._get_domain(url)
        ua = user_agent or self.user_agent
        parser = self._cache.get(domain)
        if parser is None:
            parser = await self.fetch_robots(f"https://{domain}", session)
            # Если не удалось загрузить, позволяем обход
            if parser is None:
                return True
        return parser.can_fetch(ua, url)

    async def get_crawl_delay(self, url: str, session: Optional[aiohttp.ClientSession], user_agent: Optional[str] = None) -> float:
        """
        Возвращает Crawl-delay из robots.txt, если указан, иначе 0.
        """
        domain = self._get_domain(url)
        ua = user_agent or self.user_agent
        parser = self._cache.get(domain)
        if parser is None:
            parser = await self.fetch_robots(f"https://{domain}", session)
            if parser is None:
                return 0.0
        delay = parser.crawl_delay(ua)
        return float(delay) if delay is not None else 0.0