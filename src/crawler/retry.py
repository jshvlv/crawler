"""
Повторы запросов с экспоненциальным backoff и классификация ошибок.

Ключевые сущности:
- TransientError: временные ошибки (таймауты, 429, 503, 500 и т.п.)
- PermanentError: постоянные ошибки (404, 403, 401 и т.п.)
- NetworkError: сетевые ошибки (соединение, DNS)
- ParseError: ошибки парсинга

RetryStrategy:
- Выполняет корутину с повторами и экспоненциальным backoff
- Логирует попытки, собирает статистику по ошибкам и успешным повторам
"""
import asyncio
import logging
import time
from typing import Callable, Coroutine, List, Optional, Type, Any

logger = logging.getLogger(__name__)


# --- Классификация ошибок ---
class TransientError(Exception):
    """Временная ошибка: есть смысл повторить (таймауты, 429, 503, 500)."""
    pass


class PermanentError(Exception):
    """Постоянная ошибка: повторять не нужно (404, 403, 401)."""
    pass


class NetworkError(Exception):
    """Сетевая ошибка: проблемы соединения/DNS, можно повторить."""
    pass


class ParseError(Exception):
    """Ошибка парсинга контента."""
    pass


class RetryStrategy:
    """
    Управляет повторами с экспоненциальным backoff.
    
    Параметры:
    - max_retries: сколько дополнительных попыток (0 = без повторов)
    - backoff_factor: множитель backoff (delay = base_delay * factor ** attempt)
    - base_delay: стартовая задержка перед повтором
    - retry_on: список типов ошибок, которые можно повторять
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        base_delay: float = 0.5,
        retry_on: Optional[List[Type[Exception]]] = None,
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.retry_on = retry_on or [TransientError, NetworkError]

        # Статистика
        self.error_stats = {
            "transient": 0,
            "network": 0,
            "permanent": 0,
            "parse": 0,
            "other": 0,
        }
        self.retry_successes = 0
        self.retry_delays: list[float] = []

    def _should_retry(self, err: Exception) -> bool:
        """Проверяет, входит ли ошибка в список разрешённых для повтора."""
        return any(isinstance(err, cls) for cls in self.retry_on)

    def _classify_and_count(self, err: Exception) -> None:
        """Увеличивает счётчик по типу ошибки."""
        if isinstance(err, TransientError):
            self.error_stats["transient"] += 1
        elif isinstance(err, NetworkError):
            self.error_stats["network"] += 1
        elif isinstance(err, PermanentError):
            self.error_stats["permanent"] += 1
        elif isinstance(err, ParseError):
            self.error_stats["parse"] += 1
        else:
            self.error_stats["other"] += 1

    async def execute_with_retry(self, coro: Callable[..., Coroutine[Any, Any, Any]], *args, **kwargs):
        """
        Выполняет корутину с повторами и backoff.
        
        Возвращает результат coro или пробрасывает последнюю ошибку.
        """
        attempt = 0
        last_error = None
        while attempt <= self.max_retries:
            try:
                # Передаём номер попытки как keyword (attempt), чтобы вызывающий мог варьировать таймауты
                result = await coro(*args, attempt=attempt, **kwargs)
                # Если это не первая попытка, считаем как успешный повтор
                if attempt > 0:
                    self.retry_successes += 1
                return result
            except Exception as err:  # noqa: BLE001 - хотим логировать все
                self._classify_and_count(err)
                last_error = err
                # Решаем, нужно ли повторять
                if attempt >= self.max_retries or not self._should_retry(err):
                    logger.error(f"Retry failed: attempt={attempt}, error={err}")
                    raise

                # Считаем задержку (экспоненциальный backoff)
                delay = self.base_delay * (self.backoff_factor ** attempt)
                self.retry_delays.append(delay)
                attempt += 1

                logger.warning(
                    f"Retrying (attempt {attempt}/{self.max_retries}) after error: {err} | waiting {delay:.2f}s"
                )
                await asyncio.sleep(delay)

        # Если все попытки исчерпаны
        if last_error:
            raise last_error
        raise RuntimeError("execute_with_retry finished without result or error")