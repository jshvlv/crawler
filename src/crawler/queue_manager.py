"""
Модуль для управления очередями URL и семафорами конкурентности.

Содержит:
- CrawlerQueue: очередь URL с поддержкой приоритетов
- SemaphoreManager: управление семафорами для контроля конкурентности по доменам
"""
import asyncio
import logging
from collections import defaultdict
from typing import Optional, Dict
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class CrawlerQueue:
    """
    Очередь URL для краулера с поддержкой приоритетов.
    
    Приоритеты: чем больше число, тем выше приоритет.
    URL с одинаковым приоритетом обрабатываются в порядке добавления (FIFO).
    """
    
    def __init__(self):
        # Словарь приоритетов: {приоритет: [список URL]}
        # Используем defaultdict для автоматического создания списков
        self._priority_queues: Dict[int, list] = defaultdict(list)
        
        # Множество всех URL в очереди (для быстрой проверки дубликатов)
        self._queued_urls: set = set()
        
        # Статистика: количество обработанных и неудачных URL
        self._processed_count: int = 0
        self._failed_count: int = 0
        
        # Словарь обработанных URL: {url: результат}
        self._processed_urls: Dict[str, Dict] = {}
        
        # Словарь неудачных URL: {url: описание_ошибки}
        self._failed_urls: Dict[str, str] = {}
        
        # Блокировка для потокобезопасности при работе с очередью
        self._lock = asyncio.Lock()
    
    def add_url(self, url: str, priority: int = 0) -> bool:
        """
        Добавляет URL в очередь с указанным приоритетом.
        
        Args:
            url: URL для добавления
            priority: Приоритет (больше = выше приоритет, по умолчанию 0)
        
        Returns:
            True если URL добавлен, False если уже был в очереди
        """
        # Нормализуем URL (убираем trailing slash и фрагменты для избежания дубликатов)
        normalized_url = self._normalize_url(url)
        
        # Проверяем, не был ли URL уже добавлен
        if normalized_url in self._queued_urls:
            return False
        
        # Добавляем в очередь соответствующего приоритета
        self._priority_queues[priority].append(normalized_url)
        self._queued_urls.add(normalized_url)
        
        logger.debug(f"Added URL to queue: {normalized_url} (priority: {priority})")
        return True
    
    def _normalize_url(self, url: str) -> str:
        """
        Нормализует URL: убирает фрагменты (#anchor) и trailing slash.
        
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
        
        normalized_url = url
        logger.debug(f"Normalized URL: {normalized_url}")

        return normalized_url
    
    async def get_next(self) -> Optional[str]:
        """
        Получает следующий URL из очереди с учётом приоритетов.
        
        Сначала обрабатываются URL с максимальным приоритетом,
        затем с меньшим и так далее.
        
        Returns:
            Следующий URL или None, если очередь пуста
        """
        async with self._lock:  # Потокобезопасный доступ к очереди
            # Если очередь пуста, возвращаем None
            if not self._queued_urls:
                return None
            
            # Находим максимальный приоритет (самый высокий)
            if not self._priority_queues:
                return None
            
            max_priority = max(self._priority_queues.keys())

            # Берём первый URL из очереди с максимальным приоритетом (FIFO)
            if self._priority_queues[max_priority]:
                url = self._priority_queues[max_priority].pop(0)
                self._queued_urls.remove(url)
                
                # Если очередь приоритета опустела, удаляем её
                if not self._priority_queues[max_priority]:
                    del self._priority_queues[max_priority]
                
                logger.debug(f"Retrieved URL from queue: {url} (priority: {max_priority})")
                return url
        
        return None
    
    def mark_processed(self, url: str, result: Optional[Dict] = None) -> None:
        """
        Помечает URL как успешно обработанный.
        
        Args:
            url: Обработанный URL
            result: Результат обработки (опционально)
        """
        normalized_url = self._normalize_url(url)
        self._processed_count += 1
        
        if result:
            self._processed_urls[normalized_url] = result
        else:
            self._processed_urls[normalized_url] = {"status": "processed"}
        
        logger.debug(f"Marked as processed: {normalized_url}")
    
    def mark_failed(self, url: str, error: str) -> None:
        """
        Помечает URL как неудачный с указанием ошибки.
        
        Args:
            url: URL, который не удалось обработать
            error: Описание ошибки
        """
        normalized_url = self._normalize_url(url)
        self._failed_count += 1
        self._failed_urls[normalized_url] = error
        
        logger.debug(f"Marked as failed: {normalized_url} - {error}")
    
    def get_stats(self) -> Dict:
        """
        Возвращает статистику очереди.
        
        Returns:
            Словарь со статистикой:
            - queued: количество URL в очереди
            - processed: количество обработанных URL
            - failed: количество неудачных URL
            - total: общее количество URL (queued + processed + failed)
        """
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
        """
        Проверяет, был ли URL уже обработан или помечен как неудачный.
        
        Args:
            url: URL для проверки
        
        Returns:
            True если URL уже обработан или неудачен
        """
        normalized_url = self._normalize_url(url)
        return normalized_url in self._processed_urls or normalized_url in self._failed_urls
    
    def get_processed_urls(self) -> Dict[str, Dict]:
        """Возвращает словарь всех обработанных URL."""
        return self._processed_urls.copy()
    
    def get_failed_urls(self) -> Dict[str, str]:
        """Возвращает словарь всех неудачных URL с ошибками."""
        return self._failed_urls.copy()


class SemaphoreManager:
    """
    Менеджер семафоров для контроля конкурентности запросов.
    
    Поддерживает:
    - Глобальное ограничение конкурентности (для всех запросов)
    - Ограничение по доменам (для каждого домена отдельно)
    - Отслеживание активных задач
    """
    
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 2):
        """
        Инициализирует менеджер семафоров.
        
        Args:
            global_limit: Максимальное количество одновременных запросов глобально
            per_domain_limit: Максимальное количество одновременных запросов к одному домену
        """
        # Глобальный семафор для ограничения всех запросов
        self._global_semaphore = asyncio.Semaphore(global_limit)
        
        # Словарь семафоров для каждого домена: {домен: Semaphore}
        self._domain_semaphores: Dict[str, asyncio.Semaphore] = {}
        
        # Лимит запросов на домен
        self._per_domain_limit = per_domain_limit
        
        # Блокировка для потокобезопасного создания семафоров доменов
        self._lock = asyncio.Lock()
        
        # Счётчики активных задач
        self._active_global = 0
        self._active_per_domain: Dict[str, int] = defaultdict(int)
    
    def _get_domain(self, url: str) -> str:
        """
        Извлекает домен из URL.
        
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
            return domain
        except Exception:
            # Если не удалось распарсить, возвращаем весь URL как домен
            return url
    
    async def acquire(self, domain: str) -> None:
        """
        Получает разрешение на выполнение запроса (acquire для обоих семафоров).
        
        Сначала получает глобальный семафор, затем семафор домена.
        Это гарантирует, что не будет превышен ни глобальный лимит, ни лимит домена.
        
        Args:
            domain: Домен, к которому будет выполнен запрос
        """
        # Получаем глобальный семафор (ограничение всех запросов)
        await self._global_semaphore.acquire()
        self._active_global += 1
        
        # Получаем или создаём семафор для домена
        async with self._lock:
            if domain not in self._domain_semaphores:
                # Создаём новый семафор для домена при первом обращении
                self._domain_semaphores[domain] = asyncio.Semaphore(self._per_domain_limit)
        
        # Получаем семафор домена (ограничение запросов к конкретному домену)
        domain_semaphore = self._domain_semaphores[domain]
        await domain_semaphore.acquire()
        self._active_per_domain[domain] += 1
        
        logger.debug(f"Acquired semaphores for domain: {domain} (global: {self._active_global}, domain: {self._active_per_domain[domain]})")
    
    def release(self, domain: str) -> None:
        """
        Освобождает семафоры после завершения запроса.
        
        Сначала освобождает семафор домена, затем глобальный.
        
        Args:
            domain: Домен, для которого завершился запрос
        """
        # Освобождаем семафор домена
        if domain in self._domain_semaphores:
            self._domain_semaphores[domain].release()
            self._active_per_domain[domain] = max(0, self._active_per_domain[domain] - 1)
        
        # Освобождаем глобальный семафор
        self._global_semaphore.release()
        self._active_global = max(0, self._active_global - 1)
        
        logger.debug(f"Released semaphores for domain: {domain} (global: {self._active_global}, domain: {self._active_per_domain.get(domain, 0)})")
    
    def get_active_stats(self) -> Dict:
        """
        Возвращает статистику активных задач.
        
        Returns:
            Словарь со статистикой:
            - global_active: количество активных глобальных задач
            - per_domain: словарь {домен: количество_активных_задач}
        """
        return {
            "global_active": self._active_global,
            "per_domain": dict(self._active_per_domain),
        }
