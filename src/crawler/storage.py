"""
Модуль для асинхронного сохранения данных краулера.

Поддерживает несколько форматов:
- JSON: построчная запись в JSON Lines (JSONL) для больших объёмов
- CSV: автоматическое определение заголовков, асинхронная запись
- SQLite: база данных с batch-вставками и индексами

Все операции асинхронные и не блокируют основной поток краулера.
"""
import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiofiles
import aiosqlite

logger = logging.getLogger(__name__)


class DataStorage(ABC):
    """
    Абстрактный базовый класс для сохранения данных краулера.
    
    Все реализации должны наследоваться от этого класса и реализовать
    методы save() и close().
    """
    
    @abstractmethod
    async def save(self, data: Dict[str, Any]) -> None:
        """
        Сохраняет данные одной страницы.
        
        Args:
            data: Словарь с данными страницы:
                - url: URL страницы
                - title: Заголовок страницы
                - text: Текст страницы
                - links: Список ссылок
                - metadata: Метаданные (description, keywords)
                - images: Список изображений
                - headings: Заголовки (h1, h2, h3)
                - tables: Таблицы
                - lists: Списки
                - status: HTTP статус код
                - crawled_at: Время обхода (datetime)
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """
        Закрывает соединение/файл и освобождает ресурсы.
        
        Должен вызываться после завершения работы краулера.
        """
        pass
    
    def _normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализует данные для сохранения: добавляет стандартные поля.
        
        Args:
            data: Исходные данные от парсера
        
        Returns:
            Нормализованные данные со всеми обязательными полями
        """
        # Стандартизируем формат данных
        normalized = {
            "url": data.get("url", ""),
            "title": data.get("title", ""),
            "text": data.get("text", ""),
            "links": data.get("links", []),
            "metadata": data.get("metadata", {}),
            "images": data.get("images", []),
            "headings": data.get("headings", []),
            "tables": data.get("tables", []),
            "lists": data.get("lists", []),
            # Берём статус-код из status_code, иначе из status (для обратной совместимости)
            "status_code": data.get("status_code", data.get("status", 0)),
            "crawled_at": data.get("crawled_at", datetime.now().isoformat()),
            "content_type": data.get("content_type", "text/html"),
        }
        return normalized


class JSONStorage(DataStorage):
    """
    Сохранение данных в JSON файл.
    
    Использует формат JSON Lines (JSONL): каждая строка - отдельный JSON объект.
    Это позволяет обрабатывать большие объёмы данных без загрузки всего файла в память.
    
    Особенности:
    - Асинхронная запись через aiofiles
    - Построчная запись (append mode)
    - Автоматическое форматирование JSON
    - Буферизация для производительности
    """
    
    def __init__(self, filename: str, buffer_size: int = 10):
        """
        Инициализирует JSON хранилище.
        
        Args:
            filename: Путь к JSON файлу
            buffer_size: Размер буфера (количество записей перед записью на диск)
        """
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []  # Буфер для накопления записей
        self.buffer_lock = asyncio.Lock()  # Блокировка для потокобезопасности
        self.file_handle: Optional[aiofiles.threadpool.AsyncTextIOWrapper] = None
        self.total_saved = 0  # Счётчик сохранённых записей
    
    async def _flush_buffer(self) -> None:
        """
        Записывает буфер на диск.
        
        Вызывается автоматически когда буфер заполнен или при close().
        """
        if not self.buffer:
            return
        
        async with self.buffer_lock:
            # Открываем файл в режиме append (добавление в конец)
            async with aiofiles.open(self.filename, "a", encoding="utf-8") as f:
                # Записываем каждую запись как отдельную строку JSON
                for item in self.buffer:
                    json_line = json.dumps(item, ensure_ascii=False) + "\n"
                    await f.write(json_line)
            
            # Очищаем буфер после записи
            saved_count = len(self.buffer)
            self.buffer.clear()
            self.total_saved += saved_count
            logger.debug(f"Flushed {saved_count} records to {self.filename}")
    
    async def save(self, data: Dict[str, Any]) -> None:
        """
        Сохраняет данные страницы в JSON файл.
        
        Данные добавляются в буфер и записываются на диск когда буфер заполнен.
        
        Args:
            data: Данные страницы для сохранения
        """
        try:
            # Нормализуем данные
            normalized = self._normalize_data(data)
            
            # Добавляем в буфер
            async with self.buffer_lock:
                self.buffer.append(normalized)
            
            # Если буфер заполнен, записываем на диск
            if len(self.buffer) >= self.buffer_size:
                await self._flush_buffer()
        
        except Exception as e:
            # Логируем ошибку, но не прерываем работу краулера
            logger.error(f"Error saving to JSON: {e}", exc_info=True)
    
    async def close(self) -> None:
        """
        Закрывает хранилище: записывает оставшийся буфер и закрывает файл.
        """
        # Записываем оставшиеся данные из буфера
        await self._flush_buffer()
        logger.info(f"JSONStorage closed. Total saved: {self.total_saved} records to {self.filename}")


class CSVStorage(DataStorage):
    """
    Сохранение данных в CSV файл.
    
    Особенности:
    - Автоматическое определение заголовков из первого сохранённого объекта
    - Асинхронная запись через aiofiles
    - Обработка специальных символов (экранирование)
    - Поддержка различных кодировок (UTF-8)
    - Буферизация для производительности
    """
    
    def __init__(self, filename: str, buffer_size: int = 10):
        """
        Инициализирует CSV хранилище.
        
        Args:
            filename: Путь к CSV файлу
            buffer_size: Размер буфера перед записью на диск
        """
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = asyncio.Lock()
        self.headers: Optional[List[str]] = None  # Заголовки CSV (определяются автоматически)
        self.total_saved = 0
    
    def _flatten_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует вложенные структуры в плоский формат для CSV.
        
        Списки и словари преобразуются в строки (JSON).
        
        Args:
            data: Исходные данные
        
        Returns:
            Плоский словарь для CSV
        """
        flattened = {}
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                # Преобразуем списки и словари в JSON строку
                flattened[key] = json.dumps(value, ensure_ascii=False)
            else:
                flattened[key] = value
        return flattened
    
    async def _flush_buffer(self) -> None:
        """
        Записывает буфер в CSV файл.
        """
        if not self.buffer:
            return
        
        async with self.buffer_lock:
            # Определяем заголовки из первого объекта, если ещё не определены
            if self.headers is None and self.buffer:
                # Берём все ключи из первого объекта
                first_item = self._flatten_data(self.buffer[0])
                self.headers = list(first_item.keys())
                
                # Создаём файл с заголовками (если файл новый)
                async with aiofiles.open(self.filename, "w", encoding="utf-8", newline="") as f:
                    # Записываем заголовки
                    await f.write(",".join(self.headers) + "\n")
            
            # Открываем файл в режиме append
            async with aiofiles.open(self.filename, "a", encoding="utf-8", newline="") as f:
                # Записываем каждую строку
                for item in self.buffer:
                    flattened = self._flatten_data(item)
                    # Формируем CSV строку вручную (проще чем через csv.writer в async)
                    row_values = []
                    for header in self.headers:
                        value = flattened.get(header, "")
                        # Экранируем специальные символы для CSV
                        if value is None:
                            value = ""
                        value_str = str(value)
                        # Если содержит запятую, кавычки или перенос строки - оборачиваем в кавычки
                        if "," in value_str or '"' in value_str or "\n" in value_str or "\r" in value_str:
                            value_str = '"' + value_str.replace('"', '""') + '"'
                        row_values.append(value_str)
                    csv_line = ",".join(row_values) + "\n"
                    await f.write(csv_line)
            
            saved_count = len(self.buffer)
            self.buffer.clear()
            self.total_saved += saved_count
            logger.debug(f"Flushed {saved_count} records to CSV {self.filename}")
    
    async def save(self, data: Dict[str, Any]) -> None:
        """
        Сохраняет данные страницы в CSV файл.
        
        Args:
            data: Данные страницы
        """
        try:
            normalized = self._normalize_data(data)
            async with self.buffer_lock:
                self.buffer.append(normalized)
            
            if len(self.buffer) >= self.buffer_size:
                await self._flush_buffer()
        
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}", exc_info=True)
    
    async def close(self) -> None:
        """
        Закрывает CSV хранилище: записывает буфер и закрывает файл.
        """
        await self._flush_buffer()
        logger.info(f"CSVStorage closed. Total saved: {self.total_saved} records to {self.filename}")


class SQLiteStorage(DataStorage):
    """
    Сохранение данных в SQLite базу данных.
    
    Особенности:
    - Асинхронная работа через aiosqlite
    - Batch-вставки для производительности
    - Индексы для быстрого поиска
    - Автоматическое создание таблиц
    - Поддержка больших объёмов данных
    """
    
    def __init__(self, db_path: str, batch_size: int = 50):
        """
        Инициализирует SQLite хранилище.
        
        Args:
            db_path: Путь к файлу базы данных SQLite
            batch_size: Размер batch для вставки (количество записей перед commit)
        """
        self.db_path = db_path
        self.batch_size = batch_size
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = asyncio.Lock()
        self.db: Optional[aiosqlite.Connection] = None
        self.total_saved = 0
        self._initialized = False
    
    async def init_db(self) -> None:
        """
        Инициализирует базу данных: создаёт таблицы и индексы.
        
        Вызывается автоматически при первом сохранении, но можно вызвать вручную.
        """
        if self._initialized:
            return
        
        # Открываем соединение с базой данных
        self.db = await aiosqlite.connect(self.db_path)
        
        # Создаём таблицу для страниц
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                text TEXT,
                links TEXT,  -- JSON строка со списком ссылок
                metadata TEXT,  -- JSON строка с метаданными
                images TEXT,  -- JSON строка со списком изображений
                headings TEXT,  -- JSON строка со списком заголовков
                tables TEXT,  -- JSON строка с таблицами
                lists TEXT,  -- JSON строка со списками
                status_code INTEGER,
                content_type TEXT,
                crawled_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Создаём индексы для быстрого поиска
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_url ON pages(url)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_crawled_at ON pages(crawled_at)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_status_code ON pages(status_code)")
        
        # Сохраняем изменения
        await self.db.commit()
        self._initialized = True
        logger.info(f"SQLite database initialized: {self.db_path}")
    
    async def _flush_buffer(self) -> None:
        """
        Записывает буфер в базу данных batch-вставкой.
        """
        if not self.buffer:
            return
        
        # Инициализируем БД если ещё не инициализирована
        if not self._initialized:
            await self.init_db()
        
        async with self.buffer_lock:
            try:
                # Подготавливаем данные для вставки
                insert_data = []
                for item in self.buffer:
                    normalized = self._normalize_data(item)
                    # Преобразуем списки и словари в JSON строки
                    insert_data.append((
                        normalized["url"],
                        normalized["title"],
                        normalized["text"],
                        json.dumps(normalized["links"], ensure_ascii=False),
                        json.dumps(normalized["metadata"], ensure_ascii=False),
                        json.dumps(normalized["images"], ensure_ascii=False),
                        json.dumps(normalized["headings"], ensure_ascii=False),
                        json.dumps(normalized["tables"], ensure_ascii=False),
                        json.dumps(normalized["lists"], ensure_ascii=False),
                        normalized["status_code"],
                        normalized["content_type"],
                        normalized["crawled_at"],
                    ))
                
                # Batch-вставка: используем INSERT OR REPLACE для обновления существующих записей
                await self.db.executemany("""
                    INSERT OR REPLACE INTO pages 
                    (url, title, text, links, metadata, images, headings, tables, lists, status_code, content_type, crawled_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_data)
                
                # Сохраняем изменения
                await self.db.commit()
                
                saved_count = len(self.buffer)
                self.buffer.clear()
                self.total_saved += saved_count
                logger.debug(f"Flushed {saved_count} records to SQLite {self.db_path}")
            
            except Exception as e:
                # Откатываем транзакцию при ошибке
                await self.db.rollback()
                logger.error(f"Error flushing to SQLite: {e}", exc_info=True)
                raise
    
    async def save(self, data: Dict[str, Any]) -> None:
        """
        Сохраняет данные страницы в SQLite базу данных.
        
        Args:
            data: Данные страницы
        """
        try:
            async with self.buffer_lock:
                self.buffer.append(data)
            
            # Если буфер заполнен, записываем в БД
            if len(self.buffer) >= self.batch_size:
                await self._flush_buffer()
        
        except Exception as e:
            logger.error(f"Error saving to SQLite: {e}", exc_info=True)
    
    async def close(self) -> None:
        """
        Закрывает SQLite хранилище: записывает буфер и закрывает соединение.
        """
        # Записываем оставшиеся данные
        await self._flush_buffer()
        
        # Закрываем соединение
        if self.db:
            await self.db.close()
            logger.info(f"SQLiteStorage closed. Total saved: {self.total_saved} records to {self.db_path}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику по сохранённым данным.
        
        Если соединение закрыто, временно переоткрывает его для получения статистики.
        
        Returns:
            Словарь со статистикой: общее количество записей, по статусам и т.д.
        """
        # Если БД не инициализирована, возвращаем пустую статистику
        if not self._initialized:
            return {"total": 0}
        
        # Проверяем, нужно ли переоткрыть соединение
        need_reopen = False
        if not self.db:
            need_reopen = True
        else:
            # Проверяем, не закрыто ли соединение
            try:
                # Пробуем выполнить простой запрос для проверки соединения
                await self.db.execute("SELECT 1")
            except (ValueError, AttributeError):
                # Соединение закрыто, нужно переоткрыть
                need_reopen = True
        
        # Временно переоткрываем соединение если нужно
        temp_db = None
        if need_reopen:
            try:
                temp_db = await aiosqlite.connect(self.db_path)
            except Exception as e:
                logger.error(f"Error reopening SQLite connection for stats: {e}")
                return {"total": 0}
        
        # Используем временное соединение или существующее
        db_to_use = temp_db if need_reopen else self.db
        
        try:
            # Общее количество записей
            cursor = await db_to_use.execute("SELECT COUNT(*) FROM pages")
            total = (await cursor.fetchone())[0]
            
            # Количество по статусам
            cursor = await db_to_use.execute("""
                SELECT status_code, COUNT(*) 
                FROM pages 
                GROUP BY status_code
            """)
            status_counts = {row[0]: row[1] for row in await cursor.fetchall()}
            
            return {
                "total": total,
                "by_status": status_counts,
            }
        except Exception as e:
            logger.error(f"Error getting SQLite stats: {e}", exc_info=True)
            return {"total": 0}
        finally:
            # Закрываем временное соединение если открывали
            if need_reopen and temp_db:
                await temp_db.close()
