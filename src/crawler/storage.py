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
    
    @abstractmethod
    async def save(self, data: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    async def close(self) -> None:
        pass
    
    def _normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
                                       
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
                                                                                           
            "status_code": data.get("status_code", data.get("status", 0)),
            "crawled_at": data.get("crawled_at", datetime.now().isoformat()),
            "content_type": data.get("content_type", "text/html"),
        }
        return normalized


class JSONStorage(DataStorage):
    
    def __init__(self, filename: str, buffer_size: int = 10):
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []                                
        self.buffer_lock = asyncio.Lock()                                     
        self.file_handle: Optional[aiofiles.threadpool.AsyncTextIOWrapper] = None
        self.total_saved = 0                               
    
    async def _flush_buffer(self) -> None:
        if not self.buffer:
            return
        
        async with self.buffer_lock:
                                                                 
            async with aiofiles.open(self.filename, "a", encoding="utf-8") as f:
                                                                    
                for item in self.buffer:
                    json_line = json.dumps(item, ensure_ascii=False) + "\n"
                    await f.write(json_line)
            
                                        
            saved_count = len(self.buffer)
            self.buffer.clear()
            self.total_saved += saved_count
            logger.debug(f"Flushed {saved_count} records to {self.filename}")
    
    async def save(self, data: Dict[str, Any]) -> None:
        try:
                                
            normalized = self._normalize_data(data)
            
                               
            async with self.buffer_lock:
                self.buffer.append(normalized)
            
                                                     
            if len(self.buffer) >= self.buffer_size:
                await self._flush_buffer()
        
        except Exception as e:
                                                              
            logger.error(f"Error saving to JSON: {e}", exc_info=True)
    
    async def close(self) -> None:
                                                
        await self._flush_buffer()
        logger.info(f"JSONStorage closed. Total saved: {self.total_saved} records to {self.filename}")


class CSVStorage(DataStorage):
    
    def __init__(self, filename: str, buffer_size: int = 10):
        self.filename = filename
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = asyncio.Lock()
        self.headers: Optional[List[str]] = None                                              
        self.total_saved = 0
    
    def _flatten_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        flattened = {}
        for key, value in data.items():
            if isinstance(value, (list, dict)):
                                                            
                flattened[key] = json.dumps(value, ensure_ascii=False)
            else:
                flattened[key] = value
        return flattened
    
    async def _flush_buffer(self) -> None:
        if not self.buffer:
            return
        
        async with self.buffer_lock:
                                                                             
            if self.headers is None and self.buffer:
                                                    
                first_item = self._flatten_data(self.buffer[0])
                self.headers = list(first_item.keys())
                
                                                              
                async with aiofiles.open(self.filename, "w", encoding="utf-8", newline="") as f:
                                          
                    await f.write(",".join(self.headers) + "\n")
            
                                            
            async with aiofiles.open(self.filename, "a", encoding="utf-8", newline="") as f:
                                          
                for item in self.buffer:
                    flattened = self._flatten_data(item)
                                                                                       
                    row_values = []
                    for header in self.headers:
                        value = flattened.get(header, "")
                                                                
                        if value is None:
                            value = ""
                        value_str = str(value)
                                                                                                   
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
        try:
            normalized = self._normalize_data(data)
            async with self.buffer_lock:
                self.buffer.append(normalized)
            
            if len(self.buffer) >= self.buffer_size:
                await self._flush_buffer()
        
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}", exc_info=True)
    
    async def close(self) -> None:
        await self._flush_buffer()
        logger.info(f"CSVStorage closed. Total saved: {self.total_saved} records to {self.filename}")


class SQLiteStorage(DataStorage):
    
    def __init__(self, db_path: str, batch_size: int = 50):
        self.db_path = db_path
        self.batch_size = batch_size
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = asyncio.Lock()
        self.db: Optional[aiosqlite.Connection] = None
        self.total_saved = 0
        self._initialized = False
    
    async def init_db(self) -> None:
        if self._initialized:
            return
        
                                             
        self.db = await aiosqlite.connect(self.db_path)
        
                                     
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
        
                                             
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_url ON pages(url)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_crawled_at ON pages(crawled_at)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS idx_status_code ON pages(status_code)")
        
                             
        await self.db.commit()
        self._initialized = True
        logger.info(f"SQLite database initialized: {self.db_path}")
    
    async def _flush_buffer(self) -> None:
        if not self.buffer:
            return
        
                                                        
        if not self._initialized:
            await self.init_db()
        
        async with self.buffer_lock:
            try:
                                                   
                insert_data = []
                for item in self.buffer:
                    normalized = self._normalize_data(item)
                                                                
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
                
                                                                                                 
                await self.db.executemany("""
                    INSERT OR REPLACE INTO pages 
                    (url, title, text, links, metadata, images, headings, tables, lists, status_code, content_type, crawled_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, insert_data)
                
                                     
                await self.db.commit()
                
                saved_count = len(self.buffer)
                self.buffer.clear()
                self.total_saved += saved_count
                logger.debug(f"Flushed {saved_count} records to SQLite {self.db_path}")
            
            except Exception as e:
                                                  
                await self.db.rollback()
                logger.error(f"Error flushing to SQLite: {e}", exc_info=True)
                raise
    
    async def save(self, data: Dict[str, Any]) -> None:
        try:
            async with self.buffer_lock:
                self.buffer.append(data)
            
                                                  
            if len(self.buffer) >= self.batch_size:
                await self._flush_buffer()
        
        except Exception as e:
            logger.error(f"Error saving to SQLite: {e}", exc_info=True)
    
    async def close(self) -> None:
                                      
        await self._flush_buffer()
        
                              
        if self.db:
            await self.db.close()
            logger.info(f"SQLiteStorage closed. Total saved: {self.total_saved} records to {self.db_path}")
    
    async def get_stats(self) -> Dict[str, Any]:
                                                                   
        if not self._initialized:
            return {"total": 0}
        
                                                    
        need_reopen = False
        if not self.db:
            need_reopen = True
        else:
                                                 
            try:
                                                                          
                await self.db.execute("SELECT 1")
            except (ValueError, AttributeError):
                                                       
                need_reopen = True
        
                                                      
        temp_db = None
        if need_reopen:
            try:
                temp_db = await aiosqlite.connect(self.db_path)
            except Exception as e:
                logger.error(f"Error reopening SQLite connection for stats: {e}")
                return {"total": 0}
        
                                                          
        db_to_use = temp_db if need_reopen else self.db
        
        try:
                                      
            cursor = await db_to_use.execute("SELECT COUNT(*) FROM pages")
            total = (await cursor.fetchone())[0]
            
                                    
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
                                                           
            if need_reopen and temp_db:
                await temp_db.close()
