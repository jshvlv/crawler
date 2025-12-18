"""
Парсер sitemap.xml для извлечения URL.

Поддерживает:
- Обычные sitemap (sitemap.xml)
- Индексные sitemap (sitemap_index.xml)
- Рекурсивную обработку вложенных sitemap
- Извлечение всех URL из sitemap
"""
import asyncio
import logging
from typing import List, Set
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import aiohttp

logger = logging.getLogger(__name__)


class SitemapParser:
    """
    Парсер для загрузки и обработки sitemap.xml файлов.
    
    Поддерживает:
    - Обычные sitemap с <url> элементами
    - Индексные sitemap с <sitemap> элементами
    - Рекурсивную обработку вложенных sitemap
    """
    
    def __init__(self, session: aiohttp.ClientSession = None):
        """
        Инициализирует парсер sitemap.
        
        Args:
            session: HTTP сессия для загрузки sitemap (опционально)
        """
        self.session = session
        self.visited_sitemaps: Set[str] = set()  # Множество уже обработанных sitemap
    
    async def fetch_sitemap(self, sitemap_url: str, session: aiohttp.ClientSession = None) -> List[str]:
        """
        Загружает sitemap и извлекает все URL.
        
        Поддерживает:
        - Обычные sitemap: извлекает <loc> из <url> элементов
        - Индексные sitemap: рекурсивно обрабатывает вложенные sitemap
        
        Args:
            sitemap_url: URL sitemap файла (например, https://example.com/sitemap.xml)
            session: HTTP сессия (если не передана, используется self.session)
        
        Returns:
            Список всех URL из sitemap
        """
        # Используем переданную сессию или сохранённую
        http_session = session or self.session
        
        if not http_session:
            raise ValueError("Session is required for fetching sitemap")
        
        # Нормализуем URL sitemap
        normalized_url = sitemap_url.rstrip('/')
        
        # Проверяем, не обрабатывали ли уже этот sitemap (защита от циклов)
        if normalized_url in self.visited_sitemaps:
            logger.debug(f"Sitemap already visited: {normalized_url}")
            return []
        
        self.visited_sitemaps.add(normalized_url)
        
        try:
            # Загружаем sitemap
            logger.info(f"Fetching sitemap: {normalized_url}")
#region agent log
            with open("/Users/a1111/PycharmProjects/Async_Crawler/.cursor/debug.log", "a", encoding="utf-8") as _f:
                import json, time
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "pre-fix",
                    "hypothesisId": "H1",
                    "location": "sitemap.py:fetch_sitemap:before_get",
                    "message": "about to call http_session.get",
                    "data": {
                        "sitemap_url": normalized_url,
                        "http_session_type": type(http_session).__name__
                    },
                    "timestamp": int(time.time()*1000)
                }) + "\n")
#endregion
            async with http_session.get(normalized_url) as response:
                response.raise_for_status()
                content = await response.text(encoding='utf-8')
#region agent log
            with open("/Users/a1111/PycharmProjects/Async_Crawler/.cursor/debug.log", "a", encoding="utf-8") as _f:
                import json, time
                _f.write(json.dumps({
                    "sessionId": "debug-session",
                    "runId": "pre-fix",
                    "hypothesisId": "H2",
                    "location": "sitemap.py:fetch_sitemap:after_fetch",
                    "message": "fetched sitemap content",
                    "data": {
                        "status": response.status,
                        "content_snippet": content[:50]
                    },
                    "timestamp": int(time.time()*1000)
                }) + "\n")
#endregion
            
            # Парсим XML
            try:
                root = ET.fromstring(content)
            except ET.ParseError as e:
                logger.error(f"Error parsing sitemap XML: {e}")
                return []
            
            # Определяем namespace (sitemap может быть с namespace или без)
            # Пробуем найти namespace из корневого элемента
            namespaces = {}
            if root.tag.startswith('{'):
                # Есть namespace, извлекаем его
                namespace = root.tag.split('}')[0].strip('{')
                namespaces['s'] = namespace
                # Обновляем теги для поиска с namespace
                url_tag = f"{{{namespace}}}url"
                sitemap_tag = f"{{{namespace}}}sitemap"
                loc_tag = f"{{{namespace}}}loc"
            else:
                # Нет namespace
                url_tag = "url"
                sitemap_tag = "sitemap"
                loc_tag = "loc"
            
            urls: List[str] = []
            
            # Проверяем, это обычный sitemap или индексный
            # Ищем элементы <sitemap> (индексный) или <url> (обычный)
            sitemap_elements = root.findall(f".//{sitemap_tag}")
            url_elements = root.findall(f".//{url_tag}")
            
            if sitemap_elements:
                # Это индексный sitemap - обрабатываем рекурсивно
                logger.info(f"Found sitemap index with {len(sitemap_elements)} sitemaps")
                for sitemap_elem in sitemap_elements:
                    loc_elem = sitemap_elem.find(loc_tag)
                    if loc_elem is not None and loc_elem.text:
                        nested_sitemap_url = loc_elem.text.strip()
                        # Рекурсивно обрабатываем вложенный sitemap
                        nested_urls = await self.fetch_sitemap(nested_sitemap_url, http_session)
                        urls.extend(nested_urls)
            
            if url_elements:
                # Это обычный sitemap - извлекаем URL
                logger.info(f"Found {len(url_elements)} URLs in sitemap")
                for url_elem in url_elements:
                    loc_elem = url_elem.find(loc_tag)
                    if loc_elem is not None and loc_elem.text:
                        url = loc_elem.text.strip()
                        urls.append(url)
            
            logger.info(f"Extracted {len(urls)} URLs from sitemap {normalized_url}")
            return urls
        
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching sitemap {normalized_url}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error processing sitemap {normalized_url}: {e}", exc_info=True)
            return []
    
    async def discover_sitemap_urls(self, base_url: str, session: aiohttp.ClientSession = None) -> List[str]:
        """
        Пытается найти sitemap для домена.
        
        Проверяет стандартные пути:
        - /sitemap.xml
        - /sitemap_index.xml
        - /sitemap/sitemap.xml
        - robots.txt (ищет директиву Sitemap)
        
        Args:
            base_url: Базовый URL домена (например, https://example.com)
            session: HTTP сессия
        
        Returns:
            Список найденных URL из sitemap
        """
        http_session = session or self.session
        if not http_session:
            raise ValueError("Session is required")
        
        # Стандартные пути для sitemap
        sitemap_paths = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap/sitemap.xml",
            "/sitemaps/sitemap.xml",
        ]
        
        # Пробуем загрузить каждый sitemap
        for path in sitemap_paths:
            sitemap_url = urljoin(base_url, path)
            try:
                urls = await self.fetch_sitemap(sitemap_url, http_session)
                if urls:
                    logger.info(f"Found sitemap at {sitemap_url} with {len(urls)} URLs")
                    return urls
            except Exception:
                # Продолжаем пробовать другие пути
                continue
        
        # Также проверяем robots.txt на наличие директивы Sitemap
        try:
            robots_url = urljoin(base_url, "/robots.txt")
            async with http_session.get(robots_url) as response:
                if response.status == 200:
                    content = await response.text()
                    for line in content.split('\n'):
                        line = line.strip()
                        if line.lower().startswith('sitemap:'):
                            sitemap_url = line.split(':', 1)[1].strip()
                            urls = await self.fetch_sitemap(sitemap_url, http_session)
                            if urls:
                                logger.info(f"Found sitemap from robots.txt: {sitemap_url}")
                                return urls
        except Exception as e:
            logger.debug(f"Could not check robots.txt for sitemap: {e}")
        
        logger.warning(f"Could not find sitemap for {base_url}")
        return []
