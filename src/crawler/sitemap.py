import asyncio
import logging
from typing import List, Set
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import aiohttp

logger = logging.getLogger(__name__)


class SitemapParser:
    
    def __init__(self, session: aiohttp.ClientSession = None):
        self.session = session
        self.visited_sitemaps: Set[str] = set()                                      
    
    async def fetch_sitemap(self, sitemap_url: str, session: aiohttp.ClientSession = None) -> List[str]:
                                                      
        http_session = session or self.session
        
        if not http_session:
            raise ValueError("Session is required for fetching sitemap")
        
                                 
        normalized_url = sitemap_url.rstrip('/')
        
                                                                           
        if normalized_url in self.visited_sitemaps:
            logger.debug(f"Sitemap already visited: {normalized_url}")
            return []
        
        self.visited_sitemaps.add(normalized_url)
        
        try:
                               
            logger.info(f"Fetching sitemap: {normalized_url}")
            async with http_session.get(normalized_url) as response:
                response.raise_for_status()
                content = await response.text(encoding='utf-8')
            logger.debug(
                "Fetched sitemap: %s (status=%s, bytes~=%s)",
                normalized_url,
                getattr(response, "status", None),
                len(content) if content else 0,
            )
            
                        
            try:
                root = ET.fromstring(content)
            except ET.ParseError as e:
                logger.error(f"Error parsing sitemap XML: {e}")
                return []
            
                                                                           
                                                           
            namespaces = {}
            if root.tag.startswith('{'):
                                               
                namespace = root.tag.split('}')[0].strip('{')
                namespaces['s'] = namespace
                                                       
                url_tag = f"{{{namespace}}}url"
                sitemap_tag = f"{{{namespace}}}sitemap"
                loc_tag = f"{{{namespace}}}loc"
            else:
                               
                url_tag = "url"
                sitemap_tag = "sitemap"
                loc_tag = "loc"
            
            urls: List[str] = []
            
                                                          
                                                                     
            sitemap_elements = root.findall(f".//{sitemap_tag}")
            url_elements = root.findall(f".//{url_tag}")
            
            if sitemap_elements:
                                                                 
                logger.info(f"Found sitemap index with {len(sitemap_elements)} sitemaps")
                for sitemap_elem in sitemap_elements:
                    loc_elem = sitemap_elem.find(loc_tag)
                    if loc_elem is not None and loc_elem.text:
                        nested_sitemap_url = loc_elem.text.strip()
                                                                   
                        nested_urls = await self.fetch_sitemap(nested_sitemap_url, http_session)
                        urls.extend(nested_urls)
            
            if url_elements:
                                                     
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
        http_session = session or self.session
        if not http_session:
            raise ValueError("Session is required")
        
                                      
        sitemap_paths = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap/sitemap.xml",
            "/sitemaps/sitemap.xml",
        ]
        
                                          
        for path in sitemap_paths:
            sitemap_url = urljoin(base_url, path)
            try:
                urls = await self.fetch_sitemap(sitemap_url, http_session)
                if urls:
                    logger.info(f"Found sitemap at {sitemap_url} with {len(urls)} URLs")
                    return urls
            except Exception:
                                                  
                continue
        
                                                                 
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
