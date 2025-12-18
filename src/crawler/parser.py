import asyncio
import logging
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, SoupStrainer

logger = logging.getLogger("HTMLParser")


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> Dict:
        """
        Разбор HTML с извлечением ключевых данных.
        Возвращает частичные результаты даже при ошибках парсинга.
        """
        try:
            soup = BeautifulSoup(html, "lxml", parse_only=SoupStrainer(True))
        except Exception as e:
            logger.warning(f"Ошибка парсинга для {url}: {e}")
            return {"url": url, "error": str(e)}

        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        text = soup.get_text(separator="\n", strip=True) or ""
        links = self.extract_links(soup, base_url=url)
        metadata = self.extract_metadata(soup)
        images = self.extract_images(soup, base_url=url)
        headings = self.extract_headings(soup)
        tables = self.extract_tables(soup)
        lists = self.extract_lists(soup)

        return {
            "url": url,
            "title": title,
            "text": text,
            "links": links,
            "metadata": metadata,
            "images": images,
            "headings": headings,
            "tables": tables,
            "lists": lists,
        }

    def _is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return bool(parsed.scheme and parsed.netloc)

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href:
                continue
            if href.startswith("#"):
                continue
            if href.startswith("javascript:"):
                continue
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue

            absolute = urljoin(base_url, href)
            if self._is_valid_url(absolute):
                links.append(absolute)

        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        if selector:
            element = soup.select_one(selector)
            return element.get_text(strip=True) if element else ""
        return soup.get_text(strip=True) or ""

    def extract_metadata(self, soup: BeautifulSoup) -> Dict:
        metadata: Dict[str, str] = {}
        metadata["title"] = soup.title.string.strip() if soup.title and soup.title.string else ""

        description_tag = soup.find("meta", attrs={"name": "description"})
        keywords_tag = soup.find("meta", attrs={"name": "keywords"})

        metadata["description"] = (description_tag.get("content") if description_tag else "") or ""
        metadata["keywords"] = (keywords_tag.get("content") if keywords_tag else "") or ""

        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
        images: List[Dict[str, str]] = []
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if not src:
                continue
            absolute = urljoin(base_url, src)
            if not self._is_valid_url(absolute):
                continue
            images.append(
                {
                    "src": absolute,
                    "alt": (img.get("alt") or "").strip(),
                }
            )
        return images

    def extract_headings(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        headings: Dict[str, List[str]] = {"h1": [], "h2": [], "h3": []}
        for level in headings.keys():
            for tag in soup.find_all(level):
                text = tag.get_text(strip=True)
                if text:
                    headings[level].append(text)
        return headings

    def extract_tables(self, soup: BeautifulSoup) -> List[List[List[str]]]:
        tables: List[List[List[str]]] = []
        for table in soup.find_all("table"):
            rows: List[List[str]] = []
            for tr in table.find_all("tr"):
                cells = [cell.get_text(strip=True) for cell in tr.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> Dict[str, List[List[str]]]:
        result: Dict[str, List[List[str]]] = {"ul": [], "ol": []}
        for list_tag, key in (("ul", "ul"), ("ol", "ol")):
            for tag in soup.find_all(list_tag):
                items = [li.get_text(strip=True) for li in tag.find_all("li")]
                if items:
                    result[key].append(items)
        return result