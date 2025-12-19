import asyncio
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

from crawler.config import ConfigLoader
from crawler.sitemap import SitemapParser
from crawler.stats import CrawlerStats
from crawler.advanced_crawler import AdvancedCrawler


async def test_sitemap_parser():
    print("\n=== Test: Sitemap Parser ===")
    
                                                  
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
        <loc>https://example.com/page1</loc>
    </url>
    <url>
        <loc>https://example.com/page2</loc>
    </url>
</urlset>"""

    class FakeResponse:
        def __init__(self, text: str, status: int = 200):
            self._text = text
            self.status = status
            self.headers = {}

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def text(self, encoding=None):
            return self._text

        def raise_for_status(self):
            if self.status >= 400:
                raise Exception(f"HTTP {self.status}")

    class FakeSession:
        def get(self, url):                          
            return FakeResponse(xml_text, status=200)

    session = FakeSession()
    print("DEBUG session type in test_sitemap_parser:", type(session))

    parser = SitemapParser(session=session)
    urls = await parser.fetch_sitemap("https://example.com/sitemap.xml", session)




    assert len(urls) == 2, f"Should extract 2 URLs, got {len(urls)}"
    assert "https://example.com/page1" in urls
    assert "https://example.com/page2" in urls
    
    print("PASS: Sitemap parser works correctly")


async def test_crawler_stats():
    print("\n=== Test: Crawler Stats ===")
    
    stats = CrawlerStats()
    stats.start()
    
                      
    stats.add_page("https://example.com/page1", "success", 0.5)
    stats.add_status_code(200)
    stats.add_page("https://example.com/page2", "success", 0.3)
    stats.add_status_code(200)
    stats.add_page("https://example.com/page3", "failed", 0.2, "404 Not Found")
    stats.add_status_code(404)
    
    stats.stop()
    
                          
    result = stats.get_stats()
    assert result["total_pages"] == 3
    assert result["successful"] == 2
    assert result["failed"] == 1
    assert result["status_codes"][200] == 2
    assert result["status_codes"][404] == 1
    assert result["performance"]["elapsed_time"] > 0
    
                       
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        json_file = f.name
    
    try:
        stats.export_to_json(json_file)
        assert os.path.exists(json_file), "JSON file should be created"
        os.remove(json_file)
    except Exception:
        if os.path.exists(json_file):
            os.remove(json_file)
        raise
    
                            
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.html') as f:
        html_file = f.name
    
    try:
        stats.export_to_html_report(html_file)
        assert os.path.exists(html_file), "HTML file should be created"
                                                    
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Общая статистика" in content
            assert "3" in content               
        os.remove(html_file)
    except Exception:
        if os.path.exists(html_file):
            os.remove(html_file)
        raise
    
    print("PASS: Crawler stats work correctly")


async def test_config_loader():
    print("\n=== Test: Config Loader ===")
    
                                             
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
        f.write("""
crawler:
  max_concurrent: 5
  max_depth: 2

urls:
  start_urls:
    - "https://example.com"
""")
        config_file = f.name
    
    try:
        config = ConfigLoader.load_from_file(config_file)
        
        assert config["crawler"]["max_concurrent"] == 5
        assert config["crawler"]["max_depth"] == 2
        assert config["urls"]["start_urls"] == ["https://example.com"]
                                                                
        assert "max_pages" in config["crawler"]
        
        os.remove(config_file)
    except Exception:
        if os.path.exists(config_file):
            os.remove(config_file)
        raise
    
    print("PASS: Config loader works correctly")


async def test_advanced_crawler_init():
    print("\n=== Test: Advanced Crawler Init ===")
    
                                      
    config = {
        "crawler": {
            "max_concurrent": 2,
            "max_depth": 1,
        },
        "urls": {
            "start_urls": ["https://example.com"],
        },
        "storage": {
            "type": "none",
        },
    }
    
    crawler = AdvancedCrawler(config=config)
    
    assert crawler.crawler is not None
    assert crawler.stats is not None
    assert crawler.sitemap_parser is not None
    assert crawler.crawler.max_workers == 2
    assert crawler.crawler.max_depth == 1
    
    await crawler.close()
    
    print("PASS: Advanced crawler initializes correctly")


async def test_advanced_crawler_from_config():
    print("\n=== Test: Advanced Crawler From Config ===")
    
                                             
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
        f.write("""
crawler:
  max_concurrent: 3
  max_depth: 2

urls:
  start_urls:
    - "https://example.com"

storage:
  type: "none"
""")
        config_file = f.name
    
    try:
        crawler = AdvancedCrawler.from_config(config_file)
        
        assert crawler.crawler.max_workers == 3
        assert crawler.crawler.max_depth == 2
        
        await crawler.close()
        os.remove(config_file)
    except Exception:
        if os.path.exists(config_file):
            os.remove(config_file)
        raise
    
    print("PASS: Advanced crawler from_config works correctly")


async def main():
    print("=== Running Day 7 Tests ===")
    
    await test_sitemap_parser()
    await test_crawler_stats()
    await test_config_loader()
    await test_advanced_crawler_init()
    await test_advanced_crawler_from_config()
    
    print("\n=== ALL DAY 7 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())

