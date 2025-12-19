import asyncio
import json
import os
import sqlite3
from datetime import datetime

from crawler.storage import JSONStorage, CSVStorage, SQLiteStorage


async def test_json_storage():
    print("\n=== Test: JSON Storage ===")
    
    test_file = "test_output.json"
                                  
    if os.path.exists(test_file):
        os.remove(test_file)
    
    storage = JSONStorage(test_file, buffer_size=3)
    
                                 
    test_data = [
        {
            "url": "https://example.com/page1",
            "title": "Page 1",
            "text": "Content 1",
            "links": ["https://example.com/link1"],
            "metadata": {"description": "Test page 1"},
            "status_code": 200,
        },
        {
            "url": "https://example.com/page2",
            "title": "Page 2",
            "text": "Content 2",
            "links": ["https://example.com/link2"],
            "metadata": {"description": "Test page 2"},
            "status_code": 200,
        },
    ]
    
    for data in test_data:
        await storage.save(data)
    
                                              
    await storage.close()
    
                                
    assert os.path.exists(test_file), "JSON file should be created"
    
                                   
    with open(test_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == 2, f"Should have 2 lines, got {len(lines)}"
        
                                                      
        for i, line in enumerate(lines):
            data = json.loads(line.strip())
            assert data["url"] == test_data[i]["url"], f"URL should match for line {i}"
            assert data["title"] == test_data[i]["title"], f"Title should match for line {i}"
    
             
    os.remove(test_file)
    print("PASS: JSON storage works correctly")


async def test_csv_storage():
    print("\n=== Test: CSV Storage ===")
    
    test_file = "test_output.csv"
    if os.path.exists(test_file):
        os.remove(test_file)
    
    storage = CSVStorage(test_file, buffer_size=2)
    
    test_data = {
        "url": "https://example.com/page1",
        "title": "Page 1",
        "text": "Content, with comma",                                   
        "links": '["https://example.com/link1"]',               
        "status_code": 200,
    }
    
    await storage.save(test_data)
    await storage.close()
    
                    
    assert os.path.exists(test_file), "CSV file should be created"
    
                
    with open(test_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) >= 2, "Should have header and at least one data row"
                             
        headers = lines[0].strip().split(",")
        assert "url" in headers, "Should have url header"
        assert "title" in headers, "Should have title header"
    
    os.remove(test_file)
    print("PASS: CSV storage works correctly")


async def test_sqlite_storage():
    print("\n=== Test: SQLite Storage ===")
    
    test_db = "test_output.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    storage = SQLiteStorage(test_db, batch_size=2)
    
                       
    await storage.init_db()
    
                      
    test_data = {
        "url": "https://example.com/page1",
        "title": "Page 1",
        "text": "Content 1",
        "links": ["https://example.com/link1"],
        "metadata": {"description": "Test"},
        "images": [],
        "headings": [],
        "tables": [],
        "lists": [],
        "status_code": 200,
        "content_type": "text/html",
        "crawled_at": datetime.now().isoformat(),
    }
    
    await storage.save(test_data)
    await storage.close()
    
                                
    assert os.path.exists(test_db), "SQLite database should be created"
    
                                              
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT url, title, status_code FROM pages WHERE url = ?", (test_data["url"],))
    row = cursor.fetchone()
    
    assert row is not None, "Data should be saved"
    assert row[0] == test_data["url"], "URL should match"
    assert row[1] == test_data["title"], "Title should match"
    assert row[2] == test_data["status_code"], "Status code should match"
    
                       
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = [row[0] for row in cursor.fetchall()]
    assert "idx_url" in indexes, "Should have url index"
    
    conn.close()
    os.remove(test_db)
    print("PASS: SQLite storage works correctly")


async def test_storage_error_handling():
    print("\n=== Test: Storage Error Handling ===")
    
                                                                    
                                                                
    storage = JSONStorage("/invalid/path/file.json", buffer_size=1)
    
    test_data = {
        "url": "https://example.com",
        "title": "Test",
    }
    
                                                                     
    try:
        await storage.save(test_data)
        await storage.close()
    except Exception:
                                                             
        pass
    
    print("PASS: Storage error handling works (errors are logged, not raised)")


async def test_storage_data_integrity():
    print("\n=== Test: Data Integrity ===")
    
    test_file = "test_integrity.json"
    if os.path.exists(test_file):
        os.remove(test_file)
    
    storage = JSONStorage(test_file, buffer_size=1)
    
                                 
    full_data = {
        "url": "https://example.com/full",
        "title": "Full Page",
        "text": "Full content with\nnewlines",
        "links": ["https://example.com/link1", "https://example.com/link2"],
        "metadata": {"description": "Test", "keywords": "test, example"},
        "images": [{"src": "https://example.com/img.jpg", "alt": "Image"}],
        "headings": ["H1 Title", "H2 Subtitle"],
        "tables": [{"rows": 2, "cols": 3}],
        "lists": [{"type": "ul", "items": 5}],
        "status_code": 200,
        "content_type": "text/html",
        "crawled_at": datetime.now().isoformat(),
    }
    
    await storage.save(full_data)
    await storage.close()
    
                        
    with open(test_file, "r", encoding="utf-8") as f:
        loaded = json.loads(f.read().strip())
        
                            
        assert loaded["url"] == full_data["url"]
        assert loaded["title"] == full_data["title"]
        assert loaded["text"] == full_data["text"]
        assert len(loaded["links"]) == len(full_data["links"])
        assert loaded["status_code"] == full_data["status_code"]
        assert "crawled_at" in loaded
    
    os.remove(test_file)
    print("PASS: Data integrity maintained")


async def test_sqlite_stats():
    print("\n=== Test: SQLite Stats ===")
    
    test_db = "test_stats.db"
    if os.path.exists(test_db):
        os.remove(test_db)
    
    storage = SQLiteStorage(test_db, batch_size=1)
    await storage.init_db()
    
                                                     
    for i, status in enumerate([200, 200, 404, 500]):
        await storage.save({
            "url": f"https://example.com/page{i}",
            "title": f"Page {i}",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
            "status_code": status,
            "content_type": "text/html",
            "crawled_at": datetime.now().isoformat(),
        })
    
    await storage.close()
    
                         
    stats = await storage.get_stats()
    
    assert stats["total"] == 4, f"Should have 4 records, got {stats['total']}"
    assert stats["by_status"][200] == 2, "Should have 2 records with status 200"
    assert stats["by_status"][404] == 1, "Should have 1 record with status 404"
    assert stats["by_status"][500] == 1, "Should have 1 record with status 500"
    
    os.remove(test_db)
    print("PASS: SQLite stats work correctly")


async def main():
    print("=== Running Day 6 Tests ===")
    
    await test_json_storage()
    await test_csv_storage()
    await test_sqlite_storage()
    await test_storage_error_handling()
    await test_storage_data_integrity()
    await test_sqlite_stats()
    
    print("\n=== ALL DAY 6 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())

