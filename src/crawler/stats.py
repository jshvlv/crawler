import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class CrawlerStats:
    
    def __init__(self):
                           
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        
                  
        self.total_pages = 0                            
        self.successful = 0                     
        self.failed = 0                      
        self.blocked = 0                              
        
                                    
        self.status_codes: Dict[int, int] = defaultdict(int)
        
                               
        self.domain_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {
            "pages": 0,
            "successful": 0,
            "failed": 0,
        })
        
                                               
        self.page_times: List[float] = []                                   
        
                
        self.errors: List[Dict[str, str]] = []                               
    
    def start(self) -> None:
        self.start_time = time.time()
        logger.info("Crawler stats started")
    
    def stop(self) -> None:
        self.end_time = time.time()
        logger.info("Crawler stats stopped")
    
    def add_page(self, url: str, status: str, page_time: float, error: Optional[str] = None) -> None:
        self.total_pages += 1
        self.page_times.append(page_time)
        
                                
        try:
            domain = urlparse(url).netloc
            if ':' in domain:
                domain = domain.split(':')[0]
        except Exception:
            domain = "unknown"
        
                                        
        self.domain_stats[domain]["pages"] += 1
        
                                    
        if status == "success":
            self.successful += 1
            self.domain_stats[domain]["successful"] += 1
        elif status == "failed":
            self.failed += 1
            self.domain_stats[domain]["failed"] += 1
            if error:
                self.errors.append({"url": url, "error": error, "domain": domain})
        elif status == "blocked":
            self.blocked += 1
    
    def add_status_code(self, status_code: int) -> None:
        self.status_codes[status_code] += 1
    
    def get_stats(self) -> Dict:
        elapsed_time = (self.end_time or time.time()) - (self.start_time or time.time())
        
                                    
        avg_speed = self.total_pages / elapsed_time if elapsed_time > 0 else 0
        
                                                    
        avg_page_time = sum(self.page_times) / len(self.page_times) if self.page_times else 0
        
                                           
        top_domains = sorted(
            self.domain_stats.items(),
            key=lambda x: x[1]["pages"],
            reverse=True
        )[:10]          
        
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "blocked": self.blocked,
            "status_codes": dict(self.status_codes),
            "domains": {domain: dict(stats) for domain, stats in self.domain_stats.items()},
            "top_domains": [
                {"domain": domain, "pages": stats["pages"], "successful": stats["successful"], "failed": stats["failed"]}
                for domain, stats in top_domains
            ],
            "performance": {
                "elapsed_time": elapsed_time,
                "avg_speed": avg_speed,                     
                "avg_page_time": avg_page_time,                      
                "total_time": sum(self.page_times),
            },
            "errors_count": len(self.errors),
        }
    
    def export_to_json(self, filename: str) -> None:
        stats = self.get_stats()
        
                              
        export_data = {
            "exported_at": datetime.now().isoformat(),
            "stats": stats,
            "errors": self.errors[:100],                                                 
        }
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Stats exported to JSON: {filename}")
        except Exception as e:
            logger.error(f"Error exporting stats to JSON: {e}", exc_info=True)
    
    def export_to_html_report(self, filename: str) -> None:
        stats = self.get_stats()
        
                        
        html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Отчёт краулера</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .stat-card.error {{
            background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        }}
        .stat-value {{
            font-size: 2.5em;
            font-weight: bold;
            margin: 10px 0;
        }}
        .stat-label {{
            font-size: 0.9em;
            opacity: 0.9;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .progress-bar {{
            width: 100%;
            height: 30px;
            background-color: #e0e0e0;
            border-radius: 15px;
            overflow: hidden;
            margin: 10px 0;
        }}
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #4CAF50, #8BC34A);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }}
        .timestamp {{
            color: #888;
            font-size: 0.9em;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Отчёт краулера</h1>
        <p class="timestamp">Создан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>Общая статистика</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Всего страниц</div>
                <div class="stat-value">{stats['total_pages']}</div>
            </div>
            <div class="stat-card success">
                <div class="stat-label">Успешно</div>
                <div class="stat-value">{stats['successful']}</div>
            </div>
            <div class="stat-card error">
                <div class="stat-label">Ошибок</div>
                <div class="stat-value">{stats['failed']}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Заблокировано</div>
                <div class="stat-value">{stats['blocked']}</div>
            </div>
        </div>
        
        <h2>Производительность</h2>
        <table>
            <tr>
                <th>Метрика</th>
                <th>Значение</th>
            </tr>
            <tr>
                <td>Время работы</td>
                <td>{stats['performance']['elapsed_time']:.2f} сек</td>
            </tr>
            <tr>
                <td>Средняя скорость</td>
                <td>{stats['performance']['avg_speed']:.2f} страниц/сек</td>
            </tr>
            <tr>
                <td>Среднее время на страницу</td>
                <td>{stats['performance']['avg_page_time']:.3f} сек</td>
            </tr>
        </table>
        
        <h2>Распределение по статус-кодам</h2>
        <table>
            <tr>
                <th>Статус-код</th>
                <th>Количество</th>
                <th>Процент</th>
            </tr>
"""
        
                                           
        total_with_status = sum(stats['status_codes'].values())
        for status_code, count in sorted(stats['status_codes'].items()):
            percentage = (count / total_with_status * 100) if total_with_status > 0 else 0
            html += f"""
            <tr>
                <td>{status_code}</td>
                <td>{count}</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {percentage}%">{percentage:.1f}%</div>
                    </div>
                </td>
            </tr>
"""
        
        html += """
        </table>
        
        <h2>Топ доменов</h2>
        <table>
            <tr>
                <th>Домен</th>
                <th>Страниц</th>
                <th>Успешно</th>
                <th>Ошибок</th>
            </tr>
"""
        
                               
        for domain_info in stats['top_domains']:
            html += f"""
            <tr>
                <td>{domain_info['domain']}</td>
                <td>{domain_info['pages']}</td>
                <td>{domain_info['successful']}</td>
                <td>{domain_info['failed']}</td>
            </tr>
"""
        
        html += f"""
        </table>
        
        <h2>Ошибки</h2>
        <p>Всего ошибок: {stats['errors_count']}</p>
        <table>
            <tr>
                <th>URL</th>
                <th>Домен</th>
                <th>Ошибка</th>
            </tr>
"""
        
                                    
        for error in self.errors[:50]:
            html += f"""
            <tr>
                <td>{error['url'][:80]}...</td>
                <td>{error['domain']}</td>
                <td>{error['error'][:100]}</td>
            </tr>
"""
        
        html += """
        </table>
    </div>
</body>
</html>
"""
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"HTML report exported: {filename}")
        except Exception as e:
            logger.error(f"Error exporting HTML report: {e}", exc_info=True)
