# Async Crawler

Полнофункциональный асинхронный веб-краулер на Python с поддержкой rate limiting, robots.txt, повторов и сохранения данных.

## Возможности

- ✅ Асинхронный обход сайтов с контролем конкурентности
- 🚦 Rate limiting (глобально и по доменам)
- 🤖 Соблюдение robots.txt
- 🔄 Автоматические повторы с экспоненциальным backoff
- 📊 Детальная статистика и HTML отчёты
- 💾 Сохранение данных в JSON, CSV и SQLite
- 🗺️ Поддержка sitemap.xml
- ⚙️ Конфигурация через YAML файл
- 📝 Структурированное логирование
- 🖥️ CLI интерфейс

## Установка

```bash
pip install -r requirements.txt
```

## Быстрый старт

### Использование CLI

```bash
# Простой запуск
PYTHONPATH=./src python -m crawler.cli --urls https://example.com --max-pages 50

# С конфигурационным файлом
PYTHONPATH=./src python -m crawler.cli --config config.yaml

# С дополнительными параметрами
PYTHONPATH=./src python -m crawler.cli \
  --urls https://example.com \
  --max-pages 100 \
  --max-depth 2 \
  --rate-limit 2.0 \
  --respect-robots \
  --output results.json
```

### Использование в коде

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    # Из конфигурационного файла
    crawler = AdvancedCrawler.from_config("config.yaml")
    
    # Или с настройками вручную
    from crawler.config import ConfigLoader
    config = ConfigLoader.DEFAULT_CONFIG.copy()
    config["urls"]["start_urls"] = ["https://example.com"]
    crawler = AdvancedCrawler(config=config)
    
    # Запускаем краулинг
    results = await crawler.crawl()
    
    # Получаем статистику
    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")
    
    # Экспортируем отчёты
    crawler.export_to_json("stats.json")
    crawler.export_to_html_report("report.html")
    
    await crawler.close()

asyncio.run(main())
```

## Конфигурация

Создайте файл `config.yaml` на основе `config.example.yaml`:

```yaml
crawler:
  max_concurrent: 5
  max_depth: 2
  max_pages: 50
  requests_per_second: 2.0
  respect_robots: true

urls:
  start_urls:
    - "https://example.com"
  use_sitemap: true

storage:
  type: "json"
  json:
    filename: "results.json"
```

## Параметры CLI

- `--urls` - Стартовые URL (можно указать несколько)
- `--max-pages` - Максимальное количество страниц
- `--max-depth` - Максимальная глубина обхода
- `--output` - Файл для сохранения результатов
- `--config` - Путь к конфигурационному файлу
- `--respect-robots` - Соблюдать robots.txt
- `--rate-limit` - Лимит запросов в секунду
- `--max-concurrent` - Максимальное количество одновременных запросов
- `--same-domain-only` - Обрабатывать только URL того же домена
- `--use-sitemap` - Использовать sitemap.xml
- `--log-level` - Уровень логирования (DEBUG, INFO, WARNING, ERROR)
- `--log-file` - Файл для записи логов
- `--stats-json` - Файл для экспорта статистики в JSON
- `--stats-html` - Файл для HTML отчёта

## Структура проекта

```
src/crawler/
├── advanced_crawler.py  # Главный класс AdvancedCrawler
├── fetcher.py           # Основной краулер AsyncCrawler
├── parser.py            # Парсер HTML
├── queue_manager.py     # Управление очередью и семафорами
├── rate_limiter.py      # Rate limiting и robots.txt
├── retry.py             # Повторы и обработка ошибок
├── sitemap.py           # Парсер sitemap.xml
├── stats.py             # Статистика и отчёты
├── storage.py           # Сохранение данных
├── config.py            # Загрузка конфигурации
└── cli.py               # CLI интерфейс
```

## Тестирование

```bash
# Тесты для каждого дня
PYTHONPATH=./src python tests/day1-tests.py
PYTHONPATH=./src python tests/day2-tests.py
PYTHONPATH=./src python tests/day3-tests.py
PYTHONPATH=./src python tests/day4-tests.py
PYTHONPATH=./src python tests/day5-tests.py
PYTHONPATH=./src python tests/day6-tests.py
```

## Лицензия

MIT

