"""
Командная строка для запуска краулера.

Поддерживает:
- Параметры командной строки
- Загрузку конфигурации из файла
- Комбинирование параметров CLI и конфигурации
"""
import argparse
import asyncio
import logging
import sys

from crawler.advanced_crawler import AdvancedCrawler
from crawler.config import ConfigLoader

logger = logging.getLogger(__name__)


def parse_args():
    """
    Парсит аргументы командной строки.
    
    Returns:
        Namespace с аргументами
    """
    parser = argparse.ArgumentParser(
        description="Асинхронный веб-краулер с поддержкой rate limiting, robots.txt и сохранением данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Простой запуск с одним URL
  python -m crawler.cli --urls https://example.com --max-pages 50

  # Использование конфигурационного файла
  python -m crawler.cli --config config.yaml

  # Комбинирование конфигурации и параметров CLI
  python -m crawler.cli --config config.yaml --max-pages 100 --output results.json

  # Соблюдение robots.txt и rate limiting
  python -m crawler.cli --urls https://example.com --respect-robots --rate-limit 2.0
        """
    )
    
    # Основные параметры
    parser.add_argument(
        "--urls",
        nargs="+",
        help="Стартовые URL для обхода (можно указать несколько)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="Максимальное количество страниц для обработки"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        help="Максимальная глубина обхода (0 = только стартовые URL)"
    )
    parser.add_argument(
        "--output",
        help="Файл для сохранения результатов (JSON, CSV или SQLite)"
    )
    
    # Конфигурация
    parser.add_argument(
        "--config",
        help="Путь к YAML конфигурационному файлу"
    )
    
    # Настройки краулера
    parser.add_argument(
        "--respect-robots",
        action="store_true",
        help="Соблюдать robots.txt"
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        help="Лимит запросов в секунду"
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        help="Максимальное количество одновременных запросов"
    )
    parser.add_argument(
        "--same-domain-only",
        action="store_true",
        help="Обрабатывать только URL того же домена"
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        default=None,
        help="Проверять SSL сертификаты (по умолчанию проверяются)"
    )
    parser.add_argument(
        "--no-verify-ssl",
        dest="verify_ssl",
        action="store_false",
        help="Не проверять SSL сертификаты"
    )
    
    # Логирование
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования"
    )
    parser.add_argument(
        "--log-file",
        help="Файл для записи логов"
    )
    
    # Экспорт статистики
    parser.add_argument(
        "--stats-json",
        help="Файл для экспорта статистики в JSON"
    )
    parser.add_argument(
        "--stats-html",
        help="Файл для экспорта HTML отчёта"
    )
    
    # Sitemap
    parser.add_argument(
        "--use-sitemap",
        action="store_true",
        help="Использовать sitemap.xml для обнаружения URL"
    )
    
    return parser.parse_args()


def merge_cli_with_config(config: dict, args: argparse.Namespace) -> dict:
    """
    Объединяет параметры CLI с конфигурацией из файла.
    
    Параметры CLI имеют приоритет над конфигурацией.
    
    Args:
        config: Конфигурация из файла
        args: Аргументы командной строки
    
    Returns:
        Объединённая конфигурация
    """
    # Обновляем конфигурацию краулера
    if args.max_concurrent:
        config["crawler"]["max_concurrent"] = args.max_concurrent
    if args.max_depth:
        config["crawler"]["max_depth"] = args.max_depth
    if args.max_pages:
        config["crawler"]["max_pages"] = args.max_pages
    if args.rate_limit:
        config["crawler"]["requests_per_second"] = args.rate_limit
    if args.respect_robots:
        config["crawler"]["respect_robots"] = True
    if args.same_domain_only:
        config["crawler"]["same_domain_only"] = True
    if args.verify_ssl is not None:
        config["crawler"]["verify_ssl"] = args.verify_ssl
    
    # Обновляем URL
    if args.urls:
        config["urls"]["start_urls"] = args.urls
    
    # Обновляем sitemap
    if args.use_sitemap:
        config["urls"]["use_sitemap"] = True
    
    # Обновляем хранилище
    if args.output:
        # Определяем тип хранилища по расширению файла
        if args.output.endswith(".json"):
            config["storage"]["type"] = "json"
            config["storage"]["json"]["filename"] = args.output
        elif args.output.endswith(".csv"):
            config["storage"]["type"] = "csv"
            config["storage"]["csv"]["filename"] = args.output
        elif args.output.endswith(".db"):
            config["storage"]["type"] = "sqlite"
            config["storage"]["sqlite"]["filename"] = args.output
    
    # Обновляем логирование
    if args.log_level:
        config["logging"]["level"] = args.log_level
    if args.log_file:
        config["logging"]["file"] = args.log_file
    
    # Обновляем экспорт статистики
    if args.stats_json:
        config["output"]["stats_json"] = args.stats_json
    if args.stats_html:
        config["output"]["stats_html"] = args.stats_html
    
    return config


async def main():
    """Главная функция CLI."""
    args = parse_args()
    
    # Загружаем конфигурацию
    if args.config:
        try:
            config = ConfigLoader.load_from_file(args.config)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            sys.exit(1)
    else:
        # Используем конфигурацию по умолчанию
        config = ConfigLoader.DEFAULT_CONFIG.copy()
    
    # Объединяем с параметрами CLI
    config = merge_cli_with_config(config, args)
    
    # Проверяем, что указаны URL
    start_urls = config.get("urls", {}).get("start_urls", [])
    if not start_urls:
        logger.error("No start URLs specified. Use --urls or specify in config file.")
        sys.exit(1)
    
    # Создаём краулер
    try:
        crawler = AdvancedCrawler(config=config)
    except Exception as e:
        logger.error(f"Error creating crawler: {e}", exc_info=True)
        sys.exit(1)
    
    # Запускаем краулинг
    try:
        logger.info("Starting crawl...")
        results = await crawler.crawl()
        
        # Выводим результаты
        stats = crawler.get_stats()
        print("\n" + "=" * 60)
        print("РЕЗУЛЬТАТЫ КРАУЛИНГА")
        print("=" * 60)
        print(f"Всего страниц: {stats['total_pages']}")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Заблокировано: {stats['blocked']}")
        print(f"Время работы: {stats['performance']['elapsed_time']:.2f} сек")
        print(f"Средняя скорость: {stats['performance']['avg_speed']:.2f} страниц/сек")
        print("=" * 60)
        
        # Экспортируем статистику
        crawler.export_to_json()
        crawler.export_to_html_report()
        
        logger.info("Crawl completed successfully")
    
    except KeyboardInterrupt:
        logger.warning("Crawl interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Error during crawl: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Закрываем краулер
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
