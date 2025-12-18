"""
Загрузка конфигурации краулера из YAML файла.

Поддерживает настройки:
- Параметры краулера (max_concurrent, rate_limit, etc.)
- Стартовые URL
- Фильтры и исключения
- Настройки сохранения данных
- Настройки логирования
"""
import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Загрузчик конфигурации из YAML файла.
    
    Поддерживает:
    - Загрузку из файла
    - Валидацию параметров
    - Значения по умолчанию
    """
    
    DEFAULT_CONFIG = {
        "crawler": {
            "max_concurrent": 10,
            "max_depth": 3,
            "max_pages": 100,
            "same_domain_only": False,
            "verify_ssl": True,
            "per_domain_limit": 2,
            "requests_per_second": 1.0,
            "min_delay": 0.0,
            "jitter": 0.0,
            "respect_robots": True,
            "user_agent": "AsyncCrawler/1.0",
            "retry_max_retries": 3,
            "retry_backoff_factor": 2.0,
            "retry_base_delay": 0.5,
            "connect_timeout": 5.0,
            "read_timeout": 10.0,
            "total_timeout": 15.0,
            "circuit_breaker_threshold": 5,
            "circuit_breaker_cooldown": 30.0,
        },
        "urls": {
            "start_urls": [],
            "sitemap_urls": [],
            "use_sitemap": False,
        },
        "filters": {
            "exclude_patterns": [],
            "include_patterns": [],
        },
        "storage": {
            "type": "json",  # json, csv, sqlite, или null
            "json": {
                "filename": "results.json",
                "buffer_size": 10,
            },
            "csv": {
                "filename": "results.csv",
                "buffer_size": 10,
            },
            "sqlite": {
                "filename": "results.db",
                "batch_size": 50,
            },
        },
        "logging": {
            "level": "INFO",  # DEBUG, INFO, WARNING, ERROR
            "file": None,  # Путь к файлу логов (None = только консоль)
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
        "output": {
            "stats_json": "stats.json",
            "stats_html": "report.html",
        },
    }
    
    @staticmethod
    def load_from_file(path: str) -> Dict[str, Any]:
        """
        Загружает конфигурацию из YAML файла.
        
        Args:
            path: Путь к YAML файлу
        
        Returns:
            Словарь с конфигурацией (объединённой с значениями по умолчанию)
        
        Raises:
            FileNotFoundError: Если файл не найден
            yaml.YAMLError: Если файл содержит невалидный YAML
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                user_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML config: {e}")
        
        # Объединяем с конфигурацией по умолчанию
        config = ConfigLoader._merge_config(ConfigLoader.DEFAULT_CONFIG.copy(), user_config)
        
        logger.info(f"Config loaded from {path}")
        return config
    
    @staticmethod
    def _merge_config(default: Dict, user: Dict) -> Dict:
        """
        Рекурсивно объединяет пользовательскую конфигурацию с конфигурацией по умолчанию.
        
        Args:
            default: Конфигурация по умолчанию
            user: Пользовательская конфигурация
        
        Returns:
            Объединённая конфигурация
        """
        result = default.copy()
        
        for key, value in user.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Рекурсивно объединяем вложенные словари
                result[key] = ConfigLoader._merge_config(result[key], value)
            else:
                # Перезаписываем значение
                result[key] = value
        
        return result
    
    @staticmethod
    def create_example_config(filename: str = "config.example.yaml") -> None:
        """
        Создаёт пример конфигурационного файла.
        
        Args:
            filename: Имя файла для создания
        """
        example_config = {
            "crawler": {
                "max_concurrent": 5,
                "max_depth": 2,
                "max_pages": 50,
                "same_domain_only": True,
                "verify_ssl": False,
                "requests_per_second": 2.0,
                "respect_robots": True,
                "user_agent": "MyCrawler/1.0",
            },
            "urls": {
                "start_urls": [
                    "https://example.com",
                ],
                "use_sitemap": True,
            },
            "filters": {
                "exclude_patterns": [
                    r"\.pdf$",
                    r"/admin/",
                ],
            },
            "storage": {
                "type": "json",
                "json": {
                    "filename": "results.json",
                },
            },
            "logging": {
                "level": "INFO",
                "file": "crawler.log",
            },
            "output": {
                "stats_json": "stats.json",
                "stats_html": "report.html",
            },
        }
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                yaml.dump(example_config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
            logger.info(f"Example config created: {filename}")
        except Exception as e:
            logger.error(f"Error creating example config: {e}", exc_info=True)
