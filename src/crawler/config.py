import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


class ConfigLoader:
    
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
            "type": "json",                               
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
            "level": "INFO",                               
            "file": None,                                              
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        },
        "output": {
            "stats_json": "stats.json",
            "stats_html": "report.html",
        },
    }
    
    @staticmethod
    def load_from_file(path: str) -> Dict[str, Any]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                user_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML config: {e}")
        
                                                 
        config = ConfigLoader._merge_config(ConfigLoader.DEFAULT_CONFIG.copy(), user_config)
        
        logger.info(f"Config loaded from {path}")
        return config
    
    @staticmethod
    def _merge_config(default: Dict, user: Dict) -> Dict:
        result = default.copy()
        
        for key, value in user.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                                                         
                result[key] = ConfigLoader._merge_config(result[key], value)
            else:
                                         
                result[key] = value
        
        return result
    
    @staticmethod
    def create_example_config(filename: str = "config.example.yaml") -> None:
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
