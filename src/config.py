"""
配置管理模块
"""
import os
import configparser
from typing import Dict, Any

class Config:
    """配置管理类"""
    
    def __init__(self, config_path: str = 'config.ini'):
        """初始化配置"""
        self.config = configparser.ConfigParser()
        self.config.read(config_path, encoding='utf-8')
    
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        db_config = {
            'host': self.config.get('database', 'host'),
            'port': int(self.config.get('database', 'port')),
            'user': self.config.get('database', 'user'),
            'password': self.config.get('database', 'password'),
            'database': self.config.get('database', 'database'),
            'charset': 'utf8mb4'
        }

        if self.config.has_option('database', 'ssl_mode') and self.config.get('database', 'ssl_mode').upper() == 'REQUIRED':
            db_config['ssl'] = {'mode': 'REQUIRED'}

        return db_config
    
    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置"""
        return {
            'delay_seconds': float(self.config.get('crawler', 'delay_seconds')),
            'max_retries': int(self.config.get('crawler', 'max_retries')),
            'timeout_seconds': int(self.config.get('crawler', 'timeout_seconds')),
            'max_concurrent_requests': int(self.config.get('crawler', 'max_concurrent_requests'))
        }
    
    def get_data_retention_days(self) -> int:
        """获取数据保留天数"""
        return int(self.config.get('data_retention', 'days'))
    
    def get_logging_config(self) -> Dict[str, str]:
        """获取日志配置"""
        return {
            'log_level': self.config.get('logging', 'log_level'),
            'log_file': self.config.get('logging', 'log_file')
        }
    
    def get_feed_configs(self) -> Dict[str, Dict[str, Any]]:
        """获取所有RSS源配置"""
        feeds = {}
        
        # 可用的RSS源
        working_feeds = {
            'betalist': {
                'rss_url': self.config.get('feeds', 'betalist_rss'),
                'interval': int(self.config.get('feeds', 'betalist_interval'))
            },
            'theverge': {
                'rss_url': self.config.get('feeds', 'theverge_rss'),
                'interval': int(self.config.get('feeds', 'theverge_interval'))
            },
            'indiehackers_alltime': {
                'rss_url': self.config.get('feeds', 'indiehackers_alltime_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_alltime_interval'))
            },
            'indiehackers_month': {
                'rss_url': self.config.get('feeds', 'indiehackers_month_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_month_interval'))
            },
            'indiehackers_week': {
                'rss_url': self.config.get('feeds', 'indiehackers_week_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_week_interval'))
            },
            'indiehackers_today': {
                'rss_url': self.config.get('feeds', 'indiehackers_today_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_today_interval'))
            },
            'indiehackers_growth': {
                'rss_url': self.config.get('feeds', 'indiehackers_growth_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_growth_interval'))
            },
            'indiehackers_developers': {
                'rss_url': self.config.get('feeds', 'indiehackers_developers_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_developers_interval'))
            },
            'indiehackers_saas': {
                'rss_url': self.config.get('feeds', 'indiehackers_saas_rss'),
                'interval': int(self.config.get('feeds', 'indiehackers_saas_interval'))
            },
            'techcrunch': {
                'rss_url': self.config.get('feeds', 'techcrunch_rss'),
                'interval': int(self.config.get('feeds', 'techcrunch_interval')),
                'strategy': 'crawl4ai'
            },
            'techcrunch_ai': {
                'rss_url': self.config.get('feeds', 'techcrunch_ai_rss'),
                'interval': int(self.config.get('feeds', 'techcrunch_ai_interval')),
                'strategy': 'requests'
            },
            'ycombinator': {
                'rss_url': self.config.get('feeds', 'ycombinator_rss'),
                'interval': int(self.config.get('feeds', 'ycombinator_interval')),
                'strategy': 'crawl4ai' # 使用crawl4ai获取RSS，并修复XML结构
            }
        }
        
        return working_feeds

# 全局配置实例
config = Config()