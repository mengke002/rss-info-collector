"""
配置管理模块
支持环境变量 > config.ini > 默认值的优先级机制
"""
import os
import configparser
from typing import Dict, Any
from dotenv import load_dotenv

class Config:
    """配置管理类，支持环境变量优先级的配置加载"""
    
    def __init__(self, config_path: str = 'config.ini'):
        """初始化配置"""
        # 在本地开发环境中，可以加载.env文件
        load_dotenv()
        
        # 读取config.ini文件
        self.config_parser = configparser.ConfigParser()
        self.config_file = config_path
        
        # 如果config.ini文件存在，则读取
        if os.path.exists(self.config_file):
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
            except (configparser.Error, UnicodeDecodeError):
                pass
    
    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """
        按优先级获取配置值：环境变量 > config.ini > 默认值
        
        Args:
            section: config.ini中的section名称
            key: config.ini中的key名称
            env_var: 环境变量名称
            default_value: 默认值
            value_type: 值类型转换函数
            
        Returns:
            配置值
        """
        # 1. 优先检查环境变量
        env_value = os.getenv(env_var)
        if env_value is not None:
            try:
                return value_type(env_value)
            except (ValueError, TypeError):
                return default_value
        
        # 2. 检查config.ini文件
        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                config_value = self.config_parser.get(section, key)
                try:
                    return value_type(config_value)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        # 3. 返回默认值
        return default_value
    
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置，优先级：环境变量 > config.ini > 默认值"""
        config = {
            'host': self._get_config_value('database', 'host', 'DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'DB_USER', None),
            'password': self._get_config_value('database', 'password', 'DB_PASSWORD', None),
            'database': self._get_config_value('database', 'database', 'DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'DB_PORT', 3306, int),
            'charset': 'utf8mb4'
        }
        
        # 检查SSL模式
        ssl_mode = self._get_config_value('database', 'ssl_mode', 'DB_SSL_MODE', 'disabled')
        if ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}
        
        # 验证必需的数据库配置
        required_fields = ['host', 'user', 'password', 'database']
        missing_fields = [field for field in required_fields if config[field] is None]
        if missing_fields:
            raise ValueError(f"数据库核心配置缺失: {', '.join(missing_fields)}。请在环境变量或config.ini中设置。")
        
        return config
    
    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置，优先级：环境变量 > config.ini > 默认值"""
        return {
            'delay_seconds': self._get_config_value('crawler', 'delay_seconds', 'CRAWLER_DELAY_SECONDS', 2.0, float),
            'max_retries': self._get_config_value('crawler', 'max_retries', 'CRAWLER_MAX_RETRIES', 3, int),
            'timeout_seconds': self._get_config_value('crawler', 'timeout_seconds', 'CRAWLER_TIMEOUT_SECONDS', 30, int),
            'max_concurrent_requests': self._get_config_value('crawler', 'max_concurrent_requests', 'CRAWLER_MAX_CONCURRENT_REQUESTS', 5, int)
        }
    
    def get_data_retention_days(self) -> int:
        """获取数据保留天数，优先级：环境变量 > config.ini > 默认值"""
        return self._get_config_value('data_retention', 'days', 'DATA_RETENTION_DAYS', 30, int)
    
    def get_logging_config(self) -> Dict[str, str]:
        """获取日志配置，优先级：环境变量 > config.ini > 默认值"""
        return {
            'log_level': self._get_config_value('logging', 'log_level', 'LOGGING_LOG_LEVEL', 'INFO'),
            'log_file': self._get_config_value('logging', 'log_file', 'LOGGING_LOG_FILE', 'rss_crawler.log')
        }
    
    def get_feed_configs(self) -> Dict[str, Dict[str, Any]]:
        """获取所有RSS源配置"""
        feeds = {}
        
        # 可用的RSS源配置
        working_feeds = {
            'betalist': {
                'rss_url': self._get_config_value('feeds', 'betalist_rss', 'BETALIST_RSS', 'https://betalist.com/feed'),
                'interval': self._get_config_value('feeds', 'betalist_interval', 'BETALIST_INTERVAL', 30, int)
            },
            'theverge': {
                'rss_url': self._get_config_value('feeds', 'theverge_rss', 'THEVERGE_RSS', 'https://www.theverge.com/rss/index.xml'),
                'interval': self._get_config_value('feeds', 'theverge_interval', 'THEVERGE_INTERVAL', 30, int)
            },
            'indiehackers_alltime': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_alltime_rss', 'INDIEHACKERS_ALLTIME_RSS', 'https://www.indiehackers.com/feed/alltime'),
                'interval': self._get_config_value('feeds', 'indiehackers_alltime_interval', 'INDIEHACKERS_ALLTIME_INTERVAL', 60, int)
            },
            'indiehackers_month': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_month_rss', 'INDIEHACKERS_MONTH_RSS', 'https://www.indiehackers.com/feed/month'),
                'interval': self._get_config_value('feeds', 'indiehackers_month_interval', 'INDIEHACKERS_MONTH_INTERVAL', 60, int)
            },
            'indiehackers_week': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_week_rss', 'INDIEHACKERS_WEEK_RSS', 'https://www.indiehackers.com/feed/week'),
                'interval': self._get_config_value('feeds', 'indiehackers_week_interval', 'INDIEHACKERS_WEEK_INTERVAL', 60, int)
            },
            'indiehackers_today': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_today_rss', 'INDIEHACKERS_TODAY_RSS', 'https://www.indiehackers.com/feed/today'),
                'interval': self._get_config_value('feeds', 'indiehackers_today_interval', 'INDIEHACKERS_TODAY_INTERVAL', 60, int)
            },
            'indiehackers_growth': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_growth_rss', 'INDIEHACKERS_GROWTH_RSS', 'https://www.indiehackers.com/feed/growth'),
                'interval': self._get_config_value('feeds', 'indiehackers_growth_interval', 'INDIEHACKERS_GROWTH_INTERVAL', 60, int)
            },
            'indiehackers_developers': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_developers_rss', 'INDIEHACKERS_DEVELOPERS_RSS', 'https://www.indiehackers.com/feed/developers'),
                'interval': self._get_config_value('feeds', 'indiehackers_developers_interval', 'INDIEHACKERS_DEVELOPERS_INTERVAL', 60, int)
            },
            'indiehackers_saas': {
                'rss_url': self._get_config_value('feeds', 'indiehackers_saas_rss', 'INDIEHACKERS_SAAS_RSS', 'https://www.indiehackers.com/feed/saas'),
                'interval': self._get_config_value('feeds', 'indiehackers_saas_interval', 'INDIEHACKERS_SAAS_INTERVAL', 60, int)
            },
            'techcrunch': {
                'rss_url': self._get_config_value('feeds', 'techcrunch_rss', 'TECHCRUNCH_RSS', 'https://techcrunch.com/feed/'),
                'interval': self._get_config_value('feeds', 'techcrunch_interval', 'TECHCRUNCH_INTERVAL', 30, int),
                'strategy': 'crawl4ai'
            },
            'techcrunch_ai': {
                'rss_url': self._get_config_value('feeds', 'techcrunch_ai_rss', 'TECHCRUNCH_AI_RSS', 'https://techcrunch.com/category/artificial-intelligence/feed/'),
                'interval': self._get_config_value('feeds', 'techcrunch_ai_interval', 'TECHCRUNCH_AI_INTERVAL', 30, int),
                'strategy': 'requests'
            },
            'ycombinator': {
                'rss_url': self._get_config_value('feeds', 'ycombinator_rss', 'YCOMBINATOR_RSS', 'https://rsshub.rssforever.com/hackernews'),
                'interval': self._get_config_value('feeds', 'ycombinator_interval', 'YCOMBINATOR_INTERVAL', 30, int),
                'strategy': 'crawl4ai'
            }
        }
        
        # 过滤掉URL为空的配置
        return {name: config for name, config in working_feeds.items() if config['rss_url']}

# 全局配置实例
config = Config()
