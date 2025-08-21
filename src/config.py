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
        """
        获取所有RSS源配置, 遵循 环境变量 > config.ini > 默认值 的优先级.
        同时支持从config.ini动态发现未在代码中定义的源.
        """
        # 1. 定义默认/已知的源及其默认值
        default_feeds = {
            'betalist': {'rss_url': 'https://feeds.feedburner.com/BetaList', 'interval': 1800},
            'theverge': {'rss_url': 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml', 'interval': 1800},
            'indiehackers_alltime': {'rss_url': 'https://ihrss.io/top/all-time', 'interval': 2592000},
            'indiehackers_month': {'rss_url': 'https://ihrss.io/top/month', 'interval': 604800},
            'indiehackers_week': {'rss_url': 'https://ihrss.io/top/week', 'interval': 86400},
            'indiehackers_today': {'rss_url': 'https://ihrss.io/top/today', 'interval': 1800},
            'indiehackers_growth': {'rss_url': 'https://ihrss.io/group/growth', 'interval': 1800},
            'indiehackers_developers': {'rss_url': 'https://ihrss.io/group/developers', 'interval': 1800},
            'indiehackers_saas': {'rss_url': 'https://ihrss.io/group/saas-marketing', 'interval': 1800},
            'ycombinator': {'rss_url': 'https://rsshub.rssforever.com/hackernews', 'interval': 1800},
            'techcrunch': {'rss_url': 'https://rsshub.rssforever.com/techcrunch/news', 'interval': 1800},
            'techcrunch_ai': {'rss_url': 'https://techcrunch.com/category/artificial-intelligence/feed/', 'interval': 1800},
        }

        # 2. 动态发现 `config.ini` 中的所有源
        all_feed_names = set(default_feeds.keys())
        if self.config_parser.has_section('feeds'):
            discovered_names = {key[:-4] for key in self.config_parser.options('feeds') if key.endswith('_rss')}
            all_feed_names.update(discovered_names)

        # 3. 为每个源构建最终配置, 应用优先级逻辑
        final_feeds = {}
        for name in all_feed_names:
            defaults = default_feeds.get(name, {})
            default_url = defaults.get('rss_url')
            default_interval = defaults.get('interval', 1800)

            # 构造用于查找的键
            config_key_rss = f"{name}_rss"
            env_var_rss = config_key_rss.upper()
            config_key_interval = f"{name}_interval"
            env_var_interval = config_key_interval.upper()

            # 使用辅助函数按优先级获取值
            rss_url = self._get_config_value('feeds', config_key_rss, env_var_rss, default_url)
            interval = self._get_config_value('feeds', config_key_interval, env_var_interval, default_interval, int)
            
            if rss_url:
                final_feeds[name] = {
                    'rss_url': rss_url,
                    'interval': interval
                }
                # 分配特定策略
                if 'techcrunch' in name and name != 'techcrunch_ai':
                    final_feeds[name]['strategy'] = 'crawl4ai'
                elif 'ycombinator' in name:
                    final_feeds[name]['strategy'] = 'crawl4ai'
                else:
                    final_feeds[name]['strategy'] = 'requests'

        return final_feeds

# 全局配置实例
config = Config()
