"""
数据库操作模块
"""
import pymysql
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

from .config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self):
        """初始化数据库连接"""
        self.db_config = config.get_database_config()
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()
    
    def init_database(self):
        """初始化数据库表"""
        table_schemas = self.get_table_schemas()
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for table_name, schema in table_schemas.items():
                    try:
                        cursor.execute(schema)
                        conn.commit()
                        logger.info(f"表 {table_name} 创建成功")
                    except pymysql.err.ProgrammingError as e:
                        if "already exists" in str(e):
                            logger.debug(f"表 {table_name} 已存在")
                        else:
                            raise e
    
    def get_table_schemas(self) -> Dict[str, str]:
        """获取所有表的创建SQL"""
        return {
            'rss_betalist': """
                CREATE TABLE IF NOT EXISTS rss_betalist (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    visit_url VARCHAR(512),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_theverge': """
                CREATE TABLE IF NOT EXISTS rss_theverge (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(2000),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_alltime': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_alltime (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_month': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_month (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_week': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_week (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_today': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_today (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_growth': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_growth (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_developers': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_developers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers_saas': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers_saas (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary VARCHAR(1000),
                    category VARCHAR(50),
                    guid VARCHAR(255) UNIQUE,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_created (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        }
    
    def insert_rss_item(self, table_name: str, item_data: Dict[str, Any]) -> bool:
        """插入RSS条目"""
        # 构建插入SQL
        columns = ', '.join(item_data.keys())
        placeholders = ', '.join(['%s'] * len(item_data))
        update_clause = ', '.join([f"{k} = VALUES({k})" for k in item_data.keys() if k != 'guid'])
        
        sql = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, list(item_data.values()))
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            return False
    
    def get_existing_guids(self, table_name: str) -> set:
        """获取已存在的GUID集合"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT guid FROM {table_name}")
                    return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"获取已存在GUID失败: {e}")
            return set()
    
    def cleanup_old_data(self, table_name: str, days: int = None) -> int:
        """清理旧数据"""
        if days is None:
            days = config.get_data_retention_days()
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    sql = f"DELETE FROM {table_name} WHERE created_at < %s"
                    cursor.execute(sql, (cutoff_date,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    return deleted_count
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
            return 0
    
    def get_stats(self, table_name: str) -> Dict[str, Any]:
        """获取统计信息"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 总记录数
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_count = cursor.fetchone()[0]
                    
                    # 今日新增
                    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE created_at >= %s", (today,))
                    today_count = cursor.fetchone()[0]
                    
                    # 最新记录时间
                    cursor.execute(f"SELECT MAX(created_at) FROM {table_name}")
                    latest_time = cursor.fetchone()[0]
                    
                    return {
                        'total_count': total_count,
                        'today_count': today_count,
                        'latest_time': latest_time
                    }
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {'total_count': 0, 'today_count': 0, 'latest_time': None}

# 全局数据库实例
db_manager = DatabaseManager()