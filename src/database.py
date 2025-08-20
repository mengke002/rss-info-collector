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
    
    def __init__(self, config):
        """初始化数据库连接"""
        print("Initializing DatabaseManager...")
        self.db_config = config.get_database_config()
        self.config = config
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
                        # 先检查表是否存在
                        cursor.execute(f"""
                            SELECT COUNT(*)
                            FROM information_schema.tables
                            WHERE table_schema = %s AND table_name = %s
                        """, (self.db_config['database'], table_name))
                        
                        if cursor.fetchone()[0] == 0:
                            cursor.execute(schema)
                            conn.commit()
                            logger.info(f"表 {table_name} 创建成功")
                        else:
                            logger.debug(f"表 {table_name} 已存在")
                    except Exception as e:
                        logger.error(f"初始化表 {table_name} 失败: {e}")
                        conn.rollback()
                        raise
    
    def drop_all_rss_tables(self):
        """删除所有RSS相关的表"""
        table_names = self.get_table_schemas().keys()
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for table_name in table_names:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.commit()
                        logger.info(f"表 {table_name} 删除成功")
                    except Exception as e:
                        logger.error(f"删除表 {table_name} 失败: {e}")

    def get_table_schemas(self) -> Dict[str, str]:
        """获取所有表的创建SQL"""
        return {
            'rss_betalist': """
                CREATE TABLE IF NOT EXISTS rss_betalist (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    visit_url VARCHAR(512),
                    guid VARCHAR(255) UNIQUE NOT NULL,
                    author VARCHAR(255),
                    summary TEXT,
                    image_url VARCHAR(512),
                    published_at DATETIME,
                    updated_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_ycombinator': """
                CREATE TABLE IF NOT EXISTS rss_ycombinator (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    guid VARCHAR(512) UNIQUE NOT NULL,
                    full_content MEDIUMTEXT,
                    content_fetched_at DATETIME,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link(255))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            'rss_techcrunch': """
                CREATE TABLE IF NOT EXISTS rss_techcrunch (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) UNIQUE NOT NULL,
                    full_content TEXT,
                    image_url VARCHAR(512),
                    guid VARCHAR(512) UNIQUE NOT NULL,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            'rss_techcrunch_ai': """
                CREATE TABLE IF NOT EXISTS rss_techcrunch_ai (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) UNIQUE NOT NULL,
                    full_content TEXT,
                    image_url VARCHAR(512),
                    guid VARCHAR(512) UNIQUE NOT NULL,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,

            'rss_theverge': """
                CREATE TABLE IF NOT EXISTS rss_theverge (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) UNIQUE NOT NULL,
                    author VARCHAR(255),
                    summary TEXT,
                    image_url VARCHAR(512),
                    guid VARCHAR(255) UNIQUE NOT NULL,
                    category VARCHAR(255),
                    published_at DATETIME,
                    updated_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            'rss_indiehackers': """
                CREATE TABLE IF NOT EXISTS rss_indiehackers (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(512) NOT NULL,
                    summary TEXT,
                    author VARCHAR(255),
                    category VARCHAR(100),
                    guid VARCHAR(512) UNIQUE NOT NULL,
                    image_url VARCHAR(512),
                    full_content TEXT,
                    content_fetched_at DATETIME,
                    published_at DATETIME,
                    feed_type VARCHAR(50) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME,
                    INDEX idx_published (published_at),
                    INDEX idx_link (link),
                    INDEX idx_content_fetched (content_fetched_at),
                    INDEX idx_feed_type (feed_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        }
    
    def insert_rss_item(self, table_name: str, item_data: Dict[str, Any]) -> bool:
        """插入RSS条目（单条插入，兼容旧代码）"""
        return self.insert_rss_items_batch(table_name, [item_data])
    
    def insert_rss_items_batch(self, table_name: str, items_data: List[Dict[str, Any]]) -> int:
        """批量插入RSS条目"""
        if not items_data:
            return 0
        
        # 构建插入SQL
        columns = ', '.join(items_data[0].keys())
        placeholders = ', '.join(['%s'] * len(items_data[0]))
        update_clause = ', '.join([f"{k} = VALUES({k})" for k in items_data[0].keys() if k != 'guid'])
        
        sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 批量插入
                    values_list = [list(item.values()) for item in items_data]
                    cursor.executemany(sql, values_list)
                    conn.commit()
                    inserted_count = cursor.rowcount
                    logger.info(f"批量插入 {table_name}: {inserted_count} 条记录")
                    return inserted_count
        except Exception as e:
            logger.error(f"批量插入数据失败: {e}")
            return 0
    
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
