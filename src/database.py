"""
数据库操作模块
"""
import pymysql
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import json

from .config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self, config):
        """初始化数据库连接"""
        print("Initializing DatabaseManager...")
        self.db_config = config.get_database_config()
        self.config = config
        
        # 根据配置决定是否跳过数据库表检查
        if self.db_config.get('skip_table_check', False):
            logger.info("已根据配置跳过数据库表结构检查。")
        else:
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
                # 先创建新表
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
                
                # 然后更新表结构
                try:
                    self._update_table_schemas(cursor, conn)
                except Exception as e:
                    logger.error(f"更新表结构失败: {e}")
                    conn.rollback()
                    raise
            
            # 创建报告相关表
            self._create_tables_if_not_exists()

    def _create_tables_if_not_exists(self):
        """创建所有必要的数据库表（如果它们不存在）。"""
        # 现有表的创建逻辑
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
                        logger.error(f"创建表 {table_name} 失败: {e}")
                        conn.rollback()
                        raise

        # 新增：创建报告存储表
        self._create_product_reports_table()
        self._create_technews_reports_table()
        self._create_insights_reports_table()
        self._create_synthesis_reports_table()

        logger.info("所有数据库表检查/创建完毕。")

    def _create_product_reports_table(self):
        """创建产品发现报告表"""
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS product_reports (
                report_id INT AUTO_INCREMENT PRIMARY KEY,
                report_uuid VARCHAR(36) NOT NULL UNIQUE,
                generated_at DATETIME NOT NULL,
                report_date DATE NOT NULL,
                time_range VARCHAR(50),
                product_count INT,
                source_feed_count INT,
                report_content_md LONGTEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # 检查并更新现有表的字段类型
        self._update_report_content_field('product_reports')
        logger.debug("`product_reports` 表已检查/创建。")

    def _create_technews_reports_table(self):
        """创建科技新闻报告表"""
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS technews_reports (
                report_id INT AUTO_INCREMENT PRIMARY KEY,
                report_uuid VARCHAR(36) NOT NULL UNIQUE,
                generated_at DATETIME NOT NULL,
                report_date DATE NOT NULL,
                time_range VARCHAR(50),
                article_count INT,
                main_topics JSON,
                report_content_md TEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        logger.debug("`technews_reports` 表已检查/创建。")

    def _create_insights_reports_table(self):
        """创建深度洞察报告表"""
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS insights_reports (
                report_id INT AUTO_INCREMENT PRIMARY KEY,
                report_uuid VARCHAR(36) NOT NULL UNIQUE,
                generated_at DATETIME NOT NULL,
                report_date DATE NOT NULL,
                report_title VARCHAR(255),
                related_report_uuids JSON,
                report_content_md TEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        logger.debug("`insights_reports` 表已检查/创建。")

    def _create_synthesis_reports_table(self):
        """创建综合洞察报告表"""
        self.execute_query("""
            CREATE TABLE IF NOT EXISTS synthesis_reports (
                id INT AUTO_INCREMENT PRIMARY KEY,
                report_type VARCHAR(255) NOT NULL,
                start_date DATE,
                end_date DATE,
                content TEXT,
                source_article_ids JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        logger.debug("`synthesis_reports` 表已检查/创建。")

    def _update_report_content_field(self, table_name: str, field_name: str = 'report_content_md'):
        """更新报告表的内容字段类型为LONGTEXT（仅适用于product_reports表）"""
        # 只对product_reports表进行字段类型更新
        if table_name != 'product_reports':
            return
            
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 检查字段当前类型
                    cursor.execute(f"""
                        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                    """, (self.db_config['database'], table_name, field_name))
                    
                    result = cursor.fetchone()
                    if result:
                        data_type, max_length = result
                        # 如果不是LONGTEXT，则更新
                        if data_type.upper() != 'LONGTEXT':
                            cursor.execute(f"""
                                ALTER TABLE {table_name} 
                                MODIFY COLUMN {field_name} LONGTEXT
                            """)
                            conn.commit()
                            logger.info(f"已将 {table_name}.{field_name} 字段类型更新为 LONGTEXT")
                        else:
                            logger.debug(f"{table_name}.{field_name} 字段已经是 LONGTEXT 类型")
        except Exception as e:
            logger.error(f"更新 {table_name}.{field_name} 字段类型失败: {e}")

    def _update_table_schemas(self, cursor, conn):
        """更新表结构，添加新字段"""
        # 为所有RSS表添加processing_status字段（如果不存在）
        rss_tables = [table_name for table_name in self.get_table_schemas().keys() 
                      if table_name.startswith('rss_')]
        
        for table_name in rss_tables:
            try:
                # 检查processing_status字段是否存在
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = 'processing_status'
                """, (self.db_config['database'], table_name))
                
                if cursor.fetchone()[0] == 0:
                    # 字段不存在，添加它
                    cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN processing_status VARCHAR(20) NOT NULL DEFAULT 'pending'
                    """)
                    conn.commit()
                    logger.info(f"为表 {table_name} 添加 processing_status 字段成功")
                else:
                    logger.debug(f"表 {table_name} 已存在 processing_status 字段")
                    
                # 检查analysis_result字段是否存在（用于存储分析结果）
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s AND column_name = 'analysis_result'
                """, (self.db_config['database'], table_name))
                
                if cursor.fetchone()[0] == 0:
                    # 字段不存在，添加它
                    cursor.execute(f"""
                        ALTER TABLE {table_name} 
                        ADD COLUMN analysis_result JSON COMMENT '存储文章分析结果'
                    """)
                    conn.commit()
                    logger.info(f"为表 {table_name} 添加 analysis_result 字段成功")
                else:
                    logger.debug(f"表 {table_name} 已存在 analysis_result 字段")
                    
                # 为 indiehackers 和 ezindie 表添加深度分析字段
                if table_name in ['rss_indiehackers', 'rss_ezindie']:
                    self._add_deep_analysis_fields(cursor, conn, table_name)
                    
            except Exception as e:
                logger.error(f"为表 {table_name} 添加字段失败: {e}")
                raise

    def _add_deep_analysis_fields(self, cursor, conn, table_name):
        """为指定表添加深度分析相关字段"""
        try:
            # 检查 deep_analysis_data 字段是否存在
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = 'deep_analysis_data'
            """, (self.db_config['database'], table_name))
            
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN deep_analysis_data TEXT COMMENT '深度分析结果数据'
                """)
                conn.commit()
                logger.info(f"为表 {table_name} 添加 deep_analysis_data 字段成功")
            else:
                logger.debug(f"表 {table_name} 已存在 deep_analysis_data 字段")
            
            # 检查 deep_analysis_status 字段是否存在
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s AND column_name = 'deep_analysis_status'
            """, (self.db_config['database'], table_name))
            
            if cursor.fetchone()[0] == 0:
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN deep_analysis_status SMALLINT DEFAULT 0 COMMENT '0=待处理, 1=处理成功, -1=处理失败'
                """)
                conn.commit()
                logger.info(f"为表 {table_name} 添加 deep_analysis_status 字段成功")
            else:
                logger.debug(f"表 {table_name} 已存在 deep_analysis_status 字段")
                
        except Exception as e:
            logger.error(f"为表 {table_name} 添加深度分析字段失败: {e}")
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
            """,
            
            'rss_ezindie': """
                CREATE TABLE IF NOT EXISTS rss_ezindie (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    guid VARCHAR(255) UNIQUE NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    link VARCHAR(255) NOT NULL,
                    author VARCHAR(100),
                    summary VARCHAR(512),
                    cover_image_url VARCHAR(512),
                    full_content_markdown TEXT,
                    published_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_published (published_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,

            'rss_decohack_products': """
                CREATE TABLE IF NOT EXISTS rss_decohack_products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(100) NOT NULL COMMENT '产品名称',
                    tagline VARCHAR(200) NOT NULL COMMENT '产品标语', 
                    description VARCHAR(800) NOT NULL COMMENT '产品介绍',
                    product_url VARCHAR(400) COMMENT '产品官网链接',
                    ph_url VARCHAR(400) COMMENT 'Product Hunt页面链接', 
                    image_url VARCHAR(400) COMMENT '产品图片URL',
                    vote_count SMALLINT UNSIGNED DEFAULT 0 COMMENT '投票数',
                    is_featured BOOLEAN DEFAULT FALSE COMMENT '是否精选',
                    keywords VARCHAR(300) COMMENT '产品关键词',
                    ph_publish_date DATE COMMENT 'PH发布日期',
                    crawl_date DATE NOT NULL COMMENT '抓取日期',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_product_ph_date (product_name, ph_publish_date) COMMENT '产品名称+PH发布日期唯一，实现精准去重',
                    INDEX idx_ph_publish (ph_publish_date),
                    INDEX idx_featured (is_featured),
                    INDEX idx_votes (vote_count DESC)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
                COMMENT='Decohack产品热榜数据表 - 细粒度存储每个产品信息'
            """,

            'discovered_products': """
                CREATE TABLE IF NOT EXISTS discovered_products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    product_name VARCHAR(255) NOT NULL,
                    tagline VARCHAR(512),
                    description VARCHAR(2048),
                    product_url VARCHAR(512),
                    image_url VARCHAR(512),
                    categories VARCHAR(1024),
                    metrics JSON,
                    source_feed VARCHAR(100) NOT NULL,
                    source_published_at DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_source_feed (source_feed),
                    INDEX idx_created_at (created_at),
                    INDEX idx_source_published_at (source_published_at),
                    INDEX idx_product_name (product_name),
                    INDEX idx_product_url (product_url),
                    UNIQUE KEY unique_product_source_date (product_name, source_feed, DATE(source_published_at))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='统一产品库表 - 事件日志型表，记录每一次产品发现事件，同一产品在同一来源同一天只记录一次'
            """,

            'articles': """
                CREATE TABLE IF NOT EXISTS articles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    feed_id INT DEFAULT NULL,
                    title VARCHAR(512) NOT NULL,
                    url VARCHAR(1024) NOT NULL,
                    content TEXT,
                    summary TEXT,
                    published_at DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_feed_id (feed_id),
                    INDEX idx_url (url(191)),
                    INDEX idx_published_at (published_at),
                    INDEX idx_created_at (created_at),
                    UNIQUE KEY unique_url (url(191))
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='文章表 - 存储从各种来源获取的文章信息'
            """,

            'analysis_results': """
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    rss_id INT NOT NULL,
                    analysis_date DATE NOT NULL,
                    result JSON NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
                    FOREIGN KEY (rss_id) REFERENCES rss_betalist(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='文章分析结果表 - 存储对每篇文章的分析结果'
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

    def batch_insert_decohack_products(self, products_data: List[Dict[str, Any]]) -> int:
        """批量插入Decohack产品，使用INSERT IGNORE去重"""
        if not products_data:
            return 0
        
        # 确保所有字典的键顺序一致
        columns = list(products_data[0].keys())
        columns_sql = ', '.join(f"`{c}`" for c in columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        sql = f"""
            INSERT IGNORE INTO rss_decohack_products ({columns_sql})
            VALUES ({placeholders})
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 将字典列表转换为元组列表
                    values_list = [tuple(item.get(col) for col in columns) for item in products_data]
                    
                    cursor.executemany(sql, values_list)
                    conn.commit()
                    inserted_count = cursor.rowcount
                    logger.info(f"批量插入 Decohack 产品: {inserted_count} 条新记录被插入")
                    return inserted_count
        except Exception as e:
            logger.error(f"批量插入 Decohack 产品数据失败: {e}")
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
    
    def execute_query(self, query: str, params: tuple = ()):
        """执行单个SQL查询"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    conn.commit()
                    # 保存最后插入的ID
                    self._last_insert_id = cursor.lastrowid
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            raise
    
    def get_last_insert_id(self):
        """获取最后插入记录的ID"""
        return getattr(self, '_last_insert_id', None)
    
    def close(self):
        """关闭数据库连接（占位方法，因为使用上下文管理器）"""
        # 由于使用上下文管理器，不需要显式关闭连接
        pass
    
    def get_discovered_products(self, days: int = 7, deduplicate: bool = True) -> List[Dict[str, Any]]:
        """
        获取指定天数内发现的产品
        
        Args:
            days: 过去多少天内的数据
            deduplicate: 是否进行智能去重
            
        Returns:
            产品列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    if deduplicate:
                        # 智能去重：同一产品名称只保留最新的记录
                        query = """
                            SELECT dp1.* FROM discovered_products dp1
                            INNER JOIN (
                                SELECT product_name, MAX(created_at) as max_created_at
                                FROM discovered_products 
                                WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                                GROUP BY LOWER(TRIM(product_name))
                            ) dp2 ON dp1.product_name = dp2.product_name 
                                AND dp1.created_at = dp2.max_created_at
                            ORDER BY dp1.created_at DESC
                        """
                    else:
                        # 不去重，返回所有记录
                        query = """
                            SELECT * FROM discovered_products 
                            WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            ORDER BY created_at DESC
                        """
                    
                    cursor.execute(query, (days,))
                    results = cursor.fetchall()
                    logger.info(f"获取到 {len(results)} 个产品 (过去 {days} 天，去重: {deduplicate})")
                    return results
        except Exception as e:
            logger.error(f"获取产品数据失败: {e}")
            return []

    def get_discovered_products_with_advanced_dedup(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取指定天数内发现的产品，使用高级去重策略
        
        去重策略：
        1. 同一产品名称（忽略大小写和空格）只保留一个
        2. 优先保留有URL的记录
        3. 在同等条件下保留最新的记录
        
        Args:
            days: 过去多少天内的数据
            
        Returns:
            去重后的产品列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    query = """
                        SELECT dp1.* FROM discovered_products dp1
                        INNER JOIN (
                            SELECT 
                                LOWER(TRIM(product_name)) as normalized_name,
                                MAX(
                                    CASE 
                                        WHEN product_url IS NOT NULL AND product_url != '' THEN created_at + INTERVAL 1 DAY
                                        ELSE created_at 
                                    END
                                ) as priority_created_at
                            FROM discovered_products 
                            WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                            GROUP BY LOWER(TRIM(product_name))
                        ) dp2 ON LOWER(TRIM(dp1.product_name)) = dp2.normalized_name 
                            AND (
                                (dp1.product_url IS NOT NULL AND dp1.product_url != '' AND dp1.created_at + INTERVAL 1 DAY = dp2.priority_created_at)
                                OR (dp1.created_at = dp2.priority_created_at)
                            )
                        ORDER BY dp1.created_at DESC
                    """
                    
                    cursor.execute(query, (days,))
                    results = cursor.fetchall()
                    logger.info(f"高级去重后获取到 {len(results)} 个唯一产品 (过去 {days} 天)")
                    return results
        except Exception as e:
            logger.error(f"高级去重获取产品数据失败: {e}")
            # 如果高级去重失败，回退到简单去重
            return self.get_discovered_products(days, deduplicate=True)

    def cleanup_duplicate_products(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        清理重复的产品记录
        
        Args:
            dry_run: 是否为试运行（只统计不删除）
            
        Returns:
            清理结果统计
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    # 查找重复的产品（基于产品名称，忽略大小写和前后空格）
                    cursor.execute("""
                        SELECT 
                            LOWER(TRIM(product_name)) as normalized_name,
                            COUNT(*) as count,
                            GROUP_CONCAT(id ORDER BY created_at DESC) as ids
                        FROM discovered_products 
                        GROUP BY LOWER(TRIM(product_name))
                        HAVING COUNT(*) > 1
                        ORDER BY count DESC
                    """)
                    
                    duplicates = cursor.fetchall()
                    total_duplicates = sum(dup['count'] - 1 for dup in duplicates)
                    
                    if dry_run:
                        logger.info(f"发现 {len(duplicates)} 组重复产品，共 {total_duplicates} 条重复记录")
                        return {
                            'success': True,
                            'dry_run': True,
                            'duplicate_groups': len(duplicates),
                            'total_duplicates': total_duplicates,
                            'duplicates': duplicates[:10]  # 返回前10组作为示例
                        }
                    
                    # 实际删除重复记录（保留每组中最新的记录）
                    deleted_count = 0
                    for dup in duplicates:
                        ids = dup['ids'].split(',')
                        # 保留第一个ID（最新的），删除其他的
                        ids_to_delete = ids[1:]
                        if ids_to_delete:
                            placeholders = ','.join(['%s'] * len(ids_to_delete))
                            cursor.execute(f"""
                                DELETE FROM discovered_products 
                                WHERE id IN ({placeholders})
                            """, ids_to_delete)
                            deleted_count += cursor.rowcount
                    
                    conn.commit()
                    logger.info(f"清理完成，删除了 {deleted_count} 条重复记录")
                    
                    return {
                        'success': True,
                        'dry_run': False,
                        'duplicate_groups': len(duplicates),
                        'deleted_count': deleted_count
                    }
                    
        except Exception as e:
            logger.error(f"清理重复产品失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_articles_for_analysis(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取指定天数内的文章用于分析
        
        Args:
            days: 过去多少天内的数据
            
        Returns:
            文章列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    query = """
                        SELECT * FROM articles 
                        WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        ORDER BY created_at DESC
                    """
                    cursor.execute(query, (days,))
                    results = cursor.fetchall()
                    logger.info(f"获取到 {len(results)} 篇文章用于分析 (过去 {days} 天)")
                    return results
        except Exception as e:
            logger.error(f"获取文章数据失败: {e}")
            return []
    
    def save_product_report(self, report_data: Dict[str, Any]):
        """
        将产品发现报告保存到数据库。
        :param report_data: 包含报告所有信息的字典。
        """
        query = """
            INSERT INTO product_reports (
                report_uuid, generated_at, report_date, time_range, product_count,
                source_feed_count, report_content_md, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        params = (
            report_data['report_uuid'],
            report_data['generated_at'],
            report_data['report_date'],
            report_data.get('time_range'),
            report_data.get('product_count'),
            report_data.get('source_feed_count'),
            report_data.get('report_content_md'),
            json.dumps(report_data.get('metadata')) if report_data.get('metadata') else None
        )
        self.execute_query(query, params)
        logger.info(f"产品发现报告 {report_data['report_uuid']} 已成功存入数据库。")

    def save_technews_report(self, report_data: Dict[str, Any]):
        """
        将科技新闻报告保存到数据库。
        :param report_data: 包含报告所有信息的字典。
        """
        query = """
            INSERT INTO technews_reports (
                report_uuid, generated_at, report_date, time_range, article_count,
                main_topics, report_content_md, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """
        params = (
            report_data['report_uuid'],
            report_data['generated_at'],
            report_data['report_date'],
            report_data.get('time_range'),
            report_data.get('article_count'),
            json.dumps(report_data.get('main_topics')) if report_data.get('main_topics') else None,
            report_data.get('report_content_md'),
            json.dumps(report_data.get('metadata')) if report_data.get('metadata') else None
        )
        self.execute_query(query, params)
        logger.info(f"科技新闻报告 {report_data['report_uuid']} 已成功存入数据库。")

    def save_insights_report(self, report_data: Dict[str, Any]):
        """
        将深度洞察报告保存到数据库。
        :param report_data: 包含报告所有信息的字典。
        """
        query = """
            INSERT INTO insights_reports (
                report_uuid, generated_at, report_date, report_title,
                related_report_uuids, report_content_md, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        params = (
            report_data['report_uuid'],
            report_data['generated_at'],
            report_data['report_date'],
            report_data.get('report_title'),
            json.dumps(report_data.get('related_report_uuids')) if report_data.get('related_report_uuids') else None,
            report_data.get('report_content_md'),
            json.dumps(report_data.get('metadata')) if report_data.get('metadata') else None
        )
        self.execute_query(query, params)
        logger.info(f"深度洞察报告 {report_data['report_uuid']} 已成功存入数据库。")

    # 深度分析相关的数据库操作方法
    
    def get_articles_for_deep_analysis(self, table_names: List[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        获取需要进行深度分析的文章
        
        Args:
            table_names: 要查询的表名列表，默认为 ['rss_indiehackers', 'rss_ezindie']
            limit: 限制返回的记录数
            
        Returns:
            待分析的文章列表
        """
        if table_names is None:
            table_names = ['rss_indiehackers', 'rss_ezindie']
        
        all_articles = []
        
        for table_name in table_names:
            try:
                with self.get_connection() as conn:
                    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                        # 根据表名选择合适的内容字段
                        if table_name == 'rss_ezindie':
                            content_field = 'full_content_markdown'
                        else:
                            content_field = 'full_content'
                        
                        # 查询待分析的文章（deep_analysis_status = 0 或 NULL）
                        query = f"""
                            SELECT *, '{table_name}' as source_table, {content_field} as full_content
                            FROM {table_name} 
                            WHERE (deep_analysis_status = 0 OR deep_analysis_status IS NULL)
                            AND {content_field} IS NOT NULL 
                            AND LENGTH({content_field}) > 100
                            ORDER BY published_at DESC 
                            LIMIT %s
                        """
                        cursor.execute(query, (limit,))
                        articles = cursor.fetchall()
                        all_articles.extend(articles)
                        logger.info(f"从 {table_name} 获取到 {len(articles)} 篇待分析文章")
            except Exception as e:
                logger.error(f"从 {table_name} 获取文章失败: {e}")
        
        return all_articles
    
    def update_deep_analysis_result(self, table_name: str, article_id: int, analysis_data: str, status: int = 1):
        """
        更新文章的深度分析结果
        
        Args:
            table_name: 表名
            article_id: 文章ID
            analysis_data: 分析结果数据（JSON字符串）
            status: 分析状态（1=成功, -1=失败）
        """
        try:
            query = f"""
                UPDATE {table_name} 
                SET deep_analysis_data = %s, deep_analysis_status = %s 
                WHERE id = %s
            """
            self.execute_query(query, (analysis_data, status, article_id))
            logger.info(f"更新 {table_name} 文章 {article_id} 的深度分析结果成功")
        except Exception as e:
            logger.error(f"更新 {table_name} 文章 {article_id} 的深度分析结果失败: {e}")
            raise
    
    def get_analyzed_articles_for_synthesis(self, table_names: List[str] = None, days: int = 7, 
                                          indiehackers_hours: int = None, ezindie_limit: int = None) -> List[Dict[str, Any]]:
        """
        获取已分析的文章用于综合洞察
        
        Args:
            table_names: 要查询的表名列表，默认为 ['rss_indiehackers', 'rss_ezindie']
            days: 过去多少天的数据（默认筛选条件）
            indiehackers_hours: indiehackers 数据的小时限制（优先级高于days）
            ezindie_limit: ezindie 数据的文章数量限制
            
        Returns:
            已分析的文章列表
        """
        if table_names is None:
            table_names = ['rss_indiehackers', 'rss_ezindie']
        
        all_articles = []
        
        for table_name in table_names:
            try:
                with self.get_connection() as conn:
                    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                        
                        # 根据表名设置不同的查询条件
                        if table_name == 'rss_indiehackers' and indiehackers_hours is not None:
                            # indiehackers 使用小时限制 (created_at, updated_at, or published_at)
                            query = f"""
                                SELECT id, title, link, deep_analysis_data, published_at, created_at, updated_at, '{table_name}' as source_table
                                FROM {table_name} 
                                WHERE deep_analysis_status = 1 
                                AND deep_analysis_data IS NOT NULL
                                AND (
                                    created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR) OR
                                    updated_at >= DATE_SUB(NOW(), INTERVAL %s HOUR) OR
                                    published_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                                )
                                ORDER BY published_at DESC
                            """
                            cursor.execute(query, (indiehackers_hours, indiehackers_hours, indiehackers_hours))
                            
                        elif table_name == 'rss_ezindie' and ezindie_limit is not None:
                            # ezindie 使用数量限制
                            query = f"""
                                SELECT id, title, link, deep_analysis_data, published_at, '{table_name}' as source_table
                                FROM {table_name} 
                                WHERE deep_analysis_status = 1 
                                AND deep_analysis_data IS NOT NULL
                                ORDER BY published_at DESC
                                LIMIT %s
                            """
                            cursor.execute(query, (ezindie_limit,))
                            
                        else:
                            # 默认使用天数限制
                            query = f"""
                                SELECT id, title, link, deep_analysis_data, published_at, '{table_name}' as source_table
                                FROM {table_name} 
                                WHERE deep_analysis_status = 1 
                                AND deep_analysis_data IS NOT NULL
                                AND published_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                                ORDER BY published_at DESC
                            """
                            cursor.execute(query, (days,))
                        
                        articles = cursor.fetchall()
                        all_articles.extend(articles)
                        
                        # 根据查询条件记录不同的日志
                        if table_name == 'rss_indiehackers' and indiehackers_hours is not None:
                            logger.info(f"从 {table_name} 获取到 {len(articles)} 篇已分析文章（过去{indiehackers_hours}小时）")
                        elif table_name == 'rss_ezindie' and ezindie_limit is not None:
                            logger.info(f"从 {table_name} 获取到 {len(articles)} 篇已分析文章（最新{ezindie_limit}篇）")
                        else:
                            logger.info(f"从 {table_name} 获取到 {len(articles)} 篇已分析文章（过去{days}天）")
                            
            except Exception as e:
                logger.error(f"从 {table_name} 获取已分析文章失败: {e}")
        
        return all_articles
    
    def save_synthesis_report(self, report_data: Dict[str, Any]):
        """
        保存综合洞察报告
        
        Args:
            report_data: 报告数据字典
        """
        try:
            query = """
                INSERT INTO synthesis_reports (
                    report_type, start_date, end_date, content, source_article_ids
                ) VALUES (%s, %s, %s, %s, %s)
            """
            params = (
                report_data.get('report_type', 'community_insights'),
                report_data.get('start_date'),
                report_data.get('end_date'),
                report_data.get('content'),
                json.dumps(report_data.get('source_article_ids', []))
            )
            self.execute_query(query, params)
            report_id = self.get_last_insert_id()
            logger.info(f"综合洞察报告 {report_id} 已成功存入数据库")
            return report_id
        except Exception as e:
            logger.error(f"保存综合洞察报告失败: {e}")
            raise

    def get_synthesis_reports(self, limit: int = 10, report_type: str = None) -> List[Dict[str, Any]]:
        """
        获取综合洞察报告列表
        
        Args:
            limit: 限制返回数量
            report_type: 报告类型筛选
            
        Returns:
            报告列表
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    if report_type:
                        query = """
                            SELECT id, report_type, start_date, end_date, content, 
                                   source_article_ids, created_at
                            FROM synthesis_reports 
                            WHERE report_type = %s
                            ORDER BY created_at DESC 
                            LIMIT %s
                        """
                        cursor.execute(query, (report_type, limit))
                    else:
                        query = """
                            SELECT id, report_type, start_date, end_date, content, 
                                   source_article_ids, created_at
                            FROM synthesis_reports 
                            ORDER BY created_at DESC 
                            LIMIT %s
                        """
                        cursor.execute(query, (limit,))
                    
                    reports = cursor.fetchall()
                    logger.info(f"获取到 {len(reports)} 个综合洞察报告")
                    return reports
        except Exception as e:
            logger.error(f"获取综合洞察报告失败: {e}")
            return []
