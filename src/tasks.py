"""
核心任务模块
"""
import asyncio
from datetime import datetime
from typing import Dict, Any, List

from .config import config
from .rss_parser import rss_parser
from .content_enhancer import content_enhancer
from .logger import logger
from .database import DatabaseManager


def _normalize_items_for_db(items: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    table_columns = {
        'rss_ycombinator': ['title', 'link', 'guid', 'full_content', 'content_fetched_at', 'published_at', 'updated_at'],
        'rss_indiehackers': ['title', 'link', 'summary', 'author', 'category', 'guid', 'image_url', 'full_content', 'content_fetched_at', 'published_at', 'feed_type', 'updated_at'],
        'rss_betalist': ['title', 'link', 'visit_url', 'guid', 'author', 'summary', 'image_url', 'published_at', 'updated_at'],
        'rss_theverge': ['title', 'link', 'author', 'summary', 'image_url', 'guid', 'category', 'published_at', 'updated_at'],
        'rss_techcrunch': ['title', 'link', 'summary', 'image_url', 'guid', 'published_at', 'updated_at'],
    }
    if table_name not in table_columns:
        return items
    def _tb(s: Any, n: int) -> Any:
        if s is None:
            return None
        if not isinstance(s, str):
            s = str(s)
        b = s.encode('utf-8')
        if len(b) <= n:
            return s
        return b[:n].decode('utf-8', 'ignore')
    constraints = {
        'rss_indiehackers': {
            'title': 255,
            'link': 512,
            'summary': 65000,
            'author': 255,
            'category': 100,
            'guid': 512,
            'image_url': 512,
            'full_content': 65000,
            'feed_type': 50
        },
        'rss_betalist': {
            'title': 255,
            'link': 512,
            'visit_url': 512,
            'guid': 255,
            'author': 255,
            'summary': 65000,
            'image_url': 512
        },
        'rss_theverge': {
            'title': 255,
            'link': 512,
            'author': 255,
            'summary': 65000,
            'image_url': 512,
            'guid': 255,
            'category': 255
        },
        'rss_techcrunch': {
            'title': 255,
            'link': 512,
            'summary': 65000,
            'image_url': 512,
            'guid': 512
        },
        'rss_ycombinator': {
            'title': 255,
            'link': 512,
            'guid': 512
        }
    }
    normalized_items = []
    expected_keys = set(table_columns[table_name])
    for item in items:
        normalized_item = {key: item.get(key) for key in expected_keys}
        if 'updated_at' in expected_keys and 'updated_at' not in item:
            normalized_item['updated_at'] = datetime.now()
        if table_name in constraints:
            for k, lim in constraints[table_name].items():
                if k in normalized_item and isinstance(normalized_item[k], (str, int, float)):
                    normalized_item[k] = _tb(normalized_item[k], lim)
        normalized_items.append(normalized_item)
    return normalized_items

def run_crawl_task(db_manager: DatabaseManager, feed_to_crawl: str = None) -> Dict[str, Any]:
    """执行爬取任务"""
    logger.info("开始执行RSS爬取任务")

    results = {
        'success': True,
        'feeds_processed': 0,
        'items_inserted': 0,
        'errors': []
    }

    feed_configs = config.get_feed_configs()
    feeds = feed_configs
    if feed_to_crawl:
        if feed_to_crawl not in feeds:
            results['success'] = False
            results['errors'].append(f"Feed '{feed_to_crawl}' not found in configuration.")
            return results
        feeds = {feed_to_crawl: feeds[feed_to_crawl]}

    for feed_name, feed_config in feeds.items():
        try:
            logger.info(f"处理RSS源: {feed_name}")

            # 确定表名和feed类型
            if 'indiehackers' in feed_name:
                table_name = "rss_indiehackers"
                # 从feed_name中提取feed类型，例如 indiehackers_alltime -> alltime
                feed_type = feed_name.replace('indiehackers_', '')
            else:
                table_name = f"rss_{feed_name}"
                feed_type = None

            # 获取已存在的GUID
            existing_guids = db_manager.get_existing_guids(table_name)

            # 解析RSS
            items = rss_parser.parse_feed(feed_config)

            # 过滤新条目并添加feed_type
            new_items = []
            for item in items:
                if item['guid'] not in existing_guids:
                    if feed_type:
                        item['feed_type'] = feed_type
                    new_items.append(item)

            if feed_name == 'ycombinator' and new_items:
                logger.info(f"开始为 ycombinator 的 {len(new_items)} 个新条目增强内容...")
                enhanced_items = asyncio.run(content_enhancer.enhance_items(new_items, 'ycombinator'))
            elif 'indiehackers' in feed_name and new_items:
                logger.info(f"开始为 indiehackers 的 {len(new_items)} 个新条目增强内容...")
                enhanced_items = asyncio.run(content_enhancer.enhance_items(new_items, 'indiehackers'))
            else:
                for item in new_items:
                    item['full_content'] = item.get('summary', '')
                    item['content_fetched_at'] = datetime.now()
                enhanced_items = new_items

            # 批量插入新条目
            inserted_count = 0
            if enhanced_items:
                # 特殊处理
                for item in enhanced_items:
                    if feed_name == 'betalist':
                        item['visit_url'] = rss_parser.extract_visit_url(item['guid'], 'betalist')

                # 在插入前规范化数据
                final_items = _normalize_items_for_db(enhanced_items, table_name)
                inserted_count = db_manager.insert_rss_items_batch(table_name, final_items)

            logger.info(f"{feed_name}: 新增 {inserted_count} 条记录")

            results['feeds_processed'] += 1
            results['items_inserted'] += inserted_count

        except Exception as e:
            error_msg = f"处理 {feed_name} 失败: {str(e)}"
            logger.error(error_msg)
            results['errors'].append(error_msg)

    results['success'] = len(results['errors']) == 0
    return results


def run_cleanup_task(db_manager: DatabaseManager, days: int = None) -> Dict[str, Any]:
    """执行清理任务"""
    if days is None:
        days = config.get_data_retention_days()

    logger.info(f"开始清理超过 {days} 天的旧数据")

    results = {
        'success': True,
        'deleted_counts': {},
        'total_deleted': 0
    }

    # 定义需要清理的表
    tables_to_cleanup = {
        'betalist': 'rss_betalist',
        'theverge': 'rss_theverge',
        'techcrunch': 'rss_techcrunch',
        'indiehackers': 'rss_indiehackers',
        'ycombinator': 'rss_ycombinator'
    }

    for feed_key, table_name in tables_to_cleanup.items():
        try:
            deleted_count = db_manager.cleanup_old_data(table_name, days)
            results['deleted_counts'][feed_key] = deleted_count
            results['total_deleted'] += deleted_count
            logger.info(f"{table_name}: 删除 {deleted_count} 条旧记录")
        except Exception as e:
            logger.error(f"清理 {table_name} 失败: {e}")
            results['success'] = False
            results['error'] = str(e)

    return results


def run_stats_task(db_manager: DatabaseManager) -> Dict[str, Any]:
    """执行统计任务"""
    logger.info("开始生成统计信息")

    results = {
        'success': True,
        'stats': {}
    }

    # 定义需要统计的表
    tables_to_stats = {
        'betalist': 'rss_betalist',
        'theverge': 'rss_theverge',
        'techcrunch': 'rss_techcrunch',
        'indiehackers': 'rss_indiehackers',
        'ycombinator': 'rss_ycombinator'
    }

    for feed_key, table_name in tables_to_stats.items():
        try:
            stats = db_manager.get_stats(table_name)
            results['stats'][feed_key] = stats
            logger.info(f"{feed_key}: {stats}")
        except Exception as e:
            logger.error(f"获取 {feed_key} 统计信息失败: {e}")
            results['success'] = False
            results['error'] = str(e)

    # 为indiehackers添加按feed_type的统计
    try:
        indiehackers_stats = _get_indiehackers_stats_by_type(db_manager)
        results['stats']['indiehackers_by_type'] = indiehackers_stats
    except Exception as e:
        logger.error(f"获取indiehackers分类统计失败: {e}")

    return results


def _get_indiehackers_stats_by_type(db_manager: DatabaseManager) -> Dict[str, Any]:
    """获取indiehackers按feed_type的统计"""
    try:
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT feed_type, COUNT(*) as count
                    FROM rss_indiehackers
                    GROUP BY feed_type
                """)
                results = cursor.fetchall()
                return {row[0]: row[1] for row in results}
    except Exception as e:
        logger.error(f"获取indiehackers分类统计失败: {e}")
        return {}