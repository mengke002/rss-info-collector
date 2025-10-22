"""
核心任务模块
"""
import asyncio
from datetime import datetime, date
from typing import Dict, Any, List

from .config import config
from .rss_parser import rss_parser
from .content_enhancer import content_enhancer
from .logger import logger
from .database import DatabaseManager
from . import indiehackers_scraper


def _normalize_items_for_db(items: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    table_columns = {
        'rss_ycombinator': ['title', 'link', 'guid', 'full_content', 'content_fetched_at', 'published_at', 'updated_at'],
        'rss_indiehackers': ['title', 'link', 'summary', 'author', 'category', 'guid', 'image_url', 'full_content', 'content_fetched_at', 'published_at', 'feed_type', 'updated_at'],
        'rss_betalist': ['title', 'link', 'visit_url', 'guid', 'author', 'summary', 'image_url', 'published_at', 'updated_at'],
        'rss_theverge': ['title', 'link', 'author', 'summary', 'image_url', 'guid', 'category', 'published_at', 'updated_at'],
        'rss_techcrunch': ['title', 'link', 'full_content', 'image_url', 'guid', 'published_at'],
        'rss_ezindie': ['guid', 'title', 'link', 'author', 'summary', 'cover_image_url', 'full_content_markdown', 'published_at'],
        'rss_decohack_products': ['product_name', 'tagline', 'description', 'product_url', 'ph_url', 'image_url', 'vote_count', 'is_featured', 'keywords', 'ph_publish_date', 'crawl_date'],
        'rss_weibo': ['user_id', 'guid', 'title', 'link', 'author', 'description', 'category', 'published_at', 'updated_at'],
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
            'full_content': 65000,
            'image_url': 512,
            'guid': 512
        },
        'rss_ycombinator': {
            'title': 255,
            'link': 512,
            'guid': 512
        },
       'rss_ezindie': {
           'guid': 255,
           'title': 255,
           'link': 255,
           'author': 100,
           'summary': 512,
           'cover_image_url': 512,
       },
       'rss_decohack_products': {
           'product_name': 100,
           'tagline': 200,
           'description': 800,
           'product_url': 400,
           'ph_url': 400,
           'image_url': 400,
           'keywords': 300,
       },
       'rss_weibo': {
           'user_id': 50,
           'guid': 512,
           'title': 512,
           'link': 512,
           'author': 255,
           'description': 65000,
           'category': 512,
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

    # 特殊处理：如果是 weibo，直接调用专门的任务函数
    if feed_to_crawl and feed_to_crawl.lower() == 'weibo':
        return run_weibo_crawl_task(db_manager)

    results = {
        'success': True,
        'feeds_processed': 0,
        'items_inserted': 0,
        'errors': []
    }

    feed_configs = config.get_feed_configs()
    feeds = feed_configs
    if feed_to_crawl:
        # 确保即使传递了 'ezindie_rss' 也能匹配到 'ezindie'
        normalized_feed_to_crawl = feed_to_crawl.replace('_rss', '')
        if normalized_feed_to_crawl not in feeds:
            results['success'] = False
            results['errors'].append(f"Feed '{feed_to_crawl}' not found in configuration.")
            return results
        feeds = {normalized_feed_to_crawl: feeds[normalized_feed_to_crawl]}

    for feed_name, feed_config in feeds.items():
        try:
            logger.info(f"处理RSS源: {feed_name}")

            items = []  # Initialize items list

            # 确定表名和feed类型, 并获取items
            if 'indiehackers' in feed_name:
                table_name = "rss_indiehackers"
                feed_type = feed_name.replace('indiehackers_', '')
                
                logger.info(f"Attempting to fetch Indie Hackers feed '{feed_name}' via RSS.")
                items = rss_parser.parse_feed(feed_config)

                # rss_parser returns [] on error. If items is empty, trigger the fallback scraper.
                if not items:
                    logger.warning(f"RSS feed for '{feed_name}' returned no items or failed to parse. Falling back to web scraper.")
                    try:
                        # The scraper's period for 'alltime' is 'all-time'
                        scrape_period = 'all-time' if feed_type == 'alltime' else feed_type
                        # The scraper's group name for 'saas' is 'saas-marketing'
                        scrape_group = 'saas-marketing' if feed_type == 'saas' else feed_type

                        product_types = ['alltime', 'month', 'week', 'today']
                        group_types = ['growth', 'developers', 'saas']

                        # 使用nest_asyncio来处理嵌套事件循环
                        try:
                            import nest_asyncio
                            nest_asyncio.apply()
                        except ImportError:
                            pass  # 如果没有nest_asyncio，继续尝试
                        
                        try:
                            if feed_type in product_types:
                                items = asyncio.run(indiehackers_scraper.scrape_products(scrape_period))
                            elif feed_type in group_types:
                                items = asyncio.run(indiehackers_scraper.scrape_group(scrape_group))
                        except RuntimeError as e:
                            if "cannot be called from a running event loop" in str(e):
                                # 如果在事件循环中运行，使用同步的方式调用
                                logger.warning("在事件循环中运行，跳过爬虫回滚")
                                items = []
                            else:
                                raise
                        
                        if items:
                            logger.info(f"Successfully scraped {len(items)} items for '{feed_name}'.")
                            # Normalize scraped data to match DB schema
                            for item in items:
                                item['guid'] = item.get('link') # Use link as GUID for scraped items
                        else:
                            logger.error(f"Scraper for '{feed_name}' returned no items.")

                    except Exception as scraper_e:
                        logger.error(f"Scraper for '{feed_name}' also failed: {scraper_e}", exc_info=True)
                        items = [] # Ensure items is an empty list if scraper fails too
            
            elif 'techcrunch' in feed_name:
                table_name = "rss_techcrunch"
                feed_type = 'techcrunch'
                items = rss_parser.parse_feed(feed_config)
            elif 'ezindie' in feed_name:
                table_name = "rss_ezindie"
                feed_type = 'ezindie'
                items = rss_parser.parse_feed(feed_config)
            elif 'decohack' in feed_name:
                table_name = "rss_decohack_products"
                feed_type = 'decohack'
                items = rss_parser.parse_feed(feed_config)
            else:
                # 默认情况下，表名为 rss_{feed_name}
                table_name = f"rss_{feed_name}"
                feed_type = feed_name
                items = rss_parser.parse_feed(feed_config)
            
            # 对于decohack，特殊处理
            if 'decohack' in feed_name:
                all_products = []
                for item in items:
                    if item.get('is_decohack_source') and item.get('full_content_html'):
                        products = rss_parser.parse_decohack_products(
                            item['full_content_html'], 
                            date.today()  # crawl_date 仍然传递，但不再用于去重
                        )
                        if products:
                            all_products.extend(products)
                
                logger.info(f"Decohack解析到 {len(all_products)} 个产品，准备入库...")
                
                # 规范化并直接批量插入，由数据库处理去重
                if all_products:
                    final_products = _normalize_items_for_db(all_products, table_name)
                    inserted_count = db_manager.batch_insert_decohack_products(final_products)
                    results['items_inserted'] += inserted_count
                
                results['feeds_processed'] += 1
                continue # 处理完decohack后跳过后续通用逻辑

            # --- 以下为其他RSS源的通用处理逻辑 ---

            # 获取已存在的数据用于去重
            existing_guids = db_manager.get_existing_guids(table_name)

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
            elif feed_name in ('techcrunch', 'techcrunch_ai') and new_items:
                logger.info(f"开始为 {feed_name} 的 {len(new_items)} 个新条目增强内容...")
                enhanced_items = asyncio.run(content_enhancer.enhance_items(new_items, 'techcrunch'))
            elif 'ezindie' in feed_name and new_items:
               logger.info(f"开始为 ezindie 的 {len(new_items)} 个新条目增强内容...")
               enhanced_items = asyncio.run(content_enhancer.enhance_items(new_items, 'ezindie'))
            else:
                for item in new_items:
                    if 'full_content' not in item:
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
        'ycombinator': 'rss_ycombinator',
        'ezindie': 'rss_ezindie',
        'decohack': 'rss_decohack_products'
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
        'ycombinator': 'rss_ycombinator',
        'ezindie': 'rss_ezindie',
        'decohack': 'rss_decohack_products'
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

def run_product_discovery_analysis(db_manager: DatabaseManager, batch_size: int = 50) -> Dict[str, Any]:
    """
    执行产品发现分析任务
    
    Args:
        db_manager: 数据库管理器
        batch_size: 每批处理的数量
        
    Returns:
        分析结果
    """
    # 根据记忆，每个任务方法都必须包含db_manager.init_database()调用
    db_manager.init_database()
    
    logger.info("开始执行产品发现分析任务")
    
    try:
        from .analyzer import DataAnalyzer
        
        # 创建分析器实例
        analyzer = DataAnalyzer(db_manager)
        
        # 运行产品发现分析
        result = analyzer.run_product_discovery_analysis(batch_size=batch_size)
        
        if result.get('success', False):
            logger.info(f"产品发现分析完成 - 总处理: {result['total_processed']}, 总提取: {result['total_extracted']}")
        else:
            logger.error(f"产品发现分析失败: {', '.join(result.get('errors', []))}")
        
        return result
        
    except Exception as e:
        error_msg = f"执行产品发现分析失败: {e}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg,
            'total_processed': 0,
            'total_extracted': 0
        }

def run_report_generation_task(db_manager: DatabaseManager, period: str = 'daily', include_analysis: bool = True) -> Dict[str, Any]:
    """
    执行报告生成任务
    
    Args:
        db_manager: 数据库管理器
        period: 报告周期 ('daily', 'weekly', 'monthly')
        include_analysis: 是否包含深度分析
        
    Returns:
        报告生成结果
    """
    # 移除重复的数据库初始化调用
    # db_manager.init_database()
    
    logger.info(f"开始执行{period}报告生成任务")
    
    try:
        from .report_generator import ProductDiscoveryReportGenerator
        
        # 创建报告生成器实例，传入现有的db_manager以避免重复初始化
        generator = ProductDiscoveryReportGenerator(db_manager)
        
        # 生成报告 - 使用正确的方法名
        if period == 'weekly':
            result = generator.generate_report(days=7)
        elif period == 'daily':
            result = generator.generate_report(days=1)
        elif period == 'monthly':
            result = generator.generate_report(days=30)
        else:
            raise ValueError(f"不支持的报告周期: {period}")
        
        if result:
            logger.info(f"{period}报告生成成功: 报告UUID: {result}")
            return {
                'success': True,
                'report_uuid': result,
                'report_path': f"数据库中的报告UUID: {result}",
                'products_count': 0,  # 如果需要，可以从数据库查询
                'analysis_count': 0   # 如果需要，可以从数据库查询
            }
        else:
            logger.warning(f"{period}报告生成完成，但没有生成新报告")
            return {
                'success': True,
                'report_uuid': None,
                'report_path': None,
                'products_count': 0,
                'analysis_count': 0
            }
        
    except Exception as e:
        error_msg = f"执行{period}报告生成失败: {e}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg,
            'report_path': None
        }

def run_tech_news_analysis_task(db_manager: DatabaseManager, hours_back: int = 24) -> Dict[str, Any]:
    """
    执行科技新闻分析任务
    
    Args:
        db_manager: 数据库管理器
        hours_back: 分析过去多少小时的新闻，默认24小时
        
    Returns:
        分析结果
    """
    # 初始化数据库
    db_manager.init_database()
    
    logger.info(f"开始执行科技新闻分析任务 - 分析过去{hours_back}小时的新闻")
    
    try:
        from .analyzer import TechNewsAnalyzer
        
        # 创建科技新闻分析器实例
        analyzer = TechNewsAnalyzer(db_manager)
        
        # 运行分析
        result = analyzer.run_tech_news_analysis(hours_back)

        # 移除完整报告内容以减少日志大小
        result.pop('model_reports_full', None)

        if result.get('success', False):
            logger.info(f"科技新闻分析完成 - 找到 {result.get('total_articles_found', 0)} 篇文章，成功分析 {result.get('successful_analysis_count', 0)} 篇")
        else:
            logger.error(f"科技新闻分析失败: {result.get('message', '未知错误')}")
        
        return result
        
    except Exception as e:
        error_msg = f"执行科技新闻分析失败: {e}"
        logger.error(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'analysis_results': []
        }

def run_community_analysis_task(db_manager: DatabaseManager, days_back: int = 7) -> Dict[str, Any]:
    """
    执行社区讨论分析任务
    
    Args:
        db_manager: 数据库管理器
        days_back: 分析过去多少天的社区内容，默认7天
        
    Returns:
        分析结果
    """
    # 初始化数据库
    db_manager.init_database()
    
    logger.info(f"开始执行社区讨论分析任务 - 分析过去{days_back}天的内容")
    
    try:
        # TODO: 在后续实现社区分析器后更新此代码
        # from .analyzer import CommunityAnalyzer
        # analyzer = CommunityAnalyzer(db_manager)
        # result = analyzer.run_community_analysis(days_back)
        
        # 目前返回模拟结果
        result = {
            'success': True,
            'message': '社区分析功能正在开发中',
            'analysis_period': f'过去{days_back}天',
            'analysis_results': []
        }
        
        logger.info(f"社区讨论分析完成 - {result.get('message', '')}")
        return result
        
    except Exception as e:
        error_msg = f"执行社区讨论分析失败: {e}"
        logger.error(error_msg)
        return {
            'success': False,
            'message': error_msg,
            'analysis_results': []
        }

def run_tech_news_report_generation_task(db_manager: DatabaseManager, hours_back: int = 24) -> Dict[str, Any]:
    """
    执行科技新闻分析与报告生成集成任务（基于新的两层架构）

    Args:
        db_manager: 数据库管理器
        hours_back: 分析时段

    Returns:
        任务执行结果
    """
    logger.info(f"开始执行科技新闻分析与报告生成集成任务 - {hours_back}小时")

    try:
        # 1. 使用TechNewsAnalyzer的新架构进行分析
        from .analyzer import TechNewsAnalyzer
        
        analyzer = TechNewsAnalyzer(db_manager)
        analysis_result = analyzer.run_tech_news_analysis(hours_back)

        if not analysis_result.get('success'):
            logger.error("分析步骤失败，报告生成中止。")
            sanitized_result = dict(analysis_result)
            if 'model_reports' in sanitized_result:
                sanitized_result['model_reports'] = TechNewsAnalyzer._sanitize_model_reports(
                    sanitized_result.get('model_reports_full') or sanitized_result.get('model_reports', [])
                )
            sanitized_result.pop('model_reports_full', None)
            sanitized_result['full_report'] = None
            return sanitized_result

        model_reports = analysis_result.get('model_reports_full') or analysis_result.get('model_reports', [])
        analysis_failures = analysis_result.get('failures', [])

        if not model_reports:
            logger.error("LLM分析未生成任何有效报告")
            failure_payload = {
                'success': False,
                'error': 'LLM分析未生成任何有效报告',
                'analysis_failures': analysis_failures,
                'model_reports': TechNewsAnalyzer._sanitize_model_reports(model_reports),
                'full_report': None
            }
            failure_payload.pop('model_reports_full', None)
            return failure_payload

        # 3. 收集已经立即保存的报告结果
        persisted_reports: List[Dict[str, Any]] = []
        generation_failures: List[Dict[str, Any]] = []

        for report_meta in model_reports:
            model_name = report_meta.get('model')
            display_name = report_meta.get('model_display')

            # 检查是否已经立即保存成功
            if 'db_save_result' in report_meta:
                db_result = report_meta['db_save_result']
                db_result['model'] = model_name
                db_result['model_display'] = display_name
                db_result['provider'] = report_meta.get('provider')
                persisted_reports.append(db_result)
                logger.info(
                    "科技新闻报告已立即保存 - 模型 %s, UUID %s",
                    display_name,
                    db_result.get('report_uuid')
                )
            elif 'db_save_error' in report_meta:
                error_msg = report_meta['db_save_error']
                logger.error(
                    "科技新闻报告立即保存失败 - 模型 %s: %s",
                    display_name,
                    error_msg
                )
                generation_failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                })
            else:
                # 如果没有立即保存信息，说明可能是旧版本或出现了问题
                logger.warning(f"模型 {display_name} 的报告没有立即保存信息")
                generation_failures.append({
                    'model': model_name,
                    'model_display': display_name,
                    'error': '报告未被立即保存'
                })

        overall_success = len(persisted_reports) > 0
        primary_report_uuid = persisted_reports[0]['report_uuid'] if overall_success else None

        sanitized_model_reports = TechNewsAnalyzer._sanitize_model_reports(model_reports)

        result_payload = {
            'success': overall_success,
            'reports': persisted_reports,
            'analysis_failures': analysis_failures,
            'generation_failures': generation_failures,
            'full_report': None,  # 不在日志中显示完整报告
            'primary_report_uuid': primary_report_uuid,
            'model_reports': sanitized_model_reports
        }

        return result_payload

    except Exception as e:
        error_msg = f"报告生成任务中发生未知错误: {e}"
        logger.error(error_msg)
        return {'success': False, 'error': error_msg}

def run_community_deep_analysis_task(batch_size: int = 10):
    """
    运行社区深度内容分析任务
    
    Args:
        batch_size: 单次处理的文章数量
        
    Returns:
        任务执行结果
    """
    logger.info(f"开始执行社区深度内容分析任务，批次大小: {batch_size}")
    
    try:
        # 初始化组件
        db_manager = DatabaseManager(config)
        from .analyzer import CommunityDeepAnalyzer
        analyzer = CommunityDeepAnalyzer(db_manager)
        
        # 执行批量深度分析
        success_count = analyzer.process_deep_analysis_batch(limit=batch_size)
        
        result = {
            'success': True,
            'processed_articles': success_count,
            'message': f'成功分析 {success_count} 篇文章'
        }
        
        logger.info(f"社区深度内容分析任务完成: {result['message']}" )
        return result
        
    except Exception as e:
        error_msg = f"社区深度内容分析任务失败: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg,
            'processed_articles': 0
        }

def run_community_synthesis_report_task(days: int = 7, use_custom_filter: bool = False):
    """
    运行社区综合洞察报告生成任务
    
    Args:
        days: 分析过去多少天的数据（默认筛选条件）
        use_custom_filter: 是否使用自定义筛选条件（48小时indiehackers + 最新1篇ezindie）
        
    Returns:
        任务执行结果
    """
    if use_custom_filter:
        logger.info(f"开始执行社区综合洞察报告生成任务，使用自定义筛选：48小时内indiehackers + 最新1篇ezindie")
    else:
        logger.info(f"开始执行社区综合洞察报告生成任务，分析过去 {days} 天的数据")
    
    try:
        # 初始化组件
        db_manager = DatabaseManager(config)
        from .analyzer import CommunityDeepAnalyzer
        analyzer = CommunityDeepAnalyzer(db_manager)
        
        # 根据筛选条件生成报告
        if use_custom_filter:
            report_result = analyzer.generate_synthesis_report(
                days=days,
                indiehackers_hours=48,
                ezindie_limit=1
            )
        else:
            report_result = analyzer.generate_synthesis_report(days=days)

        reports = report_result.get('reports', [])
        failures = report_result.get('failures', [])
        report_ids = [item.get('report_id') for item in reports if item.get('report_id')]

        if report_result.get('success'):
            filter_desc = "自定义筛选" if use_custom_filter else f"过去{days}天"
            result = {
                'success': True,
                'report_ids': report_ids,
                'reports': reports,
                'failures': failures,
                'message': f"成功生成 {len(reports)} 份综合洞察报告({filter_desc})"
            }
        else:
            result = {
                'success': False,
                'reports': reports,
                'failures': failures,
                'message': report_result.get('message', '生成综合洞察报告失败')
            }

        logger.info(f"社区综合洞察报告生成任务完成: {result['message']}" )
        return result
        
    except Exception as e:
        error_msg = f"社区综合洞察报告生成任务失败: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }

def run_community_analysis_and_report_task(analysis_batch_size: int = 10, report_days: int = 7, 
                                          use_custom_filter: bool = False):
    """
    运行完整的社区深度分析与报告生成任务
    
    Args:
        analysis_batch_size: 深度分析的批次大小
        report_days: 报告覆盖的天数（默认筛选条件）
        use_custom_filter: 是否使用自定义筛选条件（48小时indiehackers + 最新1篇ezindie）
        
    Returns:
        任务执行结果
    """
    filter_desc = "自定义筛选条件" if use_custom_filter else f"过去{report_days}天"
    logger.info(f"开始执行完整的社区深度分析与报告生成任务，报告使用{filter_desc}")
    
    try:
        # 步骤1：执行深度分析
        analysis_result = run_community_deep_analysis_task(batch_size=analysis_batch_size)
        
        # 步骤2：生成综合报告
        report_result = run_community_synthesis_report_task(
            days=report_days, 
            use_custom_filter=use_custom_filter
        )
        
        # 合并结果
        result = {
            'success': analysis_result['success'] and report_result['success'],
            'analysis_result': analysis_result,
            'report_result': report_result,
            'message': f"分析处理了 {analysis_result.get('processed_articles', 0)} 篇文章，报告生成: {report_result['message']}"
        }
        
        logger.info(f"完整的社区深度分析与报告生成任务完成: {result['message']}" )
        return result
        
    except Exception as e:
        error_msg = f"完整的社区深度分析与报告生成任务失败: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }


def run_product_catalog_export_task(start_date=None, end_date=None) -> Dict[str, Any]:
    """
    导出所有产品清单任务

    功能：
    1. 从数据库获取所有产品（discovered_products + rss_decohack_products）
    2. 基于产品名称去重，只保留时间最近的记录
    3. 按时间由近及远排序
    4. 生成完整的产品清单 Markdown 报告
    5. 推送到 Notion

    Args:
        start_date: 开始日期（可选）
        end_date: 结束日期（可选）

    Returns:
        执行结果字典
    """
    try:
        logger.info("=" * 60)
        logger.info("开始执行产品清单导出任务...")
        if start_date or end_date:
            if start_date and end_date:
                logger.info(f"时间范围: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
            elif start_date:
                logger.info(f"时间范围: {start_date.strftime('%Y-%m-%d')} 至今")
            else:
                logger.info(f"时间范围: 截至 {end_date.strftime('%Y-%m-%d')}")
        else:
            logger.info("时间范围: 全部产品")
        logger.info("=" * 60)

        from .product_catalog_generator import ProductCatalogGenerator

        # 创建产品清单生成器
        catalog_generator = ProductCatalogGenerator()

        # 生成并推送产品清单
        result = catalog_generator.generate_and_push_catalog(start_date, end_date)

        if result.get('success'):
            logger.info("✅ 产品清单导出任务完成！")
            logger.info(f"   - 产品总数: {result.get('product_count', 0)}")
            logger.info(f"   - 报告长度: {result.get('markdown_length', 0)} 字符")

            notion_push = result.get('notion_push', {})
            if notion_push.get('success'):
                notion_url = result.get('notion_url', '')
                if notion_push.get('skipped'):
                    logger.info(f"   - Notion: 已存在，跳过推送")
                else:
                    logger.info(f"   - Notion: 推送成功")
                if notion_url:
                    logger.info(f"   - Notion URL: {notion_url}")
            else:
                logger.warning(f"   - Notion: 推送失败 - {notion_push.get('error', '未知错误')}")
        else:
            logger.error(f"❌ 产品清单导出任务失败: {result.get('error', result.get('message', '未知错误'))}")

        logger.info("=" * 60)
        return result

    except Exception as e:
        error_msg = f"产品清单导出任务失败: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg
        }

def run_weibo_crawl_task(db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    执行微博RSS爬取任务

    Args:
        db_manager: 数据库管理器

    Returns:
        执行结果字典
    """
    logger.info("开始执行微博RSS爬取任务")

    results = {
        'success': True,
        'users_processed': 0,
        'items_inserted': 0,
        'errors': []
    }

    try:
        # 获取配置
        user_ids = config.get_weibo_user_ids()
        prefixes = config.get_rsshub_prefixes()

        if not user_ids:
            error_msg = "未配置微博用户ID列表"
            logger.error(error_msg)
            results['success'] = False
            results['errors'].append(error_msg)
            return results

        if not prefixes:
            error_msg = "未配置RSSHub前缀列表"
            logger.error(error_msg)
            results['success'] = False
            results['errors'].append(error_msg)
            return results

        logger.info(f"配置检查完成 - 用户数: {len(user_ids)}, 前缀数: {len(prefixes)}")

        # 获取已存在的GUID集合用于去重
        existing_guids = db_manager.get_existing_guids('rss_weibo')
        logger.info(f"数据库中已存在 {len(existing_guids)} 条微博记录")

        # 对每个用户ID进行爬取
        all_new_items = []
        for user_id in user_ids:
            try:
                logger.info(f"开始爬取微博用户: {user_id}")

                # 使用rss_parser的fetch_weibo_rss方法
                items = rss_parser.fetch_weibo_rss(user_id, prefixes, max_retries=5)

                if items:
                    # 过滤新条目
                    new_items = [item for item in items if item['guid'] not in existing_guids]

                    if new_items:
                        logger.info(f"微博用户 {user_id}: 获取到 {len(items)} 条，其中 {len(new_items)} 条为新微博")
                        all_new_items.extend(new_items)
                    else:
                        logger.info(f"微博用户 {user_id}: 获取到 {len(items)} 条，但都已存在")

                    results['users_processed'] += 1
                else:
                    logger.warning(f"微博用户 {user_id}: 未获取到任何微博")

            except Exception as e:
                error_msg = f"爬取微博用户 {user_id} 失败: {e}"
                logger.error(error_msg, exc_info=True)
                results['errors'].append(error_msg)

        # 批量插入新条目
        if all_new_items:
            # 规范化数据
            normalized_items = _normalize_items_for_db(all_new_items, 'rss_weibo')

            # 批量插入
            inserted_count = db_manager.insert_rss_items_batch('rss_weibo', normalized_items)
            results['items_inserted'] = inserted_count
            logger.info(f"成功插入 {inserted_count} 条新微博记录")
        else:
            logger.info("没有新的微博记录需要插入")

        results['success'] = len(results['errors']) == 0
        logger.info(f"微博RSS爬取任务完成 - 处理用户: {results['users_processed']}, 新增记录: {results['items_inserted']}")

        return results

    except Exception as e:
        error_msg = f"执行微博RSS爬取任务失败: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'success': False,
            'error': error_msg,
            'users_processed': 0,
            'items_inserted': 0,
            'errors': [error_msg]
        }
