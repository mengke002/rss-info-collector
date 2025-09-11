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
            return analysis_result

        # 2. 从分析结果中获取已生成的完整报告
        full_report_md = analysis_result.get('full_report')
        if not full_report_md:
            logger.error("分析结果中没有完整报告")
            return {
                'success': False,
                'error': '分析结果中没有完整报告'
            }

        # 3. 使用TechNewsReportGenerator保存报告到数据库
        from .report_generator import TechNewsReportGenerator
        
        generator = TechNewsReportGenerator()
        article_count = analysis_result.get('successful_analysis_count', 0)
        time_range_str = f"过去{hours_back}小时"
        
        report_result = generator.generate_report(full_report_md, article_count, time_range_str)
        
        if report_result.get('success'):
            logger.info(f"科技新闻报告生成成功: UUID {report_result.get('report_uuid')}")
            # 将完整报告内容添加到返回结果中
            report_result['full_report'] = full_report_md
        else:
            logger.error(f"报告生成失败: {report_result.get('error')}")

        return report_result

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
            # 自定义筛选：48小时内的indiehackers数据 + 最新1篇ezindie
            report_id = analyzer.generate_synthesis_report(
                days=days,  # 保留默认值作为备用
                indiehackers_hours=48,
                ezindie_limit=1
            )
        else:
            # 默认筛选：过去N天的数据
            report_id = analyzer.generate_synthesis_report(days=days)
        
        if report_id:
            filter_desc = "自定义筛选" if use_custom_filter else f"过去{days}天"
            result = {
                'success': True,
                'report_id': report_id,
                'message': f'成功生成综合洞察报告({filter_desc})，ID: {report_id}'
            }
        else:
            result = {
                'success': False,
                'message': '没有足够的已分析文章来生成报告'
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
