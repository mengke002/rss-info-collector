"""
分析模块
负责RSS数据的智能分析与信息提取
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import concurrent.futures
from contextlib import contextmanager
import pymysql

from .config import config
from .database import DatabaseManager
from .llm_client import call_llm

logger = logging.getLogger(__name__)


class DataAnalyzer:
    """数据分析器，负责RSS数据的智能处理"""

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化分析器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.max_workers = config.get_max_workers()
        logger.info(f"数据分析器初始化完成 - 最大并发数: {self.max_workers}")

    def select_and_lock_pending_items(self, source_table: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        选择并锁定待处理的数据行
        
        Args:
            source_table: 源数据表名
            limit: 限制获取的条目数量
            
        Returns:
            待处理的数据条目列表
        """
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    # 开始事务
                    conn.begin()
                    
                    # 选择pending状态的条目
                    select_sql = f"""
                        SELECT * FROM {source_table} 
                        WHERE processing_status = 'pending' 
                        ORDER BY created_at ASC 
                        LIMIT %s 
                        FOR UPDATE
                    """
                    cursor.execute(select_sql, (limit,))
                    items = cursor.fetchall()
                    
                    if not items:
                        conn.rollback()
                        logger.info(f"表 {source_table} 没有待处理的条目")
                        return []
                    
                    # 将选中的条目状态更新为processing
                    item_ids = [item['id'] for item in items]
                    update_sql = f"""
                        UPDATE {source_table} 
                        SET processing_status = 'processing' 
                        WHERE id IN ({','.join(['%s'] * len(item_ids))})
                    """
                    cursor.execute(update_sql, item_ids)
                    
                    # 提交事务
                    conn.commit()
                    
                    logger.info(f"成功锁定 {len(items)} 个待处理条目 (表: {source_table})")
                    return items
                    
        except Exception as e:
            logger.error(f"选择并锁定数据失败 (表: {source_table}): {e}")
            return []

    def update_processing_status(self, source_table: str, item_ids: List[int], 
                                status: str, batch_size: int = 50) -> bool:
        """
        批量更新条目处理状态
        
        Args:
            source_table: 源数据表名
            item_ids: 条目ID列表
            status: 新状态 ('success' 或 'failed')
            batch_size: 批处理大小
            
        Returns:
            操作是否成功
        """
        if not item_ids:
            return True
            
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 分批更新以避免单个查询过大
                    for i in range(0, len(item_ids), batch_size):
                        batch_ids = item_ids[i:i + batch_size]
                        update_sql = f"""
                            UPDATE {source_table} 
                            SET processing_status = %s 
                            WHERE id IN ({','.join(['%s'] * len(batch_ids))})
                        """
                        cursor.execute(update_sql, [status] + batch_ids)
                    
                    conn.commit()
                    logger.info(f"成功更新 {len(item_ids)} 个条目状态为 {status} (表: {source_table})")
                    return True
                    
        except Exception as e:
            logger.error(f"更新处理状态失败 (表: {source_table}): {e}")
            return False

    def extract_product_info(self, item_content: str, source_feed: str) -> Optional[Dict[str, Any]]:
        """
        使用fast_model从RSS内容中提取结构化的产品信息
        
        Args:
            item_content: RSS条目内容 (标题+摘要或全文)
            source_feed: 数据源名称
            
        Returns:
            提取的产品信息字典，失败时返回None
        """
        prompt = f"""
从以下产品描述中，提取结构化信息，并以单个JSON对象的格式返回。

请严格按照以下格式返回，如果某个字段信息缺失，请使用null：

{{
    "product_name": "[产品名称]",
    "tagline": "[一句话标语]",
    "description": "[对产品的详细描述]",
    "product_url": "[产品的主页链接]",
    "categories": "[一个包含最多5个相关标签的字符串，以逗号分隔]",
    "metrics": {{
        "problem_solved": "[它解决的核心问题]",
        "target_audience": "[目标用户，例如开发者、市场人员]",
        "tech_stack": "[提到的技术栈，例如React, Python]",
        "business_model": "[商业模式，例如SaaS, 开源]"
    }}
}}

文本: "{item_content}"
"""
        
        try:
            # 使用fast_model进行快速信息提取
            response = call_llm(prompt, model_type='fast')
            
            if not response['success']:
                logger.warning(f"LLM调用失败 (来源: {source_feed}): {response.get('error', 'Unknown error')}")
                return None
            
            # 解析JSON响应
            try:
                import re
                
                # 从响应中提取JSON内容
                content = response['content'].strip()
                product_info = None
                
                # 方案1：尝试找到JSON代码块（被```json包围）
                json_match = re.search(r'```(?:json)?\s*({.*?})\s*```', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                    try:
                        product_info = json.loads(json_str)
                    except json.JSONDecodeError:
                        pass
                
                # 方案2：如果方案1失败，尝试解析整个内容
                if not product_info:
                    try:
                        product_info = json.loads(content)
                    except json.JSONDecodeError:
                        pass
                
                # 方案3：正则表达式托底方案 - 提取JSON字段
                if not product_info:
                    logger.warning(f"使用正则表达式托底解析 (来源: {source_feed})")
                    try:
                        product_info = self._extract_json_with_regex(content)
                    except Exception as e:
                        logger.debug(f"正则表达式解析也失败: {e}")
                
                if product_info:
                    # 添加源信息
                    product_info['source_feed'] = source_feed
                    return product_info
                else:
                    logger.warning(f"所有解析方案都失败 (来源: {source_feed})")
                    logger.debug(f"原始响应内容: {content[:500]}...")
                    return None
                    
            except Exception as e:
                logger.error(f"解析LLM响应时出现异常 (来源: {source_feed}): {e}")
                logger.debug(f"原始响应内容: {response.get('content', '')[:500]}...") 
                return None
                
        except Exception as e:
            logger.error(f"提取产品信息失败 (来源: {source_feed}): {e}")
            return None

    def _extract_json_with_regex(self, content: str) -> Optional[Dict[str, Any]]:
        """
        使用正则表达式托底解析JSON字段
        
        Args:
            content: 响应内容
            
        Returns:
            提取的产品信息或None
        """
        import re
        
        try:
            # 简化的字段提取，主要提取基本的JSON字段
            product_info = {}
            
            # 只提取基本的JSON字段，避免复杂的正则表达式
            simple_patterns = {
                'product_name': r'"product_name"\s*:\s*"([^"]+)"',
                'tagline': r'"tagline"\s*:\s*"([^"]+)"',
                'description': r'"description"\s*:\s*"([^"]+)"',
                'product_url': r'"product_url"\s*:\s*"([^"]+)"',
                'categories': r'"categories"\s*:\s*"([^"]+)"'
            }
            
            for field, pattern in simple_patterns.items():
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    product_info[field] = match.group(1).strip()
                else:
                    product_info[field] = None
            
            # 添加空的metrics
            product_info['metrics'] = {
                'problem_solved': None,
                'target_audience': None,
                'tech_stack': None,
                'business_model': None
            }
            
            # 检查是否提取到了有用信息（放宽条件，不强制要求产品名称）
            has_useful_info = (
                product_info.get('product_name') or 
                product_info.get('tagline') or 
                product_info.get('description')
            )
            
            if has_useful_info:
                if product_info.get('product_name'):
                    logger.info(f"正则表达式托底解析成功：{product_info.get('product_name')}")
                else:
                    logger.info("正则表达式托底解析成功：提取到了有价值的内容信息")
                return product_info
            else:
                logger.warning("正则表达式解析未能提取到有用信息")
                return None
                
        except Exception as e:
            logger.error(f"正则表达式解析失败: {e}")
            return None

    def process_single_item(self, item: Dict[str, Any], source_feed: str) -> Optional[Dict[str, Any]]:
        """
        处理单个RSS条目
        
        Args:
            item: RSS条目数据
            source_feed: 数据源名称
            
        Returns:
            处理后的产品信息，失败时返回None
        """
        try:
            # 组合内容文本 - 支持不同的表结构
            content_parts = []
            
            # 处理标准RSS字段
            if item.get('title'):
                content_parts.append(f"标题: {item['title']}")
            if item.get('summary'):
                content_parts.append(f"摘要: {item['summary']}")
            if item.get('full_content'):
                content_parts.append(f"内容: {item['full_content']}")
            
            # 处理decohack产品表的特殊字段
            if item.get('product_name'):
                content_parts.append(f"产品名称: {item['product_name']}")
            if item.get('tagline'):
                content_parts.append(f"标语: {item['tagline']}")
            if item.get('description'):
                content_parts.append(f"描述: {item['description']}")
            if item.get('product_url'):
                content_parts.append(f"产品链接: {item['product_url']}")
            if item.get('keywords'):
                content_parts.append(f"关键词: {item['keywords']}")
            
            content_text = "\n".join(content_parts)
            
            if not content_text.strip():
                logger.warning(f"条目 {item.get('id', 'Unknown')} 内容为空")
                return None
            
            # 对于decohack表，如果已有结构化数据，直接使用
            if source_feed == 'decohack' and item.get('product_name'):
                product_info = {
                    'product_name': item.get('product_name'),
                    'tagline': item.get('tagline'),
                    'description': item.get('description'),
                    'product_url': item.get('product_url'),
                    'categories': item.get('keywords'),
                    'metrics': {
                        'problem_solved': None,
                        'target_audience': None,
                        'tech_stack': None,
                        'business_model': None
                    },
                    'source_feed': source_feed
                }
                
                # 添加时间信息
                product_info['source_published_at'] = item.get('ph_publish_date') or item.get('published_at')
                logger.debug(f"直接使用结构化数据处理条目 {item.get('id', 'Unknown')}: {product_info.get('product_name', 'Unknown')}")
                return product_info
            
            # 其他情况使用LLM提取产品信息
            product_info = self.extract_product_info(content_text, source_feed)
            
            if product_info:
                # 添加时间信息
                product_info['source_published_at'] = item.get('published_at')
                logger.debug(f"成功处理条目 {item.get('id', 'Unknown')}: {product_info.get('product_name', 'Unknown')}")
            
            return product_info
            
        except Exception as e:
            logger.error(f"处理单个条目失败 (ID: {item.get('id', 'Unknown')}): {e}")
            return None

    def batch_process_items(self, items: List[Dict[str, Any]], source_feed: str) -> Tuple[List[Dict[str, Any]], List[int], List[int]]:
        """
        并行批量处理RSS条目
        
        Args:
            items: RSS条目列表
            source_feed: 数据源名称
            
        Returns:
            (成功提取的产品信息列表, 成功处理的条目ID列表, 失败的条目ID列表)
        """
        if not items:
            return [], [], []
        
        logger.info(f"开始并行处理 {len(items)} 个条目 (来源: {source_feed}, 最大并发: {self.max_workers})")
        
        successful_products = []
        successful_ids = []
        failed_ids = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_item = {
                executor.submit(self.process_single_item, item, source_feed): item 
                for item in items
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                item_id = item.get('id')
                
                try:
                    product_info = future.result()
                    if product_info:
                        successful_products.append(product_info)
                        successful_ids.append(item_id)
                    else:
                        failed_ids.append(item_id)
                        
                except Exception as e:
                    logger.error(f"处理条目 {item_id} 时出现异常: {e}")
                    failed_ids.append(item_id)
        
        logger.info(f"批量处理完成 - 成功: {len(successful_products)}, 失败: {len(failed_ids)}")
        return successful_products, successful_ids, failed_ids

    def save_discovered_products(self, products: List[Dict[str, Any]]) -> bool:
        """
        保存发现的产品信息到统一产品库
        
        Args:
            products: 产品信息列表
            
        Returns:
            操作是否成功
        """
        if not products:
            return True
        
        try:
            # 准备插入数据
            insert_data = []
            for product in products:
                # 处理product_name为null的情况，使用占位符
                product_name = product.get('product_name')
                if not product_name or not product_name.strip():
                    # 根据来源生成描述性占位符
                    source_feed = product.get('source_feed', 'unknown') or 'unknown'
                    tagline = (product.get('tagline') or '').strip()
                    description = (product.get('description') or '').strip()
                    
                    if tagline:
                        product_name = f"[{source_feed}内容] {tagline[:50]}"
                    elif description:
                        product_name = f"[{source_feed}内容] {description[:50]}"
                    else:
                        product_name = f"[{source_feed}未命名内容]"
                    
                    logger.debug(f"为空产品名称生成占位符: {product_name}")
                
                metrics_json = json.dumps(product.get('metrics', {}), ensure_ascii=False)
                
                item_data = {
                    'product_name': product_name,
                    'tagline': product.get('tagline'),
                    'description': product.get('description'),
                    'product_url': product.get('product_url'),
                    'image_url': product.get('image_url'),
                    'categories': product.get('categories'),
                    'metrics': metrics_json,
                    'source_feed': product.get('source_feed', 'unknown') or 'unknown',
                    'source_published_at': product.get('source_published_at')
                }
                insert_data.append(item_data)
            
            # 批量插入
            inserted_count = self.db_manager.insert_rss_items_batch('discovered_products', insert_data)
            
            if inserted_count > 0:
                logger.info(f"成功保存 {inserted_count} 个产品到统一产品库")
                return True
            else:
                logger.warning("没有产品被成功保存")
                return False
                
        except Exception as e:
            logger.error(f"保存产品信息失败: {e}")
            return False

    def run_product_discovery_analysis(self, source_tables: List[str] = None, batch_size: int = 50) -> Dict[str, Any]:
        """
        运行产品发现分析的完整流程
        
        Args:
            source_tables: 要分析的源表列表，默认为所有产品相关的表
            batch_size: 每批处理的数量
            
        Returns:
            分析结果字典
        """
        if not source_tables:
            # 默认分析产品相关的表
            source_tables = [
                'rss_betalist',
                'rss_ezindie', 
                'rss_indiehackers',
                'rss_decohack_products'
            ]
        
        logger.info(f"开始产品发现分析 - 处理表: {source_tables}")
        
        results = {
            'success': True,
            'processed_tables': [],
            'total_processed': 0,
            'total_extracted': 0,
            'errors': []
        }
        
        for table_name in source_tables:
            try:
                logger.info(f"处理表: {table_name}")
                
                # 获取待处理的数据
                pending_items = self.select_and_lock_pending_items(table_name, batch_size)
                
                if not pending_items:
                    logger.info(f"表 {table_name} 没有待处理的数据")
                    continue
                
                # 确定数据源名称
                if 'indiehackers' in table_name:
                    source_feed = 'indiehackers'
                elif 'betalist' in table_name:
                    source_feed = 'betalist'
                elif 'ezindie' in table_name:
                    source_feed = 'ezindie'
                elif 'decohack' in table_name:
                    source_feed = 'decohack'
                else:
                    source_feed = table_name.replace('rss_', '')
                
                # 并行处理条目
                products, success_ids, failed_ids = self.batch_process_items(
                    pending_items, source_feed
                )
                
                # 保存成功提取的产品信息
                if products:
                    save_success = self.save_discovered_products(products)
                    if not save_success:
                        logger.error(f"保存产品信息失败 (表: {table_name})")
                        results['errors'].append(f"保存{table_name}产品信息失败")
                
                # 更新处理状态
                if success_ids:
                    self.update_processing_status(table_name, success_ids, 'success')
                
                if failed_ids:
                    self.update_processing_status(table_name, failed_ids, 'failed')
                
                # 记录结果
                table_result = {
                    'table_name': table_name,
                    'processed_count': len(pending_items),
                    'extracted_count': len(products),
                    'success_count': len(success_ids),
                    'failed_count': len(failed_ids)
                }
                
                results['processed_tables'].append(table_result)
                results['total_processed'] += len(pending_items)
                results['total_extracted'] += len(products)
                
                logger.info(f"表 {table_name} 处理完成 - 处理: {len(pending_items)}, 提取: {len(products)}")
                
            except Exception as e:
                error_msg = f"处理表 {table_name} 时出错: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
                results['success'] = False
        
        logger.info(f"产品发现分析完成 - 总处理: {results['total_processed']}, 总提取: {results['total_extracted']}")
        return results


def select_and_lock_pending_items(db_manager: DatabaseManager, source_table: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    便捷函数：选择并锁定待处理的数据行
    """
    analyzer = DataAnalyzer(db_manager)
    return analyzer.select_and_lock_pending_items(source_table, limit)


def extract_product_info(item_content: str) -> Optional[Dict[str, Any]]:
    """
    便捷函数：从内容中提取产品信息
    """
    analyzer = DataAnalyzer(None)  # 对于单纯的提取功能，不需要db_manager
    return analyzer.extract_product_info(item_content, 'unknown')


class TechNewsAnalyzer:
    """科技新闻分析器，实施科技与创投新闻分析与报告功能"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化科技新闻分析器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.max_workers = config.get_max_workers()
        logger.info(f"科技新闻分析器初始化完成 - 最大并发数: {self.max_workers}")

    def get_tech_news_articles(self, hours_back: int = 24) -> List[Dict[str, Any]]:
        """
        获取指定时间范围内的科技新闻文章
        
        Args:
            hours_back: 回溯小时数，默认24小时
            
        Returns:
            文章列表，包含id, title, content, source_feed等字段
        """
        try:
            from datetime import datetime, timedelta
            
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)
            
            # 科技新闻相关的表及其内容字段配置
            tech_tables = {
                'rss_ycombinator': {
                    'source_feed': 'ycombinator',
                    'content_field': 'full_content',
                    'base_fields': 'id, title, link, guid, published_at, created_at, analysis_result'
                },
                'rss_techcrunch': {
                    'source_feed': 'techcrunch',
                    'content_field': 'full_content',
                    'base_fields': 'id, title, link, guid, published_at, created_at, analysis_result'
                },
                'rss_theverge': {
                    'source_feed': 'theverge',
                    'content_field': 'summary',
                    'base_fields': 'id, title, link, guid, published_at, created_at, analysis_result'
                }
            }
            
            all_articles = []
            
            for table_name, table_config in tech_tables.items():
                try:
                    with self.db_manager.get_connection() as conn:
                        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                            # 根据表的结构构建查询SQL
                            content_field = table_config['content_field']
                            base_fields = table_config['base_fields']
                            source_feed = table_config['source_feed']
                            
                            # 查询指定时间范围内的文章
                            query_sql = f"""
                                SELECT {base_fields}, {content_field} as content
                                FROM {table_name}
                                WHERE created_at >= %s AND created_at <= %s
                                AND ({content_field} IS NOT NULL AND {content_field} != '')
                                ORDER BY created_at DESC
                            """
                            cursor.execute(query_sql, (start_time, end_time))
                            rows = cursor.fetchall()
                            
                            # 为每篇文章添加source_feed信息
                            for row in rows:
                                row['source_feed'] = source_feed
                                all_articles.append(row)
                                
                            logger.info(f"从 {table_name} 获取到 {len(rows)} 篇文章")
                            
                except Exception as e:
                    logger.error(f"查询表 {table_name} 失败: {e}")
                    continue
            
            logger.info(f"总共获取到 {len(all_articles)} 篇科技新闻文章 (过去{hours_back}小时)")
            return all_articles
            
        except Exception as e:
            logger.error(f"获取科技新闻文章失败: {e}")
            return []

    def analyze_single_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        层次一：单篇文章分析与核心信息提取
        先检查数据库中是否已有分析结果，避免重复分析
        
        Args:
            article: 文章数据，包含title, full_content等字段
            
        Returns:
            分析结果JSON对象或None
        """
        try:
            article_id = article.get('id')
            source_feed = article.get('source_feed')
            
            # 先检查数据库中是否已有分析结果
            existing_analysis = article.get('analysis_result')
            if existing_analysis:
                try:
                    # 尝试解析已存在的分析结果
                    if isinstance(existing_analysis, str):
                        analysis_result = json.loads(existing_analysis)
                    else:
                        analysis_result = existing_analysis
                    
                    # 验证分析结果的完整性
                    required_fields = ['summary', 'key_info', 'tags']
                    if all(field in analysis_result for field in required_fields):
                        # 验证tags结构
                        tags = analysis_result.get('tags', {})
                        if isinstance(tags, dict) and 'primary_tag' in tags:
                            # 结果完整且有效，直接使用
                            analysis_result['article_id'] = article_id
                            analysis_result['source_feed'] = source_feed
                            analysis_result['article_title'] = article.get('title')
                            analysis_result['article_link'] = article.get('link')
                            analysis_result['published_at'] = article.get('published_at')
                            
                            logger.debug(f"使用已缓存的完整分析结果 (文章ID: {article_id})")
                            return analysis_result
                        else:
                            logger.debug(f"已存在的分析结果结构不完整 (文章ID: {article_id})，重新分析")
                    else:
                        logger.debug(f"已存在的分析结果缺少必要字段 (文章ID: {article_id})，重新分析")
                        
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"解析已存在的分析结果失败 (文章ID: {article_id}): {e}，重新分析")
            
            # 没有已存在的结果，进行新的分析
            logger.debug(f"开始新的LLM分析 (文章ID: {article_id})")
            
            # 构建文章内容
            article_content = f"标题: {article.get('title', '')}\n\n内容: {article.get('content', '')}"
            
            # 确保有内容可分析
            if not article.get('content', '').strip():
                logger.warning(f"文章 {article_id} 没有内容，跳过分析")
                return None
            
            prompt = f"""
你是一位顶尖的金融与科技新闻分析师。你的任务是分析所提供的新闻文章，并以结构化的JSON格式提取关键信息。请确保所有输出内容均为中文。

**分析指南:**

1. **生成摘要**: 用5个左右句子，提供文章的精炼、中立的中文摘要。
2. **提取关键信息**: 识别并列出文章中的核心实体、数据或概念，例如公司、产品、人物、融资金额、技术术语等。
3. **分配层级标签**: 从下方提供的 `<TAG_HIERARCHY>` 中，你必须选择一个 `primary_tag`，并可以一个或多个相关的 `secondary_tags`。标签必须准确反映文章的核心主题。

**输入文章:**
{article_content}

**上下文信息: 标签层级体系**
<TAG_HIERARCHY>
- **1. 产品与项目 (Product & Project)**
    - `1.1. 新产品/项目发布 (New Launch)`: 对应 "Launch HN", "Show HN" 或 BetaList 上的新项目。
    - `1.2. 项目更新/里程碑 (Project Update)`: 如 "我们刚刚达到 1000 个用户" 或 "2.0 版本发布"。
    - `1.3. 项目经验/复盘 (Case Study)`: 如 "我如何通过...获得前100个用户" 或 "我们失败的经验教训"。
    - `1.4. 求职/招聘 (Hiring)`: 如 "Ember (YC F24) Is Hiring Full Stack Engineer"。
- **2. 公司与市场 (Corporate & Market)**
    - `2.1. 融资与并购 (Funding & M&A)`: 包括融资、收购、IPO 等。
    - `2.2. 公司战略/变动 (Strategy & Change)`: 大公司的战略调整、组织架构变动、财报发布。
    - `2.3. 市场动态/法规 (Market & Regulation)`: 行业级别的政策变动、市场准入规则、重要报告等。
- **3. 技术深度 (Technical Deep Dive)**
    - `3.1. 技术教程/指南 (Tutorial & Guide)`: "如何用...实现..." 或 "...入门指南"。
    - `3.2. 架构/原理分析 (Architecture & Principle)`: "深入理解..." 或 "...的设计原理"。
    - `3.3. 开源库/工具介绍 (Library & Tool)`: 对某个具体开源项目或开发工具的介绍。
- **4. 行业观察与观点 (Industry Insights & Opinion)**
    - `4.1. 趋势分析/预测 (Trend Analysis)`: 对某个技术或市场方向的宏观分析和未来预测。
    - `4.2. 个人观点/评论 (Opinion & Commentary)`: 对某个事件、技术或趋势的深度评论或思辨。
    - `4.3. 科学研究/突破 (Scientific Research)`: 学术界或研究机构发布的重大科研成果。
- **5. 安全与事件 (Security & Incidents)**
    - `5.1. 安全漏洞/攻击 (Vulnerability & Attack)`: 如 "Comet AI browser can get prompt injected"。
    - `5.2. 服务中断/故障 (Outage & Failure)`: 如 "Ask HN: GitHub Copilot down?"。
    - `5.3. 法律/诉讼 (Legal & Lawsuit)`: 如 "Internet Access Providers Aren't Bound by DMCA Unmasking Subpoenas"。
- **6. 社区与文化 (Community & Culture)**
    - `6.1. 社区讨论 (Discussion)`: 如 "Ask HN: Best codebases to study?"。
    - `6.2. 历史与怀旧 (History & Retro)`: 如 "Blast from the past: Facit A2400 terminal"。
</TAG_HIERARCHY>

**你的输出必须是一个单一、有效的JSON对象**，并严格遵循以下结构。所有内容值（如摘要、关键信息）都必须是**中文**：
```json
{{
  "summary": "一段精炼的5句话左右的中文文章摘要。",
  "key_info": [
    "关键信息一",
    "关键信息二",
    "..."
  ],
  "tags": {{
    "primary_tag": "从标签体系中选择的最相关的主标签 (例如: '产品与项目 (Product & Project)')",
    "secondary_tags": [
      "一个或多个相关的次级标签列表 (例如: '1.1. 新产品/项目发布 (New Launch)')"
    ]
  }}
}}
```
"""
            
            # 调用fast_model进行分析
            response = call_llm(prompt, model_type='fast')
            
            if not response.get('success', False):
                logger.warning(f"LLM调用失败 (文章ID: {article.get('id')}): {response.get('error', 'Unknown error')}")
                return None
            
            # 解析JSON响应
            import re
            content = response['content'].strip()
            analysis_result = None
            
            # 方案1：尝试找到JSON代码块
            json_match = re.search(r'```(?:json)?\s*({.*?})\s*```', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
                try:
                    analysis_result = json.loads(json_str)
                except json.JSONDecodeError as e:
                    logger.debug(f"JSON代码块解析失败: {e}")
            
            # 方案2：直接解析整个内容
            if not analysis_result:
                try:
                    analysis_result = json.loads(content)
                except json.JSONDecodeError as e:
                    logger.debug(f"整体内容JSON解析失败: {e}")
            
            if analysis_result:
                # 添加文章元数据
                analysis_result['article_id'] = article.get('id')
                analysis_result['source_feed'] = article.get('source_feed')
                analysis_result['article_title'] = article.get('title')
                analysis_result['article_link'] = article.get('link')
                analysis_result['published_at'] = article.get('published_at')
                
                # 立即保存到数据库，避免流程中断时丢失结果
                try:
                    self._save_analysis_result_to_db(article_id, source_feed, analysis_result)
                    logger.debug(f"已立即保存分析结果到数据库 (文章ID: {article_id})")
                except Exception as e:
                    logger.warning(f"保存分析结果到数据库失败 (文章ID: {article_id}): {e}")
                    # 不影响主流程，继续返回结果
                
                logger.debug(f"成功分析文章 {article.get('id')}: {article.get('title', '')[:50]}...")
                return analysis_result
            else:
                logger.warning(f"无法解析文章分析结果 (文章ID: {article.get('id')})")
                return None
                
        except Exception as e:
            logger.error(f"分析单篇文章失败 (文章ID: {article.get('id')}): {e}")
            return None
    
    def _save_analysis_result_to_db(self, article_id: int, source_feed: str, analysis_result: Dict[str, Any]) -> bool:
        """
        将分析结果保存到数据库的analysis_result字段
        
        Args:
            article_id: 文章ID
            source_feed: 数据源名称
            analysis_result: 分析结果字典
            
        Returns:
            是否保存成功
        """
        try:
            # 确定表名
            table_mapping = {
                'ycombinator': 'rss_ycombinator',
                'techcrunch': 'rss_techcrunch',
                'theverge': 'rss_theverge'
            }
            
            table_name = table_mapping.get(source_feed)
            if not table_name:
                logger.warning(f"未知的数据源: {source_feed}，无法保存分析结果")
                return False
            
            # 移除元数据，只保存纯分析结果
            clean_result = {
                'summary': analysis_result.get('summary'),
                'key_info': analysis_result.get('key_info'),
                'tags': analysis_result.get('tags'),
                'analyzed_at': datetime.now().isoformat()
            }
            
            # 将结果转为JSON字符串
            result_json = json.dumps(clean_result, ensure_ascii=False)
            
            # 更新数据库
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    update_sql = f"""
                        UPDATE {table_name}
                        SET analysis_result = %s
                        WHERE id = %s
                    """
                    cursor.execute(update_sql, (result_json, article_id))
                    conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"保存分析结果到数据库失败: {e}")
            return False
    
    def batch_save_analysis_results(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量保存分析结果到数据库，使用事务保护
        
        Args:
            analysis_results: 分析结果列表
            
        Returns:
            保存结果统计
        """
        if not analysis_results:
            return {'success': True, 'saved_count': 0, 'failed_count': 0, 'errors': []}
        
        # 按表名分组
        table_mapping = {
            'ycombinator': 'rss_ycombinator',
            'techcrunch': 'rss_techcrunch',
            'theverge': 'rss_theverge'
        }
        
        grouped_results = {}
        for result in analysis_results:
            source_feed = result.get('source_feed')
            table_name = table_mapping.get(source_feed)
            if table_name:
                if table_name not in grouped_results:
                    grouped_results[table_name] = []
                grouped_results[table_name].append(result)
        
        total_saved = 0
        total_failed = 0
        errors = []
        
        # 对每个表执行批量更新
        for table_name, table_results in grouped_results.items():
            try:
                with self.db_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        # 开始事务
                        conn.begin()
                        
                        saved_in_table = 0
                        for result in table_results:
                            try:
                                article_id = result.get('article_id')
                                
                                # 移除元数据，只保存纯分析结果
                                clean_result = {
                                    'summary': result.get('summary'),
                                    'key_info': result.get('key_info'),
                                    'tags': result.get('tags'),
                                    'analyzed_at': datetime.now().isoformat()
                                }
                                
                                # 转为JSON字符串
                                result_json = json.dumps(clean_result, ensure_ascii=False)
                                
                                # 更新数据库
                                update_sql = f"""
                                    UPDATE {table_name}
                                    SET analysis_result = %s
                                    WHERE id = %s
                                """
                                cursor.execute(update_sql, (result_json, article_id))
                                saved_in_table += 1
                                
                            except Exception as e:
                                logger.error(f"保存单个结果失败 (文章ID: {result.get('article_id')}): {e}")
                                errors.append(f"文章{result.get('article_id')}: {str(e)}")
                                total_failed += 1
                                continue
                        
                        # 提交事务
                        conn.commit()
                        total_saved += saved_in_table
                        logger.info(f"成功批量保存 {saved_in_table} 个分析结果到 {table_name}")
                        
            except Exception as e:
                logger.error(f"批量保存表 {table_name} 失败: {e}")
                errors.append(f"表{table_name}: {str(e)}")
                total_failed += len(table_results)
        
        return {
            'success': total_failed == 0,
            'saved_count': total_saved,
            'failed_count': total_failed,
            'errors': errors
        }

    def batch_analyze_articles(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        并行批量分析文章
        
        Args:
            articles: 文章列表
            
        Returns:
            分析结果列表
        """
        if not articles:
            return []
        
        logger.info(f"开始并行分析 {len(articles)} 篇文章 (最大并发: {self.max_workers})")
        
        analysis_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有分析任务
            future_to_article = {
                executor.submit(self.analyze_single_article, article): article 
                for article in articles
            }
            
            # 收集结果
            for future in concurrent.futures.as_completed(future_to_article):
                article = future_to_article[future]
                try:
                    result = future.result()
                    if result:
                        analysis_results.append(result)
                except Exception as e:
                    logger.error(f"分析文章 {article.get('id')} 时出现异常: {e}")
        
        logger.info(f"批量分析完成 - 成功分析: {len(analysis_results)} / {len(articles)}")
        return analysis_results

    def run_tech_news_analysis(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        运行完整的科技新闻分析流程
        
        Args:
            hours_back: 分析过去多少小时的新闻，默认24小时
            
        Returns:
            完整的分析结果
        """
        logger.info(f"开始运行科技新闻分析 - 分析过去{hours_back}小时的新闻")
        
        try:
            # 1. 获取科技新闻文章
            articles = self.get_tech_news_articles(hours_back)
            
            if not articles:
                logger.warning("没有找到符合条件的科技新闻文章")
                return {
                    'success': False,
                    'message': '没有找到符合条件的文章',
                    'analysis_results': []
                }
            
            # 2. 批量分析文章（层次一）- 每个结果会立即保存到数据库
            analysis_results = self.batch_analyze_articles(articles)
            
            if not analysis_results:
                logger.warning("文章分析未产生有效结果")
                return {
                    'success': False,
                    'message': '文章分析未产生有效结果',
                    'analysis_results': []
                }
            
            # 3. 生成综合洞察（层次三）
            comprehensive_insights = self.generate_comprehensive_insights(analysis_results, f'过去{hours_back}小时')
            
            # 4. 构建最终结果（所有结果已经在分析过程中保存）
            final_result = {
                'success': True,
                'analysis_period': f'过去{hours_back}小时',
                'total_articles_found': len(articles),
                'successful_analysis_count': len(analysis_results),
                'analysis_results': analysis_results,
                'comprehensive_insights': comprehensive_insights,
                'generated_at': datetime.now().isoformat()
            }
            
            logger.info(f"科技新闻分析完成 - 分析 {len(articles)} 篇文章，成功 {len(analysis_results)} 篇，结果已全部保存，生成综合洞察")
            return final_result
            
        except Exception as e:
            logger.error(f"科技新闻分析失败: {e}")
            return {
                'success': False,
                'message': f'分析过程中出现错误: {str(e)}',
                'analysis_results': []
            }
    
    def generate_comprehensive_insights(self, analysis_results: List[Dict[str, Any]], 
                                      time_period: str = "过去24小时") -> Dict[str, Any]:
        """
        层次三：综合分析与关联洞察
        整合已分析的文章数据，进行跨文章的关联分析，识别趋势和模式
        
        Args:
            analysis_results: 层次一分析的结果列表
            time_period: 分析时间范围
            
        Returns:
            综合洞察结果
        """
        if not analysis_results:
            return {
                'success': False,
                'message': '没有可用的分析结果进行综合分析',
                'insights': {}
            }
    
    def _analyze_article_statistics(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析文章的基础统计数据
        
        Args:
            analysis_results: 分析结果列表
            
        Returns:
            统计分析结果
        """
        try:
            total_articles = len(analysis_results)
            
            # 按数据源统计
            source_counts = {}
            for result in analysis_results:
                source = result.get('source_feed', 'unknown')
                source_counts[source] = source_counts.get(source, 0) + 1
            
            # 摘要长度统计
            summary_lengths = [len(result.get('summary', '')) for result in analysis_results]
            avg_summary_length = sum(summary_lengths) / len(summary_lengths) if summary_lengths else 0
            
            # 关键信息数量统计
            key_info_counts = [len(result.get('key_info', [])) for result in analysis_results]
            avg_key_info_count = sum(key_info_counts) / len(key_info_counts) if key_info_counts else 0
            
            return {
                'total_articles': total_articles,
                'source_distribution': source_counts,
                'content_metrics': {
                    'avg_summary_length': round(avg_summary_length, 1),
                    'avg_key_info_count': round(avg_key_info_count, 1),
                    'summary_length_range': {
                        'min': min(summary_lengths) if summary_lengths else 0,
                        'max': max(summary_lengths) if summary_lengths else 0
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"统计分析失败: {e}")
            return {}
    
    def _analyze_topic_distribution(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析主题标签的分布情况
        
        Args:
            analysis_results: 分析结果列表
            
        Returns:
            主题分布分析结果
        """
        try:
            primary_tag_counts = {}
            secondary_tag_counts = {}
            topic_articles = {}  # 记录每个主题对应的文章
            
            for result in analysis_results:
                tags = result.get('tags', {})
                article_id = result.get('article_id')
                article_title = result.get('article_title', '')
                
                # 统计主标签
                primary_tag = tags.get('primary_tag', '')
                if primary_tag:
                    primary_tag_counts[primary_tag] = primary_tag_counts.get(primary_tag, 0) + 1
                    
                    # 记录文章信息
                    if primary_tag not in topic_articles:
                        topic_articles[primary_tag] = []
                    topic_articles[primary_tag].append({
                        'article_id': article_id,
                        'title': article_title[:100] + '...' if len(article_title) > 100 else article_title
                    })
                
                # 统计次级标签
                secondary_tags = tags.get('secondary_tags', [])
                for tag in secondary_tags:
                    if tag:
                        secondary_tag_counts[tag] = secondary_tag_counts.get(tag, 0) + 1
            
            # 按数量排序
            sorted_primary = sorted(primary_tag_counts.items(), key=lambda x: x[1], reverse=True)
            sorted_secondary = sorted(secondary_tag_counts.items(), key=lambda x: x[1], reverse=True)
            
            # 生成主题分布数据
            topic_distribution = []
            for tag, count in sorted_primary:
                percentage = (count / len(analysis_results)) * 100
                topic_distribution.append({
                    'topic': tag,
                    'article_count': count,
                    'percentage': round(percentage, 1),
                    'sample_articles': topic_articles.get(tag, [])[:3]  # 最多3个样例
                })
            
            return {
                'topic_distribution': topic_distribution,
                'primary_tag_stats': {
                    'total_unique_tags': len(primary_tag_counts),
                    'most_common': sorted_primary[:5]  # Top 5
                },
                'secondary_tag_stats': {
                    'total_unique_tags': len(secondary_tag_counts),
                    'most_common': sorted_secondary[:10]  # Top 10
                }
            }
            
        except Exception as e:
            logger.error(f"主题分布分析失败: {e}")
            return {}
    
    def _analyze_key_information_clusters(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析关键信息的聚类情况，识别热门关键词和实体
        
        Args:
            analysis_results: 分析结果列表
            
        Returns:
            关键信息聚类分析结果
        """
        try:
            key_info_frequency = {}
            key_info_articles = {}  # 记录每个关键信息对应的文章
            all_key_info = []
            
            # 收集所有关键信息
            for result in analysis_results:
                key_info_list = result.get('key_info', [])
                article_id = result.get('article_id')
                article_title = result.get('article_title', '')
                
                for info in key_info_list:
                    if info and isinstance(info, str):
                        # 清理和标准化关键信息
                        clean_info = info.strip()
                        if len(clean_info) > 2:  # 过滤太短的信息
                            all_key_info.append(clean_info)
                            key_info_frequency[clean_info] = key_info_frequency.get(clean_info, 0) + 1
                            
                            # 记录文章信息
                            if clean_info not in key_info_articles:
                                key_info_articles[clean_info] = []
                            key_info_articles[clean_info].append({
                                'article_id': article_id,
                                'title': article_title[:80] + '...' if len(article_title) > 80 else article_title
                            })
            
            # 排序并筛选热门关键信息
            sorted_key_info = sorted(key_info_frequency.items(), key=lambda x: x[1], reverse=True)
            
            # 识别热门实体（出现频率 >= 2的）
            hot_entities = []
            trending_topics = []
            
            for info, freq in sorted_key_info:
                if freq >= 2:  # 至少出现在2篇文章中
                    entity_data = {
                        'entity': info,
                        'frequency': freq,
                        'articles': key_info_articles[info][:3]  # 最多3个样例文章
                    }
                    
                    # 简单分类：公司名、产品名、技术名词等
                    if any(keyword in info.lower() for keyword in ['openai', 'google', 'microsoft', 'meta', 'apple', 'amazon']):
                        hot_entities.append(entity_data)
                    elif any(keyword in info.lower() for keyword in ['ai', '人工智能', '机器学习', 'gpt', 'llm']):
                        trending_topics.append(entity_data)
                    else:
                        hot_entities.append(entity_data)
            
            # 生成关键词云数据（Top 20）
            keyword_cloud = []
            for info, freq in sorted_key_info[:20]:
                weight = min(freq / max(key_info_frequency.values()) * 100, 100) if key_info_frequency else 0
                keyword_cloud.append({
                    'word': info,
                    'frequency': freq,
                    'weight': round(weight, 1)
                })
            
            return {
                'hot_entities': hot_entities[:10],  # Top 10 热门实体
                'trending_topics': trending_topics[:5],  # Top 5 趋势话题
                'keyword_cloud': keyword_cloud,
                'statistics': {
                    'total_unique_key_info': len(key_info_frequency),
                    'total_key_info_mentions': len(all_key_info),
                    'avg_key_info_per_article': round(len(all_key_info) / len(analysis_results), 1) if analysis_results else 0
                }
            }
            
        except Exception as e:
            logger.error(f"关键信息聚类分析失败: {e}")
            return {}
    
    def _analyze_source_patterns(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        分析不同数据源的特点和模式
        
        Args:
            analysis_results: 分析结果列表
            
        Returns:
            数据源分析结果
        """
        try:
            source_patterns = {}
            
            # 按数据源分组
            for result in analysis_results:
                source = result.get('source_feed', 'unknown')
                if source not in source_patterns:
                    source_patterns[source] = {
                        'articles': [],
                        'topics': {},
                        'avg_key_info_count': 0,
                        'avg_summary_length': 0
                    }
                
                source_patterns[source]['articles'].append(result)
                
                # 统计主题分布
                primary_tag = result.get('tags', {}).get('primary_tag', '')
                if primary_tag:
                    source_patterns[source]['topics'][primary_tag] = source_patterns[source]['topics'].get(primary_tag, 0) + 1
            
            # 计算各源特征
            source_analysis = []
            for source, data in source_patterns.items():
                articles = data['articles']
                if not articles:
                    continue
                
                # 计算平均指标
                avg_key_info = sum(len(a.get('key_info', [])) for a in articles) / len(articles)
                avg_summary_len = sum(len(a.get('summary', '')) for a in articles) / len(articles)
                
                # 找出主导主题
                top_topics = sorted(data['topics'].items(), key=lambda x: x[1], reverse=True)[:3]
                
                source_analysis.append({
                    'source': source,
                    'article_count': len(articles),
                    'avg_key_info_count': round(avg_key_info, 1),
                    'avg_summary_length': round(avg_summary_len, 1),
                    'top_topics': top_topics,
                    'specialization': self._calculate_source_specialization(data['topics'])
                })
            
            return {
                'source_analysis': source_analysis,
                'cross_source_insights': self._generate_cross_source_insights(source_patterns)
            }
            
        except Exception as e:
            logger.error(f"数据源分析失败: {e}")
            return {}
    
    def _calculate_source_specialization(self, topics_dict: Dict[str, int]) -> Dict[str, Any]:
        """计算数据源的专业化程度"""
        if not topics_dict:
            return {'score': 0, 'dominant_topic': None}
        
        total_articles = sum(topics_dict.values())
        max_topic_count = max(topics_dict.values())
        dominant_topic = max(topics_dict.items(), key=lambda x: x[1])[0]
        
        # 专业化分数：主导主题占比
        specialization_score = (max_topic_count / total_articles) * 100
        
        return {
            'score': round(specialization_score, 1),
            'dominant_topic': dominant_topic,
            'topic_diversity': len(topics_dict)
        }
    
    def _generate_cross_source_insights(self, source_patterns: Dict[str, Any]) -> List[Dict[str, Any]]:
        """生成跨数据源的洞察"""
        insights = []
        
        try:
            # 找出共同关注的主题
            all_topics = {}
            for source, data in source_patterns.items():
                for topic, count in data['topics'].items():
                    if topic not in all_topics:
                        all_topics[topic] = []
                    all_topics[topic].append({'source': source, 'count': count})
            
            # 识别跨源热门主题
            cross_source_topics = []
            for topic, sources in all_topics.items():
                if len(sources) >= 2:  # 至少两个源关注
                    total_mentions = sum(s['count'] for s in sources)
                    cross_source_topics.append({
                        'topic': topic,
                        'total_mentions': total_mentions,
                        'sources': sources
                    })
            
            # 按热度排序
            cross_source_topics.sort(key=lambda x: x['total_mentions'], reverse=True)
            
            if cross_source_topics:
                insights.append({
                    'type': 'cross_source_trending',
                    'description': '跨数据源热门主题',
                    'data': cross_source_topics[:5]
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"跨源洞察生成失败: {e}")
            return []
    
    def _generate_deep_insights(self, statistics: Dict, topic_analysis: Dict, 
                               key_info_analysis: Dict, source_analysis: Dict, 
                               time_period: str) -> Dict[str, Any]:
        """
        使用smart_model生成深度洞察
        
        Args:
            statistics: 统计数据
            topic_analysis: 主题分析
            key_info_analysis: 关键信息分析
            source_analysis: 数据源分析
            time_period: 时间范围
            
        Returns:
            深度洞察结果
        """
        try:
            # 构建给LLM的上下文数据
            context_data = {
                'time_period': time_period,
                'total_articles': statistics.get('total_articles', 0),
                'top_topics': [item['topic'] for item in topic_analysis.get('topic_distribution', [])[:5]],
                'hot_entities': [item['entity'] for item in key_info_analysis.get('hot_entities', [])[:5]],
                'trending_topics': [item['entity'] for item in key_info_analysis.get('trending_topics', [])[:3]],
                'source_insights': source_analysis.get('cross_source_insights', [])
            }
            
            # 构建专业的prompt，严格按照项目规划文档的"So What?"分析框架
            prompt = f"""
你是一位在 a16z (Andreessen Horowitz) 工作的资深科技分析师，以能从新闻中发现别人看不到的趋势和机会而闻名。

现在，请基于以下我提供的{time_period}科技新闻多维度信息，为我生成一份深度分析报告。

**[输入信息]**
1. **数据概要**:
   - 分析文章数: {context_data['total_articles']}
   - 主要主题: {', '.join(context_data['top_topics'])}
   - 热门实体: {', '.join(context_data['hot_entities'])}
   - 趋势话题: {', '.join(context_data['trending_topics'])}

**[你的任务]**
请严格按照以下JSON格式输出你的分析，确保每个字段都经过深思熟虑，并体现你的专业性：

{{
  "analyst_take": "（在这里写你的核心定性分析，约150字。要点明{time_period}科技领域的战略意义，例如：这不仅是技术的演进，更是市场对某种叙事的一次集体押注...同时，数据也揭示了某些潜在的风险信号...）",
  "key_impacts": {{
    "for_developers": "（对开发者意味着什么？例如：新技术栈的兴起为开发者提供了新的工具选择，可能会改变现有的开发范式...）",
    "for_investors": "（对投资者意味着什么？例如：某个领域的投资窗口依然敞开，但竞争格局已趋于激烈，后续投资需要关注差异化...）",
    "for_competitors": "（对竞争者意味着什么？例如：头部企业必须加快在某个方向的布局，以应对新兴技术的冲击...）"
  }},
  "opportunity_and_risk": {{
    "opportunity": "（这些趋势揭示了哪些新的、未被满足的市场机会？例如：围绕某个技术方向的工具链和服务将成为新的蓝海...）",
    "risk": "（这些趋势面临的最大潜在风险是什么？例如：如果技术发展速度超出预期，可能导致现有投资迅速贬值...）"
  }},
  "prediction": "（基于以上所有信息，对未来6-12个月做出一个大胆但合理的预测。例如：我预测某个技术领域将在半年内出现重大突破，彻底改变行业格局...）"
}}
"""
            
            # 调用smart_model
            from .llm_client import call_llm
            response = call_llm(prompt, model_type='smart')
            
            if response.get('success', False):
                # 解析JSON响应
                import re
                content = response['content'].strip()
                
                # 尝试解析JSON
                try:
                    # 方案1：找到JSON代码块
                    json_match = re.search(r'```(?:json)?\s*({.*?})\s*```', content, re.DOTALL)
                    if json_match:
                        insights_data = json.loads(json_match.group(1))
                    else:
                        # 方案2：直接解析
                        insights_data = json.loads(content)
                    
                    insights_data['generated_by'] = 'smart_model'
                    insights_data['confidence_score'] = 0.85  # 默认置信度
                    
                    logger.info(f"成功生成深度洞察")
                    return insights_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"无法解析LLM返回的JSON: {e}")
                    # 返回结构化的备用数据
                    return self._generate_fallback_insights(context_data)
            else:
                logger.warning(f"LLM调用失败: {response.get('error', 'Unknown error')}")
                return self._generate_fallback_insights(context_data)
                
        except Exception as e:
            logger.error(f"深度洞察生成失败: {e}")
            return self._generate_fallback_insights(context_data)
    
    def _generate_fallback_insights(self, context_data: Dict) -> Dict[str, Any]:
        """生成备用洞察（当LLM调用失败时）- 使用So What分析框架"""
        return {
            'analyst_take': f"{context_data['time_period']}的科技新闻数据显示了多个重要趋势的汇聚，主要集中在{', '.join(context_data['top_topics'][:3])}等领域。这反映了技术发展的加速和市场关注点的集中，同时也暴露了某些领域可能存在的过度炒作风险。",
            'key_impacts': {
                'for_developers': f"新兴技术栈围绕{', '.join(context_data['hot_entities'][:2])}等关键实体展开，为开发者提供了新的工具选择和职业发展方向。",
                'for_investors': f"当前趋势表明{', '.join(context_data['top_topics'][:2])}领域仍有投资机会，但需要关注市场集中度和竞争加剧的风险。",
                'for_competitors': f"围绕{', '.join(context_data['trending_topics'][:2])}的竞争正在加剧，企业需要加快相关技术布局以保持竞争优势。"
            },
            'opportunity_and_risk': {
                'opportunity': f"基于{', '.join(context_data['top_topics'][:2])}等热门领域的发展，相关的工具链、服务和应用层面仍存在大量未被满足的市场需求。",
                'risk': '技术发展速度可能超出市场消化能力，导致部分投资和项目面临估值回调的风险。'
            },
            'prediction': f"预计未来6-12个月，{context_data['top_topics'][0] if context_data['top_topics'] else '主要技术领域'}将出现更多的整合和标准化动作，市场将从概念验证转向实际应用落地。",
            'generated_by': 'fallback_algorithm',
            'confidence_score': 0.3
        }

    def generate_comprehensive_insights(self, analysis_results: List[Dict[str, Any]], 
                                      time_period: str) -> Dict[str, Any]:
        """
        层次三：综合分析与关联洞察
        整合单篇文章分析结果，生成综合性洞察
        
        Args:
            analysis_results: 单篇文章分析结果列表
            time_period: 时间范围描述
            
        Returns:
            综合洞察结果
        """
        logger.info(f"开始生成综合洞察 - 分析 {len(analysis_results)} 篇文章的数据")
        
        try:
            # 1. 统计分析
            statistics = self._analyze_article_statistics(analysis_results)
            
            # 2. 主题分类分析
            topic_analysis = self._analyze_topic_distribution(analysis_results)
            
            # 3. 关键信息聚类分析
            key_info_analysis = self._analyze_key_information_clusters(analysis_results)
            
            # 4. 数据源分析
            source_analysis = self._analyze_source_patterns(analysis_results)
            
            # 5. 生成深度洞察（使用smart_model）
            deep_insights = self._generate_deep_insights(
                statistics, topic_analysis, key_info_analysis, source_analysis, time_period
            )
            
            comprehensive_result = {
                'success': True,
                'time_period': time_period,
                'total_articles_analyzed': len(analysis_results),
                'insights': {
                    'statistics': statistics,
                    'topic_analysis': topic_analysis,
                    'key_info_analysis': key_info_analysis,
                    'source_analysis': source_analysis,
                    'deep_insights': deep_insights
                },
                'generated_at': datetime.now().isoformat()
            }
            
            logger.info(f"综合洞察生成完成 - 识别到 {len(topic_analysis.get('topic_distribution', []))} 个主题")
            return comprehensive_result
            
        except Exception as e:
            logger.error(f"综合洞察生成失败: {e}")
            return {
                'success': False,
                'message': f'生成洞察过程中出现错误: {str(e)}',
                'insights': {}
            }


class CommunityDeepAnalyzer:
    """深度内容与社区讨论分析器"""

    def __init__(self, db_manager: DatabaseManager):
        """
        初始化深度分析器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        self.max_workers = config.get_max_workers()
        logger.info(f"深度内容与社区讨论分析器初始化完成 - 最大并发数: {self.max_workers}")

    def analyze_single_article_deeply(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        对单篇文章进行深度解析
        
        Args:
            article: 文章数据
            
        Returns:
            分析结果字典，失败返回None
        """
        try:
            # 构建分析prompt
            prompt = self._build_single_article_prompt(article.get('full_content', ''))
            
            # 调用快速模型进行分析
            response = call_llm(
                prompt=prompt,
                model_type='fast',
                temperature=0.3
            )
            
            if not response or not response.get('success'):
                logger.error(f"LLM调用失败，文章ID: {article.get('id')}")
                return None
            
            # 解析JSON响应
            analysis_result = self._parse_analysis_result(response.get('content', ''))
            if not analysis_result:
                logger.error(f"解析分析结果失败，文章ID: {article.get('id')}")
                return None
            
            # 添加元数据
            analysis_result['article_id'] = article.get('id')
            analysis_result['article_title'] = article.get('title')
            analysis_result['source_table'] = article.get('source_table')
            analysis_result['analyzed_at'] = datetime.now().isoformat()
            
            logger.info(f"文章深度分析完成，ID: {article.get('id')}, 类型: {analysis_result.get('factual_layer', {}).get('article_type', 'unknown')}")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"文章深度分析失败，ID: {article.get('id')}, 错误: {e}")
            return None

    def _build_single_article_prompt(self, content: str) -> str:
        """构建单篇文章分析的prompt"""
        return f"""你是一位顶尖的独立开发者社区分析师和创业导师。你的任务是深入分析给定的文章，并以结构化的JSON格式，提炼出其中所有有价值的信息。

请严格按照"事实层"、"观察层"、"思考层"三个维度进行分析，并遵循最终的JSON输出格式。

**分析维度指南:**

1.  **事实层 (Factual Layer)**: 客观地提取文章明确提到的信息。
    *   `article_type`: 判断文章类型，从 ['经验分享', '案例研究', '技术教程', '观点讨论', '产品发布', '问答'] 中选择一个最贴切的。
    *   `summary`: 用2-3句话总结文章的核心内容。
    *   `key_entities`: 提取文章中提到的关键实体，如产品名、公司名、技术栈、关键人物等。

2.  **观察层 (Observational Layer)**: 提炼作者的核心观点和可直接复用的信息。
    *   `core_insights`: 总结作者的核心洞察或主要论点，以列表形式呈现。
    *   `actionable_playbook`: 提炼出具体的、可操作的步骤或策略。如果没有，则返回空数组。
    *   `quantitative_results`: 提取所有能量化的结果，如"月收入达到$10,000"、"用户增长50%"、"转化率从1%提升到5%"等。

3.  **思考层 (Deeper Analysis Layer)**: 进行批判性思考和延伸分析。
    *   `underlying_reason`: 分析作者成功的潜在原因或其观点背后的深层逻辑是什么？
    *   `limitations_and_caveats`: 这些经验或观点有什么局限性、适用前提或潜在风险？
    *   `sparks_of_inspiration`: 这篇文章最能激发思考或带来启发的一点是什么？

**输入文章:**
'''
{content}
'''

**你的输出必须是一个单一、有效的JSON对象**，并严格遵循以下结构。所有内容值都必须是**中文**：
{{
  "factual_layer": {{
    "article_type": "经验分享",
    "summary": "文章的核心内容摘要。",
    "key_entities": ["产品名", "技术栈", "人物A"]
  }},
  "observational_layer": {{
    "core_insights": [
      "核心洞察或论点一。",
      "核心洞点或论点二。"
    ],
    "actionable_playbook": [
      "第一步：做什么。",
      "第二步：做什么。"
    ],
    "quantitative_results": [
      "月收入达到 $XXXX",
      "用户数从 Y 增长到 Z"
    ]
  }},
  "deeper_analysis_layer": {{
    "underlying_reason": "作者成功的关键可能在于其独特的市场切入点，而非仅仅是营销技巧。",
    "limitations_and_caveats": "此方法高度依赖于作者的个人品牌，对于没有粉丝基础的初学者可能不适用。",
    "sparks_of_inspiration": "将一个看似饱和的市场进行垂直细分，仍然能找到蓝海机会。"
  }}
}}"""

    def _parse_analysis_result(self, response: str) -> Optional[Dict[str, Any]]:
        """解析LLM返回的分析结果"""
        try:
            # 尝试直接解析JSON
            import json
            import re
            
            # 清理响应文本，提取JSON部分
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                return json.loads(json_str)
            else:
                logger.error("无法在响应中找到JSON格式的内容")
                return None
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            # 可以在这里添加更复杂的解析逻辑或容错机制
            return None
        except Exception as e:
            logger.error(f"解析分析结果时发生未知错误: {e}")
            return None

    def synthesize_weekly_insights(self, analyzed_articles: List[Dict[str, Any]], 
                                 start_date: str, end_date: str) -> Optional[str]:
        """
        对已分析的文章进行跨文章综合洞察
        
        Args:
            analyzed_articles: 已分析的文章列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            综合洞察报告的Markdown内容，失败返回None
        """
        try:
            if not analyzed_articles:
                logger.warning("没有已分析的文章，无法生成综合洞察")
                return None
            
            # 构建综合分析prompt
            prompt = self._build_synthesis_prompt(analyzed_articles, start_date, end_date)
            
            # 调用智能模型进行综合分析
            response = call_llm(
                prompt=prompt,
                model_type='smart',
                temperature=0.7
            )
            
            if not response or not response.get('success'):
                logger.error("综合洞察LLM调用失败")
                return None
            
            logger.info(f"综合洞察生成成功，分析了 {len(analyzed_articles)} 篇文章")
            return response.get('content', '')
            
        except Exception as e:
            logger.error(f"生成综合洞察失败: {e}")
            return None

    def _build_synthesis_prompt(self, analyzed_articles: List[Dict[str, Any]], 
                              start_date: str, end_date: str) -> str:
        """构建综合洞察的prompt"""
        
        # 准备分析数据
        analysis_data = []
        for article in analyzed_articles:
            analysis_data.append({
                "article_id": article.get('id'),
                "factual_layer": json.loads(article.get('deep_analysis_data', '{}')).get('factual_layer', {}),
                "observational_layer": json.loads(article.get('deep_analysis_data', '{}')).get('observational_layer', {}),
                "deeper_analysis_layer": json.loads(article.get('deep_analysis_data', '{}')).get('deeper_analysis_layer', {})
            })
        
        analysis_json = json.dumps(analysis_data, ensure_ascii=False, indent=2)
        
        return f"""你是一位卓越的行业分析师和编辑，擅长从大量结构化信息中发现趋势、总结模式并生成富有洞察的报告。

我现在提供给你过去一周从独立开发者社区收集的多篇文章的深度分析数据（一个JSON数组）。

**你的任务是:**

1.  **识别本周热点**: 统计`article_type`和`key_entities`，找出讨论最频繁的主题和产品/技术。
2.  **总结共性模式**: 在所有`actionable_playbook`和`underlying_reason`中，发现被反复提及的成功模式或策略。
3.  **发现矛盾与新奇观点**: 找出`core_insights`中相互矛盾或特别新颖的观点。
4.  **生成一份综合洞察周报**: 以清晰、易读的Markdown格式输出你的报告。

**输入数据 (JSON数组):**
```json
{analysis_json}
```

**请按照以下Markdown结构生成你的周报:**

# 独立开发者社区洞察周报 ({start_date} - {end_date})

## 🚀 本周热点速览

*   **热门主题**: [例如：本周讨论最多的主题是"早期用户获取"和"AI工具应用"。]
*   **焦点产品/技术**: [例如：社区对"AI Wrapper"、"Notion API"的讨论热度很高。]

## 🛠️ 本周策略风向标：发现共同的成功秘诀

*   **模式一**: [例如：多个"经验分享"类文章都强调了"先在垂直社区建立声誉，再推广产品"的策略。]
*   **模式二**: [例如：从多个案例的`underlying_reason`来看，"解决自己遇到的真实问题"是产品成功的首要前提。]

## 💡 观点碰撞：值得深思的讨论

*   **矛盾点**: [例如：关于"是否需要融资"，文章[105]认为独立开发者应保持精简，而文章[108]的案例则展示了小额融资加速发展的可能性。]
*   **新奇视角**: [例如：文章[110]提出了一个有趣的观点，认为"产品的'无聊'程度与商业成功率成正比"，这与社区普遍追求创新的风潮形成对比。]

## 📚 本周精选案例/经验速读

*   **案例 [文章ID]**: [这里可以引用某篇案例研究的`summary`和`quantitative_results`]
*   **经验 [文章ID]**: [这里可以引用某篇经验分享的`summary`和`actionable_playbook`中最核心的一条]

---
*报告基于对 {len(analyzed_articles)} 篇文章的分析生成。*"""

    def process_deep_analysis_batch(self, limit: int = 10) -> int:
        """
        批量处理深度分析任务（支持并行处理）
        
        Args:
            limit: 单次处理的文章数量限制
            
        Returns:
            成功处理的文章数量
        """
        try:
            # 获取待分析的文章
            articles = self.db_manager.get_articles_for_deep_analysis(limit=limit)
            
            if not articles:
                logger.info("没有待分析的文章")
                return 0
            
            logger.info(f"开始并行处理 {len(articles)} 篇文章 (并发数: {self.max_workers})")
            success_count = 0
            
            # 使用线程池进行并行处理
            import concurrent.futures
            import json
            
            def process_single_article(article):
                """处理单篇文章的内部函数"""
                try:
                    # 进行深度分析
                    analysis_result = self.analyze_single_article_deeply(article)
                    
                    if analysis_result:
                        # 保存分析结果
                        self.db_manager.update_deep_analysis_result(
                            table_name=article['source_table'],
                            article_id=article['id'],
                            analysis_data=json.dumps(analysis_result, ensure_ascii=False),
                            status=1  # 成功
                        )
                        return True, article['id'], "成功"
                    else:
                        # 标记为失败
                        self.db_manager.update_deep_analysis_result(
                            table_name=article['source_table'],
                            article_id=article['id'],
                            analysis_data="",
                            status=-1  # 失败
                        )
                        return False, article['id'], "分析失败"
                        
                except Exception as e:
                    logger.error(f"处理文章 {article.get('id')} 时发生错误: {e}")
                    # 标记为失败
                    try:
                        self.db_manager.update_deep_analysis_result(
                            table_name=article['source_table'],
                            article_id=article['id'],
                            analysis_data="",
                            status=-1  # 失败
                        )
                    except:
                        pass
                    return False, article['id'], f"异常: {str(e)}"
            
            # 使用ThreadPoolExecutor进行并行处理
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任务
                future_to_article = {
                    executor.submit(process_single_article, article): article 
                    for article in articles
                }
                
                # 处理完成的任务
                for future in concurrent.futures.as_completed(future_to_article):
                    article = future_to_article[future]
                    try:
                        success, article_id, message = future.result()
                        if success:
                            success_count += 1
                            logger.info(f"✅ 文章 {article_id} 处理成功 ({success_count}/{len(articles)})")
                        else:
                            logger.warning(f"❌ 文章 {article_id} 处理失败: {message}")
                    except Exception as e:
                        logger.error(f"获取文章 {article.get('id')} 处理结果失败: {e}")
            
            logger.info(f"并行批量深度分析完成，成功处理 {success_count}/{len(articles)} 篇文章")
            return success_count
            
        except Exception as e:
            logger.error(f"批量深度分析处理失败: {e}")
            return 0

    def generate_synthesis_report(self, days: int = 7, indiehackers_hours: int = None, 
                                ezindie_limit: int = None) -> Optional[int]:
        """
        生成综合洞察报告
        
        Args:
            days: 分析过去多少天的数据（默认筛选条件）
            indiehackers_hours: indiehackers 数据的小时限制（如：48小时）
            ezindie_limit: ezindie 数据的文章数量限制（如：最新1篇）
            
        Returns:
            报告ID，失败返回None
        """
        try:
            # 获取已分析的文章（使用新的筛选参数）
            analyzed_articles = self.db_manager.get_analyzed_articles_for_synthesis(
                days=days,
                indiehackers_hours=indiehackers_hours,
                ezindie_limit=ezindie_limit
            )
            
            if not analyzed_articles:
                logger.warning(f"没有符合条件的已分析文章，无法生成综合报告")
                logger.info(f"筛选条件: indiehackers={indiehackers_hours}小时, ezindie=最新{ezindie_limit}篇, 默认={days}天")
                return None
            
            # 按来源统计文章
            indiehackers_count = len([a for a in analyzed_articles if a['source_table'] == 'rss_indiehackers'])
            ezindie_count = len([a for a in analyzed_articles if a['source_table'] == 'rss_ezindie'])
            
            logger.info(f"准备生成综合报告: indiehackers {indiehackers_count}篇, ezindie {ezindie_count}篇")
            
            # 计算日期范围（基于实际数据的日期范围）
            from datetime import datetime, timedelta
            end_date = datetime.now().date()
            
            # 根据设置确定开始日期
            if indiehackers_hours:
                start_date = (datetime.now() - timedelta(hours=indiehackers_hours)).date()
            else:
                start_date = end_date - timedelta(days=days)
            
            # 生成综合洞察
            synthesis_content = self.synthesize_weekly_insights(
                analyzed_articles=analyzed_articles,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if not synthesis_content:
                logger.error("生成综合洞察内容失败")
                return None
            
            # 保存报告
            report_data = {
                'report_type': 'community_insights_custom',
                'start_date': start_date,
                'end_date': end_date,
                'content': synthesis_content,
                'source_article_ids': [article['id'] for article in analyzed_articles]
            }
            
            report_id = self.db_manager.save_synthesis_report(report_data)
            logger.info(f"自定义筛选综合洞察报告生成成功，报告ID: {report_id}")
            
            return report_id
            
        except Exception as e:
            logger.error(f"生成综合洞察报告失败: {e}")
            return None