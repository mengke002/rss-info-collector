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
from .llm_client import call_llm, get_report_model_names, LLMClient
from .notion_client import notion_client

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
            
            # 对于betalist，优先使用现有结构化数据，LLM只做补充
            if source_feed == 'betalist':
                # 尝试使用LLM提取增强信息（categories, tagline等）
                product_info = self.extract_product_info(content_text, source_feed)
                
                # 如果LLM提取失败，创建基础产品信息
                if not product_info:
                    logger.warning(f"LLM提取失败，为Betalist条目 {item.get('id')} 创建基础产品信息")
                    product_info = {
                        'product_name': None,
                        'tagline': None,
                        'description': None,
                        'product_url': None,
                        'categories': None,
                        'metrics': {
                            'problem_solved': None,
                            'target_audience': None,
                            'tech_stack': None,
                            'business_model': None
                        },
                        'source_feed': source_feed
                    }
                
                # 使用数据库可靠信息覆盖/补充LLM结果
                # 确保关键信息不会因LLM失败而丢失
                if item.get('title'):
                    product_info['product_name'] = item.get('title')  # 产品名称以数据库为准
                if item.get('visit_url'):
                    product_info['product_url'] = item.get('visit_url')  # URL以数据库为准
                if item.get('summary') and not product_info.get('description'):
                    product_info['description'] = item.get('summary')  # 描述补充
                
                # 添加时间信息
                product_info['source_published_at'] = item.get('published_at')
                product_info['source_feed'] = source_feed
                
                logger.debug(f"Betalist条目 {item.get('id')} 处理完成 - LLM增强+数据库保底，URL: {product_info.get('product_url')}")
                
            else:
                # 其他情况使用LLM提取产品信息
                product_info = self.extract_product_info(content_text, source_feed)
                
                if product_info:
                    # 添加时间信息
                    product_info['source_published_at'] = item.get('published_at')

            if product_info:
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

    @staticmethod
    def _sanitize_model_reports(model_reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """根据配置决定是否返回包含正文的模型报告"""
        if config.should_log_report_preview():
            return model_reports

        sanitized: List[Dict[str, Any]] = []
        for report in model_reports or []:
            if isinstance(report, dict):
                sanitized.append({k: v for k, v in report.items() if k != 'content'})
        return sanitized

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
                    
                    # 验证分析结果的完整性 - 新的JSON结构
                    required_fields = ['summary', 'key_points', 'event_type', 'potential_impact']
                    if all(field in analysis_result for field in required_fields):
                        # 结果完整且有效，直接使用
                        analysis_result['article_id'] = article_id
                        analysis_result['source_feed'] = source_feed
                        
                        logger.debug(f"使用已缓存的完整分析结果 (文章ID: {article_id})")
                        return analysis_result
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

1. **核心摘要**: 用200字左右，提供包含文章核心信息的中立、精炼的中文摘要。
2. **关键信息点**: 识别并列出文章中的核心实体、数据或概念，例如公司、产品、人物、融资金额、技术术语、关键数据点等。
3. **事件/主题分类**: 判断文章的核心事件或主题类型。
4. **潜在影响评估**: 简要评估此事件可能带来的潜在影响。

**输入文章:**
{article_content}

**你的输出必须是一个单一、有效的JSON对象**，并严格遵循以下结构。所有内容值都必须是**中文**：
{{
  "title": "{article.get('title', '')}",
  "link": "{article.get('link', '')}",
  "source": "{article.get('source_feed', '')}",
  "published_at": "{article.get('published_at', '')}",
  "summary": "一段200字左右的中文文章摘要。",
  "key_points": [
    "关键信息点一",
    "关键信息点二",
    "..."
  ],
  "event_type": "从['产品发布', '公司战略', '技术突破', '市场动态', '融资并购', '安全事件', '行业观点']中选择一个",
  "potential_impact": "对该事件潜在影响的简要说明 (50字以内)。"
}}
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
                # 验证新的JSON结构
                required_fields = ['summary', 'key_points', 'event_type', 'potential_impact']
                if all(field in analysis_result for field in required_fields):
                    # 添加文章元数据
                    analysis_result['article_id'] = article.get('id')
                    analysis_result['source_feed'] = article.get('source_feed')
                    
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
                    logger.warning(f"分析结果缺少必要字段 (文章ID: {article.get('id')})")
                    return None
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
                'key_points': analysis_result.get('key_points'),
                'event_type': analysis_result.get('event_type'),
                'potential_impact': analysis_result.get('potential_impact'),
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
        运行完整的科技新闻分析流程（基于tech_news_report_plan.md优化的两层架构）
        
        Args:
            hours_back: 分析过去多少小时的新闻，默认24小时
            
        Returns:
            包含完整Markdown报告的结果
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
                    'full_report': None
                }
            
            # 2. 第一层：并行调用 analyze_single_article，得到结构化数据列表
            analysis_results = self.batch_analyze_articles(articles)
            
            if not analysis_results:
                logger.warning("文章分析未产生有效结果")
                return {
                    'success': False,
                    'message': '文章分析未产生有效结果',
                    'full_report': None
                }
            
            # 3. 第二层：调用新的报告生成方法，生成完整的Markdown报告
            report_generation = self.generate_full_report(analysis_results, hours_back)

            raw_model_reports = report_generation.get('model_reports', [])
            sanitized_model_reports = self._sanitize_model_reports(raw_model_reports)

            if not report_generation.get('success'):
                logger.warning(
                    "生成完整报告失败: %s",
                    report_generation.get('error', '未知原因')
                )
                return {
                    'success': False,
                    'message': report_generation.get('error', '生成完整报告失败'),
                    'full_report': None,
                    'model_reports': sanitized_model_reports,
                    'model_reports_full': raw_model_reports,
                    'failures': report_generation.get('failures', [])
                }

            model_reports = raw_model_reports
            if not model_reports:
                logger.warning("所有模型生成完整报告失败")
                return {
                    'success': False,
                    'message': '所有模型生成完整报告失败',
                    'full_report': None,
                    'model_reports': [],
                    'model_reports_full': [],
                    'failures': report_generation.get('failures', [])
                }

            primary_report = model_reports[0]

            sanitized_model_reports = self._sanitize_model_reports(model_reports)

            # 4. 构建最终结果
            final_result = {
                'success': True,
                'analysis_period': f'过去{hours_back}小时',
                'total_articles_found': len(articles),
                'successful_analysis_count': len(analysis_results),
                'full_report': primary_report.get('content') if config.should_log_report_preview() else None,
                'model_reports': sanitized_model_reports,
                'model_reports_full': model_reports,
                'failures': report_generation.get('failures', []),
                'generated_at': datetime.now().isoformat()
            }
            
            logger.info(
                "科技新闻分析完成 - 分析 %s 篇文章，成功 %s 篇，生成 %s 份完整报告",
                len(articles),
                len(analysis_results),
                len(model_reports)
            )
            return final_result
            
        except Exception as e:
            logger.error(f"科技新闻分析失败: {e}")
            return {
                'success': False,
                'message': f'分析过程中出现错误: {str(e)}',
                'full_report': None
            }

    def _resolve_report_models(self) -> List[Dict[str, str]]:
        """获取执行最终报告生成时需要使用的模型信息"""
        resolved_models: List[Dict[str, str]] = []
        configured_models = get_report_model_names()

        if not configured_models:
            try:
                fallback = config.get_llm_config().get('smart_model_name')
                if fallback:
                    configured_models = [fallback]
            except Exception as exc:
                logger.warning(f"读取LLM配置失败: {exc}")

        for model_name in configured_models:
            display_name = LLMClient.get_model_display_name(model_name)
            if not any(item['model'] == model_name for item in resolved_models):
                resolved_models.append({'model': model_name, 'display': display_name})

        return resolved_models

    def generate_full_report(self, analysis_results: List[Dict[str, Any]], hours_back: int = 24) -> Dict[str, Any]:
        """并行调用多个模型生成完整报告"""
        if not analysis_results:
            logger.warning("没有分析结果，无法生成报告")
            return {
                'success': False,
                'error': '没有分析结果',
                'model_reports': [],
                'failures': []
            }

        try:
            structured_data = [{
                "title": result.get('title', ''),
                "link": result.get('link', ''),
                "source": result.get('source', ''),
                "summary": result.get('summary', ''),
                "key_points": result.get('key_points', []),
                "event_type": result.get('event_type', ''),
                "potential_impact": result.get('potential_impact', '')
            } for result in analysis_results]

            from datetime import datetime
            current_date = datetime.now().strftime('%Y-%m-%d')

            prompt = f"""
你是一位资深的科技行业分析师和报告撰写专家，任职于顶尖的分析机构。你的任务是基于提供的一系列科技新闻的结构化信息，撰写一份全面、深入、结构清晰的洞察报告。

报告需要遵循"由浅入深，由事实到洞察"的原则，整合所有信息，最终输出一份完整的Markdown文档。

[输入数据]
你将收到一个JSON数组，其中包含过去{hours_back}小时内多篇科技新闻的核心信息。格式如下：

{json.dumps(structured_data, ensure_ascii=False, indent=2)}

[你的任务]
请严格按照以下Markdown结构和要求，生成你的分析报告。

# 科技新闻洞察报告 ({current_date})

> 核心提要: (在这里写一段高度浓缩的、吸引人的导语，约200字。点明本次报告期内最重要的趋势、最值得关注的事件，并抛出核心观点。例如："本期科技界风起云涌，AI领域的军备竞赛进入新阶段，而资本市场则对XX赛道展现出前所未有的热情。本报告将为您深度解读这些表象之下的战略意图与未来机遇。")

## 一、关键新闻速览 (Facts First)

(此部分汇总所有输入文章的核心事实，以清晰的列表或表格呈现，让读者快速了解发生了什么。)

### 1.1 产品与发布
 * **[产品/公司A]** - 摘要内容。 [来源](链接)
 * **[产品/公司B]** - 摘要内容。 [来源](链接)

### 1.2 资本与市场
 * **[公司C]** - 摘要内容。 [来源](链接)

### 1.3 技术与趋势
 * **[技术D]** - 摘要内容。 [来源](链接)

(请根据输入数据的event_type对文章进行分类，如果某个分类下没有文章，则不显示该标题。)

## 二、趋势与模式分析 (Connecting the Dots)

(此部分是分析的中间层，需要你连接不同新闻之间的点，发现其中的模式和趋势。)

 * **热点聚焦**: (分析本期新闻中出现频率最高的key_points，识别出当前市场的热点。例如："'多模态大模型'和'端侧AI'成为本期最热门的关键词，在多篇文章中被反复提及，显示出业界对下一代AI形态的集体探索。")
 * **模式识别**: (观察不同event_type之间的关联。例如："我们观察到，在'技术突破'类新闻发布后，紧接着出现了相关的'融资并购'事件，这表明技术创新正被资本市场快速验证和吸收。")
 * **信号解读**: (发现一些值得注意的微弱信号。例如："尽管主流讨论集中在大型科技公司，但来自某个小众来源的一篇文章揭示了一个新兴的、可能被市场忽略的细分赛道。")

## 三、深度洞察与解读 (The "So What?")

(这是报告的核心，需要你提供最深刻的洞察，回答"So What?"和"What's Next?"。)

### 3.1 对开发者的影响
(例如："对于开发者而言，XX技术的成熟意味着新的工具链和开发范式即将到来，现在是学习和掌握这些技能的最佳时机...")

### 3.2 对投资者的启示
(例如："XX领域的投资窗口依然敞开，但竞争格局已趋于激烈。我们的分析表明，成功的关键在于找到能够与现有生态系统深度结合的差异化应用，而非底层技术的重复构建...")

### 3.3 对行业格局的预判
(例如："基于本期的数据，我们预测未来6个月内，XX行业将出现一波整合浪潮。领先企业可能会通过收购来弥补其技术短板，而小型创新公司则面临站队或被淘汰的压力...")

---
报告基于对 {len(structured_data)} 篇文章的分析生成。

请确保报告内容具有前瞻性、洞察性，避免简单的事实罗列，要有深度思考和独到见解。
"""

            models_meta = self._resolve_report_models()
            if not models_meta:
                error_msg = '未找到可用的报告模型'
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg,
                    'model_reports': [],
                    'failures': []
                }

            logger.info(
                "准备并行生成科技新闻报告，模型列表: %s",
                [meta['display'] for meta in models_meta]
            )

            successes: Dict[int, Dict[str, Any]] = {}
            failures: List[Dict[str, Any]] = []

            def _run_single_model(index: int, model_name: str, display_name: str) -> Dict[str, Any]:
                try:
                    llm_temperature = 0.5
                    response = call_llm(
                        prompt,
                        model_type='smart',
                        temperature=llm_temperature,
                        model_override=model_name
                    )

                    if not response.get('success'):
                        return {
                            'success': False,
                            'error': response.get('error', 'LLM调用失败'),
                            'model': model_name,
                            'model_display': display_name
                        }

                    full_report = (response.get('content') or '').strip()
                    if not full_report:
                        return {
                            'success': False,
                            'error': 'LLM返回空内容',
                            'model': model_name,
                            'model_display': display_name
                        }

                    return {
                        'success': True,
                        'model': model_name,
                        'model_display': display_name,
                        'content': full_report,
                        'provider': response.get('provider', 'openai_compatible'),
                        'temperature': llm_temperature,
                        'prompt_length': len(prompt)
                    }
                except Exception as exc:
                    return {
                        'success': False,
                        'error': str(exc),
                        'model': model_name,
                        'model_display': display_name
                    }

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(models_meta), 4) or 1) as executor:
                future_map = {}
                for idx, meta in enumerate(models_meta):
                    future = executor.submit(_run_single_model, idx, meta['model'], meta['display'])
                    future_map[future] = (idx, meta)

                for future in concurrent.futures.as_completed(future_map):
                    idx, meta = future_map[future]
                    try:
                        model_result = future.result()
                        if model_result.get('success'):
                            successes[idx] = model_result
                            logger.info(
                                "模型 %s 报告生成完成 (长度: %s)",
                                meta['display'],
                                len(model_result.get('content', ''))
                            )
                        else:
                            error_msg = model_result.get('error', '报告生成失败')
                            logger.warning(
                                "模型 %s 报告生成失败: %s",
                                meta['display'],
                                error_msg
                            )
                            failures.append({
                                'model': meta['model'],
                                'model_display': meta['display'],
                                'error': error_msg
                            })
                    except Exception as exc:
                        logger.error(
                            "模型 %s 报告生成出现未处理异常: %s",
                            meta['display'],
                            exc
                        )
                        failures.append({
                            'model': meta['model'],
                            'model_display': meta['display'],
                            'error': str(exc)
                        })

            ordered_successes = [successes[idx] for idx in sorted(successes.keys())]
            overall_success = len(ordered_successes) > 0

            return {
                'success': overall_success,
                'model_reports': ordered_successes,
                'failures': failures,
                'prompt_length': len(prompt),
                'model_count_requested': len(models_meta)
            }

        except Exception as e:
            logger.error(f"生成完整报告时出现异常: {e}")
            return {
                'success': False,
                'error': str(e),
                'model_reports': [],
                'failures': []
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
                    elif info and isinstance(info, dict) and 'info' in info:
                        # 处理字典格式的关键信息（向后兼容）
                        clean_info = info.get('info', '').strip()
                        if len(clean_info) > 2:
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
            # 获取原始分析结果以提取更丰富的内容
            analysis_results = getattr(self, '_current_analysis_results', [])
            
            # 构建给LLM的上下文数据
            context_data = {
                'time_period': time_period,
                'total_articles': statistics.get('total_articles', 0),
                'top_topics': [item['topic'] for item in topic_analysis.get('topic_distribution', [])[:5]],
                'hot_entities': [item['entity'] for item in key_info_analysis.get('hot_entities', [])[:5]],
                'trending_topics': [item['entity'] for item in key_info_analysis.get('trending_topics', [])[:3]],
                'source_insights': source_analysis.get('cross_source_insights', [])
            }
            
            # 从原始分析结果中提取文章内容摘要，按文章合并结构体
            article_contents = []
            
            for result in analysis_results:  # 不限制文章数量
                article_data = {
                    'title': result.get('article_title', '未知标题'),
                    'source': result.get('source_feed', '未知来源'),
                    'summary': result.get('summary', ''),
                    'key_info': [],
                    'primary_tag': '',
                    'secondary_tags': []
                }
                
                # 处理关键信息
                if result.get('key_info'):
                    for info in result.get('key_info', []):
                        if isinstance(info, str):
                            # 字符串格式的关键信息
                            article_data['key_info'].append(info)
                        elif isinstance(info, dict) and 'info' in info:
                            # 字典格式的关键信息（向后兼容）
                            article_data['key_info'].append(info.get('info', ''))
                
                # 处理标签
                if result.get('tags'):
                    tags = result.get('tags', {})
                    article_data['primary_tag'] = tags.get('primary_tag', '未知')
                    article_data['secondary_tags'] = tags.get('secondary_tags', [])
                
                # 只有当文章有有效内容时才添加
                if article_data['summary'] or article_data['key_info']:
                    article_contents.append(article_data)
            
            # 将合并后的文章内容添加到上下文
            context_data['article_contents'] = article_contents

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

2. **文章详细内容**（共{len(context_data['article_contents'])}篇）:
"""
            
            # 添加合并后的文章内容
            for i, article in enumerate(context_data['article_contents'], 1):
                key_info_str = '; '.join(article['key_info']) if article['key_info'] else '无'
                secondary_tags_str = ', '.join(article['secondary_tags']) if article['secondary_tags'] else '无'
                
                prompt += f"""   {i}. [{article['source']}] {article['title']}
      摘要: {article['summary']}
      关键信息: {key_info_str}
      标签: {article['primary_tag']} ({secondary_tags_str})

"""
            
            prompt += f"""

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
                insights_data = None
                
                try:
                    # 方案1：找到JSON代码块
                    json_match = re.search(r'```(?:json)?\s*({{.*?}})\s*```', content, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                        insights_data = json.loads(json_str)
                    else:
                        # 方案2：如果找不到代码块，尝试从内容中提取最外层的大括号
                        start_index = content.find('{')
                        end_index = content.rfind('}')
                        if start_index != -1 and end_index != -1:
                            json_str = content[start_index:end_index+1]
                            insights_data = json.loads(json_str)
                        else:
                            # 方案3：直接解析整个内容作为最后的尝试
                            insights_data = json.loads(content)
                    
                    insights_data['generated_by'] = 'smart_model'
                    insights_data['confidence_score'] = 0.85  # 默认置信度
                    
                    logger.info(f"成功生成深度洞察")
                    return insights_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"无法解析LLM返回的JSON: {e}。尝试使用正则表达式托底方案。")
                    insights_data = self._extract_insights_with_regex(content)
                    if insights_data:
                        insights_data['generated_by'] = 'regex_fallback'
                        insights_data['confidence_score'] = 0.5 
                        return insights_data
                    
                    logger.warning("正则表达式托底方案也失败了，将使用最终的模板托底方案。")
                    # 返回结构化的备用数据
                    return self._generate_fallback_insights(context_data)
            else:
                logger.warning(f"LLM调用失败: {response.get('error', 'Unknown error')}")
                return self._generate_fallback_insights(context_data)
                
        except Exception as e:
            logger.error(f"深度洞察生成失败: {e}")
            return self._generate_fallback_insights(context_data)

    def _extract_insights_with_regex(self, content: str) -> Optional[Dict[str, Any]]:
        """
        使用正则表达式从文本中提取深度洞察信息，作为JSON解析失败的托底方案。
        
        Args:
            content: LLM返回的原始字符串
            
        Returns:
            提取的洞察字典，如果失败则返回None
        """
        import re
        try:
            patterns = {
                "analyst_take": r'\\"analyst_take\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "for_developers": r'\\"for_developers\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "for_investors": r'\\"for_investors\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "for_competitors": r'\\"for_competitors\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "opportunity": r'\\"opportunity\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "risk": r'\\"risk\\"\\s*:\\s*\\"([^\\\"]+)\\"',
                "prediction": r'\\"prediction\\"\\s*:\\s*\\"([^\\\"]+)\\"'
            }
            
            # 备用模式，处理没有转义引号的情况
            alt_patterns = {
                "analyst_take": r'"analyst_take"\s*:\s*"([^"]+)"',
                "for_developers": r'"for_developers"\s*:\s*"([^"]+)"',
                "for_investors": r'"for_investors"\s*:\s*"([^"]+)"',
                "for_competitors": r'"for_competitors"\s*:\s*"([^"]+)"',
                "opportunity": r'"opportunity"\s*:\s*"([^"]+)"',
                "risk": r'"risk"\s*:\s*"([^"]+)"',
                "prediction": r'"prediction"\s*:\s*"([^"]+)"'
            }

            extracted_data = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, content, re.DOTALL)
                if not match:
                    # 如果带转义的模式找不到，尝试不带转义的
                    match = re.search(alt_patterns[key], content, re.DOTALL)
                extracted_data[key] = match.group(1).strip() if match else None

            # 如果一个字段都提取不到，则认为失败
            if not any(extracted_data.values()):
                logger.warning("正则托底方案未能从文本中提取任何有效字段。")
                return None

            # 重建嵌套结构
            insights = {
                'analyst_take': extracted_data.get('analyst_take'),
                'key_impacts': {
                    'for_developers': extracted_data.get('for_developers'),
                    'for_investors': extracted_data.get('for_investors'),
                    'for_competitors': extracted_data.get('for_competitors')
                },
                'opportunity_and_risk': {
                    'opportunity': extracted_data.get('opportunity'),
                    'risk': extracted_data.get('risk')
                },
                'prediction': extracted_data.get('prediction')
            }
            
            logger.info("成功通过正则表达式托底方案提取了部分或全部洞察信息。")
            return insights
            
        except Exception as e:
            logger.error(f"正则表达式托底方案在执行时发生异常: {e}")
            return None
    
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
            # 保存原始分析结果，供_generate_deep_insights使用
            self._current_analysis_results = analysis_results
            
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

    def _resolve_report_models(self) -> List[Dict[str, str]]:
        """获取用于综合洞察报告生成的模型列表"""
        resolved_models: List[Dict[str, str]] = []
        configured_models = get_report_model_names()

        if not configured_models:
            try:
                fallback = config.get_llm_config().get('smart_model_name')
                if fallback:
                    configured_models = [fallback]
            except Exception as exc:
                logger.warning(f"读取LLM配置失败: {exc}")

        for model_name in configured_models:
            display_name = LLMClient.get_model_display_name(model_name)
            if not any(item['model'] == model_name for item in resolved_models):
                resolved_models.append({'model': model_name, 'display': display_name})

        return resolved_models

    def _build_info_summary_section(self, analyzed_articles: List[Dict[str, Any]]) -> str:
        """构建资讯速览Markdown片段"""
        info_summary_md = "## 📰 本周资讯速览\n\n"

        if not analyzed_articles:
            info_summary_md += "- 暂无资讯\n"
            return info_summary_md

        for article in analyzed_articles:
            title = article.get('title', '无标题')
            link = article.get('link', '#')
            source_table = article.get('source_table', 'unknown').replace('rss_', '')
            info_summary_md += f"* **[{source_table}]** [{title}]({link})\n"

        return info_summary_md

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
    *   `summary`: 用200字左右总结文章的核心内容。
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

            # --- 步骤1: 预生成资讯速览 ---
            info_summary_md = self._build_info_summary_section(analyzed_articles)
            # --- 预生成结束 ---

            # 构建综合分析prompt
            prompt = self._build_synthesis_prompt(
                analyzed_articles=analyzed_articles, 
                start_date=start_date, 
                end_date=end_date,
                info_summary_md=info_summary_md
            )
            
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
                              start_date: str, end_date: str, info_summary_md: str) -> str:
        """构建综合洞察的prompt"""
        
        # 准备分析数据
        analysis_data = []
        for article in analyzed_articles:
            analysis_data.append({
                "article_id": article.get('id'),
                "article_link": article.get('link'), # 新增字段
                "factual_layer": json.loads(article.get('deep_analysis_data', '{}')).get('factual_layer', {}),
                "observational_layer": json.loads(article.get('deep_analysis_data', '{}')).get('observational_layer', {}),
                "deeper_analysis_layer": json.loads(article.get('deep_analysis_data', '{}')).get('deeper_analysis_layer', {})
            })
        
        analysis_json = json.dumps(analysis_data, ensure_ascii=False, indent=2)
        
        return f"""你是一位卓越的行业分析师和编辑，擅长从大量结构化信息中发现趋势、总结模式并生成富有洞察的报告。

我现在提供给你过去一周从独立开发者社区收集的多篇文章的深度分析数据（一个JSON数组）。

**你的核心任务是:**

1.  **识别热点**: 统计`article_type`和`key_entities`，找出讨论最频繁的主题和产品/技术。
2.  **总结模式**: 在所有`actionable_playbook`和`underlying_reason`中，发现被反复提及的成功模式或策略。
3.  **发现矛盾**: 找出`core_insights`中相互矛盾或特别新颖的观点。
4.  **深度解读**: 对比不同案例的成败，进行更深刻的解读。
5.  **生成周报**: 以清晰、详尽、易读的Markdown格式输出你的报告。

**输入数据 (JSON数组):**
```json
{analysis_json}
```

**请严格按照以下Markdown结构和要求生成你的周报，确保内容丰富、有深度，避免过于简略:**

# 独立开发者社区洞察周报 ({start_date} - {end_date})

## 🚀 本周热点速览

*   **热门主题**: [在这里详细阐述本周讨论最多的2-3个主题，例如:本周讨论最多的主题是"AI工具应用"和"早期用户获取"。前者集中在如何利用最新的AI API创造价值，后者则更多地讨论了冷启动阶段的各种实战技巧。]
*   **焦点产品/技术**: [在这里详细阐述社区对哪些产品或技术的讨论热度很高，并说明原因。例如:社区对"Notion API"的讨论热度很高，因为它为开发者提供了一个成熟的生态来构建各种效率工具。]

## 🛠️ 本周策略风向标：发现共同的成功秘诀

(请深入分析`actionable_playbook`和`underlying_reason`，总结出至少3-4个被反复提及的、有价值的共性模式或策略。对于每一点，都需要进行详细的阐述，并引用相关的文章ID作为论据支撑。)

*   **模式一: [模式标题]**
    [对该模式进行详细描述，说明它为什么重要，以及它是如何在不同案例中体现的。引用案例时，请使用格式 `[文章ID](链接)`，例如:多篇文章 ([488](link_to_488), [472](link_to_472)) 都强调了... ]

*   **模式二: [模式标题]**
    [对该模式进行详细描述，说明它为什么重要，以及它是如何在不同案例中体现的。引用案例时，请使用格式 `[文章ID](链接)`]

*   **模式三: [模式标题]**
    [对该模式进行详细描述，说明它为什么重要，以及它是如何在不同案例中体现的。引用案例时，请使用格式 `[文章ID](链接)`]

## 💡 观点碰撞：值得深思的讨论

(请深入分析`core_insights`，找出至少2-3个有趣或矛盾的观点进行对比和解读，并引用相关的文章ID作为论据支撑。)

*   **[矛盾点/新奇视角一]**: [详细描述这个矛盾点或新奇视角，并分析其背后的原因和价值。引用案例时，请使用格式 `[文章ID](链接)`，例如:关于"是否需要融资"，[文章105](link_to_105)认为...，而[文章108](link_to_108)的案例则展示了...]

*   **[矛盾点/新奇视角二]**: [详细描述这个矛盾点或新奇视角，并分析其背后的原因和价值。引用案例时，请使用格式 `[文章ID](链接)`]


## 📚 本周精选案例深度解读

(这是报告的核心部分。请从所有分析过的文章中，挑选2-3个最具有启发性的案例进行详细的、多段落的深度解读。每个案例的解读都应包含以下几个方面，确保内容详实、有洞见。)

### 案例一：[案例标题] - [来源文章ID](链接)

*   **案例简介**: [简要介绍这个产品/项目是做什么的，解决了什么问题。引用原文的`summary`。]
*   **核心策略/亮点**: [详细分析这个案例最核心的打法、策略或亮点是什么。结合`observational_layer`中的`core_insights`和`actionable_playbook`进行阐述。]
*   **量化结果与启发**: [列出案例中提到的具体`quantitative_results`，并分析这些结果能给我们带来什么启发或思考。]
*   **潜在价值与局限性**: [基于`deeper_analysis_layer`，分析这个案例的成功经验有哪些前提条件或局限性，以及它可能带来的更深层次的行业机会或风险。]

### 案例二：[案例标题] - [来源文章ID](链接)

*   **案例简介**: [简要介绍这个产品/项目是做什么的，解决了什么问题。引用原文的`summary`。]
*   **核心策略/亮点**: [详细分析这个案例最核心的打法、策略或亮点是什么。结合`observational_layer`中的`core_insights`和`actionable_playbook`进行阐述。]
*   **量化结果与启发**: [列出案例中提到的具体`quantitative_results`，并分析这些结果能给我们带来什么启发或思考。]
*   **潜在价值与局限性**: [基于`deeper_analysis_layer`，分析这个案例的成功经验有哪些前提条件或局限性，以及它可能带来的更深层次的行业机会或风险。]

## 📰 本周资讯速览

{info_summary_md}

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
                                ezindie_limit: int = None) -> Dict[str, Any]:
        """生成社区综合洞察报告，支持多模型并行生成"""
        try:
            analyzed_articles = self.db_manager.get_analyzed_articles_for_synthesis(
                days=days,
                indiehackers_hours=indiehackers_hours,
                ezindie_limit=ezindie_limit
            )

            if not analyzed_articles:
                logger.warning("没有符合条件的已分析文章，无法生成综合报告")
                logger.info(
                    "筛选条件: indiehackers=%s小时, ezindie=最新%s篇, 默认=%s天",
                    indiehackers_hours,
                    ezindie_limit,
                    days
                )
                return {
                    'success': False,
                    'reports': [],
                    'failures': [],
                    'message': '没有符合条件的已分析文章'
                }

            indiehackers_count = len([a for a in analyzed_articles if a['source_table'] == 'rss_indiehackers'])
            ezindie_count = len([a for a in analyzed_articles if a['source_table'] == 'rss_ezindie'])

            logger.info(
                "准备生成综合报告: indiehackers %s 篇, ezindie %s 篇, 总计 %s 篇",
                indiehackers_count,
                ezindie_count,
                len(analyzed_articles)
            )

            from datetime import datetime, timedelta
            end_date = datetime.now().date()

            if indiehackers_hours:
                start_date = (datetime.now() - timedelta(hours=indiehackers_hours)).date()
            else:
                start_date = end_date - timedelta(days=days)

            info_summary_md = self._build_info_summary_section(analyzed_articles)
            prompt = self._build_synthesis_prompt(
                analyzed_articles=analyzed_articles,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                info_summary_md=info_summary_md
            )

            models_meta = self._resolve_report_models()
            if not models_meta:
                error_msg = '未找到可用的报告模型'
                logger.error(error_msg)
                return {
                    'success': False,
                    'reports': [],
                    'failures': [{'error': error_msg}],
                    'message': error_msg
                }

            logger.info(
                "开始并行生成社区综合报告，模型: %s",
                [meta['display'] for meta in models_meta]
            )

            include_preview = config.should_log_report_preview()
            successes: Dict[int, Dict[str, Any]] = {}
            failures: List[Dict[str, Any]] = []

            def _run_single_model(index: int, model_name: str, display_name: str) -> Dict[str, Any]:
                try:
                    temperature = 0.7
                    response = call_llm(
                        prompt,
                        model_type='smart',
                        temperature=temperature,
                        model_override=model_name
                    )

                    if not response.get('success'):
                        return {
                            'success': False,
                            'error': response.get('error', 'LLM调用失败'),
                            'model': model_name,
                            'model_display': display_name
                        }

                    content = (response.get('content') or '').strip()
                    if not content:
                        return {
                            'success': False,
                            'error': 'LLM返回空内容',
                            'model': model_name,
                            'model_display': display_name
                        }

                    return {
                        'success': True,
                        'model': model_name,
                        'model_display': display_name,
                        'content': content,
                        'provider': response.get('provider', 'openai_compatible'),
                        'temperature': temperature
                    }
                except Exception as exc:
                    return {
                        'success': False,
                        'error': str(exc),
                        'model': model_name,
                        'model_display': display_name
                    }

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(models_meta), 4) or 1) as executor:
                future_map = {}
                for idx, meta in enumerate(models_meta):
                    future = executor.submit(_run_single_model, idx, meta['model'], meta['display'])
                    future_map[future] = (idx, meta)

                for future in concurrent.futures.as_completed(future_map):
                    idx, meta = future_map[future]
                    try:
                        result = future.result()
                        if result.get('success'):
                            successes[idx] = result
                            logger.info(
                                "模型 %s 生成综合报告成功，内容长度 %s",
                                meta['display'],
                                len(result.get('content', ''))
                            )
                        else:
                            error_msg = result.get('error', '报告生成失败')
                            logger.warning(
                                "模型 %s 生成综合报告失败: %s",
                                meta['display'],
                                error_msg
                            )
                            failures.append({
                                'model': meta['model'],
                                'model_display': meta['display'],
                                'error': error_msg
                            })
                    except Exception as exc:
                        logger.error(
                            "模型 %s 综合报告生成出现异常: %s",
                            meta['display'],
                            exc
                        )
                        failures.append({
                            'model': meta['model'],
                            'model_display': meta['display'],
                            'error': str(exc)
                        })

            ordered_successes = [successes[idx] for idx in sorted(successes.keys())]
            persisted_reports: List[Dict[str, Any]] = []

            for report_meta in ordered_successes:
                model_name = report_meta['model']
                display_name = report_meta['model_display']
                content = report_meta['content']

                report_type_suffix = model_name.replace('/', '_') if model_name else 'default'
                report_data = {
                    'report_type': f'community_insights_custom::{report_type_suffix}',
                    'start_date': start_date,
                    'end_date': end_date,
                    'content': content,
                    'source_article_ids': [article['id'] for article in analyzed_articles]
                }

                try:
                    report_id = self.db_manager.save_synthesis_report(report_data)
                    logger.info(
                        "综合洞察报告存储成功 - 模型 %s, 报告ID %s",
                        display_name,
                        report_id
                    )

                    notion_result = self._push_synthesis_report_to_notion(
                        report_content=content,
                        report_id=report_id,
                        model_display=display_name
                    )

                    report_entry = {
                        'model': model_name,
                        'model_display': display_name,
                        'report_id': report_id,
                        'provider': report_meta.get('provider'),
                        'notion_push': notion_result
                    }

                    if include_preview:
                        report_entry['preview'] = content[:500] + '...' if len(content) > 500 else content

                    persisted_reports.append(report_entry)

                except Exception as storage_exc:
                    logger.error(
                        "综合报告存储或推送失败 - 模型 %s: %s",
                        display_name,
                        storage_exc,
                        exc_info=True
                    )
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': str(storage_exc)
                    })

            overall_success = len(persisted_reports) > 0

            return {
                'success': overall_success,
                'reports': persisted_reports,
                'failures': failures,
                'article_counts': {
                    'total': len(analyzed_articles),
                    'indiehackers': indiehackers_count,
                    'ezindie': ezindie_count
                },
                'model_count_requested': len(models_meta)
            }

        except Exception as e:
            logger.error(f"生成综合洞察报告失败: {e}")
            return {
                'success': False,
                'reports': [],
                'failures': [{'error': str(e)}]
            }

    def _push_synthesis_report_to_notion(
        self,
        report_content: str,
        report_id: int,
        model_display: Optional[str] = None
    ) -> Dict[str, Any]:
        """将社区综合洞察报告推送到 Notion"""
        try:
            # 从报告内容中提取标题
            lines = report_content.split('\n')
            report_title = "独立开发者社区洞察报告"
            for line in lines:
                if line.startswith('# '):
                    report_title = line[2:].strip()
                    break

            if model_display:
                report_title = f"{report_title} · {model_display}"

            logger.info(f"开始推送社区洞察报告到 Notion: {report_title}")

            result = notion_client.create_report_page(report_title, report_content)

            if result.get('success'):
                if result.get('skipped'):
                    logger.info(f"报告已存在于 Notion，跳过推送: {result.get('page_url')}")
                else:
                    logger.info(f"社区洞察报告成功推送到 Notion: {result.get('page_url')}")
            else:
                logger.error(f"推送社区洞察报告到 Notion 失败: {result.get('error')}")
            return result

        except Exception as e:
            logger.error(f"推送社区洞察报告到 Notion 时出错: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
