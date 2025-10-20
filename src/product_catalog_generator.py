"""
产品清单生成器
导出所有产品并生成完整的产品目录报告
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import json
import pymysql

from .config import config
from .database import DatabaseManager
from .notion_client import get_notion_client

logger = logging.getLogger(__name__)


class ProductCatalogGenerator:
    """产品清单生成器 - 导出所有产品并生成目录报告"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """初始化产品清单生成器"""
        if db_manager is not None:
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager(config)
        logger.info("产品清单生成器初始化完成")

    def get_all_products_deduplicated(self) -> List[Dict[str, Any]]:
        """
        获取所有产品并去重

        去重策略：
        1. 基于产品名称去重（忽略大小写和前后空格）
        2. 如果重复，只保留时间最近的记录
        3. 结果按时间由近及远排序

        Returns:
            去重后的产品列表
        """
        try:
            with self.db_manager.get_connection() as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    # 方案1: 从discovered_products表获取
                    # 使用窗口函数进行去重，保留每个产品名称的最新记录
                    query_discovered = """
                        SELECT dp1.*
                        FROM discovered_products dp1
                        INNER JOIN (
                            SELECT
                                LOWER(TRIM(product_name)) as normalized_name,
                                MAX(created_at) as latest_created_at
                            FROM discovered_products
                            GROUP BY LOWER(TRIM(product_name))
                        ) dp2
                        ON LOWER(TRIM(dp1.product_name)) = dp2.normalized_name
                        AND dp1.created_at = dp2.latest_created_at
                        ORDER BY dp1.created_at DESC
                    """

                    # 方案2: 从rss_decohack_products表获取
                    query_decohack = """
                        SELECT
                            product_name,
                            tagline,
                            description,
                            product_url,
                            image_url,
                            keywords as categories,
                            NULL as metrics,
                            'decohack' as source_feed,
                            ph_publish_date as source_published_at,
                            created_at
                        FROM rss_decohack_products dp1
                        INNER JOIN (
                            SELECT
                                LOWER(TRIM(product_name)) as normalized_name,
                                MAX(created_at) as latest_created_at
                            FROM rss_decohack_products
                            GROUP BY LOWER(TRIM(product_name))
                        ) dp2
                        ON LOWER(TRIM(dp1.product_name)) = dp2.normalized_name
                        AND dp1.created_at = dp2.latest_created_at
                        ORDER BY dp1.created_at DESC
                    """

                    # 先从discovered_products获取
                    cursor.execute(query_discovered)
                    discovered_products = cursor.fetchall()
                    logger.info(f"从 discovered_products 获取到 {len(discovered_products)} 个去重产品")

                    # 再从rss_decohack_products获取
                    cursor.execute(query_decohack)
                    decohack_products = cursor.fetchall()
                    logger.info(f"从 rss_decohack_products 获取到 {len(decohack_products)} 个去重产品")

                    # 合并两个数据源，再次去重
                    all_products = discovered_products + decohack_products

                    # 使用字典去重，保留时间最近的
                    product_dict = {}
                    for product in all_products:
                        product_name = product['product_name']
                        if not product_name:
                            continue

                        # 标准化产品名称用于去重
                        normalized_name = product_name.lower().strip()

                        # 获取产品的创建时间
                        created_at = product.get('created_at')
                        if created_at is None:
                            continue

                        # 如果该产品名称还没有记录，或者当前记录更新，则更新
                        if (normalized_name not in product_dict or
                            created_at > product_dict[normalized_name].get('created_at')):
                            product_dict[normalized_name] = product

                    # 转换为列表并按时间排序
                    final_products = list(product_dict.values())
                    final_products.sort(key=lambda x: x.get('created_at', datetime.min), reverse=True)

                    logger.info(f"最终去重后获取到 {len(final_products)} 个唯一产品")
                    return final_products

        except Exception as e:
            logger.error(f"获取并去重产品失败: {e}")
            return []

    def generate_catalog_markdown(self, products: List[Dict[str, Any]]) -> str:
        """
        生成产品清单的Markdown报告

        Args:
            products: 产品列表

        Returns:
            Markdown格式的报告内容
        """
        if not products:
            logger.warning("没有产品可生成清单")
            return "# 产品清单\n\n暂无产品。\n"

        # 统计信息
        total_count = len(products)

        # 按来源统计
        source_counter = {}
        for product in products:
            source = product.get('source_feed', 'unknown')
            source_counter[source] = source_counter.get(source, 0) + 1

        # 按分类统计（如果有）
        category_counter = {}
        for product in products:
            categories = product.get('categories')
            if categories:
                # 处理可能的多个分类
                if isinstance(categories, str):
                    cats = [c.strip() for c in categories.split(',')]
                    for cat in cats:
                        if cat:
                            category_counter[cat] = category_counter.get(cat, 0) + 1

        # 生成Markdown内容
        md_lines = []

        # 标题
        current_date = datetime.now().strftime('%Y-%m-%d')
        md_lines.append(f"# 📦 产品发现清单 ({current_date})")
        md_lines.append("")

        # 概览
        md_lines.append("## 📊 概览统计")
        md_lines.append("")
        md_lines.append(f"- **总产品数**: {total_count}")
        md_lines.append("")

        # 来源分布
        if source_counter:
            md_lines.append("### 来源分布")
            md_lines.append("")
            for source, count in sorted(source_counter.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_count) * 100
                md_lines.append(f"- **{source}**: {count} ({percentage:.1f}%)")
            md_lines.append("")

        # 热门分类（Top 10）
        if category_counter:
            md_lines.append("### 热门分类 (Top 10)")
            md_lines.append("")
            top_categories = sorted(category_counter.items(), key=lambda x: x[1], reverse=True)[:10]
            for category, count in top_categories:
                md_lines.append(f"- **{category}**: {count} 个产品")
            md_lines.append("")

        md_lines.append("---")
        md_lines.append("")

        # 产品列表
        md_lines.append("## 📋 产品列表")
        md_lines.append("")
        md_lines.append(f"*共 {total_count} 个产品，按发现时间由近及远排序*")
        md_lines.append("")

        # 按月份分组
        products_by_month = {}
        for product in products:
            created_at = product.get('created_at')
            if created_at:
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                month_key = created_at.strftime('%Y年%m月')
                if month_key not in products_by_month:
                    products_by_month[month_key] = []
                products_by_month[month_key].append(product)

        # 按月份输出
        for month in sorted(products_by_month.keys(), reverse=True):
            month_products = products_by_month[month]
            md_lines.append(f"### {month} ({len(month_products)} 个产品)")
            md_lines.append("")

            for i, product in enumerate(month_products, 1):
                product_name = product.get('product_name', '未命名产品')
                tagline = product.get('tagline', '')
                description = product.get('description', '')
                product_url = product.get('product_url', '')
                source_feed = product.get('source_feed', 'unknown')
                created_at = product.get('created_at')

                # 格式化日期
                if created_at:
                    if isinstance(created_at, str):
                        created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    date_str = created_at.strftime('%Y-%m-%d')
                else:
                    date_str = '未知'

                # 产品条目
                if product_url:
                    md_lines.append(f"#### {i}. [{product_name}]({product_url})")
                else:
                    md_lines.append(f"#### {i}. {product_name}")

                md_lines.append("")

                # 基本信息
                if tagline:
                    md_lines.append(f"**一句话介绍**: {tagline}")
                    md_lines.append("")

                if description:
                    # 限制描述长度
                    desc_text = description[:300] + "..." if len(description) > 300 else description
                    md_lines.append(f"**产品介绍**: {desc_text}")
                    md_lines.append("")

                # 元信息
                md_lines.append(f"- **来源**: {source_feed}")
                md_lines.append(f"- **发现时间**: {date_str}")

                # 分类标签
                categories = product.get('categories')
                if categories:
                    if isinstance(categories, str):
                        md_lines.append(f"- **标签**: {categories}")

                md_lines.append("")
                md_lines.append("---")
                md_lines.append("")

        # 页脚
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        md_lines.append("*本清单由 RSS 产品发现系统自动生成*")
        md_lines.append("")

        return "\n".join(md_lines)

    def push_catalog_to_notion(self, catalog_markdown: str) -> Dict[str, Any]:
        """
        将产品清单推送到 Notion

        Args:
            catalog_markdown: Markdown格式的清单内容

        Returns:
            推送结果
        """
        try:
            # 提取标题
            lines = catalog_markdown.split('\n')
            title = "产品发现清单"
            for line in lines:
                if line.startswith('# '):
                    title = line[2:].strip()
                    break

            logger.info(f"开始推送产品清单到 Notion: {title}")

            # 调用 Notion 客户端
            result = get_notion_client().create_report_page(title, catalog_markdown)

            if result.get('success'):
                if result.get('skipped'):
                    logger.info(f"产品清单已存在于 Notion，跳过推送: {result.get('page_url')}")
                else:
                    logger.info(f"产品清单成功推送到 Notion: {result.get('page_url')}")
            else:
                logger.error(f"推送产品清单到 Notion 失败: {result.get('error')}")

            return result

        except Exception as e:
            logger.error(f"推送产品清单到 Notion 时出错: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }

    def generate_and_push_catalog(self) -> Dict[str, Any]:
        """
        生成产品清单并推送到 Notion

        Returns:
            执行结果
        """
        try:
            logger.info("开始生成产品清单...")

            # 1. 获取去重后的所有产品
            products = self.get_all_products_deduplicated()

            if not products:
                logger.warning("没有产品可生成清单")
                return {
                    'success': False,
                    'message': '没有产品可生成清单',
                    'product_count': 0
                }

            logger.info(f"获取到 {len(products)} 个产品，开始生成 Markdown 报告...")

            # 2. 生成 Markdown 报告
            catalog_markdown = self.generate_catalog_markdown(products)

            logger.info(f"Markdown 报告生成完成，长度: {len(catalog_markdown)} 字符")

            # 3. 推送到 Notion
            notion_result = self.push_catalog_to_notion(catalog_markdown)

            # 4. 返回结果
            return {
                'success': True,
                'product_count': len(products),
                'markdown_length': len(catalog_markdown),
                'notion_push': notion_result,
                'notion_url': notion_result.get('page_url') if notion_result.get('success') else None
            }

        except Exception as e:
            logger.error(f"生成产品清单失败: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e)
            }
