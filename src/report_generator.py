"""
RSS产品发现报告生成器
基于discovered_products表数据生成Markdown格式报告
"""
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import json
import os
import concurrent.futures
from collections import Counter
import pandas as pd
import uuid

from .config import config
from .database import DatabaseManager
from .llm_client import call_llm

logger = logging.getLogger(__name__)


class TechNewsReportGenerator:
    """科技与创投新闻分析报告生成器"""

    def __init__(self):
        """初始化报告生成器"""
        # self.reports_dir = 'reports'
        # os.makedirs(self.reports_dir, exist_ok=True)
        self.db_manager = DatabaseManager(config)
        logger.info("科技新闻报告生成器初始化完成")

    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _build_layer1_insight(self, deep_insights: Dict[str, Any]) -> str:
        """构建层次1：核心洞察"""
        if not deep_insights:
            return "> **核心洞察:** 暂无\n"
        
        analyst_take = deep_insights.get('analyst_take', '无')
        return f"> **核心洞察:** {analyst_take}\n"

    def _build_layer2_findings(self, key_info_analysis: Dict[str, Any]) -> str:
        """构建层次2：关键趋势与发现"""
        if not key_info_analysis:
            return "## 一、本周关键趋势与发现\n\n- 暂无\n"

        findings = []
        hot_entities = key_info_analysis.get('hot_entities', [])
        trending_topics = key_info_analysis.get('trending_topics', [])

        if hot_entities:
            entities_str = ", ".join([f"**{e['entity']}** ({e['frequency']}次)" for e in hot_entities[:3]])
            findings.append(f"本期最受关注的实体包括：{entities_str}。")
        
        if trending_topics:
            topics_str = ", ".join([f"**{t['entity']}** ({t['frequency']}次)" for t in trending_topics[:3]])
            findings.append(f"热门讨论的技术话题集中在：{topics_str}。")

        if not findings:
            return "## 一、本周关键趋势与发现\n\n- 暂无\n"

        findings_str = "\n".join([f"*   {f}" for f in findings])
        return f"## 一、本周关键趋势与发现\n\n{findings_str}\n"

    def _build_layer3_analysis_data(self, analysis_results: Dict[str, Any]) -> str:
        """构建层次3：详细分析数据"""
        insights = analysis_results.get('comprehensive_insights', {}).get('insights', {})
        if not insights:
            return "## 二、详细分析数据\n\n暂无详细数据。\n"

        markdown_parts = ["## 二、详细分析数据"]

        # 2.1 主题分布
        topic_analysis = insights.get('topic_analysis', {})
        if topic_analysis.get('topic_distribution'):
            markdown_parts.append("### 2.1 热门主题分布")
            df = pd.DataFrame(topic_analysis['topic_distribution'])
            df = df[['topic', 'article_count', 'percentage']]
            df.rename(columns={'topic': '主题', 'article_count': '文章数', 'percentage': '占比(%)'}, inplace=True)
            markdown_parts.append(df.to_markdown(index=False))
            markdown_parts.append("\n")

        # 2.2 热门实体
        key_info_analysis = insights.get('key_info_analysis', {})
        if key_info_analysis.get('hot_entities'):
            markdown_parts.append("### 2.2 热门实体与趋势")
            df = pd.DataFrame(key_info_analysis['hot_entities'])
            df = df[['entity', 'frequency']]
            df.rename(columns={'entity': '实体/关键词', 'frequency': '提及次数'}, inplace=True)
            
            # 使用<details>来折叠长表格
            if len(df) > 5:
                markdown_parts.append("<details>")
                markdown_parts.append(f"<summary>点击展开/折叠：Top {len(df)} 热门实体</summary>")
                markdown_parts.append("\n")
                markdown_parts.append(df.to_markdown(index=False))
                markdown_parts.append("\n</details>")
            else:
                markdown_parts.append(df.to_markdown(index=False))
            markdown_parts.append("\n")

        # 2.3 新兴技术与创新
        emerging_tech_analysis = insights.get('emerging_tech_analysis', {})
        if emerging_tech_analysis.get('trends'):
            markdown_parts.append("### 2.3 新兴技术与创新")
            df = pd.DataFrame(emerging_tech_analysis['trends'])
            df = df[['technology', 'description', 'potential_impact']]
            df.rename(columns={'technology': '技术', 'description': '描述', 'potential_impact': '潜在影响'}, inplace=True)
            markdown_parts.append(df.to_markdown(index=False))
            markdown_parts.append("\n")

        return "\n".join(markdown_parts)

    def generate_report(self, full_report_md: str, article_count: int, time_range_str: str = "过去24小时") -> Dict[str, Any]:
        """
        简化的报告生成方法：接收已生成的Markdown报告并存入数据库
        
        Args:
            full_report_md: 已经生成好的完整Markdown报告
            article_count: 分析的文章总数
            time_range_str: 时间范围描述
            
        Returns:
            生成结果
        """
        logger.info("开始将生成的科技新闻报告存入数据库...")
        
        try:
            report_uuid = str(uuid.uuid4())
            beijing_time = self.get_beijing_time()
            report_date = beijing_time.date()

            # 简化的元数据
            metadata = {
                "total_articles_analyzed": article_count,
                "time_range": time_range_str,
                "generation_method": "two_layer_llm",
                "generated_at": beijing_time.isoformat()
            }

            # 将报告存入数据库
            try:
                with self.db_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        insert_sql = """
                            INSERT INTO technews_reports (
                                report_uuid, generated_at, report_date, 
                                time_range, article_count, main_topics, 
                                report_content_md, metadata
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_sql, (
                            report_uuid,
                            beijing_time,
                            report_date,
                            time_range_str,
                            article_count,
                            json.dumps([], ensure_ascii=False),  # 主题信息可以从内容中提取
                            full_report_md,
                            json.dumps(metadata, ensure_ascii=False)
                        ))
                        
                        conn.commit()
                        logger.info(f"报告已成功存入数据库 - UUID: {report_uuid}")
                        
            except Exception as e:
                logger.error(f"存储报告到数据库失败: {e}")
                return {
                    'success': False,
                    'error': f'数据库存储失败: {str(e)}',
                    'report_uuid': None
                }

            return {
                'success': True,
                'report_uuid': report_uuid,
                'report_date': report_date.isoformat(),
                'article_count': article_count,
                'message': f'科技新闻报告生成完成，基于{article_count}篇文章的分析'
            }
            
        except Exception as e:
            logger.error(f"生成报告失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'report_uuid': None
            }

    def _save_report_as_backup_file(self, report_uuid: str, content: str, report_type: str):
        """在数据库保存失败时，将报告保存为本地文件作为备份。"""
        backup_dir = 'reports_backup'
        os.makedirs(backup_dir, exist_ok=True)
        filename = f"{report_type}_report_{report_uuid}.md"
        filepath = os.path.join(backup_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.warning(f"数据库保存失败，报告已作为备份文件保存在: {filepath}")
        except Exception as e:
            logger.error(f"保存备份报告文件失败: {e}", exc_info=True)


class ProductDiscoveryReportGenerator:
    """产品发现报告生成器"""

    def __init__(self):
        """初始化报告生成器"""
        # self.reports_dir = 'reports'
        # os.makedirs(self.reports_dir, exist_ok=True)
        self.db_manager = DatabaseManager(config)
        logger.info("产品发现报告生成器初始化完成")

    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _format_product_section(self, products: List[Dict[str, Any]]) -> str:
        """
        格式化产品部分为Markdown表格
        
        Args:
            products: 产品字典列表
            
        Returns:
            Markdown格式的产品表格
        """
        if not products:
            return "暂无新产品发现。\n"
        
        table_lines = [
            "| 产品名称 | 标语 | 分类 | 来源 | 发布时间 |",
            "|---------|------|------|------|----------|"
        ]
        
        for product in products:
            name = product.get('product_name', '未知')
            tagline = (product.get('tagline', '') or '无')[:50]  # 限制长度
            if len(tagline) > 50:
                tagline += "..."
            
            categories = product.get('categories', '无')
            source = product.get('source_feed', '未知')
            
            pub_time = product.get('source_published_at')
            if pub_time:
                time_str = pub_time.strftime('%m-%d %H:%M') if isinstance(pub_time, datetime) else str(pub_time)[:10]
            else:
                time_str = '未知'
            
            # 如果有产品URL，将产品名称设为链接
            url = product.get('product_url', '')
            if url:
                name = f"[{name}]({url})"
            
            table_lines.append(f"| {name} | {tagline} | {categories} | {source} | {time_str} |")
        
        return "\n".join(table_lines) + "\n"

    def _format_summary(self, product_count: int, source_feed_count: int, time_range: str) -> str:
        """
        格式化报告摘要部分为Markdown
        
        Args:
            product_count: 产品数量
            source_feed_count: 数据源数量
            time_range: 时间范围字符串
            
        Returns:
            Markdown格式的摘要部分
        """
        return f"""
## 摘要

- **发现产品数量**: {product_count} 个
- **数据源数量**: {source_feed_count} 个
- **报告时间范围**: {time_range}

"""

    def generate_report(self, days: int = 7):
        """
        生成产品发现报告，并将其存入数据库。
        :param days: 报告涵盖的天数
        """
        logger.info(f"开始生成过去 {days} 天的产品发现报告...")
        
        report_uuid = str(uuid.uuid4())
        beijing_time = self.get_beijing_time()
        report_date = beijing_time.date()
        
        end_date = beijing_time.date()
        start_date = end_date - timedelta(days=days)
        time_range_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        products = self.db_manager.get_discovered_products(days=days)
        if not products:
            logger.warning("在指定时间范围内没有发现任何产品，不生成报告。")
            return None

        product_count = len(products)
        source_feeds = list(set(p['source_feed'] for p in products if p.get('source_feed')))
        source_feed_count = len(source_feeds)

        # 生成Markdown内容
        report_title = f"# 产品发现周报 ({report_date.strftime('%Y-%m-%d')})"
        summary_section = self._format_summary(product_count, source_feed_count, time_range_str)
        product_section = self._format_product_section(products)
        full_report_md = f"{report_title}\n\n{summary_section}\n\n{product_section}"

        # 准备存入数据库的数据
        metadata = {
            "top_product_names": [p['product_name'] for p in products[:5]],
            "source_feeds": source_feeds,
            "llm_analysis_prompts": {
                "categorization_prompt": "...", # 示例，可以从config或代码中获取
                "summary_prompt": "..."
            }
        }

        report_data = {
            "report_uuid": report_uuid,
            "generated_at": beijing_time,
            "report_date": report_date,
            "time_range": time_range_str,
            "product_count": product_count,
            "source_feed_count": source_feed_count,
            "report_content_md": full_report_md,
            "metadata": metadata
        }

        try:
            self.db_manager.save_product_report(report_data)
            logger.info(f"产品发现报告 {report_uuid} 已成功存入数据库。")
        except Exception as e:
            logger.error(f"保存产品发现报告到数据库时失败: {e}", exc_info=True)
            self._save_report_as_backup_file(report_uuid, full_report_md, "product")

        return report_uuid

    def _save_report_as_backup_file(self, report_uuid: str, content: str, report_type: str):
        """在数据库保存失败时，将报告保存为本地文件作为备份。"""
        backup_dir = 'reports_backup'
        os.makedirs(backup_dir, exist_ok=True)
        filename = f"{report_type}_report_{report_uuid}.md"
        filepath = os.path.join(backup_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.warning(f"数据库保存失败，报告已作为备份文件保存在: {filepath}")
        except Exception as e:
            logger.error(f"保存备份报告文件失败: {e}", exc_info=True)


class InsightsReportGenerator:
    """洞察报告生成器，整合科技新闻与产品发现报告"""

    def __init__(self):
        """初始化报告生成器"""
        self.tech_news_generator = TechNewsReportGenerator()
        self.product_discovery_generator = ProductDiscoveryReportGenerator()
        logger.info("洞察报告生成器初始化完成")

    def generate_insights_report(self, analysis_results: Dict[str, Any], period: str = 'daily'):
        """
        生成综合洞察报告，包括科技新闻分析与产品发现洞察
        
        Args:
            analysis_results: 科技新闻分析结果
            period: 产品发现报告周期 ('daily', 'weekly', 'monthly')
            
        Returns:
            报告生成结果
        """
        logger.info("开始生成综合洞察报告")
        
        # 生成科技新闻周报
        if period == 'weekly':
            report_uuid = self.tech_news_generator.generate_report(analysis_results, "本周")
        else:
            logger.warning("当前仅支持周报的综合洞察报告生成")
            return {"success": False, "message": "仅支持周报的综合洞察报告生成"}
        
        # 生成产品发现报告
        product_report_uuid = self.product_discovery_generator.generate_report(days=7)
        
        return {
            "success": True,
            "tech_news_report_uuid": report_uuid,
            "product_discovery_report_uuid": product_report_uuid
        }


def generate_product_discovery_report(db_manager: DatabaseManager, period: str = 'daily', include_analysis: bool = True) -> Dict[str, Any]:
    """
    便捷函数：生成产品发现报告
    
    Args:
        db_manager: 数据库管理器
        period: 报告周期
        include_analysis: 是否包含深度分析
        
    Returns:
        报告生成结果
    """
    generator = ProductDiscoveryReportGenerator()
    return generator.generate_report(period, include_analysis)

def generate_tech_news_report(analysis_results: Dict[str, Any], time_range_str: str) -> Dict[str, Any]:
    """
    便捷函数：生成科技新闻分析报告
    
    Args:
        analysis_results: 来自TechNewsAnalyzer的完整分析结果
        time_range_str: 时间范围描述字符串
        
    Returns:
        报告生成结果
    """
    generator = TechNewsReportGenerator()
    report_uuid = generator.generate_report(analysis_results, time_range_str)
    
    if report_uuid:
        return {
            'success': True,
            'report_uuid': report_uuid,
            'report_path': f"数据库中的报告UUID: {report_uuid}" # 示例路径
        }
    else:
        return {
            'success': False,
            'error': '报告生成失败，未能获取报告UUID'
        }