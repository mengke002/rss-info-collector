#!/usr/bin/env python3
"""
RSS数据采集系统
主执行脚本
"""
import sys
import argparse
import json
import logging
from datetime import datetime, timezone, timedelta

from src.logger import setup_logging
from src.database import DatabaseManager
from src.config import config
from src.tasks import (
    run_crawl_task, run_cleanup_task, run_stats_task, 
    run_product_discovery_analysis, run_report_generation_task,
    run_tech_news_report_generation_task
)
from src.report_generator import ProductDiscoveryReportGenerator, TechNewsReportGenerator
from src.analyzer import TechNewsAnalyzer

# Initialize logging
setup_logging()
logger = logging.getLogger(__name__)


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


def run_product_discovery_report_task():
    """运行产品发现报告生成任务"""
    logger.info("开始执行产品发现报告生成任务...")
    try:
        report_generator = ProductDiscoveryReportGenerator()
        report_uuid = report_generator.generate_report(days=7)
        if report_uuid:
            logger.info(f"产品发现报告生成任务完成，报告UUID: {report_uuid}")
        else:
            logger.info("产品发现报告生成任务完成，但没有生成新报告。")
    except Exception as e:
        logger.error(f"产品发现报告生成任务失败: {e}", exc_info=True)


def run_tech_news_report_task():
    """运行科技新闻分析报告生成任务"""
    logger.info("开始执行科技新闻分析报告生成任务...")
    try:
        # 1. 从数据库获取待分析数据
        db_manager = DatabaseManager()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        time_range_str = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        
        articles = db_manager.get_articles_for_analysis(days=7)
        if not articles:
            logger.warning("在指定时间范围内没有需要分析的文章，任务终止。")
            return

        # 2. 调用分析器进行分析
        analyzer = TechNewsAnalyzer()
        analysis_results = analyzer.analyze_articles(articles)

        # 3. 调用报告生成器生成并存储报告
        report_generator = TechNewsReportGenerator()
        report_uuid = report_generator.generate_weekly_report(analysis_results, time_range_str)
        
        if report_uuid:
            logger.info(f"科技新闻分析报告生成任务完成，报告UUID: {report_uuid}")
        else:
            logger.info("科技新闻分析报告生成任务完成，但没有生成新报告。")
            
    except Exception as e:
        logger.error(f"科技新闻分析报告生成任务失败: {e}", exc_info=True)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='RSS数据采集系统')
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'analyze', 'report', 'tech_news_report', 'full', 'report_product', 'report_tech_news', 'community_analysis', 'community_report', 'community_full'],
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--recreate-db', action='store_true',
                       help='删除并重新创建所有RSS表')
    parser.add_argument('--feed', type=str,
                       help='只爬取指定的RSS源')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='分析任务的批处理大小（默认：50）')
    parser.add_argument('--report-period', choices=['daily', 'weekly', 'monthly'], default='daily',
                       help='报告周期（默认：daily）')
    parser.add_argument('--no-analysis', action='store_true',
                       help='生成报告时不包含深度分析')
    parser.add_argument('--hours-back', type=int, default=24,
                       help='科技新闻分析回溯的小时数（默认：24）')
    parser.add_argument('--custom-filter', action='store_true',
                       help='社区分析报告使用自定义筛选条件（48小时内indiehackers + 最新1篇ezindie）')
    
    args = parser.parse_args()
    
    print(f"RSS数据采集系统")
    print(f"执行时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"执行任务: {args.task}")
    print("-" * 50)

    db_manager = DatabaseManager(config)
    if args.recreate_db:
        print("正在删除并重新创建数据库表...")
        db_manager.drop_all_rss_tables()
        db_manager.init_database()
        print("数据库表已重新创建。")
    
    # 执行对应任务
    if args.task == 'crawl':
        result = run_crawl_task(db_manager, args.feed)
    elif args.task == 'cleanup':
        result = run_cleanup_task(db_manager, args.retention_days)
    elif args.task == 'stats':
        result = run_stats_task(db_manager)
    elif args.task == 'analyze':
        result = run_product_discovery_analysis(db_manager, args.batch_size)
    elif args.task == 'report':
        include_analysis = not args.no_analysis
        result = run_report_generation_task(db_manager, args.report_period, include_analysis)
    elif args.task == 'tech_news_report':
        result = run_tech_news_report_generation_task(db_manager, args.hours_back)
    elif args.task == 'full':
        result = run_full_maintenance(db_manager)
    elif args.task == 'report_product':
        run_product_discovery_report_task()
    elif args.task == 'report_tech_news':
        run_tech_news_report_task()
    elif args.task == 'community_analysis':
        from src.tasks import run_community_deep_analysis_task
        result = run_community_deep_analysis_task(batch_size=args.batch_size)
    elif args.task == 'community_report':
        from src.tasks import run_community_synthesis_report_task
        result = run_community_synthesis_report_task(days=7, use_custom_filter=args.custom_filter)
    elif args.task == 'community_full':
        from src.tasks import run_community_analysis_and_report_task
        result = run_community_analysis_and_report_task(
            analysis_batch_size=args.batch_size, 
            report_days=7, 
            use_custom_filter=args.custom_filter
        )
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)
    
    # 输出结果
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, args.task)
    
    # 根据结果设置退出码
    if result.get('success', False):
        print("\n✅ 任务执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 任务执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


def run_full_maintenance(db_manager):
    """执行完整维护任务"""
    results = {
        'success': True,
        'results': {}
    }
    
    print("执行完整维护任务...")
    
    # 1. 爬取任务
    print("1. 执行爬取任务...")
    crawl_result = run_crawl_task(db_manager)
    results['results']['crawl'] = crawl_result
    
    # 2. 分析任务
    print("2. 执行产品发现分析...")
    analysis_result = run_product_discovery_analysis(db_manager)
    results['results']['analysis'] = analysis_result
    
    # 3. 报告生成任务
    print("3. 生成产品发现报告...")
    report_result = run_report_generation_task(db_manager, period='daily', include_analysis=True)
    results['results']['report'] = report_result
    
    # 4. 清理任务
    print("4. 执行清理任务...")
    cleanup_result = run_cleanup_task(db_manager)
    results['results']['cleanup'] = cleanup_result
    
    # 5. 统计任务
    print("5. 执行统计任务...")
    stats_result = run_stats_task(db_manager)
    results['results']['stats'] = stats_result
    
    # 检查所有任务是否成功
    all_success = all(
        result.get('success', False) 
        for result in results['results'].values()
    )
    results['success'] = all_success
    
    return results


def print_result(result: dict, task_type: str):
    """打印结果"""
    if not result.get('success', False):
        print(f"❌ 任务失败: {result.get('error', '未知错误')}")
        return
    
    if task_type == 'crawl':
        print(f"✅ 爬取任务完成")
        print(f"   处理RSS源: {result.get('feeds_processed', 0)} 个")
        print(f"   新增记录: {result.get('items_inserted', 0)} 条")
        if result.get('errors'):
            print(f"   错误: {len(result['errors'])} 个")
    
    elif task_type == 'cleanup':
        print(f"✅ 清理任务完成")
        print(f"   总删除记录: {result.get('total_deleted', 0)} 条")
        for feed, count in result.get('deleted_counts', {}).items():
            print(f"   {feed}: 删除 {count} 条")
    
    elif task_type == 'stats':
        print(f"✅ 统计信息")
        for feed_name, stats in result.get('stats', {}).items():
            print(f"   {feed_name}:")
            if feed_name == 'indiehackers_by_type':
                # 特殊处理indiehackers_by_type的显示
                if isinstance(stats, dict) and stats:
                    for feed_type, count in stats.items():
                        print(f"     {feed_type}: {count} 条")
                else:
                    print(f"     暂无数据")
            else:
                # 标准格式显示
                print(f"     总记录数: {stats.get('total_count', 0)}")
                print(f"     今日新增: {stats.get('today_count', 0)}")
                if stats.get('latest_time'):
                    print(f"     最新时间: {stats['latest_time']}")
    
    elif task_type == 'analyze':
        print(f"✅ 产品发现分析完成")
        print(f"   总处理数量: {result.get('total_processed', 0)}")
        print(f"   成功提取: {result.get('total_extracted', 0)} 个产品")
        
        processed_tables = result.get('processed_tables', [])
        if processed_tables:
            print("   各表处理情况:")
            for table_info in processed_tables:
                print(f"     {table_info['table_name']}: 处理{table_info['processed_count']}, 提取{table_info['extracted_count']}")
        
        if result.get('errors'):
            print(f"   错误: {len(result['errors'])} 个")
            for error in result['errors'][:3]:  # 只显示前3个错误
                print(f"     - {error}")
    
    elif task_type == 'report':
        print(f"✅ 报告生成完成")
        print(f"   报告文件: {result.get('report_path', '未知')}")
        print(f"   产品数量: {result.get('products_count', 0)}")
        if result.get('analysis_count', 0) > 0:
            print(f"   深度分析: {result['analysis_count']} 个")
    
    elif task_type == 'tech_news_report':
        print(f"✅ 科技新闻报告生成完成")
        print(f"   报告文件: {result.get('report_path', '未知')}")

    elif task_type == 'community_analysis':
        print(f"✅ 社区深度分析完成")
        print(f"   处理文章数: {result.get('processed_articles', 0)}")
        
    elif task_type == 'community_report':
        print(f"✅ 社区综合洞察报告生成完成")
        if result.get('report_id'):
            print(f"   报告ID: {result.get('report_id')}")
        else:
            print(f"   {result.get('message', '未知')}")
            
    elif task_type == 'community_full':
        print(f"✅ 社区完整分析与报告任务完成")
        analysis_result = result.get('analysis_result', {})
        report_result = result.get('report_result', {})
        print(f"   处理文章数: {analysis_result.get('processed_articles', 0)}")
        if report_result.get('report_id'):
            print(f"   生成报告ID: {report_result.get('report_id')}")
        else:
            print(f"   报告生成: {report_result.get('message', '未知')}")

    elif task_type == 'full':
        print(f"✅ 完整维护任务完成")
        
        # 爬取结果
        crawl_result = result.get('results', {}).get('crawl', {})
        if crawl_result.get('success'):
            print(f"   爬取: 处理 {crawl_result.get('feeds_processed', 0)} 个RSS源，新增 {crawl_result.get('items_inserted', 0)} 条记录")
        
        # 分析结果
        analysis_result = result.get('results', {}).get('analysis', {})
        if analysis_result.get('success'):
            print(f"   分析: 处理 {analysis_result.get('total_processed', 0)} 条，提取 {analysis_result.get('total_extracted', 0)} 个产品")
        
        # 报告结果
        report_result = result.get('results', {}).get('report', {})
        if report_result.get('success'):
            print(f"   报告: 生成报告 {report_result.get('filename', '未知')}")
        
        # 清理结果
        cleanup_result = result.get('results', {}).get('cleanup', {})
        if cleanup_result.get('success'):
            print(f"   清理: 删除 {cleanup_result.get('total_deleted', 0)} 条旧记录")
        
        # 统计结果
        stats_result = result.get('results', {}).get('stats', {})
        if stats_result.get('success'):
            total_records = sum(
                stats.get('total_count', 0) if isinstance(stats, dict) else 0
                for stats in stats_result.get('stats', {}).values()
                if stats != 'indiehackers_by_type' # 排除特殊统计
            )
            print(f"   统计: 总记录数 {total_records}")


if __name__ == "__main__":
    main()
