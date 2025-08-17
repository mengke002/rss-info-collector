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

from src.scheduler import scheduler


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='RSS数据采集系统')
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'full', 'daemon'], 
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    
    args = parser.parse_args()
    
    print(f"RSS数据采集系统")
    print(f"执行时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"执行任务: {args.task}")
    print("-" * 50)
    
    # 执行对应任务
    if args.task == 'crawl':
        result = scheduler.run_crawl_task()
    elif args.task == 'cleanup':
        result = scheduler.run_cleanup_task(args.retention_days)
    elif args.task == 'stats':
        result = scheduler.run_stats_task()
    elif args.task == 'full':
        result = run_full_maintenance()
    elif args.task == 'daemon':
        print("启动RSS守护进程...")
        try:
            scheduler.start_scheduler()
        except KeyboardInterrupt:
            print("\n收到中断信号，正在停止...")
            scheduler.stop_scheduler()
            return
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)
    
    # 输出结果
    if args.task != 'daemon':
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


def run_full_maintenance():
    """执行完整维护任务"""
    results = {
        'success': True,
        'results': {}
    }
    
    print("执行完整维护任务...")
    
    # 1. 爬取任务
    print("1. 执行爬取任务...")
    crawl_result = scheduler.run_crawl_task()
    results['results']['crawl'] = crawl_result
    
    # 2. 清理任务
    print("2. 执行清理任务...")
    cleanup_result = scheduler.run_cleanup_task()
    results['results']['cleanup'] = cleanup_result
    
    # 3. 统计任务
    print("3. 执行统计任务...")
    stats_result = scheduler.run_stats_task()
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
            print(f"     总记录数: {stats.get('total_count', 0)}")
            print(f"     今日新增: {stats.get('today_count', 0)}")
            if stats.get('latest_time'):
                print(f"     最新时间: {stats['latest_time']}")
    
    elif task_type == 'full':
        print(f"✅ 完整维护任务完成")
        
        # 爬取结果
        crawl_result = result.get('results', {}).get('crawl', {})
        if crawl_result.get('success'):
            print(f"   爬取: 处理 {crawl_result.get('feeds_processed', 0)} 个RSS源，新增 {crawl_result.get('items_inserted', 0)} 条记录")
        
        # 清理结果
        cleanup_result = result.get('results', {}).get('cleanup', {})
        if cleanup_result.get('success'):
            print(f"   清理: 删除 {cleanup_result.get('total_deleted', 0)} 条旧记录")
        
        # 统计结果
        stats_result = result.get('results', {}).get('stats', {})
        if stats_result.get('success'):
            total_records = sum(
                stats.get('total_count', 0) 
                for stats in stats_result.get('stats', {}).values()
            )
            print(f"   统计: 总记录数 {total_records}")


if __name__ == "__main__":
    main()