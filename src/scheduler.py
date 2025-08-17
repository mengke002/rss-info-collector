"""
任务调度器模块
"""
import time
import threading
import schedule
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from .config import config
from .database import db_manager
from .rss_parser import rss_parser
from .logger import logger

class RSSScheduler:
    """RSS任务调度器"""
    
    def __init__(self):
        """初始化调度器"""
        self.feed_configs = config.get_feed_configs()
        self.running = False
        self.threads = []
    
    def run_crawl_task(self) -> Dict[str, Any]:
        """执行爬取任务"""
        logger.info("开始执行RSS爬取任务")
        
        results = {
            'success': True,
            'feeds_processed': 0,
            'items_inserted': 0,
            'errors': []
        }
        
        for feed_name, feed_config in self.feed_configs.items():
            try:
                logger.info(f"处理RSS源: {feed_name}")
                
                # 获取已存在的GUID
                table_name = f"rss_{feed_name}"
                existing_guids = db_manager.get_existing_guids(table_name)
                
                # 解析RSS
                items = rss_parser.parse_feed(feed_config['rss_url'])
                
                # 过滤新条目
                new_items = []
                for item in items:
                    if item['guid'] not in existing_guids:
                        new_items.append(item)
                
                # 插入新条目
                inserted_count = 0
                for item in new_items:
                    # 特殊处理
                    if feed_name == 'betalist':
                        item['visit_url'] = rss_parser.extract_visit_url(item['link'], 'betalist')
                    
                    if db_manager.insert_rss_item(table_name, item):
                        inserted_count += 1
                
                logger.info(f"{feed_name}: 新增 {inserted_count} 条记录")
                
                results['feeds_processed'] += 1
                results['items_inserted'] += inserted_count
                
            except Exception as e:
                error_msg = f"处理 {feed_name} 失败: {str(e)}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        results['success'] = len(results['errors']) == 0
        return results
    
    def run_cleanup_task(self, days: int = None) -> Dict[str, Any]:
        """执行清理任务"""
        if days is None:
            days = config.get_data_retention_days()
        
        logger.info(f"开始清理超过 {days} 天的旧数据")
        
        results = {
            'success': True,
            'deleted_counts': {},
            'total_deleted': 0
        }
        
        for feed_name in self.feed_configs.keys():
            table_name = f"rss_{feed_name}"
            try:
                deleted_count = db_manager.cleanup_old_data(table_name, days)
                results['deleted_counts'][feed_name] = deleted_count
                results['total_deleted'] += deleted_count
                logger.info(f"{table_name}: 删除 {deleted_count} 条旧记录")
            except Exception as e:
                logger.error(f"清理 {table_name} 失败: {e}")
                results['success'] = False
                results['error'] = str(e)
        
        return results
    
    def run_stats_task(self) -> Dict[str, Any]:
        """执行统计任务"""
        logger.info("开始生成统计信息")
        
        results = {
            'success': True,
            'stats': {}
        }
        
        for feed_name in self.feed_configs.keys():
            table_name = f"rss_{feed_name}"
            try:
                stats = db_manager.get_stats(table_name)
                results['stats'][feed_name] = stats
                logger.info(f"{feed_name}: {stats}")
            except Exception as e:
                logger.error(f"获取 {feed_name} 统计信息失败: {e}")
                results['success'] = False
                results['error'] = str(e)
        
        return results
    
    def schedule_tasks(self):
        """调度定期任务"""
        logger.info("开始调度RSS任务")
        
        # 为每个RSS源设置定时任务
        for feed_name, feed_config in self.feed_configs.items():
            interval_seconds = feed_config['interval']
            
            if interval_seconds == 1800:  # 30分钟
                schedule.every(30).minutes.do(self._run_single_feed, feed_name)
            elif interval_seconds == 3600:  # 1小时
                schedule.every().hour.do(self._run_single_feed, feed_name)
            elif interval_seconds == 86400:  # 1天
                schedule.every().day.do(self._run_single_feed, feed_name)
            elif interval_seconds == 604800:  # 1周
                schedule.every().week.do(self._run_single_feed, feed_name)
            elif interval_seconds == 2592000:  # 30天
                schedule.every(30).days.do(self._run_single_feed, feed_name)
            else:
                # 默认30分钟
                schedule.every(30).minutes.do(self._run_single_feed, feed_name)
        
        # 每日清理任务
        schedule.every().day.at("02:00").do(self.run_cleanup_task)
        
        # 每小时统计任务
        schedule.every().hour.do(self.run_stats_task)
    
    def _run_single_feed(self, feed_name: str):
        """运行单个RSS源的爬取任务"""
        try:
            feed_config = self.feed_configs[feed_name]
            
            # 获取已存在的GUID
            table_name = f"rss_{feed_name}"
            existing_guids = db_manager.get_existing_guids(table_name)
            
            # 解析RSS
            items = rss_parser.parse_feed(feed_config['rss_url'])
            
            # 过滤新条目
            new_items = []
            for item in items:
                if item['guid'] not in existing_guids:
                    new_items.append(item)
            
            # 插入新条目
            inserted_count = 0
            for item in new_items:
                if feed_name == 'betalist':
                    item['visit_url'] = rss_parser.extract_visit_url(item['link'], 'betalist')
                
                if db_manager.insert_rss_item(table_name, item):
                    inserted_count += 1
            
            logger.info(f"定时任务 - {feed_name}: 新增 {inserted_count} 条记录")
            
        except Exception as e:
            logger.error(f"定时任务 - 处理 {feed_name} 失败: {e}")
    
    def start_scheduler(self):
        """启动调度器"""
        logger.info("启动RSS调度器")
        self.running = True
        self.schedule_tasks()
        
        # 立即执行一次所有任务
        logger.info("执行初次爬取任务")
        self.run_crawl_task()
        
        # 运行调度循环
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop_scheduler(self):
        """停止调度器"""
        logger.info("停止RSS调度器")
        self.running = False
        schedule.clear()

# 全局调度器实例
scheduler = RSSScheduler()