"""
日志配置模块
"""
import logging
import sys
import time
from datetime import datetime
from .config import config

def setup_logging():
    """设置日志配置"""
    log_config = config.get_logging_config()
    
    # 创建logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_config['log_level'].upper(), logging.INFO))
    
    # 创建formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 文件处理器
    file_handler = logging.FileHandler(log_config['log_file'], encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 设置第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logger

def log_task_start(task_name):
    """记录任务开始"""
    logger = logging.getLogger()
    start_time = time.time()
    logger.info(f"🚀 开始任务: {task_name}")
    return start_time

def log_task_end(task_name, start_time, **kwargs):
    """记录任务结束"""
    logger = logging.getLogger()
    end_time = time.time()
    duration = end_time - start_time
    duration_str = f"{duration:.2f}秒"
    
    message = f"✅ 完成任务: {task_name} (耗时: {duration_str})"
    if kwargs:
        details = ", ".join([f"{k}: {v}" for k, v in kwargs.items()])
        message += f" - {details}"
    
    logger.info(message)

def log_error(task_name, error):
    """记录错误"""
    logger = logging.getLogger()
    logger.error(f"❌ 任务失败: {task_name} - {str(error)}", exc_info=True)

# 初始化日志
logger = setup_logging()