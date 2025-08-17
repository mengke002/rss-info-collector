"""
日志配置模块
"""
import logging
import sys
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

# 初始化日志
logger = setup_logging()