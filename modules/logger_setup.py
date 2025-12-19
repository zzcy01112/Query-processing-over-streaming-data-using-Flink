from loguru import logger
import sys
import os


# 移除所有已存在的处理器
logger.remove()

# 配置日志记录，输出到屏幕
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>", level="DEBUG")

# # 配置日志记录，输出到文件
# # 获取顶级目录
# path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# logger.add(f"{path}/logs/debug.log", format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>", level="INFO")
