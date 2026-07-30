from loguru import logger
import sys


logger.remove()

logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level: <8}</level> | "
           "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
           "<level>{message}</level>"
)

logger.add(
    "hyper_trader.log",
    rotation="10 MB",
    retention="30 days",
    compression="zip",
    level="INFO"
)