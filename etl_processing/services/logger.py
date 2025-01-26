# services/logger.py
import logging
import os
from typing import Optional, Dict

def setup_logging(
    log_level: str = 'INFO', 
    log_file: Optional[str] = None, 
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Configure and setup logging with console and optional file output
    
    :param log_level: Logging level (e.g., 'INFO', 'DEBUG', 'WARNING')
    :param log_file: Path to log file. If None, logs to console only
    :param log_format: Format of log messages
    :return: Configured logger instance
    
    Example format:
        2024-01-24 12:34:56 - accidents_etl - INFO - Process started
    """
    # Convert log level to uppercase
    log_level = log_level.upper()
    
    # Create logger
    logger = logging.getLogger('accidents_etl')
    logger.setLevel(getattr(logging, log_level))

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file is provided)
    if log_file:
        # Ensure directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def log_timing_detail(logger: logging.Logger, metric_name: str, duration: float, context: Dict = None):
    """Log detailed timing information.
    
    Args:
        logger: Logger instance
        metric_name: Name of the timing metric
        duration: Duration in seconds
        context: Optional context dictionary
    """
    message = f"Timing - {metric_name}: {duration:.3f}s"
    if context:
        message += f" | Context: {context}"
    logger.debug(message)