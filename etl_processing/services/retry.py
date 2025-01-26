# services/retry.py
import time
from functools import wraps
from typing import Callable, Optional, Type, Union, List, Dict
import logging

class RetryableError(Exception):
    """Base class for errors that should trigger retry."""
    pass

def with_retry(
    max_attempts: int = 3, 
    delay: float = 1.0, 
    exceptions: tuple = (Exception,),
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    logger: Optional[logging.Logger] = None
):
    """Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        exceptions: Tuple of exceptions to catch
        backoff_factor: Multiplier for exponential backoff
        max_delay: Maximum delay between retries
        logger: Optional logger for retry attempts
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_error = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        if logger:
                            logger.warning(
                                f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. "
                                f"Retrying in {current_delay:.1f}s..."
                            )
                        time.sleep(current_delay)
                        current_delay = min(current_delay * backoff_factor, max_delay)
                        continue
                    if logger:
                        logger.error(f"All {max_attempts} attempts failed: {str(e)}")
            raise last_error
        return wrapper
    return decorator

class RetryTracker:
    """Tracks retry statistics across operations."""
    
    def __init__(self):
        self.stats: Dict[str, Dict] = {}
        
    def record_attempt(self, operation_name: str, success: bool):
        if operation_name not in self.stats:
            self.stats[operation_name] = {"attempts": 0, "failures": 0}
        
        self.stats[operation_name]["attempts"] += 1
        if not success:
            self.stats[operation_name]["failures"] += 1
            
    def get_stats(self) -> Dict:
        """Returns retry statistics for all operations."""
        return {
            name: {
                "success_rate": (stats["attempts"] - stats["failures"]) / stats["attempts"]
                    if stats["attempts"] > 0 else 0,
                "total_attempts": stats["attempts"],
                "total_failures": stats["failures"]
            }
            for name, stats in self.stats.items()
        }