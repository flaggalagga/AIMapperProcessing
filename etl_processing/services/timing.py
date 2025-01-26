# services/timing.py
"""Performance timing utilities for ETL operations."""
import time
from functools import wraps
from typing import Optional, Callable

def timing_metric(metric_name: str):
    """Decorator to track execution time of ETL operations.
    
    Args:
        metric_name: Name of the metric to record
        
    Returns:
        Decorated function that logs timing data
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start = time.time()
            result = func(self, *args, **kwargs)
            duration = time.time() - start
            self.monitoring.record_timing(metric_name, duration)
            return result
        return wrapper
    return decorator