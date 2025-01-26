# services/timing.py
import time
from functools import wraps
from typing import Optional, Callable

def timing_metric(metric_name: str):
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