# services/batch_optimizer.py
from dataclasses import dataclass
from typing import List
import numpy as np

@dataclass
class BatchStats:
    size: int
    success_rate: float
    processing_time: float

class BatchOptimizer:
    def __init__(self, initial_size: int = 1000, min_size: int = 100, max_size: int = 5000):
        self.current_size = initial_size
        self.min_size = min_size
        self.max_size = max_size
        self.history: List[BatchStats] = []
        
    def adjust_size(self, success_rate: float, processing_time: float) -> int:
        """Adjust batch size based on performance metrics."""
        self.history.append(BatchStats(self.current_size, success_rate, processing_time))
        
        # More aggressive scaling factors
        if success_rate > 0.95 and processing_time < 30:
            self.current_size = min(int(self.current_size * 1.25), self.max_size)
        elif success_rate < 0.8 or processing_time > 60:
            self.current_size = max(int(self.current_size * 0.5), self.min_size)
            
        return self.current_size
        
    def get_stats(self):
        if not self.history:
            return None
            
        return {
            'avg_success_rate': np.mean([s.success_rate for s in self.history]),
            'avg_processing_time': np.mean([s.processing_time for s in self.history]),
            'size_changes': len(self.history),
            'current_size': self.current_size
        }