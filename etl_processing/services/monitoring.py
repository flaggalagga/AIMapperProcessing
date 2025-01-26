# services/monitoring.py
"""ETL monitoring and metrics tracking."""

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime

@dataclass
class ETLMetrics:
   """Container for ETL run metrics."""
   start_time: datetime
   end_time: Optional[datetime] = None
   records_processed: int = 0
   records_failed: int = 0
   error_counts: Dict[str, int] = field(default_factory=dict)
   timings: Dict[str, List[float]] = field(default_factory=dict)
   avg_processing_time: float = 0.0

class MonitoringService:
   def __init__(self, logger):
       self.logger = logger
       self.current_run = None
       self.error_history = []
       self.processing_times = []
       
   def start_run(self):
       self.current_run = ETLMetrics(
           start_time=datetime.now(),
           error_counts={},
           timings={}
       )
       
   def end_run(self):
       if self.current_run:
           self.current_run.end_time = datetime.now()
           if self.processing_times:
               self.current_run.avg_processing_time = sum(self.processing_times) / len(self.processing_times)
           self._log_metrics()
   
   def record_timing(self, metric_name: str, duration: float):
       if self.current_run:
           if metric_name not in self.current_run.timings:
               self.current_run.timings[metric_name] = []
           self.current_run.timings[metric_name].append(duration)
           
   def record_success(self, processing_time: float):
       if self.current_run:
           self.current_run.records_processed += 1
           self.processing_times.append(processing_time)
           
   def record_error(self, error_type: str, error_msg: str, record_id: Optional[str] = None):
       if self.current_run:
           self.current_run.records_failed += 1
           self.current_run.error_counts[error_type] = self.current_run.error_counts.get(error_type, 0) + 1
           self.error_history.append({
               'timestamp': datetime.now(),
               'type': error_type,
               'message': error_msg,
               'record_id': record_id
           })
           
   def get_average_timing(self, metric_name: str) -> Optional[float]:
       if not self.current_run or metric_name not in self.current_run.timings:
           return None
       timings = self.current_run.timings[metric_name]
       return sum(timings) / len(timings) if timings else None
           
   def _log_metrics(self):
       if not self.current_run or not self.current_run.end_time:
           return
           
       duration = (self.current_run.end_time - self.current_run.start_time).total_seconds()
       total_records = self.current_run.records_processed + self.current_run.records_failed
       success_rate = (self.current_run.records_processed / total_records if total_records > 0 else 0)
       
       timing_stats = {
           name: f"{sum(times)/len(times):.3f}s avg" 
           for name, times in self.current_run.timings.items()
       }
       
       self.logger.info(
           f"ETL Run Metrics:\n"
           f"Duration: {duration:.2f}s\n"
           f"Records Processed: {self.current_run.records_processed}\n"
           f"Records Failed: {self.current_run.records_failed}\n"
           f"Success Rate: {success_rate:.2%}\n"
           f"Avg Processing Time: {self.current_run.avg_processing_time:.3f}s\n"
           f"Timing Metrics: {timing_stats}\n"
           f"Error Distribution: {dict(self.current_run.error_counts)}"
       )