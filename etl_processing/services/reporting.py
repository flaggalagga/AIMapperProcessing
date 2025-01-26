# services/reporting.py
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime
import json

class ETLReport:
   def __init__(self, logger):
       self.logger = logger

   def _format_timings(self, timings: Dict[str, List[float]]) -> str:
        result = []
        for name, times in timings.items():
            avg = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            result.append(f"{name}:\n  avg={avg:.3f}s min={min_time:.3f}s max={max_time:.3f}s")
        return "\n".join(result)

   def _format_errors(self, error_counts: Dict[str, int]) -> str:
       return "\n".join(f"{error}: {count}" for error, count in error_counts.items())

   def generate_summary(self, metrics) -> str:
       duration = (metrics.end_time - metrics.start_time).total_seconds()
       total_records = metrics.records_processed + metrics.records_failed
       success_rate = metrics.records_processed/total_records if total_records > 0 else 0
       
       return f"""
ETL Summary Report
-----------------
Start Time: {metrics.start_time}
End Time: {metrics.end_time}
Duration: {duration:.2f}s

Records:
 Processed: {metrics.records_processed}
 Failed: {metrics.records_failed}
 Success Rate: {success_rate:.2%}

Timing Statistics:
{self._format_timings(metrics.timings)}

Error Distribution:
{self._format_errors(metrics.error_counts)}
"""

   def save_metrics(self, metrics):
       metrics_file = f"reports/metrics/metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
       with open(metrics_file, 'w') as f:
           json.dump({
               'timings': {
                   name: {
                       'avg': sum(times)/len(times),
                       'min': min(times),
                       'max': max(times),
                       'count': len(times)
                   } for name, times in metrics.timings.items()
               },
               'error_counts': metrics.error_counts,
               'batch_metrics': {
                   'records_processed': metrics.records_processed,
                   'records_failed': metrics.records_failed,
                   'avg_processing_time': metrics.avg_processing_time,
                   'total_duration': (metrics.end_time - metrics.start_time).total_seconds()
               }
           }, f, indent=2, default=str)

   def save_summary(self, metrics):
       summary_file = f"reports/summaries/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
       with open(summary_file, 'w') as f:
           json.dump({
               'start_time': metrics.start_time.isoformat(),
               'end_time': metrics.end_time.isoformat() if metrics.end_time else None,
               'duration': (metrics.end_time - metrics.start_time).total_seconds(),
               'success_rate': metrics.records_processed/(metrics.records_processed + metrics.records_failed)
                   if (metrics.records_processed + metrics.records_failed) > 0 else 0,
               'error_summary': metrics.error_counts,
               'performance': {
                   name: {'avg': sum(times)/len(times)}
                   for name, times in metrics.timings.items()
               }
           }, f, indent=2, default=str)

   def get_metrics_summary(self, metrics) -> Dict:
       return {
           'total_processed': metrics.records_processed,
           'total_failed': metrics.records_failed,
           'success_rate': metrics.records_processed/(metrics.records_processed + metrics.records_failed)
               if (metrics.records_processed + metrics.records_failed) > 0 else 0,
           'avg_processing_time': metrics.avg_processing_time
       }