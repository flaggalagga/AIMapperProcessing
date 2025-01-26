# services/monitoring.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List
from .cli_reporting import CLIReporter

@dataclass
class ETLMetrics:
    start_time: datetime
    end_time: datetime = None
    records_processed: int = 0
    records_failed: int = 0
    direct_matches: int = 0
    ai_matches: int = 0
    processing_times: List[float] = field(default_factory=list)
    batch_sizes: List[int] = field(default_factory=list)
    error_counts: Dict[str, int] = field(default_factory=dict)
    timings: Dict[str, List[float]] = field(default_factory=dict)

    @property
    def avg_processing_time(self) -> float:
        return sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0

class MonitoringService:
    def __init__(self, logger):
        self.logger = logger
        self.reporter = CLIReporter(logger)
        self.current_run = None
        self.error_history = []

    def start_run(self):
        self.current_run = ETLMetrics(start_time=datetime.now())

    def end_run(self):
        if self.current_run:
            self.current_run.end_time = datetime.now()
            self.reporter.report_metrics(self.get_stats())

    def record_success(self, processing_time: float):
        if self.current_run:
            self.current_run.records_processed += 1
            self.current_run.processing_times.append(processing_time)

    def record_error(self, error_type: str, error_msg: str, record_id: str = None):
        if self.current_run:
            self.current_run.records_failed += 1
            self.current_run.error_counts[error_type] = self.current_run.error_counts.get(error_type, 0) + 1
            self.error_history.append({
                'timestamp': datetime.now(),
                'type': error_type,
                'message': error_msg,
                'record_id': record_id
            })

    def record_match(self, match_type: str):
        if not self.current_run:
            return
        if match_type == 'direct':
            self.current_run.direct_matches += 1
        elif match_type == 'ai':
            self.current_run.ai_matches += 1

    def record_timing(self, metric_name: str, duration: float):
        if self.current_run:
            if metric_name not in self.current_run.timings:
                self.current_run.timings[metric_name] = []
            self.current_run.timings[metric_name].append(duration)

    def update_batch_size(self, size: int):
        if self.current_run:
            self.current_run.batch_sizes.append(size)

    def get_stats(self) -> Dict:
        if not self.current_run:
            return {}

        total_records = self.current_run.records_processed + self.current_run.records_failed
        return {
            'start_time': self.current_run.start_time,
            'end_time': self.current_run.end_time,
            'processed': self.current_run.records_processed,
            'failed': self.current_run.records_failed,
            'success_rate': (self.current_run.records_processed / total_records * 100 
                           if total_records > 0 else 0),
            'avg_processing_time': self.current_run.avg_processing_time,
            'current_batch_size': self.current_run.batch_sizes[-1] 
                                if self.current_run.batch_sizes else 1000,
            'match_types': {
                'direct': self.current_run.direct_matches,
                'ai': self.current_run.ai_matches,
                'failed': self.current_run.records_failed
            }
        }