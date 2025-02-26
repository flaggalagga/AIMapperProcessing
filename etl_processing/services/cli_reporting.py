# services/cli_reporting.py
"""Command-line reporting utilities for ETL processes."""
from typing import Dict
from datetime import datetime


class CLIReporter:
    """Handles command-line reporting of ETL metrics."""
    def __init__(self, logger):
        self.logger = logger

    def report_metrics(self, metrics: Dict):
        """Print formatted metrics report to console.
        
        Args:
            metrics: Dictionary of ETL metrics
        """
        report = f"""
ETL Process Report
-----------------
Duration: {self._format_duration(metrics['start_time'], metrics['end_time'])}
Records:
  - Processed: {metrics['processed']}
  - Failed: {metrics['failed']}
  - Success Rate: {metrics['success_rate']:.2f}%

Matching:
  - Direct Matches: {metrics['match_types']['direct']}
  - AI Matches: {metrics['match_types']['ai']}
  - Failed Matches: {metrics['match_types']['failed']}

Performance:
  - Avg Processing Time: {metrics['avg_processing_time']:.3f}s
  - Current Batch Size: {metrics['current_batch_size']}
"""
        self.logger.info(report)

    def _format_duration(self, start: datetime, end: datetime) -> str:
        """Format time duration for display.
        
        Args:
            start: Start timestamp
            end: End timestamp
            
        Returns:
            Formatted duration string
        """
        duration = end - start
        hours = duration.seconds // 3600
        minutes = (duration.seconds % 3600) // 60
        seconds = duration.seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"