# tests/test_monitoring.py
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from etl_processing.services.monitoring import MonitoringService, ETLMetrics
from etl_processing.services.error_handler import ErrorHandler
from etl_processing.services.batch_optimizer import BatchOptimizer

@pytest.fixture
def logger():
    return Mock()

@pytest.fixture
def monitoring_service(logger):
    return MonitoringService(logger)

@pytest.fixture
def error_handler(logger):
    return ErrorHandler(logger)

@pytest.fixture
def batch_optimizer():
    return BatchOptimizer(initial_size=1000)

class TestMonitoring:
    def test_metrics_tracking(self, monitoring_service):
        monitoring_service.start_run()
        
        monitoring_service.record_success(0.5)
        monitoring_service.record_success(0.7)
        monitoring_service.record_error('validation', 'Invalid data', '123')
        monitoring_service.record_timing('processing', 0.5)
        monitoring_service.record_timing('processing', 0.7)
        
        monitoring_service.end_run()
        
        metrics = monitoring_service.current_run
        assert metrics.records_processed == 2
        assert metrics.records_failed == 1
        assert 'validation' in metrics.error_counts
        assert len(metrics.timings['processing']) == 2
        assert 0.5 < metrics.avg_processing_time < 0.7

    def test_error_handling(self, error_handler):
        error = ValueError("Test error")
        context = {'record_id': '123'}
        
        error_type = error_handler._classify_error(error)
        assert error_type == 'unknown'
        
        handled = error_handler.handle_error(error, context)
        assert not handled
        assert len(error_handler.error_history) == 1
        assert error_handler.error_counts['unknown'] == 1

    def test_batch_optimization(self, batch_optimizer):
        # Test size increase on good performance
        new_size = batch_optimizer.adjust_size(success_rate=0.98, processing_time=20)
        assert new_size > 1000
        
        # Test size decrease on poor performance
        new_size = batch_optimizer.adjust_size(success_rate=0.75, processing_time=70)
        assert new_size < 1000
        
        stats = batch_optimizer.get_stats()
        assert stats['size_changes'] == 2
        assert 'avg_success_rate' in stats
        assert 'avg_processing_time' in stats

@pytest.mark.integration
class TestIntegrationMonitoring:
    def test_full_etl_monitoring(self, logger):
        monitoring = MonitoringService(logger)
        error_handler = ErrorHandler(logger, monitoring)
        batch_optimizer = BatchOptimizer()
        
        monitoring.start_run()
        
        for i in range(5):
            if i % 2 == 0:
                monitoring.record_success(0.5)
                monitoring.record_timing('processing', 0.5)
            else:
                error = ValueError(f"Error in record {i}")
                error_handler.handle_error(error, {'record_id': str(i)})
                monitoring.record_error('validation', f'Error in record {i}', str(i))
                monitoring.record_timing('processing', 1.0)
            
            success_rate = monitoring.current_run.records_processed / (i + 1)
            batch_size = batch_optimizer.adjust_size(success_rate, 0.75)
        
        monitoring.end_run()
        
        metrics = monitoring.current_run
        assert metrics.records_processed + metrics.records_failed == 5
        assert len(monitoring.error_history) > 0
        assert error_handler.get_error_summary()['total_errors'] > 0
        assert batch_optimizer.get_stats()['size_changes'] == 5