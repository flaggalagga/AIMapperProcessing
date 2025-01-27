import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from etl_processing.services.reporting import ETLReport

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def report(mock_logger):
    return ETLReport(mock_logger)

@pytest.fixture
def mock_metrics():
    metrics = Mock()
    metrics.start_time = datetime(2024, 1, 1, 10, 0)
    metrics.end_time = datetime(2024, 1, 1, 10, 1)
    metrics.records_processed = 100
    metrics.records_failed = 10
    metrics.timings = {'op': [1.0, 2.0]}
    metrics.error_counts = {'error': 5}
    metrics.avg_processing_time = 1.5
    return metrics

def test_format_timings(report):
    timings = {
        'operation1': [1.0, 2.0, 3.0],
        'operation2': [0.5, 1.5]
    }
    result = report._format_timings(timings)
    assert 'operation1' in result
    assert 'operation2' in result

def test_format_errors(report):
    errors = {'validation': 5, 'mapping': 3}
    result = report._format_errors(errors)
    assert 'validation: 5' in result
    assert 'mapping: 3' in result

def test_generate_summary(report, mock_metrics):
    result = report.generate_summary(mock_metrics)
    assert "ETL Summary Report" in result
    assert "Records:" in result
    assert "Timing Statistics:" in result
    assert "Error Distribution:" in result

def test_save_metrics(report, mock_metrics, tmp_path):
    with patch('builtins.open', create=True) as mock_open:
        report.save_metrics(mock_metrics)
        mock_open.assert_called_once()

def test_get_metrics_summary(report, mock_metrics):
    summary = report.get_metrics_summary(mock_metrics)
    assert summary['total_processed'] == 100
    assert summary['total_failed'] == 10
    assert summary['avg_processing_time'] == 1.5
    assert 'success_rate' in summary