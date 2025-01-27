import pytest
from unittest.mock import Mock
from etl_processing.services.timing import timing_metric

def test_timing_decorator():
    monitoring = Mock()
    class TestClass:
        def __init__(self):
            self.monitoring = monitoring

        @timing_metric("test_metric")
        def test_method(self):
            return "result"

    instance = TestClass()
    result = instance.test_method()

    assert result == "result"
    monitoring.record_timing.assert_called_once()
    metric_name, duration = monitoring.record_timing.call_args[0]
    assert metric_name == "test_metric"
    assert isinstance(duration, float)