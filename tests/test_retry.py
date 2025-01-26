# tests/test_retry.py
import pytest
import logging
from unittest.mock import Mock, patch
import time
from etl_processing.services.retry import with_retry, RetryTracker, RetryableError

@pytest.fixture
def logger():
    return Mock(spec=logging.Logger)

@pytest.fixture
def retry_tracker():
    return RetryTracker()

class TestRetry:
    def test_basic_retry(self, logger):
        attempts = 0
        
        @with_retry(max_attempts=3, delay=0.1, logger=logger)
        def operation():
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise ValueError("Temporary error")
            return "success"
            
        result = operation()
        assert result == "success"
        assert attempts == 2
        assert logger.warning.called
        assert not logger.error.called

    def test_exponential_backoff(self, logger):
        attempts = []
        
        @with_retry(max_attempts=3, delay=0.1, backoff_factor=2, logger=logger)
        def operation():
            attempts.append(time.time())
            raise ValueError("Always fails")
            
        with pytest.raises(ValueError):
            operation()
            
        delays = [attempts[i+1] - attempts[i] for i in range(len(attempts)-1)]
        assert 0.1 <= delays[0] <= 0.2  # Initial delay
        assert 0.2 <= delays[1] <= 0.4  # Doubled delay

    def test_max_delay(self, logger):
        attempts = []
        
        @with_retry(max_attempts=3, delay=1.0, backoff_factor=10, max_delay=2.0, logger=logger)
        def operation():
            attempts.append(time.time())
            raise ValueError("Always fails")
            
        with pytest.raises(ValueError):
            operation()
            
        delays = [attempts[i+1] - attempts[i] for i in range(len(attempts)-1)]
        assert all(d <= 2.1 for d in delays)  # Allow small timing variance

    def test_retry_tracker(self, retry_tracker):
        @with_retry(max_attempts=2, delay=0.1)
        def successful_operation():
            return "success"
            
        @with_retry(max_attempts=2, delay=0.1)
        def failing_operation():
            raise ValueError("Always fails")
            
        successful_operation()
        retry_tracker.record_attempt("successful_operation", True)
        
        with pytest.raises(ValueError):
            failing_operation()
        retry_tracker.record_attempt("failing_operation", False)
        
        stats = retry_tracker.get_stats()
        assert stats["successful_operation"]["success_rate"] == 1.0
        assert stats["failing_operation"]["success_rate"] == 0.0

    def test_custom_exceptions(self, logger):
        @with_retry(max_attempts=2, delay=0.1, exceptions=(ValueError,), logger=logger)
        def operation():
            raise KeyError("Different error")
            
        with pytest.raises(KeyError):
            operation()
        assert not logger.warning.called

    def test_retryable_error(self, logger):
        @with_retry(max_attempts=2, delay=0.1, exceptions=(RetryableError,), logger=logger)
        def operation():
            raise RetryableError("Retry me")
            
        with pytest.raises(RetryableError):
            operation()
        assert logger.warning.called