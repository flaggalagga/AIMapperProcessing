import pytest
from unittest.mock import Mock, patch
from etl_processing.services.error_handler import (
    ErrorHandler, ETLError, ValidationError, MappingError, DatabaseError
)

@pytest.fixture
def logger():
    return Mock()

@pytest.fixture
def monitoring():
    return Mock()

@pytest.fixture
def error_handler(logger, monitoring):
    return ErrorHandler(logger, monitoring)

class TestErrorHandler:
    def test_handle_validation_error(self, error_handler):
        error = ValidationError("Invalid data", "validation", {"field": "test"})
        result = error_handler.handle_error(error, {"record_id": "123"})
        assert result is True
        error_handler.logger.warning.assert_called_once()
        error_handler.monitoring_service.record_error.assert_called_once()

    def test_handle_mapping_error(self, error_handler):
        error = MappingError("Mapping failed", "mapping", {"source": "test"})
        result = error_handler.handle_error(error, {"record_id": "123"})
        assert result is True
        error_handler.logger.warning.assert_called_once()

    def test_handle_database_error(self, error_handler):
        error = DatabaseError("DB error", "database", {"query": "test"})
        result = error_handler.handle_error(error, {"record_id": "123"})
        assert result is False
        error_handler.logger.error.assert_called_once()

    def test_error_history(self, error_handler):
        error = ValidationError("Test error", "validation")
        error_handler.handle_error(error, {"test": "context"})
        assert len(error_handler.error_history) == 1
        assert error_handler.error_counts["validation"] == 1

    def test_get_error_summary(self, error_handler):
        error_handler.error_history = [{"type": "validation"}, {"type": "mapping"}]
        error_handler.error_counts = {"validation": 1, "mapping": 1}
        summary = error_handler.get_error_summary()
        assert summary["total_errors"] == 2
        assert summary["error_counts"] == {"validation": 1, "mapping": 1}