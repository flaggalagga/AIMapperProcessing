import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from etl_processing.services.error_handler import (
    ErrorHandler, ETLError, ValidationError,
    MappingError, DatabaseError, with_error_handling
)

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def mock_monitoring():
    return Mock()

@pytest.fixture
def error_handler(mock_logger, mock_monitoring):
    return ErrorHandler(mock_logger, mock_monitoring)

def test_handle_validation_error(error_handler):
    error = ValidationError("Invalid data", "validation")
    context = {"record_id": "123"}
    
    result = error_handler.handle_error(error, context)
    assert result is True
    error_handler.logger.warning.assert_called_once()
    error_handler.monitoring_service.record_error.assert_called_once()
    assert len(error_handler.error_history) == 1
    assert error_handler.error_counts["validation"] == 1

def test_handle_mapping_error(error_handler):
    error = MappingError("Mapping failed", "mapping")
    result = error_handler.handle_error(error)
    assert result is True
    error_handler.logger.warning.assert_called_once()

def test_handle_database_error(error_handler):
    error = DatabaseError("DB error", "database")
    result = error_handler.handle_error(error)
    assert result is False
    error_handler.logger.error.assert_called_once()

def test_error_handler_decorator(mock_monitoring):
    @with_error_handling("test_operation", mock_monitoring)
    def test_function(succeed: bool):
        if not succeed:
            raise ValidationError("Test error", "validation")
        return "success"
    
    # Test successful execution
    assert test_function(True) == "success"
    assert mock_monitoring.record_error.call_count == 0
    
    # Test error handling
    with pytest.raises(ValidationError):
        test_function(False)
    mock_monitoring.record_error.assert_called_once()

@pytest.mark.integration
def test_error_handler_save_report(error_handler, tmp_path):
    error_handler.handle_error(ValidationError("Test error", "validation"))
    error_handler.handle_error(MappingError("Another error", "mapping"))
    
    with patch('builtins.open', create=True) as mock_file, \
         patch('json.dump') as mock_json_dump:
        error_handler.save_error_report()
        mock_file.assert_called_once()
        mock_json_dump.assert_called_once()
        
        # Verify the structure of data passed to json.dump
        args, kwargs = mock_json_dump.call_args
        assert 'error_counts' in args[0]
        assert 'errors' in args[0]
        assert len(args[0]['errors']) == 2
        assert args[0]['error_counts']['validation'] == 1
        assert args[0]['error_counts']['mapping'] == 1

def test_handle_unknown_error(error_handler):
    """Test handling of unknown error types"""
    error = Exception("Unknown error")
    result = error_handler.handle_error(error)
    
    assert result is False
    assert error_handler.error_counts['unknown'] == 1
    assert len(error_handler.error_history) == 1
    assert error_handler.error_history[0]['type'] == 'unknown'

def test_get_error_summary(error_handler):
    """Test getting error summary"""
    error_handler.handle_error(ValidationError("Error 1", "validation"))
    error_handler.handle_error(MappingError("Error 2", "mapping"))
    error_handler.handle_error(ValidationError("Error 3", "validation"))
    
    summary = error_handler.get_error_summary()
    assert summary['total_errors'] == 3
    assert summary['error_counts']['validation'] == 2
    assert summary['error_counts']['mapping'] == 1