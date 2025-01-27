import pytest
import logging
import os
from unittest.mock import patch, Mock
from etl_processing.services.logger import setup_logging

class TestLogger:
    def test_setup_logging_console(self):
        logger = setup_logging()
        assert logger.name == 'accidents_etl'
        assert logger.level == logging.INFO
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)

    def test_setup_logging_file(self, tmp_path):
        log_file = tmp_path / "test.log"
        logger = setup_logging(log_file=str(log_file))
        assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
        assert os.path.exists(log_file)

    def test_setup_logging_custom_level(self):
        logger = setup_logging(log_level='DEBUG')
        assert logger.level == logging.DEBUG

    def test_setup_logging_custom_format(self):
        custom_format = '%(levelname)s - %(message)s'
        logger = setup_logging(log_format=custom_format)
        handler = logger.handlers[0]
        assert handler.formatter._fmt == custom_format

    def test_setup_logging_invalid_level(self):
        with pytest.raises(AttributeError):
            setup_logging(log_level='INVALID')