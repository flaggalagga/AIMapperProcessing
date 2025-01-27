import pytest
from unittest.mock import patch, Mock
import sys
from etl_processing.main import main

@pytest.fixture
def mock_config():
    return {
        'etl_types': {
            'test_type': {
                'description': 'Test ETL'
            }
        }
    }

def test_main_no_args():
    with patch('sys.argv', ['main.py']):
        with patch('yaml.safe_load', return_value={'etl_types': {'test': {'description': 'Test'}}}):
            main()

def test_main_invalid_type():
    with patch('sys.argv', ['main.py', 'invalid_type']):
        with patch('yaml.safe_load', return_value={'etl_types': {'test': {'description': 'Test'}}}):
            main()

def test_main_valid_type(mock_config):
    with patch('sys.argv', ['main.py', 'test_type']):
        with patch('yaml.safe_load', return_value=mock_config):
            with patch('etl_processing.main.GenericETL') as mock_etl:
                main()
                mock_etl.assert_called_once()

def test_main_config_error():
    with patch('sys.argv', ['main.py']):
        with patch('yaml.safe_load', side_effect=Exception("Config error")):
            main()

def test_main_etl_error(mock_config):
    with patch('sys.argv', ['main.py', 'test_type']):
        with patch('yaml.safe_load', return_value=mock_config):
            with patch('etl_processing.main.GenericETL', side_effect=Exception("ETL error")):
                main()