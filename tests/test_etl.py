# tests/test_etl.py
import pytest
from etl_processing.etl.generic import GenericETL
from unittest.mock import Mock, patch

@pytest.fixture
def sample_config():
    return {
        'etl_types': {
            'snow': {
                'description': 'Snow conditions',
                'table_name': 'tga_neiges',
                'source_table': 'accidents',
                'value_field': 'EtatNeige',
                'mapping_id_field': 'etatneige_id',
                'multiple_values': False
            }
        },
        'settings': {
            'batch_size': 10,
            'max_iterations': 1
        }
    }

@pytest.fixture
def mock_model():
    model = Mock()
    model.id = Mock()
    model.name = Mock()
    return model

class TestGenericETL:
    def test_init_config(self, sample_config, tmp_path):
        config_file = tmp_path / "config.yml"
        config_file.write_text(sample_config)
        etl = GenericETL('snow', str(config_file))
        assert etl.etl_type == 'snow'
        assert etl.settings['batch_size'] == 10

    def test_process_record_single_value(self, mock_model):
        with patch('etl_processing.etl.generic.GenericETL._find_direct_match') as mock_match:
            mock_match.return_value = 1
            # Add test implementation

    def test_process_record_multiple_values(self, mock_model):
        with patch('etl_processing.etl.generic.GenericETL._find_direct_match') as mock_match:
            mock_match.return_value = 1
            # Add test implementation