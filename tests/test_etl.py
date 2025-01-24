# tests/test_etl.py
import pytest
import yaml
from etl_processing.etl.generic import GenericETL
from unittest.mock import Mock, patch

@pytest.fixture
def sample_config():
    return {
        'database': {
            'tables': {
                'accidents': {
                    'name': 'accidents',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True}
                    }
                },
                'tga_neiges': {
                    'name': 'tga_neiges',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True}
                    }
                }
            }
        },
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