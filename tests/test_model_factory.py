# tests/test_model_factory.py
import pytest
from etl_processing.lib.model_factory import ModelFactory

@pytest.fixture
def sample_table_config():
    return {
        'name': 'test_table',
        'columns': {
            'id': {
                'type': 'int unsigned',
                'primary': True,
                'auto_increment': True
            },
            'name': {
                'type': 'varchar(255)',
                'nullable': False
            }
        }
    }

class TestModelFactory:
    def test_create_model(self, sample_table_config):
        model = ModelFactory.create_model('test', sample_table_config)
        assert model.__tablename__ == 'test_table'
        assert hasattr(model, 'id')
        assert hasattr(model, 'name')

    def test_load_models(self, tmp_path):
        config = {
            'database': {
                'tables': {
                    'test': {
                        'name': 'test_table',
                        'columns': {
                            'id': {'type': 'int unsigned', 'primary': True}
                        }
                    }
                }
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(config)
        models = ModelFactory.load_models(str(config_file))
        assert 'test' in models