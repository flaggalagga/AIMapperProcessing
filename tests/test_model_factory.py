import pytest
import yaml
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
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

@pytest.fixture
def new_base():
    return declarative_base()

class TestModelFactory:
    def test_create_model(self, sample_table_config, new_base):
        ModelFactory.Base = new_base
        model = ModelFactory.create_model('test', sample_table_config)
        assert model.__tablename__ == 'test_table'
        assert hasattr(model, 'id')
        assert hasattr(model, 'name')

    def test_load_models(self, tmp_path):
        config = {
            'database': {
                'tables': {
                    'unique_table': {
                        'name': 'unique_test_table',
                        'columns': {
                            'id': {'type': 'int unsigned', 'primary': True}
                        }
                    }
                }
            }
        }
        
        config_file = tmp_path / "config.yml"
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
            
        ModelFactory.Base = declarative_base()
        models = ModelFactory.load_models(str(config_file))
        assert 'unique_table' in models