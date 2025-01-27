import pytest
from unittest.mock import Mock, patch, MagicMock
from etl_processing.etl.generic import GenericETL
from etl_processing.services.database import DatabaseManager
from etl_processing.services.ai_matcher import AIMatcherService, MatchResult

@pytest.fixture
def sample_config(tmp_path):
    config = {
        'database': {
            'tables': {
                'source_table': {
                    'name': 'source_table',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'value_field': {'type': 'varchar(255)'},
                        'mapping_id': {'type': 'int unsigned'}
                    }
                },
                'target_table': {
                    'name': 'target_table',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'name': {'type': 'varchar(255)'}
                    }
                },
                'dictionary_table': {
                    'name': 'dictionary_table',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True, 'auto_increment': True},
                        'table_name': {'type': 'varchar(255)'},
                        'table_name_id': {'type': 'int unsigned'},
                        'name': {'type': 'varchar(255)'},
                        'ai_match_message': {'type': 'text', 'nullable': True},
                        'created': {'type': 'timestamp', 'nullable': True},
                        'modified': {'type': 'timestamp', 'nullable': True}
                    }
                }
            }
        },
        'etl_types': {
            'test_etl': {
                'description': 'Test ETL Process',
                'table_name': 'target_table',
                'source_table': 'source_table',
                'dictionary_table': 'dictionary_table',
                'value_field': 'value_field',
                'mapping_id_field': 'mapping_id',
                'multiple_values': False
            }
        },
        'settings': {
            'batch_size': 10,
            'max_iterations': 1,
            'progress_interval': 5
        }
    }
    
    import yaml
    config_file = tmp_path / "test_config.yml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f)
    return str(config_file)

@pytest.fixture
def mock_session():
    session = MagicMock()
    session.query.return_value.filter.return_value.exists.return_value = False
    session.query.return_value.filter.return_value.limit.return_value.all.return_value = []
    session.query.return_value.filter.return_value.first.return_value = None
    return session

@pytest.fixture
def etl(sample_config):
    with patch('etl_processing.etl.generic.DatabaseManager') as mock_db:
        with patch('etl_processing.etl.generic.AIMatcherService') as mock_ai:
            mock_session = Mock()
            mock_query = Mock()
            mock_query.all.return_value = [(1, "test_option"), (2, "another_option")]
            mock_session.query.return_value = mock_query
            mock_db.return_value.session_scope.return_value.__enter__.return_value = mock_session
            mock_ai.return_value.find_best_match.return_value = None
            return GenericETL('test_etl', sample_config)

class TestGenericETL:
    def test_initialization(self, etl):
        assert etl.etl_type == 'test_etl'
        assert etl.etl_config['table_name'] == 'target_table'
        assert etl.etl_config['source_table'] == 'source_table'

    def test_get_unmapped_records(self, etl, mock_session):
        records = etl.get_unmapped_records(mock_session, 10)
        mock_session.query.assert_called_once()
        
    @patch('etl_processing.etl.generic.AIMatcherService')
    def test_process_record_with_direct_match(self, mock_ai, etl, mock_session):
        record = Mock()
        record.id = 1
        setattr(record, etl.etl_config['value_field'], "test_value")
        
        mock_match = Mock()
        mock_match.id = 100
        mock_session.query.return_value.filter.return_value.first.return_value = mock_match
        
        result = etl._process_record(mock_session, record)
        assert result is True
        
    def test_split_values(self, etl):
        result = etl._split_values("single_value")
        assert result == ["single_value"]
        
        etl.etl_config['multiple_values'] = True
        etl.etl_config['value_separator'] = '[/,]'
        result = etl._split_values("value1/value2,value3")
        assert len(result) == 3
        assert "value1" in result
        
    def test_find_direct_match(self, etl, mock_session):
        result = etl._find_direct_match(mock_session, "test_value")
        assert result is None
        mock_session.query.assert_called()
        
    @patch('etl_processing.etl.generic.AIMatcherService')
    def test_process_record_with_ai_match(self, mock_ai, etl, mock_session):
        record = Mock()
        record.id = 1
        setattr(record, etl.etl_config['value_field'], "test_value")
        
        match_result = MatchResult(id=200, confidence=0.95, matched_value="matched_value")
        etl.ai_matcher.find_best_match.return_value = match_result
        etl.id_map = {200: 200}
        
        # Mock the add_synonym behavior
        mock_synonym = Mock()
        mock_session.add = Mock()
        mock_session.commit = Mock()
        
        result = etl._process_record(mock_session, record)
        assert result is True
        
    def test_is_valid_value(self, etl):
        assert etl.is_valid_value("test") is True
        assert etl.is_valid_value("") is False
        assert etl.is_valid_value(None) is False
        
        etl.etl_config['validation'] = {'skip_if_matches': '^\\d+$'}
        assert etl.is_valid_value("123") is False
        assert etl.is_valid_value("abc") is True