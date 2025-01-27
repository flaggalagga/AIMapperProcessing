import pytest
from unittest.mock import Mock, patch, MagicMock
from etl_processing.etl.generic import GenericETL
from etl_processing.services.ai_matcher import MatchResult

@pytest.fixture
def sample_config(tmp_path):
    import yaml
    config = {
        'database': {
            'tables': {
                'source': {
                    'name': 'source',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'value_field': {'type': 'varchar(255)'},
                        'context_field': {'type': 'varchar(255)'},
                        'mapping_id': {'type': 'int unsigned', 'nullable': True}
                    }
                },
                'target': {
                    'name': 'target',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'name': {'type': 'varchar(255)'}
                    }
                },
                'junction': {
                    'name': 'junction_table',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'source_id': {'type': 'int unsigned'},
                        'target_id': {'type': 'int unsigned'}
                    }
                },
                'dictionary': {
                    'name': 'dictionary',
                    'columns': {
                        'id': {'type': 'int unsigned', 'primary': True},
                        'table_name': {'type': 'varchar(255)'},
                        'table_name_id': {'type': 'int unsigned'},
                        'name': {'type': 'varchar(255)'}
                    }
                }
            }
        },
        'etl_types': {
            'test': {
                'description': 'Test ETL',
                'table_name': 'target',
                'source_table': 'source',
                'dictionary_table': 'dictionary',
                'value_field': 'value_field',
                'mapping_id_field': 'mapping_id',
                'multiple_values': False,
                'context_fields': [
                    {'field': 'context_field', 'weight': 0.5}
                ],
                'validation': {
                    'skip_if_matches': '^\\d+$'
                }
            },
            'test_multi': {
                'description': 'Test Multi-Value ETL',
                'table_name': 'target',
                'source_table': 'source',
                'junction_table': 'junction',
                'dictionary_table': 'dictionary',
                'value_field': 'value_field',
                'multiple_values': True,
                'value_separator': '[/,]',
                'junction_mapping': {
                    'source_field': 'source_id',
                    'target_field': 'target_id'
                }
            }
        },
        'settings': {
            'batch_size': 10,
            'max_iterations': 1
        }
    }
    
    config_file = tmp_path / "test_config.yml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f)
    return str(config_file)

@pytest.fixture
def mock_models():
    source_model = Mock(name='SourceModel')
    source_model.id = Mock()
    source_model.value_field = Mock()
    source_model.context_field = Mock()
    
    target_model = Mock(name='TargetModel')
    target_model.id = Mock()
    target_model.name = Mock()
    
    junction_model = Mock(name='JunctionModel')
    junction_model.source_id = Mock()
    junction_model.target_id = Mock()
    
    dictionary_model = Mock(name='DictionaryModel')
    dictionary_model.table_name = Mock()
    dictionary_model.table_name_id = Mock()
    dictionary_model.name = Mock()
    
    return {
        'source': source_model,
        'target': target_model,
        'junction': junction_model,
        'dictionary': dictionary_model
    }

@pytest.fixture
def etl(sample_config, mock_models):
    with patch('etl_processing.etl.generic.DatabaseManager') as mock_db, \
         patch('etl_processing.etl.generic.AIMatcherService') as mock_ai, \
         patch('etl_processing.etl.generic.ModelFactory.load_models', return_value=mock_models):
        
        # Configure mock session
        session = MagicMock()
        mock_db.return_value.session_scope.return_value.__enter__.return_value = session
        mock_db.return_value.session_scope.return_value.__exit__.return_value = None
        
        return GenericETL('test', sample_config)

@pytest.fixture
def etl_multi(sample_config, mock_models):
    with patch('etl_processing.etl.generic.DatabaseManager') as mock_db, \
         patch('etl_processing.etl.generic.AIMatcherService') as mock_ai, \
         patch('etl_processing.etl.generic.ModelFactory.load_models', return_value=mock_models):
        
        # Configure mock session
        session = MagicMock()
        mock_db.return_value.session_scope.return_value.__enter__.return_value = session
        mock_db.return_value.session_scope.return_value.__exit__.return_value = None
        
        return GenericETL('test_multi', sample_config)

def test_process_record_with_direct_match(etl):
    with etl.db_manager.session_scope() as session:
        record = MagicMock()
        record.id = 1
        record.value_field = "test_value"
        
        # Setup direct match
        direct_match = Mock()
        direct_match.id = 2
        session.query.return_value.filter.return_value.first.return_value = direct_match
        
        result = etl._process_record(session, record)
        assert result is True

def test_process_record_with_ai_match(etl):
    with etl.db_manager.session_scope() as session:
        record = MagicMock()
        record.id = 1
        record.value_field = "test_value"
        
        # No direct match
        session.query.return_value.filter.return_value.first.return_value = None
        
        # Setup AI match
        match_result = MatchResult(id=0, confidence=0.95, matched_value="matched")
        etl.ai_matcher.find_best_match.return_value = match_result
        etl.id_map = {0: 3}
        
        result = etl._process_record(session, record)
        assert result is True

def test_process_record_multi_value(etl_multi):
    with etl_multi.db_manager.session_scope() as session:
        record = MagicMock()
        record.id = 1
        record.value_field = "value1/value2"
        
        # Setup matches
        match1 = Mock(id=2)
        match2 = Mock(id=3)
        session.query.return_value.filter.return_value.first.side_effect = [match1, match2]
        
        result = etl_multi._process_record(session, record)
        assert result is True
        assert session.add.call_count == 2

def test_process_record_with_validation(etl):
    with etl.db_manager.session_scope() as session:
        record = MagicMock()
        record.id = 1
        record.value_field = "123"  # Should be skipped by validation
        
        result = etl._process_record(session, record)
        assert result is False

def test_get_context(etl):
    record = MagicMock()
    record.context_field = "test_context"
    
    context = etl._get_context(record)
    assert context["context_field"]["value"] == "test_context"
    assert context["context_field"]["weight"] == 0.5

def test_find_direct_match_no_match(etl):
    with etl.db_manager.session_scope() as session:
        session.query.return_value.filter.return_value.first.return_value = None
        
        result = etl._find_direct_match(session, "test_value")
        assert result is None

def test_find_direct_match_in_dictionary(etl):
    with etl.db_manager.session_scope() as session:
        # No direct match in target table
        session.query.return_value.filter.return_value.first.side_effect = [
            None,  # No match in target table
            Mock(table_name_id=2)  # Match in dictionary
        ]
        
        result = etl._find_direct_match(session, "test_value")
        assert result == 2

def test_run_with_progress(etl):
    """Replace only the monitoring assertions in this test"""
    with etl.db_manager.session_scope() as session:
        # Setup records
        record = MagicMock()
        record.id = 1
        record.value_field = "test_value"
        
        # Configure session behavior
        session.query.return_value.filter.return_value.limit.return_value.all.return_value = [record]
        session.query.return_value.filter.return_value.first.return_value = Mock(id=2)
        
        # Mock monitoring properly
        etl.monitoring.start_run = Mock()
        etl.monitoring.end_run = Mock()
        
        etl.run()
        assert etl.monitoring.start_run.call_count == 1
        assert etl.monitoring.end_run.call_count == 1
   
def test_run_with_error(etl):
    """Replace only the monitoring assertions in this test"""
    with etl.db_manager.session_scope() as session:
        # Configure session to raise an error
        session.query.side_effect = Exception("Test error")
        
        # Mock monitoring and logger properly
        etl.monitoring.start_run = Mock()
        etl.monitoring.end_run = Mock()
        etl.logger = Mock()
        
        etl.run()
        assert etl.monitoring.start_run.call_count == 1
        assert etl.monitoring.end_run.call_count == 1
        assert etl.logger.error.call_count > 0

def test_process_record_add_synonym(etl):
    with etl.db_manager.session_scope() as session:
        record = MagicMock()
        record.id = 1
        record.value_field = "test_value"
        
        # No direct match, but AI match
        session.query.return_value.filter.return_value.first.return_value = None
        match_result = MatchResult(id=0, confidence=0.95, matched_value="matched")
        etl.ai_matcher.find_best_match.return_value = match_result
        etl.id_map = {0: 3}
        
        # Add synonym
        new_synonym = Mock()
        session.add.return_value = new_synonym
        
        result = etl._process_record(session, record)
        assert result is True
        session.add.assert_called()