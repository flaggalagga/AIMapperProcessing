import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from etl_processing.services.database import DatabaseManager

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def mock_session_factory():
    session = MagicMock()
    session.commit = Mock()
    session.rollback = Mock()
    session.close = Mock()
    session.query = Mock()
    session.execute = Mock()
    return lambda: session

@pytest.fixture
def db_manager(mock_logger, mock_session_factory):
    with patch('etl_processing.services.database.create_engine'), \
         patch('etl_processing.services.database.sessionmaker') as mock_sessionmaker, \
         patch('etl_processing.services.database.load_dotenv'), \
         patch.dict('os.environ', {
             'MYSQL_USER': 'test',
             'MYSQL_PASSWORD': 'test',
             'MYSQL_HOST': 'test',
             'MYSQL_DATABASE': 'test'
         }):
        mock_sessionmaker.return_value = mock_session_factory
        return DatabaseManager(logger=mock_logger)

def test_session_scope_success(db_manager):
    with db_manager.session_scope() as session:
        session.query("test")
    assert session.commit.call_count == 1
    assert session.close.call_count == 1

def test_session_scope_error(db_manager):
    with pytest.raises(Exception):
        with db_manager.session_scope() as session:
            raise Exception("Test error")
    assert session.rollback.call_count == 1
    assert session.close.call_count == 1

def test_update_record_failure(db_manager):
    with db_manager.session_scope() as session:
        session.execute.side_effect = SQLAlchemyError()
        
        result = db_manager.update_record(
            session=session,
            record_id=1,
            table_name="test_table",
            field="test_field",
            value="test_value",
            max_retries=1
        )
        
        assert result is False
        assert session.execute.call_count == 1
        assert session.rollback.call_count == 1

def test_query_table_with_filters(db_manager):
    with db_manager.session_scope() as session:
        model = Mock()
        filters = [Mock(name='filter1'), Mock(name='filter2')]
        
        query = Mock()
        session.query.return_value = query
        query.filter.return_value = query
        
        result = db_manager.query_table(session, model, filters=filters, limit=10)
        
        session.query.assert_called_once_with(model)
        assert query.filter.call_count == len(filters)
        query.limit.assert_called_once_with(10)

def test_init_connection_failure(mock_logger):
    """Test database connection failure during initialization"""
    with patch('etl_processing.services.database.create_engine') as mock_engine, \
         patch('etl_processing.services.database.load_dotenv'):
        mock_engine.side_effect = Exception("Connection failed")
        with pytest.raises(Exception) as exc_info:
            DatabaseManager(logger=mock_logger)
        assert "Connection failed" in str(exc_info.value)
        mock_logger.error.assert_called_once()

def test_update_record_retry_success(db_manager):
    """Test successful record update after retries"""
    with db_manager.session_scope() as session:
        # First two attempts fail, third succeeds
        session.execute.side_effect = [
            SQLAlchemyError("First attempt"),
            SQLAlchemyError("Second attempt"),
            None
        ]
        
        result = db_manager.update_record(
            session=session,
            record_id=1,
            table_name="test_table",
            field="test_field",
            value="test_value",
            max_retries=3
        )
        
        assert result is True
        assert session.execute.call_count == 3
        assert session.rollback.call_count == 2
        assert session.commit.call_count == 1

def test_update_record_all_retries_fail(db_manager):
    """Test record update when all retries fail"""
    with db_manager.session_scope() as session:
        session.execute.side_effect = SQLAlchemyError("Database error")
        
        result = db_manager.update_record(
            session=session,
            record_id=1,
            table_name="test_table",
            field="test_field",
            value="test_value",
            max_retries=3
        )
        
        assert result is False
        assert session.execute.call_count == 3
        assert session.rollback.call_count == 3
        assert db_manager.logger.error.call_count == 1

def test_add_synonym_success(db_manager):
    """Test successful synonym addition"""
    with db_manager.session_scope() as session:
        # Mock the synonym model
        synonym_model = Mock()
        session.query.return_value.filter.return_value.first.return_value = None
        
        result = db_manager.add_synonym(
            session=session,
            synonym_model=synonym_model,
            value="test_value",
            target_table="test_table",
            target_id=1,
            message="AI match"
        )
        
        assert result is True
        assert session.add.call_count == 1
        assert session.flush.call_count == 1

def test_add_synonym_existing(db_manager):
    """Test adding an existing synonym"""
    with db_manager.session_scope() as session:
        synonym_model = Mock()
        session.query.return_value.filter.return_value.first.return_value = Mock()
        
        result = db_manager.add_synonym(
            session=session,
            synonym_model=synonym_model,
            value="test_value",
            target_table="test_table",
            target_id=1
        )
        
        assert result is False
        assert session.add.call_count == 0
        assert db_manager.logger.info.call_count >= 1

def test_add_synonym_error(db_manager):
    """Test error handling in synonym addition"""
    with db_manager.session_scope() as session:
        synonym_model = Mock()
        session.query.side_effect = SQLAlchemyError("Database error")
        
        result = db_manager.add_synonym(
            session=session,
            synonym_model=synonym_model,
            value="test_value",
            target_table="test_table",
            target_id=1
        )
        
        assert result is False
        assert session.rollback.call_count == 1
        assert db_manager.logger.error.call_count == 1