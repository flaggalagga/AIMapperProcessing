import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError
from etl_processing.services.database import DatabaseManager

@pytest.fixture
def mock_logger():
    return Mock()

@pytest.fixture
def mock_session():
    session = MagicMock()
    return session

@pytest.fixture
def db_manager(mock_logger):
    with patch('etl_processing.services.database.create_engine'), \
         patch('etl_processing.services.database.sessionmaker'), \
         patch('etl_processing.services.database.load_dotenv'), \
         patch.dict('os.environ', {
             'MYSQL_USER': 'test',
             'MYSQL_PASSWORD': 'test',
             'MYSQL_HOST': 'test',
             'MYSQL_DATABASE': 'test'
         }):
        return DatabaseManager(logger=mock_logger)

def test_query_table_with_filters(db_manager, mock_session):
    model = Mock()
    filters = [Mock(name='filter1'), Mock(name='filter2')]
    query = MagicMock()
    query.filter.return_value = query
    mock_session.query.return_value = query
    
    db_manager.query_table(mock_session, model, filters=filters, limit=10)
    
    filter_calls = query.filter.call_args_list
    assert len(filter_calls) == len(filters)
    query.limit.assert_called_once_with(10)

