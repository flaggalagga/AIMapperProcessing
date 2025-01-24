# /scripts/python/services/database.py
import os
from contextlib import contextmanager
import logging
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from typing import Optional, Any

class DatabaseManager:
    def __init__(self, logger=None, ai_matcher=None):
        load_dotenv()
        self.logger = logger or logging.getLogger(__name__)
        self.ai_matcher = ai_matcher

        db_user = os.getenv('MYSQL_USER')
        db_pass = os.getenv('MYSQL_PASSWORD')
        db_host = os.getenv('MYSQL_HOST')
        db_name = os.getenv('MYSQL_DATABASE')

        connection_string = f'mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}'
        
        try:
            self.engine = create_engine(
                connection_string,
                pool_size=10,
                max_overflow=20,
                pool_recycle=3600,
                pool_pre_ping=True,
                isolation_level='READ_COMMITTED'
            )
            self.Session = sessionmaker(bind=self.engine)
            self.logger.info("Database connection established successfully")
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {e}")
            raise

    def update_record(self, session: Any, record_id: int, table_name: str, field: str, value: Any, max_retries: int = 3) -> bool:
        """
        Update a record with retry logic
        
        :param session: Database session
        :param record_id: ID of record to update
        :param table_name: Name of table containing record
        :param field: Field to update
        :param value: New value
        :param max_retries: Maximum number of retry attempts
        :return: True if update successful, False otherwise
        """
        for attempt in range(max_retries):
            try:
                stmt = text(f"UPDATE {table_name} SET {field} = :value WHERE id = :id")
                session.execute(stmt, {'value': value, 'id': record_id})
                session.commit()
                self.logger.info(f"Successfully updated {table_name} record {record_id} {field} to {value}")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Retry {attempt + 1}/{max_retries} updating {table_name} record {record_id}: {e}")
                    session.rollback()
                    continue
                else:
                    self.logger.error(f"Failed to update {table_name} record {record_id} after {max_retries} attempts: {e}")
                    session.rollback()
                    raise
        return False
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            session.close()

    def query_table(self, session: Any, model: Any, filters: Optional[list] = None, limit: Optional[int] = None):
        """
        Generic query method for any table
        
        :param session: Database session
        :param model: SQLAlchemy model class
        :param filters: List of filter conditions
        :param limit: Maximum number of records to return
        :return: Query object
        """
        query = session.query(model)
        
        if filters:
            for filter_condition in filters:
                query = query.filter(filter_condition)
                
        if limit:
            query = query.limit(limit)
            
        return query

    def add_synonym(self, session: Any, synonym_model: Any, value: str, target_table: str, target_id: int, message: Optional[str] = None) -> bool:
        """
        Add a new synonym
        
        :param session: Database session
        :param synonym_model: Synonym model class
        :param value: Synonym value
        :param target_table: Target table name
        :param target_id: Target record ID
        :param message: Optional message (e.g., for AI matches)
        :return: True if successful, False otherwise
        """
        try:
            # Check for existing synonym
            existing = session.query(synonym_model).filter(
                synonym_model.table_name == target_table,
                func.trim(synonym_model.name).collate('utf8mb4_general_ci') == value.strip()
            ).first()
            
            if not existing:
                new_synonym = synonym_model(
                    table_name=target_table,
                    table_name_id=target_id,
                    name=value.strip(),
                    ai_match_message=message
                )
                session.add(new_synonym)
                session.flush()
                self.logger.info(f"Added new synonym: '{value}' -> {target_table}.{target_id}")
                return True
                
            self.logger.info(f"Synonym already exists: '{value}' -> {target_table}.{target_id}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error adding synonym: {e}")
            session.rollback()
            return False