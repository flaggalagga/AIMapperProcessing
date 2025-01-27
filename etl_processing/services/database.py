# /scripts/python/services/database.py
"""Database connection and operations manager."""

import os
from contextlib import contextmanager
import logging
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from typing import Optional, Any

class DatabaseManager:
    """Manages database connections and CRUD operations."""

    def __init__(self, logger: Optional[logging.Logger] = None, ai_matcher: Any = None) -> None:
        """Initialize database connection using environment variables.

        Args:
            logger: Optional custom logger
            ai_matcher: Optional AI matching service
        """
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

    def update_record(
        self, 
        session: Any, 
        record_id: int, 
        table_name: str, 
        field: str, 
        value: Any, 
        max_retries: int = 3
    ) -> bool:
        """Update a record with retry logic.

        Args:
            session: Database session
            record_id: ID of record to update
            table_name: Name of table containing record
            field: Field to update
            value: New value
            max_retries: Maximum retry attempts

        Returns:
            True if update successful
        """
        stmt = text(f"UPDATE {table_name} SET {field} = :value WHERE id = :id")
        
        for attempt in range(max_retries):
            try:
                session.execute(stmt, {'value': value, 'id': record_id})
                session.commit()
                self.logger.info(
                    f"Successfully updated {table_name} record {record_id} "
                    f"{field} to {value}"
                )
                return True
                
            except SQLAlchemyError as e:
                session.rollback()
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Retry {attempt + 1}/{max_retries} updating "
                        f"{table_name} record {record_id}: {e}"
                    )
                    continue
                else:
                    self.logger.error(
                        f"Failed to update {table_name} record {record_id} "
                        f"after {max_retries} attempts: {e}"
                    )
                    return False
            except Exception as e:
                session.rollback()
                self.logger.error(
                    f"Unexpected error updating {table_name} record {record_id}: {e}"
                )
                return False
                
        return False
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around operations.

        Yields:
            Database session
        """
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

    def query_table(
        self, 
        session: Any, 
        model: Any, 
        filters: Optional[list] = None, 
        limit: Optional[int] = None
    ):
        """Execute generic table query.

        Args:
            session: Database session
            model: SQLAlchemy model class
            filters: Optional filter conditions
            limit: Maximum records to return

        Returns:
            Query object
        """
        query = session.query(model)
        
        if filters:
            for filter_condition in filters:
                query = query.filter(filter_condition)
                
        if limit:
            query = query.limit(limit)
            
        return query

    def add_synonym(
        self, 
        session: Any, 
        synonym_model: Any, 
        value: str, 
        target_table: str, 
        target_id: int, 
        message: Optional[str] = None
    ) -> bool:
        """Add new synonym if it doesn't exist.

        Args:
            session: Database session
            synonym_model: Synonym model class
            value: Synonym value
            target_table: Target table name
            target_id: Target record ID
            message: Optional message (e.g., AI match details)

        Returns:
            True if added, False if exists
        """
        try:
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