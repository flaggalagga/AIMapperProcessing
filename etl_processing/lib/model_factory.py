"""Factory for creating SQLAlchemy models dynamically from YAML configuration."""

from typing import Dict, Any, Type
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.orm import declarative_base
import yaml
import uuid

Base = declarative_base()

class ModelFactory:
    """Creates SQLAlchemy models dynamically from configuration."""
    
    TYPE_MAPPING = {
        'int': Integer,
        'int unsigned': Integer,
        'varchar': String,
        'timestamp': DateTime,
        'tinyint': Boolean,
        'text': Text
    }

    @classmethod
    def create_model(cls, table_name: str, table_config: Dict[str, Any]) -> Type:
        """Creates a SQLAlchemy model class from configuration.
        
        Args:
            table_name: Name of the database table
            table_config: Table configuration dictionary
            
        Returns:
            SQLAlchemy model class
            
        Raises:
            ValueError: If column type is not supported
        """
        columns = {}
        
        for col_name, col_config in table_config['columns'].items():
            col_type = col_config['type'].lower()
            
            if col_type.startswith('varchar'):
                import re
                length = int(re.search(r'\((\d+)\)', col_type).group(1))
                col_type = 'varchar'
                column = Column(
                    String(length),
                    primary_key=col_config.get('primary', False),
                    nullable=col_config.get('nullable', True),
                    autoincrement=col_config.get('auto_increment', False)
                )
            else:
                sql_type = cls.TYPE_MAPPING.get(col_type)
                if not sql_type:
                    raise ValueError(f"Unsupported column type: {col_type}")
                
                if 'references' in col_config:
                    ref_table, ref_col = col_config['references'].split('.')
                    column = Column(
                        sql_type,
                        ForeignKey(f"{ref_table}.{ref_col}"),
                        nullable=col_config.get('nullable', True)
                    )
                else:
                    column = Column(
                        sql_type,
                        primary_key=col_config.get('primary', False),
                        nullable=col_config.get('nullable', True),
                        autoincrement=col_config.get('auto_increment', False)
                    )
            
            columns[col_name] = column

        # Generate unique class name using uuid
        unique_id = str(uuid.uuid4()).replace('-', '')
        class_name = f"Dynamic{table_name}Model_{unique_id}"
        
        return type(
            class_name,
            (Base,),
            {
                '__tablename__': table_config['name'],
                '__table_args__': {'extend_existing': True},
                **columns
            }
        )

    @classmethod
    def load_models(cls, config_path: str) -> Dict[str, Type]:
        """Loads all models from configuration file.
        
        Args:
            config_path: Path to YAML configuration
            
        Returns:
            Dictionary mapping table names to model classes
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        models = {}
        
        if 'dicosynonymes' not in config['database']['tables']:
            config['database']['tables']['dicosynonymes'] = {
                'name': 'dicosynonymes',
                'columns': {
                    'id': {
                        'type': 'int unsigned',
                        'primary': True,
                        'auto_increment': True
                    },
                    'created': {
                        'type': 'timestamp',
                        'nullable': True
                    },
                    'modified': {
                        'type': 'timestamp',
                        'nullable': True
                    },
                    'table_name_id': {
                        'type': 'int unsigned',
                        'nullable': False
                    },
                    'table_name': {
                        'type': 'varchar(255)',
                        'nullable': False
                    },
                    'name': {
                        'type': 'varchar(255)',
                        'nullable': False
                    },
                    'ai_match_message': {
                        'type': 'text',
                        'nullable': True
                    }
                }
            }
            
        for table_name, table_config in config['database']['tables'].items():
            models[table_name] = cls.create_model(table_name, table_config)
            
        return models