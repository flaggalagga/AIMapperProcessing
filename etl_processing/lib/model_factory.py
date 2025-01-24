# /etl_processing/lib/model_factory.py
from typing import Dict, Any, Type
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.orm import declarative_base
import yaml

Base = declarative_base()

class ModelFactory:
    """Factory for creating SQLAlchemy models dynamically from configuration"""
    
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
        """
        Create a SQLAlchemy model class dynamically from configuration
        
        :param table_name: Name of the table
        :param table_config: Configuration for the table
        :return: SQLAlchemy model class
        """
        columns = {}
        
        # Add columns based on configuration
        for col_name, col_config in table_config['columns'].items():
            col_type = col_config['type'].lower()
            
            # Handle varchar with length
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
                # Get SQLAlchemy type from mapping
                sql_type = cls.TYPE_MAPPING.get(col_type)
                if not sql_type:
                    raise ValueError(f"Unsupported column type: {col_type}")
                
                # Handle foreign keys
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

        # Create and return model class with unique name
        class_name = f"Dynamic{table_name}Model"
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
        """
        Load all models from configuration file
        
        :param config_path: Path to configuration file
        :return: Dictionary of table names to model classes
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        models = {}
        
        # Add dicosynonymes table configuration if not present
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