import re
import yaml
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Type
from sqlalchemy import func
from ..lib.model_factory import ModelFactory
from ..services.database import DatabaseManager
from ..services.ai_matcher import AIMatcherService
from ..services.logger import setup_logging

class GenericETL:
    def __init__(self, etl_type: str, config_path: str):
        self.config = None
        self.models = None
        self.logger = setup_logging()
        self.logger.info(f"Initializing {etl_type} ETL processor")
        
        self._init_config(config_path, etl_type)
        self._init_models(config_path)
        
        self.db_manager = DatabaseManager(logger=self.logger)
        self._init_ai_matcher()
        self.db_manager.ai_matcher = self.ai_matcher

    def _init_config(self, config_path: str, etl_type: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        if etl_type not in self.config['etl_types']:
            raise ValueError(f"Unknown ETL type: {etl_type}")
        self.etl_type = etl_type
        self.etl_config = self.config['etl_types'][etl_type]
        self.settings = self.config['settings']
        
    def _init_models(self, config_path: str):
        self.models = ModelFactory.load_models(config_path)
        self.source_model = self.models[self.etl_config['source_table']]
        self.target_model = self.models[self.etl_config['table_name']]
    
    def _init_ai_matcher(self):
        try:
            with self.db_manager.session_scope() as session:
                items = session.query(
                    self.target_model.id,
                    self.target_model.name
                ).all()
                
                self.existing_options = [name for _, name in items]
                self.ai_matcher = AIMatcherService(
                    existing_options=self.existing_options,
                    logger=self.logger,
                    config=self.config
                )
                self.id_map = {idx: id for idx, (id, _) in enumerate(items)}
        except Exception as e:
            self.logger.error(f"Failed to initialize AI matcher: {e}")
            raise
    
    def run(self):
        """Execute the ETL process"""
        try:
            batch_size = self.settings.get('batch_size', 50)
            max_iterations = self.settings.get('max_iterations', 1)
            progress_interval = self.settings.get('progress_interval', 50)

            self.logger.info(
                f"Starting {self.etl_type} ETL process with "
                f"batch_size={batch_size}, max_iterations={max_iterations}"
            )
            
            processed_total = 0
            iteration = 0

            while iteration < max_iterations:
                with self.db_manager.session_scope() as session:
                    session.autoflush = False
                    
                    self.logger.info(f"Fetching batch {iteration + 1}")
                    unmapped_records = self.get_unmapped_records(session, batch_size)
                    records = unmapped_records.all()

                    if not records:
                        self.logger.info("No more unmapped records to process")
                        break

                    self.logger.info(f"Processing {len(records)} records")
                    processed_in_batch = 0

                    for i, record in enumerate(records, 1):
                        try:
                            with self.db_manager.session_scope() as record_session:
                                fresh_record = record_session.merge(record)
                                if self._process_record(record_session, fresh_record):
                                    processed_in_batch += 1
                                
                                if i % progress_interval == 0:
                                    self.logger.info(
                                        f"Processed {i}/{len(records)} "
                                        f"records in current batch"
                                    )
                        except Exception as e:
                            self.logger.error(
                                f"Error processing record "
                                f"{getattr(record, 'id', 'unknown')}: {e}"
                            )
                            continue

                    processed_total += processed_in_batch
                    iteration += 1
                    self.logger.info(
                        f"Completed batch {iteration}: "
                        f"processed {processed_in_batch} records. "
                        f"Total processed: {processed_total}"
                    )
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}")
            self.logger.exception("Full traceback:")
        finally:
            self.logger.info(
                f"ETL process completed. "
                f"Total records processed: {processed_total}"
            )

    def get_unmapped_records(self, session, batch_size: int):
        if self.etl_config.get('multiple_values', False):
            junction_model = self.models[self.etl_config['junction_table']]
            subquery = session.query(junction_model).filter(
                junction_model.accident_id == self.source_model.id
            ).exists()
            return session.query(self.source_model).filter(
                ~subquery,
                getattr(self.source_model, self.etl_config['value_field']).isnot(None)
            ).limit(batch_size)
        else:
            return session.query(self.source_model).filter(
                getattr(self.source_model, self.etl_config['mapping_id_field']).is_(None),
                getattr(self.source_model, self.etl_config['value_field']).isnot(None)
            ).limit(batch_size)

    def _process_record(self, session, record) -> List[int]:
        """Process a single record"""
        value_field = self.etl_config['value_field']
        raw_value = getattr(record, value_field, None)
        if not raw_value:
            return []

        matched_ids = []
        try:
            for value in self._split_values(raw_value):
                if not self.is_valid_value(value):
                    self.logger.info(f"Skipping invalid value: '{value}'")
                    continue

                match_result = self._find_direct_match(session, value)
                if match_result:
                    matched_ids.append(match_result)
                else:
                    context = self._get_context(record)
                    match_result = self.ai_matcher.find_best_match(value, context)
                    if match_result:
                        actual_id = self.id_map[match_result.id]
                        matched_ids.append(actual_id)
                        self._add_synonym(session, value, actual_id, match_result.confidence)

            if matched_ids:
                if self.etl_config.get('multiple_values', False):
                    junction_model = self.models[self.etl_config['junction_table']]
                    mapping = self.etl_config['junction_mapping']
                    for target_id in matched_ids:
                        junction = junction_model(
                            **{
                                mapping['accident_field']: record.id,
                                mapping['target_field']: target_id
                            }
                        )
                        session.add(junction)
                else:
                    setattr(record, self.etl_config['mapping_id_field'], matched_ids[0])
                    session.add(record)

        except Exception as e:
            self.logger.error(f"Error processing record {getattr(record, 'id', 'unknown')}: {e}")
            return []

        return matched_ids

    def _split_values(self, raw_value: str) -> List[str]:
        """Split value if multiple values are allowed"""
        if not self.etl_config.get('multiple_values', False):
            return [raw_value]
        separator = self.etl_config.get('value_separator', '[/,]')
        return [val.strip() for val in re.split(separator, raw_value) if val.strip()]

    def _get_context(self, record) -> Dict[str, Dict[str, Any]]:
        """Get context for AI matching"""
        context = {}
        for context_field in self.etl_config.get('context_fields', []):
            field_name = context_field['field']
            field_value = getattr(record, field_name, '')
            if field_value:
                context[field_name] = {
                    'value': str(field_value),
                    'weight': context_field.get('weight', 1.0)
                }
        return context

    def _find_direct_match(self, session, value: str) -> Optional[int]:
        """Find exact match in target table or synonyms"""
        try:
            match = session.query(self.target_model).filter(
                self.target_model.name == value
            ).first()
            if match:
                return match.id

            synonym = session.query(self.models['dicosynonymes']).filter(
                self.models['dicosynonymes'].table_name == self.etl_config['table_name'],
                self.models['dicosynonymes'].name == value
            ).first()
            if synonym:
                return synonym.table_name_id

        except Exception as e:
            self.logger.error(f"Error in direct matching: {e}")
            return None

    def _add_synonym(self, session, value: str, target_id: int, confidence: float):
        """Add new synonym to dictionary"""
        try:
            existing = session.query(self.models['dicosynonymes']).filter(
                self.models['dicosynonymes'].table_name == self.etl_config['table_name'],
                func.trim(self.models['dicosynonymes'].name).collate('utf8mb4_general_ci') == value.strip()
            ).first()

            if not existing:
                synonym = self.models['dicosynonymes'](
                    table_name=self.etl_config['table_name'],
                    table_name_id=target_id,
                    name=value.strip(),
                    ai_match_message=f"AI match with confidence {confidence:.4f}"
                )
                session.add(synonym)
                session.flush()

        except Exception as e:
            self.logger.error(f"Error in synonym check/add process: {e}")
            session.rollback()

    def is_valid_value(self, value: str) -> bool:
        """Check if value should be processed"""
        if not value:
            return False

        validation = self.etl_config.get('validation', {})
        if validation:
            skip_pattern = validation.get('skip_if_matches')
            if skip_pattern and re.match(skip_pattern, value.strip()):
                return False

        return True