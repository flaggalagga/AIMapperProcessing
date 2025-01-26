# services/error_handler.py
from typing import Optional, Dict, Any, Callable
from functools import wraps
import traceback
from datetime import datetime
import json

class ETLError(Exception):
   def __init__(self, message: str, error_code: str, details: Optional[Dict[str, Any]] = None):
       self.error_code = error_code
       self.details = details or {}
       super().__init__(message)

class ValidationError(ETLError):
   pass

class MappingError(ETLError):
   pass

class DatabaseError(ETLError):
   pass

def with_error_handling(error_type: str, monitoring_service=None):
   def decorator(func: Callable):
       @wraps(func)
       def wrapper(*args, **kwargs):
           try:
               return func(*args, **kwargs)
           except ETLError as e:
               if monitoring_service:
                   monitoring_service.record_error(
                       error_type=e.error_code,
                       error_msg=str(e),
                       record_id=getattr(args[0], 'id', None) if args else None
                   )
               raise
           except Exception as e:
               error_msg = f"{error_type} operation failed: {str(e)}"
               if monitoring_service:
                   monitoring_service.record_error(
                       error_type=error_type,
                       error_msg=error_msg,
                       record_id=getattr(args[0], 'id', None) if args else None
                   )
               raise ETLError(error_msg, error_type, {
                   'original_error': str(e),
                   'traceback': traceback.format_exc()
               })
       return wrapper
   return decorator

class ErrorHandler:
   def __init__(self, logger, monitoring_service=None):
       self.logger = logger
       self.monitoring_service = monitoring_service
       self.error_handlers = {
           'validation': self._handle_validation_error,
           'mapping': self._handle_mapping_error,
           'database': self._handle_database_error
       }
       self.error_history = []
       self.error_counts = {}

   def handle_error(self, error: Exception, context: Dict[str, Any] = None) -> bool:
       error_type = self._classify_error(error)
       handler = self.error_handlers.get(error_type)
       
       self.error_history.append({
           'timestamp': datetime.now(),
           'type': error_type,
           'message': str(error),
           'context': context,
           'traceback': traceback.format_exc()
       })
       self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
       
       if handler:
           return handler(error, context)
       return False

   def _classify_error(self, error: Exception) -> str:
       if isinstance(error, ValidationError):
           return 'validation'
       if isinstance(error, MappingError):
           return 'mapping'
       if isinstance(error, DatabaseError):
           return 'database'
       return 'unknown'

   def _handle_validation_error(self, error: Exception, context: Dict[str, Any]) -> bool:
       self.logger.warning(f"Validation error: {str(error)}")
       if self.monitoring_service:
           self.monitoring_service.record_error('validation', str(error))
       return True

   def _handle_mapping_error(self, error: Exception, context: Dict[str, Any]) -> bool:
       self.logger.warning(f"Mapping error: {str(error)}")
       if self.monitoring_service:
           self.monitoring_service.record_error('mapping', str(error))
       return True

   def _handle_database_error(self, error: Exception, context: Dict[str, Any]) -> bool:
       self.logger.error(f"Database error: {str(error)}")
       if self.monitoring_service:
           self.monitoring_service.record_error('database', str(error))
       return False

   def save_error_report(self):
       if self.error_history:
           error_file = f"reports/errors/errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
           with open(error_file, 'w') as f:
               json.dump({
                   'error_counts': self.error_counts,
                   'errors': self.error_history
               }, f, indent=2, default=str)

   def get_error_summary(self) -> Dict:
       return {
           'total_errors': len(self.error_history),
           'error_counts': self.error_counts
       }