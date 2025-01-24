# etl_processing/__init__.py
from .etl.generic import GenericETL
from .services.ai_matcher import AIMatcherService
from .services.database import DatabaseManager

__version__ = "0.1.0"

__all__ = ["GenericETL", "AIMatcherService", "DatabaseManager"]