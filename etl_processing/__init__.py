# etl_processing/__init__.py
"""ETL processing package with AI-assisted matching."""

from .etl.generic import GenericETL
from .services.ai_matcher import AIMatcherService
from .services.database import DatabaseManager

__version__ = "0.1.0"

__all__ = ["GenericETL", "AIMatcherService", "DatabaseManager"]