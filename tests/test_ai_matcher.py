# tests/test_ai_matcher.py
"""Unit tests for AI-based text matching service."""
import pytest
import logging
from etl_processing.services.ai_matcher import AIMatcherService, MatchResult

class TestAIMatcher:
    def test_initialization_basic(self):
        """Test basic initialization of AIMatcherService."""
        options = ['Neige dure', 'Neige molle', 'Neige poudreuse']
        matcher = AIMatcherService(options)
        
        assert matcher.model is not None, "Model should be initialized"
        assert matcher.option_embeddings is not None, "Embeddings should be computed"
        assert len(matcher.existing_options) == 3, "All options should be preserved"

    def test_medical_term_normalization(self):
        """Test medical term normalization method."""
        options = ['Neige dure']
        matcher = AIMatcherService(options)
        
        # Test normalization of medical terms
        normalized = matcher._normalize_medical_term('Tibia Perone')
        assert 'tibia perone' in normalized
        assert 'jambe' in normalized

    def test_simple_match_scenarios(self):
        """Test various matching scenarios."""
        options = ['Neige dure', 'Neige molle', 'Neige poudreuse']
        matcher = AIMatcherService(options)
        
        # Direct match test
        match1 = matcher.find_best_match('Neige dure')
        assert match1 is not None, "Direct match should be found"
        assert match1.matched_value == 'Neige dure', "Matched value should be correct"
        
        # Partial match test
        match2 = matcher.find_best_match('dure')
        assert match2 is not None, "Partial match should be found"
        
        # Unrelated term test
        match3 = matcher.find_best_match('completely unrelated')
        assert match3 is None, "Unrelated term should not match"

    def test_context_matching(self):
        """Test matching with context."""
        options = ['Neige dure', 'Neige molle', 'Neige poudreuse']
        matcher = AIMatcherService(options)
        
        context = {
            'Station': {'value': 'Courchevel', 'weight': 0.5},
            'Difficulty': 'Intermediate'
        }
        
        # Test match with context
        match = matcher.find_best_match('Neige', context)
        assert match is not None, "Match with context should be possible"
        assert isinstance(match, MatchResult), "Result should be a MatchResult"

    def test_initialization_with_empty_options(self):
        """Test initialization with empty options."""
        matcher = AIMatcherService([])
        
        # Validate that initialization doesn't fail
        assert matcher.model is not None, "Model should still be initialized"

    def test_logging_behavior(self, caplog):
        """Test basic logging behavior."""
        caplog.set_level(logging.INFO)
        
        options = ['Neige dure']
        matcher = AIMatcherService(options)
        
        # Trigger a match to generate logs
        matcher.find_best_match('Neige dure')
        
        # Check for specific log messages
        assert any("Finding best match for" in record.message for record in caplog.records)