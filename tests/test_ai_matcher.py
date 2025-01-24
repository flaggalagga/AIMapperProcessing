# tests/test_ai_matcher.py
import pytest
from etl_processing.services.ai_matcher import AIMatcherService

@pytest.fixture
def ai_matcher():
    existing_options = ['Neige dure', 'Neige molle', 'Neige poudreuse']
    return AIMatcherService(existing_options)

class TestAIMatcher:
    def test_normalize_text(self, ai_matcher):
        assert ai_matcher._normalize_text('NEIGE-DURE') == 'neige dure'

    def test_find_best_match(self, ai_matcher):
        result = ai_matcher.find_best_match('DURE')
        assert result is not None
        assert result.confidence > 0.5

    def test_context_weights(self, ai_matcher):
        context = {'Station': {'value': 'test', 'weight': 0.5}}
        result = ai_matcher.find_best_match('DURE', context)
        assert result is not None