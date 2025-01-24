# tests/test_ai_matcher.py
import pytest
import numpy as np
import torch
from unittest.mock import Mock, patch
from etl_processing.services.ai_matcher import AIMatcherService

@pytest.fixture
def mock_embeddings():
    return np.array([
        [1.0, 0.0],
        [0.0, 1.0],
        [0.5, 0.5]
    ])

@pytest.fixture
def ai_matcher():
    with patch('sentence_transformers.SentenceTransformer') as mock:
        model = Mock()
        model.encode.return_value = np.array([[1.0, 0.0], [0.0, 1.0], [0.5, 0.5]])
        mock.return_value = model
        return AIMatcherService(['Neige dure', 'Neige molle', 'Neige poudreuse'])

class TestAIMatcher:
    def test_normalize_text(self, ai_matcher):
            from etl_processing.utils.text_processor import TextProcessor
            assert TextProcessor.normalize_text('NEIGE-DURE') == 'neige-dure'

    def test_find_best_match(self, ai_matcher):
        with patch('torch.from_numpy', return_value=torch.tensor([[1.0, 0.0]])):
            result = ai_matcher.find_best_match('DURE')
            assert result is not None
            assert result.confidence > 0.5

    def test_context_weights(self, ai_matcher):
        context = {'Station': {'value': 'test', 'weight': 0.5}}
        result = ai_matcher.find_best_match('DURE', context)
        assert result is not None