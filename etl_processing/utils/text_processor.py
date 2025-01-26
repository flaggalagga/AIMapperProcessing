# utils/text_processor.py
"""Text normalization utilities."""

import re
from typing import List, Optional

class TextProcessor:
    @staticmethod
    def parse_localization_values(localization_str: Optional[str]) -> List[str]:
        """Splits and cleans localization string.
        
        Args:
            localization_str: Raw localization string

        Returns:
            List of unique, cleaned values
        """
        if not localization_str:
            return []
        
        values = re.split(r'[/,]', localization_str)
        return [val.strip() for val in values if val.strip()]

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalizes text for comparison.
        
        Args:
            text: Input text
            
        Returns:
            Normalized text
        """
        return text.lower().strip()

    @staticmethod
    def calculate_text_similarity(text1: str, text2: str, method='simple') -> float:
        """Calculates similarity between two texts.
        
        Args:
            text1: First text
            text2: Second text
            method: Similarity calculation method
            
        Returns:
            Similarity score (0-1)
            
        Raises:
            ValueError: If method not supported
        """
        t1 = TextProcessor.normalize_text(text1)
        t2 = TextProcessor.normalize_text(text2)
        
        if method == 'simple':
            # Basic Jaccard similarity
            t1_words = set(t1.split())
            t2_words = set(t2.split())
            
            intersection = len(t1_words.intersection(t2_words))
            union = len(t1_words.union(t2_words))
            
            return intersection / union if union > 0 else 0.0
        
        raise ValueError(f"Unsupported similarity method: {method}")