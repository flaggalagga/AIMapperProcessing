# utils/text_processor.py
import re
from typing import List, Optional

class TextProcessor:
    @staticmethod
    def parse_localization_values(localization_str: Optional[str]) -> List[str]:
        """
        Parse localization string into a list of unique, cleaned values
        
        :param localization_str: Raw localization string
        :return: List of cleaned localization values
        """
        if not localization_str:
            return []
        
        # Split by slash or comma and clean up
        values = re.split(r'[/,]', localization_str)
        return [
            val.strip() 
            for val in values 
            if val.strip()
        ]

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Normalize text for comparison
        
        :param text: Input text
        :return: Normalized text
        """
        return text.lower().strip()

    @staticmethod
    def calculate_text_similarity(text1: str, text2: str, method='simple') -> float:
        """
        Calculate similarity between two texts
        
        :param text1: First text
        :param text2: Second text
        :param method: Similarity calculation method
        :return: Similarity score (0-1)
        """
        # Normalize texts
        t1 = TextProcessor.normalize_text(text1)
        t2 = TextProcessor.normalize_text(text2)
        
        if method == 'simple':
            # Basic Jaccard similarity
            t1_words = set(t1.split())
            t2_words = set(t2.split())
            
            intersection = len(t1_words.intersection(t2_words))
            union = len(t1_words.union(t2_words))
            
            return intersection / union if union > 0 else 0.0
        
        # Add more sophisticated similarity methods as needed
        raise ValueError(f"Unsupported similarity method: {method}")