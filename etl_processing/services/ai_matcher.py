# services/ai_matcher.py
from typing import List, Dict, Optional, NamedTuple, Union
import torch
import numpy as np
from sentence_transformers import SentenceTransformer, util
import logging
import unicodedata
from ..utils.text_processor import TextProcessor

class MatchResult(NamedTuple):
    """
    Represents a matching result from AI matching
    """
    id: int
    confidence: float
    matched_value: str

class AIMatcherService:
    def __init__(
        self, 
        existing_options: List[str], 
        model_name: str = 'paraphrase-multilingual-MiniLM-L12-v2',
        logger: Optional[logging.Logger] = None,
        config: Optional[dict] = None
    ):
        """
        Initialize AI matcher with existing options
        
        :param existing_options: List of existing options to match against
        :param model_name: Name of the sentence transformer model to use
        :param logger: Optional logger instance
        :param config: Optional configuration dictionary
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = config or {}
        
        self.logger.info(f"Initializing AI matcher with model: {model_name}")
        self.model = SentenceTransformer(model_name)
        
        self.existing_options = existing_options
        self.logger.info(f"Processing {len(existing_options)} existing options")
        
        # Normalize and compute embeddings for existing options
        self.normalized_options = [self._normalize_medical_term(opt) for opt in existing_options]
        self.logger.info("Computing embeddings for normalized options")
        self.option_embeddings = self.model.encode(
            self.normalized_options,
            convert_to_tensor=False
        )
        self.logger.info("AI matcher initialization complete")

    def _normalize_medical_term(self, text: str) -> str:
        """
        Normalize medical terms for better matching
        
        :param text: Input text to normalize
        :return: Normalized text
        """
        text = TextProcessor.normalize_text(text)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        
        # Medical term mappings
        medical_mappings = {
            'tibia perone': 'jambe',
            'femur': 'jambe',
            'rachis cervical': 'cou',
            'rachis lombaire': 'dos',
            'rachis dorsal': 'dos'
        }
        
        original_text = text
        for medical_term, common_term in medical_mappings.items():
            if medical_term in text:
                text = f"{text} {common_term}"
                
        if text != original_text:
            self.logger.debug(f"Normalized term: '{text}' (original: '{original_text}')")
            
        return text

    def find_best_match(
        self, 
        value: str, 
        context: Dict[str, Union[str, Dict[str, Union[str, float]]]] = {},
        similarity_threshold: Optional[float] = None
    ) -> Optional[MatchResult]:
        """
        Find the best match for a given value
        
        :param value: Value to match
        :param context: Context dictionary with optional weights
        :param similarity_threshold: Optional override for similarity threshold
        :return: MatchResult if found, None otherwise
        """
        try:
            similarity_threshold = similarity_threshold or self.config.get('ai', {}).get('similarity_threshold', 0.7)
            self.logger.info(f"Finding best match for: '{value}'")
            
            # Process context and weights
            enhanced_value = self._enhance_value_with_context(value, context)
            self.logger.info(f"Enhanced value: '{enhanced_value}'")
            
            # Compute embedding for query
            query_embedding = self.model.encode(enhanced_value, convert_to_tensor=False)
            
            # Compute similarities
            similarities = util.pytorch_cos_sim(
                torch.from_numpy(query_embedding), 
                torch.from_numpy(self.option_embeddings)
            )[0].numpy()
            
            # Apply context-based boosting if weights are provided
            similarities = self._apply_context_weights(similarities, context)
            
            # Find best matches
            top_k = min(3, len(similarities))
            top_indices = np.argsort(similarities)[-top_k:][::-1]
            
            self.logger.info(f"Top {top_k} matches:")
            for idx in top_indices:
                self.logger.info(
                    f"  - '{self.existing_options[idx]}' "
                    f"(similarity: {similarities[idx]:.4f})"
                )
            
            best_index = int(top_indices[0])
            best_similarity = float(similarities[best_index])
            
            if best_similarity >= similarity_threshold:
                self.logger.info(
                    f"Match found: '{self.existing_options[best_index]}' "
                    f"(similarity: {best_similarity:.4f})"
                )
                return MatchResult(
                    id=best_index,
                    confidence=best_similarity,
                    matched_value=self.existing_options[best_index]
                )
            
            self.logger.info(
                f"No match found above threshold ({similarity_threshold}). "
                f"Best similarity: {best_similarity:.4f}"
            )
            return None
            
        except Exception as e:
            self.logger.error(f"Error in AI matching: {e}")
            self.logger.exception("Full traceback:")
            return None

    def _enhance_value_with_context(
        self, 
        value: str, 
        context: Dict[str, Union[str, Dict[str, Union[str, float]]]]
    ) -> str:
        """
        Enhance input value with weighted contextual information
        
        :param value: Value to enhance
        :param context: Context dictionary with optional weights
        :return: Enhanced value string
        """
        value = self._normalize_medical_term(value)
        context_parts = [value]
        
        for field, content in context.items():
            if isinstance(content, dict):
                field_value = content.get('value', '')
                if field_value:
                    normalized_value = TextProcessor.normalize_text(field_value)
                    context_parts.append(normalized_value)
                    self.logger.debug(
                        f"Added context field '{field}' "
                        f"with value '{normalized_value}' "
                        f"and weight {content.get('weight', 1.0)}"
                    )
            else:
                if content:
                    normalized_value = TextProcessor.normalize_text(content)
                    context_parts.append(normalized_value)
                    self.logger.debug(
                        f"Added context field '{field}' "
                        f"with value '{normalized_value}'"
                    )
        
        enhanced_value = " ".join(context_parts)
        self.logger.debug(f"Enhanced value: '{enhanced_value}'")
        return enhanced_value

    def _apply_context_weights(
        self, 
        similarities: np.ndarray, 
        context: Dict[str, Union[str, Dict[str, Union[str, float]]]]
    ) -> np.ndarray:
        """
        Apply context-based weights to similarity scores
        
        :param similarities: Base similarity scores
        :param context: Context dictionary with weights
        :return: Weighted similarity scores
        """
        weighted_similarities = similarities.copy()
        
        # Get total weight for normalization
        total_weight = sum(
            content.get('weight', 1.0)
            for content in context.values()
            if isinstance(content, dict)
        ) or 1.0
        
        # Apply weights
        for field, content in context.items():
            if isinstance(content, dict) and content.get('value'):
                weight = content.get('weight', 1.0) / total_weight
                query_embedding = self.model.encode(content['value'], convert_to_tensor=False)
                context_similarities = util.pytorch_cos_sim(
                    torch.from_numpy(query_embedding), 
                    torch.from_numpy(self.option_embeddings)
                )[0].numpy()
                weighted_similarities += context_similarities * weight
                
                self.logger.debug(
                    f"Applied weight {weight} to context field '{field}'"
                )
        
        return weighted_similarities