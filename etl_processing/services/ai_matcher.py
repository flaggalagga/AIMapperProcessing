# services/ai_matcher.py
"""AI-based text matching service using sentence transformers for fuzzy matching."""

from typing import List, Dict, Optional, NamedTuple, Union
from sentence_transformers import SentenceTransformer, util
import torch
import numpy as np
import logging
import unicodedata
from ..utils.text_processor import TextProcessor

class MatchResult(NamedTuple):
    """Result of AI matching containing matched ID, confidence score and value."""
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
        """Initialize matcher with existing reference values and model.
        
        Args:
            existing_options: List of options to match against
            model_name: Sentence transformer model name
            logger: Optional logger instance
            config: Optional configuration dict
        """
        self.logger = logger or logging.getLogger(__name__)
        self.config = config or {}
        
        self.logger.info(f"Initializing AI matcher with model: {model_name}")
        
        try:
            self.model = SentenceTransformer(model_name)
        except Exception as e:
            self.logger.error(f"Failed to load SentenceTransformer: {e}")
            self.model = None
            self.option_embeddings = None
            return

        self.existing_options = existing_options
        self.logger.info(f"Processing {len(existing_options)} existing options")
        
        # Normalize and compute embeddings for existing options
        self.normalized_options = [self._normalize_medical_term(opt) for opt in existing_options]
        self.logger.info("Computing embeddings for normalized options")
        
        try:
            self.option_embeddings = self._compute_safe_embeddings(self.normalized_options)
            self.logger.info("Embeddings computed successfully")
        except Exception as e:
            self.logger.error(f"Failed to compute embeddings: {e}")
            self.option_embeddings = None

    def _compute_safe_embeddings(self, options: List[str]) -> np.ndarray:
        """Safely compute embeddings with robust error handling.
        
        Args:
            options: Normalized text options to embed

        Returns:
            NumPy array of embeddings
        """
        try:
            embeddings = self.model.encode(
                options,
                convert_to_tensor=False
            )
            
            # Ensure NumPy array with proper dimensions
            if not isinstance(embeddings, np.ndarray):
                embeddings = np.array(embeddings)
            
            if embeddings.ndim != 2:
                raise ValueError(f"Unexpected embedding dimensions: {embeddings.ndim}")
            
            return embeddings
        except Exception as e:
            self.logger.error(f"Embedding computation error: {e}")
            raise

    def _normalize_medical_term(self, text: str) -> str:
        """Normalize medical terms by mapping specific terms to common forms.
        
        Args:
            text: Medical term to normalize

        Returns:
            Normalized text with mapped common terms

        Example:
            'tibia perone' -> 'tibia perone jambe'
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
        """Find best matching option for the input value.
        
        Args:
            value: Value to find match for
            context: Optional weighted context values
            similarity_threshold: Optional custom threshold

        Returns:
            MatchResult if match found above threshold, None otherwise
        """
        # Check if AI matching is possible
        if self.model is None or self.option_embeddings is None:
            self.logger.warning("AI matcher not properly initialized. Skipping matching.")
            return None

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
        """Enhance value with weighted context information.
        
        Args:
            value: Base value to enhance
            context: Context values with optional weights

        Returns:
            Enhanced value string including context
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
        """Apply weighted context to base similarity scores.
        
        Args:
            similarities: Base similarity scores
            context: Context with weights

        Returns:
            Updated similarity scores with context weights applied
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