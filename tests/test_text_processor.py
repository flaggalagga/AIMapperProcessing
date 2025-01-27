import pytest
from etl_processing.utils.text_processor import TextProcessor

def test_parse_localization_values():
    assert TextProcessor.parse_localization_values("value1/value2,value3") == ["value1", "value2", "value3"]
    assert TextProcessor.parse_localization_values("") == []
    assert TextProcessor.parse_localization_values(None) == []
    assert TextProcessor.parse_localization_values(" value1 , value2 ") == ["value1", "value2"]

def test_normalize_text():
    assert TextProcessor.normalize_text("  TEST  ") == "test"
    assert TextProcessor.normalize_text("Mixed CASE") == "mixed case"
    assert TextProcessor.normalize_text("") == ""

def test_calculate_text_similarity():
    assert TextProcessor.calculate_text_similarity("test", "test") == 1.0
    assert TextProcessor.calculate_text_similarity("test", "completely different") == 0.0
    assert TextProcessor.calculate_text_similarity("", "") == 0.0

    with pytest.raises(ValueError):
        TextProcessor.calculate_text_similarity("test", "test", method="invalid")