# API Reference

## GenericETL
Main ETL processing class.

```python
from etl_processing import GenericETL

etl = GenericETL(etl_type: str, config_path: str)
etl.run()
```

## AIMatcherService
Handles AI-assisted matching using sentence transformers.

```python
from etl_processing import AIMatcherService

matcher = AIMatcherService(
    existing_options: List[str],
    model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"
)
```

## ModelFactory
Dynamic SQLAlchemy model generation.

```python
from etl_processing.lib import ModelFactory

models = ModelFactory.load_models(config_path: str)
```