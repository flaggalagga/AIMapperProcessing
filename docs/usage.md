# Usage Guide

## ETL Types
- snow: Snow conditions mapping
- country: Country name mapping
- localization: Body injury locations (multi-value)
- injury: Injury types (multi-value)

## Running ETL Process
```bash
# Direct execution
python -m etl_processing.main <etl_type>

# After installation
etl_process <etl_type>

# Docker
docker-compose up
```

## Testing
```bash
pytest tests/

# With coverage
pytest --cov=etl_processing tests/
```