# Configuration Guide

## ETL Configuration
Configuration is managed through YAML files in `/config`.

### Database Tables
```yaml
database:
  tables:
    table_name:
      name: "db_table_name"
      columns:
        column_name:
          type: "column_type"
          nullable: true/false
          references: "other_table.column"
```

### ETL Types
```yaml
etl_types:
  type_name:
    description: "Description"
    table_name: "target_table"
    source_table: "source_table"
    value_field: "field_name"
    multiple_values: true/false
    junction_table: "junction_table"  # For multiple values
    junction_mapping:
      accident_field: "field_name"
      target_field: "field_name"
```

## Environment Variables
```bash
MYSQL_HOST=hostname
MYSQL_DATABASE=dbname
MYSQL_USER=username
MYSQL_PASSWORD=password
```