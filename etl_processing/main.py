# /etl_processing/main.py
"""Main entry point for ETL processing with AI-assisted matching."""

import sys
import os
import yaml
from .etl.generic import GenericETL

def main():
    """Execute ETL process based on command line arguments."""
    # Determine config path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'etl_mappings.yml')

    # Load ETL configuration
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return

    if len(sys.argv) > 1:
        etl_type = sys.argv[1]
        if etl_type not in config['etl_types']:
            print("Available ETL types:")
            for key, etl_config in config['etl_types'].items():
                print(f"  {key}: {etl_config['description']}")
            return
    else:
        print("Available ETL types:")
        for key, etl_config in config['etl_types'].items():
            print(f"  {key}: {etl_config['description']}")
        print("\nUsage: python main.py <etl_type>")
        return

    try:
        etl = GenericETL(etl_type, config_path)
        etl.run()
    except Exception as e:
        print(f"Error running ETL process: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()