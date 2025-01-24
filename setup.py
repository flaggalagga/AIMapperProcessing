from setuptools import setup, find_packages

setup(
    name="etl_processing",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "numpy>=1.26.3",
        "python-dotenv",
        "mysql-connector-python",
        "sqlalchemy",
        "torch==2.1.2",
        "sentence-transformers",
        "pyyaml",
        "tqdm",
    ],
    include_package_data=True,
    package_data={
        "etl_processing": ["config/*.yml"],
    },
    entry_points={
        "console_scripts": [
            "etl_process=etl_processing.main:main",
        ],
    },
    author="Lars FORNELL",
    description="ETL processing with AI-assisted matching",
    python_requires=">=3.8",
)