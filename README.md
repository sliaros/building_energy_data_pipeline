# Enhanced Schema Generator

A robust and efficient tool for generating PostgreSQL schemas from CSV and Parquet files.

## Features

- Support for CSV and Parquet file formats
- Intelligent data sampling with progress tracking
- Advanced type inference and schema analysis
- Comprehensive column profiling
- SQL optimization suggestions
- Detailed logging and error handling
- Command-line interface

## Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

Basic usage:
```bash
python main.py input_file.csv
```

With options:
```bash
python main.py input_file.parquet \
    --output schema.sql \
    --table my_table \
    --schema my_schema \
    --sampling-method random \
    --max-rows 50000 \
    --sampling-ratio 0.05
```

### Python API

```python
from pathlib import Path
from schema_generator import SchemaGenerationOrchestrator
from smart_sampler import SamplingConfig

# Configure sampling
config = SamplingConfig(
    method="random",
    max_rows=50000,
    sampling_ratio=0.05
)

# Create orchestrator
orchestrator = SchemaGenerationOrchestrator(
    input_file=Path("input_file.csv"),
    output_file=Path("output.sql"),
    table_name="my_table",
    schema_name="my_schema",
    sampling_config=config
)

# Generate schema
sql = orchestrator.generate()
```

## Key Components

1. **Smart Sampling**: Progressive data sampling with multiple strategies
2. **Schema Analysis**: Advanced type inference and column profiling
3. **SQL Generation**: Optimized SQL schema generation with best practices
4. **Logging**: Comprehensive logging with rotation and formatting

## Logging

Logs are stored in the `logs` directory with the following format:
- Daily rotation
- Maximum size: 10MB
- Keeps last 5 backups
- Detailed formatting for debugging

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License