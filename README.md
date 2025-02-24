# Building Energy Data Pipeline

## Overview

The Building Energy Data Pipeline is a robust, enterprise-grade application designed to streamline the extraction, transformation, and loading (ETL) of building energy data. This comprehensive solution automates the entire data lifecycle from acquisition to database storage, enabling advanced analytics and insights for energy consumption patterns.

## Key Features

- **Automated Data Retrieval**: Seamlessly downloads data from Zenodo repositories
- **Intelligent Data Transformation**: Processes and converts raw data to optimized Parquet format
- **PostgreSQL Integration**: Efficiently loads transformed data into PostgreSQL databases
- **Schema Analysis and Generation**: Automatically analyzes data structure and generates appropriate database schemas
- **Secure Database Management**: Includes SSL certificate management for secure database connections
- **Centralized Configuration Management**: Offers flexible YAML-based configuration system
- **Comprehensive Logging**: Provides detailed logging for monitoring and troubleshooting

## Architecture

The application follows a modular architecture with clear separation of concerns:

- **Orchestration Layer**: Coordinates the entire data pipeline process
- **Data Extraction**: Handles data retrieval from external sources
- **Data Transformation**: Processes raw data into optimized formats
- **Data Loading**: Manages database interactions and data ingestion
- **Configuration Management**: Provides centralized configuration services
- **Security Management**: Handles SSL certificate generation and management

## Getting Started

### Prerequisites

- Python 3.8 or higher
- PostgreSQL 13 or higher
- OpenSSL (for secure database connections)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/building-energy-data-pipeline.git
cd building-energy-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure the application
cp config/app_config.example.yaml config/app_config.yaml
```

### Configuration

The application uses YAML configuration files located in the `./config` directory:

- `app_config.yaml`: Main application configuration
- `project_structure_config.yaml`: Defines project directory structure

Edit these files to customize the application to your environment.

### Basic Usage

```python
from src.orchestration.orchestrator import Orchestrator

# Initialize the pipeline
pipeline = Orchestrator()

# Run the complete ETL process
pipeline.retrieve_data()
pipeline.transform_data()
pipeline.load_data()
```

## Advanced Features

### Custom Database Management

```python
# Create a connection to a specific database
pipeline = Orchestrator(database_name="energy_analytics")

# Manage database sessions
active_sessions = pipeline.return_active_sessions()
pipeline.terminate_sessions("energy_analytics", state="idle")

# Database cleanup
pipeline.delete_database("old_database", force=True)
```

### Secure Database Connections

The application includes a comprehensive SSL certificate management system:

```python
from src.security.ca_manager import CaManager

# Initialize the certificate manager
ca_manager = CaManager()

# Generate a new certificate
ca_manager.generate_cert_with_cryptography()

# Configure PostgreSQL to use SSL
ca_manager.configure_postgresql_ssl(enable_ssl=True)
```

## Technical Details

### Modular Components

- **Orchestrator**: Coordinates the complete data pipeline
- **DataExtractor**: Handles data retrieval from external sources
- **DataTransformer**: Converts data between formats and optimizes storage
- **PostgresDataLoader**: Manages database operations
- **SchemaAnalysisManager**: Analyzes data structure and creates database schemas
- **ConfigManager**: Provides centralized configuration management
- **CaManager**: Handles SSL certificate operations

### Performance Considerations

- Data is processed in chunks to optimize memory usage
- Parquet format provides efficient storage and faster query performance
- Configurable chunk sizes for optimal loading performance

## Documentation

For detailed documentation on each component:

- [Orchestrator Documentation](docs/orchestrator.md)
- [Data Processing Guide](docs/data_processing.md)
- [Configuration Reference](docs/configuration.md)
- [Security Setup](docs/security.md)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- Data sources provided by Zenodo
- Inspiration from various energy data analysis frameworks