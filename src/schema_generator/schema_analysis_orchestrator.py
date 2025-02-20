from pathlib import Path
from typing import Dict, Optional, Union, List
import logging
from datetime import datetime

from .base_scema_analyzer import BaseSchemaAnalyzer, BaseColumnInfo
from .sampling_strategies import BaseSamplingStrategy, RandomSamplingStrategy
from .schema_analyzer import PostgreSQLSchemaAnalyzer, SQLSchemaGenerator
from .type_inference_engine import PostgreSQLTypeInference

class SchemaAnalysisManager:
    """
    Manages the schema analysis and SQL generation workflow.
    """

    def __init__(
            self,
            sampling_strategy: Optional[BaseSamplingStrategy] = None,
            type_inference: Optional[PostgreSQLTypeInference] = None,
            config: Optional[Dict] = None,
            logger: Optional[logging.Logger] = None
    ):
        self._sampling_strategy = sampling_strategy or RandomSamplingStrategy()
        self._type_inference = type_inference or PostgreSQLTypeInference()
        self._config = config or {'project_data': {'schemas_dir_path': 'schemas'}}
        self._logger = logger or self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Initialize logger for the manager"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def generate_schema(
            self,
            file_path: Union[str, Path],
            table_name: Optional[str] = None,
            output_folder: Optional[Union[str, Path]] = None,
            if_exists: str = 'fail'
    ) -> Optional[Dict[str, Union[str, Path]]]:
        """
        Generate PostgreSQL schema from a data file.

        Args:
            file_path: Path to source data file
            table_name: Optional custom table name
            output_folder: Optional output directory for schema
            if_exists: Strategy for existing files ('fail' or 'replace')

        Returns:
            Dict with schema generation details
        """
        assert if_exists in ['fail', 'replace'], "if_exists must be 'fail' or 'replace'"

        try:
            # Setup paths and names
            file_path = Path(file_path)
            output_folder = Path(output_folder or Path(self._config['project_data']['schemas_dir_path']))
            output_folder.mkdir(parents=True, exist_ok=True)
            table_name = table_name or SQLSchemaGenerator._derive_table_name(file_path)

            # Prepare output path
            schema_path = output_folder / f"{table_name}_schema.sql"

            # Handle existing files
            if schema_path.exists():
                self._logger.info(f"Schema file exists: {schema_path}")
                if if_exists=='fail':
                    self._logger.info(f"Aborting creation of schema: {table_name}")
                    return {
                        "table_name": table_name,
                        "schema_file_path": schema_path
                    }
                else:
                    self._logger.info(f"Replacing schema file: {schema_path}")

            # Initialize analyzer components
            analyzer = PostgreSQLSchemaAnalyzer(
                sampling_strategy=self._sampling_strategy,
                type_inference=self._type_inference
            )

            # Analyze schema
            self._logger.info(f"Analyzing schema for: {file_path}")
            columns = analyzer.analyze_schema(file_path)

            # Generate SQL schema
            sql_schema = SQLSchemaGenerator(table_name).generate_schema(columns, file_path)

            # Write schema to file
            schema_path.write_text(sql_schema)
            self._logger.info(f"Schema generated successfully: {schema_path}")

            return {
                "table_name": table_name,
                "schema_file_path": schema_path,
                "column_count": len(columns),
                "generated_at": datetime.now().isoformat()
            }

        except Exception as e:
            self._logger.error(f"Schema generation failed: {str(e)}")
            raise