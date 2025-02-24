import pytest
from pathlib import Path
import logging
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict

from src.schema_generator.schema_analysis_orchestrator import SchemaAnalysisManager
from src.schema_generator.sampling_strategies import RandomSamplingStrategy
from src.schema_generator.type_inference_engine import PostgreSQLTypeInference
from src.schema_generator.schema_analyzer import PostgreSQLSchemaAnalyzer, SQLSchemaGenerator
from src.schema_generator.base_scema_analyzer import BaseColumnInfo


@pytest.fixture
def mock_config():
    return {
        'project_data': {
            'schemas_dir_path': 'test_schemas'
        }
    }


@pytest.fixture
def mock_logger():
    return Mock(spec=logging.Logger)


@pytest.fixture
def mock_sampling_strategy():
    return Mock(spec=RandomSamplingStrategy)


@pytest.fixture
def mock_type_inference():
    return Mock(spec=PostgreSQLTypeInference)


@pytest.fixture
def schema_manager(mock_config, mock_logger, mock_sampling_strategy, mock_type_inference):
    return SchemaAnalysisManager(
        sampling_strategy=mock_sampling_strategy,
        type_inference=mock_type_inference,
        config=mock_config,
        logger=mock_logger
    )


class TestSchemaAnalysisManager:
    def test_initialization_with_defaults(self):
        """Test that manager initializes with default components when none provided"""
        manager = SchemaAnalysisManager()

        assert isinstance(manager._sampling_strategy, RandomSamplingStrategy)
        assert isinstance(manager._type_inference, PostgreSQLTypeInference)
        assert isinstance(manager._logger, logging.Logger)
        assert manager._config=={'project_data': {'schemas_dir_path': 'schemas'}}

    def test_initialization_with_custom_components(self, schema_manager, mock_config,
                                                   mock_logger, mock_sampling_strategy,
                                                   mock_type_inference):
        """Test that manager initializes correctly with custom components"""
        assert schema_manager._config==mock_config
        assert schema_manager._logger==mock_logger
        assert schema_manager._sampling_strategy==mock_sampling_strategy
        assert schema_manager._type_inference==mock_type_inference

    @pytest.mark.parametrize("if_exists", ['fail', 'replace'])
    def test_generate_schema_success(self, schema_manager, if_exists):
        """Test successful schema generation with different if_exists modes"""
        # Setup mock data
        mock_file_path = Path("test.csv")
        mock_table_name = "test_table"
        mock_output_folder = Path("test_output")
        mock_columns = [
            BaseColumnInfo(name="col1", data_type="INTEGER", nullable=False, original_type="int", stats={}),
            BaseColumnInfo(name="col2", data_type="TEXT", nullable=False, original_type="int", stats={}),
        ]

        # Mock the schema path
        mock_schema_path = mock_output_folder / f"{mock_table_name}_schema.sql"

        # Setup mocks
        with patch("pathlib.Path.mkdir") as mock_mkdir, \
                patch("pathlib.Path.exists") as mock_exists, \
                patch("pathlib.Path.write_text") as mock_write_text, \
                patch.object(PostgreSQLSchemaAnalyzer, "analyze_schema") as mock_analyze, \
                patch.object(SQLSchemaGenerator, "generate_schema") as mock_generate:
            # Configure mocks
            mock_exists.return_value = False
            mock_analyze.return_value = mock_columns
            mock_generate.return_value = "CREATE TABLE test_table..."

            # Execute
            result = schema_manager.generate_schema(
                mock_file_path,
                table_name=mock_table_name,
                output_folder=mock_output_folder,
                if_exists=if_exists
            )

            # Verify
            assert result["table_name"]==mock_table_name
            assert result["schema_file_path"]==mock_schema_path
            assert result["column_count"]==len(mock_columns)
            assert "generated_at" in result

            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_analyze.assert_called_once_with(mock_file_path)
            mock_generate.assert_called_once()
            mock_write_text.assert_called_once()

    def test_generate_schema_existing_file_fail(self, schema_manager):
        """Test schema generation when file exists and if_exists='fail'"""
        mock_file_path = Path("test.csv")
        mock_table_name = "test_table"
        mock_output_folder = Path("test_output")

        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            result = schema_manager.generate_schema(
                mock_file_path,
                table_name=mock_table_name,
                output_folder=mock_output_folder,
                if_exists='fail'
            )

            assert result["table_name"]==mock_table_name
            schema_manager._logger.info.assert_any_call(f"Schema file exists: {mock_output_folder / f'{mock_table_name}_schema.sql'}")

    def test_generate_schema_invalid_if_exists(self, schema_manager):
        """Test schema generation with invalid if_exists value"""
        with pytest.raises(AssertionError):
            schema_manager.generate_schema(
                "test.csv",
                if_exists='invalid'
            )

    def test_generate_schema_error_handling(self, schema_manager):
        """Test error handling during schema generation"""
        mock_error_message = "Test error"

        with patch.object(PostgreSQLSchemaAnalyzer, "analyze_schema") as mock_analyze, \
                pytest.raises(Exception) as exc_info:
            mock_analyze.side_effect = Exception(mock_error_message)

            schema_manager.generate_schema("test.csv")

            schema_manager._logger.error.assert_called_once_with(
                f"Schema generation failed: {mock_error_message}"
            )
            assert str(exc_info.value)==mock_error_message

    def test_setup_logger(self):
        """Test logger setup when no logger is provided"""
        manager = SchemaAnalysisManager(config={'project_data': {'schemas_dir_path': 'test'}})
        logger = manager._logger

        assert isinstance(logger, logging.Logger)
        assert logger.level==logging.INFO
        assert len(logger.handlers)==1
        assert isinstance(logger.handlers[0], logging.StreamHandler)

    @pytest.mark.parametrize("file_path,expected_name", [
        ("data.csv", "data"),
        ("path/to/data.csv", "data"),
        (Path("data.csv"), "data"),
        (Path("path/to/data.csv"), "data")
    ])
    def test_table_name_derivation(self, schema_manager, file_path, expected_name):
        """Test table name derivation from different file paths"""
        with patch.object(PostgreSQLSchemaAnalyzer, "analyze_schema") as mock_analyze, \
                patch.object(SQLSchemaGenerator, "generate_schema") as mock_generate, \
                patch("pathlib.Path.exists") as mock_exists, \
                patch("pathlib.Path.write_text") as mock_write:
            mock_analyze.return_value = []
            mock_exists.return_value = False

            result = schema_manager.generate_schema(file_path)

            assert result["table_name"]==expected_name