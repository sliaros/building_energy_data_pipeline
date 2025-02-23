import pytest
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
from unittest.mock import patch, MagicMock
from src.logging_configuration.logging_config import setup_logging  # Replace with actual import


class TestLoggingSetup:
    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """
        Fixture to clean up logging configuration before and after each test.
        This prevents tests from affecting each other through the logging system.
        """
        # Store original logging configuration
        self.original_handlers = logging.getLogger().handlers.copy()
        self.original_level = logging.getLogger().level

        # Clear all handlers
        logging.getLogger().handlers.clear()

        yield

        # Restore original logging configuration
        logging.getLogger().handlers = self.original_handlers.copy()
        logging.getLogger().setLevel(self.original_level)

        # Clean up any test log files
        if hasattr(self, 'test_log_file') and os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)
            # Clean up any rotated files
            for i in range(1, 4):
                backup_file = f"{self.test_log_file}.{i}"
                if os.path.exists(backup_file):
                    os.remove(backup_file)

    @pytest.fixture
    def temp_log_dir(self, tmp_path):
        """Create a temporary directory for log files"""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        return log_dir

    def test_default_setup(self, temp_log_dir):
        """Test logging setup with default parameters"""
        self.test_log_file = str(temp_log_dir / "application.log")

        setup_logging(self.test_log_file)
        logger = logging.getLogger()

        # Verify logger level
        assert logger.level==logging.DEBUG

        # Verify handlers
        assert len(logger.handlers)==2
        handlers = {type(h): h for h in logger.handlers}

        # Verify StreamHandler
        assert logging.StreamHandler in handlers
        stream_handler = handlers[logging.StreamHandler]
        assert stream_handler.level==logging.INFO

        # Verify RotatingFileHandler
        assert RotatingFileHandler in handlers
        file_handler = handlers[RotatingFileHandler]
        assert file_handler.level==logging.DEBUG
        assert file_handler.baseFilename==str(Path(self.test_log_file))
        assert file_handler.maxBytes==5 * 1024 * 1024  # 5 MB
        assert file_handler.backupCount==3

    def test_custom_parameters(self, temp_log_dir):
        """Test logging setup with custom parameters"""
        self.test_log_file = str(temp_log_dir / "custom.log")
        custom_max_bytes = 1024 * 1024  # 1 MB
        custom_backup_count = 5

        setup_logging(
            self.test_log_file,
            max_bytes=custom_max_bytes,
            backup_count=custom_backup_count
        )

        logger = logging.getLogger()
        file_handler = next(h for h in logger.handlers if isinstance(h, RotatingFileHandler))

        assert file_handler.maxBytes==custom_max_bytes
        assert file_handler.backupCount==custom_backup_count

    def test_log_rotation(self, temp_log_dir):
        """Test that log files rotate correctly when size limit is reached"""
        self.test_log_file = str(temp_log_dir / "rotation.log")
        max_bytes = 100  # Small size to trigger rotation

        setup_logging(self.test_log_file, max_bytes=max_bytes, backup_count=3)
        logger = logging.getLogger()

        # Write enough logs to trigger rotation
        message = "X" * 50 + "\n"  # 51 bytes with newline
        for _ in range(10):  # Should create multiple rotation files
            logger.info(message)

        # Check that files were rotated
        assert os.path.exists(self.test_log_file)
        assert os.path.exists(f"{self.test_log_file}.1")
        assert os.path.exists(f"{self.test_log_file}.2")

    def test_log_formatting(self, temp_log_dir):
        """Test that log messages are formatted correctly"""
        self.test_log_file = str(temp_log_dir / "format.log")

        setup_logging(self.test_log_file)
        logger = logging.getLogger("test_logger")

        test_message = "Test log message"
        logger.info(test_message)

        # Read the log file
        with open(self.test_log_file, 'r') as f:
            log_content = f.read()

        # Verify format components
        assert "test_logger" in log_content
        assert "INFO" in log_content
        assert test_message in log_content
        assert " - " in log_content  # Format separator

    def test_default_log_file_creation(self):
        """Test that default log file and directory are created if not specified"""
        with patch('pathlib.Path') as mock_path:
            mock_path_instance = MagicMock()
            mock_path.return_value = mock_path_instance
            mock_path_instance.parent.exists.return_value = False

            setup_logging()

            # Verify default path is used
            mock_path.assert_called_with('logs/application.log')

    def test_multiple_setup_calls(self, temp_log_dir):
        """Test that multiple calls to setup_logging don't duplicate handlers"""
        self.test_log_file = str(temp_log_dir / "multiple.log")

        # Call setup_logging multiple times
        setup_logging(self.test_log_file)
        initial_handler_count = len(logging.getLogger().handlers)

        setup_logging(self.test_log_file)
        final_handler_count = len(logging.getLogger().handlers)

        assert initial_handler_count==final_handler_count==2

    def test_log_levels(self, temp_log_dir):
        """Test that different log levels are handled correctly"""
        self.test_log_file = str(temp_log_dir / "levels.log")

        setup_logging(self.test_log_file)
        logger = logging.getLogger("test_levels")

        # Test all log levels
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")
        logger.critical("Critical message")

        # Read the log file
        with open(self.test_log_file, 'r') as f:
            log_content = f.read()

        # Debug should be in file but not console
        assert "Debug message" in log_content

        # Check all other levels
        assert "Info message" in log_content
        assert "Warning message" in log_content
        assert "Error message" in log_content
        assert "Critical message" in log_content

    @pytest.mark.parametrize("invalid_path", [
        "/nonexistent/directory/log.txt",
        "//invalid//path//log.txt",
    ])
    def test_invalid_log_file_path(self, invalid_path):
        """Test handling of invalid log file paths"""
        with pytest.raises(Exception):
            setup_logging(invalid_path)