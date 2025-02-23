import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
from src.configuration_managing.config_manager import ConfigManager

class TestConfigManagerInit(unittest.TestCase):

    @patch('src.configuration_managing.config_manager.setup_logging')
    @patch('src.configuration_managing.config_manager.logging.getLogger')
    @patch('src.configuration_managing.config_manager.ConfigManager._load_configs')
    def test_default_config_files(self, mock_load_configs, mock_get_logger, mock_setup_logging):
        # Test default configuration files are loaded when no config files are specified
        config_manager = ConfigManager()
        self.assertEqual(config_manager._config_files, ["project_structure_config.yaml", "app_config.yaml"])
        mock_load_configs.assert_called_once_with(["project_structure_config.yaml", "app_config.yaml"])

    @patch('src.configuration_managing.config_manager.setup_logging')
    @patch('src.configuration_managing.config_manager.logging.getLogger')
    @patch('src.configuration_managing.config_manager.ConfigManager._load_configs')
    def test_custom_config_files(self, mock_load_configs, mock_get_logger, mock_setup_logging):
        # Test custom configuration files are loaded when specified
        config_manager = ConfigManager(["custom_config.yaml"], "./custom_config")
        self.assertEqual(config_manager._config_files, ["custom_config.yaml"])
        mock_load_configs.assert_called_once_with(["custom_config.yaml"])

    @patch('src.configuration_managing.config_manager.setup_logging')
    @patch('src.configuration_managing.config_manager.logging.getLogger')
    @patch('src.configuration_managing.config_manager.ConfigManager._load_configs')
    def test_base_path(self, mock_load_configs, mock_get_logger, mock_setup_logging):
        # Test base path is set correctly
        config_manager = ConfigManager(base_path="./custom_config")
        self.assertEqual(config_manager.base_path, Path("./custom_config"))

    @patch('src.configuration_managing.config_manager.setup_logging')
    @patch('src.configuration_managing.config_manager.logging.getLogger')
    @patch('src.configuration_managing.config_manager.ConfigManager._load_configs')
    def test_logger(self, mock_load_configs, mock_get_logger, mock_setup_logging):
        # Test logger is set correctly
        config_manager = ConfigManager()
        mock_get_logger.assert_called_once_with('ConfigManager')

    @patch('src.configuration_managing.config_manager.setup_logging')
    @patch('src.configuration_managing.config_manager.logging.getLogger')
    @patch('src.configuration_managing.config_manager.ConfigManager._load_configs')
    def test_config_dict(self, mock_load_configs, mock_get_logger, mock_setup_logging):
        # Test configuration dictionary is initialized correctly
        config_manager = ConfigManager()
        self.assertEqual(config_manager.config, {})

if __name__ == '__main__':
    unittest.main()