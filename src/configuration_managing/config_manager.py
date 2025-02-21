import os
import yaml
import logging
from typing import Dict, Any
from pathlib import Path


class ConfigManager:
    """Centralized configuration manager for handling YAML-based settings."""

    def __init__(self, config_files: list = None, base_path: str = "./config"):
        """
        Initializes the configuration manager by loading YAML files.

        Args:
            config_files (list): List of configuration file names.
            base_path (str): The directory where configuration files are stored.
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self.base_path = Path(base_path)
        self.config = {}

        # Default config files
        if config_files is None:
            config_files = ["project_structure_config.yaml", "app_config.yaml"]

        self._load_configs(config_files)

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """Loads and validates a single YAML file."""
        try:
            with open(file_path, "r") as file:
                return yaml.safe_load(file) or {}  # Return an empty dict if YAML is empty
        except yaml.YAMLError as e:
            self._logger.error(f"Error parsing YAML file {file_path}: {e}")
            raise
        except FileNotFoundError:
            self._logger.error(f"Configuration file not found: {file_path}")
            raise

    def _load_configs(self, config_files: list = None):
        """Loads all YAML configuration files and merges them into a single config dictionary."""
        for file in config_files:
            file_path = self.base_path / file
            if file_path.exists():
                self.config.update(self._load_yaml_file(file_path))
            else:
                self._logger.warning(f"Config file {file} not found. Skipping.")

    def get(self, key: str, default=None):
        """Retrieves a configuration value safely."""
        keys = key.split(".")
        value = self.config
        for k in keys:
            value = value.get(k, {})
        return value if value else default

    def validate_config(self):
        """Validate essential configuration keys."""
        required_keys = ["logging.log_file_path", "default_database"]
        for key in required_keys:
            if not self.get(key):
                raise ValueError(f"Missing required configuration: {key}")
