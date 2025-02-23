import yaml
import logging
from typing import Dict, Any
from pathlib import Path
from src.logging_configuration.logging_config import setup_logging

class ConfigManager:
    """Centralized configuration manager for handling YAML-based settings."""

    def __init__(self, config_files: list[str] = None, base_path: str = "./config") -> None:
        """
        Initializes the configuration manager by loading YAML files.

        Args:
            config_files (list[str]): List of configuration file names.
            base_path (str): The directory where configuration files are stored.

        The configuration manager loads configuration settings from YAML files
        at the specified base path. If no configuration files are specified,
        defaults are loaded (project_structure_config.yaml, app_config.yaml).
        """
        # Setup logging first so that we can log any errors that occur
        setup_logging()
        # Get the logger for this class
        self._logger = logging.getLogger(self.__class__.__name__)
        # Set the base path for configuration files
        self.base_path = Path(base_path)
        # Initialize an empty configuration dictionary
        self.config = {}

        # If no config files are specified, load defaults
        if config_files is None:
            # Default configuration files
            config_files = [
                "project_structure_config.yaml",
                "app_config.yaml"
            ]

        # Store the list of configuration files
        self._config_files = config_files
        # Load the configuration from the specified files
        self._load_configs(config_files)

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Loads and validates a single YAML file. This private method is called
        by the _load_configs method to load individual YAML files specified in
        the self._config_files list.

        Args:
            file_path (Path): The path to the YAML file to load.

        Returns:
            Dict[str, Any]: The loaded YAML configuration as a dictionary.

        Raises:
            yaml.YAMLError: If the YAML file is invalid or has a syntax error.
            FileNotFoundError: If the YAML file is not found at the specified path.
        """
        # Open the YAML file and read its contents
        with open(file_path, "r") as file:
            try:
                # Load the YAML configuration and return it as a dictionary
                # If the YAML file is empty, return an empty dictionary
                return yaml.safe_load(file) or {}
            except yaml.YAMLError as e:
                # Log an error if the YAML file is invalid
                self._logger.error(
                    f"Error parsing YAML file {file_path}: {e}")
                # Re-raise the exception to propagate the error
                raise
            except FileNotFoundError:
                # Log an error if the YAML file is not found
                self._logger.error(
                    f"Configuration file not found: {file_path}")
                # Re-raise the exception to propagate the error
                raise

    def _load_configs(self, config_files: list[str] = None) -> None:
        """
        Loads all YAML configuration files specified in the config_files list and
        merges them into a single config dictionary.

        Args:
            config_files (list[str]): A list of configuration file names. If not
                provided, the ConfigManager will load the configuration files
                specified in the self._config_files list set during initialization.

        Returns:
            None
        """
        if config_files is None:
            # If no config_files are provided, use the list of config files provided
            # during initialization
            config_files = self._config_files

        for file in config_files:
            # Construct the full path to the YAML file
            file_path = self.base_path / file

            if file_path.exists():
                # Load the YAML configuration file and merge it with the existing
                # config dictionary
                self.config.update(self._load_yaml_file(file_path))
                # Log a message indicating the file was loaded
                self._logger.info(f"Loaded config file: {file}")
            else:
                # Log a message indicating the file was not found
                self._logger.warning(f"Config file {file} not found. Skipping.")

    def get(self, key: str, default=None) -> Any:
        """Retrieves a configuration value safely.

        Args:
            key (str): The configuration key to retrieve, optionally using dot notation for nested keys.
            default (Any): The default value to return if the key is not found.

        Returns:
            Any: The configuration value or the default value if the key is not found.

        Raises:
            AttributeError: If there is an error accessing the configuration.
        """
        try:
            # Check if the provided key is empty or None
            if not key:
                # If no key is provided, return the default value from the config dictionary
                return self.config.get(default)

            # Split the key by '.' to handle nested keys
            keys = key.split(".")
            # Start with the full config dictionary
            value = self.config
            # Traverse through each part of the key
            for k in keys:
                # Attempt to access the nested dictionary
                value = value.get(k)
                # If the key doesn't exist, return the default value from the config
                if value is None:
                    return self.config.get(default)

            # Return the final value found after traversing the keys
            return value
        except AttributeError as e:
            # Log an error message if there's an issue accessing the configuration using the key
            self._logger.error(f"Error accessing configuration key {key or default}: {e}")
            # Re-raise the exception to signal an error condition
            raise


    def validate_config(self) -> None:
        """
        Validate essential configuration keys.

        This method ensures that the required configuration keys are present in the configuration dictionary.
        This is done by attempting to access the configuration values using the provided key, and raising a ValueError
        if any of the required configuration keys are missing.

        Raises:
            ValueError: If any of the required configuration keys are missing.

        Notes:
            - The "ssl" key is required for managing SSL certificates.
            - The "default_database" key is required for specifying the default database.
            - The "project_data" key is required for specifying the data used in the project.
        """
        required_keys = ["ssl", "default_database", "project_data"]
        try:
            for key in required_keys:
                # Attempt to access the configuration value using the provided key.
                # If the key doesn't exist, get() will return None, and a ValueError will be raised.
                if not self.get(key):
                    # Raise a ValueError with a descriptive error message.
                    raise ValueError(f"Missing required configuration: {key}")
            # Log a message indicating that the configuration validation was successful.
            self._logger.info("Configuration validation successful.")
        except ValueError as e:
            # Log an error message if any of the required configuration keys are missing.
            self._logger.error(e)
            # Re-raise the ValueError to signal an error condition.
            raise
