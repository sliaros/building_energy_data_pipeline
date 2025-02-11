import logging
from logging.handlers import RotatingFileHandler

def setup_logging(log_file='logs/application.log', max_bytes=5*1024*1024, backup_count=3):
    """
    Configures the logging system.

    Args:
        log_file (str): Path to the log file.
        max_bytes (int): Maximum size of a log file before rotation (default: 5 MB).
        backup_count (int): Number of backup files to keep (default: 3).
    """
    # Create a custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels of logs

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)

    c_handler.setLevel(logging.INFO)  # Console handler set to INFO level
    f_handler.setLevel(logging.DEBUG)  # File handler set to DEBUG level

    # Create formatters and add them to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
