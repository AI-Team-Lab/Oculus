import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Ensure the log folder exists
log_folder = os.path.abspath("logs")
os.makedirs(log_folder, exist_ok=True)


def setup_logger(name, log_file_prefix, level=logging.INFO, add_stream_handler=True):
    """
    Sets up a logger with daily rotation and optional console output.

    Args:
        name (str): The name of the logger.
        log_file_prefix (str): Prefix for the log file name.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
        add_stream_handler (bool): Whether to add a stream handler for console output.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if not any(isinstance(handler, TimedRotatingFileHandler) for handler in logger.handlers):
        try:
            # Configure a file handler with rotation
            log_file = os.path.join(log_folder, f"{log_file_prefix}.log")
            file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30)
            file_handler.suffix = "%Y-%m-%d"  # Include date in the log file names
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

            # Optionally, add a stream handler for console output
            if add_stream_handler:
                stream_handler = logging.StreamHandler()
                stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
                stream_handler.setFormatter(stream_formatter)
                logger.addHandler(stream_handler)

        except Exception as e:
            print(f"⚠️ Error setting up logger '{name}': {e}")

    return logger


# Create loggers for different components
flask_logger = setup_logger("Flask", "flask", level=logging.INFO)
willhaben_logger = setup_logger("Willhaben", "willhaben", level=logging.INFO)
database_logger = setup_logger("Database", "database", level=logging.INFO)
celery_logger = setup_logger("Celery", "celery", level=logging.INFO)
