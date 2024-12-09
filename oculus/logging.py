import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# Sicherstellen, dass das Log-Verzeichnis existiert
log_folder = "logs"
os.makedirs(log_folder, exist_ok=True)


def setup_logger(name, log_file_prefix, level=logging.INFO):
    """
    Erstellt einen Logger mit täglicher Rotierung und Datumsangabe im Dateinamen.

    Args:
        name (str): Name des Loggers.
        log_file_prefix (str): Präfix für den Logdateinamen.
        level (int): Logging-Level.

    Returns:
        logging.Logger: Konfigurierter Logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Doppelte Handler vermeiden
    if not logger.hasHandlers():
        # Datum in den Dateinamen einfügen
        today_date = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(log_folder, f"{log_file_prefix}-{today_date}.log")

        # Timed Rotating File Handler einrichten
        handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30)
        handler.suffix = "%Y-%m-%d"  # Datumsformat für neue Dateien
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Optional: Stream Handler (für Konsole)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


# Logger erstellen
database_logger = setup_logger("Database", "database")
celery_logger = setup_logger("Celery", "celery")
