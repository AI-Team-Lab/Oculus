import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# Sicherstellen, dass das Log-Verzeichnis existiert
log_folder = os.path.abspath("logs")  # Absoluter Pfad
os.makedirs(log_folder, exist_ok=True)

import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

# Sicherstellen, dass das Log-Verzeichnis existiert
log_folder = os.path.abspath("logs")  # Absoluter Pfad
os.makedirs(log_folder, exist_ok=True)


def setup_logger(name, log_file_prefix, level=logging.INFO, add_stream_handler=True):
    """
    Erstellt einen Logger mit täglicher Rotierung und Datumsangabe im Dateinamen.

    Args:
        name (str): Name des Loggers.
        log_file_prefix (str): Präfix für den Logdateinamen.
        level (int): Logging-Level.
        add_stream_handler (bool): Ob ein Stream-Handler für die Konsole hinzugefügt werden soll.

    Returns:
        logging.Logger: Konfigurierter Logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Doppelte Handler vermeiden
    if not logger.hasHandlers():
        try:
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
            if add_stream_handler:
                stream_handler = logging.StreamHandler()
                stream_handler.setFormatter(formatter)
                logger.addHandler(stream_handler)

        except Exception as e:
            print(f"⚠️ Fehler beim Einrichten des Loggers '{name}': {e}")

    return logger


# Logger erstellen
database_logger = setup_logger("Database", "database", level=logging.INFO)
celery_logger = setup_logger("Celery", "celery", level=logging.INFO)

# Logger erstellen
database_logger = setup_logger("Database", "database", level=logging.INFO)
celery_logger = setup_logger("Celery", "celery", level=logging.INFO)
