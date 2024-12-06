import logging
from celery import Celery
from oculus import Willhaben, Database
from rich import print

# Konfiguration für Celery
celery = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
    broker_connection_retry_on_startup=True
)

# Logging für Celery einrichten
log_folder = "/home/ocuadmin/Oculus/logs"  # Pfad zu deinem Projektordner
celery_log_file = f"{log_folder}/celery.log"

# Logger konfigurieren
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(celery_log_file)
file_handler.setFormatter(formatter)

logger = logging.getLogger('celery')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

# Willhaben initialisieren
willhaben = Willhaben()


@celery.task(bind=True)
def fetch_cars_task(self, car_model_make=None):
    """
    Celery-Aufgabe zum Abrufen von Autos und Speichern in der Datenbank.
    """
    db = Database()
    db.connect()

    try:
        table_name = "dbo.willhaben"

        if car_model_make:
            logger.info(f"Processing cars for CAR_MODEL/MAKE: {car_model_make}")
            result = willhaben.process_cars(
                car_model_make=car_model_make,
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
        else:
            logger.info("Processing all cars.")
            result = willhaben.process_cars(
                save_type="db",
                db_instance=db,
                table_name=table_name
            )

        return result

    except Exception as e:
        logger.error(f"Task failed: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise
    finally:
        db.close()
