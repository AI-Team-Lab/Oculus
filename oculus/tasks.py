from celery import Celery
from oculus import Willhaben, Database, celery_logger

# Celery-Konfiguration
celery = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
    broker_connection_retry_on_startup=True
)

celery.conf.update(
    accept_content=["json"],
    task_serializer="json",
    result_serializer="json",
    worker_pool="solo",  # Wähle den passenden Pool
)

# Willhaben initialisieren
willhaben = Willhaben()


@celery.task(bind=True)
def fetch_cars_task(self, car_model_make=None):
    """
    Celery-Aufgabe zum Abrufen von Autos und Speichern in der Datenbank.
    """
    db = Database()

    try:
        db.connect()
        table_name = "dbo.willhaben"

        if car_model_make:
            celery_logger.info(f"Processing cars for CAR_MODEL/MAKE: {car_model_make}")
            result = willhaben.process_cars(
                car_model_make=car_model_make,
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
        else:
            celery_logger.info("Processing all cars.")
            result = willhaben.process_cars(
                save_type="db",
                db_instance=db,
                table_name=table_name
            )

        celery_logger.info(f"Task completed successfully: {result}")
        return result

    except Exception as e:
        celery_logger.error(f"Task failed: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise
    finally:
        db.close()
