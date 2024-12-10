from celery import Celery
from oculus.willhaben import Willhaben
from oculus.database import Database
from oculus.logging import celery_logger
from datetime import datetime

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
    task_track_started=True,
)

# Willhaben initialisieren
willhaben = Willhaben()


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_cars_task(self, car_model_make=None, start_make=None):
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id

    try:
        db.connect()
        table_name = "dbo.willhaben"
        celery_logger.info(f"Task {task_id} started at {start_time}. Make: {car_model_make or 'all'}")

        if car_model_make:
            celery_logger.info(f"Processing cars for make: {car_model_make}")
            result = willhaben.process_cars(
                car_model_make=car_model_make,
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
            celery_logger.info(f"Task for {car_model_make} completed successfully.")
            return {"status": "success", "message": f"Cars fetched for make: {car_model_make}"}

        elif start_make:
            celery_logger.info(f"Processing all cars starting from make: {start_make}")
            all_makes = list(willhaben.car_data.keys())
            start_index = all_makes.index(start_make.lower()) if start_make.lower() in all_makes else 0
            for i, make in enumerate(all_makes[start_index:], start=start_index):
                self.update_state(state="PROGRESS", meta={"current_make": make, "progress": f"{i}/{len(all_makes)}"})
                celery_logger.info(f"Processing make: {make}")
                result = willhaben.process_cars(
                    car_model_make=make,
                    save_type="db",
                    db_instance=db,
                    table_name=table_name
                )
                celery_logger.info(f"Completed processing for make: {make}")
            return {"status": "success", "message": f"Cars fetched starting from make: {start_make}"}

        else:
            celery_logger.info("Processing all cars.")
            result = willhaben.process_cars(
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
            celery_logger.info("Task for all cars completed successfully.")
            return {"status": "success", "message": "Cars fetched for all makes"}

    except Exception as e:
        celery_logger.error(f"Task {task_id} failed with error: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise self.retry(exc=e)

    finally:
        db.close()
        end_time = datetime.now()
        celery_logger.info(f"Task {task_id} ended at {end_time}. Duration: {end_time - start_time}")


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def periodic_fetch_task(self, periode=48, rows=200):
    """
    Periodic task to fetch cars based on a specific time range.

    Args:
        self (Task): The task instance automatically passed by Celery.
        periode (int): The number of hours to fetch cars for.
        rows (int): The number of cars to fetch per page.

    Returns:
        dict: Summary of the task.
    """
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id
    total_fetched = 0  # Gesamtanzahl der abgerufenen Fahrzeuge
    page = 1  # Start mit Seite 1

    try:
        # Log Start der Task
        celery_logger.info(f"Task {task_id} started at {start_time}. Fetching cars for the last {periode} hours.")

        # Verbindung zur Datenbank herstellen
        db.connect()
        table_name = "dbo.willhaben"

        while True:
            # Abrufen der Daten von der API
            result = willhaben.search_car(periode=periode, page=page, rows=rows)

            if not result or result.get("rowsReturned", 0) == 0:
                celery_logger.info(f"No more cars found. Total fetched: {total_fetched}")
                break

            adverts = result.get("advertSummaryList", {}).get("advertSummary", [])
            if not adverts:
                celery_logger.warning(f"No cars found on page {page}. Stopping.")
                break

            # Extrahierte Daten speichern
            data_to_save = [willhaben.extract_car_info(car) for car in adverts]
            db.insert_data(table_name, data_to_save, current_make="All", current_page=page)

            # Log Fortschritt
            fetched_count = len(data_to_save)
            total_fetched += fetched_count
            celery_logger.info(f"Page {page}: Fetched and saved {fetched_count} cars. Total so far: {total_fetched}")

            # Fortschritt für Celery aktualisieren
            self.update_state(state="PROGRESS", meta={"current_page": page, "total_fetched": total_fetched})

            # Nächste Seite
            page += 1

        # Log Gesamtabschluss
        celery_logger.info(f"Task {task_id} successfully completed. Total cars fetched: {total_fetched}")
        return {"status": "success", "message": f"Fetched and saved {total_fetched} cars for the last {periode} hours."}

    except Exception as e:
        # Fehlerprotokollierung und Fehlerbehandlung
        celery_logger.error(f"Task {task_id} failed with error: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise self.retry(exc=e)

    finally:
        # Datenbankverbindung schließen und Ende loggen
        db.close()
        end_time = datetime.now()
        celery_logger.info(f"Task {task_id} ended at {end_time}. Duration: {end_time - start_time}")
