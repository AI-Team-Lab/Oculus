from celery import Celery
from oculus.willhaben import Willhaben
from oculus.database import Database
from oculus.logging import celery_logger
from datetime import datetime

# Celery configuration
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

# Initialize Willhaben
willhaben = Willhaben()


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_cars_task(self, car_model_make=None, start_make=None):
    """
    Celery task to fetch cars from Willhaben and save them to the database.

    Args:
        self (Task): The task instance automatically passed by Celery.
        car_model_make (str): Specific car make/model to fetch. If None, fetch all.
        start_make (str): Starting make for fetching all cars. Ignored if `car_model_make` is provided.

    Returns:
        dict: Task result with success or failure details.
    """
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id

    try:
        db.connect()
        table_name = "dl.willhaben"
        celery_logger.info(f"Task {task_id} started at {start_time}. Make: {car_model_make or 'all'}")

        if car_model_make:
            celery_logger.info(f"Fetching cars for make: {car_model_make}")
            willhaben.process_cars(
                car_model_make=car_model_make,
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
            celery_logger.info(f"Successfully fetched cars for make: {car_model_make}")
            return {"status": "success", "message": f"Fetched cars for make: {car_model_make}"}

        elif start_make:
            celery_logger.info(f"Fetching cars starting from make: {start_make}")
            all_makes = list(willhaben.car_data.keys())
            start_index = all_makes.index(start_make.lower()) if start_make.lower() in all_makes else 0
            for i, make in enumerate(all_makes[start_index:], start=start_index):
                self.update_state(state="PROGRESS", meta={"current_make": make, "progress": f"{i}/{len(all_makes)}"})
                celery_logger.info(f"Fetching cars for make: {make}")
                willhaben.process_cars(
                    car_model_make=make,
                    save_type="db",
                    db_instance=db,
                    table_name=table_name
                )
                celery_logger.info(f"Completed fetching cars for make: {make}")
            return {"status": "success", "message": f"Fetched cars starting from make: {start_make}"}

        else:
            celery_logger.info("Fetching cars for all makes.")
            result = willhaben.process_cars(
                save_type="db",
                db_instance=db,
                table_name=table_name
            )
            celery_logger.info("Successfully fetched cars for all makes.")
            return {"status": "success", "message": "Fetched cars for all makes"}

    except Exception as e:
        celery_logger.error(f"Task {task_id} failed: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise self.retry(exc=e)

    finally:
        db.close()
        end_time = datetime.now()
        celery_logger.info(f"Task {task_id} ended at {end_time}. Duration: {end_time - start_time}")


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def periodic_fetch_task(self, periode=48, rows=200):
    """
    Periodic task to fetch cars for a specific time range.

    Args:
        self (Task): The task instance automatically passed by Celery.
        periode (int): Number of hours to fetch cars for.
        rows (int): Number of cars to fetch per page.

    Returns:
        dict: Summary of fetched data.
    """
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id
    total_fetched = 0
    page = 1

    try:
        celery_logger.info(f"Task {task_id} started at {start_time}. Fetching cars for the last {periode} hours.")

        db.connect()
        table_name = "dbo.willhaben"

        while True:
            result = willhaben.search_car(periode=periode, page=page, rows=rows)

            if not result or result.get("rowsReturned", 0) == 0:
                celery_logger.info(f"No more cars found. Total fetched: {total_fetched}")
                break

            adverts = result.get("advertSummaryList", {}).get("advertSummary", [])
            if not adverts:
                celery_logger.warning(f"No cars found on page {page}. Stopping.")
                break

            data_to_save = [willhaben.extract_car_info(car) for car in adverts]
            db.insert_data(table_name, data_to_save, current_make="All", current_page=page)

            fetched_count = len(data_to_save)
            total_fetched += fetched_count
            celery_logger.info(f"Page {page}: Fetched {fetched_count} cars. Total so far: {total_fetched}")

            self.update_state(state="PROGRESS", meta={"current_page": page, "total_fetched": total_fetched})
            page += 1

        celery_logger.info(f"Task {task_id} completed. Total cars fetched: {total_fetched}")
        return {"status": "success", "message": f"Fetched {total_fetched} cars for the last {periode} hours."}

    except Exception as e:
        celery_logger.error(f"Task {task_id} failed: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise self.retry(exc=e)

    finally:
        db.close()
        end_time = datetime.now()
        celery_logger.info(f"Task {task_id} ended at {end_time}. Duration: {end_time - start_time}")


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def move_data_to_dwh_task(self, delete_from_staging=False):
    """
    Moves data from the staging table to the Data Warehouse.

    Args:
        self (Task): The task instance automatically passed by Celery.
        delete_from_staging (bool): Whether to delete data from the staging table after moving.

    Returns:
        dict: Task result indicating success or failure.
    """
    db = Database()
    task_id = self.request.id

    try:
        celery_logger.info(f"Task {task_id}: Moving data to DWH. Delete from staging: {delete_from_staging}")

        db.connect()
        rows_moved = db.move_data_to_dwh(delete_from_staging=delete_from_staging)

        celery_logger.info(f"Task {task_id}: Moved {rows_moved} rows to DWH.")
        return {"status": "success", "message": f"Moved {rows_moved} rows to DWH."}

    except Exception as e:
        celery_logger.error(f"Task {task_id} failed: {e}")
        self.update_state(state="FAILURE", meta={"error": str(e)})
        raise

    finally:
        db.close()
