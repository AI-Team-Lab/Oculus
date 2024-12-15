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

# Initialize the Willhaben class
willhaben = Willhaben()


@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_cars_task(self, car_model_make=None, start_make=None):
    """
    Celery task to fetch cars from Willhaben and save them to the database.

    Args:
        self (Task): The Celery task instance.
        car_model_make (str): Specific car make/model to fetch. If None, fetch all makes.
        start_make (str): Starting make for iterating through all car makes. This is ignored if `car_model_make` is provided.

    Returns:
        dict: Task result containing status and a message.
    """
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id

    try:
        db.connect()
        table_name = "dl.willhaben"
        celery_logger.info(f"Task {task_id} started at {start_time}. Make: {car_model_make or 'all'}")

        # If a specific make is provided, fetch only that
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

        # If a starting make is specified, iterate through all makes from that point onwards
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

        # Otherwise, fetch cars for all makes
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
    Periodic Celery task to fetch cars for a given time period in hours.

    Args:
        self (Task): The Celery task instance.
        periode (int): Number of hours to fetch cars for (default 48).
        rows (int): Number of cars to fetch per page (default 200).

    Returns:
        dict: A dictionary containing the status and a message about how many cars were fetched.
    """
    db = Database()
    start_time = datetime.now()
    task_id = self.request.id
    total_fetched = 0
    page = 1

    try:
        celery_logger.info(f"Task {task_id} started at {start_time}. Fetching cars for the last {periode} hours.")

        db.connect()
        table_name = "dl.willhaben"

        # Fetch cars page by page until no more results
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
    Celery task to move data from staging tables (dl.*) to the Data Warehouse (dwh.*).

    Args:
        self (Task): The Celery task instance.
        delete_from_staging (bool): Whether to delete rows from the staging tables after moving.

    Returns:
        dict: A dictionary containing the status and a success message.
    """
    db = Database()
    task_id = self.request.id

    try:
        celery_logger.info(f"Task {task_id}: Moving reference data to DWH.")

        db.connect()

        # List of tables for delta-load
        tables_to_sync = [
            {
                "source_table": "dl.make",
                "target_table": "dwh.make",
                "source_columns": ["make_id", "make_name"],
                "target_columns": ["id", "make_name"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.model",
                "target_table": "dwh.model",
                "source_columns": ["model_id", "model_name", "make_id"],
                "target_columns": ["id", "model_name", "make_id"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.exterior_colour_main",
                "target_table": "dwh.color",
                "source_columns": ["id", "colour"],
                "target_columns": ["id", "color_name"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.equipment_search",
                "target_table": "dwh.equipment_details",
                "source_columns": ["id", "equipment_name"],
                "target_columns": ["equipment_code", "equipment"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.transmission",
                "target_table": "dwh.transmission",
                "source_columns": ["id", "transmission_type"],
                "target_columns": ["id", "transmission_type"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.motor_condition",
                "target_table": "dwh.condition",
                "source_columns": ["id", "condition"],
                "target_columns": ["id", "car_condition"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.car_type",
                "target_table": "dwh.car_type",
                "source_columns": ["id", "type"],
                "target_columns": ["id", "type"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.engine_fuel",
                "target_table": "dwh.fuel",
                "source_columns": ["id", "fuel_type"],
                "target_columns": ["id", "fuel_type"],
                "last_updated_field": "last_synced"
            },
            {
                "source_table": "dl.equipment",
                "target_table": "dwh.equipment",
                "source_columns": ["id", "willhaben_id", "equipment_code"],
                "target_columns": ["id", "willhaben_id", "equipment_code"],
                "last_updated_field": "last_synced"
            }
        ]

        # Perform delta-load for each reference table
        for table in tables_to_sync:
            last_sync_time = db.get_last_sync_time(table["source_table"])
            db.move_reference_data(
                source_table=table["source_table"],
                target_table=table["target_table"],
                source_columns=table["source_columns"],
                target_columns=table["target_columns"],
                last_sync_time=last_sync_time,
                last_updated_field=table["last_updated_field"]
            )
            # Update the sync_log table with the current timestamp
            db.update_sync_time(table["source_table"], datetime.now())

        # Transformations for dl.willhaben
        transformations_willhaben = {
            # "make": lambda x: x.lower(),
            # "model": lambda x: x.lower(),
        }

        # Transformations for dl.gebrauchtwagen
        transformations_gebrauchtwagen = {
            "make": lambda x: x.lower(),
            "model": lambda x: x.lower(),
            "engine_fuel": lambda x: x.lower()
        }

        # Move main data from dl.willhaben to dwh.willwagen
        last_sync_time = db.get_last_sync_time("dl.willhaben")
        db.move_data_to_dwh(
            staging_table="dl.willhaben",
            dwh_table="dwh.willwagen",
            transformations=transformations_willhaben,
            source_id=1,
            delete_from_staging=delete_from_staging,
            last_sync_time=last_sync_time,
            last_updated_field="last_synced"
        )
        db.update_sync_time("dl.willhaben", datetime.now())

        # Move main data from dl.gebrauchtwagen to dwh.willwagen
        last_sync_time = db.get_last_sync_time("dl.gebrauchtwagen")
        db.move_data_to_dwh(
            staging_table="dl.gebrauchtwagen",
            dwh_table="dwh.willwagen",
            transformations=transformations_gebrauchtwagen,
            source_id=2,
            delete_from_staging=delete_from_staging,
            last_sync_time=last_sync_time,
            last_updated_field="last_synced"
        )
        db.update_sync_time("dl.gebrauchtwagen", datetime.now())

        celery_logger.info(f"Task {task_id}: Data moved to DWH successfully.")
        return {"status": "success", "message": "Data moved to DWH successfully."}

    except Exception as e:
        celery_logger.error(f"Task {task_id} failed: {type(e).__name__} - {str(e)}")
        self.update_state(state="FAILURE", meta={"error": f"{type(e).__name__}: {str(e)}"})
        raise

    finally:
        db.close()
