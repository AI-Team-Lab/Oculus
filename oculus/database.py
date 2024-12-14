import os
import pymssql
import logging
from oculus.logging import database_logger
from dotenv import load_dotenv
from datetime import datetime, timezone
import json

load_dotenv()


class DatabaseError(Exception):
    pass


class Database:
    def __init__(self):
        self.logger = logging.getLogger("Database")
        self.logger.propagate = False
        self.conn = None
        self.cursor = None

        # Load environment variables
        self._load_env_file()

        # Assign environment variables
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_DATABASE")

    def _load_env_file(self):
        """
        Loads the .env file and logs if it is missing or fails to load.

        Raises:
            FileNotFoundError: If the .env file is not found.
        """
        env_file = ".env"
        if not os.path.exists(env_file):
            self.logger.error(f"Environment file '{env_file}' not found.")
            raise FileNotFoundError(f"Environment file '{env_file}' not found.")

        try:
            load_dotenv(env_file)
            self.logger.info(f"Environment file '{env_file}' loaded successfully.")
        except Exception as e:
            self.logger.error(f"Failed to load environment file '{env_file}': {e}")
            raise

    def connect(self):
        """
        Establishes a connection to the database if not already connected.

        Logs:
            - Information when the connection is already established.
            - Success message upon successful connection.
            - Error message in case of connection failure.

        Raises:
            Exception: If the connection to the database fails.
        """
        if self.conn:
            self.logger.info("Database connection is already established.")
            return

        # Check for missing critical environment variables
        missing_vars = [
            var for var in ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_DATABASE"]
            if not os.getenv(var)
        ]
        if missing_vars:
            self.logger.error(f"Missing critical environment variables: {', '.join(missing_vars)}")
            raise EnvironmentError(f"Missing critical environment variables: {', '.join(missing_vars)}")

        try:
            self.conn = pymssql.connect(
                server=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8",
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Successfully established a connection to the database.")
        except Exception as e:
            self.logger.error(f"Failed to establish database connection: {e}")
            raise

    def close(self):
        """
        Closes the database connection and resets the cursor and connection attributes.

        Logs:
            - Success message upon closing the connection.
        """
        try:
            if self.cursor:
                self.cursor.close()
                database_logger.info("Database cursor closed successfully.")
        except Exception as e:
            database_logger.warning(f"Failed to close the database cursor: {e}")

        try:
            if self.conn:
                self.conn.close()
                database_logger.info("Database connection closed successfully.")
        except Exception as e:
            database_logger.warning(f"Failed to close the database connection: {e}")

        self.conn = None
        self.cursor = None

    def ensure_connection(self):
        """
        Ensures that the database connection is active. Reconnects if necessary.

        Logs:
            - Warning message if the connection is being re-established.
            - Delegates the connection process to the `connect` method.
        """
        if not self.conn:
            database_logger.warning("No active database connection. Attempting to reconnect...")
            self.connect()

    def insert_data(self, table_name, data, current_make="Unknown", current_page="Unknown"):
        """
        Inserts data into a specified table.

        Args:
            table_name (str): The name of the table to insert data into.
            data (list): A list of dictionaries, each representing a row of data.
            current_make (str): The car make currently being processed (default: "Unknown").
            current_page (str): The page number currently being processed (default: "Unknown").

        Raises:
            DatabaseError: If the insertion process fails.
        """
        self.ensure_connection()
        successful_inserts = 0  # Counter for successful insertions

        try:
            for row in data:
                try:
                    # Replace invalid or missing values with appropriate defaults
                    for key, value in row.items():
                        if value in ("N/A", "", None):
                            row[key] = [] if key in ("equipment", "equipment_resolved", "all_image_urls") else None

                    # Extract non-equipment columns for the main table
                    main_columns = [col for col in row.keys() if
                                    col not in ("equipment", "equipment_resolved", "all_image_urls")]
                    main_placeholders = ["%s" for _ in main_columns]

                    # Prepare data for the main table
                    main_data = tuple(row[col] for col in main_columns)

                    # Insert main data into the table
                    main_sql = f"INSERT INTO {table_name} ({', '.join(main_columns)}) VALUES ({', '.join(main_placeholders)})"
                    self.cursor.execute(main_sql, main_data)
                    successful_inserts += 1  # Increment success counter

                    # Use the 'id' field as the willhaben_id
                    willhaben_id = row["id"]

                    # Insert equipment data into the 'equipment' table
                    equipment_list = row.get("equipment", [])
                    equipment_resolved_list = row.get("equipment_resolved", [])
                    equipment_sql = "INSERT INTO dl.equipment (willhaben_id, equipment_code, equipment_resolved) VALUES (%s, %s, %s)"

                    if equipment_list and equipment_resolved_list:
                        for equipment, resolved in zip(equipment_list, equipment_resolved_list):
                            self.cursor.execute(equipment_sql, (willhaben_id, equipment, resolved))

                except pymssql.IntegrityError:
                    # Log duplicate key error
                    duplicate_make = row.get("make", "Unknown")
                    self.logger.warning(
                        f"Duplicate entry for ID {row['id']}. Skipping insertion. Make: '{duplicate_make}', Page: {current_page}."
                    )
                    self.conn.rollback()
                    continue
                except Exception as e:
                    # Log any other errors
                    self.logger.error(
                        f"Error inserting data for ID {row['id']}: {e}. Make: '{current_make}', Page: {current_page}."
                    )
                    self.conn.rollback()
                    continue

            # Commit transaction after processing all rows
            self.conn.commit()

            if successful_inserts > 0:
                self.logger.info(
                    f"Data successfully saved to table '{table_name}' for make '{current_make}' on page {current_page}. "
                    f"Total successful inserts: {successful_inserts}."
                )
            else:
                self.logger.warning(
                    f"No data inserted into table '{table_name}' for make '{current_make}' on page {current_page}. "
                    "All entries were duplicates."
                )

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to insert data into '{table_name}': {e}")
            raise DatabaseError(f"Failed to insert data into '{table_name}': {e}")

    @staticmethod
    def clear_table(db_instance, table_name):
        """
        Clears all rows from the specified table.

        Args:
            db_instance (Database): The database instance.
            table_name (str): The name of the table to clear.

        Raises:
            DatabaseError: If clearing the table fails.
        """
        db_instance.ensure_connection()
        try:
            # Use TRUNCATE for a fast and efficient deletion
            truncate_query = f"TRUNCATE TABLE {table_name};"
            db_instance.cursor.execute(truncate_query)
            db_instance.conn.commit()
            db_instance.logger.info(f"Table '{table_name}' successfully cleared.")
        except Exception as e:
            db_instance.logger.error(f"Error while clearing table '{table_name}': {e}")
            db_instance.conn.rollback()
            raise DatabaseError(f"Failed to clear table '{table_name}': {e}")

    def load_car_data(self, file_path):
        """
        Loads car data from a JSON file, clears the `makes` and `models` tables, and inserts new data.

        Args:
            file_path (str): The path to the JSON file.

        Raises:
            DatabaseError: If an error occurs while inserting data.
        """
        self.ensure_connection()

        try:
            # Start transaction
            self.logger.info("Clearing 'makes' and 'models' tables...")
            try:
                # Temporarily disable constraints for dependent tables
                self.cursor.execute("ALTER TABLE dl.model NOCHECK CONSTRAINT ALL")
                self.cursor.execute("ALTER TABLE dl.make NOCHECK CONSTRAINT ALL")

                # Delete data instead of truncate to avoid foreign key constraint issues
                self.cursor.execute("DELETE FROM dl.model")
                self.cursor.execute("DELETE FROM dl.make")

                # Re-enable constraints
                self.cursor.execute("ALTER TABLE dl.model CHECK CONSTRAINT ALL")
                self.cursor.execute("ALTER TABLE dl.make CHECK CONSTRAINT ALL")

                self.logger.info("Tables 'dl.make' and 'dl.model' successfully cleared.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to clear tables 'dl.make' and 'dl.model': {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # Load data from the JSON file
            with open(file_path, 'r', encoding='utf-8') as file:
                car_data = json.load(file)

            # Insert the data
            self.logger.info("Inserting data from the JSON file into the database...")
            for make_name, make_data in car_data.items():
                make_id = make_data.get("id")
                models = make_data.get("models", {})

                # Insert make
                insert_make_query = "INSERT INTO dl.make (make_id, make_name) VALUES (%s, %s)"
                self.cursor.execute(insert_make_query, (make_id, make_name))

                # Insert models
                for model_name, model_id in models.items():
                    insert_model_query = "INSERT INTO dl.model (model_id, model_name, make_id) VALUES (%s, %s, %s)"
                    self.cursor.execute(insert_model_query, (model_id, model_name, make_id))

            # Commit changes
            self.conn.commit()
            self.logger.info(f"Data successfully loaded from {file_path} into the database.")

        except Exception as e:
            # Rollback transaction in case of errors
            self.conn.rollback()
            self.logger.error(f"Failed to load car data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load car data: {e}")

    def load_car_engine(self, file_path):
        """
        Loads engine data from a JSON file, clears the respective tables, and inserts new data.

        Args:
            file_path (str): The path to the JSON file.

        Raises:
            DatabaseError: If an error occurs during data insertion.
        """
        # Ensure the database connection is active
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for engine data...")
            try:
                # Disable constraints temporarily
                tables = [
                    "dl.engine_effect",
                    "dl.engine_fuel",
                    "dl.battery_capacity",
                    "dl.wltp_range",
                    "dl.transmission",
                    "dl.wheel_drive",
                ]
                for table in tables:
                    self.cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
                    self.cursor.execute(f"DELETE FROM {table}")
                    self.cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
                self.conn.commit()
                self.logger.info("Tables for engine data successfully cleared.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to clear tables for engine data: {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # Load data from the JSON file
            self.logger.info("Loading engine data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                engine_data = json.load(file)

            # Insert data into respective tables
            self.logger.info("Inserting engine data into the database...")
            for key, values in engine_data.items():
                if key == "engineeffect_from":
                    for effect_id, power in values.items():
                        insert_query = "INSERT INTO dl.engine_effect (power, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (effect_id, power))
                elif key == "engine_fuel":
                    for fuel_id, fuel_type in values.items():
                        insert_query = "INSERT INTO dl.engine_fuel (fuel_type, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (fuel_id, fuel_type))
                elif key == "battery_capacity_from":
                    for capacity_id, capacity in values.items():
                        insert_query = "INSERT INTO dl.battery_capacity (capacity, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (capacity_id, capacity))
                elif key == "wltp_range_from":
                    for range_id, range_value in values.items():
                        insert_query = "INSERT INTO dl.wltp_range (range, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (range_id, range_value))
                elif key == "transmission":
                    for transmission_id, transmission_type in values.items():
                        insert_query = "INSERT INTO dl.transmission (transmission_type, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (transmission_id, transmission_type))
                elif key == "wheel_drive":
                    for drive_id, drive_type in values.items():
                        insert_query = "INSERT INTO dl.wheel_drive (drive_type, id) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (drive_id, drive_type))

            # Commit changes to the database
            self.conn.commit()
            self.logger.info(f"Engine data successfully loaded from {file_path} into the database.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to load engine data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load engine data: {e}")

    def load_car_equipment(self, file_path):
        """
        Loads equipment data from a JSON file, clears the respective tables, and inserts new data.

        Args:
            file_path (str): The path to the JSON file.

        Raises:
            DatabaseError: If an error occurs during data insertion.
        """
        # Ensure the database connection is active
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for equipment data...")
            try:
                # Define the tables to be cleared
                tables = ["dl.equipment_search", "dl.exterior_colour_main", "dl.no_of_doors", "dl.no_of_seats"]

                # Loop through each table, disable constraints, clear data, and re-enable constraints
                for table in tables:
                    self.cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
                    self.cursor.execute(f"DELETE FROM {table}")
                    self.cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
                self.conn.commit()
                self.logger.info("Tables for equipment data successfully cleared.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to clear tables for equipment data: {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # Load data from the JSON file
            self.logger.info("Loading equipment data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                equipment_data = json.load(file)

            # Insert data into respective tables
            self.logger.info("Inserting equipment data into the database...")
            inserted_doors = set()  # Track inserted door IDs
            inserted_seats = set()  # Track inserted seat IDs

            for key, values in equipment_data.items():
                if key == "equipment":
                    for equipment_name, equipment_id in values.items():
                        insert_query = "INSERT INTO dl.equipment_search (id, equipment_name) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (equipment_id, equipment_name))
                elif key == "exterior_colour_main":
                    for colour, colour_id in values.items():
                        insert_query = "INSERT INTO dl.exterior_colour_main (id, colour) VALUES (%s, %s)"
                        self.cursor.execute(insert_query, (colour_id, colour))
                elif key in ["no_of_doors_from", "no_of_doors_to"]:
                    for door_count, door_id in values.items():
                        if door_id not in inserted_doors:  # Avoid duplicate entries
                            insert_query = "INSERT INTO dl.no_of_doors (id, door_count) VALUES (%s, %s)"
                            self.cursor.execute(insert_query, (door_id, door_count))
                            inserted_doors.add(door_id)
                elif key in ["no_of_seats_from", "no_of_seats_to"]:
                    for seat_count, seat_id in values.items():
                        if seat_id not in inserted_seats:  # Avoid duplicate entries
                            insert_query = "INSERT INTO dl.no_of_seats (id, seat_count) VALUES (%s, %s)"
                            self.cursor.execute(insert_query, (seat_id, seat_count))
                            inserted_seats.add(seat_id)

            # Commit changes to the database
            self.conn.commit()
            self.logger.info(f"Equipment data successfully loaded from {file_path} into the database.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to load equipment data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load equipment data: {e}")

    def load_car_location(self, file_path):
        """
        Loads location data from a JSON file, clears the respective tables, and inserts new data.

        Args:
            file_path (str): The path to the JSON file.

        Raises:
            DatabaseError: If an error occurs during data insertion.
        """
        # Ensure the database connection is active
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for location data...")
            try:
                # Define the tables to be cleared
                tables = ["dl.area", "dl.location", "dl.dealer", "dl.periode"]

                # Loop through each table, disable constraints, clear data, and re-enable constraints
                for table in tables:
                    self.cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
                    self.cursor.execute(f"DELETE FROM {table}")
                    self.cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
                self.conn.commit()
                self.logger.info("Tables for location data successfully cleared.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to clear tables for location data: {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # Load data from the JSON file
            self.logger.info("Loading location data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                location_data = json.load(file)

            # Insert data into respective tables
            self.logger.info("Inserting location data into the database...")

            # Insert Locations and Areas
            for location_name, location_details in location_data.get("locations", {}).items():
                location_id = location_details.get("id")
                if location_id is None:
                    self.logger.warning(f"Skipping location '{location_name}' due to missing ID.")
                    continue

                # Insert Location
                insert_location_query = "INSERT INTO dl.location (id, name) VALUES (%s, %s)"
                self.cursor.execute(insert_location_query, (location_id, location_name))

                # Insert Areas
                for area_name, area_id in location_details.get("areas", {}).items():
                    insert_area_query = "INSERT INTO dl.area (id, name, location_id) VALUES (%s, %s, %s)"
                    self.cursor.execute(insert_area_query, (area_id, area_name, location_id))

            # Insert Dealers
            for dealer_type, dealer_id in location_data.get("dealer", {}).items():
                insert_dealer_query = "INSERT INTO dl.dealer (id, type) VALUES (%s, %s)"
                self.cursor.execute(insert_dealer_query, (dealer_id, dealer_type))

            # Insert Periods
            for period_name, period_id in location_data.get("periode", {}).items():
                insert_periode_query = "INSERT INTO dl.periode (id, period) VALUES (%s, %s)"
                self.cursor.execute(insert_periode_query, (period_id, period_name))

            # Commit changes to the database
            self.conn.commit()
            self.logger.info(f"Location data successfully loaded from {file_path} into the database.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to load location data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load location data: {e}")

    def load_car_status(self, file_path):
        """
        Loads car status data from a JSON file, clears the respective tables, and inserts new data.

        Args:
            file_path (str): The path to the JSON file.

        Raises:
            DatabaseError: If an error occurs during data insertion.
        """
        # Ensure the database connection is active
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for car status data...")
            try:
                # Define the tables to be cleared
                tables = ["dl.car_type", "dl.motor_condition", "dl.warranty"]

                # Loop through each table, disable constraints, clear data, and re-enable constraints
                for table in tables:
                    self.cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
                    self.cursor.execute(f"DELETE FROM {table}")
                    self.cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
                self.conn.commit()
                self.logger.info("Tables for car status data successfully cleared.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to clear tables for car status data: {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # Load data from the JSON file
            self.logger.info("Loading car status data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                car_data = json.load(file)

            # Insert data into respective tables
            self.logger.info("Inserting car status data into the database...")

            # Insert car types
            for car_type_name, car_type_id in car_data.get("car_type", {}).items():
                insert_car_type_query = "INSERT INTO dl.car_type (id, type) VALUES (%s, %s)"
                self.cursor.execute(insert_car_type_query, (car_type_id, car_type_name))

            # Insert motor conditions
            for condition_name, condition_id in car_data.get("motor_condition", {}).items():
                insert_motor_condition_query = "INSERT INTO dl.motor_condition (id, condition) VALUES (%s, %s)"
                self.cursor.execute(insert_motor_condition_query, (condition_id, condition_name))

            # Insert warranty information
            for warranty_name, warranty_id in car_data.get("warranty", {}).items():
                insert_warranty_query = "INSERT INTO dl.warranty (id, warranty_available) VALUES (%s, %s)"
                self.cursor.execute(insert_warranty_query, (warranty_id, warranty_name))

            # Commit changes to the database
            self.conn.commit()
            self.logger.info(f"Car status data successfully loaded from {file_path} into the database.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to load car status data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load car status data: {e}")

    def move_reference_data(self, source_table, target_table, source_columns, target_columns, last_sync_time,
                            last_updated_field):
        """
        Moves reference data from source_table to target_table with incremental loading using MERGE.
        """
        self.ensure_connection()

        try:
            # Delta-Query basierend auf last_sync_time
            if last_sync_time:
                query = f"SELECT {', '.join(source_columns)} FROM {source_table} WHERE {last_updated_field} > %s"
                self.cursor.execute(query, (last_sync_time,))
            else:
                query = f"SELECT {', '.join(source_columns)} FROM {source_table}"
                self.cursor.execute(query)

            rows = self.cursor.fetchall()

            if not rows:
                self.logger.info(f"No new or updated records found in {source_table}.")
                return

            # Daten übertragen
            for row in rows:
                source_data = dict(zip(source_columns, row))

                # Verwende eine MERGE-Abfrage, um Duplikate zu vermeiden
                merge_query = f"""
                    MERGE {target_table} AS target
                    USING (SELECT {', '.join(['%s AS ' + col for col in target_columns])}) AS source
                    ON (target.{target_columns[0]} = source.{target_columns[0]})  -- Match anhand des Primärschlüssels
                    WHEN MATCHED THEN 
                        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in target_columns[1:]])}  -- Update wenn es existiert
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(target_columns)}) VALUES ({', '.join(['source.' + col for col in target_columns])});  -- Insert wenn es nicht existiert
                """
                self.cursor.execute(merge_query, tuple(row))

            # Aktualisiere 'last_synced' in der Quelltabelle
            if last_sync_time:
                update_query = f"""
                    UPDATE {source_table}
                    SET {last_updated_field} = GETDATE()
                    WHERE {last_updated_field} IS NULL OR {last_updated_field} > %s
                """
                self.cursor.execute(update_query, (last_sync_time,))

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to move data from {source_table} to {target_table}: {e}")
            raise

    def get_last_sync_time(self, table_name):
        """
        Retrieves the last synchronization time for a table.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            datetime: The last synchronization time, or None if no record exists.
        """
        self.ensure_connection()
        query = "SELECT last_sync_time FROM dwh.sync_log WHERE table_name = %s"
        self.cursor.execute(query, (table_name,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def update_sync_time(self, table_name, sync_time):
        """
        Updates the sync time for a specific table in the sync log.

        Args:
            table_name (str): Name of the table to update.
            sync_time (datetime): The new sync time.
        """
        self.ensure_connection()

        try:
            # Verwende MERGE, um den Synchronisationszeitpunkt zu aktualisieren oder einen neuen Eintrag zu erstellen
            query = """
            MERGE INTO dwh.sync_log AS target
            USING (SELECT %s AS table_name, %s AS last_sync_time) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN
                UPDATE SET last_sync_time = source.last_sync_time
            WHEN NOT MATCHED THEN
                INSERT (table_name, last_sync_time) VALUES (source.table_name, source.last_sync_time);
            """
            self.cursor.execute(query, (table_name, sync_time))
            self.conn.commit()
            self.logger.info(f"Sync log updated for table: {table_name}, time: {sync_time}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to update sync log for table: {table_name}. Error: {e}")
            raise

    def update_last_synced(self, table_name, sync_time):
        """
        Aktualisiert die Spalte 'last_synced' in der Quelltabelle.

        Args:
            table_name (str): Name der Quelltabelle.
            sync_time (datetime): Zeitstempel des letzten Syncs.
        """
        query = f"UPDATE {table_name} SET last_synced = %s"
        self.cursor.execute(query, (sync_time,))
        self.conn.commit()
        self.logger.info(f"Updated last_synced for table {table_name} to {sync_time}.")

    def move_data_to_dwh(self, staging_table, dwh_table, transformations=None, source_id=None,
                         delete_from_staging=False, last_sync_time=None, last_updated_field=None):
        """
        Transforms and moves data from the staging table to the Data Warehouse.

        Args:
            staging_table (str): Name of the staging table.
            dwh_table (str): Name of the target Data Warehouse table.
            transformations (dict): Dictionary of transformations to apply to specific fields.
            source_id (int): ID representing the source of the data.
            delete_from_staging (bool): Whether to delete data from the staging table after processing.
            last_sync_time (datetime): Timestamp of the last successful sync for incremental loading.
            last_updated_field (str): Name of the field used to track updates in the source table.

        Raises:
            Exception: If the data transformation or movement process fails.
        """
        self.ensure_connection()

        # Define the mapping for make transformations
        make_mapping = {
            "alfa romeo": "alfa_romeo",
            "aston martin": "aston_martin",
            "citroën": "citroen",
            "chevrolet / daewoo": "chevrolet_daewoo",
            "land rover": "land_rover",
            "mercedes-benz": "merceds_benz",
            "rolls-royce": "rolls_royce",
            "british leyland": "british_leyland",
            "graf carello": "graf_carello",
            "ds automobiles": "ds_automobiles",
            "skywell automobile": "skywell_automobile",
            "lynk & co": "lynk_&_co",
        }

        # Define the mapping for model transformations
        model_mapping = {
            "3er-reihe": "3er_reihe",
            "id.7": "id_7",
            "ioniq 6": "ioniq6",
            "cl-klasse": "cl_klasse",
            "range rover": "range_rover",
            "c3 picasso": "c3_picasso",
            "seal u": "seal_u",
            "crossland x": "crossland_x",
            "slk-klasse": "slk_klasse",
            "rx-8": "rx8",
            "r 19": "r19",
            "124 spider": "124_spider",
            "saratoga": "saratoga",
            "rx-7": "rx7",
            "vivaro-e": "vivaro_e",
            "zs ev": "zs_ev",
            "gle-klasse": "gle_klasse",
            "slc-klasse": "slc_klasse",
            "g-klasse": "g_klasse",
            "model 3": "model_3",
            "sls amg": "sls_amg",
            "h-1": "h1",
            "le baron": "le_baron",
            "trans sport": "trans_sport",
            "serie 400": "serie_400",
            "sx4 s-cross": "sx4_s_cross",
            "xedos 9": "xedos9",
            "id.5": "id_5",
            "santa fe": "santa_fe",
            "ds 3": "ds3",
            "model s": "model_s",
            "c5 x": "c5_x",
            "488 gtb": "488_gtb",
            "cle-klasse": "cle_klasse",
            "xsara picasso": "xsara_picasso",
            "vel satis": "vel_satis",
            "c4 aircross": "c4_aircross",
            "punto evo": "punto_evo",
            "200 sx": "200sx",
            "sigma": "sigma",
            "a 310": "a310",
            "wagon r": "wagon_r",
            "5er-reihe": "5er_reihe",
            "gla-klasse": "gla_klasse",
            "t-cross": "t_cross",
            "range rover evoque": "range_rover_evoque",
            "f-pace": "f_pace",
            "discovery sport": "discovery_sport",
            "grand c4 spacetourer": "grand_c4_spacetourer",
            "a4 allroad": "a4_allroad",
            "cx-30": "cx30",
            "zr-v": "zrv",
            "f-type": "f_type",
            "serie 700": "serie_700",
            "pt cruiser": "pt_cruiser",
            "cr-z": "crz",
            "morgan plus 4": "morgan_plus_4",
            "aero coupe": "aero_coupe",
            "supra katarga": "supra_katarga",
            "m-klasse": "m_klasse",
            "4er-reihe": "4er_reihe",
            "model x": "model_x",
            "altea xl": "altea_xl",
            "s-cross": "s_cross",
            "e-208": "e_208",
            "mustang mach-e": "mustang_mach_e",
            "a6 allroad": "a6_allroad",
            "mg f": "mg_f",
            "r 11": "r11",
            "fiorino qubo": "fiorino_qubo",
            "volkswagen cc": "volkswagen_cc",
            "e-tron gt": "e_tron_gt",
            "euniq 6": "euniq6",
            "370 z": "370z",
            "hi ace": "hi_ace",
            "1542": "1542",
            "2er-reihe": "2er_reihe",
            "id.3": "id_3",
            "8er-reihe": "8er_reihe",
            "300 zx": "300zx",
            "puch g": "puch_g",
            "mg tf": "mg_tf",
            "c4 x": "c4_x",
            "r 5": "r5",
            "serie 900": "serie_900",
            "town car": "town_car",
            "t-roc": "t_roc",
            "grande punto": "grande_punto",
            "s-klasse": "s_klasse",
            "ds 5": "ds5",
            "käfer": "kaefer",
            "serie 800": "serie_800",
            "350 z": "350z",
            "grand espace": "grand_espace",
            "murciélago": "murcielago",
            "1er-reihe": "1er_reihe",
            "e-tron": "e_tron",
            "cx-5": "cx5",
            "d-truck": "d_truck",
            "hr-v": "hrv",
            "transit custom": "transit_custom",
            "huracán": "huracan",
            "y / ypsilon": "ypsilon",
            "e-up!": "e_up",
            "e-klasse": "e_klasse",
            "ds 7 crossback": "ds7_crossback",
            "s-max": "s_max",
            "ioniq 5": "ioniq5",
            "cx-60": "cx60",
            "300 c": "300c",
            "t-klasse": "t_klasse",
            "morgan aero 8": "morgan_aero_8",
            "c-zero": "c_zero",
            "mx-6": "mx6",
            "a-klasse": "a_klasse",
            "e-niro": "e_niro",
            "cr-v": "crv",
            "id. buzz": "id_buzz",
            "6er-reihe": "6er_reihe",
            "9-3": "9_3",
            "yaris cross": "yaris_cross",
            "fr-v": "frv",
            "serie 200": "serie_200",
            "300 m": "300m",
            "e-truck": "e_truck",
            "grand cherokee": "grand_cherokee",
            "space star": "space_star",
            "i-pace": "i_pace",
            "c4 cactus": "c4_cactus",
            "v-klasse": "v_klasse",
            "doblò": "doblo",
            "cx-7": "cx7",
            "sj 413": "sj413",
            "clc-klasse": "clc_klasse",
            "cx-9": "cx9",
            "cla-klasse": "cla_klasse",
            "c4 spacetourer": "c4_spacetourer",
            "m.go": "mgo",
            "mx-30": "mx30",
            "sl-klasse": "sl_klasse",
            "coupé": "coupe",
            "amg gt": "amg_gt",
            "up!": "up",
            "gls-klasse": "gls_klasse",
            "488 spider": "488_spider",
            "cx-80": "cx80",
            "urban cruiser": "urban_cruiser",
            "morgan roadster": "morgan_roadster",
            "100 nx": "100nx",
            "gt-r": "gtr",
            "morgan 4/4": "morgan_4_4",
            "grand scénic": "grand_scenic",
            "gran turismo": "gran_turismo",
            "glc-klasse": "glc_klasse",
            "c-max": "c_max",
            "range rover velar": "range_rover_velar",
            "model y": "model_y",
            "r-klasse": "r_klasse",
            "s-type": "s_type",
            "gl-klasse": "gl_klasse",
            "grandland x": "grandland_x",
            "glk-klasse": "glk_klasse",
            "e-rifter": "e_rifter",
            "space wagon": "space_wagon",
            "range rover sport": "range_rover_sport",
            "f8 tributo": "f8_tributo",
            "mégane": "megane",
            "7er-reihe": "7er_reihe",
            "c-hr": "c_hr",
            "minauto": "minauto",
            "b-max": "b_max",
            "c3 aircross": "c3_aircross",
            "x-bow": "x_bow",
            "c4 picasso": "c4_picasso",
            "ds 4": "ds4",
            "crx / cr-x": "crx",
            "mx-3": "mx3",
            "lanos": "lanos",
            "gran cabrio": "gran_cabrio",
            "etp 3": "etp_3",
            "marvel r": "marvel_r",
            "qashqai": "qashqai",
            "id.4": "id_4",
            "x-trail": "xtrail",
            "scénic": "scenic",
            "c5 aircross": "c5_aircross",
            "e-pace": "e_pace",
            "e-2008": "e_2008",
            "mx-5": "mx5",
            "4-runner": "4_runner",
            "morgan plus 8": "morgan_plus_8",
            "x-type": "x_type",
            "3200 gt": "3200_gt",
            "f8 spider": "f8_spider",
            "b-klasse": "b_klasse",
            "c-klasse": "c_klasse",
            "500 e": "500e",
            "cls-klasse": "cls_klasse",
            "cx-3": "cx3",
            "land cruiser": "land_cruiser",
            "passat cc": "passat_cc",
            "clk-klasse": "clk_klasse",
            "corolla cross": "corolla_cross",
            "9-5": "9_5",
            "c-crosser": "c_crosser",
        }

        # Define the mapping for car_type transformations
        car_type_mapping = {
            "Cabrio / Roadster": "convertible",
            "Klein-/ Kompaktwagen": "compact_car",
            "Kleinbus": "minibus",
            "Kombi / Family Van": "station_wagon",
            "Limousine": "sedan",
            "Mopedauto": "moped_car",
            "Sportwagen / Coupé": "sports_car",
            "SUV / Geländewagen": "suv"
        }

        try:
            self.logger.info(f"Starting data transfer from {staging_table} to {dwh_table}")

            # Construct the query with incremental loading if last_sync_time is provided
            if last_sync_time and last_updated_field:
                query = f"SELECT * FROM {staging_table} WHERE {last_updated_field} > %s"
                self.cursor.execute(query, (last_sync_time,))
            else:
                query = f"SELECT * FROM {staging_table}"
                self.cursor.execute(query)

            rows = self.cursor.fetchall()

            # Check if any rows are found
            if not rows:
                self.logger.info(f"No new or updated records found in {staging_table} since last sync.")
                return  # Exit the function early if no rows are found

            self.logger.info(f"Found {len(rows)} new or updated records in {staging_table} since last sync.")

            # Get column names
            columns = [col[0] for col in self.cursor.description]

            for row in rows:
                row_dict = dict(zip(columns, row))

                # Apply transformations if defined
                if transformations:
                    for key, transform_func in transformations.items():
                        if key in row_dict:
                            original_value = row_dict[key]
                            row_dict[key] = transform_func(row_dict[key])
                            self.logger.debug(f"Transformed {key}: {original_value} -> {row_dict[key]}")

                ###########################################
                # Transform fields from dl.gebrauchtwagen #
                ###########################################

                if staging_table == "dl.gebrauchtwagen":
                    # Lookup make_id
                    make_id = self.lookup("dwh.make", "make_name", row_dict["make"])
                    if not make_id:
                        self.logger.error(
                            f"Make '{row_dict['make']}' not found in dwh.make. Skipping row {row_dict['id']}.")
                        continue

                    ############################
                    make = row_dict.get("make")
                    transformed_make = make_mapping.get(make, make)

                    if not transformed_make:
                        self.logger.warning(f"Unknown make '{make}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    # Check if the transformed make exists in dwh.make
                    make_id = self.lookup("dwh.make", "make_name", transformed_make)

                    if not make_id:
                        self.logger.error(
                            f"Make '{transformed_make}' not found in dwh.make. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    make_id = self.lookup("dwh.make", "make_name", transformed_make)
                    ############################

                    # Lookup model_id
                    model_id = self.lookup("dwh.model", "model_name", row_dict["model"])
                    if not model_id:
                        self.logger.error(
                            f"Model '{row_dict['model']}' not found in dwh.model. Skipping row {row_dict['id']}.")
                        continue

                    # Lookup engine_fuel_id
                    engine_fuel_id = self.lookup("dwh.fuel", "fuel_type", row_dict["engine_fuel"])
                    if not engine_fuel_id:
                        self.logger.error(
                            f"Fuel type '{row_dict['engine_fuel']}' not found in dwh.engine_fuel. Skipping row {row_dict['id']}.")
                        continue

                    self.logger.warning(f"ID: {row_dict['id']}")

                    # Transform data for dwh.willwagen
                    transformed_willwagen = {
                        "gw_guid": row_dict["id"],
                        "source_id": source_id or 2,  # Default source ID if not provided
                        "make_id": make_id,
                        "model_id": model_id,
                        "mileage": row_dict["mileage"],
                        "power_in_kw": row_dict["engine_effect"],
                        "engine_fuel_id": engine_fuel_id,
                        "year_model": row_dict["year_model"],
                        "price": row_dict["price"],
                    }

                    self.logger.debug(f"Inserting transformed_willwagen: {transformed_willwagen}")
                    self.insert_into_table(dwh_table, transformed_willwagen, return_id=False)

                    # Split location into postcode and city
                    location_parts = row_dict["location"].split(" ", 1)
                    postcode = location_parts[0] if len(location_parts) > 1 else None
                    city = location_parts[1] if len(location_parts) > 1 else None

                    # Transform data for dwh.location
                    transformed_location = {
                        "gebrauchtwagen_guid": row_dict["id"],
                        "postcode": postcode,
                        "location": city,
                    }

                    self.logger.debug(f"Inserting transformed_location: {transformed_location}")
                    self.insert_into_table("dwh.location", transformed_location, return_id=False)

                ######################################
                # Transform fields from dl.willhaben #
                ######################################

                if staging_table == "dl.willhaben":
                    # Transform car_type using the mapping
                    car_type = row_dict.get("car_type")  # Verwende get(), um mögliche KeyError zu vermeiden
                    transformed_car_type = car_type_mapping.get(car_type)

                    if not transformed_car_type:
                        self.logger.warning(
                            f"Unknown car_type '{car_type}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    # Check if the transformed car_type exists in dwh.car_type
                    car_type_id = self.lookup("dwh.car_type", "type", transformed_car_type)

                    if not car_type_id:
                        self.logger.error(
                            f"Car type '{transformed_car_type}' not found in dwh.car_type. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Transform make using the mapping
                    make = row_dict.get("make")  # Verwende get(), um mögliche KeyError zu vermeiden
                    transformed_make = make_mapping.get(make, make)

                    if not transformed_make:
                        self.logger.warning(f"Unknown make '{make}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    # Check if the transformed make exists in dwh.make
                    make_id = self.lookup("dwh.make", "make_name", transformed_make)

                    if not make_id:
                        self.logger.error(
                            f"Make '{transformed_make}' not found in dwh.make. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    make_id = self.lookup("dwh.make", "make_name", transformed_make)

                    # Transform model using the mapping
                    model = row_dict.get("model")  # Verwende get(), um mögliche KeyError zu vermeiden
                    transformed_model = model_mapping.get(model, model)

                    if not transformed_model:
                        self.logger.warning(
                            f"Unknown model '{model}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    # Check if the transformed model exists in dwh.model
                    model_id = self.lookup("dwh.model", "model_name", transformed_model)

                    if not model_id:
                        self.logger.error(
                            f"Model '{transformed_model}' not found in dwh.model. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Check if willhaben_id already exists in dwh.willwagen
                    query_check = f"SELECT 1 FROM {dwh_table} WHERE willhaben_id = %s"
                    self.cursor.execute(query_check, (row_dict["id"],))
                    if self.cursor.fetchone():
                        self.logger.warning(f"Duplicate willhaben_id '{row_dict['id']}' found, skipping row.")
                        continue  # Skip the current row if it already exists

                    # Transform data for dwh.willwagen
                    transformed_willwagen = {
                        "willhaben_id": row_dict["id"],
                        "source_id": source_id or 1,  # Default source ID if not provided
                        "make_id": make_id,
                        "model_id": model_id,
                        "year_model": row_dict["year_model"],
                        "transmission_id": row_dict["transmission"],
                        "mileage": row_dict["mileage"],
                        "noofseats": row_dict["noofseats"],
                        "power_in_kw": row_dict["engine_effect"],
                        "engine_fuel_id": row_dict["engine_fuel"],
                        "car_type_id": car_type_id,
                        "no_of_owners": row_dict["no_of_owners"],
                        "color_id": row_dict["color"],
                        "condition_id": row_dict["condition"],
                        "price": row_dict["price"],
                        "warranty": row_dict["warranty"] == 1,  # True if 1
                        "published": self.convert_unix_to_datetime(row_dict["published"]),
                        "last_updated": self.convert_unix_to_datetime(row_dict["last_updated"]),
                        "isprivate": row_dict["isprivate"] == 1,  # True if 1
                    }

                    self.logger.debug(f"Inserting transformed_willwagen: {transformed_willwagen}")
                    self.insert_into_table(dwh_table, transformed_willwagen, return_id=True)

                    try:
                        # Transform data for dwh.location
                        coordinates = row_dict["coordinates"].split(",") if row_dict["coordinates"] else [None, None]
                        transformed_location = {
                            "willhaben_id": row_dict["id"],  # ID aus dl.willhaben
                            "address": row_dict["address"],
                            "location": row_dict["location"],
                            "postcode": row_dict["postcode"],
                            "district": row_dict["district"],
                            "state": row_dict["state"],
                            "country": row_dict["country"],
                            "longitude": coordinates[0],
                            "latitude": coordinates[1],
                        }

                        # Prüfe die Länge des Werts in der 'postcode'-Spalte
                        max_length_postcode = 50  # Passe die maximale Länge an dein Schema an
                        if transformed_location["postcode"] and len(
                                transformed_location["postcode"]) > max_length_postcode:
                            self.logger.warning(
                                f"Truncated value for 'postcode': {transformed_location['postcode']} in row {row_dict['id']}. Skipping row."
                            )
                            continue  # Überspringe diese Zeile

                        # Insert transformed data into dwh.location
                        self.insert_or_update("dwh.location", transformed_location, keys=["willhaben_id"])

                        # Deactivate foreign key constraint for dwh.specification
                        self.cursor.execute(
                            "ALTER TABLE dwh.specification NOCHECK CONSTRAINT FK_specification_willwagen")

                        # Transform data for dwh.specification
                        transformed_specification = {
                            "willhaben_id": row_dict["id"],
                            "specification": row_dict["specification"],
                        }

                        # Insert transformed data into dwh.specification
                        self.insert_into_table("dwh.specification", transformed_specification)

                        # Reactivate foreign key constraint for dwh.specification
                        self.cursor.execute("ALTER TABLE dwh.specification CHECK CONSTRAINT FK_specification_willwagen")

                        # Deactivate foreign key constraint for dwh.description
                        self.cursor.execute("ALTER TABLE dwh.description NOCHECK CONSTRAINT FK_description_willwagen")

                        # Transform data for dwh.description
                        transformed_description = {
                            "willhaben_id": row_dict["id"],
                            "description": row_dict["description"],
                        }

                        # Insert transformed data into dwh.description
                        self.insert_into_table("dwh.description", transformed_description)

                        # Reactivate foreign key constraint for dwh.description
                        self.cursor.execute("ALTER TABLE dwh.description CHECK CONSTRAINT FK_description_willwagen")

                        # Deactivate foreign key constraint for dwh.image_url
                        self.cursor.execute("ALTER TABLE dwh.image_url NOCHECK CONSTRAINT FK_image_url_willwagen")

                        # Transform data for dwh.image_url
                        transformed_image_url = {
                            "willhaben_id": row_dict["id"],
                            "image_url": row_dict["main_image_url"],
                        }

                        # Insert transformed data into dwh.image_url
                        self.insert_into_table("dwh.image_url", transformed_image_url)

                        # Reactivate foreign key constraint for dwh.image_url
                        self.cursor.execute("ALTER TABLE dwh.image_url CHECK CONSTRAINT FK_image_url_willwagen")

                        # Deactivate foreign key constraint for dwh.seo_url
                        self.cursor.execute("ALTER TABLE dwh.seo_url NOCHECK CONSTRAINT FK_seo_url_willwagen")

                        # Transform data for dwh.seo_url
                        transformed_seo_url = {
                            "willhaben_id": row_dict["id"],
                            "seo_url": row_dict["seo_url"],
                        }

                        # Insert transformed data into dwh.seo_url
                        self.insert_into_table("dwh.seo_url", transformed_seo_url)

                        # Reactivate foreign key constraint for dwh.seo_url
                        self.cursor.execute("ALTER TABLE dwh.seo_url CHECK CONSTRAINT FK_seo_url_willwagen")

                    except Exception as e:
                        self.logger.error(f"Failed to process row {row_dict['id']}: {e}")
                        self.conn.rollback()  # Rollback if there's an error
                        raise
                    finally:
                        self.conn.commit()  # Commit changes

                # Optionally delete data from the staging table
                if delete_from_staging:
                    delete_query = f"DELETE FROM {staging_table}"
                    self.cursor.execute(delete_query)

                # Update the `last_synced` field in the source table
                if last_sync_time and last_updated_field:
                    update_query = f"""
                    UPDATE {staging_table}
                    SET {last_updated_field} = GETDATE()
                    WHERE {last_updated_field} IS NULL OR {last_updated_field} > %s
                    """
                    self.logger.debug(f"Updating last_synced field in {staging_table} with query: {update_query}")
                    self.cursor.execute(update_query, (last_sync_time,))

            # Commit changes
            self.conn.commit()
            self.logger.info(f"Data transfer from {staging_table} to {dwh_table} completed successfully")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to move data to DWH: {e}")
            raise Exception(f"Failed to move data to DWH: {e}")

    def lookup(self, table, column, value):
        """
        Look up the ID for a value in a reference table.
        """
        if not value:
            return None  # Indicate that the value is missing

        self.cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
        result = self.cursor.fetchone()
        if result:
            return result[0]  # Return the ID if found
        else:
            return None  # Return None if not found (without inserting)

    def lookup_or_insert(self, table, column, value):
        """
        Look up the ID for a value in a reference table or insert it if not found.
        """
        if not value:
            return None
        self.cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        self.cursor.execute(f"INSERT INTO {table} ({column}) OUTPUT inserted.id VALUES (%s)", (value,))
        return self.cursor.fetchone()[0]

    def insert_into_table(self, table_name, data, return_id=False):
        """
        Inserts data into the specified table.

        Args:
            table_name (str): Name of the table to insert data into.
            data (dict): Dictionary of column-value pairs to insert.
            return_id (bool): Whether to return the generated ID for the inserted row.

        Returns:
            int: The generated ID if return_id is True; otherwise, None.

        Raises:
            Exception: If the insertion fails.
        """
        try:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            if return_id:
                query += "; SELECT SCOPE_IDENTITY()"

            self.cursor.execute(query, tuple(data.values()))
            if return_id:
                generated_id = self.cursor.fetchone()[0]
                return generated_id

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to insert into {table_name}: {e}")
            raise Exception(f"Failed to insert into {table_name}: {e}")

    def insert_or_update(self, table_name, data, keys):
        """
        Inserts data into the specified table or updates it if a conflict occurs.

        Args:
            table_name (str): The name of the table to insert or update data.
            data (dict): A dictionary where keys are column names and values are the corresponding values.
            keys (list): A list of column names to check for conflicts (e.g., primary or unique keys).

        Raises:
            Exception: If the operation fails.
        """
        try:
            columns = list(data.keys())
            values = list(data.values())
            key_conditions = " AND ".join([f"target.{key} = source.{key}" for key in keys])

            merge_query = f"""
            MERGE INTO {table_name} AS target
            USING (VALUES ({', '.join(['%s'] * len(columns))})) AS source ({', '.join(columns)})
            ON {key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in columns if col not in keys])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(columns)}) 
                VALUES ({', '.join([f"source.{col}" for col in columns])});
            """
            self.cursor.execute(merge_query, values)
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to insert or update in {table_name}: {e}")
            raise Exception(f"Failed to insert or update in {table_name}: {e}")

    @staticmethod
    def convert_unix_to_datetime(unix_time):
        """
        Convert UNIX time (in seconds or milliseconds) to a timezone-aware UTC datetime object.

        Args:
            unix_time (int): UNIX timestamp in seconds or milliseconds.

        Returns:
            datetime: Timezone-aware UTC datetime object or None if unix_time is None or invalid.
        """
        if not unix_time:
            return None

        try:
            # Convert UNIX time in milliseconds to seconds if needed
            if unix_time > 10 ** 10:  # Likely in milliseconds
                unix_time /= 1000

            return datetime.fromtimestamp(unix_time, tz=timezone.utc)
        except Exception as e:
            raise ValueError(f"Failed to convert UNIX time {unix_time} to datetime: {e}")
