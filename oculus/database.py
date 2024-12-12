import os
import pymssql
import logging
from oculus.logging import database_logger
from dotenv import load_dotenv
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

    def move_data_to_dwh(self, staging_table, dwh_table, transformations=None, delete_from_staging=False):
        """
        Moves data from the staging table to the data warehouse table, applying transformations if needed.

        Args:
            staging_table (str): The name of the staging table.
            dwh_table (str): The name of the DWH table.
            transformations (dict): Optional. A dictionary specifying column transformations.
                                    Key: column name in staging.
                                    Value: a function to transform the column value.
            delete_from_staging (bool): Whether to delete rows from the staging table after insertion. Default is False.
        """
        self.ensure_connection()
        try:
            # Fetch data from the staging table
            select_query = f"SELECT * FROM {staging_table}"
            self.cursor.execute(select_query)
            rows = self.cursor.fetchall()

            if not rows:
                self.logger.info(f"No data found in the staging table '{staging_table}'. Nothing to move.")
                return

            # Apply transformations if specified
            transformed_rows = []
            try:
                for row in rows:
                    transformed_row = list(row)
                    if transformations:
                        for column, transform_func in transformations.items():
                            # Find the column index
                            col_index = [desc[0] for desc in self.cursor.description].index(column)
                            # Apply transformation
                            transformed_row[col_index] = transform_func(row[col_index])
                    transformed_rows.append(transformed_row)
            except Exception as e:
                self.logger.error(f"Error during transformations: {e}")
                raise DatabaseError(f"Failed to apply transformations: {e}")

            # Insert data into the DWH table
            columns = [desc[0] for desc in self.cursor.description]
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {dwh_table} ({', '.join(columns)}) VALUES ({placeholders})"

            try:
                for row in transformed_rows:
                    self.cursor.execute(insert_query, row)

                self.conn.commit()
                self.logger.info(
                    f"Successfully moved {len(rows)} rows from '{staging_table}' to '{dwh_table}'."
                )
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Failed to insert data into DWH table '{dwh_table}': {e}")
                raise DatabaseError(f"Failed to insert data into DWH: {e}")

            # Optionally delete rows from the staging table
            if delete_from_staging:
                try:
                    delete_query = f"DELETE FROM {staging_table}"
                    self.cursor.execute(delete_query)
                    self.conn.commit()
                    self.logger.info(f"Staging table '{staging_table}' cleared after moving data.")
                except Exception as e:
                    self.conn.rollback()
                    self.logger.error(f"Failed to clear staging table '{staging_table}': {e}")
                    raise DatabaseError(f"Failed to clear staging table: {e}")
            else:
                self.logger.info(f"ℹData retained in staging table '{staging_table}' after moving to DWH.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error moving data from '{staging_table}' to '{dwh_table}': {e}")
            raise DatabaseError(f"Failed to move data to DWH: {e}")
