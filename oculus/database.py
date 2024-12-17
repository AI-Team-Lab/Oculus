import os
import pymssql
import logging
import pandas as pd
from oculus.price_prediction import CarPricePredictionModelD
from oculus.logging import database_logger
from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path
import json

load_dotenv()


class DatabaseError(Exception):
    pass


class Database:
    def __init__(self):
        """
        Initializes the Database object, loads environment variables, sets up logging,
        and attempts to load JSON mapping files for willhaben and gebrauchtwagen.
        """
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

        # Load mappings
        try:
            json_path_willhaben = os.path.join(os.path.dirname(__file__), "mapping", "willhaben_mapping.json")
            with open(json_path_willhaben, mode="r", encoding="utf-8") as f:
                self.willhaben_mappings = json.load(f)
            self.logger.info(f"Successfully loaded willhaben_mapping.json from {json_path_willhaben}")
        except Exception as e:
            self.logger.error(f"Error loading willhaben_mapping.json: {e}")
            self.willhaben_mappings = {}

        try:
            json_path_gebrauchtwagen = os.path.join(os.path.dirname(__file__), "mapping", "gebrauchtwagen_mapping.json")
            with open(json_path_gebrauchtwagen, mode="r", encoding="utf-8") as f:
                self.gebrauchtwagen_mappings = json.load(f)
            self.logger.info(f"Successfully loaded gebrauchtwagen_mapping.json from {json_path_gebrauchtwagen}")
        except Exception as e:
            self.logger.error(f"Error loading gebrauchtwagen_mapping.json: {e}")
            self.gebrauchtwagen_mappings = {}

        try:
            json_path_gebrauchtwagen = os.path.join(os.path.dirname(__file__), "mapping", "gebrauchtwagen_mapping.json")
            with open(json_path_gebrauchtwagen, mode="r", encoding="utf-8") as f:
                self.gebrauchtwagen_mappings = json.load(f)
            self.logger.info(f"Successfully loaded gebrauchtwagen_mapping.json from {json_path_gebrauchtwagen}")
        except Exception as e:
            self.logger.error(f"Error loading gebrauchtwagen_mapping.json: {e}")
            self.gebrauchtwagen_mappings = {}

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

    def insert_data_gebrauchtwagen(self, df):
        """
        Fügt Daten aus einem DataFrame in die Tabelle 'dl.Gebrauchtwagen' ein.

        Args:
            df (pd.DataFrame): DataFrame mit den einzufügenden Daten.

        Raises:
            DatabaseError: Wenn das Einfügen der Daten fehlschlägt.
        """
        insert_query = """
        INSERT INTO dl.Gebrauchtwagen (id, make, model, mileage, engine_effect, engine_fuel, year_model, location, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        successful_inserts = 0

        try:
            for _, row in df.iterrows():
                try:
                    self.cursor.execute(insert_query, (
                        row['id'],
                        row['make'],
                        row['model'],
                        row['mileage'],
                        row['engine_effect'],
                        row['engine_fuel'],
                        row['year_model'],
                        row['location'],
                        row['price']
                    ))
                    successful_inserts += 1
                except pymssql.IntegrityError as e:
                    self.logger.warning(f"IntegrityError inserting row ID {row['id']}: {e}. Skipping row.")
                    self.conn.rollback()
                    continue
                except Exception as e:
                    self.logger.error(f"Error inserting row ID {row['id']}: {e}. Skipping row.")
                    self.conn.rollback()
                    continue

            self.conn.commit()
            self.logger.info(f"Successfully inserted {successful_inserts} rows into 'dl.Gebrauchtwagen'.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to insert data into 'dl.Gebrauchtwagen': {e}")
            raise DatabaseError(f"Failed to insert data into 'dl.Gebrauchtwagen': {e}")

    def execute_query(self, query, parameters=None):
        """
        Executes a SQL query with optional parameters.

        Args:
            query (str): The SQL query to execute.
            parameters (tuple, optional): The parameters to pass with the query.

        Raises:
            DatabaseError: If the query execution fails.
        """
        self.ensure_connection()
        try:
            self.cursor.execute(query, parameters) if parameters else self.cursor.execute(query)
        except pymssql.Error as e:
            self.logger.error(f"SQL query error: {e}")
            raise DatabaseError(f"SQL query error: {e}")

    def create_table_gebrauchtwagen(self):
        """
        Erstellt die Tabelle 'dl.Gebrauchtwagen' in der Datenbank.

        Raises:
            DatabaseError: Wenn das Erstellen der Tabelle fehlschlägt.
        """
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'gebrauchtwagen' AND schema_id = SCHEMA_ID('dl'))
        BEGIN
            CREATE TABLE dl.gebrauchtwagen (
                id UNIQUEIDENTIFIER PRIMARY KEY,
                make NVARCHAR(50) NOT NULL,
                model NVARCHAR(50) NOT NULL,
                mileage FLOAT NOT NULL,
                engine_effect INT NOT NULL,
                engine_fuel NVARCHAR(20) NOT NULL,
                year_model INT NOT NULL,
                location NVARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL
            );
        END
        """
        try:
            self.execute_query(create_table_query)
            self.logger.info("Table 'dl.Gebrauchtwagen' created successfully.")
        except Exception as e:
            self.logger.error(f"Failed to create table 'dl.Gebrauchtwagen': {e}")
            raise DatabaseError(f"Failed to create table 'dl.Gebrauchtwagen': {e}")

    def read_csv(self, csv_file_path):
        """
        Lädt Daten aus einer CSV-Datei in ein pandas DataFrame.

        Args:
            csv_file_path (str): Pfad zur CSV-Datei.

        Returns:
            pd.DataFrame: Daten aus der CSV-Datei.

        Raises:
            FileNotFoundError: Wenn die CSV-Datei nicht gefunden wird.
            pd.errors.ParserError: Wenn ein Fehler beim Parsen der CSV-Datei auftritt.
        """
        try:
            df = pd.read_csv(csv_file_path)
            self.logger.info(f"CSV file loaded successfully: {csv_file_path}")
            self.logger.debug(f"DataFrame head:\n{df.head()}")
            self.logger.debug(f"Missing values before fillna:\n{df.isna().sum()}")
            df = df.fillna('')
            self.logger.debug(f"Missing values after fillna:\n{df.isna().sum()}")
            return df
        except FileNotFoundError as e:
            self.logger.error(f"CSV file not found: {e}")
            raise
        except pd.errors.ParserError as e:
            self.logger.error(f"Error parsing the CSV file: {e}")
            raise

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
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for engine data...")
            try:
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

            self.logger.info("Loading engine data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                engine_data = json.load(file)

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
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for equipment data...")
            try:
                tables = ["dl.equipment_search", "dl.exterior_colour_main", "dl.no_of_doors", "dl.no_of_seats"]
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

            self.logger.info("Loading equipment data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                equipment_data = json.load(file)

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
                        if door_id not in inserted_doors:
                            insert_query = "INSERT INTO dl.no_of_doors (id, door_count) VALUES (%s, %s)"
                            self.cursor.execute(insert_query, (door_id, door_count))
                            inserted_doors.add(door_id)
                elif key in ["no_of_seats_from", "no_of_seats_to"]:
                    for seat_count, seat_id in values.items():
                        if seat_id not in inserted_seats:
                            insert_query = "INSERT INTO dl.no_of_seats (id, seat_count) VALUES (%s, %s)"
                            self.cursor.execute(insert_query, (seat_id, seat_count))
                            inserted_seats.add(seat_id)

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
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for location data...")
            try:
                tables = ["dl.area", "dl.location", "dl.dealer", "dl.periode"]
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

            self.logger.info("Loading location data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                location_data = json.load(file)

            self.logger.info("Inserting location data into the database...")

            # Insert Locations and Areas
            for location_name, location_details in location_data.get("locations", {}).items():
                location_id = location_details.get("id")
                if location_id is None:
                    self.logger.warning(f"Skipping location '{location_name}' due to missing ID.")
                    continue

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
        self.ensure_connection()

        try:
            # Clear the tables
            self.logger.info("Clearing tables for car status data...")
            try:
                tables = ["dl.car_type", "dl.motor_condition", "dl.warranty"]
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

            self.logger.info("Loading car status data from JSON file...")
            with open(file_path, 'r', encoding='utf-8') as file:
                car_data = json.load(file)

            self.logger.info("Inserting car status data into the database...")

            # Insert car types
            for car_type_name, car_type_id in car_data.get("car_type", {}).items():
                insert_car_type_query = "INSERT INTO dl.car_type (id, type) VALUES (%s, %s)"
                self.cursor.execute(insert_car_type_query, (car_type_id, car_type_name))

            # Insert motor conditions
            for condition_name, condition_id in car_data.get("motor_condition", {}).items():
                insert_motor_condition_query = "INSERT INTO dl.motor_condition (id, condition) VALUES (%s, %s)"
                self.cursor.execute(insert_motor_condition_query, (condition_id, condition_name))

            # Insert warranty
            for warranty_name, warranty_id in car_data.get("warranty", {}).items():
                insert_warranty_query = "INSERT INTO dl.warranty (id, warranty_available) VALUES (%s, %s)"
                self.cursor.execute(insert_warranty_query, (warranty_id, warranty_name))

            self.conn.commit()
            self.logger.info(f"Car status data successfully loaded from {file_path} into the database.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to load car status data from {file_path}: {e}")
            raise DatabaseError(f"Failed to load car status data: {e}")

    def move_reference_data(self, source_table, target_table, source_columns, target_columns, last_sync_time,
                            last_updated_field):
        """
        Moves reference data from source_table to target_table using an incremental load approach via MERGE.

        Args:
            source_table (str): The name of the source (staging) table.
            target_table (str): The name of the target (DWH) table.
            source_columns (list): Columns to select from the source table.
            target_columns (list): Corresponding columns in the target table.
            last_sync_time (datetime): Timestamp for incremental loading. If None, load all data.
            last_updated_field (str): Field in the source table used to track updates.
        """
        self.ensure_connection()

        try:
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

            for row in rows:
                source_data = dict(zip(source_columns, row))

                # Use MERGE to upsert data
                merge_query = f"""
                    MERGE {target_table} AS target
                    USING (SELECT {', '.join(['%s AS ' + col for col in target_columns])}) AS source
                    ON (target.{target_columns[0]} = source.{target_columns[0]})
                    WHEN MATCHED THEN 
                        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in target_columns[1:]])}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(target_columns)}) 
                        VALUES ({', '.join(['source.' + col for col in target_columns])});
                """
                self.cursor.execute(merge_query, tuple(row))

            # Update the `last_synced` field in the source table if needed
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
        Retrieves the last synchronization time for a given table from dwh.sync_log.

        Args:
            table_name (str): The name of the table to check in the sync_log.

        Returns:
            datetime or None: The last synchronization timestamp, or None if no log record exists.
        """
        self.ensure_connection()
        query = "SELECT last_sync_time FROM dwh.sync_log WHERE table_name = %s"
        self.cursor.execute(query, (table_name,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def update_sync_time(self, table_name, sync_time):
        """
        Updates the sync time for a specific table in the dwh.sync_log table using a MERGE statement.

        Args:
            table_name (str): The name of the table whose sync time should be updated.
            sync_time (datetime): The new sync timestamp.
        """
        self.ensure_connection()

        try:
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
        Updates the 'last_synced' column in the source table.

        Args:
            table_name (str): Name of the source table.
            sync_time (datetime): The last sync timestamp.
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

        try:
            self.logger.info(f"Starting data transfer from {staging_table} to {dwh_table}")

            # Construct the query for incremental loading if last_sync_time is provided
            if last_sync_time and last_updated_field:
                query = f"SELECT * FROM {staging_table} WHERE {last_updated_field} > %s"
                self.cursor.execute(query, (last_sync_time,))
            else:
                query = f"SELECT * FROM {staging_table}"
                self.cursor.execute(query)

            rows = self.cursor.fetchall()

            # If no rows, log and exit early
            if not rows:
                self.logger.info(f"No new or updated records found in {staging_table} since last sync.")
                return

            self.logger.info(f"Found {len(rows)} new or updated records in {staging_table} since last sync.")

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
                    gebrauchtwagen_make_mapping = self.gebrauchtwagen_mappings["gebrauchtwagen_make_mapping"]
                    gebrauchtwagen_model_mapping = self.gebrauchtwagen_mappings["gebrauchtwagen_model_mapping"]
                    gebrauchtwagen_engine_fuel_mapping = self.gebrauchtwagen_mappings[
                        "gebrauchtwagen_engine_fuel_mapping"]

                    # Transform make
                    make = row_dict.get("make")
                    transformed_make = gebrauchtwagen_make_mapping.get(make, make)
                    if not transformed_make:
                        self.logger.warning(f"Unknown make '{make}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    make_id = self.lookup("dwh.make", "make_name", transformed_make)
                    if not make_id:
                        self.logger.error(
                            f"Make '{transformed_make}' not found in dwh.make. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Transform model
                    model = row_dict.get("model")
                    # Check if model is not None or empty
                    if not model or model.strip() == "":
                        self.logger.warning(f"Empty model in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    transformed_model = gebrauchtwagen_model_mapping.get(model, model)
                    if not transformed_model:
                        self.logger.warning(
                            f"Unknown model '{model}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    model_id = self.lookup("dwh.model", "model_name", transformed_model)
                    if not model_id:
                        continue  # skipping instead of logging error

                    # Transform engine_fuel
                    engine_fuel = row_dict.get("engine_fuel")
                    if not engine_fuel or engine_fuel.strip() == "":
                        self.logger.warning(f"Empty engine_fuel in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    transformed_engine_fuel = gebrauchtwagen_engine_fuel_mapping.get(engine_fuel, engine_fuel)
                    if not transformed_engine_fuel:
                        self.logger.warning(
                            f"Unknown engine_fuel '{engine_fuel}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    engine_fuel_id = self.lookup("dwh.fuel", "fuel_type", transformed_engine_fuel)
                    if not engine_fuel_id:
                        self.logger.error(
                            f"Engine fuel '{transformed_engine_fuel}' not found in dwh.fuel. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Check for duplicate gw_guid in dwh.willwagen
                    query_check = f"SELECT 1 FROM {dwh_table} WHERE gw_guid = %s"
                    self.cursor.execute(query_check, (row_dict["id"],))
                    if self.cursor.fetchone():
                        self.logger.warning(f"Duplicate gw_guid '{row_dict['id']}' found, skipping row.")
                        continue

                    # Create the dictionary to insert into dwh.willwagen
                    transformed_willwagen = {
                        "gw_guid": row_dict["id"],
                        "source_id": source_id or 2,
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

                    # Handle location
                    location_parts = row_dict["location"].split(" ", 1)
                    postcode = location_parts[0] if len(location_parts) > 1 else None
                    city = location_parts[1] if len(location_parts) > 1 else None

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
                    willhaben_make_mapping = self.willhaben_mappings["willhaben_make_mapping"]
                    willhaben_model_mapping = self.willhaben_mappings["willhaben_model_mapping"]
                    willhaben_car_type_mapping = self.willhaben_mappings["willhaben_car_type_mapping"]

                    # Transform car_type
                    car_type = row_dict.get("car_type")
                    transformed_car_type = willhaben_car_type_mapping.get(car_type)
                    if not transformed_car_type:
                        self.logger.warning(
                            f"Unknown car_type '{car_type}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    car_type_id = self.lookup("dwh.car_type", "type", transformed_car_type)
                    if not car_type_id:
                        self.logger.error(
                            f"Car type '{transformed_car_type}' not found in dwh.car_type. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Transform make
                    make = row_dict.get("make")
                    transformed_make = willhaben_make_mapping.get(make, make)
                    if not transformed_make:
                        self.logger.warning(f"Unknown make '{make}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    make_id = self.lookup("dwh.make", "make_name", transformed_make)
                    if not make_id:
                        self.logger.error(
                            f"Make '{transformed_make}' not found in dwh.make. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Transform model
                    model = row_dict.get("model")
                    transformed_model = willhaben_model_mapping.get(model, model)
                    if not transformed_model:
                        self.logger.warning(
                            f"Unknown model '{model}' in row {row_dict.get('id', 'unknown')}, skipping.")
                        continue

                    model_id = self.lookup("dwh.model", "model_name", transformed_model)
                    if not model_id:
                        self.logger.error(
                            f"Model '{transformed_model}' not found in dwh.model. Skipping row {row_dict.get('id', 'unknown')}.")
                        continue

                    # Check for duplicate willhaben_id in dwh.willwagen
                    query_check = f"SELECT 1 FROM {dwh_table} WHERE willhaben_id = %s"
                    self.cursor.execute(query_check, (row_dict["id"],))
                    if self.cursor.fetchone():
                        self.logger.warning(f"Duplicate willhaben_id '{row_dict['id']}' found, skipping row.")
                        continue

                    transformed_willwagen = {
                        "willhaben_id": row_dict["id"],
                        "source_id": source_id or 1,
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
                        "warranty": row_dict["warranty"] == 1,
                        "published": self.convert_unix_to_datetime(row_dict["published"]),
                        "last_updated": self.convert_unix_to_datetime(row_dict["last_updated"]),
                        "isprivate": row_dict["isprivate"] == 1,
                    }

                    self.logger.debug(f"Inserting transformed_willwagen: {transformed_willwagen}")
                    inserted_id = self.insert_into_table(dwh_table, transformed_willwagen, return_id=True)

                    try:
                        # Transform location
                        coordinates = row_dict["coordinates"].split(",") if row_dict["coordinates"] else [None, None]
                        transformed_location = {
                            "willhaben_id": row_dict["id"],
                            "address": row_dict["address"],
                            "location": row_dict["location"],
                            "postcode": row_dict["postcode"],
                            "district": row_dict["district"],
                            "state": row_dict["state"],
                            "country": row_dict["country"],
                            "longitude": coordinates[0],
                            "latitude": coordinates[1],
                        }

                        # Check length of the 'postcode' value
                        max_length_postcode = 50
                        if transformed_location["postcode"] and len(
                                transformed_location["postcode"]) > max_length_postcode:
                            self.logger.warning(
                                f"Truncated value for 'postcode': {transformed_location['postcode']} in row {row_dict['id']}. Skipping row."
                            )
                            continue

                        self.insert_or_update("dwh.location", transformed_location, keys=["willhaben_id"])

                        transformed_specification = {
                            "willhaben_id": row_dict["id"],
                            "specification": row_dict["specification"],
                        }

                        self.insert_into_table("dwh.specification", transformed_specification)

                        transformed_description = {
                            "willhaben_id": row_dict["id"],
                            "description": row_dict["description"],
                        }
                        self.insert_into_table("dwh.description", transformed_description)

                        transformed_image_url = {
                            "willhaben_id": row_dict["id"],
                            "image_url": row_dict["main_image_url"],
                        }
                        self.insert_into_table("dwh.image_url", transformed_image_url)

                        transformed_seo_url = {
                            "willhaben_id": row_dict["id"],
                            "seo_url": row_dict["seo_url"],
                        }
                        self.insert_into_table("dwh.seo_url", transformed_seo_url)

                    except Exception as e:
                        self.logger.error(f"Failed to process row {row_dict['id']}: {e}")
                        self.conn.rollback()
                        raise
                    finally:
                        self.conn.commit()

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

            self.conn.commit()
            self.logger.info(f"Data transfer from {staging_table} to {dwh_table} completed successfully")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Failed to move data to DWH: {e}")
            raise Exception(f"Failed to move data to DWH: {e}")

    def lookup(self, table, column, value):
        """
        Looks up the ID for a given value in a reference table. Returns None if not found.

        Args:
            table (str): The table name to look up.
            column (str): The column name in the reference table.
            value (str): The value to find in that column.

        Returns:
            int or None: The corresponding ID or None if not found.
        """
        if not value:
            return None
        self.cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def lookup_or_insert(self, table, column, value):
        """
        Looks up the ID for a given value in a reference table or inserts a new record if not found.

        Args:
            table (str): The table name to look up or insert.
            column (str): The column name in the reference table.
            value (str): The value to find or insert.

        Returns:
            int: The existing or newly inserted ID.
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
        Inserts a record into the specified table.

        Args:
            table_name (str): Name of the table to insert data into.
            data (dict): A dictionary of column-value pairs to insert.
            return_id (bool): Whether to return the generated ID for the inserted row.

        Returns:
            int or None: The generated ID if return_id=True, otherwise None.

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
        Inserts or updates data in the specified table using a MERGE statement.

        Args:
            table_name (str): The name of the table to insert or update.
            data (dict): A dictionary of column-value pairs to insert or update.
            keys (list): A list of column names that serve as primary or unique keys.

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
        Converts UNIX time (in seconds or milliseconds) to a timezone-aware UTC datetime object.

        Args:
            unix_time (int): UNIX timestamp in seconds or milliseconds.

        Returns:
            datetime or None: Timezone-aware UTC datetime, or None if invalid or not provided.
        """
        if not unix_time:
            return None

        try:
            if unix_time > 10 ** 10:  # Likely in milliseconds
                unix_time /= 1000

            return datetime.fromtimestamp(unix_time, tz=timezone.utc)
        except Exception as e:
            raise ValueError(f"Failed to convert UNIX time {unix_time} to datetime: {e}")

    @staticmethod
    def update_predicted_prices(db_connection):
        try:
            # Log den Start der Aktualisierung
            database_logger.info("Starting update_predicted_prices.")

            # Bestimme das Basisverzeichnis (vermutlich der Projekt-Root oder oculus/)
            base_dir = Path(__file__).resolve().parent.parent  # Passe dies an dein Projektlayout an

            # Setze den korrekten model_dir
            model_dir = base_dir / 'oculus' / 'model_d'  # Stelle sicher, dass dieser Pfad korrekt ist

            # Log den Pfad zum Modell
            database_logger.debug(f"Loading model from: {model_dir / 'trained_model.keras'}")

            # Initialisiere die Modellklasse mit dem korrekten model_dir
            car_model = CarPricePredictionModelD(model_dir=model_dir)
            car_model.load_model_and_scaler()

            # Abrufen der Daten aus der Datenbank
            query = """
                       SELECT ww.willhaben_id,
                              m.make_name as make,
                              m2.model_name as model,
                              ww.mileage,
                              ww.power_in_kw as engine_effect,
                              f.fuel_type as engine_fuel,
                              ww.year_model
                       FROM dwh.willwagen ww
                                JOIN dwh.make m ON m.id = ww.make_id
                                JOIN dwh.model m2 ON m2.id = ww.model_id
                                JOIN dwh.fuel f ON f.id = ww.engine_fuel_id
                       WHERE ww.source_id = 1
                       AND ww.year_model IS NOT NULL
               """
            database_logger.debug("Executing query to retrieve car data.")
            db_connection.execute_query(query)
            cars = db_connection.cursor.fetchall()
            database_logger.info(f"Retrieved {len(cars)} cars from 'willwagen' table.")

            # Vorhersage und Aktualisierung der Preise
            for car in cars:
                try:
                    willhaben_id, make, model, mileage, engine_effect, engine_fuel, year_model = car
                    database_logger.debug(
                        f"Predicting price for willhaben_id {willhaben_id}: Make={make}, Model={model}, Mileage={mileage}, "
                        f"Engine_Effect={engine_effect}, Engine_Fuel={engine_fuel}, Year_Model={year_model}")

                    predicted_price = car_model.predict(make, model, mileage, engine_effect, engine_fuel, year_model)

                    # Runde den vorhergesagten Preis auf das nächste 10er-Intervall
                    predicted_price = round(predicted_price / 10) * 10
                    database_logger.debug(f"Predicted price for willhaben_id {willhaben_id}: {predicted_price} EUR")

                    # Aktualisiere den vorhergesagten Händlerpreis in der 'willwagen' Tabelle
                    update_query = """
                           UPDATE dwh.willwagen
                           SET predicted_dealer_price = %s
                           WHERE willhaben_id = %s
                       """
                    db_connection.execute_query(update_query, (predicted_price, willhaben_id))
                    database_logger.debug(f"Updated predicted_dealer_price for willhaben_id {willhaben_id}.")

                except Exception as e:
                    # Log Fehler bei der Vorhersage oder Aktualisierung einzelner Autos
                    database_logger.error(
                        f"Failed to predict/update price for willhaben_id {willhaben_id} (Make: {make}, Model: {model}): {e}",
                        exc_info=False)
                    continue  # Fortfahren trotz Fehler

            # Commit der Transaktion
            db_connection.conn.commit()
            database_logger.info("Predicted prices updated successfully.")

        except Exception as e:
            # Log Fehler während der gesamten Methode
            database_logger.error(f"Failed to update predicted prices: {e}", exc_info=False)
            db_connection.conn.rollback()
            raise pymssql.Error(f"Failed to update predicted prices: {e}")

        except Exception as e:
            # Log Fehler während der gesamten Methode
            database_logger.error(f"Failed to update predicted prices: {e}", exc_info=False)
            db_connection.rollback()
            raise pymssql.Error(f"Failed to update predicted prices: {e}")
