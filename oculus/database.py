import os
import pymssql
import logging
from oculus.logging import database_logger
from dotenv import load_dotenv
from rich import print
import json

load_dotenv()


class DatabaseError(Exception):
    pass


class Database:
    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_DATABASE")
        self.logger = logging.getLogger("Database")
        self.conn = None
        self.cursor = None

        self.logger.propagate = False

    def connect(self):
        if self.conn:
            database_logger.info("✅[yellow] Database connection already established.[/yellow]")
            return
        try:
            self.conn = pymssql.connect(
                server=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8",
            )
            self.cursor = self.conn.cursor()
            print("✅[green] Database connection established.[/green]")
        except Exception as e:
            database_logger.error(f"❌ Database connection failed: {e}")
            raise

    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None
        self.cursor = None
        print("✅[green] Database connection closed.[/green]")

    def ensure_connection(self):
        if not self.conn:
            print("[yellow]Reconnecting to the database...[/yellow]")
            self.connect()

    def load_car_data(self, file_path):
        """
        Lädt Autodaten aus einer JSON-Datei, leert die Tabellen `makes` und `models` und fügt neue Daten ein.

        Args:
            file_path (str): Der Pfad zur JSON-Datei.

        Raises:
            DatabaseError: Wenn ein Fehler beim Einfügen auftritt.
        """
        # Verbindung sicherstellen
        self.ensure_connection()

        try:
            # Tabellen leeren
            self.logger.info("Tabellen 'makes' und 'models' werden geleert...")
            try:
                # Constraints deaktivieren
                self.cursor.execute("ALTER TABLE models NOCHECK CONSTRAINT ALL")
                self.cursor.execute("ALTER TABLE makes NOCHECK CONSTRAINT ALL")

                # Tabellen leeren
                self.cursor.execute("DELETE FROM models")
                self.cursor.execute("DELETE FROM makes")
                self.conn.commit()

                # Constraints aktivieren
                self.cursor.execute("ALTER TABLE models CHECK CONSTRAINT ALL")
                self.cursor.execute("ALTER TABLE makes CHECK CONSTRAINT ALL")

                self.logger.info("Tabellen 'makes' und 'models' wurden erfolgreich geleert.")
            except Exception as e:
                self.conn.rollback()
                self.logger.error(f"Fehler beim Leeren der Tabellen: {e}")
                raise DatabaseError(f"Failed to clear tables: {e}")

            # JSON-Datei laden
            with open(file_path, 'r', encoding='utf-8') as file:
                car_data = json.load(file)

            # Daten einfügen
            self.logger.info("Daten aus der JSON-Datei werden geladen...")
            for make_name, make_data in car_data.items():
                make_id = make_data.get("id")
                models = make_data.get("models", {})

                # Marke einfügen
                insert_make_query = "INSERT INTO makes (make_id, make_name) VALUES (%s, %s)"
                self.cursor.execute(insert_make_query, (make_id, make_name))

                # Modelle einfügen
                for model_name, model_id in models.items():
                    insert_model_query = "INSERT INTO models (model_id, model_name, make_id) VALUES (%s, %s, %s)"
                    self.cursor.execute(insert_model_query, (model_id, model_name, make_id))

            # Änderungen speichern
            self.conn.commit()
            self.logger.info(f"Daten aus {file_path} erfolgreich geladen.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Fehler beim Laden der Autodaten: {e}")
            raise DatabaseError(f"Failed to load car data: {e}")

    def move_data_to_dwh(self, staging_table, dwh_table, transformations=None, delete_from_staging=False):
        """
        Moves data from the staging table to the data warehouse table, applying transformations if needed.

        Args:
            staging_table (str): The name of the staging table.
            dwh_table (str): The name of the DWH table.
            transformations (dict): Optional. A dictionary specifying column transformations.
                                    Key: column name in staging.
                                    Value: a function to transform the column.
            delete_from_staging (bool): Whether to delete rows from the staging table after insertion. Default is False.
        """
        self.ensure_connection()
        try:
            # Fetch data from the staging table
            select_query = f"SELECT * FROM {staging_table}"
            self.cursor.execute(select_query)
            rows = self.cursor.fetchall()

            if not rows:
                self.logger.info(f"No data found in staging table '{staging_table}'.")
                return

            # Apply transformations if specified
            transformed_rows = []
            for row in rows:
                transformed_row = list(row)
                if transformations:
                    for column, transform_func in transformations.items():
                        col_index = self.cursor.description.index((column,))
                        transformed_row[col_index] = transform_func(row[col_index])
                transformed_rows.append(transformed_row)

            # Insert data into the DWH table
            columns = [desc[0] for desc in self.cursor.description]
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {dwh_table} ({', '.join(columns)}) VALUES ({placeholders})"

            for row in transformed_rows:
                self.cursor.execute(insert_query, row)

            self.conn.commit()
            self.logger.info(
                f"Successfully moved {len(rows)} rows from '{staging_table}' to '{dwh_table}'."
            )

            # Optionally delete rows from the staging table
            if delete_from_staging:
                delete_query = f"DELETE FROM {staging_table}"
                self.cursor.execute(delete_query)
                self.conn.commit()
                self.logger.info(f"Staging table '{staging_table}' cleared after moving data.")
            else:
                self.logger.info(f"Data retained in staging table '{staging_table}' after moving to DWH.")

        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error moving data from '{staging_table}' to '{dwh_table}': {e}")
            raise DatabaseError(f"Failed to move data to DWH: {e}")
