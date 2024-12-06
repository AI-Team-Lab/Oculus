import os
import pymssql
from dotenv import load_dotenv
from rich import print

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
        self.conn = None
        self.cursor = None

    def connect(self):
        if self.conn:
            print("✅[yellow] Database connection already established.[/yellow]")
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
            raise DatabaseError(f"Database connection failed: {e}")

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
        """
        Ensures that the database connection is active.
        Reconnects if necessary.
        """
        if not self.conn:
            print("[yellow]Reconnecting to the database...[/yellow]")
            self.connect()

    def execute_query(self, query, params=None):
        """
        Executes a query and fetches all results.
        """
        self.ensure_connection()
        try:
            self.cursor.execute(query, params or [])
            return self.cursor.fetchall()
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")

    def insert_data(self, table_name, data):
        """
        Inserts data into a specified table.

        Args:
            table_name (str): The name of the table to insert into.
            data (list): A list of dictionaries, each representing a row of data.
        """
        self.ensure_connection()
        try:
            for row in data:
                try:
                    # Replace 'N/A', empty strings, and None with appropriate default values
                    for key, value in row.items():
                        if value in ("N/A", "", None):
                            row[key] = [] if key in ("equipment", "equipment_resolved", "all_image_urls") else None

                    # Extract non-equipment columns for main table
                    main_columns = [col for col in row.keys() if col not in ("equipment", "equipment_resolved", "all_image_urls")]
                    main_placeholders = ["%s" for _ in main_columns]

                    # Extract data for main table
                    main_data = tuple(row[col] for col in main_columns)

                    # Insert main data into the `willhaben` table
                    main_sql = f"INSERT INTO {table_name} ({', '.join(main_columns)}) VALUES ({', '.join(main_placeholders)})"
                    self.cursor.execute(main_sql, main_data)

                    # Use row['id'] as the willhaben_id
                    willhaben_id = row["id"]

                    # Extract equipment and equipment_resolved lists
                    equipment_list = row.get("equipment", [])
                    equipment_resolved_list = row.get("equipment_resolved", [])

                    # Insert equipment data into the `equipment` table
                    equipment_sql = "INSERT INTO equipment (willhaben_id, equipment_code, equipment_resolved) VALUES (%s, %s, %s)"
                    if equipment_list and equipment_resolved_list:
                        for equipment, resolved in zip(equipment_list, equipment_resolved_list):
                            self.cursor.execute(equipment_sql, (willhaben_id, equipment, resolved))
                    else:
                        print(f"No equipment data to insert for willhaben_id {willhaben_id}")

                except pymssql.IntegrityError:
                    # Handle duplicate key error
                    print(f"Duplicate entry for id {row['id']}. Skipping insertion.")
                    self.conn.rollback()
                    continue
                except Exception as e:
                    # Handle other exceptions
                    print(f"Error inserting data for id {row['id']}: {e}")
                    self.conn.rollback()
                    continue

            # Commit the transaction after processing all rows
            self.conn.commit()
            print(f"✅ Data successfully saved to database table '{table_name}'")
        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Failed to insert data: {e}")
