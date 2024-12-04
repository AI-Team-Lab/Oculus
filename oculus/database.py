import os
import pymssql
from dotenv import load_dotenv
from rich import print

load_dotenv()


class DatabaseError(Exception):
    pass


class Database:
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.database = os.getenv('DB_DATABASE')
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = pymssql.connect(
                server=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            print("✅[green] Database connection established.[/green]")
        except Exception as e:
            raise DatabaseError(f"Database connection failed: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✅[green] Database connection closed.[/green]")

    def execute_query(self, query, params=None):
        """
        Executes a query and fetches all results.
        """
        try:
            self.cursor.execute(query, params or [])
            return self.cursor.fetchall()
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")

    def insert_data(self, table_name, data):
        """
        Inserts data into a specified table.
        """
        try:
            for row in data:
                columns = ", ".join(row.keys())
                placeholders = ", ".join(["%s"] * len(row))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                self.cursor.execute(sql, tuple(row.values()))
            self.conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to insert data: {e}")
