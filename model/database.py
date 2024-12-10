import pymssql
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        # Read database connection details from environment variables
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.database = os.getenv("DB_DATABASE")
        self.conn = None

    def connect(self):
        """Establish a connection to the Azure SQL database."""
        try:
            # Use environment variables to connect to the database
            self.conn = pymssql.connect(
                server=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port  # Ensure port is passed if needed
            )
            print("Connected to Azure SQL successfully.")
        except pymssql.Error as e:
            print(f"Connection error: {e}")
            raise

    def execute_query(self, query, parameters=None):
        """Execute an SQL query with optional parameters."""
        try:
            with self.conn.cursor() as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
        except pymssql.Error as e:
            print(f"SQL query error: {e}")
            raise

    def commit(self):
        """Commit changes to the database."""
        if self.conn:
            self.conn.commit()

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")


class GebrauchtwagenData:
    def __init__(self, database, csv_file_path):
        self.database = database
        self.csv_file_path = csv_file_path

    def create_table(self):
        """Create the 'Gebrauchtwagen' table in Azure SQL."""
        create_table_query = """
        CREATE TABLE dbo.gebrauchtwagen (
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
        """
        self.database.execute_query(create_table_query)
        print("Table 'Gebrauchtwagen' created successfully.")

    def read_csv(self):
        """Load data from the specified CSV file and return it as a pandas DataFrame."""
        try:
            df = pd.read_csv(self.csv_file_path)
            print(f"CSV file loaded successfully: {self.csv_file_path}")
            print(df.head())  # Display the first few rows of the CSV
            print(df.isna().sum())
            df=df.fillna('')
            print(df.isna().sum())
            return df
        except FileNotFoundError as e:
            print(f"CSV file not found: {e}")
            raise
        except pd.errors.ParserError as e:
            print(f"Error parsing the CSV file: {e}")
            raise

    def insert_data(self, df):
        """Insert data from the DataFrame into the 'Gebrauchtwagen' table."""
        insert_query = """
        INSERT INTO dbo.Gebrauchtwagen (id, make, model, mileage, engine_effect, engine_fuel, year_model, location, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.database.conn.cursor() as cursor:
            for _, row in df.iterrows():
                try:
                    cursor.execute(insert_query,
                                   (row['id'],
                                    row['make'],
                                    row['model'],
                                    row['mileage'],
                                    row['engine_effect'],
                                    row['engine_fuel'],
                                    row['year_model'],
                                    row['location'],
                                    row['price']))
                except pymssql.Error as e:
                    print(f"Error inserting data into 'Gebrauchtwagen': {e}")
                    raise
        print("All rows inserted into 'Gebrauchtwagen' successfully.")


def main():
    # Path to the CSV file (ensure this path is correct)
    csv_file_path = os.path.join(os.getcwd(), 'output_api/gebrauchtwagen_data_122024.csv')

    # Create a database connection object
    db = DatabaseConnection()

    try:
        db.connect()

        # Create a GebrauchtwagenData object to handle CSV and database operations
        csv_importer = GebrauchtwagenData(db, csv_file_path)

        # Create the 'Gebrauchtwagen' table in the database
        csv_importer.create_table()

        # Load data from the CSV file
        df = csv_importer.read_csv()

        # Insert the loaded data into the database table
        csv_importer.insert_data(df)

        # Commit changes to the database
        db.commit()
        print("Data imported into the 'Gebrauchtwagen' table successfully.")

    except Exception as e:
        print(f"An error occurred during the process: {e}")
    finally:
        # Ensure the database connection is closed
        db.close()


if __name__ == "__main__":
    main()
