from tensorflow.keras.models import load_model
from database import DatabaseConnection # xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx DATEI xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
from joblib import load
import numpy as np
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '4'

class CarPricePredictionModelD:
    def __init__(self, model_dir="model_d"):
        """
        Initializes the CarPricePredictionModel object and sets up the model directory for loading the files.

        Args:
            model_dir (str): Directory where the model, encoders, and scaler are saved.
        """
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None
        self.model_dir = model_dir

    def load_model_and_scaler(self):
        """
        Loads the trained model, label encoders, and scaler from the specified directory.

        The method loads:
        - Keras model from 'trained_model.keras'
        - Label encoders ('brand_encoder.joblib', 'model_encoder.joblib', 'fuel_encoder.joblib')
        - StandardScaler ('scaler.joblib')
        """
        # Load the model and data preparation objects from the specified directory
        self.model = load_model(os.path.join(self.model_dir, 'trained_model.keras'))
        self.brand_encoder = load(os.path.join(self.model_dir, 'brand_encoder.joblib'))
        self.model_encoder = load(os.path.join(self.model_dir, 'model_encoder.joblib'))
        self.fuel_encoder = load(os.path.join(self.model_dir, 'fuel_encoder.joblib'))
        self.scaler = load(os.path.join(self.model_dir, 'scaler.joblib'))

    def predict(self, make, model, mileage, engine_effect, engine_fuel, year_model):
        """
        Makes a prediction for the car price based on the input data.

        Args:
            make (str): The make of the car (e.g., "Toyota").
            model (str): The model of the car (e.g., "Corolla").
            mileage (float): The mileage of the car.
            engine_effect (float): The engine power in kW.
            engine_fuel (str): The fuel type (e.g., "Benzin").
            year_model (int): The year of first registration.

        Returns:
            float: The predicted car price in EUR.
        """
        # Convert make, model, and engine_fuel to lowercase for case-insensitive encoding
        make = make.lower()
        model = model.lower()
        engine_fuel = engine_fuel.lower()

        # Encode the make, model, and fuel type
        encoded_make = self.brand_encoder.transform([make])[0]
        encoded_model = self.model_encoder.transform([model])[0]
        encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

        # Scale the numerical input values
        scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])

        # Combine the 2D and 1D arrays by adding the 1D array as rows
        input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))

        # Make prediction
        prediction = self.model.predict(input_features)
        predicted_price = np.expm1(prediction[0])  # Inverse transform of the price

        return predicted_price



class CarPricePredictionModelP:
    def __init__(self, model_dir="model_p"):
        """
        Initializes the CarPricePredictionModel object and sets up the model directory for loading the files.

        Args:
            model_dir (str): Directory where the model, encoders, and scaler are saved.
        """
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None
        self.model_dir = model_dir

    def load_model_and_scaler(self):
        """
        Loads the trained model, label encoders, and scaler from the specified directory.

        The method loads:
        - Keras model from 'trained_model.keras'
        - Label encoders ('brand_encoder.joblib', 'model_encoder.joblib', 'fuel_encoder.joblib')
        - StandardScaler ('scaler.joblib')
        """
        # Load the model and data preparation objects from the specified directory
        self.model = load_model(os.path.join(self.model_dir, 'trained_model.keras'))
        self.brand_encoder = load(os.path.join(self.model_dir, 'brand_encoder.joblib'))
        self.model_encoder = load(os.path.join(self.model_dir, 'model_encoder.joblib'))
        self.fuel_encoder = load(os.path.join(self.model_dir, 'fuel_encoder.joblib'))
        self.scaler = load(os.path.join(self.model_dir, 'scaler.joblib'))

    def predict(self, make, model, mileage, engine_effect, engine_fuel, year_model):
        """
        Makes a prediction for the car price based on the input data.

        Args:
            make (str): The make of the car (e.g., "Toyota").
            model (str): The model of the car (e.g., "Corolla").
            mileage (float): The mileage of the car.
            engine_effect (float): The engine power in kW.
            engine_fuel (str): The fuel type (e.g., "Benzin").
            year_model (int): The year of first registration.

        Returns:
            float: The predicted car price in EUR.
        """
        # Convert make, model, and engine_fuel to lowercase for case-insensitive encoding
        make = make.lower()
        model = model.lower()
        engine_fuel = engine_fuel.lower()

        # Encode the make, model, and fuel type
        encoded_make = self.brand_encoder.transform([make])[0]
        encoded_model = self.model_encoder.transform([model])[0]
        encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

        # Scale the numerical input values
        scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])

        # Combine the 2D and 1D arrays by adding the 1D array as rows
        input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))

        # Make prediction
        prediction = self.model.predict(input_features)
        predicted_price = np.expm1(prediction[0])  # Inverse transform of the price

        return predicted_price



def update_predicted_prices(db_connection):
    """
    Update the predicted car prices in the database for all cars in the 'willhaben' table.

    Args:
        db_connection (DatabaseConnection): The database connection object.

    Raises:
        pymssql.Error: If database queries or updates fail.
    """
    # Create CarPricePredictionModel instance
    car_model = CarPricePredictionModelD()
    car_model.load_model_and_scaler()

    # Retrieve data from the 'willwagen' table
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
    """
    db_connection.execute_query(query)
    cars = db_connection.conn.cursor().fetchall()

    # For each car, predict the price and update the 'predicted_dealer_price'
    for car in cars:
        willhaben_id, make, model, mileage, engine_effect, engine_fuel, year_model = car
        predicted_price = car_model.predict(make, model, mileage, engine_effect, engine_fuel, year_model)

        # Round the predicted price to the nearest 10
        predicted_price = round(predicted_price[0] / 10) * 10

        # Update the predicted dealer price in the 'willwagen' table
        update_query = """
            UPDATE dwh.willwagen
            SET predicted_dealer_price = %s
            WHERE willhaben_id = %s
        """
        db_connection.execute_query(update_query, (predicted_price, willhaben_id))

    # Commit the transaction
    db_connection.commit()
    print("Predicted prices updated successfully.")





def main():
    """
    Main function to connect to the database, update predicted prices, and handle exceptions.

    It initializes the database connection, performs price prediction updates, and ensures the connection is properly closed.
    """
    db = DatabaseConnection()

    try:
        db.connect()
        update_predicted_prices(db)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
