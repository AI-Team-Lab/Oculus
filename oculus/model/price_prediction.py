import numpy as np
from tensorflow.keras.models import load_model
from joblib import load
import warnings
import os

# Define the directory where the model files are located
MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '4'

warnings.filterwarnings("ignore")


class CarPricePredictionModel:
    def __init__(self):
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None

    def load_model_and_scaler(self):
        # Define the directory where the model files are located
        MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")

        # Load the model and data preparation objects with the correct path
        self.model = load_model(os.path.join(MODELS_DIR, 'trained_model.keras'))  # Use the correct path for the model
        self.brand_encoder = load(os.path.join(MODELS_DIR, 'brand_encoder.joblib'))
        self.model_encoder = load(os.path.join(MODELS_DIR, 'model_encoder.joblib'))
        self.fuel_encoder = load(os.path.join(MODELS_DIR, 'fuel_encoder.joblib'))
        self.scaler = load(os.path.join(MODELS_DIR, 'scaler.joblib'))

    def predict(self, make, model, mileage, engine_effect, engine_fuel, year_model):
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


def main():
    # User inputs
    print("Please enter the following vehicle data:")

    make = input("Make: ")
    model = input("Model: ")
    mileage = float(input("Mileage: "))
    engine_effect = float(input("Engine power in kW: "))
    engine_fuel = input("Fuel type: ")
    year_model = int(input("Year of first registration: "))

    # Load the model and scaler
    car_model = CarPricePredictionModel()
    car_model.load_model_and_scaler()

    # Make the prediction
    predicted_price = car_model.predict(make, model, mileage, engine_effect, engine_fuel, year_model)

    # Round the predicted price to the nearest 10
    rounded_price = round(predicted_price[0] / 10) * 10

    # Print the rounded predicted price
    print(f"Predicted price for the vehicle: {rounded_price} EUR")

if __name__ == '__main__':
    main()
