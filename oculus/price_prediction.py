from tensorflow.keras.models import load_model
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
        Macht eine Vorhersage für den Fahrzeugpreis basierend auf den Eingabedaten.

        Args:
            make (str): Die Marke des Fahrzeugs (z.B. "Toyota").
            model (str): Das Modell des Fahrzeugs (z.B. "Corolla").
            mileage (float): Der Kilometerstand des Fahrzeugs.
            engine_effect (float): Die Motorleistung in kW.
            engine_fuel (str): Der Kraftstofftyp (z.B. "Benzin").
            year_model (int): Das Baujahr des Fahrzeugs.

        Returns:
            float: Der vorhergesagte Fahrzeugpreis in EUR.
        """
        # Konvertiere make, model und engine_fuel zu Kleinbuchstaben für case-insensitive Encoding
        make = make.lower()
        model = model.lower()
        engine_fuel = engine_fuel.lower()

        # Encodiere make, model und fuel type
        encoded_make = self.brand_encoder.transform([make])[0]
        encoded_model = self.model_encoder.transform([model])[0]
        encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

        # Skaliere die numerischen Eingabewerte
        scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])

        # Kombiniere die 2D- und 1D-Arrays
        input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))

        # Mache die Vorhersage
        prediction = self.model.predict(input_features)

        # Inverse Transformation des Preises und extrahiere den skalaren Wert
        predicted_price = np.expm1(prediction[0]).item()  # .item() extrahiert den float Wert

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
        Macht eine Vorhersage für den Fahrzeugpreis basierend auf den Eingabedaten.

        Args:
            make (str): Die Marke des Fahrzeugs (z.B. "Toyota").
            model (str): Das Modell des Fahrzeugs (z.B. "Corolla").
            mileage (float): Der Kilometerstand des Fahrzeugs.
            engine_effect (float): Die Motorleistung in kW.
            engine_fuel (str): Der Kraftstofftyp (z.B. "Benzin").
            year_model (int): Das Baujahr des Fahrzeugs.

        Returns:
            float: Der vorhergesagte Fahrzeugpreis in EUR.
        """
        # Konvertiere make, model und engine_fuel zu Kleinbuchstaben für case-insensitive Encoding
        make = make.lower()
        model = model.lower()
        engine_fuel = engine_fuel.lower()

        # Encodiere make, model und fuel type
        encoded_make = self.brand_encoder.transform([make])[0]
        encoded_model = self.model_encoder.transform([model])[0]
        encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

        # Skaliere die numerischen Eingabewerte
        scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])

        # Kombiniere die 2D- und 1D-Arrays
        input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))

        # Mache die Vorhersage
        prediction = self.model.predict(input_features)

        # Inverse Transformation des Preises und extrahiere den skalaren Wert
        predicted_price = np.expm1(prediction[0]).item()  # .item() extrahiert den float Wert

        return predicted_price
