from tensorflow.keras.models import load_model
from joblib import load
import numpy as np
import os
from dotenv import load_dotenv
from oculus.logging import model_logger_d, model_logger_p

# Load environment variables from the .env file
load_dotenv()


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
        model_logger_d.info(f"Initialized CarPricePredictionModelD with model_dir={self.model_dir}")

    def load_model_and_scaler(self):
        """
        Loads the trained model, label encoders, and scaler from the specified directory.

        The method loads:
        - Keras model from 'trained_model.keras'
        - Label encoders ('brand_encoder.joblib', 'model_encoder.joblib', 'fuel_encoder.joblib')
        - StandardScaler ('scaler.joblib')
        """
        try:
            model_path = os.path.join(self.model_dir, 'trained_model.keras')
            brand_encoder_path = os.path.join(self.model_dir, 'brand_encoder.joblib')
            model_encoder_path = os.path.join(self.model_dir, 'model_encoder.joblib')
            fuel_encoder_path = os.path.join(self.model_dir, 'fuel_encoder.joblib')
            scaler_path = os.path.join(self.model_dir, 'scaler.joblib')

            model_logger_d.info(f"Loading model from {model_path}")
            self.model = load_model(model_path)
            model_logger_d.info("Model loaded successfully.")

            model_logger_d.info(f"Loading brand encoder from {brand_encoder_path}")
            self.brand_encoder = load(brand_encoder_path)
            model_logger_d.info("Brand encoder loaded successfully.")

            model_logger_d.info(f"Loading model encoder from {model_encoder_path}")
            self.model_encoder = load(model_encoder_path)
            model_logger_d.info("Model encoder loaded successfully.")

            model_logger_d.info(f"Loading fuel encoder from {fuel_encoder_path}")
            self.fuel_encoder = load(fuel_encoder_path)
            model_logger_d.info("Fuel encoder loaded successfully.")

            model_logger_d.info(f"Loading scaler from {scaler_path}")
            self.scaler = load(scaler_path)
            model_logger_d.info("Scaler loaded successfully.")

        except Exception as e:
            model_logger_d.error(f"Error loading model and scaler: {e}", exc_info=False)
            raise

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
        try:
            model_logger_d.debug(f"Received prediction request: Make={make}, Model={model}, Mileage={mileage}, "
                                 f"Engine_Effect={engine_effect}, Engine_Fuel={engine_fuel}, Year_Model={year_model}")

            # Konvertiere make, model und engine_fuel zu Kleinbuchstaben für case-insensitive Encoding
            make = make.lower()
            model = model.lower()
            engine_fuel = engine_fuel.lower()

            # Encodiere make, model und fuel type
            encoded_make = self.brand_encoder.transform([make])[0]
            encoded_model = self.model_encoder.transform([model])[0]
            encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

            model_logger_d.debug(
                f"Encoded Make={encoded_make}, Encoded Model={encoded_model}, Encoded Fuel={encoded_fuel}")

            # Skaliere die numerischen Eingabewerte
            scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])
            model_logger_d.debug(f"Scaled features: {scaled_features}")

            # Kombiniere die 2D- und 1D-Arrays
            input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))
            model_logger_d.debug(f"Input features for prediction: {input_features}")

            # Mache die Vorhersage
            prediction = self.model.predict(input_features)
            model_logger_d.debug(f"Raw prediction output: {prediction}")

            # Inverse Transformation des Preises und extrahiere den skalaren Wert
            predicted_price = np.expm1(prediction[0]).item()  # .item() extrahiert den float Wert
            model_logger_d.info(f"Predicted price: {predicted_price} EUR for Make={make}, Model={model}")

            return predicted_price

        except Exception as e:
            model_logger_d.error(f"Error during prediction: {e}", exc_info=False)
            raise


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
        model_logger_p.info(f"Initialized CarPricePredictionModelP with model_dir={self.model_dir}")

    def load_model_and_scaler(self):
        """
        Loads the trained model, label encoders, and scaler from the specified directory.

        The method loads:
        - Keras model from 'trained_model.keras'
        - Label encoders ('brand_encoder.joblib', 'model_encoder.joblib', 'fuel_encoder.joblib')
        - StandardScaler ('scaler.joblib')
        """
        try:
            model_path = os.path.join(self.model_dir, 'trained_model.keras')
            brand_encoder_path = os.path.join(self.model_dir, 'brand_encoder.joblib')
            model_encoder_path = os.path.join(self.model_dir, 'model_encoder.joblib')
            fuel_encoder_path = os.path.join(self.model_dir, 'fuel_encoder.joblib')
            scaler_path = os.path.join(self.model_dir, 'scaler.joblib')

            model_logger_p.info(f"Loading model from {model_path}")
            self.model = load_model(model_path)
            model_logger_p.info("Model loaded successfully.")

            model_logger_p.info(f"Loading brand encoder from {brand_encoder_path}")
            self.brand_encoder = load(brand_encoder_path)
            model_logger_p.info("Brand encoder loaded successfully.")

            model_logger_p.info(f"Loading model encoder from {model_encoder_path}")
            self.model_encoder = load(model_encoder_path)
            model_logger_p.info("Model encoder loaded successfully.")

            model_logger_p.info(f"Loading fuel encoder from {fuel_encoder_path}")
            self.fuel_encoder = load(fuel_encoder_path)
            model_logger_p.info("Fuel encoder loaded successfully.")

            model_logger_p.info(f"Loading scaler from {scaler_path}")
            self.scaler = load(scaler_path)
            model_logger_p.info("Scaler loaded successfully.")

        except Exception as e:
            model_logger_p.error(f"Error loading model and scaler: {e}", exc_info=False)
            raise

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
        try:
            model_logger_p.debug(f"Received prediction request: Make={make}, Model={model}, Mileage={mileage}, "
                                 f"Engine_Effect={engine_effect}, Engine_Fuel={engine_fuel}, Year_Model={year_model}")

            # Konvertiere make, model und engine_fuel zu Kleinbuchstaben für case-insensitive Encoding
            make = make.lower()
            model = model.lower()
            engine_fuel = engine_fuel.lower()

            # Encodiere make, model und fuel type
            encoded_make = self.brand_encoder.transform([make])[0]
            encoded_model = self.model_encoder.transform([model])[0]
            encoded_fuel = self.fuel_encoder.transform([engine_fuel])[0]

            model_logger_p.debug(
                f"Encoded Make={encoded_make}, Encoded Model={encoded_model}, Encoded Fuel={encoded_fuel}")

            # Skaliere die numerischen Eingabewerte
            scaled_features = self.scaler.transform([[mileage, engine_effect, 2024 - year_model]])
            model_logger_p.debug(f"Scaled features: {scaled_features}")

            # Kombiniere die 2D- und 1D-Arrays
            input_features = np.hstack((scaled_features, np.array([[encoded_make, encoded_model, encoded_fuel]])))
            model_logger_p.debug(f"Input features for prediction: {input_features}")

            # Mache die Vorhersage
            prediction = self.model.predict(input_features)
            model_logger_p.debug(f"Raw prediction output: {prediction}")

            # Inverse Transformation des Preises und extrahiere den skalaren Wert
            predicted_price = np.expm1(prediction[0]).item()  # .item() extrahiert den float Wert
            model_logger_p.info(f"Predicted price: {predicted_price} EUR for Make={make}, Model={model}")

            return predicted_price

        except Exception as e:
            model_logger_p.error(f"Error during prediction: {e}", exc_info=False)
            raise
