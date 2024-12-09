import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Dropout, Input
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_percentage_error
import matplotlib.pyplot as plt
from tensorflow.keras.callbacks import EarlyStopping
import os
from joblib import dump  # For saving the encoders and scaler

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'


class CarPricePredictionModel:
    def __init__(self, data_path):
        # Set the data path to the file in the 'output_api' folder
        self.data_path = os.path.join("output_api", data_path)
        # Read the CSV data from the provided path
        self.df = pd.read_csv(self.data_path)
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None

    def preprocess_data(self):
        # 2. Fill in the year_model: If mileage <= 1000 and year_model is missing, fill with 2024
        self.df.loc[:, 'year_model'] = self.df.apply(
            lambda row: 2024 if pd.isna(row['year_model']) and row['mileage'] <= 1000 else row['year_model'], axis=1)

        # 3. Remove rows with missing (NaN) values
        self.df = self.df.dropna()

        # 4. Remove rows with engine power (kW) equal to 0
        self.df = self.df[self.df['engine_effect'] != 0]

        # 5. Apply logarithmic transformation to the price target
        self.df.loc[:, 'price'] = np.log1p(self.df['price']).astype(float)

        # 6. Define features (X) and target variable (y)
        X = self.df[['make', 'model', 'mileage', 'engine_effect', 'engine_fuel', 'year_model']]
        y = self.df['price']

        # 7. Apply LabelEncoder to make, model, and engine_fuel
        self.brand_encoder = LabelEncoder()
        self.model_encoder = LabelEncoder()
        self.fuel_encoder = LabelEncoder()

        with pd.option_context('mode.chained_assignment', None):
            X.loc[:, 'make'] = self.brand_encoder.fit_transform(X['make'].str.lower())  # Ensure case-insensitivity
            X.loc[:, 'model'] = self.model_encoder.fit_transform(X['model'].str.lower())  # Ensure case-insensitivity
            X.loc[:, 'engine_fuel'] = self.fuel_encoder.fit_transform(
                X['engine_fuel'].str.lower())  # Ensure case-insensitivity

        # 8. Create a copy of X to avoid SettingWithCopyWarning
        X = X.copy()

        # 9. Add a new feature 'age' (current year - year_model)
        X.loc[:, 'age'] = (2024 - X['year_model']).astype(int)

        # 10. Scale numerical features (mileage, engine_effect, age)
        self.scaler = StandardScaler()
        numerical_features = X[['mileage', 'engine_effect', 'age']].copy()

        # Check for non-numeric or infinite values
        numerical_features = numerical_features.replace([np.inf, -np.inf], np.nan)
        numerical_features = numerical_features.fillna(0)

        # Apply scaling
        numerical_features_scaled = self.scaler.fit_transform(numerical_features)

        # 11. Combine all features together
        X_combined = pd.DataFrame(numerical_features_scaled, columns=['mileage', 'engine_effect', 'age'])
        X_combined['make'] = X['make'].values
        X_combined['model'] = X['model'].values
        X_combined['engine_fuel'] = X['engine_fuel'].values

        # 12. Split data into training and test sets
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X_combined, y, test_size=0.2,
                                                                                random_state=42)

        # Save the encoders and scaler
        dump(self.brand_encoder, 'brand_encoder.joblib')
        dump(self.model_encoder, 'model_encoder.joblib')
        dump(self.fuel_encoder, 'fuel_encoder.joblib')
        dump(self.scaler, 'scaler.joblib')
        print("Encoders and scaler have been successfully saved!")

    def build_model(self):
        # 13. Build the model
        input_features = Input(shape=(self.X_train.shape[1],))
        x = Dense(256, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(input_features)
        x = Dropout(0.2)(x)
        x = Dense(128, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(x)
        x = Dropout(0.3)(x)
        x = Dense(64, activation='relu')(x)
        output = Dense(1, activation='linear')(x)

        self.model = Model(inputs=input_features, outputs=output)

        # 14. Compile the model
        self.model.compile(optimizer=Adam(learning_rate=0.0001), loss='mean_squared_error', metrics=['mae'])

    def train_model(self):
        # 15. Add Early Stopping
        early_stopping = EarlyStopping(monitor='val_loss', patience=20, restore_best_weights=True)

        # 16. Train the model
        self.history = self.model.fit(
            self.X_train, self.y_train,
            epochs=20,
            validation_data=(self.X_test, self.y_test),
            callbacks=[early_stopping]
        )

    def evaluate_model(self):
        # 17. Evaluate the model
        test_loss, test_mae = self.model.evaluate(self.X_test, self.y_test)
        print(f"Test Loss: {test_loss}, Test MAE: {test_mae}")

    def plot_metrics(self):
        # 18. Visualize the metrics during training
        plt.figure(figsize=(12, 6))

        # Plot the loss function
        plt.subplot(1, 2, 1)
        plt.plot(self.history.history['loss'], label='Train Loss')
        plt.plot(self.history.history['val_loss'], label='Validation Loss')
        plt.title('Loss during training')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()

        # Plot the mean absolute error (MAE)
        plt.subplot(1, 2, 2)
        plt.plot(self.history.history['mae'], label='Train MAE')
        plt.plot(self.history.history['val_mae'], label='Validation MAE')
        plt.title('MAE during training')
        plt.xlabel('Epochs')
        plt.ylabel('MAE')
        plt.legend()

        plt.tight_layout()
        plt.show()

    def predict(self, input_data=None):
        # Falls keine Eingabedaten übergeben werden, verwenden wir die Testdaten (self.X_test)
        if input_data is None:
            input_data = self.X_test

        # Vorhersagen mit den Eingabedaten
        predictions = self.model.predict(input_data)

        # Berechnung der R2 und MAPE
        r2 = r2_score(self.y_test, predictions)
        mape = mean_absolute_percentage_error(self.y_test, predictions)
        print(f"R²: {r2}, MAPE: {mape}")

        # Farbcode: Differenz zwischen tatsächlichem Preis und vorhergesagtem Preis
        error = self.y_test - predictions.flatten()

        # Definiere Farben: Rot für große Fehler, Grün für kleine Fehler
        colors = np.where(np.abs(error) > 0.5, 'red', 'green')

        # Streudiagramm der tatsächlichen Preise vs. der vorhergesagten Preise mit Farbcode
        plt.scatter(self.y_test, predictions, c=colors, alpha=0.5)
        plt.title("Actual Prices vs Predicted Prices")
        plt.xlabel("Actual Prices")
        plt.ylabel("Predicted Prices")
        plt.show()

    def save_model(self, filename='trained_model.keras'):
        # 20. Save the model
        self.model.save(filename)


def main():
    # 1. Instantiate and use the model
    car_model = CarPricePredictionModel('gebrauchtwagen_data_122024.csv')

    # Preprocess the data
    car_model.preprocess_data()

    # Build the model
    car_model.build_model()

    # Train the model
    car_model.train_model()

    # Evaluate the model
    car_model.evaluate_model()

    # Visualize the metrics
    car_model.plot_metrics()

    # Make predictions and calculate metrics
    car_model.predict()

    # Save the model
    car_model.save_model()


if __name__ == '__main__':
    main()
