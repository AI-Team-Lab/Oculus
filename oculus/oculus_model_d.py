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
from joblib import dump

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'


class CarPricePredictionModel:
    def __init__(self, data_path, extra_data_path, output_dir='model_d/'):
        """
        Initializes the CarPricePredictionModel object, sets file paths for the data,
        and loads the combined data into the dataframe.

        Args:
            data_path (str): Path to the main dataset CSV file.
            extra_data_path (str): Path to the additional dataset CSV file.
            output_dir (str): Directory path to save encoders, scaler, and model files.
        """
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.data_path = os.path.join("../../../Downloads/model_and_prediction/output_api", data_path)
        self.extra_data_path = os.path.join("../../../Downloads/model_and_prediction/output_api", extra_data_path)
        self.df = self.load_and_combine_data()
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None

    def load_and_combine_data(self):
        """
        Loads and combines the primary and extra data from CSV files, removing any duplicate entries.

        Returns:
            pd.DataFrame: The combined dataset with duplicates removed.
        """
        # Load primary data
        main_data = pd.read_csv(self.data_path)

        # Load extra data and handle inconsistent rows
        extra_data = pd.read_csv(self.extra_data_path, on_bad_lines='warn')

        # Combine data and remove duplicates
        combined_data = pd.concat([main_data, extra_data], ignore_index=True)
        combined_data.drop_duplicates(subset=['make', 'model', 'year_model', 'mileage',
                                              'engine_effect', 'engine_fuel', 'price'], inplace=True)
        return combined_data

    def preprocess_data(self):
        """
        Preprocesses the data by handling missing values, encoding categorical variables,
        applying transformations, and scaling numerical features for model training.

        This function does the following:
        - Fills missing 'year_model' values based on the 'mileage' column.
        - Replaces infinite values with NaN and removes rows with missing values.
        - Log-transforms the 'price' column.
        - Encodes categorical variables like 'make', 'model', and 'engine_fuel'.
        - Standardizes the numerical features like 'mileage', 'engine_effect', and 'age'.
        - Splits the data into training and testing sets.
        """
        # Set year_model to 2024 if it's NaN and mileage is <= 1000
        self.df.loc[:, 'year_model'] = self.df.apply(
            lambda row: 2024 if pd.isna(row['year_model']) and row['mileage'] <= 1000 else row['year_model'], axis=1)

        # Replace Inf values with NaN and remove rows with NaN values
        self.df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Drop rows with missing important values
        self.df.dropna(subset=['mileage', 'engine_effect', 'price'], inplace=True)

        # Replace 0 in engine_effect with NaN and drop those rows
        self.df['engine_effect'].replace(0, np.nan, inplace=True)
        self.df.dropna(subset=['engine_effect'], inplace=True)

        # Apply log transformation to price
        self.df.loc[:, 'price'] = np.log1p(self.df['price']).astype(float)

        # Define features and target
        X = self.df[['make', 'model', 'mileage', 'engine_effect', 'engine_fuel', 'year_model']]
        y = self.df['price']

        # Create encoders for categorical variables
        self.brand_encoder = LabelEncoder()
        self.model_encoder = LabelEncoder()
        self.fuel_encoder = LabelEncoder()

        # Encode categorical features
        with pd.option_context('mode.chained_assignment', None):
            X.loc[:, 'make'] = self.brand_encoder.fit_transform(X['make'].str.lower())
            X.loc[:, 'model'] = self.model_encoder.fit_transform(X['model'].str.lower())
            X.loc[:, 'engine_fuel'] = self.fuel_encoder.fit_transform(X['engine_fuel'].str.lower())

        # Add 'age' feature
        X = X.copy()
        X.loc[:, 'age'] = (2024 - X['year_model']).astype(int)

        # Standardize numerical features
        self.scaler = StandardScaler()
        numerical_features = X[['mileage', 'engine_effect', 'age']].copy()
        numerical_features = numerical_features.replace([np.inf, -np.inf], np.nan).fillna(0)

        # Standardize the numerical features
        numerical_features_scaled = self.scaler.fit_transform(numerical_features)

        X_combined = pd.DataFrame(numerical_features_scaled, columns=['mileage', 'engine_effect', 'age'])
        X_combined['make'] = X['make'].values
        X_combined['model'] = X['model'].values
        X_combined['engine_fuel'] = X['engine_fuel'].values

        # Split the data into training and testing sets
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X_combined, y, test_size=0.2,
                                                                                random_state=15)

        # Save the encoders and scaler in the specified output directory
        dump(self.brand_encoder, os.path.join(self.output_dir, 'brand_encoder.joblib'))
        dump(self.model_encoder, os.path.join(self.output_dir, 'model_encoder.joblib'))
        dump(self.fuel_encoder, os.path.join(self.output_dir, 'fuel_encoder.joblib'))
        dump(self.scaler, os.path.join(self.output_dir, 'scaler.joblib'))
        print("Encoders and scaler have been successfully saved in the output/model_d directory!")

    def build_model(self):
        """
        Builds the deep learning model for car price prediction.

        This model consists of:
        - An input layer that takes in feature data.
        - Three dense layers with ReLU activations and L2 regularization.
        - Dropout layers to reduce overfitting.
        - A final output layer with a linear activation for price prediction.

        The model is compiled using the Adam optimizer and mean squared error as the loss function.
        """
        input_features = Input(shape=(self.X_train.shape[1],))
        x = Dense(256, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(input_features)
        x = Dropout(0.3)(x)
        x = Dense(128, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(x)
        x = Dropout(0.3)(x)
        x = Dense(64, activation='relu')(x)
        output = Dense(1, activation='linear')(x)

        self.model = Model(inputs=input_features, outputs=output)
        self.model.compile(optimizer=Adam(learning_rate=0.00005), loss='mean_squared_error', metrics=['mae'])

    def train_model(self):
        """
        Trains the model using the training data.

        Early stopping is applied to prevent overfitting, where training stops if the validation loss
        doesn't improve for 20 epochs.
        """
        early_stopping = EarlyStopping(monitor='val_loss', patience=20, restore_best_weights=True)
        self.history = self.model.fit(
            self.X_train, self.y_train,
            epochs=40,
            validation_data=(self.X_test, self.y_test),
            callbacks=[early_stopping]
        )

    def evaluate_model(self):
        """
        Evaluates the model's performance on the test set and prints the loss and mean absolute error (MAE).
        """
        test_loss, test_mae = self.model.evaluate(self.X_test, self.y_test)
        print(f"Test Loss: {test_loss}, Test MAE: {test_mae}")

    def plot_metrics(self):
        """
        Plots the training and validation loss, as well as training and validation mean absolute error (MAE)
        over the epochs.
        """
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        plt.plot(self.history.history['loss'], label='Train Loss')
        plt.plot(self.history.history['val_loss'], label='Validation Loss')
        plt.title('Loss during training')
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.legend()

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
        """
        Predicts the car prices using the trained model and evaluates the performance with R² and MAPE scores.

        Args:
            input_data (optional): Data for prediction. If None, uses the test set.
        """
        if input_data is None:
            input_data = self.X_test

        predictions = self.model.predict(input_data)
        r2 = r2_score(self.y_test, predictions)
        mape = mean_absolute_percentage_error(self.y_test, predictions)
        print(f"R²: {r2}, MAPE: {mape}")

        error = self.y_test - predictions.flatten()
        colors = np.where(np.abs(error) > 0.5, 'red', 'green')
        plt.scatter(self.y_test, predictions, c=colors, alpha=0.5)
        plt.title("Actual Prices vs Predicted Prices")
        plt.xlabel("Actual Prices")
        plt.ylabel("Predicted Prices")
        plt.show()

    def save_model(self, filename='trained_model.keras'):
        """
        Saves the trained model to a file in the specified output directory.

        Args:
            filename (str): The file name to save the model.
        """
        model_path = os.path.join(self.output_dir, filename)
        self.model.save(model_path)
        print(f"Model has been saved to {model_path}")


def main():
    """
    Main function to run the car price prediction pipeline. It initializes the model, preprocesses the data,
    builds the model, trains it, evaluates it, plots the metrics, makes predictions, and saves the trained model.
    """
    car_model = CarPricePredictionModel('gebrauchtwagen_data_122024.csv', 'willhaben_data_122024_d.csv')
    car_model.preprocess_data()
    car_model.build_model()
    car_model.train_model()
    car_model.evaluate_model()
    car_model.plot_metrics()
    car_model.predict()
    car_model.save_model()


if __name__ == '__main__':
    main()
