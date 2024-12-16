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
from tensorflow.keras.callbacks import EarlyStopping, LearningRateScheduler
import os
from joblib import dump

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'


class CarPricePredictionModel:
    def __init__(self, data_path, output_dir='model_p'):
        """
        Initializes the CarPricePredictionModel object, sets file paths for the data,
        and prepares the output directory to save encoders, scaler, and model files.

        Args:
            data_path (str): Path to the dataset CSV file.
            output_dir (str): Directory path to save the encoders, scaler, and trained model.
        """
        self.data_path = os.path.join("../../../Downloads/model_and_prediction/output_api", data_path)
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        self.df = self.load_data()
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.model = None
        self.brand_encoder = None
        self.model_encoder = None
        self.fuel_encoder = None
        self.scaler = None

    def load_data(self):
        """
        Loads the dataset from the CSV file and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded dataset.
        """
        main_data = pd.read_csv(self.data_path, on_bad_lines='warn')
        return main_data

    def preprocess_data(self):
        """
        Preprocesses the data by handling missing values, encoding categorical variables,
        applying transformations, and scaling numerical features for model training.

        This includes:
        - Filling missing 'year_model' values based on 'mileage'.
        - Dropping rows with missing values.
        - Removing rows with zero engine effect (invalid data).
        - Applying log transformation to the 'price' column.
        - Encoding categorical variables ('make', 'model', 'engine_fuel').
        - Adding an 'age' feature based on 'year_model'.
        - Standardizing numerical features ('mileage', 'engine_effect', 'age').
        - Splitting the data into training and testing sets.
        """
        # Replace NaN values for year_model based on mileage logic
        self.df.loc[:, 'year_model'] = self.df.apply(
            lambda row: 2024 if pd.isna(row['year_model']) and row['mileage'] <= 1000 else row['year_model'], axis=1)

        # Drop any rows with NaN values
        self.df = self.df.dropna()

        # Remove rows where engine effect is 0 (invalid)
        self.df = self.df[self.df['engine_effect'] != 0]

        # Log transform price
        self.df.loc[:, 'price'] = np.log1p(self.df['price']).astype(float)

        # Select features and target
        X = self.df[['make', 'model', 'mileage', 'engine_effect', 'engine_fuel', 'year_model']]
        y = self.df['price']

        # Encoding categorical variables
        self.brand_encoder = LabelEncoder()
        self.model_encoder = LabelEncoder()
        self.fuel_encoder = LabelEncoder()

        with pd.option_context('mode.chained_assignment', None):
            X.loc[:, 'make'] = self.brand_encoder.fit_transform(X['make'].str.lower())
            X.loc[:, 'model'] = self.model_encoder.fit_transform(X['model'].str.lower())
            X.loc[:, 'engine_fuel'] = self.fuel_encoder.fit_transform(X['engine_fuel'].str.lower())

        # Add age feature based on the year_model
        X = X.copy()
        X.loc[:, 'age'] = (2024 - X['year_model']).astype(int)

        # Standardize the numerical features
        self.scaler = StandardScaler()
        numerical_features = X[['mileage', 'engine_effect', 'age']].copy()
        numerical_features = numerical_features.replace([np.inf, -np.inf], np.nan).fillna(0)
        numerical_features_scaled = self.scaler.fit_transform(numerical_features)

        # Combine scaled numerical features with encoded categorical features
        X_combined = pd.DataFrame(numerical_features_scaled, columns=['mileage', 'engine_effect', 'age'])
        X_combined['make'] = X['make'].values
        X_combined['model'] = X['model'].values
        X_combined['engine_fuel'] = X['engine_fuel'].values

        # Split data into training and testing sets
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(X_combined, y, test_size=0.2,
                                                                                random_state=50)

        # Save encoders and scaler to disk
        dump(self.brand_encoder, os.path.join(self.output_dir, 'brand_encoder.joblib'))
        dump(self.model_encoder, os.path.join(self.output_dir, 'model_encoder.joblib'))
        dump(self.fuel_encoder, os.path.join(self.output_dir, 'fuel_encoder.joblib'))
        dump(self.scaler, os.path.join(self.output_dir, 'scaler.joblib'))
        print("Encoders and scaler have been successfully saved to the output directory!")

    def lr_schedule(self, epoch, lr):
        """
        Defines a learning rate scheduler that adjusts the learning rate based on the epoch.

        Args:
            epoch (int): The current epoch.
            lr (float): The current learning rate.

        Returns:
            float: The updated learning rate.
        """
        if epoch < 10:
            return 0.0001
        elif epoch < 20:
            return 0.00006
        elif epoch < 30:
            return 0.00004
        else:
            return 0.00002

    def build_model(self):
        """
        Builds the deep learning model for car price prediction.

        The model consists of:
        - An input layer with the shape of the features.
        - Three dense layers with ReLU activations and L2 regularization.
        - Dropout layers to prevent overfitting.
        - A final output layer with linear activation for price prediction.

        The model is compiled with the Adam optimizer and mean squared error loss function.
        """
        input_features = Input(shape=(self.X_train.shape[1],))
        x = Dense(64, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(input_features)
        x = Dropout(0.3)(x)
        x = Dense(64, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(0.01))(x)
        x = Dropout(0.3)(x)
        x = Dense(32, activation='relu')(x)
        output = Dense(1, activation='linear')(x)

        self.model = Model(inputs=input_features, outputs=output)
        self.model.compile(optimizer=Adam(learning_rate=0.000085), loss='mean_squared_error', metrics=['mae'])

    def train_model(self):
        """
        Trains the model using the training data with early stopping and learning rate scheduler.

        Early stopping is used to stop training if the validation loss doesn't improve for 3 epochs.
        The learning rate is adjusted during training using the lr_schedule function.
        """
        early_stopping = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)
        lr_scheduler = LearningRateScheduler(self.lr_schedule)

        self.history = self.model.fit(
            self.X_train, self.y_train,
            epochs=40,
            validation_data=(self.X_test, self.y_test),
            callbacks=[early_stopping, lr_scheduler]
        )

    def evaluate_model(self):
        """
        Evaluates the model on the test set and prints the test loss and mean absolute error (MAE).

        Returns:
            None
        """
        test_loss, test_mae = self.model.evaluate(self.X_test, self.y_test)
        print(f"Test Loss: {test_loss}, Test MAE: {test_mae}")

    def plot_metrics(self):
        """
        Plots the training and validation loss, as well as the training and validation mean absolute error (MAE)
        over the epochs.

        Returns:
            None
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
        Makes predictions using the trained model and evaluates the results.

        Args:
            input_data (optional): Data for prediction. If None, the test set is used.

        Returns:
            None
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

        Returns:
            None
        """
        model_path = os.path.join(self.output_dir, filename)
        self.model.save(model_path)
        print(f"Model has been saved to {model_path}")


def main():
    """
    Main function to run the car price prediction pipeline. It initializes the model, preprocesses the data,
    builds the model, trains it, evaluates it, plots the metrics, makes predictions, and saves the trained model.
    """
    car_model = CarPricePredictionModel('willhaben_data_122024_p.csv')
    car_model.preprocess_data()
    car_model.build_model()
    car_model.train_model()
    car_model.evaluate_model()
    car_model.plot_metrics()
    car_model.predict()
    car_model.save_model()


if __name__ == '__main__':
    main()
