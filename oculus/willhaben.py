import csv
import os
import requests
import json
import random
import time

# List of user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
]


class WillHaben:
    """
    This class provides an interface for interacting with the Willhaben car search API.
    It allows fetching car-related data and performing advanced search queries based on various filters.
    """

    def __init__(self, base_url: str = "https://www.willhaben.at", data_files: dict = None):
        """
        Initializes the WillHaben class with the base URL and data files.

        Args:
            base_url (str): The base URL of the Willhaben API.
            data_files (dict): A dictionary of file paths for loading car-related metadata.
        """
        self.base_url = base_url
        self.search_url = self.base_url + "/webapi/iad/search/atz/seo/gebrauchtwagen/auto/gebrauchtwagenboerse"

        # Default headers
        self.headers = {
            "Language": "de-DE,de;q=0.9",
            "accept": "application/json",
            "X-Wh-Client": "api@willhaben.at;responsive_web;server;1.0.0;desktop",
            "User-Agent": random.choice(user_agents)  # Choose a random user agent
        }

        # Load car metadata from JSON files
        self.data_files = data_files if data_files else {
            "car_data": "oculus/data/car_data.json",
            "car_status": "oculus/data/car_status.json",
            "car_engine": "oculus/data/car_engine.json",
            "car_equipment": "oculus/data/car_equipment.json",
            "car_location": "oculus/data/car_location.json"
        }

        # Load car metadata
        self.car_data = self.load_data(self.data_files["car_data"])
        self.car_status = self.load_data(self.data_files["car_status"])
        self.car_engine = self.load_data(self.data_files["car_engine"])
        self.car_equipment = self.load_data(self.data_files["car_equipment"])
        self.car_location = self.load_data(self.data_files["car_location"])

    @staticmethod
    def load_data(file_path: str):
        """
        Loads JSON data from the specified file path.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            dict: The loaded JSON data or an empty dictionary if loading fails.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error: Error during JSON decoding: {e}")
            return {}

    def get_response(self, url: str, params: dict = None):
        """
        Sends a GET request to the specified URL with optional parameters.

        Args:
            url (str): The endpoint to send the request to.
            params (dict): Optional query parameters for the request.

        Returns:
            dict: The JSON response from the API or None if an error occurs.
        """
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTPError: {http_err}")
        except Exception as e:
            print(f"Error: {e}")

    def search_car(self, keyword: str = None, page: int = 1, rows: int = 30, sort: int = 1,
                   car_model_make: str = None, car_model_model: str = None, price_from: int = None,
                   price_to: int = None, mileage_from: int = None, mileage_to: int = None, year_model_from: int = None,
                   year_model_to: int = None, car_type: str = None, motor_condition: str = None, warranty: str = None,
                   engine_effect_from: str = None, engine_effect_to: str = None, engine_fuel: str = None,
                   battery_capacity_from: str = None, battery_capacity_to: str = None, wltp_range_from: str = None,
                   wltp_range_to: str = None, transmission: str = None, wheel_drive: str = None, equipment: list = None,
                   exterior_colour_main: str = None, no_of_doors_from: str = None, no_of_doors_to: str = None,
                   no_of_seats_from: str = None, no_of_seats_to: str = None, area_id: str = None, dealer: str = None,
                   periode: str = None):
        """
                Performs a car search query on the Willhaben API based on various filters.

                Keyword Args:
                    keyword (str): Search keyword.
                    page (int): Page number for pagination.
                    rows (int): Number of results per page.
                    sort (int): Sorting preference.
                    car_model_make (list | str): Car make (e.g., BMW, Audi).
                    car_model_model (str): Car model.
                    price_from (int): Minimum price.
                    price_to (int): Maximum price.
                    mileage_from (int): Minimum mileage.
                    mileage_to (int): Maximum mileage.
                    year_model_from (int): Minimum model year.
                    year_model_to (int): Maximum model year.
                    car_type (str): Car type (e.g., SUV, sedan).
                    motor_condition (str): Motor condition (e.g., new, used).
                    warranty (str): Warranty information.
                    engine_effect_from (str): Minimum engine power.
                    engine_effect_to (str): Maximum engine power.
                    engine_fuel (str): Type of fuel (e.g., petrol, diesel).
                    battery_capacity_from (str): Minimum battery capacity.
                    battery_capacity_to (str): Maximum battery capacity.
                    wltp_range_from (str): Minimum range (for electric cars).
                    wltp_range_to (str): Maximum range (for electric cars).
                    transmission (str): Transmission type (e.g., automatic, manual).
                    wheel_drive (str): Drive type (e.g., AWD, FWD).
                    equipment (list): List of equipment features.
                    exterior_colour_main (str): Main exterior color.
                    no_of_doors_from (str): Minimum number of doors.
                    no_of_doors_to (str): Maximum number of doors.
                    no_of_seats_from (str): Minimum number of seats.
                    no_of_seats_to (str): Maximum number of seats.
                    area_id (str): Location area ID.
                    dealer (str): Dealer information.
                    periode (str): Period (e.g., daily, weekly).

                Returns:
                    dict: The search results or None if no cars are found.
                """
        car_model_make_id = self.car_data.get(car_model_make.lower(), {}).get("id") if car_model_make else None
        car_model_model_id = self.car_data.get(car_model_make.lower(), {}).get("models", {}).get(
            car_model_model) if car_model_make and car_model_model else None
        car_type_id = self.car_status["car_type"].get(car_type) if car_type in self.car_status["car_type"] else None
        motor_condition_id = self.car_status["motor_condition"].get(motor_condition) if motor_condition in \
                                                                                        self.car_status[
                                                                                            "motor_condition"] else None
        warranty_id = self.car_status["warranty"].get(warranty) if warranty in self.car_status["warranty"] else None
        engine_effect_from_id = self.car_engine["engineeffect_from"].get(
            engine_effect_from) if engine_effect_from else None
        engine_effect_to_id = self.car_engine["engineeffect_to"].get(
            engine_effect_to) if engine_effect_to else None
        engine_fuel_id = self.car_engine["engine_fuel"].get(engine_fuel) if engine_fuel in self.car_engine[
            "engine_fuel"] else None
        battery_capacity_from_id = self.car_engine["battery_capacity_from"].get(
            battery_capacity_from) if battery_capacity_from else None
        battery_capacity_to_id = self.car_engine["battery_capacity_to"].get(
            battery_capacity_to) if battery_capacity_to else None
        wltp_range_from_id = self.car_engine["wltp_range_from"].get(wltp_range_from) if wltp_range_from else None
        wltp_range_to_id = self.car_engine["wltp_range_to"].get(wltp_range_to) if wltp_range_to else None
        transmission_id = self.car_engine["transmission"].get(transmission) if transmission in self.car_engine[
            "transmission"] else None
        wheel_drive_id = self.car_engine["wheel_drive"].get(wheel_drive) if wheel_drive in self.car_engine[
            "wheel_drive"] else None

        equipment_id = ";".join(
            str(self.car_equipment["equipment"].get(eq))
            for eq in car_type
            if eq in self.car_equipment["equipment"]
        ) if equipment else None

        exterior_colour_main_id = self.car_equipment["exterior_colour_main"].get(
            exterior_colour_main) if exterior_colour_main in self.car_equipment[
            "exterior_colour_main"] else None
        no_of_doors_from_id = self.car_equipment["no_of_doors_from"].get(no_of_doors_from) if no_of_doors_from in \
                                                                                              self.car_equipment[
                                                                                                  "no_of_doors_from"] else None
        no_of_doors_to_id = self.car_equipment["no_of_doors_to"].get(no_of_doors_to) if no_of_doors_to in \
                                                                                        self.car_equipment[
                                                                                            "no_of_doors_to"] else None
        no_of_seats_from_id = self.car_equipment["no_of_seats_from"].get(no_of_seats_from) if no_of_seats_from in \
                                                                                              self.car_equipment[
                                                                                                  "no_of_seats_from"] else None
        no_of_seats_to_id = self.car_equipment["no_of_seats_to"].get(no_of_seats_to) if no_of_seats_to in \
                                                                                        self.car_equipment[
                                                                                            "no_of_seats_to"] else None

        if area_id:
            region = self.car_location["locations"].get(area_id)
            if region:
                area_id = region["id"]
            else:
                for state_data in self.car_location["locations"].values():
                    if "areas" in state_data and area_id in state_data["areas"]:
                        area_id = state_data["areas"][area_id]
                        break

        dealer_id = self.car_location["dealer"].get(dealer) if dealer in self.car_location["dealer"] else None
        periode_id = self.car_location["periode"].get(periode) if periode in self.car_location["periode"] else None

        params = {
            "keyword": keyword,
            "page": page,
            "rows": rows,
            "sort": sort,
            "CAR_MODEL/MAKE": car_model_make_id,
            "CAR_MODEL/MODEL": car_model_model_id,
            "PRICE_FROM": price_from,
            "PRICE_TO": price_to,
            "MILEAGE_FROM": mileage_from,
            "MILEAGE_TO": mileage_to,
            "YEAR_MODEL_FROM": year_model_from,
            "YEAR_MODEL_TO": year_model_to,
            "CAR_TYPE": car_type_id,
            "MOTOR_CONDITION": motor_condition_id,
            "WARRANTY": warranty_id,
            "ENGINEEFFECT_FROM": engine_effect_from_id,
            "ENGINEEFFECT_TO": engine_effect_to_id,
            "ENGINE/FUEL": engine_fuel_id,
            "BATTERY_CAPACITY_FROM": battery_capacity_from_id,
            "BATTERY_CAPACITY_TO": battery_capacity_to_id,
            "WLTP_RANGE_FROM": wltp_range_from_id,
            "WLTP_RANGE_TO": wltp_range_to_id,
            "TRANSMISSION": transmission_id,
            "WHEEL_DRIVE": wheel_drive_id,
            "EQUIPMENT": equipment_id,
            "EXTERIOR_COLOUR_MAIN": exterior_colour_main_id,
            "NO_OF_DOORS_FROM": no_of_doors_from_id,
            "NO_OF_DOORS_TO": no_of_doors_to_id,
            "NO_OF_SEATS_FROM": no_of_seats_from_id,
            "NO_OF_SEATS_TO": no_of_seats_to_id,
            "areaId": area_id,
            "DEALER": dealer_id,
            "periode": periode_id
        }
        # Remove None values
        params = {key: value for key, value in params.items() if value is not None}

        # Request the API
        response = self.get_response(self.search_url, params)
        return response

    @staticmethod
    def extract_car_info(car):
        attributes = {attr["name"]: attr["values"][0] for attr in car.get("attributes", {}).get("attribute", [])}
        return {
            "id": car.get("id"),
            "description": car.get("description", "N/A"),
            "mileage": attributes.get("MILEAGE", "N/A"),
            "make": attributes.get("CAR_MODEL/MAKE", "N/A"),
            "model_specification": attributes.get("CAR_MODEL/MODEL_SPECIFICATION", "N/A"),
            "country": attributes.get("COUNTRY", "N/A"),
            "price": attributes.get("PRICE/AMOUNT", "N/A"),
            "location": attributes.get("LOCATION", "N/A"),
        }

    def process_cars(self, car_model_make):
        # Verzeichnisname definieren
        directory = "csv_exports"
        os.makedirs(directory, exist_ok=True)  # Verzeichnis erstellen, falls nicht vorhanden

        # CSV-Dateiname im Verzeichnis
        filename = os.path.join(directory, f"car_make_{car_model_make}.csv")

        # CSV-Datei initialisieren und Header schreiben
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["Title", "Mileage", "Make", "Country", "Model Specification", "Price"])

        page = 1
        while True:
            # Verzögerung vor jeder Anfrage, um die API zu entlasten
            time.sleep(5)

            # Anfrage an die API
            result = self.search_car(car_model_make=car_model_make, page=page, rows=200)
            if not result or result.get("rowsReturned") == 0:
                print(f"No more vehicles found for CAR_MODEL/MAKE {car_model_make}.")
                break

            rows_returned = result.get("rowsReturned", 0)
            print(f"Processing {rows_returned} vehicles on page {page}...")

            # Verarbeiten der Fahrzeuge und in CSV schreiben
            with open(filename, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                for car in result.get("advertSummaryList", {}).get("advertSummary", []):
                    car_info = self.extract_car_info(car)
                    print(car_info)

                    # Informationen in CSV-Format umwandeln
                    writer.writerow([
                        car_info.get("description", "N/A"),
                        car_info.get("mileage", "N/A"),
                        car_info.get("make", "N/A"),
                        car_info.get("country", "N/A"),
                        car_info.get("model_specification", "N/A"),
                        car_info.get("price", "N/A")
                    ])

            page += 1
