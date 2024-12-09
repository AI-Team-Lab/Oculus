import csv
import os
import httpx
import json
import random
import time
import re
from rich import print
from oculus.database import DatabaseError

# List of user agents
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Mobile Safari/537.36",
]


class Willhaben:
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

        # Initialize an HTTPX client with HTTP/2 support
        self.client = httpx.Client(http2=True, headers=self.headers)

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

    def get_response(self, url: str, params: dict = None, retries: int = 3, delay: int = 30):
        """
        Sends a GET request to the specified URL with optional parameters.
        Handles retries in case of errors and checks for IP blocking.

        Args:
            url (str): The endpoint to send the request to.
            params (dict): Optional query parameters for the request.
            retries (int): Number of retry attempts in case of failure. Default is 3.
            delay (int): Delay in seconds between retry attempts. Default is 30.

        Returns:
            dict: The JSON response from the API or None if an error occurs.
        """
        block_message = "Your IP address is blocked"

        for attempt in range(1, retries + 1):
            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()

                # Check for block message in the response content
                if block_message in response.text:
                    print(f"[red]IP blocked detected on attempt {attempt}: {url}[/red]")
                    raise Exception("IP address is blocked.")

                # Log success and return JSON
                print(f"[green]Request succeeded on attempt {attempt} for URL: {url}[/green]")
                return response.json()

            except httpx.HTTPStatusError as http_err:
                print(
                    f"[red]HTTP Error on attempt {attempt}: {http_err.response.status_code} - {http_err.response.text}[/red]")
            except Exception as e:
                print(f"[red]Unexpected error on attempt {attempt}: {e}[/red]")

            # Wait before retrying
            if attempt < retries:
                print(f"[yellow]Retrying in {delay} seconds...[/yellow]")
                time.sleep(delay)

        # Final failure
        print("[red]All retry attempts failed. Request could not be completed.[/red]")
        return None

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
    def clean_and_truncate(value, max_length=None, default="N/A"):
        """
        Cleans a string by removing unnecessary whitespaces and non-breaking spaces,
        and truncates it to a specified maximum length.

        Args:
            value (str): The string to clean and truncate.
            max_length (int): The maximum allowed length for the string.
            default (str): The default value to return if the input is None or invalid.

        Returns:
            str: The cleaned and truncated string.
        """
        if not value or value == "N/A":
            return default
        cleaned_value = re.sub(r"\s+", " ", str(value).replace("\u00a0", " ").replace("\ufeff", " ").strip())
        if max_length is not None:
            return cleaned_value[:max_length]
        return cleaned_value

    @staticmethod
    def split_and_clean(value, delimiter=";", default=None, max_length=None):
        """
        Splits a string by a delimiter and cleans each part.

        Args:
            value (str): The string to split.
            delimiter (str): The delimiter to split by.
            default (list): The default value to return if the input is invalid.
            max_length (int): The maximum allowed length for each split part.

        Returns:
            list: A list of cleaned strings.
        """
        if not value or value == "N/A":
            return default or []
        # Use the `clean_and_truncate` method correctly
        return [
            Willhaben.clean_and_truncate(part, max_length=max_length) for part in value.split(delimiter)
        ]

    @staticmethod
    def extract_car_info(car):
        """
        Extracts detailed information about a car from the provided JSON structure.

        Args:
            car (dict): A dictionary containing detailed information about a car.

        Returns:
            dict: A dictionary with the extracted and formatted car information.
        """
        try:
            # Extract attributes and handle values that may be lists
            attributes = {
                attr.get("name"): attr.get("values", ["N/A"])[0]
                if len(attr.get("values", [])) == 1 else attr.get("values", "N/A")
                for attr in car.get("attributes", {}).get("attribute", [])
            }

            # Extract main image URL
            advert_image_list = car.get("advertImageList", {}).get("advertImage", [])
            main_image_url = advert_image_list[0].get("mainImageUrl") if advert_image_list else "N/A"

            # Clean and truncate fields to match database column sizes
            specification = Willhaben.clean_and_truncate(attributes.get("CAR_MODEL/MODEL_SPECIFICATION"),
                                                         max_length=255)
            description_head = Willhaben.clean_and_truncate(car.get("description"), max_length=255)
            description = Willhaben.clean_and_truncate(attributes.get("BODY_DYN", "N/A"), max_length=4000)
            heading = Willhaben.clean_and_truncate(attributes.get("HEADING"), max_length=255)

            # Clean and process equipment
            raw_equipment = attributes.get("EQUIPMENT", "N/A")
            equipment = Willhaben.split_and_clean(raw_equipment)

            # Clean and process all image URLs
            raw_all_image_urls = attributes.get("ALL_IMAGE_URLS", "N/A")
            all_image_urls = Willhaben.split_and_clean(raw_all_image_urls)

            # Return the extracted information as a dictionary
            return {
                "id": car.get("id"),
                "advertStatus": car.get("advertStatus", {}).get("id", "N/A"),
                "make": attributes.get("CAR_MODEL/MAKE"),
                "model": attributes.get("CAR_MODEL/MODEL"),
                "specification": specification,
                "description_head": description_head,
                "description": description,
                "year_model": attributes.get("YEAR_MODEL"),
                "transmission": attributes.get("TRANSMISSION"),
                "transmission_resolved": attributes.get("TRANSMISSION_RESOLVED"),
                "mileage": attributes.get("MILEAGE"),
                "noofseats": attributes.get("NOOFSEATS"),
                "engine_effect": attributes.get("ENGINE/EFFECT"),
                "engine_fuel": attributes.get("ENGINE/FUEL"),
                "engine_fuel_resolved": attributes.get("ENGINE/FUEL_RESOLVED"),
                "heading": heading,
                "car_type": attributes.get("CAR_TYPE"),
                "no_of_owners": attributes.get("NO_OF_OWNERS"),
                "color": attributes.get("EXTERIORCOLOURMAIN"),
                "condition": attributes.get("CONDITION"),
                "condition_resolved": attributes.get("CONDITION_RESOLVED"),
                "equipment": equipment,
                "equipment_resolved": attributes.get("EQUIPMENT_RESOLVED"),
                "address": attributes.get("ADDRESS"),
                "location": attributes.get("LOCATION"),
                "postcode": attributes.get("POSTCODE"),
                "district": attributes.get("DISTRICT"),
                "state": attributes.get("STATE"),
                "country": attributes.get("COUNTRY"),
                "coordinates": attributes.get("COORDINATES"),
                "price": attributes.get("PRICE/AMOUNT"),
                "price_for_display": attributes.get("PRICE_FOR_DISPLAY"),
                "warranty": attributes.get("WARRANTY"),
                "warranty_resolved": attributes.get("WARRANTY_RESOLVED"),
                "published": attributes.get("PUBLISHED"),
                "published_string": attributes.get("PUBLISHED_String"),
                "last_updated": attributes.get("LAST_UPDATED"),
                "isprivate": attributes.get("ISPRIVATE"),
                "seo_url": attributes.get("SEO_URL"),
                "main_image_url": main_image_url,
                "all_image_urls": all_image_urls,
            }
        except Exception as e:
            print(f"[red]Error extracting car info: {e}[/red]")
            return {}

    @staticmethod
    def save_data(data, save_type="csv", filename=None, db_instance=None, table_name=None):
        """
        Saves data either to a CSV file or a database.

        Args:
            data (list of dict): The data to save.
            save_type (str): The type of storage ("csv" or "db").
            filename (str): The filename for the CSV (if save_type is "csv").
            db_instance (Database): The database instance object (if save_type is "db").
            table_name (str): The name of the database table (if save_type is "db").
        Raises:
            ValueError: If required arguments are missing or invalid.
        """
        if save_type == "csv":
            # CSV Speicher-Logik
            if not filename:
                raise ValueError("Filename must be provided for CSV storage.")
            try:
                with open(filename, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.writer(file)
                    # Schreibe die Zeilen aus den Daten
                    for row in data:
                        writer.writerow(row.values())
                print(f"✅ Data successfully saved to CSV: {filename}")
            except PermissionError as e:
                print(f"❌ Permission error: Unable to write to '{filename}'. Details: {e}")
            except Exception as e:
                print(f"❌ Unexpected error while saving to CSV: {e}")

        elif save_type == "db":
            # Datenbank Speicher-Logik
            if not db_instance or not table_name:
                raise ValueError("Database instance and table name must be provided for database storage.")
            try:
                # Verwende die zentrale `insert_data`-Methode aus der `Database`-Klasse
                db_instance.insert_data(table_name, data)
                print(f"✅ Data successfully saved to database table '{table_name}'")
            except DatabaseError as e:
                print(f"❌ Database error while saving data: {e}")
            except Exception as e:
                print(f"❌ Unexpected error while saving to database: {e}")

        else:
            # Fehlerhafte `save_type`
            raise ValueError("Invalid save_type. Use 'csv' or 'db'.")

    def process_cars(self, car_model_make=None, save_type="csv", db_instance=None, table_name=None):
        """
        Process cars and save them to the desired output (CSV or database).

        Args:
            car_model_make (str): The car make/model to process. If None, all makes are processed.
            save_type (str): The type of output ("csv" or "db").
            db_instance (Database): Database instance for saving data.
            table_name (str): Name of the database table.

        Returns:
            dict: Summary of the processing.
        """
        # Handle all car makes if `car_model_make` is None
        if car_model_make is None:
            print("[blue]Processing all car makes...[/blue]")
            results = []
            for make in self.car_data.keys():  # Iterate through all available car makes
                print(f"[green]Processing car make: {make}[/green]")
                result = self.process_cars(
                    car_model_make=make,
                    save_type=save_type,
                    db_instance=db_instance,
                    table_name=table_name
                )
                results.append({make: result})
            return {"status": "success", "message": "Processed all car makes.", "results": results}

        # Überprüfen, ob `car_model_make` in `car_data` existiert
        if car_model_make.lower() not in self.car_data:
            error_message = f"[red]Error: No data found for CAR_MODEL/MAKE '{car_model_make}'.[/red]"
            print(error_message)
            return {"status": "error", "message": error_message}

        print(f"[blue]Processing cars for CAR_MODEL/MAKE: {car_model_make}[/blue]")
        directory = "csv_exports"
        os.makedirs(directory, exist_ok=True)  # Create directory if it doesn't exist

        filename = os.path.join(directory, f"car_make_{car_model_make or 'all'}.csv")

        if save_type == "csv":
            # Initialize the CSV with headers dynamically
            try:
                sample_result = self.search_car(car_model_make=car_model_make, page=1, rows=1)
                if sample_result and "advertSummaryList" in sample_result and "advertSummary" in sample_result[
                    "advertSummaryList"]:
                    sample_car = sample_result["advertSummaryList"]["advertSummary"][0]
                    sample_headers = self.extract_car_info(sample_car).keys()
                    with open(filename, mode="w", newline="", encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerow(sample_headers)
            except Exception as e:
                print(f"[red]Error initializing CSV: {e}[/red]")
                return {"status": "error", "message": str(e)}

        # Fetch and process cars
        page = 1
        while True:
            time.sleep(5)  # Avoid overloading the API
            result = self.search_car(car_model_make=car_model_make, page=page, rows=200)
            if not result or result.get("rowsReturned") == 0:
                print(f"[yellow]No more vehicles found for CAR_MODEL/MAKE {car_model_make}.[/yellow]")
                break

            print(f"[green]Processing {result.get('rowsReturned', 0)} vehicles on page {page}...[/green]")

            car_data = [self.extract_car_info(car) for car in
                        result.get("advertSummaryList", {}).get("advertSummary", [])]

            try:
                self.save_data(
                    data=car_data,
                    save_type=save_type,
                    filename=filename if save_type == "csv" else None,
                    db_instance=db_instance if save_type == "db" else None,
                    table_name=table_name if save_type == "db" else None,
                )
            except Exception as e:
                print(f"[red]Error saving data: {e}[/red]")
                return {"status": "error", "message": str(e)}

            page += 1

        return {"status": "success", "message": f"Processed cars for make: {car_model_make}"}

    def close(self):
        """
        Closes the HTTPX client.
        """
        self.client.close()
