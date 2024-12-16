import requests
import urllib3
import time
import pandas as pd
from random import choice
from oculus.logging import gebrauchtwagen_logger
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.gebrauchtwagen.at/api"
headers_list = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Connection": "keep-alive",
    },
]

global_seen_ids = set()  # Global set to avoid duplicates


class Gebrauchtwagen:
    def __init__(self):
        self.data_list = []
        self.logger = gebrauchtwagen_logger

    def safe_request(self, url, retries=3, timeout=5):
        """Safe HTTP request with retries."""
        for i in range(retries):
            try:
                response = requests.get(url, headers=choice(headers_list), timeout=timeout, verify=False)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    self.logger.warning(f"404 error for URL: {url}. Skipping.")
                    return None
                self.logger.info(f"Retry {i + 1}/{retries} for URL: {url} (Status: {response.status_code})")
                time.sleep(3)
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout for URL: {url}. Retry {i + 1}/{retries}.")
                time.sleep(3)
            except Exception as e:
                self.logger.error(f"Error requesting URL {url}: {e}")
                time.sleep(3)
        return None

    def fetch_makes_and_models(self):
        """Fetch vehicle makes and models."""
        makes_url = f"{BASE_URL}/taxonomy/makes/"
        data = []
        response = self.safe_request(makes_url)
        if response is None:
            self.logger.error("Failed to fetch makes.")
            return []

        try:
            makes_data = response.json()
            for make in makes_data:
                make_id = make.get("MAKE_ID")
                make_name = make.get("GENERIC_NAME").replace(" ", "-").lower()
                models_url = f"{BASE_URL}/taxonomy/makes/{make_id}/models"
                models_response = self.safe_request(models_url)
                if models_response is None:
                    self.logger.warning(f"Failed to fetch models for make: {make_name}.")
                    continue
                models_data = models_response.json()
                for model in models_data:
                    model_name = model.get("GENERIC_NAME").replace(" ", "-").lower()
                    data.append((make_name, model_name))
        except Exception as e:
            self.logger.error(f"Error fetching makes and models: {e}")
        return data

    def fetch_filtered_data(self, make, model, year_from):
        """Fetch data for a specific make, model, and year."""
        page = 1
        while True:
            if page == 1:
                url = f"{BASE_URL}/v2/search-listings?sort=year&desc=0&custtype=D&ustate=N%2CU&safemake={make}&safemodel={model}&fregfrom={year_from}"
            else:
                url = f"{BASE_URL}/v2/search-listings?sort=year&desc=0&page={page}&custtype=D&ustate=N%2CU&safemake={make}&safemodel={model}&fregfrom={year_from}"

            response = self.safe_request(url)
            if response is None:
                self.logger.warning(f"Failed to fetch data for page {page}. Skipping.")
                break

            try:
                data = response.json()
            except ValueError:
                self.logger.error(f"Invalid JSON response for page {page}. URL: {url}")
                break

            if "listings" not in data or not data["listings"]:
                self.logger.info("No more results available.")
                break

            new_listings = []
            for vehicle in data["listings"]:
                vehicle_id = vehicle.get("id", "")
                if vehicle_id not in global_seen_ids:
                    global_seen_ids.add(vehicle_id)
                    new_listings.append(vehicle)
                    self.data_list.append({
                        "id": vehicle.get("id", ""),
                        "make": vehicle.get("make", {}).get("formatted", ""),
                        "model": vehicle.get("model", {}).get("formatted", ""),
                        "mileage": vehicle.get("mileage", {}).get("raw", None),
                        "engine_effect": vehicle.get('powerInKW', ''),
                        "engine_fuel": vehicle.get("fuel", ""),
                        "year_model": vehicle.get("firstRegistrationDate", "").split('-')[0] if vehicle.get(
                            "firstRegistrationDate") else None,
                        "location": vehicle.get("location", ""),
                        "price": vehicle.get("price", {}).get("raw", None)
                    })

            if len(new_listings) == 0:
                self.logger.info("No new results found. Exiting.")
                break

            self.logger.info(f"Page {page}: {len(new_listings)} new results saved.")

            if page == 20:
                last_year = None
                for i in range(len(data["listings"]) - 1, -1, -1):
                    vehicle = data["listings"][i]
                    first_registration = vehicle.get("firstRegistrationDate", "")
                    if first_registration and first_registration[:4].isdigit():
                        last_year = int(first_registration[:4])
                        break

                if last_year is not None:
                    self.logger.info(f"Last valid registration year found: {last_year}. Fetching next year.")
                    self.fetch_filtered_data(make, model, last_year + 1)
                else:
                    self.logger.warning("No valid registration year found. Exiting.")
                break

            if len(data["listings"]) < 20:
                self.logger.info("No more pages available. Exiting.")
                break

            page += 1

    def save_to_csv(self):
        """Save collected data to a CSV file."""
        # Get the current date in YYYYMMDD format
        current_date = time.strftime("%Y%m%d")
        # Define the output directory
        output_dir = os.path.join(os.getcwd(), 'api_exports')
        os.makedirs(output_dir, exist_ok=True)
        # Create the filename with the current date
        filename = os.path.join(output_dir, f"gebrauchtwagen-{current_date}.csv")
        # Save the data to the CSV file
        df = pd.DataFrame(self.data_list)
        self.logger.info(f"All data successfully processed. Total: {len(df)} vehicles.")
        df.to_csv(filename, index=False)
        self.logger.info(f"Data saved to '{filename}'.")
