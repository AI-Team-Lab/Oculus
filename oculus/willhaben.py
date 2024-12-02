import requests
import json


class WillHaben:
    def __init__(self, base_url: str = "https://www.willhaben.at", data_files: dict = None):
        self.base_url = base_url
        self.search_url = self.base_url + "/webapi/iad/search/atz/seo/gebrauchtwagen/auto/gebrauchtwagenboerse"

        self.headers = {
            "Language": "de-DE,de;q=0.9",
            "accept": "application/json",
            "X-Wh-Client": "api@willhaben.at;responsive_web;server;1.0.0;desktop"
        }

        self.data_files = data_files if data_files else {
            "car_data": "oculus/data/car_data.json",
            "car_status": "oculus/data/car_status.json",
            "car_engine": "oculus/data/car_engine.json",
            "car_equipment": "oculus/data/car_equipment.json",
            "car_location": "oculus/data/car_location.json"
        }

        self.car_data = self.load_data(self.data_files["car_data"])
        self.car_status = self.load_data(self.data_files["car_status"])
        self.car_engine = self.load_data(self.data_files["car_engine"])
        self.car_equipment = self.load_data(self.data_files["car_equipment"])
        self.car_location = self.load_data(self.data_files["car_location"])

    @staticmethod
    def load_data(file_path: str):
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
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTPError: {http_err}")
        except Exception as e:
            print(f"Error: {e}")

    def search_car(self, keyword: str = None, page: int = 1, rows: int = 30, sort: int = 1, car_model_make: str = None,
                   car_model_model: str = None, price_from: int = None,
                   price_to: int = None, mileage_from: int = None, mileage_to: int = None, year_model_from: int = None,
                   year_model_to: int = None, car_type: str = None, motor_condition: str = None, warranty: str = None,
                   engine_effect_from: str = None, engine_effect_to: str = None, engine_fuel: str = None,
                   battery_capacity_from: str = None, battery_capacity_to: str = None, wltp_range_from: str = None,
                   wltp_range_to: str = None, transmission: str = None, wheel_drive: str = None, equipment: list = None,
                   exterior_colour_main: str = None, no_of_doors_from: str = None, no_of_doors_to: str = None,
                   no_of_seats_from: str = None, no_of_seats_to: str = None, area_id: str = None, dealer: str = None,
                   periode: str = None):

        car_model_make_id = self.car_data[car_model_make]["id"] if car_model_make in self.car_data else None
        car_model_model_id = self.car_data[car_model_make]["models"].get(
            car_model_model) if car_model_make in self.car_data and car_model_model else None
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
            for eq in equipment
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

        params = {key: value for key, value in params.items() if value is not None}

        print(params)

        response = self.get_response(self.search_url, params)

        if response and "rowsFound" in response and response["rowsFound"] == 0:
            print("Keine Fahrzeuge gefunden.")
            return None

        return response
