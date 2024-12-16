import requests
import urllib3
import time
import pandas as pd
from random import choice
import os  # Neu hinzugefügt für Verzeichnisoperationen

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.gebrauchtwagen.at/api"
headers_list = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Connection": "keep-alive",
    },
]

global_seen_ids = set()  # Globales Set zur Vermeidung von Duplikaten


class FahrzeugDatenApiClient:
    def __init__(self):
        self.data_list = []

    def safe_request(self, url, retries=3, timeout=5):
        """Sichere HTTP-Anfrage mit mehreren Versuchen."""
        for i in range(retries):
            try:
                response = requests.get(url, headers=choice(headers_list), timeout=timeout, verify=False)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    print(f"404-Fehler für URL: {url}. Überspringe.")
                    return None
                print(f"Retry {i + 1}/{retries} für URL: {url} (Status: {response.status_code})")
                time.sleep(3)
            except requests.exceptions.Timeout:
                print(f"Timeout für URL: {url}. Retry {i + 1}/{retries}.")
                time.sleep(3)
            except Exception as e:
                print(f"Fehler bei Anfrage für URL {url}: {e}")
                time.sleep(3)
        return None

    def fetch_makes_and_models(self):
        """Abrufen der Fahrzeugmarken und Modelle."""
        makes_url = f"{BASE_URL}/taxonomy/makes/"
        data = []
        response = self.safe_request(makes_url)
        if response is None:
            print("Fehler beim Abrufen der Marken.")
            return []

        try:
            makes_data = response.json()
            for make in makes_data:
                make_id = make.get("MAKE_ID")
                make_name = make.get("GENERIC_NAME").replace(" ", "-").lower()
                models_url = f"{BASE_URL}/taxonomy/makes/{make_id}/models"
                models_response = self.safe_request(models_url)
                if models_response is None:
                    print(f"Fehler beim Abrufen der Modelle für Marke: {make_name}.")
                    continue
                models_data = models_response.json()
                for model in models_data:
                    model_name = model.get("GENERIC_NAME").replace(" ", "-").lower()
                    data.append((make_name, model_name))
        except Exception as e:
            print(f"Fehler beim Abrufen der Marken und Modelle: {e}")
        return data

    def fetch_filtered_data(self, marke, modell, year_from):
        """Daten für eine bestimmte Marke, Modell und Jahr abrufen."""
        page = 1
        while True:
            if page == 1:
                url = f"{BASE_URL}/v2/search-listings?sort=year&desc=0&custtype=D&ustate=N%2CU&safemake={marke}&safemodel={modell}&fregfrom={year_from}"
            else:
                url = f"{BASE_URL}/v2/search-listings?sort=year&desc=0&page={page}&custtype=D&ustate=N%2CU&safemake={marke}&safemodel={modell}&fregfrom={year_from}"

            response = self.safe_request(url)
            if response is None:
                print(f"Fehler bei Seite {page}. Überspringe...")
                break

            try:
                data = response.json()
            except ValueError:
                print(f"Ungültige JSON-Antwort bei Seite {page}. URL: {url}")
                break

            if "listings" not in data or not data["listings"]:
                print("Keine weiteren Ergebnisse.")
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
                print("Keine neuen Ergebnisse gefunden. Beenden.")
                break

            print(f"Seite {page}: {len(new_listings)} neue Ergebnisse gespeichert.")

            if page == 20:
                last_year = None
                for i in range(len(data["listings"]) - 1, -1, -1):
                    vehicle = data["listings"][i]
                    first_registration = vehicle.get("firstRegistrationDate", "")
                    if first_registration and first_registration[:4].isdigit():
                        last_year = int(first_registration[:4])
                        break

                if last_year is not None:
                    print(f"Letztes Fahrzeug mit gültiger Erstzulassung gefunden. Jahr: {last_year}.")
                    self.fetch_filtered_data(marke, modell, last_year + 1)  # Nächstes Jahr abfragen
                else:
                    print("Keine gültige Erstzulassung gefunden. Beenden.")
                break

            if len(data["listings"]) < 20:
                print("Keine weiteren Seiten verfügbar. Beenden.")
                break

            page += 1

    def save_to_csv(self, filename):
        """Speichern der gesammelten Daten in eine CSV-Datei."""
        # Ordner sicherstellen
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df = pd.DataFrame(self.data_list)
        print(f"Alle Daten wurden erfolgreich verarbeitet. Insgesamt {len(df)} Fahrzeuge.")
        df.to_csv(filename, index=False)
        print(f"Daten wurden in '{filename}' gespeichert.")


def main():
    api_client = FahrzeugDatenApiClient()
    makes_and_models = api_client.fetch_makes_and_models()
    if not makes_and_models:
        print("Keine Marken und Modelle gefunden.")
        return

    for marke, modell in makes_and_models:
        print(f"Starte Abfrage für Marke: {marke}, Modell: {modell}...")
        api_client.fetch_filtered_data(marke, modell, 1920)

    api_client.save_to_csv("output_api/gebrauchtwagen_data.csv")


if __name__ == "__main__":
    main()
