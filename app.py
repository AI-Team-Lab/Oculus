import os
import json
from pathlib import Path
from flask import Flask, render_template, request, jsonify, g
from oculus import *
from celery.result import AsyncResult
from datetime import datetime

# Initialize Flask
app = Flask(__name__)

# Initialize Classes
willhaben = Willhaben()
gebrauchtwagen = Gebrauchtwagen()

# Initialisiere und lade die Modelle beim Start der App
base_dir = Path(__file__).resolve().parent  # Verzeichnis von app.py

car_model_d = CarPricePredictionModelD(model_dir=base_dir / 'oculus' / 'model_d')
car_model_p = CarPricePredictionModelP(model_dir=base_dir / 'oculus' / 'model_p')

try:
    car_model_d.load_model_and_scaler()
    flask_logger.info("CarPricePredictionModelD loaded successfully.")
except Exception as e:
    flask_logger.error(f"Failed to load CarPricePredictionModelD: {e}")

try:
    car_model_p.load_model_and_scaler()
    flask_logger.info("CarPricePredictionModelP loaded successfully.")
except Exception as e:
    flask_logger.error(f"Failed to load CarPricePredictionModelP: {e}")


# Laden der Mapping-Datei
def load_mappings():
    mapping_path = os.path.join(os.getcwd(), 'oculus', 'mapping', 'willhaben_mapping.json')
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        flask_logger.info(f"Successfully loaded mappings from {mapping_path}")
        return mappings
    except FileNotFoundError:
        flask_logger.error(f"Mapping file {mapping_path} not found.")
        return {}
    except json.JSONDecodeError as e:
        flask_logger.error(f"Error decoding JSON from {mapping_path}: {e}")
        return {}
    except Exception as e:
        flask_logger.error(f"Unexpected error loading mappings: {e}")
        return {}


# Lade die Mappings beim Start der App
mappings = load_mappings()

# Erstellen von Reverse-Mappings (internal_value: display_name)
reverse_make_mapping = {v: k for k, v in mappings.get('willhaben_make_mapping', {}).items()}
reverse_model_mapping = {v: k for k, v in mappings.get('willhaben_model_mapping', {}).items()}
reverse_fuel_mapping = {v: k for k, v in mappings.get('willhaben_fuel_type_mapping', {}).items()}


def get_db():
    """
    Provides a database connection for the current app context.
    """
    if 'db' not in g:
        g.db = Database()
        try:
            g.db.connect()
            flask_logger.info("Database connection established.")
        except Exception as e:
            flask_logger.error(f"Error connecting to database: {e}")
            raise e
    return g.db


@app.teardown_appcontext
def close_resources(error):
    """
    Cleans up resources at the end of the app context.
    """
    db = g.pop('db', None)
    if db is not None:
        try:
            db.close()
            flask_logger.info("Database connection closed.")
        except Exception as e:
            flask_logger.error(f"Error closing database connection: {e}")


def get_make_options():
    """
    Retrieves vehicle make options from the database and maps them to the corresponding display names.
    Only includes makes that have associated models.
    """
    try:
        db = get_db()
        sql = """
            SELECT DISTINCT m1.make_name
            FROM dwh.make m1
            JOIN dwh.model m2 ON m1.id = m2.make_id
            WHERE m2.model_name <> 'sonstige'
            ORDER BY m1.make_name
        """
        db.cursor.execute(sql)
        makes = db.cursor.fetchall()
        options = []
        for (make_internal,) in makes:
            # Mapping: internal_value -> display_name
            display_name = reverse_make_mapping.get(make_internal, make_internal.replace('_', ' ').title())
            options.append((display_name, make_internal))
            if make_internal not in reverse_make_mapping:
                flask_logger.warning(
                    f"No display mapping found for make '{make_internal}', using '{display_name}' as default.")
        flask_logger.info("Fetched and filtered make options from database.")
        return options
    except Exception as e:
        flask_logger.error(f"Error fetching make options: {e}")
        return []


def get_fuel_options():
    """
    Retrieves fuel type options from the mapping file.
    Returns a list of tuples: (fuel_display, fuel_value)
    """
    fuel_mapping = mappings.get('willhaben_fuel_type_mapping', {})
    options = [(fuel, value) for fuel, value in fuel_mapping.items()]
    flask_logger.info("Fetched fuel options from mappings.")
    return options


@app.route('/')
def index():
    return render_template('index.html', cars=None)


@app.route('/search', methods=['GET', 'POST'])
def search():
    query = request.form.get('query') if request.method == 'POST' else request.args.get('query')
    page = request.args.get('page', 1, type=int)
    per_page = 30
    offset = (page - 1) * per_page

    if query:
        db = get_db()
        try:
            sql = """
                SELECT 
                    willhaben_id, 
                    make_name, 
                    model_name, 
                    specification, 
                    description, 
                    year_model, 
                    transmission_type,
                    mileage, 
                    noofseats, 
                    power_in_kw, 
                    fuel_type, 
                    type, 
                    no_of_owners, 
                    color_name, 
                    car_condition,
                    address, 
                    location, 
                    postcode, 
                    district, 
                    state, 
                    country, 
                    price, 
                    predicted_dealer_price,
                    warranty, 
                    isprivate, 
                    published, 
                    last_updated, 
                    image_url
                FROM dwh.willhaben
                WHERE LOWER(make_name) LIKE %s 
                   OR LOWER(model_name) LIKE %s
                   OR LOWER(specification) LIKE %s
                ORDER BY last_updated DESC
                OFFSET %s ROWS FETCH NEXT %s ROWS ONLY
            """
            like_query = f"%{query.lower()}%"
            db.cursor.execute(sql, (like_query, like_query, like_query, offset, per_page))
            rows = db.cursor.fetchall()

            # Annahme: Die Spalten sind in der gleichen Reihenfolge wie im SELECT
            cars = []
            for row in rows:
                (
                    willhaben_id,
                    make_internal,
                    model_internal,
                    specification,
                    description,
                    year_model,
                    transmission_type,
                    mileage,
                    noofseats,
                    power_in_kw,
                    fuel_type,
                    type_,
                    no_of_owners,
                    color_name,
                    car_condition,
                    address,
                    location,
                    postcode,
                    district,
                    state,
                    country,
                    price,
                    predicted_dealer_price,
                    warranty,
                    isprivate,
                    published,
                    last_updated,
                    image_url
                ) = row

                # Mapping der Felder
                mapped_make = mappings.get('willhaben_make_mapping', {})
                reverse_make_mapping = {v.lower(): k for k, v in mapped_make.items()}
                mapped_make_name = reverse_make_mapping.get(make_internal.lower(), make_internal)

                mapped_model = mappings.get('willhaben_model_mapping', {})
                reverse_model_mapping = {v.lower(): k for k, v in mapped_model.items()}
                mapped_model_name = reverse_model_mapping.get(model_internal.lower(), model_internal)

                car_type_mapping = mappings.get('willhaben_car_type_mapping', {})
                reverse_car_type_mapping = {v.lower(): k for k, v in car_type_mapping.items()}
                mapped_car_type = reverse_car_type_mapping.get(type_.lower(), type_)

                transmission_type_mapping = mappings.get('willhaben_transmission_type_mapping', {})
                reverse_transmission_type_mapping = {v.lower(): k for k, v in transmission_type_mapping.items()}
                mapped_transmission_type = reverse_transmission_type_mapping.get(transmission_type.lower(),
                                                                                 transmission_type)

                fuel_type_mapping = mappings.get('willhaben_fuel_type_mapping', {})
                reverse_fuel_type_mapping = {v.lower(): k for k, v in fuel_type_mapping.items()}
                mapped_fuel_type = reverse_fuel_type_mapping.get(fuel_type.lower(), fuel_type)

                color_mapping = mappings.get('willhaben_color_mapping', {})
                reverse_color_mapping = {v.lower(): k for k, v in color_mapping.items()}
                mapped_color = reverse_color_mapping.get(color_name.lower(), color_name)

                condition_mapping = mappings.get('willhaben_condition_mapping', {})
                reverse_condition_mapping = {v.lower(): k for k, v in condition_mapping.items()}
                mapped_condition = reverse_condition_mapping.get(car_condition.lower(), car_condition)

                # Formatierung und Standardwerte
                formatted_published = published.strftime('%d.%m.%Y %H:%M') if published else "N/A"
                formatted_last_updated = last_updated.strftime('%d.%m.%Y %H:%M') if last_updated else "N/A"

                # Konvertiere `predicted_dealer_price` zu Float oder setze auf None
                try:
                    if predicted_dealer_price is not None:
                        predicted_dealer_price = float(predicted_dealer_price)
                    else:
                        predicted_dealer_price = None
                except (ValueError, TypeError):
                    predicted_dealer_price = None

                # Hinzufügen des Fahrzeugs zur Liste
                cars.append({
                    'willhaben_id': willhaben_id,
                    'make_name': mapped_make_name,
                    'model_name': mapped_model_name,
                    'specification': specification or "N/A",
                    'description': description or "N/A",
                    'year_model': year_model or "N/A",
                    'transmission_type': mapped_transmission_type or "N/A",
                    'mileage': mileage or "N/A",
                    'noofseats': noofseats or "N/A",
                    'power_in_kw': power_in_kw or "N/A",
                    'fuel_type': mapped_fuel_type or "N/A",
                    'type': mapped_car_type or "N/A",
                    'no_of_owners': no_of_owners if no_of_owners else "Unknown",
                    'color_name': mapped_color or "N/A",
                    'car_condition': mapped_condition or "N/A",
                    'address': address or "N/A",
                    'location': location or "N/A",
                    'postcode': postcode or "N/A",
                    'district': district or "N/A",
                    'state': state or "N/A",
                    'country': country or "N/A",
                    'price': price or "N/A",
                    'predicted_dealer_price': predicted_dealer_price,
                    'warranty': "Available" if warranty else "None",
                    'isprivate': "Yes" if isprivate else "No",
                    'published': formatted_published,
                    'last_updated': formatted_last_updated,
                    'image_url': image_url or "https://placehold.co/300x200?text=Kein+Bild"
                })

            flask_logger.info(f"Fetched {len(cars)} cars for query '{query}'.")
            return render_template('index.html', cars=cars, query=query, page=page)
        except Exception as e:
            flask_logger.error(f"Error during search: {e}")
            return render_template('index.html', cars=None, query=None, page=None)

    return render_template('index.html', cars=None, query=None, page=None)


@app.route('/prediction', methods=['GET', 'POST'])
def prediction():
    errors = {}
    result = None
    selected_make = None
    selected_model = None
    selected_fuel = None
    kilometer = None
    leistung_kw = None
    erstzulassung = None

    current_year = datetime.now().year  # Dynamisches aktuelles Jahr

    if request.method == 'POST':
        selected_make = request.form.get('make')
        selected_model = request.form.get('model')
        selected_fuel = request.form.get('fuel')
        kilometer = request.form.get('kilometer')
        leistung_kw = request.form.get('leistung_kw')
        erstzulassung = request.form.get('erstzulassung')

        flask_logger.info(
            f"Received prediction request: make_internal_value={selected_make}, model_internal_value={selected_model}, "
            f"fuel={selected_fuel}, kilometer={kilometer}, leistung_kw={leistung_kw}, erstzulassung={erstzulassung}"
        )

        # Validierung der Eingaben
        if not all([selected_make, selected_model, selected_fuel, kilometer, leistung_kw, erstzulassung]):
            errors['prediction'] = "Alle Felder müssen ausgefüllt werden."
            flask_logger.warning("Prediction request has missing fields.")
        else:
            try:
                # Mapping der internen Werte zu Anzeigenamen
                make_display = reverse_make_mapping.get(selected_make.lower(), selected_make.replace('_', ' ').title())
                model_display = reverse_model_mapping.get(selected_model.lower(),
                                                          selected_model.replace('_', ' ').title())
                fuel_display = reverse_fuel_mapping.get(selected_fuel.lower(), selected_fuel.replace('_', ' ').title())

                # Durchführung der Vorhersagen mit beiden Modellen
                prediction_d = car_model_d.predict(
                    make=make_display,
                    model=model_display,
                    mileage=float(kilometer),
                    engine_effect=float(leistung_kw),
                    engine_fuel=fuel_display,
                    year_model=int(erstzulassung)
                )
                flask_logger.debug("Prediction with CarPricePredictionModelD completed.")

                prediction_p = car_model_p.predict(
                    make=make_display,
                    model=model_display,
                    mileage=float(kilometer),
                    engine_effect=float(leistung_kw),
                    engine_fuel=fuel_display,
                    year_model=int(erstzulassung)
                )
                flask_logger.debug("Prediction with CarPricePredictionModelP completed.")

                # Runde die vorhergesagten Preise auf das nächste 10er-Intervall
                predicted_price_d = round(prediction_d / 10) * 10
                predicted_price_p = round(prediction_p / 10) * 10
                flask_logger.debug(f"Rounded predicted_price_d: {predicted_price_d} EUR")
                flask_logger.debug(f"Rounded predicted_price_p: {predicted_price_p} EUR")

                # Formatierung des Ergebnisses
                result = (
                    f"<strong>Vorhersage für Auto:</strong><br>"
                    f"Marke: {make_display}<br>"
                    f"Modell: {model_display}<br>"
                    f"Treibstoff: {fuel_display}<br>"
                    f"Kilometer: {kilometer} KM<br>"
                    f"Leistung: {leistung_kw} KW<br>"
                    f"Erstzulassung: {erstzulassung}<br><br>"
                    f"<div class='indented'>Händlereinkaufspreis: {predicted_price_d} €</div>"
                    f"<div class='indented'>Privatverkaufspreis: {predicted_price_p} €</div>"
                )
                flask_logger.info(f"Prediction result generated: {result}")

            except Exception as e:
                errors['prediction'] = "Fehler bei der Erstellung der Vorhersage."
                flask_logger.error(f"Error during prediction: {e}", exc_info=True)

    return render_template(
        'prediction.html',
        make_options=get_make_options(),
        fuel_options=get_fuel_options(),
        result=result,
        selected_make=selected_make,
        selected_model=selected_model,
        selected_fuel=selected_fuel,
        kilometer=kilometer,
        leistung_kw=leistung_kw,
        erstzulassung=erstzulassung,
        errors=errors,
        current_year=current_year
    )


@app.route('/detailed_search', methods=['GET', 'POST'])
def detailed_search():
    if request.method == 'POST':
        # Verarbeiten Sie die detaillierte Suchanfrage
        # Beispiel: Holen Sie sich Suchparameter aus dem Formular
        make = request.form.get('make')
        model = request.form.get('model')
        year = request.form.get('year')
        # Führen Sie Ihre Suchlogik durch, z.B. Datenbankabfrage
        # cars = perform_detailed_search(make, model, year)

        # Placeholder für Suchergebnisse
        cars = [
            {
                'make_display': reverse_make_mapping.get(make, make.replace('_', ' ').title()),
                'model_display': reverse_model_mapping.get(model, model.replace('_', ' ').title()),
                'year_model': year,
                'image_url': 'https://placehold.co/300x200?text=Car+Image',  # Beispielbild
                # Weitere Fahrzeugdaten
            },
            # Weitere Fahrzeuge...
        ]

        flask_logger.info(f"Detailed search performed: make={make}, model={model}, year={year}")
        return render_template('detailed_search.html', cars=cars, make=make, model=model, year=year)

    # GET-Anfrage: Zeigen Sie das Suchformular ohne Ergebnisse an
    return render_template('detailed_search.html', cars=None)


@app.route("/fetch_cars", methods=["GET", "POST"])
def fetch_cars():
    """
    Initiates a Celery task to fetch cars based on the provided parameters.
    Supports both GET and POST methods.

    GET Parameters:
        - car_model_make (str): Specific car make/model to fetch.
        - start_make (str): Start point for fetching cars by make.

    POST Payload (JSON):
        {
            "car_model_make": "string",
            "start_make": "string"
        }

    Returns:
        JSON response containing the task ID or an error message.
    """
    try:
        car_model_make = None
        start_make = None  # Starting point for makes

        # Handle GET request
        if request.method == "GET":
            car_model_make = request.args.get("car_model_make", None)
            start_make = request.args.get("start_make", None)

        # Handle POST request
        elif request.method == "POST":
            data = request.get_json()
            if not data:
                flask_logger.error("Invalid JSON payload received in POST request.")
                return jsonify({"status": "error", "message": "Invalid JSON payload."}), 400
            car_model_make = data.get("car_model_make", None)
            start_make = data.get("start_make", None)

        # Validate car_model_make
        if car_model_make and car_model_make.lower() not in willhaben.car_data:
            error_message = f"Invalid CAR_MODEL/MAKE: '{car_model_make}'. Please provide a valid car make."
            flask_logger.error(error_message)
            return jsonify({"status": "error", "message": error_message}), 400

        # Validate start_make
        if start_make and start_make.lower() not in willhaben.car_data:
            error_message = f"Invalid START_MAKE: '{start_make}'. Please provide a valid start make."
            flask_logger.error(error_message)
            return jsonify({"status": "error", "message": error_message}), 400

        # Log task queuing information
        if car_model_make:
            flask_logger.info(f"Queuing task for CAR_MODEL/MAKE: {car_model_make}")
        elif start_make:
            flask_logger.info(f"Queuing task for all cars starting from make: {start_make}")
        else:
            flask_logger.info("Queuing task for all cars.")

        # Pass car_model_make or start_make to the Celery task
        task = fetch_cars_task.apply_async(args=[car_model_make, start_make])

        # Log successful task queuing
        flask_logger.info(f"Task queued successfully with Task ID: {task.id}")
        return jsonify({"status": "success", "task_id": task.id})

    except Exception as e:
        # Log unexpected errors
        flask_logger.error(f"Error queuing task: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/fetch_gebrauchtwagen', methods=['GET', 'POST'])
def fetch_gebrauchtwagen():
    """
    Initiates a Celery task to fetch data from the Gebrauchtwagen API.
    Supports both GET and POST methods.

    GET Parameters:
        - year_from (int): Optional start year for fetching vehicles.

    POST Payload (JSON):
        {
            "year_from": 1920
        }

    Returns:
        JSON response containing the task ID or an error message.
    """
    try:
        year_from = 1920  # Default start year

        # Handle GET request
        if request.method == "GET":
            year_from = request.args.get("year_from", 1920, type=int)

        # Handle POST request
        elif request.method == "POST":
            data = request.get_json()
            if not data:
                flask_logger.error("Invalid JSON payload received in POST request.")
                return jsonify({"status": "error", "message": "Invalid JSON payload."}), 400
            year_from = data.get("year_from", 1920)

        # Log task initiation
        flask_logger.info(f"Queuing Gebrauchtwagen task starting from year {year_from}.")

        # Trigger the Celery task
        task = fetch_gebrauchtwagen_task.apply_async(args=[year_from])

        flask_logger.info(f"Gebrauchtwagen task queued successfully with Task ID: {task.id}")
        return jsonify({"status": "success", "task_id": task.id})

    except Exception as e:
        flask_logger.error(f"Error queuing Gebrauchtwagen task: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/task_status/<task_id>", methods=["GET"])
def task_status(task_id):
    """
    Retrieves the status of a Celery task by its task ID.

    Args:
        task_id (str): The ID of the Celery task.

    Returns:
        JSON response containing the task status, result, or error information.
    """
    try:
        # Fetch the task result using Celery's AsyncResult
        task_result = AsyncResult(task_id)

        # Log task state
        if task_result.state == "PENDING":
            flask_logger.info(f"Task {task_id} is pending.")
        elif task_result.state == "STARTED":
            flask_logger.info(f"Task {task_id} has started.")
        elif task_result.state == "SUCCESS":
            flask_logger.info(f"Task {task_id} completed successfully.")
        elif task_result.state == "FAILURE":
            flask_logger.error(f"Task {task_id} failed with error: {task_result.info}")

        # Build response based on task state
        if task_result.state == "FAILURE":
            response = {
                "task_id": task_id,
                "status": task_result.state,
                "error": str(task_result.info),  # Provides detailed error information
            }
        else:
            response = {
                "task_id": task_id,
                "status": task_result.state,
                "result": task_result.result,
            }

        return jsonify(response)

    except Exception as e:
        flask_logger.error(f"Error while retrieving status for task {task_id}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/load_json', methods=['GET'])
def load_json():
    """
    Unified route to load JSON data into corresponding database tables.
    Expected query parameters:
    - entity: The name of the entity (e.g., 'car_data', 'car_engine', 'car_equipment', 'car_location', etc.)
    - file_path (optional): The path to the JSON file (defaults to a file located in 'oculus/data')
    """
    db = None
    entity = None

    try:
        # Base directory for JSON files
        base_path = os.path.join(os.getcwd(), 'oculus', 'data')

        # Read entity name and file path from query parameters
        entity = request.args.get('entity')
        if not entity:
            flask_logger.error("The 'entity' parameter is missing.")
            return jsonify({"status": "error", "message": "The 'entity' parameter is missing."}), 400

        filename = request.args.get('file_path', f'{entity}.json')
        file_path = os.path.join(base_path, filename)

        # Check if the file exists
        if not os.path.isfile(file_path):
            flask_logger.error(f"File {file_path} not found.")
            return jsonify({"status": "error", "message": f"File {file_path} not found."}), 400

        # Database instance
        db = get_db()

        # Define a mapping between entities and their respective tables in sync_log
        table_mapping = {
            "car_data": ["dl.make", "dl.model"],
            "car_engine": ["dl.engine_effect", "dl.engine_fuel", "dl.battery_capacity", "dl.wltp_range",
                           "dl.transmission", "dl.wheel_drive"],
            "car_equipment": ["dl.equipment_search", "dl.exterior_colour_main", "dl.no_of_doors", "dl.no_of_seats"],
            "car_location": ["dl.area", "dl.location", "dl.dealer", "dl.periode"],
            "car_status": ["dl.car_type", "dl.motor_condition", "dl.warranty"]
        }

        # Delete entries from `dwh.sync_log` for the corresponding tables
        if entity in table_mapping:
            for table_name in table_mapping[entity]:
                db.cursor.execute("DELETE FROM dwh.sync_log WHERE table_name = %s", (table_name,))
                flask_logger.info(f"Deleted sync_log entry for table '{table_name}'.")

        # Call the entity-specific function
        try:
            if entity == "car_data":
                db.load_car_data(file_path)
                flask_logger.info(f"'car_data' loaded successfully from {file_path}")
            elif entity == "car_engine":
                db.load_car_engine(file_path)
                flask_logger.info(f"'car_engine' loaded successfully from {file_path}")
            elif entity == "car_equipment":
                db.load_car_equipment(file_path)
                flask_logger.info(f"'car_equipment' loaded successfully from {file_path}")
            elif entity == "car_location":
                db.load_car_location(file_path)
                flask_logger.info(f"'car_location' loaded successfully from {file_path}")
            elif entity == "car_status":
                db.load_car_status(file_path)
                flask_logger.info(f"'car_status' loaded successfully from {file_path}")
            else:
                flask_logger.error(f"Unknown entity '{entity}'")
                return jsonify({"status": "error", "message": f"Unknown entity '{entity}'."}), 400
        except Exception as e:
            flask_logger.error(f"Error loading entity '{entity}': {e}")
            return jsonify({"status": "error", "message": str(e)}), 500
        finally:
            db.close()

        return jsonify(
            {"status": "success", "message": f"Data for '{entity}' successfully loaded from {file_path}."})
    except Exception as e:
        if db:
            flask_logger.error(f"Error while loading data for '{entity}': {e}")
        else:
            flask_logger.error(f"Error while loading data for '{entity}': {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/move_data_to_dwh", methods=["GET"])
def move_data_to_dwh():
    """
    Triggers a Celery task to move data from staging to the Data Warehouse.
    """
    try:
        delete_from_staging = request.args.get("delete_from_staging", "false").lower() == "true"
        task = move_data_to_dwh_task.apply_async(args=[delete_from_staging])
        flask_logger.info(f"move_data_to_dwh task queued with Task ID: {task.id}")
        return jsonify({"status": "success", "task_id": task.id})
    except Exception as e:
        flask_logger.error(f"Error triggering move_data_to_dwh_task: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/get_models/<make_internal_value>', methods=['GET'])
def get_models(make_internal_value):
    """
    Gibt eine Liste von Fahrzeugmodellen zurück, die zu einer gegebenen Fahrzeugmarke gehören.
    Die Marke wird über den internen Wert (make_internal_value) identifiziert.
    """
    try:
        db = get_db()
        # Ermitteln der make_id basierend auf make_internal_value
        sql = "SELECT id FROM dwh.make WHERE make_name = %s"
        db.cursor.execute(sql, (make_internal_value,))
        result_make = db.cursor.fetchone()
        if not result_make:
            flask_logger.error(f"Make '{make_internal_value}' not found in database.")
            return jsonify({"status": "error", "message": "Fahrzeugmarke nicht in der Datenbank gefunden."}), 400
        make_id = result_make[0]

        # SQL-Abfrage, um Modelle für die gegebene make_id abzurufen
        sql = """
            SELECT model_name
            FROM dwh.model
            WHERE make_id = %s
            AND model_name <> 'sonstige'
            ORDER BY model_name
        """
        db.cursor.execute(sql, (make_id,))
        models = db.cursor.fetchall()

        # Erstellen der Liste von Modellen mit Mapping-Werten
        model_list = []
        for (model_internal,) in models:
            display_name = reverse_model_mapping.get(model_internal, model_internal.replace('_', ' ').title())
            model_list.append({"display_name": display_name, "value": model_internal})
            if model_internal not in reverse_model_mapping:
                flask_logger.warning(
                    f"No display mapping found for model '{model_internal}', using '{display_name}' as default.")

        flask_logger.info(f"Fetched {len(model_list)} models for make_internal_value='{make_internal_value}'.")
        return jsonify({"status": "success", "models": model_list})
    except Exception as e:
        flask_logger.error(f"Error fetching models for make_internal_value '{make_internal_value}': {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/import_gebrauchtwagen', methods=['GET'])  # Support only GET
# Optional: Add authentication
# @auth.login_required
def import_gebrauchtwagen():
    """
    Route to create the 'dl.Gebrauchtwagen' table and import data from a CSV file.
    Optionally, the table can be deleted before the import.

    Supports only GET requests.

    Expected GET parameters:
        - delete_before_import: true/false (optional, default: false)
        - csv_file_path: Filename of the CSV file (optional, default path is used if not provided)

    Returns:
        JSON response with status and message.
    """
    db = None  # Initialize 'db' with None
    try:
        # Extract query parameters
        delete_before_import_str = request.args.get("delete_before_import", "false").lower()
        delete_before_import = delete_before_import_str == "true"

        # Extract the filename and ensure it's safe
        csv_filename = request.args.get("csv_file_path", "gebrauchtwagen_data_122024.csv")

        # Prevent directory traversal by ensuring only the filename is provided
        if os.path.basename(csv_filename) != csv_filename:
            flask_logger.error("Invalid 'csv_file_path' parameter. Only filenames are allowed.")
            return jsonify({
                "status": "error",
                "message": "Invalid 'csv_file_path' parameter. Only filenames are allowed."
            }), 400

        # Define the directory where CSV files are located
        csv_directory = os.path.join(os.getcwd(), 'oculus', 'train_data')

        # Construct the full path to the CSV file
        csv_file_path = os.path.join(csv_directory, csv_filename)

        # Ensure the CSV directory exists
        if not os.path.isdir(csv_directory):
            flask_logger.error(f"CSV directory not found: {csv_directory}")
            return jsonify({
                "status": "error",
                "message": f"CSV directory not found: {csv_directory}"
            }), 400

        db = get_db()

        # Check if the table already exists
        check_table_query = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'dl' 
          AND table_name = 'gebrauchtwagen'
        """
        db.cursor.execute(check_table_query)
        table_exists = db.cursor.fetchone()[0] > 0

        if table_exists:
            if delete_before_import:
                # Clear the table
                db.clear_table(db, 'dl.Gebrauchtwagen')
                db.logger.info("Existing 'dl.Gebrauchtwagen' table cleared.")
            else:
                db.logger.info("'dl.Gebrauchtwagen' table already exists. Skipping creation.")
                return jsonify({
                    "status": "info",
                    "message": "'dl.Gebrauchtwagen' table already exists. Use 'delete_before_import=true' to clear it before import."
                }), 200

        # Create the table
        db.create_table_gebrauchtwagen()

        # Load the CSV data
        df = db.read_csv(csv_file_path)

        # Insert the data into the table
        db.insert_data_gebrauchtwagen(df)

        db.logger.info("Gebrauchtwagen data successfully imported.")
        return jsonify({
            "status": "success",
            "message": "Gebrauchtwagen data successfully imported."
        }), 200

    except DatabaseError as e:
        if db:
            db.logger.error(f"Database error during import: {e}")
            return jsonify({"status": "error", "message": f"Database error: {e}"}), 500
        else:
            flask_logger.error(f"Database error during import: {e}")
            return jsonify({"status": "error", "message": f"Database error: {e}"}), 500

    except FileNotFoundError as e:
        if db:
            db.logger.error(f"CSV file not found: {e}")
            return jsonify({"status": "error", "message": f"CSV file not found: {e}"}), 400
        else:
            flask_logger.error(f"CSV file not found: {e}")
            return jsonify({"status": "error", "message": f"CSV file not found: {e}"}), 400

    except Exception as e:
        if db:
            db.logger.error(f"Unexpected error during import: {e}")
        else:
            flask_logger.error(f"Unexpected error during import: {e}")
        return jsonify({"status": "error", "message": f"Unexpected error: {e}"}), 500


@app.route('/update_predicted_prices', methods=['POST', 'GET'])
def update_predicted_prices_route():
    try:
        # Starte den Celery-Task
        task = update_predicted_prices_task.apply_async()
        flask_logger.info(f"update_predicted_prices_task queued with Task ID: {task.id}")
        return jsonify({
            "status": "success",
            "task_id": task.id,
            "message": "Predicted prices update started."
        }), 202
    except Exception as e:
        flask_logger.error(f"Error queuing update_predicted_prices_task: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True,
        ssl_context=("./config/certs/fullchain.pem", "./config/certs/privkey.pem"),
    )
