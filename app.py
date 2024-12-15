import os
import json
from flask import Flask, render_template, request, jsonify, g
from oculus import Willhaben, Database, fetch_cars_task, move_data_to_dwh_task
from oculus.logging import flask_logger
from celery.result import AsyncResult

# Initialize Flask
app = Flask(__name__)

# Initialize Willhaben
willhaben = Willhaben()


# Laden der Mapping-Datei
def load_mappings():
    mapping_path = os.path.join(os.getcwd(), 'oculus', 'mapping', 'willhaben_mapping.json')
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            mappings = json.load(f)
        app.logger.info(f"Successfully loaded mappings from {mapping_path}")
        return mappings
    except FileNotFoundError:
        app.logger.error(f"Mapping file {mapping_path} not found.")
        return {}
    except json.JSONDecodeError as e:
        app.logger.error(f"Error decoding JSON from {mapping_path}: {e}")
        return {}
    except Exception as e:
        app.logger.error(f"Unexpected error loading mappings: {e}")
        return {}


# Lade die Mappings beim Start der App
mappings = load_mappings()


def get_db():
    """
    Provides a database connection for the current app context.
    """
    if 'db' not in g:
        g.db = Database()
        g.db.connect()
    return g.db


@app.teardown_appcontext
def close_resources(error):
    """
    Cleans up resources at the end of the app context.
    """
    db = g.pop('db', None)
    if db is not None:
        db.close()


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

        # Erweiterte Suche: make_name, model_name, specification
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
            mapped_make_name = reverse_make_mapping.get(make_name.lower(), make_name)

            mapped_model = mappings.get('willhaben_model_mapping', {})
            reverse_model_mapping = {v.lower(): k for k, v in mapped_model.items()}
            mapped_model_name = reverse_model_mapping.get(model_name.lower(), model_name)

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

        return render_template('index.html', cars=cars, query=query, page=page)
    return render_template('index.html', cars=None, query=None, page=None)


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
                app.logger.error("Invalid JSON payload received in POST request.")
                return jsonify({"status": "error", "message": "Invalid JSON payload."}), 400
            car_model_make = data.get("car_model_make", None)
            start_make = data.get("start_make", None)

        # Validate car_model_make
        if car_model_make and car_model_make.lower() not in willhaben.car_data:
            error_message = f"Invalid CAR_MODEL/MAKE: '{car_model_make}'. Please provide a valid car make."
            app.logger.error(error_message)
            return jsonify({"status": "error", "message": error_message}), 400

        # Validate start_make
        if start_make and start_make.lower() not in willhaben.car_data:
            error_message = f"Invalid START_MAKE: '{start_make}'. Please provide a valid start make."
            app.logger.error(error_message)
            return jsonify({"status": "error", "message": error_message}), 400

        # Log task queuing information
        if car_model_make:
            app.logger.info(f"Queuing task for CAR_MODEL/MAKE: {car_model_make}")
        elif start_make:
            app.logger.info(f"Queuing task for all cars starting from make: {start_make}")
        else:
            app.logger.info("Queuing task for all cars.")

        # Pass car_model_make or start_make to the Celery task
        task = fetch_cars_task.apply_async(args=[car_model_make, start_make])

        # Log successful task queuing
        app.logger.info(f"Task queued successfully with Task ID: {task.id}")
        return jsonify({"status": "success", "task_id": task.id})

    except Exception as e:
        # Log unexpected errors
        app.logger.error(f"Error queuing task: {e}")
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
            app.logger.info(f"Task {task_id} is pending.")
        elif task_result.state == "STARTED":
            app.logger.info(f"Task {task_id} has started.")
        elif task_result.state == "SUCCESS":
            app.logger.info(f"Task {task_id} completed successfully.")
        elif task_result.state == "FAILURE":
            app.logger.error(f"Task {task_id} failed with error: {task_result.info}")

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
        app.logger.error(f"Error while retrieving status for task {task_id}: {e}")
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
            return jsonify({"status": "error", "message": "The 'entity' parameter is missing."}), 400

        filename = request.args.get('file_path', f'{entity}.json')
        file_path = os.path.join(base_path, filename)

        # Check if the file exists
        if not os.path.isfile(file_path):
            return jsonify({"status": "error", "message": f"File {file_path} not found."}), 400

        # Database instance
        db = Database()
        db.connect()

        # Define a mapping between entities and their respective tables in sync_log
        table_mapping = {
            "car_data": ["dl.make", "dl.model"],
            "car_engine": ["dl.engine_effect", "dl.engine_fuel", "dl.battery_capacity", "dl.wltp_range",
                           "dl.transmission", "dl.wheel_drive",
                           ],

            "car_equipment": ["dl.equipment_search", "dl.exterior_colour_main", "dl.no_of_doors", "dl.no_of_seats"],
            "car_location": ["dl.area", "dl.location", "dl.dealer", "dl.periode"],
            "car_status": ["dl.car_type", "dl.motor_condition", "dl.warranty"]
        }

        # Delete entries from `dwh.sync_log` for the corresponding tables
        if entity in table_mapping:
            for table_name in table_mapping[entity]:
                db.cursor.execute("DELETE FROM dwh.sync_log WHERE table_name = %s", (table_name,))
                db.logger.info(f"Deleted sync_log entry for table '{table_name}'.")

        # Call the entity-specific function
        try:
            if entity == "car_data":
                db.load_car_data(file_path)
                db.logger.info(f"'car_data' loaded successfully from {file_path}")
            elif entity == "car_engine":
                db.load_car_engine(file_path)
                db.logger.info(f"'car_engine' loaded successfully from {file_path}")
            elif entity == "car_equipment":
                db.load_car_equipment(file_path)
                db.logger.info(f"'car_equipment' loaded successfully from {file_path}")
            elif entity == "car_location":
                db.load_car_location(file_path)
                db.logger.info(f"'car_location' loaded successfully from {file_path}")
            elif entity == "car_status":
                db.load_car_status(file_path)
                db.logger.info(f"'car_status' loaded successfully from {file_path}")
            else:
                db.logger.error(f"Unknown entity '{entity}'")
                return jsonify({"status": "error", "message": f"Unknown entity '{entity}'."}), 400
        finally:
            db.close()

        return jsonify({"status": "success", "message": f"Data for '{entity}' successfully loaded from {file_path}."})
    except Exception as e:
        db.logger.error(f"Error while loading data for '{entity}': {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/move_data_to_dwh", methods=["GET"])
def move_data_to_dwh():
    """
    Triggers a Celery task to move data from staging to the Data Warehouse.
    """
    try:
        delete_from_staging = request.args.get("delete_from_staging", "false").lower() == "true"
        task = move_data_to_dwh_task.apply_async(args=[delete_from_staging])
        return jsonify({"status": "success", "task_id": task.id})
    except Exception as e:
        flask_logger.error(f"Error triggering move_data_to_dwh_task: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True,
        ssl_context=("./certs/fullchain.pem", "./certs/privkey.pem"),
    )
