import os
from flask import Flask, render_template, request, jsonify, g
from oculus import Willhaben, Database, fetch_cars_task, move_data_to_dwh_task
from oculus.logging import flask_logger
from celery.result import AsyncResult

# Initialize Flask
app = Flask(__name__)

# Initialize Willhaben
willhaben = Willhaben()


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


@app.route("/", methods=["GET"])
def index():
    """
    Renders the homepage with an optional search query for car listings.
    """
    keyword = request.args.get("keyword", None)
    results = []

    if keyword:
        try:
            response = willhaben.search_car(keyword=keyword, rows=30)

            if response and "advertSummaryList" in response and "advertSummary" in response["advertSummaryList"]:
                for ad in response["advertSummaryList"]["advertSummary"]:
                    car_info = willhaben.extract_car_info(ad)
                    flask_logger.info(f"Fetching car with ID: {car_info['id']}")
                    results.append(car_info)
        except Exception as e:
            flask_logger.error(f"Error fetching cars: {e}")
            return render_template("index.html", results=[], error=str(e))

    return render_template("index.html", results=results)


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
