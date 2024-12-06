from flask import Flask, render_template, request, jsonify, g
from oculus import Willhaben, Database
from oculus.tasks import fetch_cars_task
from rich import print

# Initialize Flask
app = Flask(__name__)

# Initialize Willhaben
willhaben = Willhaben()


def get_db():
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
                    print(f"Fetching car with ID: {car_info['id']}")
                    results.append(car_info)
        except Exception as e:
            print(f"[red]Error fetching cars: {e}[/red]")
            return render_template("index.html", results=[], error=str(e))

    return render_template("index.html", results=results)


@app.route("/fetch_cars", methods=["GET", "POST"])
def fetch_cars():
    """
    Initiates a Celery task to fetch cars.
    Supports both GET and POST methods.
    """
    try:
        car_model_make = None

        # Handling GET request
        if request.method == "GET":
            car_model_make = request.args.get("car_model_make", None)

        # Handling POST request
        elif request.method == "POST":
            data = request.get_json()
            if not data:
                return jsonify({"status": "error", "message": "Invalid JSON payload."}), 400
            car_model_make = data.get("car_model_make", None)

        if car_model_make:
            print(f"[blue]Queuing task for CAR_MODEL/MAKE: {car_model_make}[/blue]")
        else:
            print("[blue]Queuing task for all cars.[/blue]")

        # Trigger Celery Task
        task = fetch_cars_task.apply_async(args=[car_model_make])
        return jsonify({"status": "success", "task_id": task.id})

    except Exception as e:
        print(f"[red]Error queuing task: {e}[/red]")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/task_status/<task_id>", methods=["GET"])
def task_status(task_id):
    """
    Returns the status of a Celery task.
    """
    from celery.result import AsyncResult

    task_result = AsyncResult(task_id)
    response = {
        "task_id": task_id,
        "status": task_result.status,
        "result": task_result.result
    }
    return jsonify(response)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, ssl_context=("./certs/fullchain.pem", "./certs/privkey.pem"))
