from flask import Flask, render_template, request, jsonify
from oculus import WillHaben, Database
from rich import print

# Initialize Flask and other components
app = Flask(__name__)
willhaben = WillHaben()

# Initialize and connect to the database
db = Database()
try:
    db.connect()
except Exception as e:
    print(f"[red]Error connecting to the database: {e}[/red]")
    db = None


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
                    print(car_info["id"])
                    results.append(car_info)
        except Exception as e:
            print(f"[red]Error fetching cars: {e}[/red]")
            return render_template("index.html", results=[], error=str(e))

    return render_template("index.html", results=results)


@app.route("/fetch_cars", methods=["POST"])
@app.route("/fetch_cars", methods=["GET", "POST"])
def fetch_cars():
    """
    Fetches cars based on the provided car model make and saves the data to the database or CSV.
    """
    valid_car_makes = willhaben.car_data.keys()

    try:
        if request.method == "GET":
            # Get car_model_make from query parameters
            car_model_make = request.args.get("car_model_make", None)
        elif request.method == "POST":
            # Get car_model_make from JSON body
            car_model_make = request.json.get("car_model_make", None)

        if car_model_make:
            if car_model_make.lower() not in valid_car_makes:
                return jsonify({
                    "status": "error",
                    "message": f"Invalid car make: '{car_model_make}'. Valid car makes are: {', '.join(valid_car_makes)}."
                }), 400

            print(f"Processing cars for CAR_MODEL/MAKE: {car_model_make}")
            result = willhaben.process_cars(
                car_model_make=car_model_make,
                save_type="db",
                db_connection=db.conn,
                table_name="willhaben"
            )
        else:
            print("Processing all cars.")
            result = willhaben.process_cars(
                save_type="db",
                db_connection=db.conn,
                table_name="willhaben"
            )

        return jsonify(result)
    except Exception as e:
        print(f"[red]Error processing cars: {e}[/red]")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.teardown_appcontext
def close_db(exception):
    """
    Closes the database connection when the app context ends.
    """
    if db:
        db.close()


if __name__ == "__main__":
    app.run(debug=True)
