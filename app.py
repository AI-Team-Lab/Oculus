from flask import Flask, render_template, request, jsonify, g
from oculus import Willhaben, Database
from rich import print

# Initialize Flask and other components
app = Flask(__name__)
willhaben = Willhaben()

# Initialize and connect to the database
db_init = Database()
db_init.connect()


def get_db():
    if 'db' not in g:
        g.db = Database()
        g.db.connect()
    return g.db


@app.teardown_appcontext
def close_db(error):
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
                    print(car_info["id"])
                    results.append(car_info)
        except Exception as e:
            print(f"[red]Error fetching cars: {e}[/red]")
            return render_template("index.html", results=[], error=str(e))

    return render_template("index.html", results=results)


@app.route("/fetch_cars", methods=["GET", "POST"])
def fetch_cars():
    """
    Fetches cars based on the provided car model make and saves the data to the database or CSV.
    """

    db = get_db()
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
                db_instance=db,  # Übergib die gesamte Datenbank-Instanz
                table_name="dbo.willhaben"
            )
        else:
            print("Processing all cars.")
            result = willhaben.process_cars(
                save_type="db",
                db_instance=db,  # Übergib die gesamte Datenbank-Instanz
                table_name="dbo.willhaben"
            )

        return jsonify(result)
    except Exception as e:
        print(f"[red]Error processing cars: {e}[/red]")
        return jsonify({"status": "error", "message": str(e)}), 500



if __name__ == "__main__":
    app.run(debug=True)
