from flask import Flask, render_template, request, jsonify, redirect, url_for
from oculus.willhaben import WillHaben
from rich import print

willhaben = WillHaben()
app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    keyword = request.args.get("keyword", None)
    results = []

    if keyword:
        response = willhaben.search_car(keyword=keyword, rows=30)

        if response and "advertSummaryList" in response and "advertSummary" in response["advertSummaryList"]:
            for ad in response["advertSummaryList"]["advertSummary"]:
                car_info = willhaben.extract_car_info(ad)
                print(car_info)
                results.append(car_info)

    return render_template("index.html", results=results)


@app.route("/fetch_cars", methods=["GET"])
def fetch_cars():
    car_model_make = request.args.get("car_model_make", None)

    if car_model_make:
        print(f"Processing cars for car_model_make: {car_model_make}")
        result = willhaben.process_cars(car_model_make=car_model_make)
    else:
        print("Processing all cars.")
        result = willhaben.process_cars()

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)
