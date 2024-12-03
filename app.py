from rich import print
from flask import Flask, render_template, request, redirect, url_for
from oculus.willhaben import WillHaben

willhaben = WillHaben()
app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    keyword = request.args.get("keyword", None)
    results = None

    car_make_ids = ["bmw"]

    for car_make_id in car_make_ids:
        print(f"Searching vehicles for CAR_MODEL/MAKE ID: {car_make_id}")
        willhaben.process_cars(car_make_id)

    if keyword:
        response = willhaben.search_car(keyword=keyword)
        # response = willhaben.search_car(
        #     car_model_make="ferrari",
        #         car_model_model="x1",
        #         engine_effect_from="300hp",
        #         car_type="suv",
        #         motor_condition="used_car",
        #         warranty="yes",
        #         engine_fuel="petrol",
        #         transmission="automatic",
        #         wheel_drive="all_wheel",
        #         equipment=["onboard_computer"],
        #         exterior_colour_main="blue",
        #         area_id="kaernten"
        # )

        if response and "advertSummaryList" in response and "advertSummary" in response["advertSummaryList"]:
            results = [
                {
                    "title": ad.get("description"),
                    "price": next(
                        (attr["values"][0] for attr in ad.get("attributes", {}).get("attribute", [])
                         if attr["name"] == "PRICE_FOR_DISPLAY"), "Preis nicht verfügbar"
                    ),
                    "location": next(
                        (attr["values"][0] for attr in ad.get("attributes", {}).get("attribute", [])
                         if attr["name"] == "LOCATION"), "Ort nicht verfügbar"
                    ),
                    "image": next(
                        (attr["values"][0] for attr in ad.get("attributes", {}).get("attribute", [])
                         if attr["name"] == "MMO"), None
                    ),
                    "link": willhaben.base_url + "/iad/object?adId=" + ad.get("id", ""),
                }
                for ad in response["advertSummaryList"]["advertSummary"]
            ]

    return render_template("index.html", results=results)


if __name__ == '__main__':
    app.run(debug=True)
