import requests


class WillHaben:
    def __init__(self, base_url: str = "https://www.willhaben.at"):
        self.base_url = base_url

        self.headers = {
            "Language": "de-DE,de;q=0.9",
            "accept": "application/json",
            "X-Wh-Client": "api@willhaben.at;responsive_web;server;1.0.0;desktop"
        }

    def get_response(self, endpoint: str, params: dict = None):
        try:
            url = self.base_url + endpoint
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTPError: {http_err}")
        except Exception as e:
            print(f"Error: {e}")

    def search_car(self, keyword: str, page: int = 1, rows=30, sort=1):
        params = {
            "keyword": keyword,
            "page": page,
            "rows": rows,
            "sort": sort
        }
        return self.get_response("/webapi/iad/search/atz/seo/gebrauchtwagen/auto/gebrauchtwagenboerse", params)
