from dataclasses import dataclass
from datetime import date
from json import loads
from psycopg2 import connect
from requests import get

# 4SQ API Keys
_CLIENT_ID = ""
_CLIENT_SECRET = ""


_CITIES = {
    "Bar": (42.0912, 19.0899),
    "Ulcinj": (41.9311, 19.2148),
    "Petrovac": (42.2053, 18.9458),
    "Budva": (42.2911, 18.8403),
    "Tivat": (42.4350, 18.7066),
    "Kotor": (42.4247, 18.7712),
    "Herceg Novi": (42.4572, 18.5315),
    "Podgorica": (42.4304, 19.2594),
    "Kolasin": (42.8205, 19.5241),
    "Cetinje": (42.3931, 18.9116),
    "Niksic": (42.7805, 18.9562),
    "Zabljak": (43.1555, 19.1226),
    "Danilovgrad": (42.5538, 19.1077),
    "Pljevlja": (43.3582, 19.3513),
    "Bijelo Polje": (43.0369, 19.7562),
}


@dataclass
class Venue:
    id: str
    name: str
    lat: float
    lng: float


def get_page(lat: float, lng: float) -> [Venue]:
    url = 'https://api.foursquare.com/v2/venues/search'
    params = {
        "client_id": _CLIENT_ID,
        "client_secret": _CLIENT_SECRET,
        "ll": f"{lat},{lng}",
        "radius": 20000,
        "categoryId": "4bf58dd8d48988d1ca941735",
        "limit": 50,
        "v": date.today().strftime("%Y%m%d")
    }

    resp = get(url=url, params=params)
    data = loads(resp.text)
    result = []
    for venue in data.get("response", []).get("venues", []):
        id = venue.get("id")
        name = venue.get("name")
        lat = venue.get("location", {}).get("lat")
        lng = venue.get("location", {}).get("lng")
        if any(value is None for value in (id, name, lat, lng)):
            continue

        result.append(Venue(id, name, lat, lng))

    return result


def _scrap_pizzerias(connection):
    # INSERT INTO pizzerias(name, point) VALUES(%s, ll_to_earth(%s,%s));
    query = "INSERT INTO pizzerias(name, lat, lng) VALUES(%s, %s,%s);"
    filtered = {}
    for city, (lat, lng) in _CITIES.items():
        data = get_page(lat, lng)
        print(city, len(data))
        for rec in data:
            filtered[rec.id] = rec

    no_dupes = filtered.values()

    print(f"Saving {len(no_dupes)} items")
    with connection.cursor() as cursor:
        for item in no_dupes:
            cursor.execute(query, (item.name, item.lat, item.lng))
        else:
            connection.commit()


def _add_cities(connection):
    query = "INSERT INTO cities_min(name, lat, lng) VALUES(%s, %s, %s);"
    with connection.cursor() as cursor:
        for name, (lat, lng) in _CITIES.items():
            cursor.execute(query, (name, lat, lng))
        else:
            connection.commit()


def _main():
    conn = connect(host='localhost', user='', password='', database='geotest')
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE pizzerias;")
    cursor.execute("TRUNCATE TABLE cities_min")
    conn.commit()

    print("Adding cities")
    _add_cities(conn)
    print("Scrapping pizzerias")
    _scrap_pizzerias(conn)


if __name__ == "__main__":
    _main()
