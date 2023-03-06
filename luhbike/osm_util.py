from geopy import Nominatim
from geopy import Point
from typing import Dict

api = Nominatim(user_agent="luhbike")


def location_to_osm_id(lat: float, lon: float) -> Dict:
    point = Point(latitude=lat, longitude=lon)
    result = api.reverse(point)

    return result.raw["osm_id"]


if __name__ == "__main__":
    location_to_osm_id(52.54085945, 13.37914944)

