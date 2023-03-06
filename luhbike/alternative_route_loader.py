import psycopg2
import numpy as np
import pandas as pd
from io import StringIO
import requests
import json
from datetime import datetime
from tqdm import tqdm
import time

ORS_KEYS = [
    "dummy"
]

conn_info = {
    'host': 'dummy',
    'dbname': 'dummy',
    'user': 'dummy',
    'password': 'dummy',
}

"""
CREATE TABLE berlin_alt_routes_4k (
    route_id INT NOT NULL,
    ride_id INT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    timestamp BIGINT,
    geo_point geometry(POINT,3857),
    year SMALLINT,
    month SMALLINT,
    day SMALLINT,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT,
    osm_id BIGINT
);
"""

SOURCE_RELATION = "berlin_simra_rides_4k"
RELATION = "berlin_alt_routes_4k"

BASE_ALTERNATIVE_RIDE_ID = 1000000

conn = psycopg2.connect(**conn_info)
cur = conn.cursor()

routes_query = f"SELECT ride_id, timestamp, lat, lon " \
               f"FROM {SOURCE_RELATION} " \
               f"WHERE (ride_id, timestamp) IN (" \
               f"   SELECT ride_id, MIN(timestamp) " \
               f"   FROM {SOURCE_RELATION} " \
               f"   WHERE lat IS NOT NULL AND lon IS NOT NULL AND timestamp IS NOT NULL " \
               f"   GROUP BY ride_id " \
               f"UNION " \
               f"   SELECT ride_id, MAX(timestamp) " \
               f"   FROM {SOURCE_RELATION} " \
               f"   WHERE lat IS NOT NULL AND lon IS NOT NULL AND timestamp IS NOT NULL " \
               f"   GROUP BY ride_id " \
               f") " \
               f"ORDER BY ride_id, timestamp"

print("### Fetching start and stop locations for all simra rides. ###")
cur.execute(routes_query)
rides = cur.fetchall()
print(f"### Done. ###")

route_id = 0
for i in tqdm(range(len(rides) - 1)):
    start, stop = rides[i], rides[i + 1]

    if start[0] != stop[0]:
        # different ride IDs
        continue

    ride_id = BASE_ALTERNATIVE_RIDE_ID + int(start[0])

    url = f"https://api.openrouteservice.org/v2/directions/cycling-regular"
    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
    }

    status = 429   # assuming exceeded limit
    cooldown = 0.5   # initial cooldown in sec
    while True:
        params = {
            "api_key": np.random.choice(ORS_KEYS),
            "start": f"{start[3]}, {start[2]}",
            "end": f"{stop[3]}, {stop[2]}",
            "profile": "cycling-regular"
        }

        call = requests.get(url, params=params, headers=headers)
        status = call.status_code

        if status != 429:
            break
        
        # exceeded limit
        time.sleep(cooldown)

    if status != 200:
        print(f"Invalid status code ({status}).")
        continue

    try:
        route = json.loads(call.text)
        coordinates = route["features"][0]["geometry"]["coordinates"]
    except:
        continue

    buff = StringIO()
    for coordinate, timestamp in zip(coordinates, np.linspace(start[1], stop[1], len(coordinates))):
        lon, lat = coordinate

        buff.write(f"{route_id}\t{ride_id}\t{lat}\t{lon}\t{int(timestamp)}\t\\N\t")

        dt = datetime.fromtimestamp(timestamp / 1000.)
        buff.write(f"{dt.year}\t{dt.month}\t{dt.day}\t{dt.hour}\t{dt.minute}\t{dt.second}")
        buff.write(f"\t\\N\n")     # empty osm id

    # write alternative route to DB
    buff.seek(0)
    cur.copy_from(buff, RELATION, sep='\t', null='\\N')
    cur.execute("COMMIT")
    route_id += 1



