import psycopg2
import numpy as np
import pandas as pd
from io import StringIO
import json
from datetime import datetime
from tqdm import tqdm
import time
from collections import defaultdict
import itertools


conn_info = {
    'host': 'dummy',
    'dbname': 'dummy',
    'user': 'dummy',
    'password': 'dummy',
}

conn = psycopg2.connect(**conn_info)
cur = conn.cursor()


"""
SELECT ARRAY_AGG[ride_id]
FROM berlin_simra_rides_start_end_hash
GROUP BY start_hash, end_hash
ORDER BY ct DESC;
"""

"""
CREATE TABLE berlin_simra_rides_start_end (
    ride_id INT,
    start_hash CHAR(9),
    end_hash CHAR(9)
);
"""


"""
INSERT INTO berlin_simra_rides_start_end_hash
SELECT start_points.ride_id, start_hash, end_hash FROM 
    (
        -- ride id and start location
        SELECT DISTINCT ON (ride_id) ride_id, ST_GeoHash(ST_SetSRID(ST_MakePoint(lon, lat), 4326),9) as start_hash
        FROM berlin_simra_rides
        WHERE (ride_id, timestamp) IN (
            SELECT ride_id, MIN(timestamp)
            FROM berlin_simra_rides
            WHERE lat >= -90 AND lat <= 90 AND lon >= -180 AND lon <= 180 AND timestamp IS NOT NULL
            GROUP BY ride_id
        )
    ) as start_points
INNER JOIN
    (
        -- ride id and end location
        SELECT DISTINCT ON (ride_id) ride_id, ST_GeoHash(ST_SetSRID(ST_MakePoint(lon, lat), 4326),9) as end_hash
        FROM berlin_simra_rides
        WHERE (ride_id, timestamp) IN (
            SELECT ride_id, MIN(timestamp)
            FROM berlin_simra_rides
            WHERE lat >= -90 AND lat <= 90 AND lon >= -180 AND lon <= 180 AND timestamp IS NOT NULL
            GROUP BY ride_id
        )
    ) as end_points
ON start_points.ride_id = end_points.ride_id;
"""

"""
CREATE TABLE berlin_simra_rides_groups (
    ride_id INT,
    group_id INT,
    group_size INT 
);
"""

def create_groups_table():
    START_END_RELATION = "berlin_simra_rides_start_end_hash"
    GROUPS_RELATION = "berlin_simra_rides_groups"

    ride_groups_query = f"SELECT ARRAY_AGG(ride_id) \n" \
                        f"FROM {START_END_RELATION} \n" \
                        f"GROUP BY start_hash, end_hash;"

    print("### Fetching groups of rides. ###")
    print()
    print("---------------------------------")
    print(ride_groups_query)
    print("---------------------------------")
    print()

    cur.execute(ride_groups_query)
    groups = cur.fetchall()
    print(f"### Done. Found {len(groups)} groups. ###")

    buff = StringIO()
    for group_id, group in tqdm(enumerate(groups)):

        ride_ids = group[0]
        group_size = len(ride_ids)

        for ride_id in ride_ids:
            buff.write(f"{ride_id}\t{group_id}\t{group_size}")
            buff.write(f"\n")   

    buff.seek(0)
    cur.copy_from(buff, TARGET_RELATION, sep='\t', null='\\N')
    cur.execute("COMMIT")


"""
SELECT sim.ride_id, group_id, osm_ids
FROM (
    SELECT ride_id, ARRAY_AGG(DISTINCT osm_id) as osm_ids
    FROM berlin_simra_rides_4k
    GROUP BY ride_id
) as sim INNER JOIN berlin_simra_groups as g
ON (sim.ride_id = g.ride_id)
WHERE group_size > 1
ORDER BY sim.ride_id, group_id
"""

"""
CREATE TABLE berlin_simra_rides_scores (
    ride_id INT,
    group_id INT,
    score DOUBLE PRECISION
);
"""

def score_rides():
    scores_table = "berlin_simra_rides_scores"

    query = f"SELECT sim.ride_id, group_id, osm_ids " \
            f"FROM ( " \
            f"   SELECT ride_id, ARRAY_AGG(DISTINCT osm_id) as osm_ids " \
            f"   FROM berlin_simra_rides " \
            f"   GROUP BY ride_id " \
            f") as sim JOIN berlin_simra_rides_groups as g " \
            f"ON (sim.ride_id = g.ride_id) " \
            f"ORDER BY group_id" \
    
    cur.execute(query)

    groups = defaultdict(list)          # group_id -> List[ride_ids]
    osm_ids_group = defaultdict(list)   # group_id -> List[osm_ids]
    osm_ids_ride = {}                        # ride_id -> List[osm_ids]

    result = cur.fetchall()

    for row in result:
        ride_id, group_id, osm_ids = row

        groups[group_id] += [ride_id]
        osm_ids_group[group_id] += osm_ids
        osm_ids_ride[ride_id] = set(osm_ids)
   
    for group_id in tqdm(groups.keys()):
        buff = StringIO()
        this_group_osm_ids = np.array(osm_ids_group[group_id])
        osm_value_counts = np.unique(this_group_osm_ids, return_counts=True)

        osm_id_counts = {}
        for osm_id, count in zip(osm_value_counts[0], osm_value_counts[1]):
            osm_id_counts[osm_id] = count

        ride_ids = []
        ride_scores = []
        for ride_id in groups[group_id]:
            ride_score = 0
            for osm_id in osm_ids_ride[ride_id]:
                ride_score += osm_id_counts[osm_id]
            ride_ids += [ride_id]
            ride_scores += [osm_id]
        
        # normalize scores
        ride_scores = np.array(ride_scores, dtype=np.float64)
        ride_scores = ride_scores / np.sum(ride_scores)

        for ride_id, score in zip(ride_ids, ride_scores):
            buff.write(f"{ride_id}\t{group_id}\t{score}")
            buff.write(f"\n")   

        buff.seek(0)
        cur.copy_from(buff, scores_table, sep='\t', null='\\N')
        cur.execute("COMMIT")


if __name__ == "__main__":
    score_rides()



