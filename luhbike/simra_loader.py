import os
import sys

sys.path.insert(0, ".")

from pathlib import Path

import numpy as np
import pandas as pd

from luhbike.osm_util import location_to_osm_id

from tqdm import tqdm
from io import StringIO
from datetime import datetime
from typing import Dict, Tuple
from psycopg2 import connect


UPLOAD_INTERVAL = 10
ADD_OSM_ID = False

path = "dummy"

# local debugging
if "dummy" in os.getcwd():
    path = "dummy"

rides_directory = Path(path).resolve()

conn_info = {
    'host': 'dummy',
    'dbname': 'dummy',
    'user': 'dummy',
    'password': 'dummy',
}

INCIDENT_INT_COLS = ["key", "timeStamp", "bike", "pLoc", "incident"]
RIDE_INT_COLS = ["timeStamp"]


FILE_DELIMITER = "======"
RIDE_COLS = ["lat", "lon", "X", "Y", "Z", "timeStamp", "acc", "a", "b", "c", "obsDistanceLeft1",
             "obsDistanceLeft2", "obsDistanceRight1", "obsDistanceRight2", "obsClosePassEvent",
             "XL", "YL", "ZL", "RX", "RY", "RZ", "RC"]

INCIDENT_COLS = ["key","lat","lon","timeStamp","bike","childCheckBox","trailerCheckBox","pLoc",
                 "incident","i1","i2","i3","i4","i5","i6","i7","i8","i9","scary","desc","i10"]

"""
UPDATE berlin_simra_incidents
SET osm_id = (
    SELECT subq.osm_id from (
        SELECT osm_id, way <-> berlin_simra_incidents.geo_point as dist 
        FROM berlin_osm_roads
        ORDER BY dist LIMIT 1
    ) as subq
);

UPDATE berlin_simra_rides
SET osm_id = (
    SELECT subq.osm_id from (
        SELECT osm_id, way <-> berlin_simra_rides.geo_point as dist 
        FROM berlin_osm_roads
        ORDER BY dist LIMIT 1
    ) as subq
);
"""

"""
CREATE TABLE btw_dsc_simra_incidents (
    ride_id INT NOT NULL,
    key INT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    ts BIGINT,
    bike SMALLINT,
    childCheckBox BOOLEAN,
    trailerCheckBox BOOLEAN,
    pLoc SMALLINT,
    incident SMALLINT,
    i1 BOOLEAN,
    i2 BOOLEAN,
    i3 BOOLEAN,
    i4 BOOLEAN,
    i5 BOOLEAN,
    i6 BOOLEAN,
    i7 BOOLEAN,
    i8 BOOLEAN,
    i9 BOOLEAN,
    scary BOOLEAN,
    description TEXT,
    i10 BOOLEAN,
    year SMALLINT,
    month SMALLINT,
    day SMALLINT,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT,
    osm_id BIGINT
);

CREATE TABLE btw_dsc_simra_rides (
    ride_id INT NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    X DOUBLE PRECISION,
    Y DOUBLE PRECISION,
    Z DOUBLE PRECISION,
    timeStamp BIGINT,
    acc DOUBLE PRECISION,
    a DOUBLE PRECISION,
    b DOUBLE PRECISION,
    c DOUBLE PRECISION,
    obsDistanceLeft1 DOUBLE PRECISION,
    obsDistanceLeft2 DOUBLE PRECISION,
    obsDistanceRight1 DOUBLE PRECISION,
    obsDistanceRight2 DOUBLE PRECISION,
    obsClosePassEvent BOOLEAN,
    XL DOUBLE PRECISION,
    YL DOUBLE PRECISION,
    ZL DOUBLE PRECISION,
    RX DOUBLE PRECISION,
    RY DOUBLE PRECISION,
    RZ DOUBLE PRECISION,
    RC DOUBLE PRECISION,
    year SMALLINT,
    month SMALLINT,
    day SMALLINT,
    hour SMALLINT,
    minute SMALLINT,
    second SMALLINT,
    osm_id BIGINT
);
"""


class SimraLoader:
    def __init__(
            self,
            rides_path: str,
            db_config: Dict[str, str],
            incident_table: str,
            ride_table: str
    ):
        self.__rides_directory = Path(rides_path).resolve()
        self.__conn = connect(**db_config)
        self.__cur = self.__conn.cursor()

        self.__incident_table = incident_table
        self.__ride_table = ride_table

        self.__incident_buff = None
        self.__ride_buff = None

        self.__ride_id = 0
        try:
            self.__cur.execute(f"SELECT MAX({self.__ride_table}.ride_int) FROM {self.__ride_table}")
            self.__ride_id = int(self.__cur.fetchall()[0][0]) + 1
        except Exception as e:
            print("Unable to fetch ride_id, falling back to 0.")

        print("### INIT DONE ###")

    @staticmethod
    def parse_simra_dataset(path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Reads a simRa dataset from file.

        Parameters
        ----------
        path : str
            Filepath.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            Incidents and Ride data frame.
        """

        delimiter_row = -1
        with open(path, 'r') as file:
            lines = file.readlines()
            for i, line in enumerate(lines):
                if FILE_DELIMITER in line:
                    delimiter_row = i + 1
                    break

        if delimiter_row < 0:
            raise IOError(f"Invalid dataset: {path}")

        nrows_incidents = delimiter_row - 4  # number of rows in incidents block
        skiprows_ride = delimiter_row + 1  # number of rows to skip before ride block starts

        ride = pd.read_csv(path, skiprows=skiprows_ride)

        for col in RIDE_COLS:
            if col not in ride.columns:
                ride[col] = np.nan

        incidents = pd.read_csv(path, skiprows=1, nrows=nrows_incidents)
        incidents = incidents.rename(columns={"ts": "timeStamp"})

        for col in INCIDENT_COLS:
            if col not in incidents.columns:
                incidents[col] = np.nan

        return incidents, ride

    @staticmethod
    def add_df_to_buffer(buff: StringIO, df: pd.DataFrame, ride_id: int) -> None:
        for _, row in df.iterrows():
            row_vals = [str(ride_id)] + [str(val).replace("\t", " ").replace("\n", " ")
                                         for val in row.to_list()]

            # SimRa attributes
            values = "\t".join(row_vals) + "\t"
            values = values.replace("nan", "\\N").replace(".0\t", "\t")
            buff.write(values)

            # Custom time format
            if not np.isnan(row["timeStamp"]):
                dt = datetime.fromtimestamp(row["timeStamp"] / 1000.)
                buff.write(f"{dt.year}\t{dt.month}\t{dt.day}\t{dt.hour}\t{dt.minute}\t{dt.second}")
            else:
                buff.write(f"\\N\t\\N\t\\N\t\\N\t\\N\t\\N")

            # OSM ID for location
            osm_id = "\\N"
            #lat, lon = row["lat"], row["lon"]
            #if ADD_OSM_ID and not np.isnan(lat) and not np.isnan(lon):
            #    osm_id = location_to_osm_id(lat, lon)
            buff.write(f"\t{osm_id}")

            buff.write("\n")

    def reset(self):
        self.__incident_buff = StringIO()
        self.__ride_buff = StringIO()

    def upload(self):
        self.__incident_buff.seek(0)
        self.__cur.copy_from(self.__incident_buff, self.__incident_table, sep='\t', null='\\N')

        self.__ride_buff.seek(0)
        self.__cur.copy_from(self.__ride_buff, self.__ride_table, sep='\t', null='\\N')
        self.__cur.execute("COMMIT")

        self.reset()

    def run(self) -> None:
        self.__incident_buff = StringIO()
        self.__ride_buff = StringIO()
        errors = 0

        files = os.listdir(rides_directory)
        for ride_id, filename in tqdm(enumerate(files), total=len(files)):
            try:
                path = os.path.join(rides_directory, filename)
                incident, ride = self.parse_simra_dataset(str(path))

                if not incident.empty:
                    self.add_df_to_buffer(self.__incident_buff, incident, self.__ride_id)

                self.add_df_to_buffer(self.__ride_buff, ride, self.__ride_id)
                self.__ride_id += 1

            except Exception as _:
                errors += 1
                pass

            if self.__ride_id % UPLOAD_INTERVAL == 0:
                try:
                    self.upload()
                except Exception as e:
                    print(e)
                    self.reset()
                    errors += 1

        self.__conn.close()

        print("### FINISHED ###")
        print(f"Total errors = {errors}")


if __name__ == "__main__":
    loader = SimraLoader(path, conn_info, "btw_dsc_simra_incidents", "btw_dsc_simra_rides")
    loader.run()

