import sys
# import psycopg2
import pandas as pd
import os
import json

sys.path.insert(0, ".")

__BASE__ = 'dataset'
__DIR__ = 'weather'
# __STR__ = 'Radverkehrsanlagen'
# __DATA__ = 'Radverkehrsanlagen.geojson'

# __SUBDIR__ = ['hourly_airtemp_humid_hist', 'hourly_groundtemp_hist', 'hourly_rainfall_hist', 'hourly_sunshine_hist', 'hourly_wind_hist']
# __DATA__ = 'dataset.txt'
__DATA__ = ['airtemp_humidity.txt', 'groundtemp.txt', 'rainfall.txt', 'sunshine.txt', 'wind.txt']

__SUBSTITUT_DICT__ = {
    'hourly_airtemp_humid_hist': {
        'TT_TU': 'airtemp',
        'RF_TU': 'rel_humid'
    },
    'hourly_groundtemp_hist':{
        'V_TE002': 'temp_2cm',
        'V_TE005': 'temp_5cm',
        'V_TE010': 'temp_10cm',
        'V_TE020': 'temp_20cm',
        'V_TE050': 'temp_50cm',
        'V_TE100': 'temp_100cm'
    },
    'hourly_rainfall_hist': {
        '  R1': 'precipitation_1h',
        'RS_IND': 'precipitation_indicator',
        'WRTR': 'precipitation_type'
    },
    'hourly_sunshine_hist': {
        'SD_SO': 'dur_sunshine'
    },
    'hourly_wind_hist': {
        '  D': 'wind_direction',
        '  F': 'wind_spd'
    }
}

__TIMESTAMP__ = ['year', 'month', 'date', 'hour']
# __DATA__ = 'sample.geojson'

from dbaccess.dbconn import dbconn

def main():
    # data = pd.read_csv(os.path.join(__BASE__, __DIR__, __SUBDIR__[0], __DATA__), delimiter = ';')
    data = pd.read_csv(os.path.join(__BASE__, __DIR__, __DATA__), delimiter = ';')
    cols = list(data.columns)
    for item in cols: 
        if(item in __SUBSTITUT_DICT__['hourly_airtemp_humid_hist'].keys()):
            idx = cols.index(item)
            cols[idx] = __SUBSTITUT_DICT__['hourly_airtemp_humid_hist'][item]
    data.columns = cols
    for item in __TIMESTAMP__:
        data[item] = None
    rowidx = 0
    for val in data['MESS_DATUM'].values:
        data.iloc[rowidx, -4] = str(val)[:4]
        data.iloc[rowidx, -3] = str(val)[4:6]
        data.iloc[rowidx, -2] = str(val)[6:8]
        data.iloc[rowidx, -1] = str(val)[8:]
        rowidx += 1
    print(data.head())

class WeatherParse():
    def __init__(self, conn = None) -> None:
        # self.__L1Schema = ['type', 'crs', 'features']
        # self.__L3Schema = ['type', 'properties', 'geometry']
        # self.__L4Schema_properties = ['gml_id', 'SOBJ_KZ', 'SEGM_SEGM', 'SEGM_BEZ', 'STST_STR', 'STOR_NAME', 'ORTSTL', 'RVA_TYP', 'SORVT_TYP', 'LAENGE', 'B_PFLICHT', 'edge_geo']
        # self.__L4Schema_geometry = ['type', 'coordinates']
        if(not conn):
            self.__conn = dbconn.connect()
        else:
            self.__conn = conn

    def parse_worker(self, filename, dslabel) -> None:
        data = pd.read_csv(filename, delimiter = ';')
        cols = list(data.columns)
        for item in cols: 
            if(item.strip() in __SUBSTITUT_DICT__[dslabel].keys()):
                idx = cols.index(item)
                cols[idx] = __SUBSTITUT_DICT__[dslabel][item]
        data.columns = cols
        for item in __TIMESTAMP__:
            data[item] = None
        rowidx = 0
        for val in data['MESS_DATUM'].values:
            data.iloc[rowidx, -4] = str(val)[:4]
            data.iloc[rowidx, -3] = str(val)[4:6]
            data.iloc[rowidx, -2] = str(val)[6:8]
            data.iloc[rowidx, -1] = str(val)[8:]
            rowidx += 1
        
        data.to_csv(str(filename)[:-4] + '_processed.csv', index = False)

    def parse(self) -> None:
        for dataset in __DATA__:
            self.parse_worker(os.path.join(__BASE__, __DIR__, dataset), dataset)

if __name__ == "__main__":
    parser = WeatherParse()
    parser.parse()
