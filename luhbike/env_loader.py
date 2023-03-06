import sys
import psycopg2
import pandas as pd
import os
import json

sys.path.insert(0, ".")

__BASE__ = 'dataset'
__MCLOUD__ = 'mCloud'
__STR__ = 'Radverkehrsanlagen'
__DATA__ = 'Radverkehrsanlagen.geojson'
# __DATA__ = 'sample.geojson'

from dbaccess.dbconn import dbconn

__L1Schema = ['type', 'crs', 'features']
__L3Schema = ['type', 'properties', 'geometry']
__L4Schema_properties = ['gml_id', 'SOBJ_KZ', 'SEGM_SEGM', 'SEGM_BEZ', 'STST_STR', 'STOR_NAME', 'ORTSTL', 'RVA_TYP', 'SORVT_TYP', 'LAENGE', 'B_PFLICHT']
__L4Schema_geometry = ['type', 'coordinates']

class RVAnlagenParse():
    def __init__(self, conn = None, filepath = None) -> None:
        self.__L1Schema = ['type', 'crs', 'features']
        self.__L3Schema = ['type', 'properties', 'geometry']
        self.__L4Schema_properties = ['gml_id', 'SOBJ_KZ', 'SEGM_SEGM', 'SEGM_BEZ', 'STST_STR', 'STOR_NAME', 'ORTSTL', 'RVA_TYP', 'SORVT_TYP', 'LAENGE', 'B_PFLICHT', 'edge_geo']
        self.__L4Schema_geometry = ['type', 'coordinates']
        if(not conn):
            self.__conn = dbconn.connect()
        else:
            self.__conn = conn

        if((not filepath) or (not type(filepath) is str)):
            self.__filepath = os.path.join(__BASE__, __STR__, __DATA__)
        else:
            self.__filepath = filepath

    def parse(self):
        with open(self.__filepath, 'r', encoding = 'utf8') as f:
            datadict = json.load(f)
        datalist = datadict['features']
        strpd = pd.DataFrame(columns = self.__L4Schema_properties)
        # coordpd = pd.DataFrame(columns = ['gml_id', 'edge_geo'])

        for item in datalist:
            if(str(item['geometry']['type']).upper() == 'MULTILINESTRING'):
                continue
            # strpd = strpd.append(item['properties'])
            # if(len(item['geometry']['coordinates']) == 2):
            geostr = ''
            geostr += str(item['geometry']['type']).upper()
            geostr += '('
            for val in item['geometry']['coordinates']:
                geostr += (str(val[0]) + ' ' + str(val[1]) + ', ')
            geostr = geostr[:-2]
            geostr += ')'
            strdict = item['properties']
            strdict['edge_geo'] = geostr
            # print(type(strdict))
            strpd = strpd.append(strdict, ignore_index = True)

        strpd.to_csv(self.__filepath[:-4] + '_processed.csv', index = False)






def main():
    # with open(os.path.join(__BASE__, __STR__, __DATA__), 'r', encoding = 'utf8') as f:
    #     datadict = json.load(f)
    # datalist = datadict['features']
    # # print(str(datalist[0]['properties']['STST_STR']).encode('utf-8').decode('latin1'))
    # print(str(datalist[0]['properties']['STST_STR']))

    # for item in datalist:
    #     print(all(k in __L4Schema_properties for k in item['properties'].keys()))
    rvap = RVAnlagenParse()
    rvap.parse()

if __name__ == "__main__":
    main()