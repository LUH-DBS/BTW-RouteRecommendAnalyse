import sys
from typing import List, Union
sys.path.insert(0, ".")
import statistics

import pandas as pd

from dbaccess.dbconn import dbconn


__OSM_ROAD_ATTR__ = ['osm_id', "access", "addr:housename", "addr:housenumber", "addr:interpolation", 'admin_level', 'aerialway', 'aeroway', 'amenity', 'area', 'barrier',
	                 'bicycle', 'brand', 'bridge', 'boundary', 'building', 'construction', 'covered', 'culvert', 'cutting', 'denomination', 'disused', 'embankment',
                     'foot', "generator:source", 'harbour', 'highway', 'historic', 'horse', 'intermittent', 'junction', 'landuse', 'layer', 'leisure', "lock", 
                     'man_made', 'military', 'motorcar', 'name', "natural", 'office', 'oneway', "operator", 'place', 'population', 'power', 'power_source', 'public_transport',
                     'railway', "ref", 'religion', 'route', 'service', 'shop', 'sport', 'surface', 'toll', 'tourism', "tower:type", 'tracktype', 'tunnel', 'water', 'waterway',
                     'wetland', 'width', 'wood', 'z_order', 'way_area', 'way']

def _isNone_(arg):
    if(arg is None):
        return True
    if(str(arg).strip() == ''):
        return True
    if(str(arg) == 'None' or str(arg) == 'none' or str(arg) == 'NONE'):
        return True
    return False

class __featureAggWorker():
    def __init__(self, DBconn = None) -> None:
        if(not DBconn):
            self.__dbconn = dbconn.connect()
            self.__cur = self.__dbconn.cursor()
        else:
            self.__dbconn = DBconn
            self.__cur = self.__dbconn.cursor()

    def setOSMIDForBerlinBikeArea(self) -> None:
        _queryallls = """
                      SELECT ST_ASTEXT(edge_geo) FROM berlin_bikearea
                      """
        self.__cur.execute(_queryallls)
        lsList = list()
        for item in self.__cur.fetchall():
            # print(item)
            lsList.append(item[0])
        # print(lsList[0])
        
        _updatebbsql = """
                       UPDATE berlin_bikearea SET osm_id = (
                        SELECT subq.osm_id from (
                            SELECT osm_id, way <-> ST_TRANSFORM('SRID=4326;{}'::geometry,3857) as dist 
                            FROM berlin_osm_roads
                            ORDER BY dist LIMIT 1
                        ) subq
                       ) WHERE ST_ASTEXT(edge_geo) = '{}'
                       """
        counter = 0
        for item in lsList:
            _statement = _updatebbsql.format(str(item), str(item))
            try:
                self.__cur.execute(_statement)
                counter += 1
                if(not counter % 100):
                    self.__cur.execute("COMMIT")
                    counter = 0
                # break
            except:
                print("Can't update osm_id for " + str(item))
                print(_statement)
                break

    def setOSMIDForBerlinBikeNum(self) -> None:
        _totalnum_ = 23161226
        _queryallls = """
                      SELECT ST_ASTEXT(edge_geo) FROM berlin_bikenum
                      """
        self.__cur.execute(_queryallls)
        lsList = list()
        for item in self.__cur.fetchall():
            # print(item)
            lsList.append(item[0])
        # print(lsList[0])
        
        _updatebbsql = """
                       UPDATE berlin_bikenum SET osm_id = (
                        SELECT subq.osm_id from (
                            SELECT osm_id, way <-> ST_TRANSFORM('SRID=4326;{}'::geometry,3857) as dist 
                            FROM berlin_osm_roads
                            ORDER BY dist LIMIT 1
                        ) subq
                       ) WHERE ST_ASTEXT(edge_geo) = '{}'
                       """
        counter = 0
        commitcnt = 0
        for item in lsList:
            _statement = _updatebbsql.format(str(item), str(item))
            try:
                self.__cur.execute(_statement)
                commitcnt += 1
                counter += 1
                if(not commitcnt % 100):
                    self.__cur.execute("COMMIT")
                    print('{} / {} finished ({})'.format(str(counter), str(_totalnum_), str(counter / _totalnum_)))
                    commitcnt = 0
                # break
            except:
                print("Can't update osm_id for " + str(item))
                print(_statement)
                break

    def getSimraRide(self, attList: List[str] = None, cond: str = None) -> List[tuple]:
        _sql = """
               SELECT {} FROM btw_dsc_simra_rides {}
               """

        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        if(not cond):
            condstr = ''
        else:
            condstr = 'WHERE ' + cond
            
        _statement = _sql.format(att, condstr)
        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)
        return res

    def getSimraRide_batch(self, attList: List[str] = None, cond: str = None, offset: int = None, limit: int = None) -> List[tuple]:
        _sql = """
               SELECT {} FROM btw_dsc_simra_rides {}
               OFFSET {} {}
               """
        
        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        if(not cond):
            condstr = ''
        else:
            condstr = 'WHERE ' + cond
        if(not offset):
            start = str(0)
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)

        _statement = _sql.format(att, condstr, start, count)

        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)

        return res

    def joinSimraAndOSMRoadByOSMID(self, fetchleft = True, cond = None, offset = None, limit = None):
        _sql = """
               SELECT {} FROM btw_dsc_simra_rides sr JOIN berlin_osm_roads osmr ON sr.osm_id = osmr.osm_id
               {}
               OFFSET {} {}
               """

        if(fetchleft):
            att = '*'
        else:
            att = __OSM_ROAD_ATTR__
        if(not cond):
            condstr = ''
        else:
            condstr = cond
        if(not offset):
            start = str(0)
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)
        
        _statement = _sql.format(att, condstr, start, count)

        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)
        
        return res

    def joinSimraAndOSMLineByOSMID(self, fetchleft = True, cond = None, offset = None, limit = None):
        _sql = """
               SELECT {} FROM btw_dsc_simra_rides sr JOIN berlin_osm_lines osmr ON sr.osm_id = osmr.osm_id
               {}
               OFFSET {} {}
               """

        if(fetchleft):
            att = '*'
        else:
            att = __OSM_ROAD_ATTR__
        if(not cond):
            condstr = ''
        else:
            condstr = cond
        if(not offset):
            start = str(0)
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)
        
        _statement = _sql.format(att, condstr, start, count)

        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)
        
        return res

    def getOSMRoadFromOSMID(self, osmid: str, attList: List[str]) -> List[tuple]:
        _sql = """
               SELECT {} FROM berlin_osm_roads 
               WHERE osm_id = '{}'
               """

        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        _statement = _sql.format(att, osmid)

        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)

        return res

    def getClostestOSMRoadTop10(self, point: str, sSRID: Union[str, int] = 3857, tSRID: Union[str, int] = 3857) -> List[tuple]:
        _sql = """
               SELECT name, way <-> 
               ST_TRANSFORM('SRID={};{}'::geometry,{}) as dist 
               FROM berlin_osm_roads bor ORDER BY dist LIMIT 10
               """
        if(not point):
            return None
        
        _statement = _sql.format(str(sSRID), point, str(tSRID))
        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)

        return res

    def getRoadAvgSpeedTop10(self, point: str, sSRID: Union[str, int] = 3857, tSRID: Union[str, int] = 3857) -> List[tuple]:
        _sql = """
               SELECT avg_speed_kmh, edge_geo <-> 
               ST_TRANSFORM('SRID={};{}'::geometry,{}) as dist 
               FROM berlin_speed bs ORDER BY dist LIMIT 10
               """
        if(not point):
            return None

        _statement = _sql.format(str(sSRID), point, str(tSRID))
        self.__cur.execute(_statement)
        res = list()
        for item in self.__cur.fetchall():
            res.append(item)

        return res

    def joinAllSimraAndGeoData(self, attList = None, cond = None, limit = None, offset = None) -> None:
        _sql = """
               SELECT {} FROM 
               berlin_simra_rides bsr NATURAL JOIN 
               berlin_bikearea bba NATURAL JOIN 
               berlin_bikenum bbn NATURAL JOIN 
               berlin_speed bs NATURAL JOIN
               berlin_osm_roads bor
               {}
               {} {}
               """
        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        if(not cond):
            condstr = ''
        else:
            condstr = cond
        if(not offset):
            start = ''
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)

        _statement = _sql.format(att, condstr, start, count)
        
        # Uncomment this for create table, below for test
        # _statement = 'CREATE TABLE berlin_ride_geo AS ' + _statement
        # self.__cur.execute(_statement)

        # Use this to test if the generated feature vec is correct
        self.__cur.execute(_statement)
        for item in self.__cur.fetchall():
            print(item)
            break
    
    def joinAllSimraAndWeatherData(self, attList = None, cond = None, limit = None, offset = None) -> None:
        _sql = """
               SELECT {} FROM 
               berlin_simra_rides bsr NATURAL JOIN 
               berlin_airtemp_humidity bah NATURAL JOIN 
               berlin_groundtemp bgt NATURAL JOIN 
               berlin_rainfall brf NATURAL JOIN
               berlin_sunshine bss NATURAL JOIN
               berlin_wind bw
               {}
               {} {}
               """
        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        if(not cond):
            condstr = ''
        else:
            condstr = cond
        if(not offset):
            start = ''
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)

        _statement = _sql.format(att, condstr, start, count)
        
        # Uncomment this for create table, below for test
        # _statement = 'CREATE TABLE berlin_ride_weather AS ' + _statement
        # self.__cur.execute(_statement)

        # Use this to test if the generated feature vec is correct
        self.__cur.execute(_statement)
        for item in self.__cur.fetchall():
            print(item)
            break

    def joinAllSimraAndGeoOnOSMIDAndWeatherOnYMDH(self, attList = None, cond = None, limit = None, offset = None) -> None:
        _sql = """
               SELECT {} FROM (
               SELECT * FROM 
               berlin_simra_rides bsr NATURAL JOIN 
               berlin_bikearea bba NATURAL JOIN 
               berlin_bikenum bbn NATURAL JOIN 
               berlin_speed bs NATURAL JOIN
               berlin_osm_roads bor
               ) geo
               JOIN (
               SELECT * FROM 
               berlin_simra_rides bsr NATURAL JOIN 
               berlin_airtemp_humidity bah NATURAL JOIN 
               berlin_groundtemp bgt NATURAL JOIN 
               berlin_rainfall brf NATURAL JOIN
               berlin_sunshine bss NATURAL JOIN
               berlin_wind bw
               ) wea
               ON geo.year = wea.year AND geo.month = wea.month AND geo.day = wea.day AND geo.hour = wea.hour
               {}
               {} {}
               """
        if(not attList):
            att = '*'
        else:
            att = ','.join(attList)
        if(not cond):
            condstr = ''
        else:
            condstr = cond
        if(not offset):
            start = ''
        else:
            start = str(offset)
        if(not limit):
            count = ''
        else:
            count = 'LIMIT ' + str(limit)

        _statement = _sql.format(att, condstr, start, count)
        
        # Uncomment this for create table, below for test
        # _statement = 'CREATE TABLE berlin_ride_full AS ' + _statement
        # self.__cur.execute(_statement)

        # Use this to test if the generated feature vec is correct
        self.__cur.execute(_statement)
        for item in self.__cur.fetchall():
            print(item)
            break

    def addColforCategoryVars(self):
        # _finalTable = 'berlin_final'
        _featureTable_ = 'berlin_feature_partial_final_new'
        _SQL_ADD_COL = """
                       ALTER TABLE {} {}
                       """
        _type_ = 'FLOAT8'
        _dist_col_ = """
                     SELECT DISTINCT {} FROM {}
                     """
        _cat_col_names_ = ["rva_typ", "sorvt_typ", "bicycle", "construction", "cutting", "foot", "junction", "highway", "place", "railway", "service", "surface", "tunnel"]
        # {
        #     "berlin_bikearea": ["rva_typ", "sorvt_typ"],
        #     "berlin_osm_roads": ["bicycle", "construction", "cutting", "foot", "junction", "highway", "oneway", "place", "railway", "service", "surface", "tunnel"]
        # }

        _bool_col_names = {
            "berlin_osm_roads": ["covered", "embankment", "horse", "motorcar", "oneway"]
        }


        addColList = list()

        for colname in _cat_col_names_:
            _sql = _dist_col_.format(colname, _featureTable_)
            self.__cur.execute(_sql)
            for item in self.__cur.fetchall():
                addColList.append(str(colname) + "_" + str(item[0]))
        highway = ["highway_trunk", "highway_footway", "highway_platform", "highway_secondary", "highway_secondary_link", "highway_pedestrian", "highway_primary", "highway_residential", "highway_track", "highway_primary_link", "highway_motorway_link","highway_service", "highway_motorway", "highway_path"]
        railway = ["railway_platform", "railway_subway", "railway_rail", "railway_light_rail", "railway_abandoned", "railway_construction", "railway_platform_edge", "railway_tram", "railway_proposed", "railway_narrow_gauge", "railway_razed", "railway_disused"]
        surface = ["surface_concrete:plates", "surface_dirt", "surface_gravel", "surface_sett", "surface_cobblestone", "surface_compacted", "surface_paving_stones", "surface_cobblestone:flattened", "surface_concrete", "surface_asphalt", "surface_sand"]
        bicycle = ["bicycle_discouraged", "bicycle_use_sidepath", "bicycle_designated", "bicycle_yes", "bicycle_optional_sidepath", "bicycle_no"]
        service = ["service_driveway", "service_regional", "service_yard", "service_crossover", "service_spur", "service_siding"]
        place = ["place_suburb", "place_city", "place_borough"]
        foot = ["foot_no", "foot_use_sidepath", "foot_designated", "foot_yes"]
        rva = ["rva_typ_Radfahrstreifen", "rva_typ_Bussonderfahrstreifen", "rva_typ_Schutzstreifen", "rva_typ_Radwege"]
        sorvt = ["sorvt_typ_Radfahrstreifen Z 295, ruh.Verkehr mit Begrenzung", "sorvt_typ_Radverkehrsanlage Z 340 im/am Knotenpunktsbereich", "sorvt_typ_Gehweg, mit Radverkehr frei", "sorvt_typ_Bussonderfahrstreifen Z 295",
                    "sorvt_typ_Geh-/Radweg, durch Markierung unterschieden", "sorvt_typ_Radweg, baulich getrennt", "sorvt_typ_Radfahrerfurt Z 340", "sorvt_typ_Schutzstreifen Z 340 ohne ruhenden Verkehr",
                    "sorvt_typ_Geh-/Radweg, baulich unterschieden", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr ohne Begrenzung", "sorvt_typ_Radfahrstreifen Z 295, ohne ruh.Verkehr", "sorvt_typ_Geh-/Radweg, ohne Trennung",
                    "sorvt_typ_Bussonderfahrstreifen Z 340", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr mit Begrenzung"]
        for item in highway:
            if item not in addColList:
                addColList.append(item)
        for item in railway:
            if item not in addColList:
                addColList.append(item)
        for item in surface:
            if item not in addColList:
                addColList.append(item)
        for item in bicycle:
            if item not in addColList:
                addColList.append(item)
        for item in service:
            if item not in addColList:
                addColList.append(item)
        for item in place:
            if item not in addColList:
                addColList.append(item)
        for item in foot:
            if item not in addColList:
                addColList.append(item)
        for item in rva:
            if item not in addColList:
                addColList.append(item)
        for item in sorvt:
            if item not in addColList:
                addColList.append(item)

        print(addColList)
        addColStringList = ['ADD COLUMN IF NOT EXISTS "' + str(item) + '" FLOAT8' for item in addColList if item is not None]
        addColString = ", ".join(addColStringList)
        _sql = _SQL_ADD_COL.format(_featureTable_, addColString)
        self.__cur.execute(_sql)
        self.__cur.execute("COMMIT")

        # --- finished adding columns

        # _sql_get_rides_ = """
        #                   SELECT "ride_id", "rva_typ", "sorvt_typ", "highway", "railway", "surface"
        #                   FROM {}
        #                   """
        # self.__cur.execute(_sql_get_rides_.format(_featureTable_))

    def processCategoryVals(self):
        _sql = """
               CREATE TABLE IF NOT EXISTS berlin_final_new AS (SELECT * FROM berlin_feature_partial_final_new LIMIT 0)
               """
        self.__cur.execute(_sql)
        # _sql = """
        #        ALTER TABLE berlin_final_new ADD "duration" int8
        #        """
        # self.__cur.execute(_sql)
        # self.__cur.execute("COMMIT")
        _finalTable = 'berlin_final_new'
        _featureTable_ = "berlin_feature_partial_final_new"
        _sql_ = """
                INSERT INTO {} (
                ride_id, x, y, z, "duration", acc, a, b, c, obsdistanceleft1, obsdistanceleft2, obsdistanceright1, obsdistanceright2,
                xl, yl, zl, rx, ry, rz, rc, year, month, day, hour,
                laenge, "occurrences", "avg_speed_kmh",
                population,
                airtemp, "rel_humid", "temp_2cm", "temp_5cm", "temp_10cm", "temp_20cm", "temp_50cm", "temp_100cm",
                "precipitation_1h", "precipitation_indicator", "precipitation_type", "dur_sunshine", "wind_spd", "wind_direction",
                "label",
                "rva_typ_Radfahrstreifen", "rva_typ_Bussonderfahrstreifen", "rva_typ_Schutzstreifen", "rva_typ_Radwege",
                "sorvt_typ_Radfahrstreifen Z 295, ruh.Verkehr mit Begrenzung", "sorvt_typ_Radverkehrsanlage Z 340 im/am Knotenpunktsbereich", "sorvt_typ_Gehweg, mit Radverkehr frei", "sorvt_typ_Bussonderfahrstreifen Z 295",
                "sorvt_typ_Geh-/Radweg, durch Markierung unterschieden", "sorvt_typ_Radweg, baulich getrennt", "sorvt_typ_Radfahrerfurt Z 340", "sorvt_typ_Schutzstreifen Z 340 ohne ruhenden Verkehr",
                "sorvt_typ_Geh-/Radweg, baulich unterschieden", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr ohne Begrenzung", "sorvt_typ_Radfahrstreifen Z 295, ohne ruh.Verkehr", "sorvt_typ_Geh-/Radweg, ohne Trennung",
                "sorvt_typ_Bussonderfahrstreifen Z 340", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr mit Begrenzung",
                "bicycle_discouraged", bicycle_use_sidepath, bicycle_designated, bicycle_yes, bicycle_optional_sidepath, bicycle_no, 
                construction_bridge, construction_rail, construction_yes, construction_light_rail, construction_tram,
                foot_no, foot_use_sidepath, foot_designated, foot_yes,
                highway_trunk, highway_footway, highway_platform, highway_secondary, highway_secondary_link, highway_pedestrian, highway_primary, highway_residential, highway_track, highway_primary_link, highway_motorway_link,
                highway_service, highway_motorway, highway_path,
                place_suburb, place_city, place_borough,
                railway_platform, railway_subway, railway_rail, railway_light_rail, railway_abandoned, railway_construction, railway_platform_edge, railway_tram, railway_proposed, railway_narrow_gauge, railway_razed, railway_disused,
                service_driveway, service_regional, service_yard, service_crossover, service_spur, service_siding,
                "surface_concrete:plates", surface_dirt, surface_gravel, surface_sett, surface_cobblestone, surface_compacted, surface_paving_stones, "surface_cobblestone:flattened", surface_concrete, surface_asphalt, surface_sand,
                tunnel_no, tunnel_building_passage, tunnel_yes
                )
                VALUES {}
                """
        
        unique_rideid = list()
        _unique_rideid_sql = """
                             SELECT DISTINCT ride_id FROM {}
                             """
        self.__cur.execute(_unique_rideid_sql.format(_featureTable_))
        for item in self.__cur.fetchall():
            unique_rideid.append(item[0])

        _all_sql_ = """
                    SELECT * FROM {}
                    """
        self.__cur.execute(_all_sql_.format(_featureTable_))
        ride_dict = dict()
        for item in self.__cur.fetchall():
            if(item[0] not in ride_dict.keys()):
                ride_dict[item[0]] = dict()

            if('x' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['x'] = {"val": float(item[3]), "count": 1}
                except:
                    ride_dict[item[0]]['x'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['x']['val'] += float(item[3])
                except:
                    ride_dict[item[0]]['x']['val'] += 0
                ride_dict[item[0]]['x']['count'] += 1
            if('y' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['y'] = {"val": float(item[4]), "count": 1}
                except:
                    ride_dict[item[0]]['y'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['y']['val'] += float(item[4])
                except:
                    ride_dict[item[0]]['y']['val'] += 0
                ride_dict[item[0]]['y']['count'] += 1
            if('z' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['z'] = {"val": float(item[5]), "count": 1}
                except:
                    ride_dict[item[0]]['z'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['z']['val'] += float(item[5])
                except:
                    ride_dict[item[0]]['z']['val'] += 0
                ride_dict[item[0]]['z']['count'] += 1

            if('timestamp' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['timestamp'] = {"min": int(item[6]), "max": int(item[6])}
                except:
                    ride_dict[item[0]]['timestamp'] = {"min": int(9000000000000), "max": int(-1)}
            else:
                try:
                    ts = int(item[6])
                    if(ts < ride_dict[item[0]]['timestamp']['min']):
                        ride_dict[item[0]]['timestamp']['min'] = ts
                    if(ts > ride_dict[item[0]]['timestamp']['max']):
                        ride_dict[item[0]]['timestamp']['max']
                except:
                    pass

            if('acc' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['acc'] = {"val": float(item[7]), "count": 1}
                except:
                    ride_dict[item[0]]['acc'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['acc']['val'] += float(item[7])
                except:
                    ride_dict[item[0]]['acc']['val'] += 0
                ride_dict[item[0]]['acc']['count'] += 1
            if('a' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['a'] = {"val": float(item[8]), "count": 1}
                except:
                    ride_dict[item[0]]['a'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['a']['val'] += float(item[8])
                except:
                    ride_dict[item[0]]['a']['val'] += 0
                ride_dict[item[0]]['a']['count'] += 1
            if('b' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['b'] = {"val": float(item[9]), "count": 1}
                except:
                    ride_dict[item[0]]['b'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['b']['val'] += float(item[9])
                except:
                    ride_dict[item[0]]['b']['val'] += 0
                ride_dict[item[0]]['b']['count'] += 1
            if('c' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['c'] = {"val": float(item[10]), "count": 1}
                except:
                    ride_dict[item[0]]['c'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['c']['val'] += float(item[10])
                except:
                    ride_dict[item[0]]['c']['val'] += 0
                ride_dict[item[0]]['c']['count'] += 1

            if('obsdistanceleft1' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['obsdistanceleft1'] = {"val": float(item[11]), "count": 1}
                except:
                    ride_dict[item[0]]['obsdistanceleft1'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['obsdistanceleft1']['val'] += float(item[11])
                except:
                    ride_dict[item[0]]['obsdistanceleft1']['val'] += 0
                ride_dict[item[0]]['obsdistanceleft1']['count'] += 1
            if('obsdistanceleft2' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['obsdistanceleft2'] = {"val": float(item[12]), "count": 1}
                except:
                    ride_dict[item[0]]['obsdistanceleft2'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['obsdistanceleft2']['val'] += float(item[12])
                except:
                    ride_dict[item[0]]['obsdistanceleft2']['val'] += 0
                ride_dict[item[0]]['obsdistanceleft2']['count'] += 1
            if('obsdistanceright1' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['obsdistanceright1'] = {"val": float(item[13]), "count": 1}
                except:
                    ride_dict[item[0]]['obsdistanceright1'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['obsdistanceright1']['val'] += float(item[13])
                except:
                    ride_dict[item[0]]['obsdistanceright1']['val'] += 0
                ride_dict[item[0]]['obsdistanceright1']['count'] += 1
            if('obsdistanceright2' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['obsdistanceright2'] = {"val": float(item[14]), "count": 1}
                except:
                    ride_dict[item[0]]['obsdistanceright2'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['obsdistanceright2']['val'] += float(item[14])
                except:
                    ride_dict[item[0]]['obsdistanceright2']['val'] += 0
                ride_dict[item[0]]['obsdistanceright2']['count'] += 1
            
            if('xl' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['xl'] = {"val": float(item[16]), "count": 1}
                except:
                    ride_dict[item[0]]['xl'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['xl']['val'] += float(item[16])
                except:
                    ride_dict[item[0]]['xl']['val'] += 0
                ride_dict[item[0]]['xl']['count'] += 1
            if('yl' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['yl'] = {"val": float(item[17]), "count": 1}
                except:
                    ride_dict[item[0]]['yl'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['yl']['val'] += float(item[17])
                except:
                    ride_dict[item[0]]['yl']['val'] += 0
                ride_dict[item[0]]['yl']['count'] += 1
            if('zl' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['zl'] = {"val": float(item[18]), "count": 1}
                except:
                    ride_dict[item[0]]['zl'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['zl']['val'] += float(item[18])
                except:
                    ride_dict[item[0]]['zl']['val'] += 0
                ride_dict[item[0]]['zl']['count'] += 1
            if('rx' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['rx'] = {"val": float(item[19]), "count": 1}
                except:
                    ride_dict[item[0]]['rx'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['rx']['val'] += float(item[19])
                except:
                    ride_dict[item[0]]['rx']['val'] += 0
                ride_dict[item[0]]['rx']['count'] += 1
            if('ry' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['ry'] = {"val": float(item[20]), "count": 1}
                except:
                    ride_dict[item[0]]['ry'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['ry']['val'] += float(item[20])
                except:
                    ride_dict[item[0]]['ry']['val'] += 0
                ride_dict[item[0]]['ry']['count'] += 1
            if('rz' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['rz'] = {"val": float(item[21]), "count": 1}
                except:
                    ride_dict[item[0]]['rz'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['rz']['val'] += float(item[21])
                except:
                    ride_dict[item[0]]['rz']['val'] += 0
                ride_dict[item[0]]['rz']['count'] += 1
            if('rc' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['rc'] = {"val": float(item[22]), "count": 1}
                except:
                    ride_dict[item[0]]['rc'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['rc']['val'] += float(item[22])
                except:
                    ride_dict[item[0]]['rc']['val'] += 0
                ride_dict[item[0]]['rc']['count'] += 1

            if('year' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['year'] = item[23]
            if('month' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['month'] = item[24]
            if('day' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['day'] = item[25]
            if('hour' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['hour'] = item[26]

            if('rva_typ' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['rva_typ'] = dict()
                if(not _isNone_(item[36])):
                    if(item[36] not in ride_dict[item[0]]['rva_typ'].keys()):
                        ride_dict[item[0]]['rva_typ'][item[36]] = 1
                    else:
                        ride_dict[item[0]]['rva_typ'][item[36]] += 1
            if('sorvt_typ' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['sorvt_typ'] = dict()
                if(not _isNone_(item[37])):
                    if(item[37] not in ride_dict[item[0]]['sorvt_typ'].keys()):
                        ride_dict[item[0]]['sorvt_typ'][item[37]] = 1
                    else:
                        ride_dict[item[0]]['sorvt_typ'][item[37]] += 1

            if('laenge' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['laenge'] = [float(item[38])]
                except:
                    ride_dict[item[0]]['laenge'] = [0]
            else:
                try:
                    ride_dict[item[0]]['laenge'].append(float(item[38]))
                except:
                    ride_dict[item[0]]['laenge'].append(0) 
            if('occurrences' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['occurrences'] = {"val": float(item[41]), "count": 1}
                except:
                    ride_dict[item[0]]['occurrences'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['occurrences']['val'] += float(item[41])
                except:
                    ride_dict[item[0]]['occurrences']['val'] += 0
                ride_dict[item[0]]['occurrences']['count'] += 1   
            if('avg_speed_kmh' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['avg_speed_kmh'] = {"val": float(item[42]), "count": 1}
                except:
                    ride_dict[item[0]]['avg_speed_kmh'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['avg_speed_kmh']['val'] += float(item[42])
                except:
                    ride_dict[item[0]]['avg_speed_kmh']['val'] += 0
                ride_dict[item[0]]['avg_speed_kmh']['count'] += 1
            if('population' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['population'] = {"val": float(item[86]), "count": 1}
                except:
                    ride_dict[item[0]]['population'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['population']['val'] += float(item[86])
                except:
                    ride_dict[item[0]]['population']['val'] += 0
                ride_dict[item[0]]['population']['count'] += 1

            if('oneway' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['oneway'] = dict()
            if(item[83] == 'yes' or item[83] == 'Yes' or item[83] == 'YES' or item[83] == 'ja' or item[83] == 'Ja' or item[83] == 'JA'):
                if('yes' not in ride_dict[item[0]]['oneway'].keys()):
                    ride_dict[item[0]]['oneway']['yes'] = 1
                else:
                    ride_dict[item[0]]['oneway']['yes'] += 1

            if('bicycle' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['bicycle'] = dict()
                if(not _isNone_(item[53])):
                    if(item[53] not in ride_dict[item[0]]['bicycle'].keys()):
                        ride_dict[item[0]]['bicycle'][item[53]] = 1
                    else:
                        ride_dict[item[0]]['bicycle'][item[53]] += 1
            
            if('highway' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['highway'] = dict()
                if(not _isNone_(item[68])):
                    if(item[68] not in ride_dict[item[0]]['highway'].keys()):
                        ride_dict[item[0]]['highway'][item[68]] = 1
                    else:
                        ride_dict[item[0]]['highway'][item[68]] += 1

            if('railway' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['railway'] = dict()
                if(not _isNone_(item[90])):
                    if(item[90] not in ride_dict[item[0]]['railway'].keys()):
                        ride_dict[item[0]]['railway'][item[90]] = 1
                    else:
                        ride_dict[item[0]]['railway'][item[90]] += 1
            
            if('place' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['place'] = dict()
                if(not _isNone_(item[85])):
                    if(item[85] not in ride_dict[item[0]]['place'].keys()):
                        ride_dict[item[0]]['place'][item[85]] = 1
                    else:
                        ride_dict[item[0]]['place'][item[85]] += 1

            if('foot' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['foot'] = dict()
                if(not _isNone_(item[65])):
                    if(item[65] not in ride_dict[item[0]]['foot'].keys()):
                        ride_dict[item[0]]['foot'][item[65]] = 1
                    else:
                        ride_dict[item[0]]['foot'][item[65]] += 1

            if('service' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['service'] = dict()
                if(not _isNone_(item[94])):
                    if(item[94] not in ride_dict[item[0]]['service'].keys()):
                        ride_dict[item[0]]['service'][item[94]] = 1
                    else:
                        ride_dict[item[0]]['service'][item[94]] += 1
            if('surface' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['surface'] = dict()
                if(not _isNone_(item[97])):
                    if(item[97] not in ride_dict[item[0]]['surface'].keys()):
                        ride_dict[item[0]]['surface'][item[97]] = 1
                    else:
                        ride_dict[item[0]]['surface'][item[97]] += 1

            if('airtemp' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['airtemp'] = {"val": float(item[111]), "count": 1}
                except:
                    ride_dict[item[0]]['airtemp'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['airtemp']['val'] += float(item[111])
                except:
                    ride_dict[item[0]]['airtemp']['val'] += 0
                ride_dict[item[0]]['airtemp']['count'] += 1
            if('rel_humid' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['rel_humid'] = {"val": float(item[112]), "count": 1}
                except:
                    ride_dict[item[0]]['rel_humid'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['rel_humid']['val'] += float(item[112])
                except:
                    ride_dict[item[0]]['rel_humid']['val'] += 0
                ride_dict[item[0]]['rel_humid']['count'] += 1
            if('temp_2cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_2cm'] = {"val": float(item[113]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_2cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_2cm']['val'] += float(item[113])
                except:
                    ride_dict[item[0]]['temp_2cm']['val'] += 0
                ride_dict[item[0]]['temp_2cm']['count'] += 1
            if('temp_5cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_5cm'] = {"val": float(item[113]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_5cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_5cm']['val'] += float(item[113])
                except:
                    ride_dict[item[0]]['temp_5cm']['val'] += 0
                ride_dict[item[0]]['temp_5cm']['count'] += 1
            if('temp_10cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_10cm'] = {"val": float(item[114]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_10cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_10cm']['val'] += float(item[114])
                except:
                    ride_dict[item[0]]['temp_10cm']['val'] += 0
                ride_dict[item[0]]['temp_10cm']['count'] += 1
            if('temp_20cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_20cm'] = {"val": float(item[115]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_20cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_20cm']['val'] += float(item[115])
                except:
                    ride_dict[item[0]]['temp_20cm']['val'] += 0
                ride_dict[item[0]]['temp_20cm']['count'] += 1
            if('temp_50cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_50cm'] = {"val": float(item[116]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_50cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_50cm']['val'] += float(item[116])
                except:
                    ride_dict[item[0]]['temp_50cm']['val'] += 0
                ride_dict[item[0]]['temp_50cm']['count'] += 1
            if('temp_100cm' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['temp_100cm'] = {"val": float(item[117]), "count": 1}
                except:
                    ride_dict[item[0]]['temp_100cm'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['temp_100cm']['val'] += float(item[117])
                except:
                    ride_dict[item[0]]['temp_100cm']['val'] += 0
                ride_dict[item[0]]['temp_100cm']['count'] += 1

            if('precipitation_1h' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['precipitation_1h'] = {"val": float(item[119]), "count": 1}
                except:
                    ride_dict[item[0]]['precipitation_1h'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['precipitation_1h']['val'] += float(item[119])
                except:
                    ride_dict[item[0]]['precipitation_1h']['val'] += 0
                ride_dict[item[0]]['precipitation_1h']['count'] += 1
            if('precipitation_indicator' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['precipitation_indicator'] = {"val": float(item[120]), "count": 1}
                except:
                    ride_dict[item[0]]['precipitation_indicator'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['precipitation_indicator']['val'] += float(item[120])
                except:
                    ride_dict[item[0]]['precipitation_indicator']['val'] += 0
                ride_dict[item[0]]['precipitation_indicator']['count'] += 1
            
            if('dur_sunshine' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['dur_sunshine'] = {"val": float(item[122]), "count": 1}
                except:
                    ride_dict[item[0]]['dur_sunshine'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['dur_sunshine']['val'] += float(item[122])
                except:
                    ride_dict[item[0]]['dur_sunshine']['val'] += 0
                ride_dict[item[0]]['dur_sunshine']['count'] += 1
            if('wind_spd' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['wind_spd'] = {"val": float(item[123]), "count": 1}
                except:
                    ride_dict[item[0]]['wind_spd'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['wind_spd']['val'] += float(item[123])
                except:
                    ride_dict[item[0]]['wind_spd']['val'] += 0
                ride_dict[item[0]]['wind_spd']['count'] += 1
            if('wind_direction' not in ride_dict[item[0]].keys()):
                try:
                    ride_dict[item[0]]['wind_direction'] = {"val": float(item[124]), "count": 1}
                except:
                    ride_dict[item[0]]['wind_direction'] = {"val": 0, "count": 1}
            else:
                try:
                    ride_dict[item[0]]['wind_direction']['val'] += float(item[124])
                except:
                    ride_dict[item[0]]['wind_direction']['val'] += 0
                ride_dict[item[0]]['wind_direction']['count'] += 1
            
            if('label' not in ride_dict[item[0]].keys()):
                ride_dict[item[0]]['label'] = item[125]

            # if('speed' not in ride_dict[item[0]].keys()):
            #     try:
            #         ride_dict[item[0]]['speed'] = {"val": float(item[218]), "count": 1}
            #     except:
            #         ride_dict[item[0]]['speed'] = {"val": 0, "count": 1}
            # else:
            #     try:
            #         ride_dict[item[0]]['speed']['val'] += float(item[218])
            #     except:
            #         ride_dict[item[0]]['speed']['val'] += 0
            #     ride_dict[item[0]]['speed']['count'] += 1
            # if('absl' not in ride_dict[item[0]].keys()):
            #     try:
            #         ride_dict[item[0]]['absl'] = {"val": float(item[22193]), "count": 1}
            #     except:
            #         ride_dict[item[0]]['absl'] = {"val": 0, "count": 1}
            # else:
            #     try:
            #         ride_dict[item[0]]['absl']['val'] += float(item[219])
            #     except:
            #         ride_dict[item[0]]['absl']['val'] += 0
            #     ride_dict[item[0]]['absl']['count'] += 1
            # if('absr' not in ride_dict[item[0]].keys()):
            #     try:
            #         ride_dict[item[0]]['absr'] = {"val": float(item[220]), "count": 1}
            #     except:
            #         ride_dict[item[0]]['absr'] = {"val": 0, "count": 1}
            # else:
            #     try:
            #         ride_dict[item[0]]['absr']['val'] += float(item[220])
            #     except:
            #         ride_dict[item[0]]['absr']['val'] += 0
            #     ride_dict[item[0]]['absr']['count'] += 1
            # print(item[232])
            # print(ride_dict[item[0]]['speed'])
            # print(item[233])
            # print(ride_dict[item[0]]['absl'])
            # print(item[234])
            # print(ride_dict[item[0]]['absr'])



        avgFeatures = ['x', 'y', 'z', 'acc', 'a', 'b', 'c', 'obsdistanceleft1', 'obsdistanceleft2', 'obsdistanceright1', 'obsdistanceright2', 'xl', 'yl', 'zl', 'rx', 'ry', 'rz', 'rc', 'laenge', 'occurrences', 'avg_speed_kmh']
        boolFeatures = ['oneway']
        constFeatures = ['year', 'month', 'day', 'hour']

        for key, val in ride_dict.items():
            insertStr = list()
            insertStr.append(key)
            insertStr.append(val['x']['val'] / val['x']['count'])
            insertStr.append(val['y']['val'] / val['y']['count'])
            insertStr.append(val['z']['val'] / val['z']['count'])
            if(val['timestamp']['max'] != -1 and val['timestamp']['min'] < 9000000000000):
                duration = val['timestamp']['max'] - val['timestamp']['min']
            else:
                duration = 0
            insertStr.append(duration)
            insertStr.append(val['acc']['val'] / val['acc']['count'])
            insertStr.append(val['a']['val'] / val['a']['count'])
            insertStr.append(val['b']['val'] / val['b']['count'])
            insertStr.append(val['c']['val'] / val['c']['count'])

            insertStr.append(val['obsdistanceleft1']['val'] / val['obsdistanceleft1']['count'])
            insertStr.append(val['obsdistanceleft2']['val'] / val['obsdistanceleft2']['count'])
            insertStr.append(val['obsdistanceright1']['val'] / val['obsdistanceright1']['count'])
            insertStr.append(val['obsdistanceright2']['val'] / val['obsdistanceright2']['count'])
            insertStr.append(val['xl']['val'] / val['xl']['count'])
            insertStr.append(val['yl']['val'] / val['yl']['count'])
            insertStr.append(val['zl']['val'] / val['zl']['count'])
            insertStr.append(val['rx']['val'] / val['rx']['count'])
            insertStr.append(val['ry']['val'] / val['ry']['count'])
            insertStr.append(val['rz']['val'] / val['rz']['count'])
            insertStr.append(val['rc']['val'] / val['rc']['count'])
            insertStr.append(val['year'])
            insertStr.append(val['month'])
            insertStr.append(val['day'])
            insertStr.append(val['hour'])
            # insertStr.append(statistics.median(val['laenge']))
            insertStr.append(sum(val['laenge']))
            insertStr.append(val['occurrences']['val'] / val['occurrences']['count'])
            insertStr.append(val['avg_speed_kmh']['val'] / val['avg_speed_kmh']['count'])
            insertStr.append(val['population']['val'] / val['population']['count'])
            insertStr.append(val['airtemp']['val'] / val['airtemp']['count'])
            insertStr.append(val['rel_humid']['val'] / val['rel_humid']['count'])
            insertStr.append(val['temp_2cm']['val'] / val['temp_2cm']['count'])
            insertStr.append(val['temp_5cm']['val'] / val['temp_5cm']['count'])
            insertStr.append(val['temp_10cm']['val'] / val['temp_10cm']['count'])
            insertStr.append(val['temp_20cm']['val'] / val['temp_20cm']['count'])
            insertStr.append(val['temp_50cm']['val'] / val['temp_50cm']['count'])
            insertStr.append(val['temp_100cm']['val'] / val['temp_100cm']['count'])
            insertStr.append(val['precipitation_1h']['val'] / val['precipitation_1h']['count'])
            insertStr.append(val['precipitation_indicator']['val'] / val['precipitation_indicator']['count'])
            insertStr.append('0')
            insertStr.append(val['dur_sunshine']['val'] / val['dur_sunshine']['count'])
            insertStr.append(val['wind_spd']['val'] / val['wind_spd']['count'])
            insertStr.append(val['wind_direction']['val'] / val['wind_direction']['count'])
            insertStr.append(val['label'])

            newdict = dict()
            sumc = 0
            for k, v in val['rva_typ'].items():
                sumc += v
            for k, v in val['rva_typ'].items():
                newdict['rva_typ_' + str(k)] = float(float(v) / float(sumc))
            # print(val['rva_typ'])
            cols = ["rva_typ_Radfahrstreifen", "rva_typ_Bussonderfahrstreifen", "rva_typ_Schutzstreifen", "rva_typ_Radwege"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['sorvt_typ'].items():
                sumc += v
            for k, v in val['sorvt_typ'].items():
                newdict['sorvt_typ_' + str(k)] = float(float(v) / float(sumc))
            cols = ["sorvt_typ_Radfahrstreifen Z 295, ruh.Verkehr mit Begrenzung", "sorvt_typ_Radverkehrsanlage Z 340 im/am Knotenpunktsbereich", "sorvt_typ_Gehweg, mit Radverkehr frei", "sorvt_typ_Bussonderfahrstreifen Z 295",
                    "sorvt_typ_Geh-/Radweg, durch Markierung unterschieden", "sorvt_typ_Radweg, baulich getrennt", "sorvt_typ_Radfahrerfurt Z 340", "sorvt_typ_Schutzstreifen Z 340 ohne ruhenden Verkehr",
                    "sorvt_typ_Geh-/Radweg, baulich unterschieden", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr ohne Begrenzung", "sorvt_typ_Radfahrstreifen Z 295, ohne ruh.Verkehr", "sorvt_typ_Geh-/Radweg, ohne Trennung",
                    "sorvt_typ_Bussonderfahrstreifen Z 340", "sorvt_typ_Schutzstreifen Z 340, mit ruh.Verkehr mit Begrenzung"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['bicycle'].items():
                sumc += v
            for k, v in val['bicycle'].items():
                newdict['bicycle_' + str(k)] = float(float(v) / float(sumc))
            cols = ["bicycle_discouraged", "bicycle_use_sidepath", "bicycle_designated", "bicycle_yes", "bicycle_optional_sidepath", "bicycle_no"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            for i in range(5):
                insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['foot'].items():
                sumc += v
            for k, v in val['foot'].items():
                newdict['foot_' + str(k)] = float(float(v) / float(sumc))
            cols = ["foot_no", "foot_use_sidepath", "foot_designated", "foot_yes"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['highway'].items():
                sumc += v
            for k, v in val['highway'].items():
                newdict['highway_' + str(k)] = float(float(v) / float(sumc))
            cols = ["highway_trunk", "highway_footway", "highway_platform", "highway_secondary", "highway_secondary_link", "highway_pedestrian", "highway_primary", "highway_residential", "highway_track", "highway_primary_link", "highway_motorway_link",
                    "highway_service", "highway_motorway", "highway_path"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['place'].items():
                sumc += v
            for k, v in val['place'].items():
                newdict['place_' + str(k)] = float(float(v) / float(sumc))
            cols = ["place_suburb", "place_city", "place_borough"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['railway'].items():
                sumc += v
            for k, v in val['railway'].items():
                newdict['railway_' + str(k)] = float(float(v) / float(sumc))
            cols = ["railway_platform", "railway_subway", "railway_rail", "railway_light_rail", "railway_abandoned", "railway_construction", "railway_platform_edge", "railway_tram", "railway_proposed", "railway_narrow_gauge", "railway_razed", "railway_disused"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['service'].items():
                sumc += v
            for k, v in val['service'].items():
                newdict['service_' + str(k)] = float(float(v) / float(sumc))
            cols = ["service_driveway", "service_regional", "service_yard", "service_crossover", "service_spur", "service_siding"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            newdict = dict()
            sumc = 0
            for k, v in val['surface'].items():
                sumc += v
            for k, v in val['surface'].items():
                newdict['surface_' + str(k)] = float(float(v) / float(sumc))
            cols = ["surface_concrete:plates", "surface_dirt", "surface_gravel", "surface_sett", "surface_cobblestone", "surface_compacted", "surface_paving_stones", "surface_cobblestone:flattened", "surface_concrete", "surface_asphalt", "surface_sand"]
            for item in cols:
                if(item in newdict.keys()):
                    insertStr.append(newdict[item])
                else:
                    insertStr.append(0)

            for i in range(3):
                insertStr.append(0)
            # insertStr.append(val['speed']['val'] / val['speed']['count'])
            # insertStr.append(val['absl']['val'] / val['absl']['count'])
            # insertStr.append(val['absr']['val'] / val['absr']['count'])
            # insertStr.append(1)

            insertStr = [x if x is not None else 0 for x in insertStr]

            insertTuple = tuple(insertStr)

            self.__cur.execute(_sql_.format(_finalTable, str(insertTuple)))
            self.__cur.execute("COMMIT")

                
            

    def insertInBatch(self):
        _sql = """
               INSERT INTO berlin_feature_partial (
                SELECT bsr.ride_id, bsr.lat, bsr.lon, bsr.x, bsr.y, bsr.z, bsr."timestamp", bsr.acc, bsr.a, bsr.b, bsr.c, bsr.obsdistanceleft1, bsr.obsdistanceleft2, bsr.obsdistanceright1, bsr.obsdistanceright2, bsr.obsclosepassevent, bsr.xl, bsr.yl, bsr.zl, bsr.rx, bsr.ry, bsr.rz, bsr.rc, bsr."year", bsr."month", bsr."day", bsr."hour", bsr.osm_id, bsr.geo_point, bba.gml_id, sobj_kz, bba.segm_segm, bba.segm_bez, bba.stst_str, bba.stor_name, bba.ortstl, bba.rva_typ, bba.sorvt_typ, bba.laenge, bba.b_pflicht, bba.edge_geo_3857, bbn.occurrences, bs.avg_speed_kmh, bs.visits, access, bor."addr:housename", bor."addr:housenumber", bor."addr:interpolation", bor.admin_level, bor.aerialway, bor.aeroway, bor.amenity, bor.area, bor.barrier, bor.bicycle, bor.brand, bor.bridge, bor.boundary, bor.building, bor.construction, bor.covered, bor.culvert, bor.cutting, bor.denomination, bor.disused, bor.embankment, bor.foot, bor."generator:source", bor.harbour, bor.highway, bor.historic, bor.horse, bor.intermittent, bor.junction, bor.landuse, bor.layer, bor.leisure, bor.lock, bor.man_made, bor.military, bor.motorcar, bor."name", bor."natural", bor.office, bor.oneway, bor."operator", bor.place, bor.population, bor.power, bor.power_source, bor.public_transport, bor.railway, bor."ref", bor.religion, bor.route, bor.service, bor.shop, bor.sport, bor.surface, bor.toll, bor.tourism, bor."tower:type", bor.tracktype, bor.tunnel, bor.water, bor.waterway, bor.wetland, bor."width", bor.wood, bor.z_order, bor.way_area, bor.way, bah.airtemp, bah.rel_humid, bg.temp_2cm, bg.temp_5cm, bg.temp_10cm, bg.temp_20cm, bg.temp_50cm, bg.temp_100cm, br.precipitation_1h, br.precipitation_indicator, br.precipitation_type, bss.dur_sunshine, bw.wind_spd, bw.wind_direction 
                FROM berlin_simra_rides_4k bsr 
                LEFT JOIN berlin_bikearea bba ON bba.osm_id = bsr.osm_id 
                JOIN berlin_bikenum bbn ON bbn.osm_id = bsr.osm_id
                JOIN berlin_speed bs ON bs.osm_id = bsr.osm_id
                JOIN berlin_osm_roads bor ON bor.osm_id = bsr.osm_id
                JOIN berlin_airtemp_humidity bah ON bah."year" = bsr."year" AND bah."month" = bsr."month" AND bah."day" = bsr."day" AND bah."hour" = bsr."hour"
                JOIN berlin_groundtemp bg ON bg."year" = bsr."year" AND bg."month" = bsr."month" AND bg."day" = bsr."day" AND bg."hour" = bsr."hour"
                JOIN berlin_rainfall br ON br."year" = bsr."year" AND br."month" = bsr."month" AND br."day" = bsr."day" AND br."hour" = bsr."hour"
                JOIN berlin_sunshine bss ON bss."year" = bsr."year" AND bss."month" = bsr."month" AND bss."day" = bsr."day" AND bss."hour" = bsr."hour"
                JOIN berlin_wind bw ON bw."year" = bsr."year" AND bw."month" = bsr."month" AND bw."day" = bsr."day" AND bw."hour" = bsr."hour"
                OFFSET {} LIMIT {}
                )
               """
        offset = 0
        limit = 500

        while(offset <= 1000000):
            _statement = _sql.format(offset, limit)
            self.__cur.execute(_statement)
            self.__cur.execute("COMMIT")
            offset += limit
    
    def calculateAvgSpeed(self):
        _sql = """
               SELECT osm_id, avg_speed_kmh, visits FROM berlin_speed
               """
        self.__cur.execute(_sql)
        wavgDict = dict()

        for item in self.__cur.fetchall():
            if(not item[0] in wavgDict.keys()):
                wavgDict[item[0]] = {'avg': float(item[1]), 'visits': int(item[2])}
            else:
                wavgDict[item[0]]['avg'] += float(item[1]) * int(item[2])
                wavgDict[item[0]]['visits'] += int(item[2])

        _sql = """
               UPDATE berlin_speed_avg 
               SET avg_speed_kmh = {}
               WHERE osm_id = {}
               """
        counter = 0
        for key, val in wavgDict.items():
            _statement = _sql.format(str(val['avg'] / val['visits']), str(key))
            self.__cur.execute(_statement)
            counter += 1
            if(not counter % 100):
                counter = 0
                self.__cur.execute("COMMIT")

    def calculateAvgBikenum(self):
        _sql = """
               SELECT osm_id, occurrences FROM berlin_bikenum
               """
        self.__cur.execute(_sql)
        wavgDict = dict()

        for item in self.__cur.fetchall():
            if(not item[0] in wavgDict.keys()):
                wavgDict[item[0]] = {'avg': int(item[1]), 'visits': int(1)}
            else:
                wavgDict[item[0]]['avg'] += int(item[1])
                wavgDict[item[0]]['visits'] += 1

        _sql = """
               UPDATE berlin_bikenum_avg 
               SET occurrences = {}
               WHERE osm_id = {}
               """
        counter = 0
        for key, val in wavgDict.items():
            _statement = _sql.format(str(val['avg'] / val['visits']), str(key))
            self.__cur.execute(_statement)
            counter += 1
            if(not counter % 100):
                counter = 0
                self.__cur.execute("COMMIT")

    def updateDuration(self):
        _sql = """
               SELECT ride_id, "timestamp" FROM berlin_feature_partial_final_new
               WHERE ride_id IN (
                SELECT DISTINCT ride_id FROM berlin_final_new
               )
               """
        
        self.__cur.execute(_sql)
        durDict = dict()
        for item in self.__cur.fetchall():
            if(item[0] not in durDict.keys()):
                durDict[item[0]] = {'max': -1, 'min': 90000000000000}
            if(int(item[1]) > durDict[item[0]]['max']):
                durDict[item[0]]['max'] = int(item[1])
            if(int(item[1]) < durDict[item[0]]['min']):
                durDict[item[0]]['min'] = int(item[1])

        durProcessed = {k: int(v['max'] - v['min']) for k, v in durDict.items()}
        _sql = """
               UPDATE berlin_final_new SET duration = {} WHERE ride_id = {}
               """
        
        count = 0
        for k, v in durProcessed.items():
            _statement = _sql.format(str(v), str(k))
            self.__cur.execute(_statement)
            count += 1
            if(not count % 500):
                self.__cur.execute("COMMIT")
                count = 0

    def createTableAndImportData(self):
        _table_ = 'berlin_feature_partial_final_new'
        _simra_table = 'berlin_simra_rides_4k'
        _alt_table_ = 'berlin_alt_routes_4k_new'
        _sql = """
               CREATE TABLE IF NOT EXISTS {} AS
                (SELECT bsr.ride_id, bsr.lat, bsr.lon, bsr.x, bsr.y, bsr.z, bsr.acc, bsr.a, bsr.b, bsr.c, bsr."timestamp", bsr.obsdistanceleft1, bsr.obsdistanceleft2, bsr.obsdistanceright1, bsr.obsdistanceright2, bsr.obsclosepassevent, bsr.xl,bsr.yl, bsr.zl, bsr.rx, bsr.ry, bsr.rz,
                bsr.rc, bsr."year", bsr."month", bsr."day", bsr."hour", bsr.osm_id, bsr.geo_point, bba.gml_id, sobj_kz, bba.segm_segm, bba.segm_bez, bba.stst_str, bba.stor_name, bba.ortstl, bba.rva_typ, bba.sorvt_typ, bba.laenge, bba.b_pflicht, bba.edge_geo_3857, bbn.occurrences, bs.avg_speed_kmh, "access", bor."addr:housename", bor."addr:housenumber", bor."addr:interpolation", bor.admin_level, bor.aerialway, bor.aeroway, bor.amenity, bor."area", bor.barrier, bor.bicycle, bor.brand, bor.bridge, bor.boundary, bor.building, bor.construction, bor.covered, bor.culvert, bor.cutting, bor.denomination, bor.disused, bor.embankment, bor.foot, bor."generator:source", bor.harbour, bor.highway, bor.historic, bor.horse, bor.intermittent, bor.junction, bor.landuse, bor.layer, bor.leisure, bor."lock", bor.man_made, bor.military, bor.motorcar, bor."name", bor."natural", bor.office, bor.oneway, bor."operator", bor.place, bor.population, bor.power, bor.power_source, bor.public_transport, bor.railway, bor."ref", bor.religion, bor.route, bor.service, bor.shop, bor.sport, bor.surface, bor.toll, bor.tourism, bor."tower:type", bor.tracktype, bor.tunnel, bor.water, bor.waterway, bor.wetland, bor."width", bor.wood, bor.z_order, bor.way_area, bor.way, bah.airtemp, bah.rel_humid, bg.temp_2cm, bg.temp_5cm, bg.temp_10cm, bg.temp_20cm, bg.temp_50cm, bg.temp_100cm, br.precipitation_1h, br.precipitation_indicator, br.precipitation_type, bss.dur_sunshine, bw.wind_spd, bw.wind_direction
                FROM {} bsr 
                LEFT OUTER JOIN berlin_bikearea bba ON bba.osm_id = bsr.osm_id
                LEFT OUTER JOIN berlin_speed_avg bs ON bs.osm_id = bsr.osm_id
                LEFT OUTER JOIN berlin_bikenum_avg bbn ON bbn.osm_id = bsr.osm_id
                JOIN berlin_osm_roads bor ON bor.osm_id = bsr.osm_id
                JOIN berlin_airtemp_humidity bah ON bah."year" = bsr."year" AND bah."month" = bsr."month" AND bah."day" = bsr."day" AND bah."hour" = bsr."hour"
                JOIN berlin_groundtemp bg ON bg."year" = bsr."year" AND bg."month" = bsr."month" AND bg."day" = bsr."day" AND bg."hour" = bsr."hour"
                JOIN berlin_rainfall br ON br."year" = bsr."year" AND br."month" = bsr."month" AND br."day" = bsr."day" AND br."hour" = bsr."hour"
                JOIN berlin_sunshine bss ON bss."year" = bsr."year" AND bss."month" = bsr."month" AND bss."day" = bsr."day" AND bss."hour" = bsr."hour"
                JOIN berlin_wind bw ON bw."year" = bsr."year" AND bw."month" = bsr."month" AND bw."day" = bsr."day" AND bw."hour" = bsr."hour")

               """
        _statement = _sql.format(_table_, _simra_table)
        self.__cur.execute(_statement)
        self.__cur.execute('COMMIT')

        _sql = """
               ALTER TABLE {} ADD duration float8
               """
        _statement = _sql.format(_table_)
        self.__cur.execute(_statement)
        self.__cur.execute('COMMIT')

        _sql = """
               ALTER TABLE {} ADD label int4
               """
        _statement = _sql.format(_table_)
        self.__cur.execute(_statement)
        self.__cur.execute('COMMIT')

        _sql = """
               UPDATE {} SET label = 1
               """
        _statement = _sql.format(_table_)
        self.__cur.execute(_statement)
        self.__cur.execute('COMMIT')

        _sql = """
               INSERT INTO {} (
                ride_id, lat, lon, "timestamp", "year", "month", "day", "hour", osm_id, geo_point, gml_id, sobj_kz, segm_segm, segm_bez, stst_str, stor_name, ortstl, rva_typ, sorvt_typ, laenge, b_pflicht, edge_geo_3857, occurrences, avg_speed_kmh, "access", "addr:housename", "addr:housenumber", "addr:interpolation", admin_level, aerialway, aeroway, amenity, "area", barrier, bicycle, brand, bridge, boundary, building, construction, covered, culvert, cutting, denomination, disused, embankment, foot, "generator:source", harbour, highway, historic, horse, intermittent, junction, landuse, layer, leisure, "lock", man_made, military, motorcar, "name", "natural", office, oneway, "operator", place, population, "power", power_source, public_transport, railway, "ref", religion, route, service, shop, sport, surface, toll, tourism, "tower:type", tracktype, tunnel, water, waterway, wetland, "width", wood, z_order, way_area, way, airtemp, rel_humid, temp_2cm, temp_5cm, temp_10cm, temp_20cm, temp_50cm, temp_100cm, precipitation_1h, precipitation_indicator, precipitation_type, dur_sunshine, wind_spd, wind_direction, "label")
                (SELECT bsr.ride_id, bsr.lat, bsr.lon, bsr."timestamp", 
                bsr."year", bsr."month", bsr."day", bsr."hour", bsr.osm_id, bsr.geo_point, bba.gml_id, sobj_kz, bba.segm_segm, bba.segm_bez, bba.stst_str, bba.stor_name, bba.ortstl, bba.rva_typ, bba.sorvt_typ, bba.laenge, bba.b_pflicht, bba.edge_geo_3857, bbn.occurrences, bs.avg_speed_kmh, "access", bor."addr:housename", bor."addr:housenumber", bor."addr:interpolation", bor.admin_level, bor.aerialway, bor.aeroway, bor.amenity, bor."area", bor.barrier, bor.bicycle, bor.brand, bor.bridge, bor.boundary, bor.building, bor.construction, bor.covered, bor.culvert, bor.cutting, bor.denomination, bor.disused, bor.embankment, bor.foot, bor."generator:source", bor.harbour, bor.highway, bor.historic, bor.horse, bor.intermittent, bor.junction, bor.landuse, bor.layer, bor.leisure, bor."lock", bor.man_made, bor.military, bor.motorcar, bor."name", bor."natural", bor.office, bor.oneway, bor."operator", bor.place, bor.population, bor.power, bor.power_source, bor.public_transport, bor.railway, bor."ref", bor.religion, bor.route, bor.service, bor.shop, bor.sport, bor.surface, bor.toll, bor.tourism, bor."tower:type", bor.tracktype, bor.tunnel, bor.water, bor.waterway, bor.wetland, bor."width", bor.wood, bor.z_order, bor.way_area, bor.way, bah.airtemp, bah.rel_humid, bg.temp_2cm, bg.temp_5cm, bg.temp_10cm, bg.temp_20cm, bg.temp_50cm, bg.temp_100cm, br.precipitation_1h, br.precipitation_indicator, br.precipitation_type, bss.dur_sunshine, bw.wind_spd, bw.wind_direction, 0 
                FROM {} bsr 
                LEFT OUTER JOIN berlin_bikearea bba ON bba.osm_id = bsr.osm_id
                LEFT OUTER JOIN berlin_speed_avg bs ON bs.osm_id = bsr.osm_id
                LEFT OUTER JOIN berlin_bikenum_avg bbn ON bbn.osm_id = bsr.osm_id
                JOIN berlin_osm_roads bor ON bor.osm_id = bsr.osm_id
                JOIN berlin_airtemp_humidity bah ON bah."year" = bsr."year" AND bah."month" = bsr."month" AND bah."day" = bsr."day" AND bah."hour" = bsr."hour"
                JOIN berlin_groundtemp bg ON bg."year" = bsr."year" AND bg."month" = bsr."month" AND bg."day" = bsr."day" AND bg."hour" = bsr."hour"
                JOIN berlin_rainfall br ON br."year" = bsr."year" AND br."month" = bsr."month" AND br."day" = bsr."day" AND br."hour" = bsr."hour"
                JOIN berlin_sunshine bss ON bss."year" = bsr."year" AND bss."month" = bsr."month" AND bss."day" = bsr."day" AND bss."hour" = bsr."hour"
                JOIN berlin_wind bw ON bw."year" = bsr."year" AND bw."month" = bsr."month" AND bw."day" = bsr."day" AND bw."hour" = bsr."hour")

               """
        
        _statement = _sql.format(_table_, _alt_table_)
        self.__cur.execute(_statement)
        self.__cur.execute('COMMIT')

class featureLoader():
    def __init__(self) -> None:
        self.featureworker = __featureAggWorker()

    def load() -> 'pd.DataFrame':
        pass

if __name__ == "__main__":
    flworker = __featureAggWorker()
    # flworker.updateDuration()
    # flworker.addColforCategoryVars()
    flworker.createTableAndImportData()
    flworker.addColforCategoryVars()
    flworker.processCategoryVals()
    flworker.updateDuration()