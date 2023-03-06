import psycopg2 as pg
import os
import json

from util.singleton import SingletonMeta

__DBBASE__ = '..'
__DBCONF__ = 'dummy'
__DBCNFPATH__ = os.path.join(__DBBASE__, __DBCONF__)

class dbconn(metaclass = SingletonMeta):
    def __init__(self) -> None:
        pass

    @staticmethod
    def connect(conf = __DBCONF__) -> 'pg.connection':
        if(type(conf) is dict):
            try:
                return pg.connect(**conf)
            except:
                return None
        else:
            with open(conf, 'r') as f:
                conninfo = json.load(f)
            try:
                return pg.connect(**conninfo)
            except:
                return None